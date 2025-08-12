use anyhow::{Context, Result};
use futures::{future, Future, SinkExt, StreamExt};
use metrics::counter;
use reqwest::{Client, Proxy};
use serde::Deserialize;
use std::{collections::HashMap, env, pin::Pin, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{sleep, Duration, Instant},
};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async, tungstenite::protocol::Message, MaybeTlsStream,
    WebSocketStream,
};
#[cfg(feature = "debug-logs")]
use tracing::debug;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;

use binance_us_and_global::{
    apply_depth_update, chunk_streams_with_config,
    events::{Event, StreamMessage},
    handle_stream_event, next_backoff, stream_config_for_exchange, DepthSnapshot, OrderBook,
};

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
struct SymbolInfo {
    symbol: String,
    status: String,
}

struct ExchangeConfig {
    name: &'static str,
    info_url: &'static str,
    ws_base: &'static str,
}

static EXCHANGES: &[ExchangeConfig] = &[
    ExchangeConfig {
        name: "Binance.US Spot",
        info_url: "https://api.binance.us/api/v3/exchangeInfo",
        ws_base: "wss://stream.binance.us:9443/stream?streams=",
    },
    ExchangeConfig {
        name: "Binance Global Spot",
        info_url: "https://api.binance.com/api/v3/exchangeInfo",
        ws_base: "wss://stream.binance.com:9443/stream?streams=",
    },
    ExchangeConfig {
        name: "Binance Futures",
        info_url: "https://fapi.binance.com/fapi/v1/exchangeInfo",
        ws_base: "wss://fstream.binance.com/stream?streams=",
    },
    ExchangeConfig {
        name: "Binance Delivery",
        info_url: "https://dapi.binance.com/dapi/v1/exchangeInfo",
        ws_base: "wss://dstream.binance.com/stream?streams=",
    },
    ExchangeConfig {
        name: "Binance Options",
        info_url: "https://vapi.binance.com/vapi/v1/exchangeInfo",
        ws_base: "wss://vstream.binance.com/stream?streams=",
    },
];

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let proxy_url = env::var("SOCKS5_PROXY").unwrap_or_default();

    let mut client_builder = Client::builder()
        .user_agent("binance-us-all-streams")
        .use_rustls_tls();
    if !proxy_url.is_empty() {
        client_builder = client_builder
            .proxy(Proxy::all(format!("socks5h://{}", proxy_url)).context("invalid proxy URL")?);
    }
    let client = client_builder.build().context("building HTTP client")?;

    const MAX_STREAMS_PER_CONN: usize = 100;
    let chunk_size = env::var("CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(MAX_STREAMS_PER_CONN);

    let tasks = Arc::new(Mutex::new(JoinSet::new()));

    let (event_tx, _event_rx) = mpsc::unbounded_channel::<StreamMessage<Event>>();

    let mut inits: Vec<Pin<Box<dyn Future<Output = (&'static str, Result<()>)> + Send>>> =
        Vec::new();

    for cfg in EXCHANGES.iter() {
        let tasks = tasks.clone();
        let client = client.clone();
        let proxy_url = proxy_url.clone();
        let event_tx = event_tx.clone();
        let name = cfg.name;
        let info_url = cfg.info_url;
        let ws_base = cfg.ws_base;
        inits.push(Box::pin(async move {
            (
                name,
                spawn_exchange(
                    name, info_url, ws_base, &client, chunk_size, &proxy_url, tasks, event_tx,
                )
                .await,
            )
        }));
    }

    for (name, result) in future::join_all(inits).await {
        if let Err(e) = result {
            error!("Failed to initialize {}: {}", name, e);
        }
    }

    let mut tasks = tasks.lock().await;
    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            error!("task error: {}", e);
        }
    }

    Ok(())
}

async fn spawn_exchange(
    name: &str,
    exchange_info_url: &str,
    ws_base: &str,
    client: &Client,
    chunk_size: usize,
    proxy_url: &str,
    tasks: Arc<Mutex<JoinSet<()>>>,
    event_tx: mpsc::UnboundedSender<StreamMessage<Event>>,
) -> Result<()> {
    // 1) Pull tradable symbols
    let info: ExchangeInfo = client
        .get(exchange_info_url)
        .send()
        .await
        .context("sending exchangeInfo request")?
        .error_for_status()
        .context("non-2xx status fetching exchangeInfo")?
        .json()
        .await
        .context("parsing exchangeInfo JSON")?;

    let symbols: Vec<String> = info
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING")
        .map(|s| s.symbol)
        .collect();
    let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();

    // 2) Seed order books via REST depth snapshots, so depth updates apply cleanly.
    let depth_base = exchange_info_url.trim_end_matches("exchangeInfo");
    let mut books_map: HashMap<String, OrderBook> = HashMap::new();
    for sym in &symbols {
        let depth_url = format!("{}depth?symbol={}", depth_base, sym);
        let resp = match client.get(&depth_url).send().await {
            Ok(resp) => resp,
            Err(e) => {
                warn!("depth snapshot GET failed for {} ({}): {}", sym, depth_url, e);
                continue;
            }
        };

        let resp = match resp.error_for_status() {
            Ok(resp) => resp,
            Err(e) => {
                warn!("depth snapshot non-2xx for {} ({}): {}", sym, depth_url, e);
                continue;
            }
        };

        if let Ok(snapshot) = resp.json::<DepthSnapshot>().await {
            books_map.insert(sym.clone(), snapshot.into());
        }
    }
    let orderbooks = Arc::new(Mutex::new(books_map));

    // 3) Chunk streams and spawn WS connections
    let cfg = stream_config_for_exchange(name);
    let chunks = chunk_streams_with_config(&symbol_refs, chunk_size, cfg);
    let total_streams: usize = chunks.iter().map(|c| c.len()).sum();
    info!("🔌 Total {} streams: {}", name, total_streams);

    for chunk in chunks {
        let chunk_len = chunk.len();
        let param = chunk.join("/");
        let url = Url::parse(&format!("{}{}", ws_base, param)).context("parsing WebSocket URL")?;
        let proxy = proxy_url.to_string();
        let name = name.to_string();
        let tasks = tasks.clone();
        let books = orderbooks.clone();
        let event_tx = event_tx.clone();

        tasks.lock().await.spawn(async move {
            let max_backoff_secs = env::var("MAX_BACKOFF_SECS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(64);
            let max_backoff = Duration::from_secs(max_backoff_secs);
            let min_stable = Duration::from_secs(30);
            let mut backoff = Duration::from_secs(1);
            loop {
                info!("→ opening WS ({}): {} ({} streams)", name, url, chunk_len);
                let start = Instant::now();
                let mut connected = false;
                let result = if proxy.is_empty() {
                    match connect_async(url.clone()).await {
                        Ok((ws_stream, _)) => {
                            connected = true;
                            backoff = Duration::from_secs(1);
                            run_ws(ws_stream, books.clone(), event_tx.clone()).await
                        }
                        Err(e) => Err(e.into()),
                    }
                } else {
                    match connect_via_socks5(url.clone(), &proxy).await {
                        Ok(ws_stream) => {
                            connected = true;
                            backoff = Duration::from_secs(1);
                            run_ws(ws_stream, books.clone(), event_tx.clone()).await
                        }
                        Err(e) => Err(e),
                    }
                };

                let ok = result.is_ok();
                if !ok {
                    if let Err(e) = &result {
                        error!("WS error: {}", e);
                    }
                } else {
                    warn!("WS stream closed");
                }

                let elapsed = start.elapsed();
                if connected {
                    backoff = next_backoff(backoff, elapsed, ok, max_backoff, min_stable);
                } else {
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }

                warn!("Reconnecting in {:?}...", backoff);
                sleep(backoff).await;
            }
        });
    }

    Ok(())
}

// Establish a WebSocket connection through a SOCKS5 proxy.
async fn connect_via_socks5(
    url: Url,
    proxy_addr: &str,
) -> Result<WebSocketStream<MaybeTlsStream<Socks5Stream<TcpStream>>>> {
    let host = url.host_str().context("URL missing host")?;
    let port = url.port_or_known_default().context("URL missing port")?;
    let target = format!("{}:{}", host, port);

    let stream = Socks5Stream::connect(proxy_addr, target)
        .await
        .context("connecting via SOCKS5 proxy")?;

    let (ws_stream, _) = client_async_tls_with_config(url, stream, None, None)
        .await
        .context("WebSocket handshake via proxy")?;

    Ok(ws_stream)
}

// Shared WebSocket read loop for both direct and proxied connections.
async fn run_ws<S>(
    ws_stream: WebSocketStream<S>,
    books: Arc<Mutex<HashMap<String, OrderBook>>>,
    event_tx: mpsc::UnboundedSender<StreamMessage<Event>>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut write, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => match serde_json::from_str::<StreamMessage<Event>>(&text) {
                Ok(event) => {
                    // metrics + optional debug logging
                    counter!("ws_events").increment(1);
                    #[cfg(feature = "debug-logs")]
                    debug!(?event);

                    // always call the shared handler
                    handle_stream_event(&event, &text);

                    // if it's a depth update, keep the local order book warm
                    if let Event::DepthUpdate(ref update) = event.data {
                        let mut map = books.lock().await;
                        if let Some(book) = map.get_mut(&update.symbol) {
                            apply_depth_update(book, update);
                        }
                    }

                    // fan out raw event to the channel for other consumers
                    let _ = event_tx.send(event);
                }
                Err(e) => error!("failed to parse message: {}", e),
            },
            Ok(Message::Ping(payload)) => {
                write.send(Message::Pong(payload)).await?;
            }
            Ok(Message::Pong(_)) => {}
            Ok(_) => {} // ignore binary and other messages
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}
