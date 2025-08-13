use crate::{
    apply_depth_update, chunk_streams_with_config, handle_stream_event, next_backoff,
    stream_config_for_exchange, DepthSnapshot, OrderBook,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::{stream, SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use std::{collections::HashMap, env, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{mpsc, Mutex},
    task::JoinSet,
    time::{sleep, Duration, Instant},
};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async, tungstenite::protocol::Message, MaybeTlsStream,
    WebSocketStream,
};
use url::Url;

use crate::events::{Event, StreamMessage};

use super::ExchangeAdapter;

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
struct SymbolInfo {
    symbol: String,
    status: String,
}

/// Configuration for a single Binance exchange endpoint.
pub struct BinanceConfig {
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
}

/// All Binance exchanges supported by this adapter.
pub const BINANCE_EXCHANGES: &[BinanceConfig] = &[
    BinanceConfig {
        name: "Binance.US Spot",
        info_url: "https://api.binance.us/api/v3/exchangeInfo",
        ws_base: "wss://stream.binance.us:9443/stream?streams=",
    },
    BinanceConfig {
        name: "Binance Global Spot",
        info_url: "https://api.binance.com/api/v3/exchangeInfo",
        ws_base: "wss://stream.binance.com:9443/stream?streams=",
    },
    BinanceConfig {
        name: "Binance Futures",
        info_url: "https://fapi.binance.com/fapi/v1/exchangeInfo",
        ws_base: "wss://fstream.binance.com/stream?streams=",
    },
    BinanceConfig {
        name: "Binance Delivery",
        info_url: "https://dapi.binance.com/dapi/v1/exchangeInfo",
        ws_base: "wss://dstream.binance.com/stream?streams=",
    },
    BinanceConfig {
        name: "Binance Options",
        info_url: "https://vapi.binance.com/vapi/v1/exchangeInfo",
        ws_base: "wss://vstream.binance.com/stream?streams=",
    },
];

/// Adapter implementing the `ExchangeAdapter` trait for Binance.
pub struct BinanceAdapter {
    cfg: &'static BinanceConfig,
    client: Client,
    chunk_size: usize,
    proxy_url: String,
    tasks: Arc<Mutex<JoinSet<()>>>,
    event_tx: mpsc::Sender<StreamMessage<Event>>,
    symbols: Vec<String>,
    orderbooks: Arc<Mutex<HashMap<String, OrderBook>>>,
}

impl BinanceAdapter {
    pub fn new(
        cfg: &'static BinanceConfig,
        client: Client,
        chunk_size: usize,
        proxy_url: String,
        tasks: Arc<Mutex<JoinSet<()>>>,
        event_tx: mpsc::Sender<StreamMessage<Event>>,
    ) -> Self {
        Self {
            cfg,
            client,
            chunk_size,
            proxy_url,
            tasks,
            event_tx,
            symbols: Vec::new(),
            orderbooks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ExchangeAdapter for BinanceAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        let symbol_refs: Vec<&str> = self.symbols.iter().map(|s| s.as_str()).collect();
        let cfg = stream_config_for_exchange(self.cfg.name);
        let chunks = chunk_streams_with_config(&symbol_refs, self.chunk_size, cfg);
        let total_streams: usize = chunks.iter().map(|c| c.len()).sum();
        tracing::info!(
            "\u{1F50C} Total {} streams: {}",
            self.cfg.name,
            total_streams
        );

        for chunk in chunks {
            let chunk_len = chunk.len();
            let param = chunk.join("/");
            let url = Url::parse(&format!("{}{}", self.cfg.ws_base, param))
                .context("parsing WebSocket URL")?;
            let proxy = self.proxy_url.clone();
            let name = self.cfg.name.to_string();
            let tasks = self.tasks.clone();
            let books = self.orderbooks.clone();
            let event_tx = self.event_tx.clone();

            tasks.lock().await.spawn(async move {
                let max_backoff_secs = env::var("MAX_BACKOFF_SECS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(64);
                let max_backoff = Duration::from_secs(max_backoff_secs);
                let min_stable = Duration::from_secs(30);
                let mut backoff = Duration::from_secs(1);
                loop {
                    tracing::info!(
                        "\u{2192} opening WS ({}): {} ({} streams)",
                        name,
                        url,
                        chunk_len
                    );
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
                            tracing::error!("WS error: {}", e);
                        }
                    } else {
                        tracing::warn!("WS stream closed");
                    }

                    let elapsed = start.elapsed();
                    if connected {
                        backoff = next_backoff(backoff, elapsed, ok, max_backoff, min_stable);
                    } else {
                        backoff = std::cmp::min(backoff * 2, max_backoff);
                    }

                    tracing::warn!("Reconnecting in {:?}...", backoff);
                    sleep(backoff).await;
                }
            });
        }

        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        self.auth().await?;
        self.backfill().await?;
        self.subscribe().await
    }

    async fn heartbeat(&mut self) -> Result<()> {
        Ok(())
    }

    async fn auth(&mut self) -> Result<()> {
        Ok(())
    }

    async fn backfill(&mut self) -> Result<()> {
        let info: ExchangeInfo = self
            .client
            .get(self.cfg.info_url)
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

        let depth_base = self.cfg.info_url.trim_end_matches("exchangeInfo");
        let mut books_map: HashMap<String, OrderBook> = HashMap::new();

        let fetches = stream::iter(symbols.clone())
            .map(|sym| {
                let depth_url = format!("{}depth?symbol={}", depth_base, sym);
                let client = self.client.clone();
                async move {
                    let resp = match client.get(&depth_url).send().await {
                        Ok(resp) => resp,
                        Err(e) => {
                            tracing::warn!(
                                "depth snapshot GET failed for {} ({}): {}",
                                sym,
                                depth_url,
                                e
                            );
                            return None;
                        }
                    };

                    let resp = match resp.error_for_status() {
                        Ok(resp) => resp,
                        Err(e) => {
                            tracing::warn!(
                                "depth snapshot non-2xx for {} ({}): {}",
                                sym,
                                depth_url,
                                e
                            );
                            return None;
                        }
                    };

                    if let Ok(snapshot) = resp.json::<DepthSnapshot>().await {
                        Some((sym, snapshot.into()))
                    } else {
                        None
                    }
                }
            })
            .buffer_unordered(10);

        futures::pin_mut!(fetches);
        while let Some(result) = fetches.next().await {
            if let Some((sym, book)) = result {
                books_map.insert(sym, book);
            }
        }

        self.symbols = symbol_refs.into_iter().map(|s| s.to_string()).collect();
        self.orderbooks = Arc::new(Mutex::new(books_map));
        Ok(())
    }
}

// --- Internal helpers -----------------------------------------------------

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

async fn run_ws<S>(
    ws_stream: WebSocketStream<S>,
    books: Arc<Mutex<HashMap<String, OrderBook>>>,
    event_tx: mpsc::Sender<StreamMessage<Event>>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut write, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => match serde_json::from_str::<StreamMessage<Event>>(&text) {
                Ok(event) => {
                    metrics::counter!("ws_events").increment(1);
                    #[cfg(feature = "debug-logs")]
                    tracing::debug!(?event);
                    handle_stream_event(&event, &text);
                    if let Event::DepthUpdate(ref update) = event.data {
                        let mut map = books.lock().await;
                        if let Some(book) = map.get_mut(&update.symbol) {
                            apply_depth_update(book, update);
                        }
                    }
                    if let Err(e) = event_tx.send(event).await {
                        tracing::warn!("failed to send event: {}", e);
                    }
                }
                Err(e) => tracing::error!("failed to parse message: {}", e),
            },
            Ok(Message::Ping(payload)) => {
                write.send(Message::Pong(payload)).await?;
            }
            Ok(Message::Pong(_)) => {}
            Ok(_) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
