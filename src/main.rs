use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use reqwest::{Client, Proxy};
use serde::Deserialize;
use std::env;
use tokio::task::JoinSet;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{sleep, Duration},
};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async, tungstenite::protocol::Message, MaybeTlsStream,
    WebSocketStream,
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;

use binance_us_and_global::{
    chunk_streams,
    events::{Event, StreamMessage},
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

    let mut tasks = JoinSet::new();

    spawn_exchange(
        "Binance.US Spot",
        "https://api.binance.us/api/v3/exchangeInfo",
        "wss://stream.binance.us:9443/stream?streams=",
        &client,
        chunk_size,
        &proxy_url,
        &mut tasks,
    )
    .await?;

    spawn_exchange(
        "Binance Global Spot",
        "https://api.binance.com/api/v3/exchangeInfo",
        "wss://stream.binance.com:9443/stream?streams=",
        &client,
        chunk_size,
        &proxy_url,
        &mut tasks,
    )
    .await?;

    spawn_exchange(
        "Binance Futures",
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "wss://fstream.binance.com/stream?streams=",
        &client,
        chunk_size,
        &proxy_url,
        &mut tasks,
    )
    .await?;

    spawn_exchange(
        "Binance Delivery",
        "https://dapi.binance.com/dapi/v1/exchangeInfo",
        "wss://dstream.binance.com/stream?streams=",
        &client,
        chunk_size,
        &proxy_url,
        &mut tasks,
    )
    .await?;

    while let Some(res) = tasks.join_next().await {
        res?;
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
    tasks: &mut JoinSet<()>,
) -> Result<()> {
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

    let chunks = chunk_streams(&symbol_refs, chunk_size);
    let total_streams: usize = chunks.iter().map(|c| c.len()).sum();
    info!("🔌 Total {} streams: {}", name, total_streams);

    for chunk in chunks {
        let chunk_len = chunk.len();
        let param = chunk.join("/");
        let url = Url::parse(&format!("{}{}", ws_base, param)).context("parsing WebSocket URL")?;
        let proxy = proxy_url.to_string();
        let name = name.to_string();

        tasks.spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                info!("→ opening WS ({}): {} ({} streams)", name, url, chunk_len);
                let result = if proxy.is_empty() {
                    match connect_async(url.clone()).await {
                        Ok((ws_stream, _)) => run_ws(ws_stream).await,
                        Err(e) => Err(e.into()),
                    }
                } else {
                    match connect_via_socks5(url.clone(), &proxy).await {
                        Ok(ws_stream) => run_ws(ws_stream).await,
                        Err(e) => Err(e),
                    }
                };

                if let Err(e) = result {
                    error!("WS error: {}", e);
                } else {
                    warn!("WS stream closed");
                }

                warn!("Reconnecting in {:?}...", backoff);
                sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, Duration::from_secs(64));
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
async fn run_ws<S>(ws_stream: WebSocketStream<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut write, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => match serde_json::from_str::<StreamMessage<Event>>(&text) {
                Ok(event) => {
                    match &event.data {
                        Event::Trade(data) => info!(event = "trade", symbol = %data.symbol),
                        Event::AggTrade(data) => info!(event = "aggTrade", symbol = %data.symbol),
                        Event::DepthUpdate(data) => {
                            info!(event = "depthUpdate", symbol = %data.symbol)
                        }
                        Event::Kline(data) => info!(event = "kline", symbol = %data.symbol),
                        Event::MiniTicker(data) => {
                            info!(event = "miniTicker", symbol = %data.symbol)
                        }
                        Event::Ticker(data) => info!(event = "ticker", symbol = %data.symbol),
                        Event::BookTicker(data) => {
                            info!(event = "bookTicker", symbol = %data.symbol)
                        }
                        Event::MarkPrice(data) => {
                            info!(event = "markPriceUpdate", symbol = %data.symbol)
                        }
                        Event::ForceOrder(data) => {
                            info!(event = "forceOrder", symbol = %data.order.symbol)
                        }
                        Event::Unknown => info!(event = "unknown", stream = %event.stream),
                    }
                    debug!(?event);
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
