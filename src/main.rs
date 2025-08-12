use anyhow::{Context, Result};
use futures::StreamExt;
use reqwest::{Client, Proxy};
use serde::Deserialize;
use std::env;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    task,
    time::{sleep, Duration},
};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async, tungstenite::protocol::Message, MaybeTlsStream,
    WebSocketStream,
};
use tracing::{error, info, warn};
use url::Url;

use binance_us_and_global::{chunk_streams, events::{Event, StreamMessage}};

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
    tracing_subscriber::fmt::init();

    // Optional SOCKS5 proxy, e.g. "host:port"
    let proxy_url = env::var("SOCKS5_PROXY").unwrap_or_default();

    // Build HTTP client (with rustls) and apply proxy if present
    let mut client_builder = Client::builder()
        .user_agent("binance-us-all-streams")
        .use_rustls_tls();
    if !proxy_url.is_empty() {
        client_builder = client_builder
            .proxy(Proxy::all(format!("socks5h://{}", proxy_url)).context("invalid proxy URL")?);
    }
    let client = client_builder.build().context("building HTTP client")?;

    // 1) Fetch exchangeInfo from Binance.US
    let api_base = "https://api.binance.us";
    let info: ExchangeInfo = client
        .get(format!("{}/api/v3/exchangeInfo", api_base))
        .send()
        .await
        .context("sending exchangeInfo request")?
        .error_for_status()
        .context("non-2xx status fetching exchangeInfo")?
        .json()
        .await
        .context("parsing exchangeInfo JSON")?;

    // 2) Gather all TRADING symbols
    let symbols: Vec<String> = info
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING")
        .map(|s| s.symbol)
        .collect();
    let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();

    // 3) Chunk the list to avoid exceeding URL length/connection limits
    //    Binance says ~100 streams per connection works.
    const MAX_STREAMS_PER_CONN: usize = 100;
    let chunk_size = env::var("CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(MAX_STREAMS_PER_CONN);

    let chunks = chunk_streams(&symbol_refs, chunk_size);
    let total_streams: usize = chunks.iter().map(|c| c.len()).sum();
    info!("🔌 Total Binance.US streams: {}", total_streams);

    let ws_base = "wss://stream.binance.us:9443/stream?streams=";
    let mut handles = Vec::new();

    for chunk in chunks {
        // Build URL per chunk
        let param = chunk.join("/");
        let chunk_len = chunk.len();

        let url = Url::parse(&format!("{}{}", ws_base, param)).context("parsing WebSocket URL")?;

        let proxy = proxy_url.clone();

        handles.push(task::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                info!("→ opening WS: {} ({} streams)", url, chunk_len);
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
        }));
    }

    // 4) Await all connection tasks (runs indefinitely)
    for handle in handles {
        handle.await?;
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
    let (_, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => match serde_json::from_str::<StreamMessage<Event>>(&text) {
                Ok(event) => info!("{:#?}", event),
                Err(e) => error!("failed to parse message: {}", e),
            },
            Ok(_) => {} // ignore pings/pongs and binary
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}
