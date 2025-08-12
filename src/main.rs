use anyhow::{Context, Result};
use futures::StreamExt;
use reqwest::{Client, Proxy};
use serde::Deserialize;
use std::env;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    task,
};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async, tungstenite::protocol::Message, MaybeTlsStream,
    WebSocketStream,
};
use url::Url;

mod events;
use events::{Event, StreamMessage};

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

    // 2) Prepare global "@arr" streams, including 1h & 4h rolling-window tickers
    let mut streams = vec![
        "!miniTicker@arr".to_string(),
        "!ticker@arr".to_string(),
        "!bookTicker@arr".to_string(),
        "!ticker_1h@arr".to_string(),
        "!ticker_4h@arr".to_string(),
    ];

    // 3) Define per-symbol suffixes (spot only), incl. rolling-window tickers
    let suffixes = &[
        "trade",
        "aggTrade",
        "depth",
        "depth5",
        "depth10",
        "depth20",
        "depth@100ms",
        "kline_1m",
        "kline_3m",
        "kline_5m",
        "kline_15m",
        "kline_30m",
        "kline_1h",
        "kline_2h",
        "kline_4h",
        "kline_6h",
        "kline_8h",
        "kline_12h",
        "kline_1d",
        "kline_3d",
        "kline_1w",
        "kline_1M",
        "miniTicker",
        "ticker",
        "bookTicker",
        "ticker_1h",
        "ticker_4h",
    ];

    // 4) Build full list of streams for all TRADING symbols
    for s in info.symbols.into_iter().filter(|s| s.status == "TRADING") {
        let sym = s.symbol.to_lowercase();
        for &suffix in suffixes.iter() {
            streams.push(format!("{}@{}", sym, suffix));
        }
    }

    // 5) Chunk the list to avoid exceeding URL length/connection limits
    //    Binance says ~100 streams per connection works.
    const MAX_STREAMS_PER_CONN: usize = 100;
    let chunk_size = env::var("CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(MAX_STREAMS_PER_CONN);

    let total_streams = streams.len();
    println!("🔌 Total Binance.US streams: {}", total_streams);

    let ws_base = "wss://stream.binance.us:9443/stream?streams=";
    let mut handles = Vec::new();

    for chunk in streams.chunks(chunk_size) {
        // Build URL per chunk
        let param = chunk.join("/");
        let chunk_len = chunk.len();

        let url = Url::parse(&format!("{}{}", ws_base, param)).context("parsing WebSocket URL")?;

        let proxy = proxy_url.clone();

        handles.push(task::spawn(async move {
            println!("→ opening WS: {} ({} streams)", url, chunk_len);
            if proxy.is_empty() {
                let (ws_stream, _) = connect_async(url).await.context("connecting WebSocket")?;
                run_ws(ws_stream).await?;
            } else {
                let ws_stream = connect_via_socks5(url, &proxy).await?;
                run_ws(ws_stream).await?;
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    // 6) Await all connections (runs indefinitely)
    for handle in handles {
        handle.await??;
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
                Ok(event) => println!("{:#?}", event),
                Err(e) => eprintln!("failed to parse message: {}", e),
            },
            Ok(_) => {} // ignore pings/pongs and binary
            Err(e) => {
                eprintln!("❌ WS error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
