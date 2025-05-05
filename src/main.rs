use anyhow::{Context, Result};
use futures::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use tokio::task;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

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
    // Build an HTTP client with rustls TLS
    let client = Client::builder()
        .user_agent("binance-us-all-streams")
        .build()
        .context("building HTTP client")?;

    // 1. Fetch exchangeInfo from Binance.US
    let resp = client
        .get("https://api.binance.us/api/v3/exchangeInfo")
        .send()
        .await
        .context("sending exchangeInfo request")?
        .error_for_status()
        .context("non-2xx status fetching exchangeInfo")?;

    // 2. Parse JSON, ensuring we have a `symbols` array
    let info: ExchangeInfo = resp
        .json()
        .await
        .context("parsing exchangeInfo JSON")?;

    // 3. Prepare global "arr" streams
    let mut streams: Vec<String> = vec![
        "!miniTicker@arr".into(),
        "!ticker@arr".into(),
        "!bookTicker".into(),
    ];

    // 4. Define per-symbol suffixes
    let suffixes = [
        "trade", "aggTrade",
        "depth", "depth5", "depth10", "depth20", "depth@100ms",
        "kline_1m", "kline_3m", "kline_5m", "kline_15m", "kline_30m",
        "kline_1h", "kline_2h", "kline_4h", "kline_6h", "kline_8h", "kline_12h",
        "kline_1d", "kline_3d", "kline_1w", "kline_1M",
        "miniTicker", "ticker", "bookTicker",
        "markPrice", "forceOrder",
    ];

    // 5. Build full list of streams
    for s in info.symbols.into_iter().filter(|s| s.status == "TRADING") {
        let sym = s.symbol.to_lowercase();
        for &suffix in &suffixes {
            streams.push(format!("{}@{}", sym, suffix));
        }
    }

    println!("🔌 Total Binance.US streams: {}", streams.len());

    // 6. Chunk the list to avoid URLs that are too long
    let chunk_size = 100;
    let mut handles = Vec::new();

    for chunk in streams.chunks(chunk_size) {
        // Compute these up front so the async move block doesn't borrow `streams`
        let param = chunk.join("/");
        let chunk_len = chunk.len();
        let url_str = format!("wss://stream.binance.us:9443/stream?streams={}", param);
        let url = Url::parse(&url_str).context("parsing WebSocket URL")?;

        handles.push(task::spawn(async move {
            println!("→ opening WS: {} ({} streams)", url, chunk_len);
            let (ws_stream, _) = connect_async(url).await.context("connecting WebSocket")?;
            let (_, mut read) = ws_stream.split();

            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => println!("{}", text),
                    Ok(_) => {}, // ignore non-text
                    Err(e) => {
                        eprintln!("❌ WS error: {}", e);
                        break;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    // 7. Await all connections to run indefinitely
    for handle in handles {
        let _ = handle.await?;
    }

    Ok(())
}
