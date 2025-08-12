use anyhow::{Context, Result};
use futures::StreamExt;
use reqwest::{Client, Proxy};
use serde::Deserialize;
use std::env;
use tokio::task;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

use binance_us_and_global::chunk_streams;

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

    // 1. Fetch exchangeInfo from Binance.US
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

    // 2. Collect trading symbols
    let symbols: Vec<String> = info
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING")
        .map(|s| s.symbol)
        .collect();
    let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();

    // 3. Generate and chunk streams
    let chunks = chunk_streams(&symbol_refs);
    let total_streams: usize = chunks.iter().map(|c| c.len()).sum();
    println!("🔌 Total Binance.US streams: {}", total_streams);

    let ws_base = "wss://stream.binance.us:9443/stream?streams=";
    let mut handles = Vec::new();

    for chunk in chunks {
        // **capture only owned data for the task**
        let param = chunk.join("/");
        let chunk_len = chunk.len();
        let url = Url::parse(&format!("{}{}", ws_base, param)).context("parsing WebSocket URL")?;

        handles.push(task::spawn(async move {
            println!("→ opening WS: {} ({} streams)", url, chunk_len);
            let (ws_stream, _) = connect_async(url).await.context("connecting WebSocket")?;
            let (_, mut read) = ws_stream.split();

            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        // TODO: deserialize `text` into your event structs
                        println!("{}", text);
                    }
                    Ok(_) => {} // ignore pings/pongs and binary
                    Err(e) => {
                        eprintln!("❌ WS error: {}", e);
                        break;
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        }));
    }

    // 4. Await all connections (runs indefinitely)
    for handle in handles {
        handle.await??;
    }

    Ok(())
}
