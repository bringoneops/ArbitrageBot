use anyhow::{Context, Result};
use futures::future;
use reqwest::{Client, Proxy};
use std::{env, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tracing::{error};
use tracing_subscriber::EnvFilter;

use binance_us_and_global::events::{Event, StreamMessage};
use binance_us_and_global::adapter::{ExchangeAdapter};
use binance_us_and_global::adapter::binance::{BinanceAdapter, BINANCE_EXCHANGES};

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

    let mut adapters: Vec<Box<dyn ExchangeAdapter + Send>> = Vec::new();
    for cfg in BINANCE_EXCHANGES {
        adapters.push(Box::new(BinanceAdapter::new(
            cfg,
            client.clone(),
            chunk_size,
            proxy_url.clone(),
            tasks.clone(),
            event_tx.clone(),
        )));
    }

    let init_futures: Vec<_> = adapters
        .into_iter()
        .map(|adapter| async move {
            let mut adapter = adapter;
            adapter.run().await
        })
        .collect();

    for result in future::join_all(init_futures).await {
        if let Err(e) = result {
            error!("Failed to initialize adapter: {}", e);
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

