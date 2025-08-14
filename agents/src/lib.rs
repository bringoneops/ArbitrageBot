use anyhow::Result;
use arb_core as core;
use dashmap::DashMap;
use reqwest::Client;
use rustls::ClientConfig;
use std::sync::Arc;
use tokio::{
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};
use tracing::error;

pub mod adapter;
pub use adapter::binance::{
    fetch_symbols as fetch_binance_symbols, BinanceAdapter, BINANCE_EXCHANGES,
};
pub use adapter::coinex::{CoinexAdapter, COINEX_EXCHANGES};
pub use adapter::ExchangeAdapter;

/// Run a collection of exchange adapters to completion.
///
/// Each adapter is spawned on the Tokio runtime and awaited. Errors from
/// individual adapters are logged but do not cause early termination.
pub async fn run_adapters<A>(adapters: Vec<A>) -> Result<()>
where
    A: ExchangeAdapter + Send + 'static,
{
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();

    for mut adapter in adapters {
        tasks.spawn(async move { adapter.run().await });
    }

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("adapter error: {}", e),
            Err(e) => error!("task error: {}", e),
        }
    }

    Ok(())
}

/// Instantiate and spawn exchange adapters based on the provided configuration.
///
/// Adapters are created for all enabled exchanges and spawned onto the supplied
/// task set. Each adapter forwards its events through the provided channel.
pub async fn spawn_adapters(
    cfg: &'static core::config::Config,
    client: Client,
    task_tx: mpsc::UnboundedSender<JoinHandle<()>>,
    event_txs: Arc<DashMap<String, mpsc::Sender<core::events::StreamMessage<'static>>>>,
    tls_config: Arc<ClientConfig>,
    event_buffer_size: usize,
) -> Result<Vec<mpsc::Receiver<core::events::StreamMessage<'static>>>> {
    let chunk_size = cfg.chunk_size;
    let mut receivers = Vec::new();

    for exch in BINANCE_EXCHANGES {
        let is_spot = exch.name.contains("Spot");
        let is_derivative = exch.name.contains("Futures")
            || exch.name.contains("Delivery")
            || exch.name.contains("Options");

        if (is_spot && !cfg.enable_spot) || (is_derivative && !cfg.enable_futures) {
            continue;
        }

        let mut symbols = if is_spot {
            cfg.spot_symbols.clone()
        } else {
            cfg.futures_symbols.clone()
        };

        if symbols.is_empty() {
            symbols = fetch_binance_symbols(exch.info_url).await?;
        }

        for symbol in &symbols {
            let (tx, rx) = mpsc::channel(event_buffer_size);
            let key = format!("{}:{}", exch.name, symbol);
            event_txs.insert(key, tx);
            receivers.push(rx);
        }

        let adapter = BinanceAdapter::new(
            exch,
            client.clone(),
            chunk_size,
            cfg.proxy_url.clone().unwrap_or_default(),
            task_tx.clone(),
            event_txs.clone(),
            symbols,
            tls_config.clone(),
        );

        let tx = task_tx.clone();
        let handle = tokio::spawn(async move {
            let mut adapter = adapter;
            if let Err(e) = adapter.run().await {
                error!("Failed to run adapter: {}", e);
            }
        });
        let _ = tx.send(handle);
    }

    for exch in COINEX_EXCHANGES {
        if !cfg.enable_spot {
            continue;
        }

        let mut symbols = cfg.spot_symbols.clone();
        if symbols.is_empty() {
            symbols = adapter::coinex::fetch_symbols(exch.info_url).await?;
        }

        for symbol in &symbols {
            let (tx, rx) = mpsc::channel(event_buffer_size);
            let key = format!("{}:{}", exch.name, symbol);
            event_txs.insert(key, tx);
            receivers.push(rx);
        }

        let adapter = CoinexAdapter::new(
            exch,
            client.clone(),
            chunk_size,
            task_tx.clone(),
            event_txs.clone(),
            symbols,
        );

        let tx = task_tx.clone();
        let handle = tokio::spawn(async move {
            let mut adapter = adapter;
            if let Err(e) = adapter.run().await {
                error!("Failed to run adapter: {}", e);
            }
        });
        let _ = tx.send(handle);
    }

    Ok(receivers)
}
