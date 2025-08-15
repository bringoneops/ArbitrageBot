use anyhow::Result;
use arb_core as core;
use dashmap::DashMap;
use reqwest::Client;
use rustls::ClientConfig;
use std::sync::Arc;
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinSet,
};
use tracing::error;

pub mod adapter;
pub mod registry;
pub use adapter::binance::{
    fetch_symbols as fetch_binance_symbols, BinanceAdapter, BINANCE_EXCHANGES,
};
pub use adapter::mexc::{fetch_symbols, MexcAdapter, MEXC_EXCHANGES};
pub use adapter::ExchangeAdapter;

/// Shared task set type for spawning and tracking asynchronous tasks.
pub type TaskSet = Arc<Mutex<JoinSet<()>>>;

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
    task_set: TaskSet,
    event_txs: Arc<DashMap<String, mpsc::Sender<core::events::StreamMessage<'static>>>>,
    tls_config: Arc<ClientConfig>,
    event_buffer_size: usize,
) -> Result<Vec<mpsc::Receiver<core::events::StreamMessage<'static>>>> {
    adapter::binance::register();
    adapter::mexc::register();

    let mut receivers = Vec::new();

    for exch in &cfg.exchanges {
        if let Some(factory) = registry::get_adapter(&exch.id) {
            let mut res = factory(
                cfg,
                exch,
                client.clone(),
                task_set.clone(),
                event_txs.clone(),
                tls_config.clone(),
                event_buffer_size,
            )
            .await?;
            receivers.append(&mut res);
        } else {
            error!("No adapter factory registered for {}", exch.id);
        }
    }

    Ok(receivers)
}
