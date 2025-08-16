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
pub use adapter::bitmart::{
    fetch_symbols as fetch_bitmart_symbols, BitmartAdapter, BITMART_EXCHANGES,
};
pub use adapter::kucoin::{fetch_symbols as fetch_kucoin_symbols, KucoinAdapter, KUCOIN_EXCHANGES};
pub use adapter::latoken::{
    fetch_symbols as fetch_latoken_symbols, LatokenAdapter, LATOKEN_EXCHANGES,
};
pub use adapter::mexc::{fetch_symbols, MexcAdapter, MEXC_EXCHANGES};
pub use adapter::xt::{fetch_symbols as fetch_xt_symbols, XtAdapter, XT_EXCHANGES};
pub use adapter::ExchangeAdapter;

/// Shared task set type for spawning and tracking asynchronous tasks.
pub type TaskSet = Arc<Mutex<JoinSet<()>>>;

/// Registry for event channels, creating them lazily on first use.
#[derive(Clone)]
pub struct ChannelRegistry {
    senders: Arc<DashMap<String, mpsc::Sender<core::events::StreamMessage<'static>>>>,
    buffer: usize,
}

impl ChannelRegistry {
    /// Create a new registry using the provided channel buffer size.
    pub fn new(buffer: usize) -> Self {
        Self {
            senders: Arc::new(DashMap::new()),
            buffer,
        }
    }

    /// Get an existing channel sender or create a new channel pair.
    ///
    /// Returns the sender and `Some(receiver)` if a new channel was created.
    pub fn get_or_create(
        &self,
        key: &str,
    ) -> (
        mpsc::Sender<core::events::StreamMessage<'static>>,
        Option<mpsc::Receiver<core::events::StreamMessage<'static>>>,
    ) {
        use dashmap::mapref::entry::Entry;

        match self.senders.entry(key.to_string()) {
            Entry::Occupied(entry) => (entry.get().clone(), None),
            Entry::Vacant(entry) => {
                let (tx, rx) = mpsc::channel(self.buffer);
                entry.insert(tx.clone());
                (tx, Some(rx))
            }
        }
    }

    /// Retrieve the sender for an existing channel without creating one.
    pub fn get(&self, key: &str) -> Option<mpsc::Sender<core::events::StreamMessage<'static>>> {
        self.senders.get(key).map(|tx| tx.clone())
    }

    /// Current number of registered channels.
    pub fn len(&self) -> usize {
        self.senders.len()
    }
}

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
    channels: ChannelRegistry,
    tls_config: Arc<ClientConfig>,
) -> Result<Vec<mpsc::Receiver<core::events::StreamMessage<'static>>>> {
    adapter::binance::register();
    adapter::gateio::register();
    adapter::mexc::register();
    adapter::bingx::register();
    adapter::kucoin::register();
    adapter::xt::register();
    adapter::bitmart::register();
    adapter::coinex::register();
    adapter::latoken::register();
    adapter::bitget::register();

    let mut receivers = Vec::new();

    for exch in &cfg.exchanges {
        if let Some(factory) = registry::get_adapter(&exch.id) {
            let mut res = factory(
                cfg,
                exch,
                client.clone(),
                task_set.clone(),
                channels.clone(),
                tls_config.clone(),
            )
            .await?;
            receivers.append(&mut res);
        } else {
            error!("No adapter factory registered for {}", exch.id);
        }
    }

    Ok(receivers)
}
