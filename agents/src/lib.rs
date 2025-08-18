use anyhow::Result;
use arb_core as core;
use dashmap::DashMap;
use reqwest::Client;
use rustls::ClientConfig;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Channel sender wrapping separate queues for different market data channels.
#[derive(Clone)]
pub struct StreamSender {
    book: mpsc::Sender<core::events::StreamMessage<'static>>,
    trade: mpsc::Sender<core::events::StreamMessage<'static>>,
    ticker: mpsc::Sender<core::events::StreamMessage<'static>>,
}

impl StreamSender {
    /// Route messages to the appropriate channel queue. Messages are dropped
    /// if the corresponding queue is full.
    #[allow(clippy::result_large_err)]
    pub fn send(
        &self,
        msg: core::events::StreamMessage<'static>,
    ) -> Result<(), mpsc::error::TrySendError<core::events::StreamMessage<'static>>> {
        use core::events::Event;

        let metrics_enabled = core::config::metrics_enabled();
        let channel;

        let res = match msg.data {
            Event::DepthUpdate(_) | Event::BookTicker(_) => {
                channel = "book";
                self.book.try_send(msg)
            }
            Event::Trade(_) | Event::AggTrade(_) => {
                channel = "trade";
                self.trade.try_send(msg)
            }
            Event::Ticker(_) | Event::MiniTicker(_) => {
                channel = "ticker";
                self.ticker.try_send(msg)
            }
            _ => {
                channel = "trade";
                self.trade.try_send(msg)
            }
        };

        if metrics_enabled {
            let depth = match channel {
                "book" => self.book.max_capacity() - self.book.capacity(),
                "trade" => self.trade.max_capacity() - self.trade.capacity(),
                "ticker" => self.ticker.max_capacity() - self.ticker.capacity(),
                _ => 0,
            } as f64;
            metrics::gauge!("adapter_queue_depth", "channel" => channel).set(depth);

            if matches!(res, Err(mpsc::error::TrySendError::Full(_))) {
                metrics::counter!("md_backpressure_drops_total", "channel" => channel).increment(1);
            }
        }

        res
    }
}

/// Registry for event channels, creating them lazily on first use.
#[derive(Clone)]
pub struct ChannelRegistry {
    senders: Arc<DashMap<String, StreamSender>>,
    seq_counters: Arc<DashMap<String, AtomicU64>>,
    book_buffer: usize,
    trade_buffer: usize,
    ticker_buffer: usize,
}

impl ChannelRegistry {
    /// Create a new registry using the provided channel buffer size.
    pub fn new(buffer: usize) -> Self {
        let trade = std::cmp::max(1, buffer / 2);
        let ticker = std::cmp::max(1, buffer / 4);
        Self {
            senders: Arc::new(DashMap::new()),
            seq_counters: Arc::new(DashMap::new()),
            book_buffer: buffer,
            trade_buffer: trade,
            ticker_buffer: ticker,
        }
    }

    /// Get an existing channel sender or create a new channel pair.
    ///
    /// Returns the sender and `Some(receiver)` if a new channel was created.
    pub fn get_or_create(
        &self,
        key: &str,
    ) -> (
        StreamSender,
        Option<mpsc::Receiver<core::events::StreamMessage<'static>>>,
    ) {
        use dashmap::mapref::entry::Entry;

        match self.senders.entry(key.to_string()) {
            Entry::Occupied(entry) => (entry.get().clone(), None),
            Entry::Vacant(entry) => {
                let (book_tx, book_rx) = mpsc::channel(self.book_buffer);
                let (trade_tx, trade_rx) = mpsc::channel(self.trade_buffer);
                let (ticker_tx, ticker_rx) = mpsc::channel(self.ticker_buffer);
                let (out_tx, out_rx) =
                    mpsc::channel(self.book_buffer + self.trade_buffer + self.ticker_buffer);

                dispatcher(
                    book_rx,
                    trade_rx,
                    ticker_rx,
                    out_tx.clone(),
                    self.book_buffer,
                    self.trade_buffer,
                    self.ticker_buffer,
                );

                let tx = StreamSender {
                    book: book_tx,
                    trade: trade_tx,
                    ticker: ticker_tx,
                };
                entry.insert(tx.clone());
                (tx, Some(out_rx))
            }
        }
    }

    /// Retrieve the sender for an existing channel without creating one.
    pub fn get(&self, key: &str) -> Option<StreamSender> {
        self.senders.get(key).map(|tx| tx.clone())
    }

    /// Current number of registered channels.
    pub fn len(&self) -> usize {
        self.senders.len()
    }

    /// Returns true if there are no registered channels.
    pub fn is_empty(&self) -> bool {
        self.senders.is_empty()
    }

    /// Get the next sequence number for the given stream.
    pub fn next_seq_no(&self, key: &str) -> u64 {
        use dashmap::mapref::entry::Entry;
        match self.seq_counters.entry(key.to_string()) {
            Entry::Occupied(entry) => entry.get().fetch_add(1, Ordering::Relaxed),
            Entry::Vacant(entry) => {
                entry.insert(AtomicU64::new(1));
                0
            }
        }
    }
}

fn dispatcher(
    mut book_rx: mpsc::Receiver<core::events::StreamMessage<'static>>,
    mut trade_rx: mpsc::Receiver<core::events::StreamMessage<'static>>,
    mut ticker_rx: mpsc::Receiver<core::events::StreamMessage<'static>>,
    out_tx: mpsc::Sender<core::events::StreamMessage<'static>>,
    book_cap: usize,
    trade_cap: usize,
    ticker_cap: usize,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                Some(msg) = book_rx.recv() => {
                    if out_tx.send(msg).await.is_err() { break; }
                }
                Some(msg) = trade_rx.recv() => {
                    if out_tx.send(msg).await.is_err() { break; }
                }
                Some(msg) = ticker_rx.recv() => {
                    if out_tx.send(msg).await.is_err() { break; }
                }
                else => break,
            }

            if core::config::metrics_enabled() {
                metrics::gauge!("md_queue_depth", "channel" => "book").set(book_rx.len() as f64);
                metrics::gauge!("md_queue_depth", "channel" => "trade").set(trade_rx.len() as f64);
                metrics::gauge!("md_queue_depth", "channel" => "ticker")
                    .set(ticker_rx.len() as f64);
            }

            if ticker_rx.len() >= ticker_cap {
                let _ = ticker_rx.try_recv();
                if core::config::metrics_enabled() {
                    metrics::counter!("md_backpressure_drops_total", "channel" => "ticker")
                        .increment(1);
                }
            }
            if trade_rx.len() >= trade_cap && ticker_rx.try_recv().is_err() {
                let _ = trade_rx.try_recv();
                if core::config::metrics_enabled() {
                    metrics::counter!("md_backpressure_drops_total", "channel" => "trade")
                        .increment(1);
                }
            }
            if book_rx.len() >= book_cap
                && ticker_rx.try_recv().is_err()
                && trade_rx.try_recv().is_err()
            {
                let _ = book_rx.try_recv();
                if core::config::metrics_enabled() {
                    metrics::counter!("md_backpressure_drops_total", "channel" => "book")
                        .increment(1);
                }
            }
        }
    });
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
    adapter::lbank::register();
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
