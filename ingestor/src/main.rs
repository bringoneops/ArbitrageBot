use anyhow::{Context, Result};
use reqwest::{Client, Proxy};
use rustls::ClientConfig;
use std::{env, num::NonZeroUsize, sync::Arc};
use tokio::time::Duration;
use once_cell::sync::Lazy;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use lru::LruCache;
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinSet,
};
use tracing::{debug, error};
use tracing_subscriber::EnvFilter;

use agents::{spawn_adapters, TaskSet};
use arb_core as core;
use canonical::MdEvent;
use core::config;
use core::events::StreamMessage;
use core::tls;
use agents::ChannelRegistry;
mod sink;
use sink::Sink;

mod ops;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}

fn build_client(cfg: &config::Config, tls_config: Arc<ClientConfig>) -> Result<Client> {
    let user_agent = env::var("USER_AGENT")
        .unwrap_or_else(|_| format!("ArbitrageBot/{}", env!("CARGO_PKG_VERSION")));
    let mut client_builder = Client::builder()
        .timeout(Duration::from_secs(cfg.http_timeout_secs))
        .user_agent(user_agent)
        .use_preconfigured_tls(tls_config);
    if let Some(proxy) = cfg.proxy_url.as_ref().filter(|p| !p.is_empty()) {
        client_builder = client_builder
            .proxy(Proxy::all(format!("socks5h://{}", proxy)).context("invalid proxy URL")?);
    }
    client_builder.build().context("building HTTP client")
}

static START: Lazy<Instant> = Lazy::new(Instant::now);

type DedupeKey = (String, u8, String, u64);
static DEDUPE_CACHE: Lazy<Mutex<LruCache<DedupeKey, ()>>> = Lazy::new(|| {
    Mutex::new(LruCache::new(NonZeroUsize::new(1024).unwrap()))
});

fn dedupe_key(ev: &MdEvent) -> Option<DedupeKey> {
    match ev {
        MdEvent::Trade(e) => e
            .trade_id
            .map(|id| (e.exchange.clone(), ev.channel() as u8, e.symbol.clone(), id)),
        MdEvent::DepthL2Update(e) => e.final_update_id.map(|id| {
            (e.exchange.clone(), ev.channel() as u8, e.symbol.clone(), id)
        }),
        MdEvent::DepthSnapshot(e) => Some((
            e.exchange.clone(),
            ev.channel() as u8,
            e.symbol.clone(),
            e.last_update_id,
        )),
        _ => None,
    }
}

async fn process_stream_event(
    msg: StreamMessage<'static>,
    metrics_enabled: bool,
    channels: ChannelRegistry,
    sink: Arc<dyn Sink>,
) {
    match MdEvent::try_from(msg.data) {
        Ok(mut ev) => {
            if let Some(key) = dedupe_key(&ev) {
                let mut cache = DEDUPE_CACHE.lock().await;
                if cache.put(key, ()).is_some() {
                    if metrics_enabled {
                        metrics::counter!("md_dedupe_total").increment(1);
                    }
                    #[cfg(feature = "debug-logs")]
                    debug!(stream = %msg.stream, "duplicate event dropped");
                    return;
                }
            }

            if metrics_enabled {
                metrics::counter!("md_events_total").increment(1);
            }
            #[cfg(feature = "debug-logs")]
            debug!(?ev, stream = %msg.stream, "normalized event");

            let monotonic = START.elapsed().as_nanos() as u64;
            let utc = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let seq_no = channels.next_seq_no(&msg.stream);
            match &mut ev {
                MdEvent::Trade(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::DepthL2Update(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::BookTicker(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::MiniTicker(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::Kline(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::DepthSnapshot(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::AvgPrice(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::MarkPrice(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::IndexPrice(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::FundingRate(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::OpenInterest(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEvent::Liquidation(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
            }

            if let Err(e) = sink.publish_batch(&[ev]).await {
                error!(error = %e, "failed to publish event batch");
            }
        }
        Err(e) => {
            error!(error = %format!("{:?}", e), stream = %msg.stream, "failed to normalize event");
        }
    }
}

async fn spawn_consumers(
    receivers: Vec<mpsc::Receiver<StreamMessage<'static>>>,
    join_set: TaskSet,
    metrics_enabled: bool,
    channels: ChannelRegistry,
    sink: Arc<dyn Sink>,
) {
    for mut event_rx in receivers {
        let set = join_set.clone();
        let channels = channels.clone();
        let sink_cl = sink.clone();
        let mut set = set.lock().await;
        set.spawn(async move {
            while let Some(msg) = event_rx.recv().await {
                process_stream_event(msg, metrics_enabled, channels.clone(), sink_cl.clone()).await;
            }
        });
    }
}

pub async fn run() -> Result<()> {
    init_tracing();

    let cfg = config::load()?;
    debug!(?cfg, "loaded config");

    let tls_config = tls::build_tls_config(cfg.ca_bundle.as_deref(), &cfg.cert_pins)?;
    let client = build_client(&cfg, tls_config.clone())?;

    let metrics_enabled = core::config::metrics_enabled();
    ops::serve_all(metrics_enabled)?;

    let join_set: TaskSet = Arc::new(Mutex::new(JoinSet::new()));

    let event_buffer_size = cfg.event_buffer_size;
    let channels = agents::ChannelRegistry::new(event_buffer_size);

    // Create and spawn exchange adapters, collecting receivers for each partition.
    let receivers = spawn_adapters(
        cfg,
        client,
        join_set.clone(),
        channels.clone(),
        tls_config.clone(),
    )
    .await?;

    // Initialize sink from environment.
    let sink = sink::init_from_env().await?;

    // Spawn a consumer task per partition to normalize events.
    spawn_consumers(
        receivers,
        join_set.clone(),
        metrics_enabled,
        channels.clone(),
        sink,
    )
    .await;

    // Drop the original senders so receivers can terminate once all adapters finish.
    drop(channels);

    // Await all spawned tasks and log any errors.
    {
        let mut join_set = join_set.lock().await;
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                error!("task error: {}", e);
            }
        }
    }

    ops::set_ready(false);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::time as ttime;

    fn sample_msg() -> StreamMessage<'static> {
        let json = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"BTCUSDT","b":"0.1","B":"2","a":"0.2","A":"3"}}"#;
        serde_json::from_str(json).unwrap()
    }

    #[tokio::test]
    async fn backoff_doubles_delay() {
        struct FailSink {
            times: Arc<Mutex<Vec<Instant>>>,
        }

        #[async_trait]
        impl Sink for FailSink {
            async fn publish_batch(&self, _events: &[MdEvent]) -> Result<()> {
                self.times.lock().unwrap().push(Instant::now());
                Err(anyhow!("fail"))
            }
        }

        let times = Arc::new(Mutex::new(Vec::new()));
        let sink = sink::AtLeastOnceSink::new(
            FailSink {
                times: times.clone(),
            },
            None,
            None,
            3,
        );

        let _ = sink.publish_batch(&[]).await;

        let times = times.lock().unwrap();
        assert_eq!(times.len(), 4);
        let deltas: Vec<_> = times.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(deltas[0] >= Duration::from_millis(100) && deltas[0] < Duration::from_millis(150));
        assert!(deltas[1] >= Duration::from_millis(200) && deltas[1] < Duration::from_millis(300));
        assert!(deltas[2] >= Duration::from_millis(400) && deltas[2] < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn stops_after_max_retries() {
        struct FailSink {
            attempts: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl Sink for FailSink {
            async fn publish_batch(&self, _events: &[MdEvent]) -> Result<()> {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                Err(anyhow!("fail"))
            }
        }

        let attempts = Arc::new(AtomicUsize::new(0));
        let sink = sink::AtLeastOnceSink::new(
            FailSink {
                attempts: attempts.clone(),
            },
            None,
            None,
            3,
        );

        let _ = sink.publish_batch(&[]).await;

        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_increments_and_stops_after_max_attempts() {
        struct FailThenSucceed {
            attempts: Arc<AtomicUsize>,
            times: Arc<Mutex<Vec<ttime::Instant>>>,
            fail_times: usize,
        }

        #[async_trait]
        impl Sink for FailThenSucceed {
            async fn publish_batch(&self, _events: &[MdEvent]) -> Result<()> {
                self.times.lock().unwrap().push(ttime::Instant::now());
                let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
                if attempt < self.fail_times {
                    Err(anyhow!("fail"))
                } else {
                    Ok(())
                }
            }
        }

        let attempts = Arc::new(AtomicUsize::new(0));
        let times: Arc<Mutex<Vec<ttime::Instant>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = sink::AtLeastOnceSink::new(
            FailThenSucceed {
                attempts: attempts.clone(),
                times: times.clone(),
                fail_times: 5,
            },
            None,
            None,
            3,
        );

        let _ = sink.publish_batch(&[]).await;

        assert_eq!(attempts.load(Ordering::SeqCst), 4);
        let times = times.lock().unwrap();
        assert_eq!(times.len(), 4);
        let deltas: Vec<_> = times.windows(2).map(|w| w[1] - w[0]).collect();
        assert_eq!(
            deltas,
            vec![
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400)
            ]
        );
    }

    #[tokio::test]
    async fn stamps_ingest_fields_and_seq() {
        struct CaptureSink(Arc<Mutex<Vec<MdEvent>>>);

        #[async_trait]
        impl Sink for CaptureSink {
            async fn publish_batch(&self, events: &[MdEvent]) -> Result<()> {
                self.0.lock().unwrap().extend_from_slice(events);
                Ok(())
            }
        }

        let msg = sample_msg();
        let msg2 = sample_msg();
        let channels = ChannelRegistry::new(1);
        let store = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(CaptureSink(store.clone()));

        process_stream_event(msg, false, channels.clone(), sink.clone()).await;
        process_stream_event(msg2, false, channels.clone(), sink).await;

        let events = store.lock().unwrap();
        match &events[0] {
            MdEvent::BookTicker(bt) => {
                assert!(bt.ingest_ts_monotonic > 0);
                assert!(bt.ingest_ts_utc > 0);
                assert_eq!(bt.seq_no, 0);
            }
            _ => panic!("expected book ticker"),
        }
        match &events[1] {
            MdEvent::BookTicker(bt) => {
                assert_eq!(bt.seq_no, 1);
            }
            _ => panic!("expected book ticker"),
        }
    }
}
