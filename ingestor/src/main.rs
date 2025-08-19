use anyhow::{Context, Result};
use lru::LruCache;
use once_cell::sync::Lazy;
use reqwest::{Client, Proxy};
use rustls::ClientConfig;
use std::future::Future;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{env, num::NonZeroUsize, sync::Arc};
use tokio::time::{sleep, Duration};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinSet,
};
use tracing::{debug, error};
use tracing_subscriber::EnvFilter;

use agents::ChannelRegistry;
use agents::{spawn_adapters, TaskSet};
use arb_core as core;
use canonical::{MdEvent, MdEventKind};
use core::config;
use core::events::StreamMessage;
use core::tls;
use sink::{FileSink, KafkaSink, Sink, Wal};

mod ops;
mod sink;

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
            .proxy(Proxy::all(format!("socks5h://{proxy}")).context("invalid proxy URL")?);
    }
    client_builder.build().context("building HTTP client")
}

static START: Lazy<Instant> = Lazy::new(Instant::now);

type DedupeKey = (String, u8, String, u64);
static DEDUPE_CACHE: Lazy<Mutex<LruCache<DedupeKey, ()>>> =
    Lazy::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(1024).unwrap())));

fn dedupe_key(ev: &MdEvent) -> Option<DedupeKey> {
    match &ev.event {
        MdEventKind::Trade(e) => e
            .trade_id
            .map(|id| (e.exchange.clone(), ev.channel() as u8, e.symbol.clone(), id)),
        MdEventKind::DepthL2Update(e) => e
            .final_update_id
            .map(|id| (e.exchange.clone(), ev.channel() as u8, e.symbol.clone(), id)),
        MdEventKind::DepthSnapshot(e) => Some((
            e.exchange.clone(),
            ev.channel() as u8,
            e.symbol.clone(),
            e.last_update_id,
        )),
        _ => None,
    }
}

async fn process_stream_event<F, Fut>(
    msg: StreamMessage<'static>,
    metrics_enabled: bool,
    channels: ChannelRegistry,
    mut forward_fn: F,
) where
    F: FnMut(&MdEvent) -> Fut,
    Fut: Future<Output = Result<()>>,
{
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

            let mut attempts = 0;
            let max_retries = 3;
            let mut delay = Duration::from_millis(100);
            let max_delay = Duration::from_secs(1);
            let monotonic = START.elapsed().as_nanos() as u64;
            let utc = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let seq_no = channels.next_seq_no(&msg.stream);
            match &mut ev.event {
                MdEventKind::Trade(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::DepthL2Update(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::BookTicker(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::MiniTicker(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::Kline(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::DepthSnapshot(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::AvgPrice(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::MarkPrice(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::IndexPrice(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::FundingRate(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::OpenInterest(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
                MdEventKind::Liquidation(e) => {
                    e.ingest_ts_monotonic = monotonic;
                    e.ingest_ts_utc = utc;
                    e.seq_no = seq_no;
                }
            }

            loop {
                let start = Instant::now();
                let res = forward_fn(&ev).await;
                if metrics_enabled {
                    metrics::histogram!("sink_latency_us")
                        .record(start.elapsed().as_micros() as f64);
                }
                match res {
                    Ok(()) => break,
                    Err(e) if attempts < max_retries => {
                        attempts += 1;
                        error!(
                            error = %e,
                            attempt = attempts,
                            "failed to forward event, retrying",
                        );
                        sleep(delay).await;
                        delay = std::cmp::min(delay.saturating_mul(2), max_delay);
                    }
                    Err(e) => {
                        error!(error = %e, "failed to forward event, giving up");
                        break;
                    }
                }
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
        let sink = sink.clone();
        let mut set = set.lock().await;
        set.spawn(async move {
            while let Some(msg) = event_rx.recv().await {
                if metrics_enabled {
                    metrics::gauge!("consumer_queue_depth").set(event_rx.len() as f64);
                }
                let sink_cl = sink.clone();
                process_stream_event(msg, metrics_enabled, channels.clone(), move |ev| {
                    let sink_cl = sink_cl.clone();
                    let ev = ev.clone();
                    async move { sink_cl.publish(&ev).await }
                })
                .await;
            }
            if let Err(e) = sink.flush().await {
                error!(error = %e, "failed to flush sink");
            }
        });
    }
}

pub async fn run() -> Result<()> {
    init_tracing();

    let cfg = config::load()?;
    debug!(?cfg, "loaded config");

    let tls_config = tls::build_tls_config(cfg.ca_bundle.as_deref(), &cfg.cert_pins)?;
    let client = build_client(cfg, tls_config.clone())?;

    let metrics_enabled = core::config::metrics_enabled();
    ops::serve_all(metrics_enabled)?;

    let sink: Arc<dyn Sink> = if let Ok(brokers) = env::var("MD_SINK_KAFKA_BROKERS") {
        if brokers.is_empty() {
            let sink_path = env::var("MD_SINK_FILE").context("MD_SINK_FILE is not set")?;
            Arc::new(FileSink::new(sink_path).await?)
        } else {
            let wal_path = env::var("MD_SINK_WAL_FILE").unwrap_or_else(|_| "md.wal".into());
            let kafka = KafkaSink::new(&brokers, "md_events")?;
            Arc::new(Wal::new(wal_path, kafka).await?)
        }
    } else {
        let sink_path = env::var("MD_SINK_FILE").context("MD_SINK_FILE is not set")?;
        Arc::new(FileSink::new(sink_path).await?)
    };

    let join_set: TaskSet = Arc::new(Mutex::new(JoinSet::new()));
    // Install signal-based shutdown handling before starting intake tasks.
    ops::shutdown::install(join_set.clone());

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

    // Spawn a consumer task per partition to normalize events.
    spawn_consumers(
        receivers,
        join_set.clone(),
        metrics_enabled,
        channels.clone(),
        sink.clone(),
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

    sink.flush().await?;

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
        let msg = sample_msg();
        let channels = ChannelRegistry::new(1);
        let times = Arc::new(Mutex::new(Vec::new()));
        let times_clone = times.clone();

        let forward = move |_ev: &MdEvent| {
            let times_clone = times_clone.clone();
            async move {
                times_clone.lock().unwrap().push(Instant::now());
                Err(anyhow!("fail"))
            }
        };

        tokio::spawn(process_stream_event(msg, false, channels, forward))
            .await
            .unwrap();

        let times = times.lock().unwrap();
        assert_eq!(times.len(), 4);
        let deltas: Vec<_> = times.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(deltas[0] >= Duration::from_millis(100) && deltas[0] < Duration::from_millis(150));
        assert!(deltas[1] >= Duration::from_millis(200) && deltas[1] < Duration::from_millis(300));
        assert!(deltas[2] >= Duration::from_millis(400) && deltas[2] < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn stops_after_max_retries() {
        let msg = sample_msg();
        let channels = ChannelRegistry::new(1);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let forward = move |_ev: &MdEvent| {
            let attempts_clone = attempts_clone.clone();
            async move {
                attempts_clone.fetch_add(1, Ordering::SeqCst);
                Err(anyhow!("fail"))
            }
        };

        tokio::spawn(process_stream_event(msg, false, channels, forward))
            .await
            .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_increments_and_stops_after_max_attempts() {
        let msg = sample_msg();
        let channels = ChannelRegistry::new(1);
        let attempts = Arc::new(AtomicUsize::new(0));
        let times: Arc<Mutex<Vec<ttime::Instant>>> = Arc::new(Mutex::new(Vec::new()));
        let attempts_clone = attempts.clone();
        let times_clone = times.clone();

        let fail_times = 5;
        let forward = move |_ev: &MdEvent| {
            let attempts_clone = attempts_clone.clone();
            let times_clone = times_clone.clone();
            async move {
                times_clone.lock().unwrap().push(ttime::Instant::now());
                let attempt = attempts_clone.fetch_add(1, Ordering::SeqCst);
                if attempt < fail_times {
                    Err(anyhow!("fail"))
                } else {
                    Ok(())
                }
            }
        };

        let task = tokio::spawn(process_stream_event(msg, false, channels, forward));

        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        ttime::advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        ttime::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        ttime::advance(Duration::from_millis(199)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        ttime::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        ttime::advance(Duration::from_millis(399)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        ttime::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 4);

        ttime::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 4);

        task.await.unwrap();

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
        let msg = sample_msg();
        let channels = ChannelRegistry::new(1);
        let first = Arc::new(Mutex::new(None));
        let first_cl = first.clone();
        let forward = move |ev: &MdEvent| {
            let first_cl = first_cl.clone();
            let ev_clone = ev.clone();
            async move {
                *first_cl.lock().unwrap() = Some(ev_clone);
                Ok(())
            }
        };
        process_stream_event(msg, false, channels.clone(), forward).await;
        let ev1 = first.lock().unwrap().clone().unwrap();
        match ev1.event {
            MdEventKind::BookTicker(bt) => {
                assert!(bt.ingest_ts_monotonic > 0);
                assert!(bt.ingest_ts_utc > 0);
                assert_eq!(bt.seq_no, 0);
            }
            _ => panic!("expected book ticker"),
        }

        let second = Arc::new(Mutex::new(None));
        let second_cl = second.clone();
        let forward2 = move |ev: &MdEvent| {
            let second_cl = second_cl.clone();
            let ev_clone = ev.clone();
            async move {
                *second_cl.lock().unwrap() = Some(ev_clone);
                Ok(())
            }
        };
        let msg2 = sample_msg();
        process_stream_event(msg2, false, channels.clone(), forward2).await;
        let ev2 = second.lock().unwrap().clone().unwrap();
        match ev2.event {
            MdEventKind::BookTicker(bt) => {
                assert_eq!(bt.seq_no, 1);
            }
            _ => panic!("expected book ticker"),
        }
    }
}
