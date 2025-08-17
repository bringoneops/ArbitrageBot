use anyhow::{Context, Result};
use reqwest::{Client, Proxy};
use rustls::ClientConfig;
use std::{env, sync::Arc};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinSet,
};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

use agents::{spawn_adapters, TaskSet};
use arb_core as core;
use canonical::MdEvent;
use core::config;
use core::events::StreamMessage;
use core::tls;

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
        .user_agent(user_agent)
        .use_preconfigured_tls(tls_config);
    if let Some(proxy) = cfg.proxy_url.as_ref().filter(|p| !p.is_empty()) {
        client_builder = client_builder
            .proxy(Proxy::all(format!("socks5h://{}", proxy)).context("invalid proxy URL")?);
    }
    client_builder.build().context("building HTTP client")
}

fn forward_event(ev: &MdEvent) -> Result<()> {
    let json = serde_json::to_string(ev).context("serializing MdEvent")?;
    info!(event = %json, "forwarded md event");
    Ok(())
}

async fn process_stream_event<F>(
    msg: StreamMessage<'static>,
    metrics_enabled: bool,
    mut forward_fn: F,
) where
    F: FnMut(&MdEvent) -> Result<()>,
{
    match MdEvent::try_from(msg.data) {
        Ok(ev) => {
            if metrics_enabled {
                metrics::counter!("md_events_total").increment(1);
            }
            #[cfg(feature = "debug-logs")]
            debug!(?ev, stream = %msg.stream, "normalized event");

            let mut attempts = 0;
            let max_retries = 3;
            let mut delay = Duration::from_millis(100);
            let max_delay = Duration::from_secs(1);
            loop {
                match forward_fn(&ev) {
                    Ok(()) => break,
                    Err(e) if attempts < max_retries => {
                        attempts += 1;
                        error!(
                            error = %e,
                            attempt = attempts,
                            "failed to forward event, retrying",
                        );
                        sleep(delay).await;
                        delay = std::cmp::min(delay * 2, max_delay);
                    }
                    Err(e) => {
                        error!(error = %e, "failed to forward event, giving up");
                        break;
                    }
                }
            }
        }
        Err(_) => {
            error!(stream = %msg.stream, "failed to normalize event");
        }
    }
}

async fn spawn_consumers(
    receivers: Vec<mpsc::Receiver<StreamMessage<'static>>>,
    join_set: TaskSet,
    metrics_enabled: bool,
) {
    for mut event_rx in receivers {
        let set = join_set.clone();
        let mut set = set.lock().await;
        set.spawn(async move {
            while let Some(msg) = event_rx.recv().await {
                process_stream_event(msg, metrics_enabled, forward_event).await;
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

    // Spawn a consumer task per partition to normalize events.
    let metrics_enabled = core::config::metrics_enabled();
    spawn_consumers(receivers, join_set.clone(), metrics_enabled).await;

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

    fn sample_msg() -> StreamMessage<'static> {
        let json = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"BTCUSDT","b":"0.1","B":"2","a":"0.2","A":"3"}}"#;
        serde_json::from_str(json).unwrap()
    }

    #[tokio::test]
    async fn backoff_doubles_delay() {
        let msg = sample_msg();
        let times = Arc::new(Mutex::new(Vec::new()));
        let times_clone = times.clone();

        let forward = move |_ev: &MdEvent| -> Result<()> {
            times_clone.lock().unwrap().push(Instant::now());
            Err(anyhow!("fail"))
        };

        tokio::spawn(process_stream_event(msg, false, forward))
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
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let forward = move |_ev: &MdEvent| -> Result<()> {
            attempts_clone.fetch_add(1, Ordering::SeqCst);
            Err(anyhow!("fail"))
        };

        tokio::spawn(process_stream_event(msg, false, forward))
            .await
            .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }
}

