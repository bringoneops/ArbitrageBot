use anyhow::{Context, Result};
use reqwest::{Client, Proxy};
use rustls::ClientConfig;
use std::{env, sync::Arc};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinSet,
};
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

fn process_stream_event(msg: StreamMessage<'static>, metrics_enabled: bool) {
    match MdEvent::try_from(msg.data) {
        Ok(ev) => {
            if metrics_enabled {
                metrics::counter!("md_events_total").increment(1);
            }
            #[cfg(feature = "debug-logs")]
            debug!(?ev, stream = %msg.stream, "normalized event");

            let mut attempts = 0;
            let max_retries = 3;
            loop {
                match forward_event(&ev) {
                    Ok(()) => break,
                    Err(e) if attempts < max_retries => {
                        attempts += 1;
                        error!(
                            error = %e,
                            attempt = attempts,
                            "failed to forward event, retrying"
                        );
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
                process_stream_event(msg, metrics_enabled);
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
