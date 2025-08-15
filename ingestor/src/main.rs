use anyhow::{Context, Result};
use dashmap::DashMap;
use reqwest::{Client, Proxy};
use rustls::ClientConfig;
use std::{env, sync::Arc};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error};
use tracing_subscriber::EnvFilter;

use agents::spawn_adapters;
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

fn spawn_consumers(
    receivers: Vec<mpsc::Receiver<StreamMessage<'static>>>,
    task_tx: mpsc::UnboundedSender<JoinHandle<()>>,
) {
    for mut event_rx in receivers {
        let tx = task_tx.clone();
        let handle = tokio::spawn(async move {
            while let Some(msg) = event_rx.recv().await {
                match MdEvent::try_from(msg.data) {
                    Ok(_ev) => {
                        if core::config::metrics_enabled() {
                            metrics::counter!("md_events_total").increment(1);
                        }
                        #[cfg(feature = "debug-logs")]
                        debug!(?_ev, stream = %msg.stream, "normalized event");
                    }
                    Err(_) => {
                        error!(stream = %msg.stream, "failed to normalize event");
                    }
                }
            }
        });
        let _ = tx.send(handle);
    }
}

async fn await_tasks(mut task_rx: mpsc::UnboundedReceiver<JoinHandle<()>>) {
    while let Some(handle) = task_rx.recv().await {
        if let Err(e) = handle.await {
            error!("task error: {}", e);
        }
    }
}

pub async fn run() -> Result<()> {
    init_tracing();

    let cfg = config::load()?;
    debug!(?cfg, "loaded config");

    let tls_config = tls::build_tls_config(cfg.ca_bundle.as_deref(), &cfg.cert_pins)?;
    let client = build_client(&cfg, tls_config.clone())?;

    let (task_tx, task_rx) = mpsc::unbounded_channel::<JoinHandle<()>>();

    let event_buffer_size = cfg.event_buffer_size;
    let event_txs: Arc<DashMap<String, mpsc::Sender<StreamMessage<'static>>>> =
        Arc::new(DashMap::new());

    // Create and spawn exchange adapters, collecting receivers for each partition.
    let receivers = spawn_adapters(
        cfg,
        client,
        task_tx.clone(),
        event_txs.clone(),
        tls_config.clone(),
        event_buffer_size,
    )
    .await?;

    // Spawn a consumer task per partition to normalize events.
    spawn_consumers(receivers, task_tx.clone());

    // Drop the original senders so receivers can terminate once all adapters finish.
    drop(event_txs);
    drop(task_tx);

    // Await all spawned tasks and log any errors.
    await_tasks(task_rx).await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}
