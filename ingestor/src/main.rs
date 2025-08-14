use anyhow::{Context, Result};
use reqwest::{Client, Proxy};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use agents::spawn_adapters;
use arb_core as core;
use canonical::MdEvent;
use core::config;
use core::events::StreamMessage;
use core::tls;

pub async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = config::load()?;
    info!(?cfg, "loaded config");

    let tls_config = tls::build_tls_config(cfg.ca_bundle.as_deref(), &cfg.cert_pins)?;
    let mut client_builder = Client::builder()
        .user_agent("binance-us-all-streams")
        .use_preconfigured_tls(tls_config.clone());
    if let Some(proxy) = &cfg.proxy_url {
        if !proxy.is_empty() {
            client_builder = client_builder
                .proxy(Proxy::all(format!("socks5h://{}", proxy)).context("invalid proxy URL")?);
        }
    }
    let client = client_builder.build().context("building HTTP client")?;

    let tasks = Arc::new(Mutex::new(JoinSet::new()));

    let event_buffer_size = cfg.event_buffer_size;
    let (event_tx, mut event_rx) = mpsc::channel::<StreamMessage<'static>>(event_buffer_size);

    // Spawn a task to normalize and forward events from all adapters.
    {
        let mut tasks_guard = tasks.lock().await;
        tasks_guard.spawn(async move {
            while let Some(msg) = event_rx.recv().await {
                match MdEvent::try_from(msg.data) {
                    Ok(ev) => {
                        // Forward normalized events to downstream handlers.
                        info!(?ev, stream = %msg.stream, "normalized event");
                    }
                    Err(_) => {
                        error!(stream = %msg.stream, "failed to normalize event");
                    }
                }
            }
        });
    }

    // Create and spawn exchange adapters.
    spawn_adapters(
        cfg,
        client,
        tasks.clone(),
        event_tx.clone(),
        tls_config.clone(),
    )
    .await?;

    // Drop the original sender so the receiver can terminate once all adapters finish.
    drop(event_tx);

    // Await all spawned tasks and log any errors.
    let mut tasks = tasks.lock().await;
    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            error!("task error: {}", e);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}
