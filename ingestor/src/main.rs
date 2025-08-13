use anyhow::{Context, Result};
use futures::future;
use reqwest::{Client, Proxy};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use agents::adapter::binance::{BinanceAdapter, BINANCE_EXCHANGES};
use agents::adapter::ExchangeAdapter;
use agents::spawn_adapters;
use arb_core as core;
use canonical::MdEvent;
use core::config;
use core::events::{Event, StreamMessage};
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

    let chunk_size = cfg.chunk_size;

    let tasks = Arc::new(Mutex::new(JoinSet::new()));

    let event_buffer_size = cfg.event_buffer_size;
    let (event_tx, mut event_rx) = mpsc::channel::<StreamMessage<'static>>(event_buffer_size);

    {
        let mut tasks_guard = tasks.lock().await;
        tasks_guard.spawn(async move {
            while let Some(event) = event_rx.recv().await {
                match event.data {
                    Event::Trade(e) => {
                        let _ = MdEvent::from(e);
                    }
                    Event::DepthUpdate(e) => {
                        let _ = MdEvent::from(e);
                    }
                    _ => {}
                }
            }
        });
    }

    spawn_adapters().await?;

    let mut adapters: Vec<Box<dyn ExchangeAdapter + Send>> = Vec::new();
    for exch in BINANCE_EXCHANGES {
        let is_spot = exch.name.contains("Spot");
        let is_derivative = exch.name.contains("Futures")
            || exch.name.contains("Delivery")
            || exch.name.contains("Options");
        if (is_spot && !cfg.enable_spot) || (is_derivative && !cfg.enable_futures) {
            continue;
        }
        let symbols = if is_spot {
            cfg.spot_symbols.clone()
        } else {
            cfg.futures_symbols.clone()
        };
        adapters.push(Box::new(BinanceAdapter::new(
            exch,
            client.clone(),
            chunk_size,
            cfg.proxy_url.clone().unwrap_or_default(),
            tasks.clone(),
            event_tx.clone(),
            symbols,
            tls_config.clone(),
        )));
    }

    let init_futures: Vec<_> = adapters
        .into_iter()
        .map(|adapter| async move {
            let mut adapter = adapter;
            adapter.run().await
        })
        .collect();

    for result in future::join_all(init_futures).await {
        if let Err(e) = result {
            error!("Failed to initialize adapter: {}", e);
        }
    }

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
