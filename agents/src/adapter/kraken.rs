use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};
use anyhow::Result;
use arb_core as core;
use async_trait::async_trait;
use futures::future::BoxFuture;
use reqwest::Client;
use rustls::ClientConfig;
use std::sync::{Arc, Once};
use tokio::sync::mpsc;
use tracing::error;

/// Basic configuration for a Kraken exchange endpoint.
pub struct KrakenConfig {
    pub id: &'static str,
    pub name: &'static str,
}

/// All Kraken exchanges supported by this adapter.
pub const KRAKEN_EXCHANGES: &[KrakenConfig] = &[KrakenConfig {
    id: "kraken",
    name: "Kraken",
}];

/// Minimal adapter for Kraken implementing the `ExchangeAdapter` trait.
pub struct KrakenAdapter {
    _cfg: &'static KrakenConfig,
}

impl KrakenAdapter {
    fn new(cfg: &'static KrakenConfig) -> Self {
        Self { _cfg: cfg }
    }
}

#[async_trait]
impl ExchangeAdapter for KrakenAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        Ok(())
    }

    async fn heartbeat(&mut self) -> Result<()> {
        Ok(())
    }

    async fn auth(&mut self) -> Result<()> {
        Ok(())
    }

    async fn backfill(&mut self) -> Result<()> {
        Ok(())
    }
}

static REGISTER: Once = Once::new();

/// Register the Kraken adapter factory.
pub fn register() {
    REGISTER.call_once(|| {
        for exch in KRAKEN_EXCHANGES {
            let cfg_ref: &'static KrakenConfig = exch;
            registry::register_adapter(
                cfg_ref.id,
                Arc::new(
                    move |_global_cfg: &'static core::config::Config,
                          _exchange_cfg: &core::config::ExchangeConfig,
                          _client: Client,
                          task_set: TaskSet,
                          _channels: ChannelRegistry,
                          _tls_config: Arc<ClientConfig>|
                          -> BoxFuture<
                        'static,
                        Result<Vec<mpsc::Receiver<core::events::StreamMessage<'static>>>>,
                    > {
                        Box::pin(async move {
                            let adapter = KrakenAdapter::new(cfg_ref);
                            {
                                let mut set = task_set.lock().await;
                                set.spawn(async move {
                                    let mut adapter = adapter;
                                    if let Err(e) = adapter.run().await {
                                        error!("Failed to run adapter: {}", e);
                                    }
                                });
                            }
                            Ok(Vec::new())
                        })
                    },
                ),
            );
        }
    });
}
