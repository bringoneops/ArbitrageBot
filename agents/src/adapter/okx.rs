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

/// Basic configuration for an OKX exchange endpoint.
pub struct OkxConfig {
    pub id: &'static str,
    pub name: &'static str,
}

/// All OKX exchanges supported by this adapter.
pub const OKX_EXCHANGES: &[OkxConfig] = &[OkxConfig {
    id: "okx",
    name: "OKX",
}];

/// Minimal adapter for OKX implementing the `ExchangeAdapter` trait.
pub struct OkxAdapter {
    _cfg: &'static OkxConfig,
}

impl OkxAdapter {
    fn new(cfg: &'static OkxConfig) -> Self {
        Self { _cfg: cfg }
    }
}

#[async_trait]
impl ExchangeAdapter for OkxAdapter {
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

/// Register the OKX adapter factory.
pub fn register() {
    REGISTER.call_once(|| {
        for exch in OKX_EXCHANGES {
            let cfg_ref: &'static OkxConfig = exch;
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
                            let adapter = OkxAdapter::new(cfg_ref);
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
