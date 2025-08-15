use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use futures::future::BoxFuture;
use reqwest::Client;
use serde_json::Value;
use std::sync::{Arc, Once};
use tokio::sync::mpsc;

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};
use rustls::ClientConfig;

/// Configuration for a single KuCoin exchange endpoint.
pub struct KucoinConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub ws_base: &'static str,
}

/// All KuCoin exchanges supported by this adapter.
pub const KUCOIN_EXCHANGES: &[KucoinConfig] = &[
    KucoinConfig {
        id: "kucoin_spot",
        name: "KuCoin Spot",
        ws_base: "wss://ws-api.kucoin.com/endpoint",
    },
    KucoinConfig {
        id: "kucoin_futures",
        name: "KuCoin Futures",
        ws_base: "wss://ws-api-futures.kucoin.com/endpoint",
    },
];

/// Retrieve all trading symbols for KuCoin across spot and futures markets.
pub async fn fetch_symbols() -> Result<Vec<String>> {
    let client = Client::new();
    let mut symbols: Vec<String> = Vec::new();

    let spot: Value = client
        .get("https://api.kucoin.com/api/v2/symbols")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let spot_arr = spot
        .get("data")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing data array"))?;
    for item in spot_arr {
        let active = item
            .get("enableTrading")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if active {
            if let Some(sym) = item.get("symbol").and_then(|v| v.as_str()) {
                symbols.push(sym.to_string());
            }
        }
    }

    let futures: Value = client
        .get("https://api-futures.kucoin.com/api/v1/contracts/active")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let fut_arr = futures
        .get("data")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing data array"))?;
    for item in fut_arr {
        let active = item
            .get("status")
            .and_then(|v| v.as_str())
            .map(|s| s.eq_ignore_ascii_case("open"))
            .unwrap_or(false);
        if active {
            if let Some(sym) = item.get("symbol").and_then(|v| v.as_str()) {
                symbols.push(sym.to_string());
            }
        }
    }

    symbols.sort();
    symbols.dedup();
    Ok(symbols)
}

/// Adapter implementing the `ExchangeAdapter` trait for KuCoin.
pub struct KucoinAdapter {
    cfg: &'static KucoinConfig,
    _client: Client,
    symbols: Vec<String>,
}

impl KucoinAdapter {
    pub fn new(cfg: &'static KucoinConfig, client: Client, symbols: Vec<String>) -> Self {
        Self {
            cfg,
            _client: client,
            symbols,
        }
    }
}

#[async_trait]
impl ExchangeAdapter for KucoinAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        self.subscribe().await
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

/// Register KuCoin adapters for all market types.
pub fn register() {
    REGISTER.call_once(|| {
        for exch in KUCOIN_EXCHANGES {
            let cfg_ref: &'static KucoinConfig = exch;
            registry::register_adapter(
                cfg_ref.id,
                Arc::new(
                    move |_global_cfg: &'static core::config::Config,
                          exchange_cfg: &core::config::ExchangeConfig,
                          client: Client,
                          task_set: TaskSet,
                          channels: ChannelRegistry,
                          _tls_config: Arc<ClientConfig>|
                          -> BoxFuture<
                        'static,
                        Result<Vec<mpsc::Receiver<core::events::StreamMessage<'static>>>>,
                    > {
                        let cfg = cfg_ref;
                        let initial_symbols = exchange_cfg.symbols.clone();
                        Box::pin(async move {
                            let mut symbols = initial_symbols;
                            if symbols.is_empty() {
                                symbols = fetch_symbols().await?;
                            }

                            let mut receivers = Vec::new();
                            for symbol in &symbols {
                                let key = format!("{}:{}", cfg.name, symbol);
                                let (_, rx) = channels.get_or_create(&key);
                                if let Some(rx) = rx {
                                    receivers.push(rx);
                                }
                            }

                            let adapter = KucoinAdapter::new(cfg, client.clone(), symbols);

                            {
                                let mut set = task_set.lock().await;
                                set.spawn(async move {
                                    let mut adapter = adapter;
                                    if let Err(e) = adapter.run().await {
                                        tracing::error!("Failed to run adapter: {}", e);
                                    }
                                });
                            }

                            Ok(receivers)
                        })
                    },
                ),
            );
        }
    });
}
