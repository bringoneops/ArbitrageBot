use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use futures::future::BoxFuture;
use reqwest::Client;
use serde_json::Value;
use std::sync::{Arc, Once};
use tokio::sync::mpsc;
use tracing::error;

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};

/// Configuration for a single BitMart exchange endpoint.
pub struct BitmartConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub spot_url: &'static str,
    pub contract_url: &'static str,
}

/// All BitMart exchanges supported by this adapter.
pub const BITMART_EXCHANGES: &[BitmartConfig] = &[
    BitmartConfig {
        id: "bitmart_spot",
        name: "BitMart Spot",
        spot_url: "https://api-cloud.bitmart.com/spot/v1/symbols/details",
        contract_url: "https://api-cloud.bitmart.com/contract/v1/ifcontract/contracts",
    },
    BitmartConfig {
        id: "bitmart_contract",
        name: "BitMart Contract",
        spot_url: "https://api-cloud.bitmart.com/spot/v1/symbols/details",
        contract_url: "https://api-cloud.bitmart.com/contract/v1/ifcontract/contracts",
    },
];

/// Retrieve all spot trading symbols from BitMart.
pub async fn fetch_spot_symbols(url: &str) -> Result<Vec<String>> {
    let resp = Client::new().get(url).send().await?.error_for_status()?;
    let data: Value = resp.json().await?;
    let arr = data
        .get("data")
        .and_then(|v| v.get("symbols"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing symbols array"))?;

    let mut result: Vec<String> = arr
        .iter()
        .filter_map(|s| {
            let tradable = s
                .get("trade_status")
                .and_then(|v| v.as_str())
                .map(|st| st.eq_ignore_ascii_case("trading"))
                .unwrap_or(false);
            if tradable {
                s.get("symbol")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string())
            } else {
                None
            }
        })
        .collect();
    result.sort();
    result.dedup();
    Ok(result)
}

/// Retrieve all contract trading symbols from BitMart.
pub async fn fetch_contract_symbols(url: &str) -> Result<Vec<String>> {
    let resp = Client::new().get(url).send().await?.error_for_status()?;
    let data: Value = resp.json().await?;
    let arr = data
        .get("data")
        .and_then(|v| v.get("symbols").or_else(|| v.get("contracts")))
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing contracts array"))?;

    let mut result: Vec<String> = arr
        .iter()
        .filter_map(|s| {
            s.get("symbol")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string())
        })
        .collect();
    result.sort();
    result.dedup();
    Ok(result)
}

/// Fetch and merge spot and contract symbols.
pub async fn fetch_symbols(cfg: &BitmartConfig) -> Result<Vec<String>> {
    let mut symbols = fetch_spot_symbols(cfg.spot_url).await?;
    let mut contracts = fetch_contract_symbols(cfg.contract_url).await?;
    symbols.append(&mut contracts);
    symbols.sort();
    symbols.dedup();
    Ok(symbols)
}

static REGISTER: Once = Once::new();

pub fn register() {
    REGISTER.call_once(|| {
        for exch in BITMART_EXCHANGES {
            let cfg_ref: &'static BitmartConfig = exch;
            registry::register_adapter(
                cfg_ref.id,
                Arc::new(
                    move |_global_cfg: &'static core::config::Config,
                          exchange_cfg: &core::config::ExchangeConfig,
                          client: Client,
                          task_set: TaskSet,
                          channels: ChannelRegistry,
                          _tls_config: Arc<rustls::ClientConfig>|
                          -> BoxFuture<
                        'static,
                        Result<Vec<mpsc::Receiver<core::events::StreamMessage<'static>>>>,
                    > {
                        let cfg = cfg_ref;
                        let initial_symbols = exchange_cfg.symbols.clone();
                        Box::pin(async move {
                            let mut symbols = initial_symbols;
                            if symbols.is_empty() {
                                symbols = fetch_symbols(cfg).await?;
                            }

                            let mut receivers = Vec::new();
                            for symbol in &symbols {
                                let key = format!("{}:{}", cfg.name, symbol);
                                let (_, rx) = channels.get_or_create(&key);
                                if let Some(rx) = rx {
                                    receivers.push(rx);
                                }
                            }

                            let adapter = BitmartAdapter::new(cfg, client.clone(), symbols);

                            {
                                let mut set = task_set.lock().await;
                                set.spawn(async move {
                                    let mut adapter = adapter;
                                    if let Err(e) = adapter.run().await {
                                        error!("Failed to run adapter: {}", e);
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

/// Adapter implementing the `ExchangeAdapter` trait for BitMart.
pub struct BitmartAdapter {
    cfg: &'static BitmartConfig,
    _client: Client,
    symbols: Vec<String>,
}

impl BitmartAdapter {
    pub fn new(cfg: &'static BitmartConfig, client: Client, symbols: Vec<String>) -> Self {
        Self {
            cfg,
            _client: client,
            symbols,
        }
    }
}

#[async_trait]
impl ExchangeAdapter for BitmartAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        // Subscription logic will be implemented later.
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        self.auth().await?;
        self.backfill().await?;
        self.subscribe().await?;
        self.heartbeat().await?;
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
