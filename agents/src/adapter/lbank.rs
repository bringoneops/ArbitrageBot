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

/// Configuration for a single LBank exchange endpoint.
pub struct LbankConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub ws_base: &'static str,
}

/// All LBank exchanges supported by this adapter.
pub const LBANK_EXCHANGES: &[LbankConfig] = &[LbankConfig {
    id: "lbank_spot",
    name: "LBank",
    ws_base: "wss://api.lbkex.com/ws/v2/",
}];

const SPOT_URL: &str = "https://api.lbkex.com/v2/currencyPairs.do";
const CONTRACT_URL: &str = "https://api.lbkex.com/v2/contract/pairs.do";

/// Retrieve all trading symbols for LBank across spot and contract markets.
pub async fn fetch_symbols() -> Result<Vec<String>> {
    let client = Client::new();

    async fn fetch_from(client: &Client, url: &str) -> Result<Vec<String>> {
        let resp = client.get(url).send().await?.error_for_status()?;
        let data: Value = resp.json().await?;
        let arr = data
            .get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("missing data array"))?;
        let mut res = Vec::new();
        for item in arr {
            if let Some(s) = item.as_str() {
                res.push(s.to_string());
            } else if let Some(s) = item.get("symbol").and_then(|v| v.as_str()) {
                res.push(s.to_string());
            } else if let Some(s) = item.get("pair").and_then(|v| v.as_str()) {
                res.push(s.to_string());
            }
        }
        Ok(res)
    }

    let mut symbols = fetch_from(&client, SPOT_URL).await?;
    if let Ok(mut contracts) = fetch_from(&client, CONTRACT_URL).await {
        symbols.append(&mut contracts);
    }

    symbols.sort();
    symbols.dedup();
    Ok(symbols)
}

static REGISTER: Once = Once::new();

pub fn register() {
    REGISTER.call_once(|| {
        for exch in LBANK_EXCHANGES {
            let cfg_ref: &'static LbankConfig = exch;
            registry::register_adapter(
                cfg_ref.id,
                Arc::new(
                    move |global_cfg: &'static core::config::Config,
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

                            let adapter = LbankAdapter::new(
                                cfg,
                                client.clone(),
                                global_cfg.chunk_size,
                                symbols,
                            );

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

/// Adapter implementing the `ExchangeAdapter` trait for LBank.
pub struct LbankAdapter {
    _cfg: &'static LbankConfig,
    _client: Client,
    _chunk_size: usize,
    _symbols: Vec<String>,
}

impl LbankAdapter {
    pub fn new(
        cfg: &'static LbankConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
    ) -> Self {
        Self {
            _cfg: cfg,
            _client: client,
            _chunk_size: chunk_size,
            _symbols: symbols,
        }
    }
}

#[async_trait]
impl ExchangeAdapter for LbankAdapter {
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
