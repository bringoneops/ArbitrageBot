use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use core::stream_config_for_exchange;
use futures::future::BoxFuture;
use reqwest::Client;
use serde_json::Value;
use std::sync::{Arc, Once};
use tokio::sync::mpsc;
use tracing::error;

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};

/// Configuration for a single Gate.io exchange endpoint.
pub struct GateioConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
}

/// All Gate.io exchanges supported by this adapter.
pub const GATEIO_EXCHANGES: &[GateioConfig] = &[
    GateioConfig {
        id: "gateio_spot",
        name: "Gate.io Spot",
        info_url: "https://api.gateio.ws/api/v4/spot/currency_pairs",
        ws_base: "wss://api.gateio.ws/ws/v4/",
    },
    GateioConfig {
        id: "gateio_futures",
        name: "Gate.io Futures",
        info_url: "https://api.gateio.ws/api/v4/futures/{settle}/contracts",
        ws_base: "wss://fx-ws.gateio.ws/v4/ws/{settle}",
    },
];

/// Retrieve all trading symbols for Gate.io across market types.
pub async fn fetch_symbols(cfg: &GateioConfig) -> Result<Vec<String>> {
    let client = Client::new();
    let mut result: Vec<String> = Vec::new();
    let limit = 100u32;

    if cfg.info_url.contains("{settle}") {
        // Futures/perpetual contracts, iterate through settle currencies
        let settles = ["usdt", "btc", "usd"];
        for settle in settles.iter() {
            let mut page = 1u32;
            loop {
                let url = cfg.info_url.replace("{settle}", settle);
                let resp = client
                    .get(&url)
                    .query(&[("limit", limit), ("page", page)])
                    .send()
                    .await?
                    .error_for_status()?;
                let data: Value = resp.json().await?;
                let arr = data
                    .as_array()
                    .ok_or_else(|| anyhow!("expected array"))?;
                if arr.is_empty() {
                    break;
                }
                for item in arr {
                    let trading = item
                        .get("status")
                        .and_then(|v| v.as_str())
                        .map(|s| s.eq_ignore_ascii_case("trading"))
                        .unwrap_or(false)
                        && !item
                            .get("in_delisting")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                    if trading {
                        if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                            result.push(name.to_string());
                        }
                    }
                }
                if arr.len() < limit as usize {
                    break;
                }
                page += 1;
            }
        }
    } else {
        // Spot symbols
        let mut page = 1u32;
        loop {
            let resp = client
                .get(cfg.info_url)
                .query(&[("limit", limit), ("page", page)])
                .send()
                .await?
                .error_for_status()?;
            let data: Value = resp.json().await?;
            let arr = data
                .as_array()
                .ok_or_else(|| anyhow!("expected array"))?;
            if arr.is_empty() {
                break;
            }
            for item in arr {
                let tradable = item
                    .get("trade_status")
                    .and_then(|v| v.as_str())
                    .map(|s| s.eq_ignore_ascii_case("tradable"))
                    .unwrap_or(false);
                if tradable {
                    if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                        result.push(id.to_string());
                    }
                }
            }
            if arr.len() < limit as usize {
                break;
            }
            page += 1;
        }
    }

    result.sort();
    result.dedup();
    Ok(result)
}

static REGISTER: Once = Once::new();

pub fn register() {
    REGISTER.call_once(|| {
        for exch in GATEIO_EXCHANGES {
            let cfg_ref: &'static GateioConfig = exch;
            registry::register_adapter(
                cfg_ref.id,
                Arc::new(
                    move |
                          global_cfg: &'static core::config::Config,
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

                            let adapter = GateioAdapter::new(
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

/// Minimal Gate.io adapter implementing [`ExchangeAdapter`].
pub struct GateioAdapter {
    cfg: &'static GateioConfig,
    _client: Client,
    _chunk_size: usize,
    symbols: Vec<String>,
}

impl GateioAdapter {
    pub fn new(
        cfg: &'static GateioConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
    ) -> Self {
        Self {
            cfg,
            _client: client,
            _chunk_size: chunk_size,
            symbols,
        }
    }
}

#[async_trait]
impl ExchangeAdapter for GateioAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        // Real implementation would subscribe to depth, trades, ticker, etc.
        let _ = stream_config_for_exchange(self.cfg.name);
        let _ = &self.symbols;
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        self.backfill().await?;
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

