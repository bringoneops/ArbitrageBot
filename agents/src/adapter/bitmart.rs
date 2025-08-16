use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use futures::{future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Once,
};
use tokio::sync::mpsc;
use tokio::{
    signal,
    task::JoinHandle,
    time::{interval, sleep, Duration, Instant},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};

/// Configuration for a single BitMart exchange endpoint.
pub struct BitmartConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub spot_url: &'static str,
    pub contract_url: &'static str,
    pub ws_base: &'static str,
}

/// All BitMart exchanges supported by this adapter.
pub const BITMART_EXCHANGES: &[BitmartConfig] = &[
    BitmartConfig {
        id: "bitmart_spot",
        name: "BitMart Spot",
        spot_url: "https://api-cloud.bitmart.com/spot/v1/symbols/details",
        contract_url: "https://api-cloud.bitmart.com/contract/v1/ifcontract/contracts",
        ws_base: "wss://ws-manager-compress.bitmart.com?protocol=1.1",
    },
    BitmartConfig {
        id: "bitmart_contract",
        name: "BitMart Contract",
        spot_url: "https://api-cloud.bitmart.com/spot/v1/symbols/details",
        contract_url: "https://api-cloud.bitmart.com/contract/v1/ifcontract/contracts",
        ws_base: "wss://ws-manager-compress.bitmart.com?protocol=1.1",
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

                            let adapter = BitmartAdapter::new(
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

/// Adapter implementing the `ExchangeAdapter` trait for BitMart.
pub struct BitmartAdapter {
    cfg: &'static BitmartConfig,
    _client: Client,
    chunk_size: usize,
    symbols: Vec<String>,
    tasks: Vec<JoinHandle<Result<()>>>,
    shutdown: Arc<AtomicBool>,
}

impl BitmartAdapter {
    pub fn new(
        cfg: &'static BitmartConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
    ) -> Self {
        Self {
            cfg,
            _client: client,
            chunk_size,
            symbols,
            tasks: Vec::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl ExchangeAdapter for BitmartAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        for symbols in self.symbols.chunks(self.chunk_size) {
            let topics: Vec<String> = symbols
                .iter()
                .flat_map(|s| {
                    [
                        format!("spot/trade:{}", s),
                        format!("spot/depth5:{}", s),
                        format!("spot/depth50:{}", s),
                        format!("spot/depth/increase100:{}", s),
                        format!("spot/kline1m:{}", s),
                    ]
                })
                .collect();
            let snapshot_topics: Vec<String> = symbols
                .iter()
                .map(|s| format!("spot/depth50:{}", s))
                .collect();
            let topic_count = topics.len();
            let ws_url = self.cfg.ws_base.to_string();
            let shutdown = self.shutdown.clone();
            let handle = tokio::spawn(async move {
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    match connect_async(&ws_url).await {
                        Ok((mut ws, _)) => {
                            info!(endpoint = %ws_url, topics = topic_count, "bitmart websocket connected");
                            let sub =
                                serde_json::json!({"action":"subscribe","args": topics.clone()});
                            if ws.send(Message::Text(sub.to_string())).await.is_err() {
                                warn!("bitmart subscription failed");
                                break;
                            }
                            let snap = serde_json::json!({"action":"request","args": snapshot_topics.clone()});
                            if ws.send(Message::Text(snap.to_string())).await.is_err() {
                                warn!("bitmart snapshot request failed");
                            }
                            info!(endpoint = %ws_url, topics = topic_count, "bitmart subscribed");
                            let mut last_ping = Instant::now();
                            let mut hb = interval(Duration::from_secs(30));
                            loop {
                                tokio::select! {
                                    msg = ws.next() => {
                                        match msg {
                                            Some(Ok(Message::Ping(p))) => {
                                                last_ping = Instant::now();
                                                if ws.send(Message::Pong(p)).await.is_err() { break; }
                                            }
                                            Some(Ok(Message::Pong(_))) => { last_ping = Instant::now(); }
                                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => { break; }
                                            _ => {}
                                        }
                                    }
                                    _ = hb.tick() => {
                                        if last_ping.elapsed() >= Duration::from_secs(30) {
                                            if ws.send(Message::Ping(Vec::new())).await.is_err() { break; }
                                            last_ping = Instant::now();
                                        }
                                    }
                                }
                            }
                            info!("bitmart websocket disconnected, reconnecting");
                        }
                        Err(e) => warn!("bitmart connect error: {}", e),
                    }
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    sleep(Duration::from_secs(5)).await;
                }
                Ok::<(), anyhow::Error>(())
            });
            self.tasks.push(handle);
        }
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        self.subscribe().await?;
        let _ = signal::ctrl_c().await;
        self.shutdown.store(true, Ordering::SeqCst);
        for handle in self.tasks.drain(..) {
            let _ = handle.await;
        }
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
