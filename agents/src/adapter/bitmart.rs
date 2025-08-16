use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::{future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::{
    borrow::Cow,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Once,
    },
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

fn parse_depth_side(side: Option<&Value>) -> Vec<[String; 2]> {
    let mut out = Vec::new();
    if let Some(arr) = side.and_then(|v| v.as_array()) {
        for level in arr {
            if let Some(vals) = level.as_array() {
                if vals.len() >= 2 {
                    let price = vals[0].as_str().unwrap_or_default().to_string();
                    let qty = vals[1].as_str().unwrap_or_default().to_string();
                    out.push([price, qty]);
                }
            } else if let Some(obj) = level.as_object() {
                let price = obj
                    .get("price")
                    .or_else(|| obj.get("p"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let qty = obj
                    .get("amount")
                    .or_else(|| obj.get("q"))
                    .or_else(|| obj.get("volume"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                out.push([price, qty]);
            }
        }
    }
    out
}

async fn fetch_depth_snapshot(
    client: &Client,
    symbol: &str,
    is_contract: bool,
) -> Result<core::OrderBook> {
    let url = if is_contract {
        format!(
            "https://api-cloud.bitmart.com/contract/public/depth?symbol={}",
            symbol
        )
    } else {
        format!(
            "https://api-cloud.bitmart.com/spot/v1/symbols/book?symbol={}",
            symbol
        )
    };

    let resp = client.get(&url).send().await?.error_for_status()?;
    let val: Value = resp.json().await?;
    let data = val.get("data").ok_or_else(|| anyhow!("missing data"))?;

    let bids = parse_depth_side(data.get("buys").or_else(|| data.get("bids")));
    let asks = parse_depth_side(data.get("sells").or_else(|| data.get("asks")));

    let last_update_id = data
        .get("timestamp")
        .or_else(|| data.get("ms_t"))
        .or_else(|| data.get("seq_id"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    Ok(core::DepthSnapshot {
        last_update_id,
        bids,
        asks,
    }
    .into())
}

fn parse_depth_update_frame(
    val: &Value,
) -> Result<core::events::DepthUpdateEvent<'static>> {
    let symbol = val
        .get("symbol")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing symbol"))?
        .to_string();

    let bids = parse_depth_side(val.get("bids").or_else(|| val.get("buys")))
        .into_iter()
        .map(|[p, q]| [Cow::Owned(p), Cow::Owned(q)])
        .collect();
    let asks = parse_depth_side(val.get("asks").or_else(|| val.get("sells")))
        .into_iter()
        .map(|[p, q]| [Cow::Owned(p), Cow::Owned(q)])
        .collect();

    let final_update_id = val
        .get("seq_id")
        .or_else(|| val.get("sequence"))
        .or_else(|| val.get("version"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let prev_final_update_id = val
        .get("prev_seq_id")
        .or_else(|| val.get("prev_sequence"))
        .or_else(|| val.get("last_version"))
        .and_then(|v| v.as_u64())
        .unwrap_or(final_update_id);
    let first_update_id = prev_final_update_id + 1;
    let event_time = val
        .get("timestamp")
        .or_else(|| val.get("ms_t"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    Ok(core::events::DepthUpdateEvent {
        event_time,
        symbol,
        first_update_id,
        final_update_id,
        previous_final_update_id: prev_final_update_id,
        bids,
        asks,
    })
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
    client: Client,
    chunk_size: usize,
    symbols: Vec<String>,
    tasks: Vec<JoinHandle<Result<()>>>,
    shutdown: Arc<AtomicBool>,
    orderbooks: Arc<DashMap<String, core::OrderBook>>,
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
            client,
            chunk_size,
            symbols,
            tasks: Vec::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            orderbooks: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl ExchangeAdapter for BitmartAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        for symbols in self.symbols.chunks(self.chunk_size) {
            let prefix = if self.cfg.id == "bitmart_spot" {
                "spot"
            } else {
                "futures"
            };
            let topics: Vec<String> = symbols
            let chunk_symbols: Vec<String> = symbols.to_vec();
            let topics: Vec<String> = chunk_symbols
                .iter()
                .flat_map(|s| {
                    [
                        format!("spot/trade:{}", s),
                        format!("spot/depth5:{}", s),
                        format!("spot/depth50:{}", s),
                        format!("spot/depth/increase100:{}", s),
                        format!("spot/kline1m:{}", s),
                    ]
                    let mut t = vec![
                        format!("{}/trade:{}", prefix, s),
                        format!("{}/ticker:{}", prefix, s),
                    ];
                    if prefix == "spot" {
                        t.push(format!("{}/kline1m:{}", prefix, s));
                        t.push(format!("{}/depth5:{}", prefix, s));
                        t.push(format!("{}/depth20:{}", prefix, s));
                    } else {
                        t.push(format!("{}/klineBin1m:{}", prefix, s));
                        t.push(format!("{}/depth5:{}", prefix, s));
                        t.push(format!("{}/depth20:{}", prefix, s));
                        t.push(format!("{}/depthIncrease5:{}", prefix, s));
                        t.push(format!("{}/depthIncrease20:{}", prefix, s));
                        t.push(format!("{}/fundingRate:{}", prefix, s));
                    }
                    t
                })
                .collect();
            let snapshot_topics: Vec<String> = symbols
                .iter()
                .map(|s| format!("spot/depth50:{}", s))
                .collect();
            let topic_count = topics.len();
            let ws_url = self.cfg.ws_base.to_string();
            let shutdown = self.shutdown.clone();
            let client = self.client.clone();
            let books = self.orderbooks.clone();
            let cfg = self.cfg;
            let has_depth_increase = topics
                .iter()
                .any(|t| t.starts_with("spot/depth/increase") || t.starts_with("futures/depthIncrease"));
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

                            if has_depth_increase {
                                let is_contract = cfg.id.contains("contract");
                                for sym in &chunk_symbols {
                                    match fetch_depth_snapshot(&client, sym, is_contract).await {
                                        Ok(book) => { books.insert(sym.clone(), book); }
                                        Err(e) => warn!(symbol = %sym, "snapshot fetch failed: {}", e),
                                    }
                                }
                            }

                            let mut last_ping = Instant::now();
                            let mut hb = interval(Duration::from_secs(30));
                            loop {
                                tokio::select! {
                                    msg = ws.next() => {
                                        match msg {
                                            Some(Ok(Message::Text(text))) => {
                                                if has_depth_increase {
                                                    if let Ok(val) = serde_json::from_str::<Value>(&text) {
                                                        if let Some(table) = val.get("table").and_then(|v| v.as_str()) {
                                                            if table.starts_with("spot/depth/increase") || table.starts_with("futures/depthIncrease") {
                                                                if let Some(arr) = val.get("data").and_then(|v| v.as_array()) {
                                                                    for entry in arr {
                                                                        if let Ok(update) = parse_depth_update_frame(entry) {
                                                                            let sym = update.symbol.clone();
                                                                            if let Some(mut book) = books.get_mut(&sym) {
                                                                                let res = core::apply_depth_update(&mut book, &update);
                                                                                if res == core::ApplyResult::Gap {
                                                                                    let is_contract = cfg.id.contains("contract");
                                                                                    if let Ok(new_book) = fetch_depth_snapshot(&client, &sym, is_contract).await {
                                                                                        books.insert(sym.clone(), new_book);
                                                                                    }
                                                                                }
                                                                            } else {
                                                                                let snap = core::DepthSnapshot {
                                                                                    last_update_id: update.final_update_id,
                                                                                    bids: update
                                                                                        .bids
                                                                                        .iter()
                                                                                        .map(|[p, q]| [p.to_string(), q.to_string()])
                                                                                        .collect(),
                                                                                    asks: update
                                                                                        .asks
                                                                                        .iter()
                                                                                        .map(|[p, q]| [p.to_string(), q.to_string()])
                                                                                        .collect(),
                                                                                };
                                                                                books.insert(sym.clone(), snap.into());
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
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
