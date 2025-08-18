use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use core::rate_limit::TokenBucket;
use core::{chunk_streams_with_config, stream_config_for_exchange, OrderBook};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{
    signal,
    task::JoinHandle,
    time::{sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, StreamSender, TaskSet};
use futures::future::BoxFuture;
use std::sync::Once;
use tokio::sync::mpsc;
use tracing::error;
use std::collections::HashMap;

pub struct CoinexConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub ws_base: &'static str,
}

pub const COINEX_EXCHANGES: &[CoinexConfig] = &[
    CoinexConfig {
        id: "coinex_spot",
        name: "CoinEx Spot",
        ws_base: "wss://socket.coinex.com/",
    },
    CoinexConfig {
        id: "coinex_perpetual",
        name: "CoinEx Perpetual",
        ws_base: "wss://perpetual.coinex.com/",
    },
];

const SPOT_INFO_URL: &str = "https://api.coinex.com/v1/market/info";
const PERP_INFO_URL: &str = "https://api.coinex.com/perpetual/v1/market/list";

pub async fn fetch_symbols() -> Result<Vec<String>> {
    let client = Client::new();
    let mut symbols: Vec<String> = Vec::new();

    let mut page = 1;
    let limit = 100;
    loop {
        let url = format!("{SPOT_INFO_URL}?limit={limit}&page={page}");
        let resp = client.get(&url).send().await?.error_for_status()?;
        let data: Value = resp.json().await?;
        let obj = data
            .get("data")
            .and_then(|v| v.as_object())
            .ok_or_else(|| anyhow!("missing data object"))?;
        let count = obj.len();
        symbols.extend(obj.keys().cloned());
        if count < limit {
            break;
        }
        page += 1;
    }

    let mut page = 1;
    loop {
        let url = format!("{PERP_INFO_URL}?limit={limit}&page={page}");
        let resp = client.get(&url).send().await?.error_for_status()?;
        let data: Value = resp.json().await?;
        let arr = data
            .get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("missing data array"))?;
        let count = arr.len();
        for item in arr {
            if let Some(sym) = item.get("name").and_then(|v| v.as_str()) {
                symbols.push(sym.to_string());
            }
        }
        if count < limit {
            break;
        }
        page += 1;
    }

    symbols.sort();
    symbols.dedup();
    Ok(symbols)
}

#[derive(Debug, Deserialize)]
struct BboData {
    #[serde(rename = "b", alias = "bid")]
    b: String,
    #[serde(rename = "B", alias = "bidVolume", default)]
    B: String,
    #[serde(rename = "a", alias = "ask")]
    a: String,
    #[serde(rename = "A", alias = "askVolume", default)]
    A: String,
}

static REGISTER: Once = Once::new();

pub fn register() {
    REGISTER.call_once(|| {
        for exch in COINEX_EXCHANGES {
            let cfg_ref: &'static CoinexConfig = exch;
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

                            let adapter = CoinexAdapter::new(
                                cfg,
                                client.clone(),
                                global_cfg.chunk_size,
                                symbols,
                                channels.clone(),
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

pub struct CoinexAdapter {
    cfg: &'static CoinexConfig,
    _client: Client,
    chunk_size: usize,
    symbols: Vec<String>,
    _books: Arc<DashMap<String, OrderBook>>,
    http_bucket: Arc<TokenBucket>,
    ws_bucket: Arc<TokenBucket>,
    channels: ChannelRegistry,
    tasks: Vec<JoinHandle<Result<()>>>,
    shutdown: Arc<AtomicBool>,
}

impl CoinexAdapter {
    pub fn new(
        cfg: &'static CoinexConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
        channels: ChannelRegistry,
    ) -> Self {
        let global_cfg = core::config::get();
        Self {
            cfg,
            _client: client,
            chunk_size,
            symbols,
            _books: Arc::new(DashMap::new()),
            http_bucket: Arc::new(TokenBucket::new(
                global_cfg.http_burst,
                global_cfg.http_refill_per_sec,
                std::time::Duration::from_secs(1),
            )),
            ws_bucket: Arc::new(TokenBucket::new(
                global_cfg.ws_burst,
                global_cfg.ws_refill_per_sec,
                std::time::Duration::from_secs(1),
            )),
            channels,
            tasks: Vec::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl ExchangeAdapter for CoinexAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        let symbol_refs: Vec<&str> = self.symbols.iter().map(|s| s.as_str()).collect();
        let cfg = stream_config_for_exchange(self.cfg.name);
        let chunks = chunk_streams_with_config(&symbol_refs, self.chunk_size, cfg);

        for chunk in chunks {
            let symbols = chunk.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            let ws_url = self.cfg.ws_base.to_string();
            let ws_bucket = self.ws_bucket.clone();
            let shutdown = self.shutdown.clone();
            let channels = self.channels.clone();
            let cfg = self.cfg;

            let handle = tokio::spawn(async move {
                let mut senders: HashMap<String, StreamSender> = HashMap::new();
                for s in &symbols {
                    let key = format!("{}:{}", cfg.name, s);
                    if let Some(tx) = channels.get(&key) {
                        senders.insert(s.clone(), tx);
                    }
                }

                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    if ws_bucket.acquire(1).await.is_err() {
                        break;
                    }
                    match connect_async(&ws_url).await {
                        Ok((mut ws, _)) => {
                            for symbol in &symbols {
                                let sub = serde_json::json!({
                                    "method": "depth.subscribe",
                                    "params": [symbol, 50, "0"],
                                    "id": 0,
                                });
                                let _ = ws.send(Message::Text(sub.to_string())).await;
                                let sub = serde_json::json!({
                                    "method": "deals.subscribe",
                                    "params": [symbol],
                                    "id": 0,
                                });
                                let _ = ws.send(Message::Text(sub.to_string())).await;
                                let sub = serde_json::json!({
                                    "method": "state.subscribe",
                                    "params": [symbol],
                                    "id": 0,
                                });
                                let _ = ws.send(Message::Text(sub.to_string())).await;
                                let sub = serde_json::json!({
                                    "method": "kline.subscribe",
                                    "params": [symbol, 60],
                                    "id": 0,
                                });
                                let _ = ws.send(Message::Text(sub.to_string())).await;
                                let sub = serde_json::json!({
                                    "method": "bbo.subscribe",
                                    "params": [symbol],
                                    "id": 0,
                                });
                                let _ = ws.send(Message::Text(sub.to_string())).await;
                                let sub = serde_json::json!({
                                    "method": "index.subscribe",
                                    "params": [symbol],
                                    "id": 0,
                                });
                                let _ = ws.send(Message::Text(sub.to_string())).await;
                            }
                            let topics = symbols.len() * 6;
                            tracing::info!("subscribed {topics} topics to {ws_url}");
                            loop {
                                tokio::select! {
                                    msg = ws.next() => {
                                        match msg {
                                            Some(Ok(Message::Ping(p))) => {
                                                ws.send(Message::Pong(p)).await.map_err(|e| {
                                                    tracing::error!("coinex ws pong error: {}", e);
                                                    e
                                                })?;
                                            },
                                            Some(Ok(Message::Text(text))) => {
                                                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                                    if let Some(method) = v.get("method").and_then(|m| m.as_str()) {
                                                        match method {
                                                            "bbo.update" => {
                                                                if let Some(params) = v.get("params").and_then(|p| p.as_array()) {
                                                                    if params.len() >= 2 {
                                                                        let symbol = params[0].as_str().unwrap_or_default().to_string();
                                                                        if let Ok(data) = serde_json::from_value::<BboData>(params[1].clone()) {
                                                                            let event = core::events::StreamMessage {
                                                                                stream: format!("{}@bbo", symbol),
                                                                                data: core::events::Event::BookTicker(core::events::BookTickerEvent {
                                                                                    update_id: 0,
                                                                                    symbol: symbol.clone(),
                                                                                    best_bid_price: data.b.into(),
                                                                                    best_bid_qty: data.B.into(),
                                                                                    best_ask_price: data.a.into(),
                                                                                    best_ask_qty: data.A.into(),
                                                                                }),
                                                                            };
                                                                            if let Some(tx) = senders.get(&symbol) {
                                                                                let _ = tx.send(event);
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                            "index.update" => {
                                                                if let Some(params) = v.get("params").and_then(|p| p.as_array()) {
                                                                    if params.len() >= 2 {
                                                                        let symbol = params[0].as_str().unwrap_or_default().to_string();
                                                                        if let Some(price) = params[1].as_str() {
                                                                            let event = core::events::StreamMessage {
                                                                                stream: format!("{}@index", symbol),
                                                                                data: core::events::Event::IndexPrice(core::events::IndexPriceEvent {
                                                                                    event_time: 0,
                                                                                    symbol: symbol.clone(),
                                                                                    index_price: price.to_string().into(),
                                                                                }),
                                                                            };
                                                                            if let Some(tx) = senders.get(&symbol) {
                                                                                let _ = tx.send(event);
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                            _ => {}
                                                        }
                                                    }
                                                }
                                            },
                                            Some(Ok(Message::Close(_))) | None => { break; },
                                            Some(Ok(_)) => {},
                                            Some(Err(e)) => { tracing::warn!("coinex ws error: {}", e); break; },
                                        }
                                    }
                                    _ = async {
                                        while !shutdown.load(Ordering::Relaxed) {
                                            sleep(Duration::from_secs(1)).await;
                                        }
                                    } => {
                                        let _ = ws.close(None).await;
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("coinex connect error: {}", e);
                        }
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
        self.http_bucket.acquire(1).await?;
        Ok(())
    }
}
