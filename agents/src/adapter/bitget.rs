use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use core::events::StreamMessage;
use futures::{future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Once,
    },
};
use tokio::sync::mpsc;
use tokio::{
    signal,
    task::JoinHandle,
    time::{sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, StreamSender, TaskSet};

/// Configuration for the Bitget exchange.
pub struct BitgetConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub ws_base: &'static str,
}

/// Supported Bitget exchange endpoints.
pub const BITGET_EXCHANGES: &[BitgetConfig] = &[BitgetConfig {
    id: "bitget",
    name: "Bitget",
    ws_base: "wss://ws.bitget.com/spot/v1/stream",
}];

/// Retrieve all trading symbols across Bitget spot and futures markets.
pub async fn fetch_symbols() -> Result<Vec<String>> {
    let client = Client::new();
    let mut result = Vec::new();

    // Spot symbols
    let resp = client
        .get("https://api.bitget.com/api/spot/v1/public/products")
        .send()
        .await?
        .error_for_status()?;
    let data: Value = resp.json().await?;
    let arr = data
        .get("data")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing data array"))?;
    result.extend(arr.iter().filter_map(|s| {
        let status = s.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status.eq_ignore_ascii_case("online") {
            s.get("symbol")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string())
        } else {
            None
        }
    }));

    // Futures product types
    let product_types = ["umcbl", "dmcbl", "cmcbl"];
    for pt in &product_types {
        let resp = client
            .get("https://api.bitget.com/api/mix/v1/market/contracts")
            .query(&[("productType", *pt)])
            .send()
            .await?
            .error_for_status()?;
        let data: Value = resp.json().await?;
        let arr = data
            .get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("missing data array"))?;
        result.extend(arr.iter().filter_map(|s| {
            let status = s.get("symbolStatus").and_then(|v| v.as_str()).unwrap_or("");
            if status.eq_ignore_ascii_case("normal") {
                s.get("symbol")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string())
            } else {
                None
            }
        }));
    }

    result.sort();
    result.dedup();
    Ok(result)
}

static REGISTER: Once = Once::new();

/// Register Bitget adapter factories.
pub fn register() {
    REGISTER.call_once(|| {
        for exch in BITGET_EXCHANGES {
            let cfg_ref: &'static BitgetConfig = exch;
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
                        Result<Vec<mpsc::Receiver<StreamMessage<'static>>>>,
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

                              let adapter = BitgetAdapter::new(
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

/// Placeholder adapter for Bitget. Full streaming support is not yet implemented.
pub struct BitgetAdapter {
    cfg: &'static BitgetConfig,
    _client: Client,
    chunk_size: usize,
    symbols: Vec<String>,
    channels: ChannelRegistry,
    tasks: Vec<JoinHandle<Result<()>>>,
    shutdown: Arc<AtomicBool>,
}

impl BitgetAdapter {
    pub fn new(
        cfg: &'static BitgetConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
        channels: ChannelRegistry,
    ) -> Self {
        Self {
            cfg,
            _client: client,
            chunk_size,
            symbols,
            channels,
            tasks: Vec::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl super::ExchangeAdapter for BitgetAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        for symbols in self.symbols.chunks(self.chunk_size) {
            let symbol_list = symbols.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            let ws_url = self.cfg.ws_base.to_string();
            let shutdown = self.shutdown.clone();
            let channels = self.channels.clone();
            let cfg = self.cfg;

            let handle = tokio::spawn(async move {
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    match connect_async(&ws_url).await {
                        Ok((mut ws, _)) => {
                            let mut args = Vec::new();
                            let mut senders: HashMap<String, StreamSender> = HashMap::new();
                            for s in &symbol_list {
                                args.push(serde_json::json!({"channel":"trade","instId":s}));
                                args.push(serde_json::json!({"channel":"depth","instId":s}));
                                args.push(serde_json::json!({"channel":"candle1m","instId":s}));
                                let key = format!("{}:{}", cfg.name, s);
                                if let Some(tx) = channels.get(&key) {
                                    senders.insert(s.clone(), tx);
                                }
                            }
                            let sub = serde_json::json!({"op":"subscribe","args": args});
                            if ws.send(Message::Text(sub.to_string())).await.is_ok() {
                                info!("bitget subscribed {} topics", args.len());
                            }

                            let mut hb = tokio::time::interval(Duration::from_secs(20));
                            loop {
                                tokio::select! {
                                    msg = ws.next() => {
                                        match msg {
                                            Some(Ok(Message::Text(text))) => {
                                                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                                    if let Some(event) = parse_candle_message(&v) {
                                                        if let Some(sym) = event.data.symbol() {
                                                            if let Some(tx) = senders.get(sym) {
                                                                if tx.send(event).is_err() { break; }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            Some(Ok(Message::Ping(p))) => {
                                                if ws.send(Message::Pong(p)).await.is_err() { break; }
                                            }
                                            Some(Ok(Message::Pong(_))) => {}
                                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => { break; }
                                            _ => {}
                                        }
                                    }
                                    _ = hb.tick() => {
                                        if ws.send(Message::Ping(Vec::new())).await.is_err() { break; }
                                    }
                                }
                            }
                            warn!("bitget websocket disconnected, reconnecting");
                        }
                        Err(e) => warn!("bitget connect error: {}", e),
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

fn parse_candle_message(val: &Value) -> Option<StreamMessage<'static>> {
    let arg = val.get("arg")?.as_object()?;
    let channel = arg.get("channel")?.as_str()?;
    if !channel.starts_with("candle") {
        return None;
    }
    let inst_id = arg.get("instId")?.as_str()?.to_string();
    let interval = channel.strip_prefix("candle").unwrap_or("");
    let data = val.get("data")?.as_array()?.get(0)?.as_array()?;
    let ts = data.get(0)?.as_str()?.parse::<u64>().unwrap_or(0);
    let open = data.get(1).and_then(|v| v.as_str()).unwrap_or("0");
    let high = data.get(2).and_then(|v| v.as_str()).unwrap_or("0");
    let low = data.get(3).and_then(|v| v.as_str()).unwrap_or("0");
    let close = data.get(4).and_then(|v| v.as_str()).unwrap_or("0");
    let vol = data.get(5).and_then(|v| v.as_str()).unwrap_or("0");

    let event = core::events::KlineEvent {
        event_time: ts,
        symbol: inst_id.clone(),
        kline: core::events::Kline {
            start_time: ts,
            close_time: ts,
            interval: interval.to_string(),
            open: Cow::Owned(open.to_string()),
            close: Cow::Owned(close.to_string()),
            high: Cow::Owned(high.to_string()),
            low: Cow::Owned(low.to_string()),
            volume: Cow::Owned(vol.to_string()),
            trades: 0,
            is_closed: true,
            quote_volume: Cow::Owned("0".to_string()),
            taker_buy_base_volume: Cow::Owned("0".to_string()),
            taker_buy_quote_volume: Cow::Owned("0".to_string()),
        },
    };

    Some(StreamMessage {
        stream: Box::leak(format!("{}@{}", inst_id, channel).into_boxed_str()).into(),
        data: core::events::Event::Kline(event),
    })
}
