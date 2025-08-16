use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use futures::{future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::borrow::Cow;
use std::sync::{Arc, Once};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info};
use uuid::Uuid;

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
    ws_base: "wss://www.lbkex.net/ws/",
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

/// Adapter implementing the `ExchangeAdapter` trait for LBank.
pub struct LbankAdapter {
    _cfg: &'static LbankConfig,
    _client: Client,
    _chunk_size: usize,
    _symbols: Vec<String>,
    channels: ChannelRegistry,
}

impl LbankAdapter {
    pub fn new(
        cfg: &'static LbankConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
        channels: ChannelRegistry,
    ) -> Self {
        Self {
            _cfg: cfg,
            _client: client,
            _chunk_size: chunk_size,
            _symbols: symbols,
            channels,
        }
    }

    async fn run_symbol(
        url: String,
        symbol: String,
        tx: mpsc::Sender<core::events::StreamMessage<'static>>,
    ) -> Result<()> {
        loop {
            let (ws_stream, _) = connect_async(&url).await?;
            let (mut write, mut read) = ws_stream.split();

            // subscribe to topics
            let sub_trade = json!({
                "action": "subscribe",
                "subscribe": "trade",
                "pair": symbol.clone()
            });
            write.send(Message::Text(sub_trade.to_string())).await?;
            let sub_depth = json!({
                "action": "subscribe",
                "subscribe": "depth",
                "depth": "100",
                "pair": symbol.clone()
            });
            write.send(Message::Text(sub_depth.to_string())).await?;
            let sub_kbar = json!({
                "action": "subscribe",
                "subscribe": "kbar",
                "kbar": "1min",
                "pair": symbol.clone()
            });
            write.send(Message::Text(sub_kbar.to_string())).await?;
            info!("subscribed 3 topics to {}", url);

            let write = Arc::new(Mutex::new(write));
            let ping_writer = write.clone();
            let last_pong = Arc::new(Mutex::new(Instant::now()));
            let pong_check = last_pong.clone();
            tokio::spawn(async move {
                let mut intv = interval(Duration::from_secs(30));
                loop {
                    intv.tick().await;
                    let ping = json!({"action":"ping","ping": Uuid::new_v4().to_string()});
                    {
                        let mut w = ping_writer.lock().await;
                        if w.send(Message::Text(ping.to_string())).await.is_err() {
                            break;
                        }
                        if pong_check.lock().await.elapsed() > Duration::from_secs(60) {
                            let _ = w.close().await;
                            break;
                        }
                    }
                }
            });

            let pong_updater = last_pong.clone();
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Some(event) = parse_message(&text) {
                            if tx.send(event).await.is_err() {
                                return Ok(());
                            }
                        } else if let Ok(v) = serde_json::from_str::<Value>(&text) {
                            if v.get("action").and_then(|a| a.as_str()) == Some("ping") {
                                if let Some(id) = v.get("ping").and_then(|p| p.as_str()) {
                                    let pong = json!({"action":"pong","pong":id});
                                    let _ = write
                                        .lock()
                                        .await
                                        .send(Message::Text(pong.to_string()))
                                        .await;
                                }
                            } else if v.get("action").and_then(|a| a.as_str()) == Some("pong") {
                                *pong_updater.lock().await = Instant::now();
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        let _ = write.lock().await.send(Message::Pong(data)).await;
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        }
    }
}

/// Parse a raw JSON text into a canonical stream message if possible.
fn parse_message(text: &str) -> Option<core::events::StreamMessage<'static>> {
    if let Ok(val) = serde_json::from_str::<Value>(text) {
        if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
            match t {
                "trade" => return parse_trade_frame(&val).ok(),
                "depth" => return parse_depth_frame(&val).ok(),
                "kbar" => return parse_kbar_frame(&val).ok(),
                _ => {}
            }
        }
    }
    None
}

fn parse_kbar_frame(val: &Value) -> Result<core::events::StreamMessage<'static>> {
    let pair = val
        .get("pair")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing pair"))?
        .to_string();
    let kbar = val
        .get("kbar")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("missing kbar"))?;
    let open = kbar.get("o").and_then(|v| v.as_f64()).unwrap_or_default();
    let high = kbar.get("h").and_then(|v| v.as_f64()).unwrap_or_default();
    let low = kbar.get("l").and_then(|v| v.as_f64()).unwrap_or_default();
    let close = kbar.get("c").and_then(|v| v.as_f64()).unwrap_or_default();
    let volume = kbar.get("v").and_then(|v| v.as_f64()).unwrap_or_default();
    let event = core::events::KlineEvent {
        event_time: 0,
        symbol: pair.clone(),
        kline: core::events::Kline {
            start_time: 0,
            close_time: 0,
            interval: "1m".to_string(),
            open: Cow::Owned(open.to_string()),
            close: Cow::Owned(close.to_string()),
            high: Cow::Owned(high.to_string()),
            low: Cow::Owned(low.to_string()),
            volume: Cow::Owned(volume.to_string()),
            trades: kbar.get("n").and_then(|v| v.as_u64()).unwrap_or(0),
            is_closed: true,
            quote_volume: Cow::Owned("0".to_string()),
            taker_buy_base_volume: Cow::Owned("0".to_string()),
            taker_buy_quote_volume: Cow::Owned("0".to_string()),
        },
    };
    Ok(core::events::StreamMessage {
        stream: Box::leak(format!("{}@kbar", pair).into_boxed_str()).into(),
        data: core::events::Event::Kline(event),
    })
}

/// Parse trade frame into canonical event.
pub fn parse_trade_frame(val: &Value) -> Result<core::events::StreamMessage<'static>> {
    let pair = val
        .get("pair")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing pair"))?
        .to_string();
    let trade = val
        .get("trade")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("missing trade"))?;
    let price = trade
        .get("price")
        .and_then(|v| v.as_f64())
        .unwrap_or_default();
    let volume = trade
        .get("volume")
        .and_then(|v| v.as_f64())
        .unwrap_or_default();
    let direction = trade
        .get("direction")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let ev = core::events::TradeEvent {
        event_time: 0,
        symbol: pair.clone(),
        trade_id: 0,
        price: Cow::Owned(price.to_string()),
        quantity: Cow::Owned(volume.to_string()),
        buyer_order_id: 0,
        seller_order_id: 0,
        trade_time: 0,
        buyer_is_maker: direction.eq_ignore_ascii_case("sell"),
        best_match: true,
    };
    Ok(core::events::StreamMessage {
        stream: Box::leak(format!("{}@trade", pair).into_boxed_str()).into(),
        data: core::events::Event::Trade(ev),
    })
}

/// Parse depth frame into canonical event.
pub fn parse_depth_frame(val: &Value) -> Result<core::events::StreamMessage<'static>> {
    let pair = val
        .get("pair")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing pair"))?
        .to_string();
    let depth = val
        .get("depth")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("missing depth"))?;
    let mut bids = Vec::new();
    if let Some(arr) = depth.get("bids").and_then(|v| v.as_array()) {
        for level in arr.iter().filter_map(|l| l.as_array()) {
            if level.len() == 2 {
                let price = level[0].as_f64().unwrap_or_default().to_string();
                let qty = level[1].as_f64().unwrap_or_default().to_string();
                bids.push([Cow::Owned(price), Cow::Owned(qty)]);
            }
        }
    }
    let mut asks = Vec::new();
    if let Some(arr) = depth.get("asks").and_then(|v| v.as_array()) {
        for level in arr.iter().filter_map(|l| l.as_array()) {
            if level.len() == 2 {
                let price = level[0].as_f64().unwrap_or_default().to_string();
                let qty = level[1].as_f64().unwrap_or_default().to_string();
                asks.push([Cow::Owned(price), Cow::Owned(qty)]);
            }
        }
    }

    let ev = core::events::DepthUpdateEvent {
        event_time: 0,
        symbol: pair.clone(),
        first_update_id: 0,
        final_update_id: 0,
        previous_final_update_id: 0,
        bids,
        asks,
    };

    Ok(core::events::StreamMessage {
        stream: Box::leak(format!("{}@depth", pair).into_boxed_str()).into(),
        data: core::events::Event::DepthUpdate(ev),
    })
}

#[async_trait]
impl ExchangeAdapter for LbankAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        for symbol in &self._symbols {
            if let Some(tx) = self.channels.get(&format!("{}:{}", self._cfg.name, symbol)) {
                let url = self._cfg.ws_base.to_string();
                let sym = symbol.clone();
                tokio::spawn(async move {
                    if let Err(e) = LbankAdapter::run_symbol(url, sym, tx).await {
                        error!("lbank stream error: {}", e);
                    }
                });
            }
        }
        Ok(())
    }
    async fn run(&mut self) -> Result<()> {
        self.subscribe().await?;
        futures::future::pending::<()>().await;
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
