use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use futures::{future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::{
    borrow::Cow,
    sync::{Arc, Once},
};
use tokio::{
    sync::mpsc,
    time::{interval, sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use uuid::Uuid;

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};
use core::events::{KucoinKline, KucoinLevel2, KucoinStreamMessage, KucoinTrade};
use rustls::ClientConfig;

/// Candle intervals supported by KuCoin.
const CANDLE_INTERVALS: &[&str] = &[
    "1min", "3min", "5min", "15min", "30min", "1hour", "2hour", "4hour", "6hour", "8hour",
    "12hour", "1day", "1week", "1month",
];

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
    channels: ChannelRegistry,
}

impl KucoinAdapter {
    pub fn new(
        cfg: &'static KucoinConfig,
        client: Client,
        symbols: Vec<String>,
        channels: ChannelRegistry,
    ) -> Self {
        Self {
            cfg,
            _client: client,
            symbols,
            channels,
        }
    }

    async fn connect_once(&self) -> Result<()> {
        let api_base = if self.cfg.id.contains("futures") {
            "https://api-futures.kucoin.com"
        } else {
            "https://api.kucoin.com"
        };
        let bullet_url = format!("{api_base}/api/v1/bullet-public");
        let resp: Value = self
            ._client
            .post(&bullet_url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let data = resp.get("data").ok_or_else(|| anyhow!("missing data"))?;
        let token = data
            .get("token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("missing token"))?;
        let instance = data
            .get("instanceServers")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .ok_or_else(|| anyhow!("missing instance server"))?;
        let endpoint = instance
            .get("endpoint")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("missing endpoint"))?;
        let ping_interval = instance
            .get("pingInterval")
            .and_then(|v| v.as_u64())
            .unwrap_or(50000);

        let ws_url = format!("{endpoint}?token={token}");
        let (ws_stream, _) = connect_async(&ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        for symbol in &self.symbols {
            let mut topics = vec![
                format!("/market/match:{symbol}"),
                format!("/market/level2:{symbol}"),
            ];
            for interval in CANDLE_INTERVALS {
                topics.push(format!("/market/candles:{interval}:{symbol}"));
            }
            let topics: Vec<String> = if self.cfg.id.contains("futures") {
                vec![
                    format!("/contractMarket/ticker:{symbol}"),
                    format!("/contractMarket/level2:{symbol}"),
                    format!("/contractMarket/level2Depth5:{symbol}"),
                    format!("/contractMarket/level2Depth50:{symbol}"),
                    format!("/contractMarket/execution:{symbol}"),
                    format!("/contractMarket/indexPrice:{symbol}"),
                    format!("/contractMarket/markPrice:{symbol}"),
                    format!("/contractMarket/fundingRate:{symbol}"),
                    format!("/contractMarket/candles:1min:{symbol}"),
                ]
            } else {
                vec![
                    format!("/market/match:{symbol}"),
                    format!("/market/level2:{symbol}"),
                    format!("/market/candles:1min:{symbol}"),
                ]
            };
            for topic in topics {
                let msg = json!({
                    "id": Uuid::new_v4().to_string(),
                    "type": "subscribe",
                    "topic": topic,
                    "privateChannel": false,
                    "response": true,
                });
                write.send(Message::Text(msg.to_string())).await?;
            }
        }

        let mut intv = interval(Duration::from_millis(ping_interval));
        loop {
            tokio::select! {
                _ = intv.tick() => {
                    let ping = json!({
                        "id": Uuid::new_v4().to_string(),
                        "type": "ping"
                    });
                    if write.send(Message::Text(ping.to_string())).await.is_err() {
                        break;
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(ev_msg) = serde_json::from_str::<KucoinStreamMessage>(&text) {
                                if let Some(event) = map_message(ev_msg) {
                                    let symbol_key = match &event.data {
                                        core::events::Event::Trade(e) => &e.symbol,
                                        core::events::Event::DepthUpdate(e) => &e.symbol,
                                        core::events::Event::Kline(e) => &e.symbol,
                                        _ => "",
                                    };
                                    if !symbol_key.is_empty() {
                                        let key = format!("{name}:{symbol}", name = self.cfg.name, symbol = symbol_key);
                                        let (tx, _) = self.channels.get_or_create(&key);
                                        if tx.send(event).is_err() {
                                            break;
                                        }
                                    }
                                }
                            } else if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                if v.get("type").and_then(|v| v.as_str()) == Some("ping") {
                                    if let Some(id) = v.get("id").and_then(|v| v.as_str()) {
                                        let pong = json!({"id": id, "type": "pong"});
                                        let _ = write.send(Message::Text(pong.to_string())).await;
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }
}

fn map_message(msg: KucoinStreamMessage) -> Option<core::events::StreamMessage<'static>> {
    match msg.subject.as_str() {
        "trade.l3match" => {
            let data: KucoinTrade = serde_json::from_value(msg.data).ok()?;
            let buyer_is_maker = matches!(data.side.as_ref(), "sell");
            let ev = core::events::TradeEvent {
                event_time: data.trade_time,
                symbol: data.symbol.clone(),
                trade_id: data.sequence,
                price: Cow::Owned(data.price.into_owned()),
                quantity: Cow::Owned(data.size.into_owned()),
                buyer_order_id: 0,
                seller_order_id: 0,
                trade_time: data.trade_time,
                buyer_is_maker,
                best_match: true,
            };
            Some(core::events::StreamMessage {
                stream: format!("{symbol}@trade", symbol = ev.symbol),
                data: core::events::Event::Trade(ev),
            })
        }
        "trade.l2update" => {
            let data: KucoinLevel2 = serde_json::from_value(msg.data).ok()?;
            let bids = data
                .changes
                .bids
                .into_iter()
                .map(|lvl| {
                    [
                        Cow::Owned(lvl[0].clone().into_owned()),
                        Cow::Owned(lvl[1].clone().into_owned()),
                    ]
                })
                .collect();
            let asks = data
                .changes
                .asks
                .into_iter()
                .map(|lvl| {
                    [
                        Cow::Owned(lvl[0].clone().into_owned()),
                        Cow::Owned(lvl[1].clone().into_owned()),
                    ]
                })
                .collect();
            let ev = core::events::DepthUpdateEvent {
                event_time: data.sequence_end,
                symbol: data.symbol.clone(),
                first_update_id: data.sequence_start,
                final_update_id: data.sequence_end,
                previous_final_update_id: 0,
                bids,
                asks,
            };
            Some(core::events::StreamMessage {
                stream: format!("{symbol}@depth", symbol = ev.symbol),
                data: core::events::Event::DepthUpdate(ev),
            })
        }
        "trade.candles.update" => {
            let data: KucoinKline = serde_json::from_value(msg.data).ok()?;
            let interval = msg.topic.split(':').nth(1).unwrap_or("").to_string();
            let start = data.candles[0].parse::<u64>().unwrap_or(data.time);
            let ev = core::events::KlineEvent {
                event_time: data.time,
                symbol: data.symbol.clone(),
                kline: core::events::Kline {
                    start_time: start,
                    close_time: data.time,
                    interval: interval.clone(),
                    open: Cow::Owned(data.candles[1].clone().into_owned()),
                    close: Cow::Owned(data.candles[2].clone().into_owned()),
                    high: Cow::Owned(data.candles[3].clone().into_owned()),
                    low: Cow::Owned(data.candles[4].clone().into_owned()),
                    volume: Cow::Owned(data.candles[5].clone().into_owned()),
                    trades: 0,
                    is_closed: true,
                    quote_volume: Cow::Owned("0".to_string()),
                    taker_buy_base_volume: Cow::Owned("0".to_string()),
                    taker_buy_quote_volume: Cow::Owned("0".to_string()),
                },
            };
            Some(core::events::StreamMessage {
                stream: format!("{symbol}@kline_{interval}", symbol = ev.symbol),
                data: core::events::Event::Kline(ev),
            })
        }
        _ => None,
    }
}

#[async_trait]
impl ExchangeAdapter for KucoinAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        loop {
            if let Err(e) = self.connect_once().await {
                tracing::error!("kucoin connection error: {}", e);
                sleep(Duration::from_secs(1)).await;
            }
        }
        #[allow(unreachable_code)]
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
                                let key = format!("{name}:{symbol}", name = cfg.name, symbol = symbol);
                                let (_, rx) = channels.get_or_create(&key);
                                if let Some(rx) = rx {
                                    receivers.push(rx);
                                }
                            }

                            let adapter =
                                KucoinAdapter::new(cfg, client.clone(), symbols, channels.clone());

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
