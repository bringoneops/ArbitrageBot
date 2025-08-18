use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use futures::{future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::borrow::Cow;
use std::sync::{Arc, Once};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info};

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};
use core::events::{
    Event, GateioDepth, GateioKline, GateioStreamMessage, GateioTrade, StreamMessage,
};

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
        ws_base: "wss://ws.gate.com/v3/",
    },
    GateioConfig {
        id: "gateio_futures",
        name: "Gate.io Futures",
        info_url: "https://api.gateio.ws/api/v4/futures/{settle}/contracts",
        ws_base: "wss://ws.gate.com/v3/",
    },
];

/// Retrieve all trading symbols for Gate.io across market types using the provided HTTP client.
///
/// The caller must supply a reusable [`reqwest::Client`] instance for efficiency.
pub async fn fetch_symbols(client: &Client, cfg: &GateioConfig) -> Result<Vec<String>> {
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
                let arr = data.as_array().ok_or_else(|| anyhow!("expected array"))?;
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
            let arr = data.as_array().ok_or_else(|| anyhow!("expected array"))?;
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
                                symbols = fetch_symbols(&client, cfg).await?;
                            }

                            let mut receivers = Vec::new();
                            for symbol in &symbols {
                                let key = format!("{name}:{symbol}", name = cfg.name, symbol = symbol);
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

/// Minimal Gate.io adapter implementing [`ExchangeAdapter`].
pub struct GateioAdapter {
    cfg: &'static GateioConfig,
    _client: Client,
    _chunk_size: usize,
    symbols: Vec<String>,
    channels: ChannelRegistry,
}

impl GateioAdapter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: &'static GateioConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
        channels: ChannelRegistry,
    ) -> Self {
        Self {
            cfg,
            _client: client,
            _chunk_size: chunk_size,
            symbols,
            channels,
        }
    }
}

fn map_message(msg: GateioStreamMessage<'_>) -> Option<StreamMessage<'static>> {
    match msg.method {
        "trades.update" => parse_trade_frame(&msg).ok(),
        "depth.update" => parse_depth_frame(&msg).ok(),
        "kline.update" => parse_kline_frame(&msg).ok(),
        _ => None,
    }
}

fn parse_trade_frame(msg: &GateioStreamMessage<'_>) -> Result<StreamMessage<'static>> {
    let symbol = msg
        .params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing symbol"))?;
    let trades: Vec<GateioTrade> = serde_json::from_value(
        msg.params
            .get(1)
            .cloned()
            .ok_or_else(|| anyhow!("missing trades"))?,
    )?;
    let trade = trades.first().ok_or_else(|| anyhow!("missing trade"))?;
    let ev = core::events::TradeEvent {
        event_time: trade.create_time_ms,
        symbol: symbol.to_string(),
        trade_id: trade.id,
        price: Cow::Owned(trade.price.clone().into_owned()),
        quantity: Cow::Owned(trade.amount.clone().into_owned()),
        buyer_order_id: 0,
        seller_order_id: 0,
        trade_time: trade.create_time_ms,
        buyer_is_maker: trade.side.eq_ignore_ascii_case("sell"),
        best_match: true,
    };
    Ok(StreamMessage {
        stream: format!("{symbol}@trade"),
        data: Event::Trade(ev),
    })
}

fn parse_depth_frame(msg: &GateioStreamMessage<'_>) -> Result<StreamMessage<'static>> {
    let symbol = msg
        .params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing symbol"))?;
    let depth: GateioDepth = serde_json::from_value(
        msg.params
            .get(1)
            .cloned()
            .ok_or_else(|| anyhow!("missing depth"))?,
    )?;
    let bids = depth
        .bids
        .into_iter()
        .map(|[p, q]| [Cow::Owned(p.into_owned()), Cow::Owned(q.into_owned())])
        .collect();
    let asks = depth
        .asks
        .into_iter()
        .map(|[p, q]| [Cow::Owned(p.into_owned()), Cow::Owned(q.into_owned())])
        .collect();
    let id = depth.id.unwrap_or_default();
    let ev = core::events::DepthUpdateEvent {
        event_time: depth.timestamp,
        symbol: symbol.to_string(),
        first_update_id: id,
        final_update_id: id,
        previous_final_update_id: 0,
        bids,
        asks,
    };
    Ok(StreamMessage {
        stream: format!("{symbol}@depth"),
        data: Event::DepthUpdate(ev),
    })
}

fn parse_kline_frame(msg: &GateioStreamMessage<'_>) -> Result<StreamMessage<'static>> {
    let symbol = msg
        .params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing symbol"))?;
    let klines: Vec<GateioKline> = serde_json::from_value(
        msg.params
            .get(1)
            .cloned()
            .ok_or_else(|| anyhow!("missing kline"))?,
    )?;
    let k = klines
        .first()
        .ok_or_else(|| anyhow!("missing kline data"))?;
    let interval = msg.params.get(2).and_then(|v| v.as_str()).unwrap_or("1m");
    let ev = core::events::KlineEvent {
        event_time: k.timestamp,
        symbol: symbol.to_string(),
        kline: core::events::Kline {
            start_time: k.timestamp,
            close_time: k.timestamp,
            interval: interval.to_string(),
            open: Cow::Owned(k.open.clone().into_owned()),
            close: Cow::Owned(k.close.clone().into_owned()),
            high: Cow::Owned(k.high.clone().into_owned()),
            low: Cow::Owned(k.low.clone().into_owned()),
            volume: Cow::Owned(k.volume.clone().into_owned()),
            trades: 0,
            is_closed: true,
            quote_volume: Cow::Owned("0".to_string()),
            taker_buy_base_volume: Cow::Owned("0".to_string()),
            taker_buy_quote_volume: Cow::Owned("0".to_string()),
        },
    };
    Ok(StreamMessage {
        stream: format!("{symbol}@kline_{interval}"),
        data: Event::Kline(ev),
    })
}

#[async_trait]
impl ExchangeAdapter for GateioAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        let symbols = self.symbols.clone();
        loop {
            match connect_async(self.cfg.ws_base).await {
                Ok((ws_stream, _)) => {
                    let (write, mut read) = ws_stream.split();
                    let write = Arc::new(Mutex::new(write));

                    info!(
                        "connected to {}: trades={}, depth={}, kline={}",
                        self.cfg.name,
                        symbols.len(),
                        symbols.len(),
                        symbols.len()
                    );

                    {
                        let mut w = write.lock().await;
                        w.send(Message::Text(
                            json!({
                                "id": 1,
                                "method": "trades.subscribe",
                                "params": symbols.clone()
                            })
                            .to_string(),
                        ))
                        .await?;
                        w.send(Message::Text(
                            json!({
                                "id": 2,
                                "method": "depth.subscribe",
                                "params": symbols.clone()
                            })
                            .to_string(),
                        ))
                        .await?;
                        let kline_params: Vec<Value> =
                            symbols.iter().map(|s| json!([s, "1m"])).collect();
                        w.send(Message::Text(
                            json!({
                                "id": 3,
                                "method": "kline.subscribe",
                                "params": kline_params
                            })
                            .to_string(),
                        ))
                        .await?;
                    }

                    let ping_writer = write.clone();
                    let heartbeat = tokio::spawn(async move {
                        let mut intv = interval(Duration::from_secs(30));
                        loop {
                            intv.tick().await;
                            if ping_writer
                                .lock()
                                .await
                                .send(Message::Text(json!({"method": "server.ping"}).to_string()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    });

                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                if let Ok(msg) = serde_json::from_str::<GateioStreamMessage>(&text)
                                {
                                    if msg.method == "server.ping" {
                                        let _ = write
                                            .lock()
                                            .await
                                            .send(Message::Text(
                                                json!({"method": "server.pong"}).to_string(),
                                            ))
                                            .await;
                                        continue;
                                    }
                                    if let Some(event) = map_message(msg) {
                                        let symbol_key = match &event.data {
                                            Event::Trade(ev) => &ev.symbol,
                                            Event::DepthUpdate(ev) => &ev.symbol,
                                            Event::Kline(ev) => &ev.symbol,
                                            _ => unreachable!(),
                                        };
                                        let key = format!("{name}:{symbol}", name = self.cfg.name, symbol = symbol_key);
                                        let (tx, _) = self.channels.get_or_create(&key);
                                        if tx.send(event).is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                            Ok(Message::Ping(p)) => {
                                let _ = write.lock().await.send(Message::Pong(p)).await;
                            }
                            Ok(Message::Close(_)) => break,
                            Err(e) => {
                                error!("websocket error: {}", e);
                                break;
                            }
                            _ => {}
                        }
                    }

                    heartbeat.abort();
                }
                Err(e) => {
                    error!("connect error: {}", e);
                }
            }
            sleep(Duration::from_secs(5)).await;
            info!("reconnecting to {}", self.cfg.name);
        }
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
