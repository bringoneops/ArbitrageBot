use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use core::events::{DepthUpdateEvent, Event, Kline, KlineEvent, StreamMessage, TradeEvent};
use futures::{future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::borrow::Cow;
use std::sync::{Arc, Once};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::error;

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};

/// Configuration for the Bitget exchange.
pub struct BitgetConfig {
    pub id: &'static str,
    pub name: &'static str,
}

/// Supported Bitget exchange endpoints.
pub const BITGET_EXCHANGES: &[BitgetConfig] = &[BitgetConfig {
    id: "bitget",
    name: "Bitget",
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
    _chunk_size: usize,
    symbols: Vec<String>,
    channels: ChannelRegistry,
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
            _chunk_size: chunk_size,
            symbols,
            channels,
        }
    }

    async fn run_symbol(
        url: String,
        symbol: String,
        tx: mpsc::Sender<StreamMessage<'static>>,
    ) -> Result<()> {
        let (ws_stream, _) = connect_async(&url).await?;
        let (mut write, mut read) = ws_stream.split();

        let sub = json!({
            "op": "subscribe",
            "args": [
                {"channel": "trade", "instId": symbol},
                {"channel": "books", "instId": symbol},
                {"channel": "candle1m", "instId": symbol}
            ]
        });
        write.send(Message::Text(sub.to_string())).await?;

        let write = Arc::new(Mutex::new(write));
        let ping_writer = write.clone();
        tokio::spawn(async move {
            let mut intv = interval(Duration::from_secs(20));
            loop {
                intv.tick().await;
                if ping_writer
                    .lock()
                    .await
                    .send(Message::Text(json!({"op":"ping"}).to_string()))
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
                    if text.contains("ping") {
                        let _ = write
                            .lock()
                            .await
                            .send(Message::Text(text.replace("ping", "pong")))
                            .await;
                        continue;
                    }
                    if let Some(ev) = handle_text(&text) {
                        if tx.send(ev).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(Message::Ping(p)) => {
                    let _ = write.lock().await.send(Message::Pong(p)).await;
                }
                Ok(Message::Close(_)) | Err(_) => {
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }
}

pub fn handle_text(text: &str) -> Option<StreamMessage<'static>> {
    let v: Value = serde_json::from_str(text).ok()?;
    let channel = v
        .get("arg")
        .and_then(|a| a.get("channel"))
        .and_then(|c| c.as_str())?;
    match channel {
        "trade" => parse_trade_frame(&v).ok(),
        "books" => parse_depth_frame(&v).ok(),
        "candle1m" => parse_kline_frame(&v).ok(),
        _ => None,
    }
}

pub fn parse_trade_frame(v: &Value) -> Result<StreamMessage<'static>> {
    let arg = v.get("arg").ok_or_else(|| anyhow!("missing arg"))?;
    let symbol = arg
        .get("instId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing instId"))?
        .to_string();
    let data = v
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow!("missing data"))?;
    let price = data.get("price").and_then(|v| v.as_str()).unwrap_or("0");
    let size = data.get("size").and_then(|v| v.as_str()).unwrap_or("0");
    let side = data.get("side").and_then(|v| v.as_str()).unwrap_or("");
    let ts = data
        .get("ts")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    let event = TradeEvent {
        event_time: ts,
        symbol: symbol.clone(),
        trade_id: 0,
        price: Cow::Owned(price.to_string()),
        quantity: Cow::Owned(size.to_string()),
        buyer_order_id: 0,
        seller_order_id: 0,
        trade_time: ts,
        buyer_is_maker: side.eq_ignore_ascii_case("sell"),
        best_match: true,
    };
    Ok(StreamMessage {
        stream: format!("{}@trade", symbol),
        data: Event::Trade(event),
    })
}

pub fn parse_depth_frame(v: &Value) -> Result<StreamMessage<'static>> {
    let arg = v.get("arg").ok_or_else(|| anyhow!("missing arg"))?;
    let symbol = arg
        .get("instId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing instId"))?
        .to_string();
    let data = v
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow!("missing data"))?;
    let ts = data
        .get("ts")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    let mut bids = Vec::new();
    if let Some(arr) = data.get("bids").and_then(|v| v.as_array()) {
        for level in arr.iter().filter_map(|l| l.as_array()) {
            if level.len() >= 2 {
                bids.push([
                    Cow::Owned(level[0].as_str().unwrap_or("0").to_string()),
                    Cow::Owned(level[1].as_str().unwrap_or("0").to_string()),
                ]);
            }
        }
    }
    let mut asks = Vec::new();
    if let Some(arr) = data.get("asks").and_then(|v| v.as_array()) {
        for level in arr.iter().filter_map(|l| l.as_array()) {
            if level.len() >= 2 {
                asks.push([
                    Cow::Owned(level[0].as_str().unwrap_or("0").to_string()),
                    Cow::Owned(level[1].as_str().unwrap_or("0").to_string()),
                ]);
            }
        }
    }

    let event = DepthUpdateEvent {
        event_time: ts,
        symbol: symbol.clone(),
        first_update_id: 0,
        final_update_id: 0,
        previous_final_update_id: 0,
        bids,
        asks,
    };

    Ok(StreamMessage {
        stream: format!("{}@depth", symbol),
        data: Event::DepthUpdate(event),
    })
}

pub fn parse_kline_frame(v: &Value) -> Result<StreamMessage<'static>> {
    let arg = v.get("arg").ok_or_else(|| anyhow!("missing arg"))?;
    let symbol = arg
        .get("instId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing instId"))?
        .to_string();
    let data = v
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing data"))?;

    let start = data
        .get(0)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let open = data.get(1).and_then(|v| v.as_str()).unwrap_or("0");
    let high = data.get(2).and_then(|v| v.as_str()).unwrap_or("0");
    let low = data.get(3).and_then(|v| v.as_str()).unwrap_or("0");
    let close = data.get(4).and_then(|v| v.as_str()).unwrap_or("0");
    let volume = data.get(5).and_then(|v| v.as_str()).unwrap_or("0");

    let kline = Kline {
        start_time: start,
        close_time: start + 60_000,
        interval: "1m".to_string(),
        open: Cow::Owned(open.to_string()),
        close: Cow::Owned(close.to_string()),
        high: Cow::Owned(high.to_string()),
        low: Cow::Owned(low.to_string()),
        volume: Cow::Owned(volume.to_string()),
        trades: 0,
        is_closed: true,
        quote_volume: Cow::Owned("0".to_string()),
        taker_buy_base_volume: Cow::Owned("0".to_string()),
        taker_buy_quote_volume: Cow::Owned("0".to_string()),
    };
    let event = KlineEvent {
        event_time: start + 60_000,
        symbol: symbol.clone(),
        kline,
    };
    Ok(StreamMessage {
        stream: format!("{}@kline_1m", symbol),
        data: Event::Kline(event),
    })
}

#[async_trait]
impl super::ExchangeAdapter for BitgetAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        for symbol in &self.symbols {
            if let Some(tx) = self.channels.get(&format!("{}:{}", self.cfg.name, symbol)) {
                let url = "wss://ws.bitget.com/spot/v1/stream?compress=false".to_string();
                let sym = symbol.clone();
                tokio::spawn(async move {
                    if let Err(e) = BitgetAdapter::run_symbol(url, sym, tx).await {
                        error!("bitget stream error: {}", e);
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
