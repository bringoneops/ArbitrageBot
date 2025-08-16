use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use core::{chunk_streams_with_config, stream_config_for_exchange};
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use tokio::sync::mpsc;
use tokio::{
    signal,
    task::JoinHandle,
    time::{interval, sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::error;

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};
use core::rate_limit::TokenBucket;
use dashmap::DashMap;
use futures::future::BoxFuture;

/// Configuration for a single LATOKEN exchange endpoint.
pub struct LatokenConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
}

/// All LATOKEN exchanges supported by this adapter.
pub const LATOKEN_EXCHANGES: &[LatokenConfig] = &[LatokenConfig {
    id: "latoken_spot",
    name: "LATOKEN",
    info_url: "https://api.latoken.com/v2/ticker",
    ws_base: "wss://ws.latoken.com",
}];

/// Retrieve all trading symbols for LATOKEN using its ticker endpoint.
pub async fn fetch_symbols(info_url: &str) -> Result<Vec<String>> {
    let resp = Client::new()
        .get(info_url)
        .send()
        .await?
        .error_for_status()?;
    let data: Value = resp.json().await?;
    let arr = data.as_array().unwrap_or(&Vec::new()).clone();
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

static REGISTER: Once = Once::new();

pub fn register() {
    REGISTER.call_once(|| {
        for exch in LATOKEN_EXCHANGES {
            let cfg_ref: &'static LatokenConfig = exch;
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
                                symbols = fetch_symbols(cfg.info_url).await?;
                            }

                            let mut receivers = Vec::new();
                            for symbol in &symbols {
                                let key = format!("{}:{}", cfg.name, symbol);
                                let (_, rx) = channels.get_or_create(&key);
                                if let Some(rx) = rx {
                                    receivers.push(rx);
                                }
                            }

                            let adapter = LatokenAdapter::new(
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

/// Adapter implementing the `ExchangeAdapter` trait for LATOKEN.
pub struct LatokenAdapter {
    cfg: &'static LatokenConfig,
    _client: Client,
    chunk_size: usize,
    symbols: Vec<String>,
    _books: Arc<DashMap<String, core::OrderBook>>,
    http_bucket: Arc<TokenBucket>,
    ws_bucket: Arc<TokenBucket>,
    tasks: Vec<JoinHandle<Result<()>>>,
    shutdown: Arc<AtomicBool>,
}

impl LatokenAdapter {
    pub fn new(
        cfg: &'static LatokenConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
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
            tasks: Vec::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl ExchangeAdapter for LatokenAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        let symbol_refs: Vec<&str> = self.symbols.iter().map(|s| s.as_str()).collect();
        let cfg = stream_config_for_exchange(self.cfg.name);
        let chunks = chunk_streams_with_config(&symbol_refs, self.chunk_size, cfg);

        for chunk in chunks {
            let symbols = chunk.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            let ws_url = self.cfg.ws_base.to_string();
            let ws_bucket = self.ws_bucket.clone();
            let shutdown = self.shutdown.clone();

            let handle = tokio::spawn(async move {
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    ws_bucket.acquire(1).await;
                    match connect_async(&ws_url).await {
                        Ok((ws_stream, _)) => {
                            let (mut write, mut read) = ws_stream.split();

                            for symbol in &symbols {
                                let sub = serde_json::json!({
                                    "action": "subscribe",
                                    "channel": "trades",
                                    "symbol": symbol
                                });
                                let _ = write.send(Message::Text(sub.to_string())).await;
                                let sub = serde_json::json!({
                                    "action": "subscribe",
                                    "channel": "book",
                                    "symbol": symbol
                                });
                                let _ = write.send(Message::Text(sub.to_string())).await;
                                let sub = serde_json::json!({
                                    "action": "subscribe",
                                    "channel": "kline",
                                    "symbol": symbol,
                                    "interval": "1m"
                                });
                                let _ = write.send(Message::Text(sub.to_string())).await;
                            }

                            let mut ping_int = interval(Duration::from_secs(30));

                            loop {
                                tokio::select! {
                                    msg = read.next() => {
                                        match msg {
                                            Some(Ok(Message::Text(txt))) => { let _ = parse_message(&txt); },
                                            Some(Ok(Message::Ping(p))) => { let _ = write.send(Message::Pong(p)).await; },
                                            Some(Ok(Message::Close(_))) | None => { break; },
                        Some(Ok(_)) => {},
                                            Some(Err(e)) => { tracing::warn!("latoken ws error: {}", e); break; },
                                        }
                                    }
                                    _ = ping_int.tick() => {
                                        let _ = write.send(Message::Ping(Vec::new())).await;
                                    }
                                    _ = async {
                                        while !shutdown.load(Ordering::Relaxed) {
                                            sleep(Duration::from_secs(1)).await;
                                        }
                                    } => {
                                        let _ = write.close().await;
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("latoken connect error: {}", e);
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
        Ok(())
    }
}

use core::events::{DepthUpdateEvent, Event, Kline, KlineEvent, StreamMessage, TradeEvent};

pub fn parse_message(text: &str) -> Result<StreamMessage<'static>> {
    let v: Value = serde_json::from_str(text)?;
    let topic = v
        .get("channel")
        .or_else(|| v.get("topic"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing channel"))?;
    let symbol = v.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
    let stream = format!("{}@{}", symbol, topic);
    match topic {
        "trades" => {
            let data = v
                .get("data")
                .and_then(|d| d.as_object())
                .ok_or_else(|| anyhow!("missing data"))?;
            let price = data
                .get("price")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let qty = data
                .get("qty")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let id = data.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            let ts = data.get("ts").and_then(|v| v.as_u64()).unwrap_or(0);
            let side = data.get("side").and_then(|v| v.as_str()).unwrap_or("buy");
            let trade = TradeEvent {
                event_time: ts,
                symbol: symbol.to_string(),
                trade_id: id,
                price: price.into(),
                quantity: qty.into(),
                buyer_order_id: 0,
                seller_order_id: 0,
                trade_time: ts,
                buyer_is_maker: side.eq_ignore_ascii_case("sell"),
                best_match: true,
            };
            Ok(StreamMessage {
                stream,
                data: Event::Trade(trade),
            })
        }
        "book" => {
            let data = v
                .get("data")
                .and_then(|d| d.as_object())
                .ok_or_else(|| anyhow!("missing data"))?;
            let bids = data
                .get("bids")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let asks = data
                .get("asks")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let ts = data.get("ts").and_then(|v| v.as_u64()).unwrap_or(0);
            let seq_start = data.get("seq_start").and_then(|v| v.as_u64()).unwrap_or(0);
            let seq_end = data.get("seq_end").and_then(|v| v.as_u64()).unwrap_or(0);

            let bids = bids
                .into_iter()
                .filter_map(|b| {
                    let arr = b.as_array()?;
                    Some([
                        arr.get(0)?.as_str()?.to_string().into(),
                        arr.get(1)?.as_str()?.to_string().into(),
                    ])
                })
                .collect();
            let asks = asks
                .into_iter()
                .filter_map(|a| {
                    let arr = a.as_array()?;
                    Some([
                        arr.get(0)?.as_str()?.to_string().into(),
                        arr.get(1)?.as_str()?.to_string().into(),
                    ])
                })
                .collect();

            let depth = DepthUpdateEvent {
                event_time: ts,
                symbol: symbol.to_string(),
                first_update_id: seq_start,
                final_update_id: seq_end,
                previous_final_update_id: seq_start.saturating_sub(1),
                bids,
                asks,
            };
            Ok(StreamMessage {
                stream,
                data: Event::DepthUpdate(depth),
            })
        }
        "kline" => {
            let data = v
                .get("data")
                .and_then(|d| d.as_object())
                .ok_or_else(|| anyhow!("missing data"))?;
            let start = data.get("start").and_then(|v| v.as_u64()).unwrap_or(0);
            let end = data.get("end").and_then(|v| v.as_u64()).unwrap_or(0);
            let open = data
                .get("open")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let high = data
                .get("high")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let low = data
                .get("low")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let close = data
                .get("close")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let volume = data
                .get("volume")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let trades = data.get("trades").and_then(|v| v.as_u64()).unwrap_or(0);
            let kline = Kline {
                start_time: start,
                close_time: end,
                interval: "1m".to_string(),
                open: open.into(),
                close: close.into(),
                high: high.into(),
                low: low.into(),
                volume: volume.into(),
                trades,
                is_closed: true,
                quote_volume: "0".into(),
                taker_buy_base_volume: "0".into(),
                taker_buy_quote_volume: "0".into(),
            };
            let evt = KlineEvent {
                event_time: end,
                symbol: symbol.to_string(),
                kline,
            };
            Ok(StreamMessage {
                stream,
                data: Event::Kline(evt),
            })
        }
        _ => Err(anyhow!("unknown topic")),
    }
}
