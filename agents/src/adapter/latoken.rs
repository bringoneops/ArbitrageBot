use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};
use anyhow::Result;
use arb_core as core;
use async_trait::async_trait;
use core::rate_limit::TokenBucket;
use core::{chunk_streams_with_config, stream_config_for_exchange};
use futures::{future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Once;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Duration, Instant, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info};
use core::events::{
    self,
    LatokenDepthEvent,
    LatokenKlineEvent,
    LatokenStreamMessage,
    LatokenTradeEvent,
    StreamMessage as CoreStreamMessage,
};

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
    ws_base: "wss://api.latoken.com/ws",
}];

/// Retrieve all trading symbols for LATOKEN using its ticker endpoint.
pub async fn fetch_symbols(client: &Client, info_url: &str) -> Result<Vec<String>> {
    let resp = client.get(info_url).send().await?.error_for_status()?;
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
                                symbols = fetch_symbols(&client, cfg.info_url).await?;
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

/// Adapter implementing the `ExchangeAdapter` trait for LATOKEN.
pub struct LatokenAdapter {
    cfg: &'static LatokenConfig,
    _client: Client,
    chunk_size: usize,
    symbols: Vec<String>,
    channels: ChannelRegistry,
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
        channels: ChannelRegistry,
    ) -> Self {
        let global_cfg = core::config::get();
        Self {
            cfg,
            _client: client,
            chunk_size,
            symbols,
            channels,
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
        let _chunks = chunk_streams_with_config(&symbol_refs, self.chunk_size, cfg);

        for symbol in self.symbols.clone() {
            let url = self.cfg.ws_base.to_string();
            let shutdown = self.shutdown.clone();
            let channels = self.channels.clone();
            let name = self.cfg.name;
            let handle = tokio::spawn(async move {
                if let Err(e) = LatokenAdapter::run_symbol(url, symbol, name, channels, shutdown).await {
                    error!("latoken stream error: {}", e);
                }
                Ok(())
            });
            self.tasks.push(handle);
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

fn parse_message(text: &str) -> Option<CoreStreamMessage<'static>> {
    let msg: LatokenStreamMessage<'_> = serde_json::from_str(text).ok()?;
    let symbol = msg.symbol.clone();
    match msg.topic.as_ref() {
        "trade" => {
            let t: LatokenTradeEvent<'_> = serde_json::from_value(msg.data).ok()?;
            let event = events::TradeEvent {
                event_time: t.timestamp,
                symbol: symbol.clone(),
                trade_id: t.id.unwrap_or(0),
                price: Cow::Owned(t.price.into_owned()),
                quantity: Cow::Owned(t.quantity.into_owned()),
                buyer_order_id: 0,
                seller_order_id: 0,
                trade_time: t.timestamp,
                buyer_is_maker: t.maker.unwrap_or(false),
                best_match: true,
            };
            Some(CoreStreamMessage {
                stream: format!("{}@trade", symbol),
                data: events::Event::Trade(event),
            })
        }
        "depth" | "orderbook" => {
            let d: LatokenDepthEvent<'_> = serde_json::from_value(msg.data).ok()?;
            let bids = d
                .bids
                .into_iter()
                .map(|[p, q]| [Cow::Owned(p.into_owned()), Cow::Owned(q.into_owned())])
                .collect();
            let asks = d
                .asks
                .into_iter()
                .map(|[p, q]| [Cow::Owned(p.into_owned()), Cow::Owned(q.into_owned())])
                .collect();
            let event = events::DepthUpdateEvent {
                event_time: d.timestamp,
                symbol: symbol.clone(),
                first_update_id: d.first_update_id.unwrap_or(0),
                final_update_id: d.final_update_id.unwrap_or(0),
                previous_final_update_id: 0,
                bids,
                asks,
            };
            Some(CoreStreamMessage {
                stream: format!("{}@depth", symbol),
                data: events::Event::DepthUpdate(event),
            })
        }
        "kline" => {
            let k: LatokenKlineEvent<'_> = serde_json::from_value(msg.data).ok()?;
            let event = events::KlineEvent {
                event_time: k.timestamp,
                symbol: symbol.clone(),
                kline: events::Kline {
                    start_time: k.timestamp,
                    close_time: k.timestamp,
                    interval: "1m".to_string(),
                    open: Cow::Owned(k.open.into_owned()),
                    close: Cow::Owned(k.close.into_owned()),
                    high: Cow::Owned(k.high.into_owned()),
                    low: Cow::Owned(k.low.into_owned()),
                    volume: Cow::Owned(k.volume.into_owned()),
                    trades: 0,
                    is_closed: true,
                    quote_volume: Cow::Owned("0".to_string()),
                    taker_buy_base_volume: Cow::Owned("0".to_string()),
                    taker_buy_quote_volume: Cow::Owned("0".to_string()),
                },
            };
            Some(CoreStreamMessage {
                stream: format!("{}@kline", symbol),
                data: events::Event::Kline(event),
            })
        }
        _ => None,
    }
}

impl LatokenAdapter {
    async fn run_symbol(
        url: String,
        symbol: String,
        name: &'static str,
        channels: ChannelRegistry,
        shutdown: Arc<AtomicBool>,
    ) -> Result<()> {
        let key = format!("{}:{}", name, symbol);
        loop {
            let (ws_stream, _) = connect_async(&url).await?;
            info!("latoken connected: {}", symbol);
            let (mut write, mut read) = ws_stream.split();

            let subs = vec![
                json!({"type": "subscribe", "symbol": symbol.clone(), "topic": "trade"}),
                json!({"type": "subscribe", "symbol": symbol.clone(), "topic": "depth"}),
                json!({"type": "subscribe", "symbol": symbol.clone(), "topic": "kline"}),
            ];
            for sub in subs {
                if write.send(Message::Text(sub.to_string())).await.is_err() {
                    break;
                }
            }

            let (tx, _) = channels.get_or_create(&key);
            let mut ping_interval = interval(Duration::from_secs(30));
            ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut last_pong = Instant::now();

            loop {
                tokio::select! {
                    _ = ping_interval.tick() => {
                        if write.send(Message::Ping(Vec::new())).await.is_err() {
                            break;
                        }
                        if last_pong.elapsed() > Duration::from_secs(60) {
                            let _ = write.close().await;
                            break;
                        }
                    }
                    msg = read.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                if let Some(event) = parse_message(&text) {
                                    let _ = tx.send(event);
                                } else if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                    if let Some(ping) = v.get("ping").cloned() {
                                        let pong = json!({"pong": ping});
                                        let _ = write.send(Message::Text(pong.to_string())).await;
                                    } else if v.get("pong").is_some() {
                                        last_pong = Instant::now();
                                    }
                                }
                            }
                            Some(Ok(Message::Ping(data))) => {
                                let _ = write.send(Message::Pong(data)).await;
                            }
                            Some(Ok(Message::Pong(_))) => {
                                last_pong = Instant::now();
                            }
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                error!("latoken {} error: {}", symbol, e);
                                break;
                            }
                            None => break,
                        }
                    }
                }

                if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            }

            info!("latoken disconnected: {}", symbol);

            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            sleep(Duration::from_secs(5)).await;
        }
        Ok(())
    }
}
