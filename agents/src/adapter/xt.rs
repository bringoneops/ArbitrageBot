use anyhow::Result;
use arb_core as core;
use async_trait::async_trait;
use core::rate_limit::TokenBucket;
use core::{chunk_streams_with_config, stream_config_for_exchange, OrderBook};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
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

use core::events::{
    BookTickerEvent, DepthUpdateEvent, Event, Kline, KlineEvent, StreamMessage, TradeEvent,
    XtEvent, XtStreamMessage,
};

pub const SPOT_SYMBOL_URL: &str = "https://api.xt.com/data/api/v4/public/symbol";
pub const FUTURES_SYMBOL_URL: &str = "https://futures.xt.com/api/v4/public/symbol";

/// Configuration for a single XT exchange endpoint.
pub struct XtConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub ws_base: &'static str,
}

/// All XT exchanges supported by this adapter.
pub const XT_EXCHANGES: &[XtConfig] = &[
    XtConfig {
        id: "xt_spot",
        name: "XT Spot",
        ws_base: "wss://stream.xt.com/public",
    },
    XtConfig {
        id: "xt_futures",
        name: "XT Futures",
        ws_base: "wss://stream.xt.com/futures/public",
    },
];

/// Recursively extract symbol strings from arbitrary JSON structures.
fn extract_symbols(val: &Value, out: &mut Vec<String>) {
    match val {
        Value::Array(arr) => {
            for v in arr {
                extract_symbols(v, out);
            }
        }
        Value::Object(map) => {
            for (k, v) in map {
                if (k == "symbol" || k == "s" || k == "id" || k == "pair") && v.is_string() {
                    if let Some(s) = v.as_str() {
                        out.push(s.to_string());
                    }
                }
                extract_symbols(v, out);
            }
        }
        _ => {}
    }
}

/// Retrieve all trading symbols from both the spot and futures REST endpoints.
pub async fn fetch_symbols() -> Result<Vec<String>> {
    let client = Client::new();
    let mut symbols = Vec::new();
    for url in [SPOT_SYMBOL_URL, FUTURES_SYMBOL_URL] {
        if let Ok(resp) = client.get(url).send().await {
            if let Ok(resp) = resp.error_for_status() {
                let data: Value = resp.json().await?;
                extract_symbols(&data, &mut symbols);
            }
        }
    }
    symbols.sort();
    symbols.dedup();
    Ok(symbols)
}

static REGISTER: Once = Once::new();

pub fn register() {
    REGISTER.call_once(|| {
        for exch in XT_EXCHANGES {
            let cfg_ref: &'static XtConfig = exch;
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
                                let key = format!("{name}:{symbol}", name = cfg.name, symbol = symbol);
                                let (_, rx) = channels.get_or_create(&key);
                                if let Some(rx) = rx {
                                    receivers.push(rx);
                                }
                            }

                            let adapter = XtAdapter::new(
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

/// Adapter implementing the `ExchangeAdapter` trait for XT.
pub struct XtAdapter {
    cfg: &'static XtConfig,
    _client: Client,
    chunk_size: usize,
    symbols: Vec<String>,
    _books: Arc<DashMap<String, OrderBook>>,
    http_bucket: Arc<TokenBucket>,
    ws_bucket: Arc<TokenBucket>,
    tasks: Vec<JoinHandle<Result<()>>>,
    shutdown: Arc<AtomicBool>,
    channels: ChannelRegistry,
}

impl XtAdapter {
    pub fn new(
        cfg: &'static XtConfig,
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
            tasks: Vec::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            channels,
        }
    }
}

#[async_trait]
impl ExchangeAdapter for XtAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        let symbol_refs: Vec<&str> = self.symbols.iter().map(|s| s.as_str()).collect();
        let cfg = stream_config_for_exchange(self.cfg.name);
        let chunks = chunk_streams_with_config(&symbol_refs, self.chunk_size, cfg);

        for chunk in chunks {
            let topics = chunk.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            tracing::debug!(?topics, "xt subscribing to topics");
            let topic_count = topics.len();
            let ws_url = self.cfg.ws_base.to_string();
            let ws_bucket = self.ws_bucket.clone();
            let shutdown = self.shutdown.clone();
            let channels = self.channels.clone();
            let exchange = self.cfg.name.to_string();

            let handle = tokio::spawn(async move {
                let topics = topics;
                let ws_url = ws_url;
                // build symbol -> sender map
                let mut senders: HashMap<String, StreamSender> = HashMap::new();
                for t in &topics {
                    if let Some((sym, _)) = t.split_once('@') {
                        let key = format!("{exchange}:{sym}");
                        if let Some(tx) = channels.get(&key) {
                            senders.insert(sym.to_string(), tx);
                        }
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
                            let sub = serde_json::json!({
                                "op": "sub",
                                "topics": topics.clone(),
                            });
                            let _ = ws.send(Message::Text(sub.to_string())).await;
                            tracing::info!("subscribed {} topics to {}", topic_count, ws_url);

                            let mut ping_intv = tokio::time::interval(Duration::from_secs(30));
                            loop {
                                tokio::select! {
                                    _ = ping_intv.tick() => {
                                        if let Err(e) = ws.send(Message::Ping(Vec::new())).await {
                                            tracing::warn!("xt ws ping error: {}", e);
                                            break;
                                        }
                                    }
                                    msg = ws.next() => {
                                        match msg {
                                            Some(Ok(Message::Ping(p))) => {
                                                ws.send(Message::Pong(p)).await.map_err(|e| {
                                                    tracing::error!("xt ws pong error: {}", e);
                                                    e
                                                })?;
                                            },
                                            Some(Ok(Message::Text(text))) => {
                                                if let Some(events) = parse_message(&text) {
                                                    for (sym, event) in events {
                                                        if let Some(tx) = senders.get(&sym) {
                                                            let _ = tx.send(event);
                                                        }
                                                    }
                                                }
                                            }
                                            Some(Ok(Message::Close(_))) | None => { break; },
                                            Some(Ok(_)) => {},
                                            Some(Err(e)) => { tracing::warn!("xt ws error: {}", e); break; },
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
                            tracing::warn!("xt connect error: {}", e);
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

/// Parse raw XT websocket text into one or more canonical stream messages.
fn parse_message(text: &str) -> Option<Vec<(String, StreamMessage<'static>)>> {
    let msg: XtStreamMessage<'_> = serde_json::from_str(text).ok()?;
    let topic = msg.topic.as_ref();
    let (symbol, _channel) = topic.split_once('@')?;
    let symbol_string = symbol.to_string();

    match msg.data {
        XtEvent::Trade(trades) => {
            let mut out = Vec::new();
            for t in trades {
                let event = TradeEvent {
                    event_time: t.trade_time,
                    symbol: symbol_string.clone(),
                    trade_id: t.i.unwrap_or(0),
                    price: Cow::Owned(t.price.into_owned()),
                    quantity: Cow::Owned(t.quantity.into_owned()),
                    buyer_order_id: 0,
                    seller_order_id: 0,
                    trade_time: t.trade_time,
                    buyer_is_maker: t.buyer_is_maker.unwrap_or(false),
                    best_match: true,
                };
                out.push((
                    symbol_string.clone(),
                    StreamMessage {
                        stream: format!("{symbol}@trade"),
                        data: Event::Trade(event),
                    },
                ));
            }
            Some(out)
        }
        XtEvent::Depth(d) => {
            let update = DepthUpdateEvent {
                event_time: d.timestamp,
                symbol: symbol_string.clone(),
                first_update_id: 0,
                final_update_id: 0,
                previous_final_update_id: 0,
                bids: d
                    .bids
                    .into_iter()
                    .map(|[p, q]| [Cow::Owned(p.into_owned()), Cow::Owned(q.into_owned())])
                    .collect(),
                asks: d
                    .asks
                    .into_iter()
                    .map(|[p, q]| [Cow::Owned(p.into_owned()), Cow::Owned(q.into_owned())])
                    .collect(),
            };
            Some(vec![(
                symbol_string.clone(),
                StreamMessage {
                    stream: format!("{symbol}@depth"),
                    data: Event::DepthUpdate(update),
                },
            )])
        }
        XtEvent::Kline(k) => {
            let ev = KlineEvent {
                event_time: k.timestamp,
                symbol: symbol_string.clone(),
                kline: Kline {
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
            Some(vec![(
                symbol_string.clone(),
                StreamMessage {
                    stream: format!("{symbol}@kline"),
                    data: Event::Kline(ev),
                },
            )])
        }
        XtEvent::Ticker(t) => {
            let ev = BookTickerEvent {
                update_id: t.timestamp,
                symbol: symbol_string.clone(),
                best_bid_price: Cow::Owned(t.bid_price.into_owned()),
                best_bid_qty: Cow::Owned(t.bid_qty.into_owned()),
                best_ask_price: Cow::Owned(t.ask_price.into_owned()),
                best_ask_qty: Cow::Owned(t.ask_qty.into_owned()),
            };
            Some(vec![(
                symbol_string.clone(),
                StreamMessage {
                    stream: format!("{symbol}@ticker"),
                    data: Event::BookTicker(ev),
                },
            )])
        }
    }
}
