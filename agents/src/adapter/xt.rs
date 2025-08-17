use anyhow::Result;
use arb_core as core;
use async_trait::async_trait;
use core::rate_limit::TokenBucket;
use core::{chunk_streams_with_config, stream_config_for_exchange, OrderBook};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
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
use crate::{registry, ChannelRegistry, TaskSet};
use futures::future::BoxFuture;
use std::sync::Once;
use tokio::sync::mpsc;
use tracing::error;

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
                                let key = format!("{}:{}", cfg.name, symbol);
                                let (_, rx) = channels.get_or_create(&key);
                                if let Some(rx) = rx {
                                    receivers.push(rx);
                                }
                            }

                            let adapter =
                                XtAdapter::new(cfg, client.clone(), global_cfg.chunk_size, symbols);

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
}

impl XtAdapter {
    pub fn new(
        cfg: &'static XtConfig,
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

            let handle = tokio::spawn(async move {
                let topics = topics;
                let ws_url = ws_url;
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
