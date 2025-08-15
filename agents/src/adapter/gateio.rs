use anyhow::{anyhow, Result};
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

/// Configuration for a single Gate.io exchange endpoint.
pub struct GateioConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
}

/// All Gate.io exchanges supported by this adapter.
pub const GATEIO_EXCHANGES: &[GateioConfig] = &[GateioConfig {
    id: "gateio_spot",
    name: "Gate.io Spot",
    info_url: "https://api.gateio.ws/api/v4/spot/currency_pairs",
    ws_base: "wss://api.gateio.ws/ws/v4/",
}];

/// Retrieve all trading symbols for Gate.io using its REST endpoint.
pub async fn fetch_symbols(info_url: &str) -> Result<Vec<String>> {
    let resp = Client::new()
        .get(info_url)
        .send()
        .await?
        .error_for_status()?;
    let data: Value = resp.json().await?;
    let arr = data
        .as_array()
        .ok_or_else(|| anyhow!("expected array"))?;

    let mut result: Vec<String> = arr
        .iter()
        .filter_map(|s| {
            let tradable = s
                .get("trade_status")
                .and_then(|v| v.as_str())
                .map(|st| st.eq_ignore_ascii_case("tradable"))
                .unwrap_or(false);
            if tradable {
                s.get("id")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string())
            } else {
                None
            }
        })
        .collect();
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

                            let adapter = GateioAdapter::new(
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

/// Adapter implementing the `ExchangeAdapter` trait for Gate.io.
pub struct GateioAdapter {
    cfg: &'static GateioConfig,
    _client: Client,
    chunk_size: usize,
    symbols: Vec<String>,
    _books: Arc<DashMap<String, OrderBook>>,
    http_bucket: Arc<TokenBucket>,
    ws_bucket: Arc<TokenBucket>,
    tasks: Vec<JoinHandle<Result<()>>>,
    shutdown: Arc<AtomicBool>,
}

impl GateioAdapter {
    pub fn new(
        cfg: &'static GateioConfig,
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
impl ExchangeAdapter for GateioAdapter {
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
                        Ok((mut ws, _)) => {
                            for symbol in &symbols {
                                let sub = serde_json::json!({
                                    "channel": "spot.order_book_update",
                                    "event": "subscribe",
                                    "payload": [symbol, "20", "100ms"],
                                });
                                let _ = ws.send(Message::Text(sub.to_string())).await;
                            }
                            loop {
                                tokio::select! {
                                    msg = ws.next() => {
                                        match msg {
                                            Some(Ok(Message::Ping(p))) => {
                                                ws.send(Message::Pong(p)).await.map_err(|e| {
                                                    tracing::error!("gateio ws pong error: {}", e);
                                                    e
                                                })?;
                                            },
                                            Some(Ok(Message::Close(_))) | None => { break; },
                                            Some(Ok(_)) => {},
                                            Some(Err(e)) => { tracing::warn!("gateio ws error: {}", e); break; },
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
                            tracing::warn!("gateio connect error: {}", e);
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
        self.http_bucket.acquire(1).await;
        Ok(())
    }
}

