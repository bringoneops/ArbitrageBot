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
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Once;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info};

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
            let handle = tokio::spawn(async move {
                if let Err(e) = LatokenAdapter::run_symbol(url, symbol, shutdown).await {
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

impl LatokenAdapter {
    async fn run_symbol(url: String, symbol: String, shutdown: Arc<AtomicBool>) -> Result<()> {
        loop {
            let (ws_stream, _) = connect_async(&url).await?;
            info!("latoken connected: {}", symbol);
            let (write, mut read) = ws_stream.split();
            let write = Arc::new(Mutex::new(write));

            let subs = vec![
                json!({"type": "subscribe", "symbol": symbol, "topic": "trade"}),
                json!({"type": "subscribe", "symbol": symbol, "topic": "depth"}),
                json!({"type": "subscribe", "symbol": symbol, "topic": "kline"}),
            ];
            for sub in subs {
                if write
                    .lock()
                    .await
                    .send(Message::Text(sub.to_string()))
                    .await
                    .is_err()
                {
                    break;
                }
            }

            let ping_writer = write.clone();
            let ping_shutdown = shutdown.clone();
            tokio::spawn(async move {
                let mut intv = interval(Duration::from_secs(30));
                while !ping_shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                    intv.tick().await;
                    if ping_writer
                        .lock()
                        .await
                        .send(Message::Ping(Vec::new()))
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
                        if let Ok(v) = serde_json::from_str::<Value>(&text) {
                            if let Some(ping) = v.get("ping").cloned() {
                                let pong = json!({"pong": ping});
                                let _ = write
                                    .lock()
                                    .await
                                    .send(Message::Text(pong.to_string()))
                                    .await;
                                continue;
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        let _ = write.lock().await.send(Message::Pong(data)).await;
                    }
                    Ok(Message::Pong(_)) => {}
                    Err(e) => {
                        error!("latoken {} error: {}", symbol, e);
                        break;
                    }
                    _ => {}
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
