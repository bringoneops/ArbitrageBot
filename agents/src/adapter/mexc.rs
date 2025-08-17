use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use core::rate_limit::TokenBucket;
use core::{chunk_streams_with_config, stream_config_for_exchange};
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
    time::{interval, sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};
use futures::future::BoxFuture;
use std::sync::Once;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Configuration for a single MEXC exchange endpoint.
pub struct MexcConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
}

/// All MEXC exchanges supported by this adapter.
pub const MEXC_EXCHANGES: &[MexcConfig] = &[MexcConfig {
    id: "mexc_spot",
    name: "MEXC Spot",
    info_url: "https://api.mexc.com/api/v3/exchangeInfo",
    ws_base: "wss://wbs-api.mexc.com/ws",
}];

/// Retrieve all trading symbols for MEXC using its `exchangeInfo` endpoint.
pub async fn fetch_symbols(info_url: &str) -> Result<Vec<String>> {
    let resp = Client::new()
        .get(info_url)
        .send()
        .await?
        .error_for_status()?;
    let data: Value = resp.json().await?;
    let symbols = data
        .get("symbols")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing symbols array"))?;

    let mut result: Vec<String> = symbols
        .iter()
        .filter_map(|s| {
            let status_ok = s
                .get("status")
                .and_then(|v| v.as_str())
                .map(|st| {
                    st == "1"
                        || st.eq_ignore_ascii_case("ENABLED")
                        || st.eq_ignore_ascii_case("TRADING")
                })
                .unwrap_or(false);
            if status_ok {
                s.get("symbol")
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
        for exch in MEXC_EXCHANGES {
            let cfg_ref: &'static MexcConfig = exch;
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

                            let adapter = MexcAdapter::new(
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

/// Adapter implementing the `ExchangeAdapter` trait for MEXC.
pub struct MexcAdapter {
    cfg: &'static MexcConfig,
    _client: Client,
    chunk_size: usize,
    symbols: Vec<String>,
    http_bucket: Arc<TokenBucket>,
    ws_bucket: Arc<TokenBucket>,
    tasks: Vec<JoinHandle<Result<()>>>,
    shutdown: Arc<AtomicBool>,
}

impl MexcAdapter {
    pub fn new(
        cfg: &'static MexcConfig,
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
impl ExchangeAdapter for MexcAdapter {
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
                    if ws_bucket.acquire(1).await.is_err() {
                        break;
                    }
                    match connect_async(&ws_url).await {
                        Ok((mut ws, _)) => {
                            let params: Vec<String> = symbols
                                .iter()
                                .flat_map(|s| {
                                    vec![
                                        format!("spot@public.depth.v3.api@{}@5", s),
                                        format!("spot@public.deals.v3.api@{}", s),
                                        format!("spot@public.kline.v3.api@{}@Min1", s),
                                    ]
                                })
                                .collect();
                            let sub = serde_json::json!({
                                "method": "SUBSCRIPTION",
                                "params": params,
                                "id": rand::random::<u32>(),
                            });
                            if ws.send(Message::Text(sub.to_string())).await.is_ok() {
                                info!("mexc subscribed {} topics", params.len());
                                let mut hb = interval(Duration::from_secs(20));
                                loop {
                                    tokio::select! {
                                        msg = ws.next() => {
                                            match msg {
                                                Some(Ok(Message::Ping(p))) => {
                                                    if ws.send(Message::Pong(p)).await.is_err() {
                                                        break;
                                                    }
                                                },
                                                Some(Ok(Message::Pong(_))) => {},
                                                Some(Ok(Message::Close(_))) | None => { break; },
                                                Some(Ok(_)) => {},
                                                Some(Err(e)) => { warn!("mexc ws error: {}", e); break; },
                                            }
                                        }
                                        _ = hb.tick() => {
                                            if ws.send(Message::Ping(Vec::new())).await.is_err() {
                                                break;
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
                        }
                        Err(e) => {
                            warn!("mexc connect error: {}", e);
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
        // Placeholder for potential REST snapshot calls respecting HTTP rate limits
        self.http_bucket.acquire(1).await?;
        Ok(())
    }
}
