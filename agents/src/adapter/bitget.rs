use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use core::events::StreamMessage;
use futures::{future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Once,
};
use tokio::sync::mpsc;
use tokio::{
    signal,
    task::JoinHandle,
    time::{sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::error;

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};

/// Configuration for the Bitget exchange.
pub struct BitgetConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub ws_base: &'static str,
}

/// Supported Bitget exchange endpoints.
pub const BITGET_EXCHANGES: &[BitgetConfig] = &[BitgetConfig {
    id: "bitget",
    name: "Bitget",
    ws_base: "wss://ws.bitget.com/spot/v1/stream",
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
    chunk_size: usize,
    symbols: Vec<String>,
    tasks: Vec<JoinHandle<Result<()>>>,
    shutdown: Arc<AtomicBool>,
}

impl BitgetAdapter {
    pub fn new(
        cfg: &'static BitgetConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
    ) -> Self {
        Self {
            cfg,
            _client: client,
            chunk_size,
            symbols,
            tasks: Vec::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl super::ExchangeAdapter for BitgetAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        let symbol_refs: Vec<&str> = self.symbols.iter().map(|s| s.as_str()).collect();
        let cfg = core::stream_config_for_exchange(self.cfg.name);
        let chunks = core::chunk_streams_with_config(&symbol_refs, self.chunk_size, cfg);

        for chunk in chunks {
            let symbols = chunk.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            let ws_url = self.cfg.ws_base.to_string();
            let shutdown = self.shutdown.clone();

            let handle = tokio::spawn(async move {
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    match connect_async(&ws_url).await {
                        Ok((mut ws, _)) => {
                            let args: Vec<_> = symbols
                                .iter()
                                .map(|s| serde_json::json!({"channel":"ticker","instId":s}))
                                .collect();
                            let sub = serde_json::json!({"op":"subscribe","args": args});
                            let _ = ws.send(Message::Text(sub.to_string())).await;
                            while let Some(msg) = ws.next().await {
                                match msg {
                                    Ok(Message::Ping(p)) => {
                                        let _ = ws.send(Message::Pong(p)).await;
                                    }
                                    Ok(Message::Close(_)) | Err(_) => break,
                                    _ => {}
                                }
                            }
                        }
                        Err(_) => {}
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
