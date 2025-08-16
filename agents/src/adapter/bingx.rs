use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use core::{chunk_streams_with_config, stream_config_for_exchange};
use core::events::BingxStreamMessage;
use futures::{future, future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::sync::{Arc, Once};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};

#[derive(Clone, Copy)]
pub enum MarketType {
    Spot,
    Swap,
}

pub struct BingxConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
    pub market: MarketType,
}

pub const BINGX_EXCHANGES: &[BingxConfig] = &[
    BingxConfig {
        id: "bingx_spot",
        name: "BingX Spot",
        info_url: "https://open-api.bingx.com/openApi/spot/v1/common/symbols",
        ws_base: "wss://open-api-ws.bingx.com/",
        market: MarketType::Spot,
    },
    BingxConfig {
        id: "bingx_swap",
        name: "BingX Swap",
        info_url: "https://open-api.bingx.com/openApi/swap/v2/market/symbolList",
        ws_base: "wss://open-api-ws.bingx.com/",
        market: MarketType::Swap,
    },
];

async fn fetch_spot_symbols(info_url: &str) -> Result<Vec<String>> {
    let resp = Client::new()
        .get(info_url)
        .send()
        .await?
        .error_for_status()?;
    let data: Value = resp.json().await?;
    let symbols = data
        .get("data")
        .and_then(|d| d.get("symbols"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing symbols array"))?;
    let mut result: Vec<String> = symbols
        .iter()
        .filter_map(|s| {
            if s.get("status").and_then(|v| v.as_i64()).unwrap_or(0) == 0 {
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

async fn fetch_swap_symbols(info_url: &str) -> Result<Vec<String>> {
    let client = Client::new();
    let mut page = 1;
    let mut result = Vec::new();
    loop {
        let url = format!("{}?page={}", info_url, page);
        let resp = client.get(&url).send().await?.error_for_status()?;
        let data: Value = resp.json().await?;
        let arr = data
            .get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("missing data array"))?;
        if arr.is_empty() {
            break;
        }
        for s in arr {
            if s.get("status").and_then(|v| v.as_i64()).unwrap_or(1) == 1 {
                if let Some(sym) = s.get("symbol").and_then(|v| v.as_str()) {
                    result.push(sym.to_string());
                }
            }
        }
        page += 1;
    }
    result.sort();
    result.dedup();
    Ok(result)
}

async fn fetch_symbols(cfg: &BingxConfig) -> Result<Vec<String>> {
    match cfg.market {
        MarketType::Spot => fetch_spot_symbols(cfg.info_url).await,
        MarketType::Swap => fetch_swap_symbols(cfg.info_url).await,
    }
}

static REGISTER: Once = Once::new();

pub fn register() {
    REGISTER.call_once(|| {
        for exch in BINGX_EXCHANGES {
            let cfg_ref: &'static BingxConfig = exch;
            registry::register_adapter(
                cfg_ref.id,
                Arc::new(
                    move |global_cfg: &'static core::config::Config,
                          exchange_cfg: &core::config::ExchangeConfig,
                          client: Client,
                          task_set: TaskSet,
                          channels: ChannelRegistry,
                          _tls: Arc<rustls::ClientConfig>|
                          -> BoxFuture<
                        'static,
                        Result<Vec<mpsc::Receiver<core::events::StreamMessage<'static>>>>,
                    > {
                        let cfg = cfg_ref;
                        let initial_symbols = exchange_cfg.symbols.clone();
                        Box::pin(async move {
                            let mut symbols = initial_symbols;
                            if symbols.is_empty() {
                                symbols = fetch_symbols(cfg).await?;
                            }
                            let mut receivers = Vec::new();
                            for symbol in &symbols {
                                let key = format!("{}:{}", cfg.name, symbol);
                                let (_, rx) = channels.get_or_create(&key);
                                if let Some(rx) = rx {
                                    receivers.push(rx);
                                }
                            }
                            let adapter = BingxAdapter::new(
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

pub struct BingxAdapter {
    cfg: &'static BingxConfig,
    _client: Client,
    chunk_size: usize,
    symbols: Vec<String>,
}

impl BingxAdapter {
    pub fn new(
        cfg: &'static BingxConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
    ) -> Self {
        Self {
            cfg,
            _client: client,
            chunk_size,
            symbols,
        }
    }
}

#[async_trait]
impl ExchangeAdapter for BingxAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        let symbol_refs: Vec<&str> = self.symbols.iter().map(|s| s.as_str()).collect();
        let cfg = stream_config_for_exchange(self.cfg.name);
        let chunks = chunk_streams_with_config(&symbol_refs, self.chunk_size, cfg);

        for chunk in chunks {
            let ws_url = self.cfg.ws_base.to_string();
            tokio::spawn(async move {
                loop {
                    match connect_async(&ws_url).await {
                        Ok((mut ws, _)) => {
                            let sub = serde_json::json!({
                                "op": "subscribe",
                                "args": chunk,
                            });
                            if ws.send(Message::Text(sub.to_string())).await.is_ok() {
                                info!("bingx subscribed {} topics", chunk.len());
                            }

                            let mut hb = interval(Duration::from_secs(20));
                            loop {
                                tokio::select! {
                                    msg = ws.next() => {
                                        match msg {
                                            Some(Ok(Message::Ping(p))) => {
                                                if ws.send(Message::Pong(p)).await.is_err() {
                                                    break;
                                                }
                                            }
                                            Some(Ok(Message::Pong(_))) => {}
                                            Some(Ok(Message::Text(text))) => {
                                                if let Ok(msg) = serde_json::from_str::<BingxStreamMessage>(&text) {
                                                    match msg {
                                                        BingxStreamMessage::Trade(_) => {
                                                            info!("bingx trade event: {}", text);
                                                        }
                                                        BingxStreamMessage::DepthUpdate(_) => {
                                                            info!("bingx depth event: {}", text);
                                                        }
                                                        BingxStreamMessage::Unknown => {}
                                                    }
                                                } else if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                                    if let Some(ping) = v.get("ping").cloned() {
                                                        let pong = serde_json::json!({"pong": ping});
                                                        if ws
                                                            .send(Message::Text(pong.to_string()))
                                                            .await
                                                            .is_err()
                                                        {
                                                            break;
                                                        }
                                                    } else if v
                                                        .get("e")
                                                        .and_then(|e| e.as_str())
                                                        == Some("kline")
                                                        || v.get("kline").is_some()
                                                    {
                                                        info!("bingx received kline event: {}", text);
                                                    }
                                                }
                                            }
                                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                    _ = hb.tick() => {
                                        if ws.send(Message::Ping(Vec::new())).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                            info!("bingx websocket disconnected, reconnecting");
                        }
                        Err(e) => warn!("bingx connect error: {}", e),
                    }
                    sleep(Duration::from_secs(5)).await;
                }
            });
        }

        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        self.subscribe().await?;
        future::pending::<()>().await;
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
