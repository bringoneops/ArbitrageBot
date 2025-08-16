use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use futures::{future::BoxFuture, SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::{Arc, Once};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info};

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};

/// Configuration for a single Gate.io exchange endpoint.
pub struct GateioConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
}

/// All Gate.io exchanges supported by this adapter.
pub const GATEIO_EXCHANGES: &[GateioConfig] = &[
    GateioConfig {
        id: "gateio_spot",
        name: "Gate.io Spot",
        info_url: "https://api.gateio.ws/api/v4/spot/currency_pairs",
        ws_base: "wss://ws.gate.com/v3/",
    },
    GateioConfig {
        id: "gateio_futures",
        name: "Gate.io Futures",
        info_url: "https://api.gateio.ws/api/v4/futures/{settle}/contracts",
        ws_base: "wss://ws.gate.com/v3/",
    },
];

/// Retrieve all trading symbols for Gate.io across market types using the provided HTTP client.
///
/// The caller must supply a reusable [`reqwest::Client`] instance for efficiency.
pub async fn fetch_symbols(client: &Client, cfg: &GateioConfig) -> Result<Vec<String>> {
    let mut result: Vec<String> = Vec::new();
    let limit = 100u32;

    if cfg.info_url.contains("{settle}") {
        // Futures/perpetual contracts, iterate through settle currencies
        let settles = ["usdt", "btc", "usd"];
        for settle in settles.iter() {
            let mut page = 1u32;
            loop {
                let url = cfg.info_url.replace("{settle}", settle);
                let resp = client
                    .get(&url)
                    .query(&[("limit", limit), ("page", page)])
                    .send()
                    .await?
                    .error_for_status()?;
                let data: Value = resp.json().await?;
                let arr = data.as_array().ok_or_else(|| anyhow!("expected array"))?;
                if arr.is_empty() {
                    break;
                }
                for item in arr {
                    let trading = item
                        .get("status")
                        .and_then(|v| v.as_str())
                        .map(|s| s.eq_ignore_ascii_case("trading"))
                        .unwrap_or(false)
                        && !item
                            .get("in_delisting")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                    if trading {
                        if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                            result.push(name.to_string());
                        }
                    }
                }
                if arr.len() < limit as usize {
                    break;
                }
                page += 1;
            }
        }
    } else {
        // Spot symbols
        let mut page = 1u32;
        loop {
            let resp = client
                .get(cfg.info_url)
                .query(&[("limit", limit), ("page", page)])
                .send()
                .await?
                .error_for_status()?;
            let data: Value = resp.json().await?;
            let arr = data.as_array().ok_or_else(|| anyhow!("expected array"))?;
            if arr.is_empty() {
                break;
            }
            for item in arr {
                let tradable = item
                    .get("trade_status")
                    .and_then(|v| v.as_str())
                    .map(|s| s.eq_ignore_ascii_case("tradable"))
                    .unwrap_or(false);
                if tradable {
                    if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                        result.push(id.to_string());
                    }
                }
            }
            if arr.len() < limit as usize {
                break;
            }
            page += 1;
        }
    }

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
                                symbols = fetch_symbols(&client, cfg).await?;
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

/// Minimal Gate.io adapter implementing [`ExchangeAdapter`].
pub struct GateioAdapter {
    cfg: &'static GateioConfig,
    _client: Client,
    _chunk_size: usize,
    symbols: Vec<String>,
}

impl GateioAdapter {
    pub fn new(
        cfg: &'static GateioConfig,
        client: Client,
        chunk_size: usize,
        symbols: Vec<String>,
    ) -> Self {
        Self {
            cfg,
            _client: client,
            _chunk_size: chunk_size,
            symbols,
        }
    }
}

#[async_trait]
impl ExchangeAdapter for GateioAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        let symbols = self.symbols.clone();
        loop {
            match connect_async(self.cfg.ws_base).await {
                Ok((ws_stream, _)) => {
                    let (write, mut read) = ws_stream.split();
                    let write = Arc::new(Mutex::new(write));

                    info!(
                        "connected to {}: trades={}, depth={}, kline={}",
                        self.cfg.name,
                        symbols.len(),
                        symbols.len(),
                        symbols.len()
                    );

                    {
                        let mut w = write.lock().await;
                        w.send(Message::Text(
                            json!({
                                "id": 1,
                                "method": "trades.subscribe",
                                "params": symbols.clone()
                            })
                            .to_string(),
                        ))
                        .await?;
                        w.send(Message::Text(
                            json!({
                                "id": 2,
                                "method": "depth.subscribe",
                                "params": symbols.clone()
                            })
                            .to_string(),
                        ))
                        .await?;
                        let kline_params: Vec<Value> =
                            symbols.iter().map(|s| json!([s, "1m"])).collect();
                        w.send(Message::Text(
                            json!({
                                "id": 3,
                                "method": "kline.subscribe",
                                "params": kline_params
                            })
                            .to_string(),
                        ))
                        .await?;
                    }

                    let ping_writer = write.clone();
                    let heartbeat = tokio::spawn(async move {
                        let mut intv = interval(Duration::from_secs(30));
                        loop {
                            intv.tick().await;
                            if ping_writer
                                .lock()
                                .await
                                .send(Message::Text(json!({"method": "server.ping"}).to_string()))
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
                                    if v.get("method")
                                        .and_then(|m| m.as_str())
                                        == Some("kline.update")
                                    {
                                        if let Some(params) =
                                            v.get("params").and_then(|p| p.as_array())
                                        {
                                            if let Some(symbol) =
                                                params.get(0).and_then(|s| s.as_str())
                                            {
                                                debug!("kline update for {}", symbol);
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(Message::Ping(p)) => {
                                let _ = write.lock().await.send(Message::Pong(p)).await;
                            }
                            Ok(Message::Close(_)) => break,
                            Err(e) => {
                                error!("websocket error: {}", e);
                                break;
                            }
                            _ => {}
                        }
                    }

                    heartbeat.abort();
                }
                Err(e) => {
                    error!("connect error: {}", e);
                }
            }
            sleep(Duration::from_secs(5)).await;
            info!("reconnecting to {}", self.cfg.name);
        }
    }

    async fn run(&mut self) -> Result<()> {
        self.backfill().await?;
        self.subscribe().await
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
