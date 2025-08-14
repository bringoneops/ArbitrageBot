use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use core::events::StreamMessage;
use core::rate_limit::TokenBucket;
use core::OrderBook;
use dashmap::DashMap;
use futures::SinkExt;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use super::ExchangeAdapter;

/// Configuration for a single CoinEx exchange endpoint.
pub struct CoinexConfig {
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
}

/// All CoinEx exchanges supported by this adapter.
pub const COINEX_EXCHANGES: &[CoinexConfig] = &[CoinexConfig {
    name: "CoinEx Spot",
    info_url: "https://api.coinex.com/v1/market/info",
    ws_base: "wss://socket.coinex.com/v2/spot",
}];

/// Retrieve all trading symbols for CoinEx using its `market/info` endpoint.
pub async fn fetch_symbols(info_url: &str) -> Result<Vec<String>> {
    let resp = Client::new()
        .get(info_url)
        .send()
        .await?
        .error_for_status()?;
    let data: Value = resp.json().await?;
    let symbols = data
        .get("data")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("missing data object"))?;
    let mut result: Vec<String> = symbols.keys().cloned().collect();
    result.sort();
    result.dedup();
    Ok(result)
}

/// Adapter implementing the `ExchangeAdapter` trait for CoinEx.
pub struct CoinexAdapter {
    cfg: &'static CoinexConfig,
    _client: Client,
    chunk_size: usize,
    _task_tx: mpsc::UnboundedSender<JoinHandle<()>>,
    _event_txs: Arc<DashMap<String, mpsc::Sender<StreamMessage<'static>>>>,
    symbols: Vec<String>,
    _books: Arc<DashMap<String, OrderBook>>,
    http_bucket: Arc<TokenBucket>,
    ws_bucket: Arc<TokenBucket>,
}

impl CoinexAdapter {
    pub fn new(
        cfg: &'static CoinexConfig,
        client: Client,
        chunk_size: usize,
        task_tx: mpsc::UnboundedSender<JoinHandle<()>>,
        event_txs: Arc<DashMap<String, mpsc::Sender<StreamMessage<'static>>>>,
        symbols: Vec<String>,
    ) -> Self {
        let global_cfg = core::config::get();
        Self {
            cfg,
            _client: client,
            chunk_size,
            _task_tx: task_tx,
            _event_txs: event_txs,
            symbols,
            _books: Arc::new(DashMap::new()),
            http_bucket: Arc::new(TokenBucket::new(
                global_cfg.http_burst,
                global_cfg.http_refill_per_sec,
                Duration::from_secs(1),
            )),
            ws_bucket: Arc::new(TokenBucket::new(
                global_cfg.ws_burst,
                global_cfg.ws_refill_per_sec,
                Duration::from_secs(1),
            )),
        }
    }
}

#[async_trait]
impl ExchangeAdapter for CoinexAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        let symbol_refs: Vec<&str> = self.symbols.iter().map(|s| s.as_str()).collect();
        for chunk in symbol_refs.chunks(self.chunk_size) {
            self.ws_bucket.acquire(1).await;
            let (mut ws, _) = connect_async(self.cfg.ws_base).await?;
            let params: Vec<String> = chunk.iter().map(|s| format!("depth.step0.{}", s)).collect();
            let sub = serde_json::json!({
                "id": rand::random::<u32>(),
                "method": "subscribe",
                "params": params,
            });
            ws.send(Message::Text(sub.to_string())).await.ok();
        }
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        self.subscribe().await
    }

    async fn heartbeat(&mut self) -> Result<()> {
        Ok(())
    }

    async fn auth(&mut self) -> Result<()> {
        Ok(())
    }

    async fn backfill(&mut self) -> Result<()> {
        // Placeholder for potential REST snapshot respecting HTTP rate limits
        self.http_bucket.acquire(1).await;
        Ok(())
    }
}
