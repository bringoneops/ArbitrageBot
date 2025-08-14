use anyhow::{anyhow, Result};
use arb_core as core;
use async_trait::async_trait;
use core::rate_limit::TokenBucket;
use core::{chunk_streams_with_config, stream_config_for_exchange, OrderBook};
use dashmap::DashMap;
use futures::SinkExt;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use super::ExchangeAdapter;

/// Configuration for a single MEXC exchange endpoint.
pub struct MexcConfig {
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
}

/// All MEXC exchanges supported by this adapter.
pub const MEXC_EXCHANGES: &[MexcConfig] = &[MexcConfig {
    name: "MEXC Spot",
    info_url: "https://api.mexc.com/api/v3/exchangeInfo",
    ws_base: "wss://wbs.mexc.com/ws",
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

/// Adapter implementing the `ExchangeAdapter` trait for MEXC.
pub struct MexcAdapter {
    cfg: &'static MexcConfig,
    _client: Client,
    chunk_size: usize,
    _task_tx: mpsc::UnboundedSender<JoinHandle<()>>,
    symbols: Vec<String>,
    _books: Arc<DashMap<String, OrderBook>>,
    http_bucket: Arc<TokenBucket>,
    ws_bucket: Arc<TokenBucket>,
}

impl MexcAdapter {
    pub fn new(
        cfg: &'static MexcConfig,
        client: Client,
        chunk_size: usize,
        task_tx: mpsc::UnboundedSender<JoinHandle<()>>,
        symbols: Vec<String>,
    ) -> Self {
        let global_cfg = core::config::get();
        Self {
            cfg,
            _client: client,
            chunk_size,
            _task_tx: task_tx,
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
            self.ws_bucket.acquire(1).await;
            let (mut ws, _) = connect_async(self.cfg.ws_base).await?;
            let params: Vec<String> = chunk
                .iter()
                .map(|s| format!("spot@public.depth.v3.api@{}@5", s))
                .collect();
            let sub = serde_json::json!({
                "method": "SUBSCRIPTION",
                "params": params,
                "id": rand::random::<u32>(),
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
        // Placeholder for potential REST snapshot calls respecting HTTP rate limits
        self.http_bucket.acquire(1).await;
        Ok(())
    }
}
