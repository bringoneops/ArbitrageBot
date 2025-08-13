use crate::{
    apply_depth_update, chunk_streams_with_config, fast_forward, handle_stream_event, next_backoff,
    stream_config_for_exchange, ApplyResult, DepthSnapshot, OrderBook,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::{stream, SinkExt, StreamExt};
use rand::Rng;
use reqwest::{Client, StatusCode};
use rustls::ClientConfig;
use simd_json::serde::from_slice;
use std::{collections::HashMap, env, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{mpsc, Mutex},
    task::JoinSet,
    time::{interval, sleep, timeout, Duration, Instant, MissedTickBehavior},
};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async_tls_with_config, tungstenite::protocol::Message,
    Connector, MaybeTlsStream, WebSocketStream,
};
use url::Url;

use crate::events::{Event, StreamMessage};
use crate::rate_limit::TokenBucket;

use super::ExchangeAdapter;

/// Configuration for a single Binance exchange endpoint.
pub struct BinanceConfig {
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
}

/// All Binance exchanges supported by this adapter.
pub const BINANCE_EXCHANGES: &[BinanceConfig] = &[
    BinanceConfig {
        name: "Binance.US Spot",
        info_url: "https://api.binance.us/api/v3/exchangeInfo",
        ws_base: "wss://stream.binance.us:9443/stream?streams=",
    },
    BinanceConfig {
        name: "Binance Global Spot",
        info_url: "https://api.binance.com/api/v3/exchangeInfo",
        ws_base: "wss://stream.binance.com:9443/stream?streams=",
    },
    BinanceConfig {
        name: "Binance Futures",
        info_url: "https://fapi.binance.com/fapi/v1/exchangeInfo",
        ws_base: "wss://fstream.binance.com/stream?streams=",
    },
    BinanceConfig {
        name: "Binance Delivery",
        info_url: "https://dapi.binance.com/dapi/v1/exchangeInfo",
        ws_base: "wss://dstream.binance.com/stream?streams=",
    },
    BinanceConfig {
        name: "Binance Options",
        info_url: "https://vapi.binance.com/vapi/v1/exchangeInfo",
        ws_base: "wss://vstream.binance.com/stream?streams=",
    },
];

/// Adapter implementing the `ExchangeAdapter` trait for Binance.
pub struct BinanceAdapter {
    cfg: &'static BinanceConfig,
    client: Client,
    chunk_size: usize,
    proxy_url: String,
    tasks: Arc<Mutex<JoinSet<()>>>,
    event_tx: mpsc::Sender<StreamMessage<'static>>,
    symbols: Vec<String>,
    orderbooks: Arc<Mutex<HashMap<String, OrderBook>>>,
    tls_config: Arc<ClientConfig>,
    http_bucket: Arc<TokenBucket>,
    ws_bucket: Arc<TokenBucket>,
}

impl BinanceAdapter {
    pub fn new(
        cfg: &'static BinanceConfig,
        client: Client,
        chunk_size: usize,
        proxy_url: String,
        tasks: Arc<Mutex<JoinSet<()>>>,
        event_tx: mpsc::Sender<StreamMessage<'static>>,
        symbols: Vec<String>,
        tls_config: Arc<ClientConfig>,
    ) -> Self {
        Self {
            cfg,
            client,
            chunk_size,
            proxy_url,
            tasks,
            event_tx,
            symbols,
            orderbooks: Arc::new(Mutex::new(HashMap::new())),
            tls_config,
            http_bucket: Arc::new(TokenBucket::new(10, 10, Duration::from_secs(1))),
            ws_bucket: Arc::new(TokenBucket::new(5, 5, Duration::from_secs(1))),
        }
    }
}

#[async_trait]
impl ExchangeAdapter for BinanceAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        let symbol_refs: Vec<&str> = self.symbols.iter().map(|s| s.as_str()).collect();
        let cfg = stream_config_for_exchange(self.cfg.name);
        let chunks = chunk_streams_with_config(&symbol_refs, self.chunk_size, cfg);
        let total_streams: usize = chunks.iter().map(|c| c.len()).sum();
        tracing::info!(
            "\u{1F50C} Total {} streams: {}",
            self.cfg.name,
            total_streams
        );

        let depth_base = self
            .cfg
            .info_url
            .trim_end_matches("exchangeInfo")
            .to_string();
        let tls_cfg = self.tls_config.clone();

        for chunk in chunks {
            let chunk_len = chunk.len();
            let param = chunk.join("/");
            let url = Url::parse(&format!("{}{}", self.cfg.ws_base, param))
                .context("parsing WebSocket URL")?;
            let proxy = self.proxy_url.clone();
            let name = self.cfg.name.to_string();
            let tasks = self.tasks.clone();
            let books = self.orderbooks.clone();
            let event_tx = self.event_tx.clone();
            let client = self.client.clone();
            let depth_base = depth_base.clone();
            let tls_config = tls_cfg.clone();
            let http_bucket = self.http_bucket.clone();
            let ws_bucket = self.ws_bucket.clone();

            tasks.lock().await.spawn(async move {
                let max_backoff_secs = env::var("MAX_BACKOFF_SECS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(64);
                let max_backoff = Duration::from_secs(max_backoff_secs);
                let min_stable = Duration::from_secs(30);
                let mut backoff = Duration::from_secs(1);
                let mut failures: u32 = 0;
                let max_failures = env::var("MAX_FAILURES")
                    .ok()
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(10);
                loop {
                    ws_bucket.acquire(1).await;
                    tracing::info!(
                        "\u{2192} opening WS ({}): {} ({} streams)",
                        name,
                        url,
                        chunk_len
                    );
                    let start = Instant::now();
                    let mut connected = false;
                    let result = if proxy.is_empty() {
                        match connect_async_tls_with_config(
                            url.clone(),
                            None,
                            false,
                            Some(Connector::Rustls(tls_config.clone())),
                        )
                        .await
                        {
                            Ok((ws_stream, _)) => {
                                connected = true;
                                backoff = Duration::from_secs(1);
                                run_ws(
                                    ws_stream,
                                    books.clone(),
                                    event_tx.clone(),
                                    client.clone(),
                                    depth_base.clone(),
                                    name.clone(),
                                    http_bucket.clone(),
                                )
                                .await
                            }
                            Err(e) => Err(e.into()),
                        }
                    } else {
                        match connect_via_socks5(url.clone(), &proxy, tls_config.clone()).await {
                            Ok(ws_stream) => {
                                connected = true;
                                backoff = Duration::from_secs(1);
                                run_ws(
                                    ws_stream,
                                    books.clone(),
                                    event_tx.clone(),
                                    client.clone(),
                                    depth_base.clone(),
                                    name.clone(),
                                    http_bucket.clone(),
                                )
                                .await
                            }
                            Err(e) => Err(e),
                        }
                    };

                    let ok = result.is_ok();
                    if !ok {
                        if let Err(e) = &result {
                            tracing::error!("WS error: {}", e);
                        }
                        failures += 1;
                        if failures >= max_failures {
                            tracing::error!("max WS failures reached, giving up");
                            break;
                        }
                    } else {
                        tracing::warn!("WS stream closed");
                        failures = 0;
                    }

                    let elapsed = start.elapsed();
                    if connected {
                        backoff = next_backoff(backoff, elapsed, ok, max_backoff, min_stable);
                    } else {
                        backoff = std::cmp::min(backoff * 2, max_backoff);
                    }

                    let jitter: f32 = rand::thread_rng().gen_range(0.8..1.2);
                    let sleep_dur = backoff.mul_f32(jitter);
                    tracing::warn!("Reconnecting in {:?}...", sleep_dur);
                    if crate::config::metrics_enabled() {
                        metrics::counter!("md_ws_reconnects_total").increment(1);
                    }
                    sleep(sleep_dur).await;
                }
            });
        }

        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        self.auth().await?;
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
        let symbols = self.symbols.clone();
        let depth_base = self.cfg.info_url.trim_end_matches("exchangeInfo");
        let mut books_map: HashMap<String, OrderBook> = HashMap::new();

        let bucket = self.http_bucket.clone();
        let fetches = stream::iter(symbols.clone())
            .map(|sym| {
                let depth_url = format!("{}depth?symbol={}", depth_base, sym);
                let client = self.client.clone();
                let bucket = bucket.clone();
                async move {
                    let resp = match rate_limited_get(&client, &depth_url, &bucket).await {
                        Ok(resp) => resp,
                        Err(e) => {
                            tracing::warn!(
                                "depth snapshot GET failed for {} ({}): {}",
                                sym,
                                depth_url,
                                e
                            );
                            return None;
                        }
                    };

                    if let Ok(snapshot) = resp.json::<DepthSnapshot>().await {
                        Some((sym, snapshot.into()))
                    } else {
                        None
                    }
                }
            })
            .buffer_unordered(10);

        futures::pin_mut!(fetches);
        while let Some(result) = fetches.next().await {
            if let Some((sym, book)) = result {
                books_map.insert(sym, book);
            }
        }

        self.symbols = symbols;
        self.orderbooks = Arc::new(Mutex::new(books_map));
        Ok(())
    }
}

// --- Internal helpers -----------------------------------------------------

async fn connect_via_socks5(
    url: Url,
    proxy_addr: &str,
    tls_config: Arc<ClientConfig>,
) -> Result<WebSocketStream<MaybeTlsStream<Socks5Stream<TcpStream>>>> {
    let host = url.host_str().context("URL missing host")?;
    let port = url.port_or_known_default().context("URL missing port")?;
    let target = format!("{}:{}", host, port);

    let stream = Socks5Stream::connect(proxy_addr, target)
        .await
        .context("connecting via SOCKS5 proxy")?;

    let (ws_stream, _) =
        client_async_tls_with_config(url, stream, None, Some(Connector::Rustls(tls_config)))
            .await
            .context("WebSocket handshake via proxy")?;

    Ok(ws_stream)
}

async fn rate_limited_get(
    client: &Client,
    url: &str,
    bucket: &TokenBucket,
) -> Result<reqwest::Response> {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(64);
    loop {
        bucket.acquire(1).await;
        match client.get(url).send().await {
            Ok(resp) => {
                if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                    tracing::warn!("HTTP 429 for {}", url);
                    sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                    continue;
                }
                return resp.error_for_status().map_err(|e| e.into());
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn fetch_depth_snapshot(
    client: &Client,
    depth_base: &str,
    symbol: &str,
    bucket: &TokenBucket,
) -> Option<OrderBook> {
    let depth_url = format!("{}depth?symbol={}", depth_base, symbol);
    let resp = rate_limited_get(client, &depth_url, bucket).await.ok()?;
    resp.json::<DepthSnapshot>().await.ok().map(|s| s.into())
}

async fn run_ws<S>(
    ws_stream: WebSocketStream<S>,
    books: Arc<Mutex<HashMap<String, OrderBook>>>,
    event_tx: mpsc::Sender<StreamMessage<'static>>,
    client: Client,
    depth_base: String,
    exchange: String,
    http_bucket: Arc<TokenBucket>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut write, mut read) = ws_stream.split();
    let mut ping_interval = interval(Duration::from_secs(30));
    ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_pong = Instant::now();

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                    let _ = write.close().await;
                    return Err(e.into());
                }
                if last_pong.elapsed() > Duration::from_secs(60) {
                    tracing::warn!("no pong received in 60s, closing socket");
                    if crate::config::metrics_enabled() {
                        metrics::counter!("ws_heartbeat_failures").increment(1);
                    }
                    let _ = write.close().await;
                    return Err(anyhow!("heartbeat timeout"));
                }
            }
            msg = timeout(Duration::from_secs(60), read.next()) => {
                let msg = match msg {
                    Ok(Some(msg)) => msg,
                    Ok(None) => break,
                    Err(_) => {
                        tracing::warn!("WS read timeout, closing socket");
                        if crate::config::metrics_enabled() {
                            metrics::counter!("ws_heartbeat_failures").increment(1);
                        }
                        let _ = write.close().await;
                        return Err(anyhow!("read timeout"));
                    }
                };

                match msg {
                    Ok(Message::Text(text)) => {
                        let mut bytes = text.into_bytes();
                        match from_slice::<StreamMessage<'static>>(&mut bytes) {
                        Ok(event) => {
                            let event_time = event.data.event_time();
                            let symbol = event
                                .data
                                .symbol()
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| {
                                    event
                                        .stream
                                        .split('@')
                                        .next()
                                        .unwrap_or("")
                                        .to_string()
                                });
                            let span = tracing::info_span!("ws_event", exchange = %exchange, symbol = %symbol);
                            let _enter = span.enter();
                            let pipeline_start = Instant::now();
                            if crate::config::metrics_enabled() {
                                metrics::counter!("md_ws_events_total").increment(1);
                                if let Some(ev_time) = event_time {
                                    let now_ns = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_nanos();
                                    let lag_ns = now_ns.saturating_sub(ev_time as u128 * 1_000_000);
                                    metrics::gauge!("md_ws_lag_ns").set(lag_ns as f64);
                                }
                            }
                            #[cfg(feature = "debug-logs")]
                            tracing::debug!(?event);
                            // original text no longer available; reconstruct from bytes for logging
                            let raw = unsafe { std::str::from_utf8_unchecked(&bytes) };
                            handle_stream_event(&event, raw);
                            if let Event::DepthUpdate(ref update) = event.data {
                                let symbol = update.symbol.clone();
                                let mut map = books.lock().await;
                                if let Some(book) = map.get_mut(&symbol) {
                                    match apply_depth_update(book, update) {
                                        ApplyResult::Applied | ApplyResult::Outdated => {}
                                        ApplyResult::Gap => {
                                            let buffer = vec![update.clone()];
                                            drop(map);
                                            if let Some(mut new_book) =
                                                fetch_depth_snapshot(&client, &depth_base, &symbol, &http_bucket)
                                                    .await
                                            {
                                                fast_forward(&mut new_book, &buffer);
                                                let mut map = books.lock().await;
                                                map.insert(symbol, new_book);
                                            }
                                        }
                                    }
                                }
                            }
                            if let Err(e) = event_tx.send(event).await {
                                tracing::warn!("failed to send event: {}", e);
                            }
                            if crate::config::metrics_enabled() {
                                metrics::gauge!("md_pipeline_p99_us")
                                    .set(pipeline_start.elapsed().as_micros() as f64);
                            }
                        }
                        Err(e) => tracing::error!("failed to parse message: {}", e),
                    }
                },
                    Ok(Message::Ping(payload)) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Ok(Message::Pong(_)) => {
                        last_pong = Instant::now();
                    }
                    Ok(_) => {}
                    Err(e) => {
                        let _ = write.close().await;
                        return Err(e.into());
                    }
                }
            }
        }
    }

    let _ = write.close().await;
    Ok(())
}
