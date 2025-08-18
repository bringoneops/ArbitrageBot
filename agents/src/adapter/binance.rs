use anyhow::{anyhow, Context, Result};
use arb_core as core;
use async_trait::async_trait;
use core::{
    apply_depth_update, chunk_streams_with_config, fast_forward, handle_stream_event, next_backoff,
    stream_config_for_exchange, ApplyResult, DepthSnapshot, OrderBook,
};
use dashmap::DashMap;
use futures::stream::SplitSink;
use futures::{stream, SinkExt, StreamExt};
use rand::Rng;
use reqwest::{Client, StatusCode};
use rustls::ClientConfig;
use serde_json::Value;
use simd_json::serde::from_slice;
use std::{env, future::Future, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::mpsc,
    time::{interval, sleep, timeout, Duration, Instant, MissedTickBehavior},
};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async_tls_with_config, tungstenite::protocol::Message,
    Connector, MaybeTlsStream, WebSocketStream,
};
use tracing::{error, warn, Span};
use url::Url;

#[cfg(test)]
use once_cell::sync::Lazy;
#[cfg(test)]
use std::sync::Mutex;

use core::events::{DepthUpdateEvent, Event, StreamMessage};
use core::rate_limit::TokenBucket;

use super::ExchangeAdapter;
use crate::{registry, ChannelRegistry, TaskSet};
use futures::future::BoxFuture;
use std::sync::Once;
/// Configuration for a single Binance exchange endpoint.
pub struct BinanceConfig {
    pub id: &'static str,
    pub name: &'static str,
    pub info_url: &'static str,
    pub ws_base: &'static str,
}

/// All Binance exchanges supported by this adapter.
pub const BINANCE_EXCHANGES: &[BinanceConfig] = &[
    BinanceConfig {
        id: "binance_us_spot",
        name: "Binance.US Spot",
        info_url: "https://api.binance.us/api/v3/exchangeInfo",
        ws_base: "wss://stream.binance.us:9443/stream?streams=",
    },
    BinanceConfig {
        id: "binance_global_spot",
        name: "Binance Global Spot",
        info_url: "https://api.binance.com/api/v3/exchangeInfo",
        ws_base: "wss://stream.binance.com:9443/stream?streams=",
    },
    BinanceConfig {
        id: "binance_futures",
        name: "Binance Futures",
        info_url: "https://fapi.binance.com/fapi/v1/exchangeInfo",
        ws_base: "wss://fstream.binance.com/stream?streams=",
    },
    BinanceConfig {
        id: "binance_delivery",
        name: "Binance Delivery",
        info_url: "https://dapi.binance.com/dapi/v1/exchangeInfo",
        ws_base: "wss://dstream.binance.com/stream?streams=",
    },
    BinanceConfig {
        id: "binance_options",
        name: "Binance Options",
        info_url: "https://vapi.binance.com/vapi/v1/exchangeInfo",
        ws_base: "wss://voptions.binance.com/stream",
    },
];

/// Retrieve all trading symbols for an exchange using its `exchangeInfo` endpoint.
pub async fn fetch_symbols(client: &Client, info_url: &str) -> Result<Vec<String>> {
    let resp = client.get(info_url).send().await?.error_for_status()?;
    let data: Value = resp.json().await?;
    let symbols = data
        .get("symbols")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing symbols array"))?;

    let mut result: Vec<String> = symbols
        .iter()
        .filter_map(|s| {
            if s.get("status")
                .and_then(|v| v.as_str())
                .map(|st| st == "TRADING")
                .unwrap_or(false)
            {
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
        for exch in BINANCE_EXCHANGES {
            let cfg_ref: &'static BinanceConfig = exch;
            registry::register_adapter(
                cfg_ref.id,
                Arc::new(
                    move |global_cfg: &'static core::config::Config,
                          exchange_cfg: &core::config::ExchangeConfig,
                          client: Client,
                          task_set: TaskSet,
                          channels: ChannelRegistry,
                          tls_config: Arc<ClientConfig>|
                          -> BoxFuture<
                        'static,
                        Result<Vec<mpsc::Receiver<StreamMessage<'static>>>>,
                    > {
                        let cfg = cfg_ref;
                        let initial_symbols = exchange_cfg.symbols.clone();
                        Box::pin(async move {
                            let mut symbols = initial_symbols;
                            if symbols.is_empty() {
                                symbols = fetch_symbols(&client, cfg.info_url).await?;
                            }

                            let mut receivers = Vec::new();
                            for symbol in &symbols {
                                let key = format!("{}:{}", cfg.name, symbol);
                                let (_, rx) = channels.get_or_create(&key);
                                if let Some(rx) = rx {
                                    receivers.push(rx);
                                }
                            }

                            let adapter = BinanceAdapter::new(
                                cfg,
                                client.clone(),
                                global_cfg.chunk_size,
                                global_cfg.proxy_url.clone().unwrap_or_default(),
                                task_set.clone(),
                                channels.clone(),
                                symbols,
                                tls_config.clone(),
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

/// Adapter implementing the `ExchangeAdapter` trait for Binance.
pub struct BinanceAdapter {
    cfg: &'static BinanceConfig,
    client: Client,
    chunk_size: usize,
    proxy_url: String,
    task_set: TaskSet,
    channels: ChannelRegistry,
    symbols: Vec<String>,
    orderbooks: Arc<DashMap<String, OrderBook>>,
    tls_config: Arc<ClientConfig>,
    http_bucket: Arc<TokenBucket>,
    ws_bucket: Arc<TokenBucket>,
    book_refresh_interval: Duration,
}

impl BinanceAdapter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: &'static BinanceConfig,
        client: Client,
        chunk_size: usize,
        proxy_url: String,
        task_set: TaskSet,
        channels: ChannelRegistry,
        symbols: Vec<String>,
        tls_config: Arc<ClientConfig>,
    ) -> Self {
        let global_cfg = core::config::get();
        Self {
            cfg,
            client,
            chunk_size,
            proxy_url,
            task_set,
            channels,
            symbols,
            orderbooks: Arc::new(DashMap::new()),
            tls_config,
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
            book_refresh_interval: Duration::from_secs(global_cfg.book_refresh_secs),
        }
    }

    async fn spawn_chunk_reader(&self, chunk: Vec<String>, depth_base: &str) -> Result<()> {
        let chunk_len = chunk.len();
        let param = chunk.join("/");
        let url = Url::parse(&format!("{}{}", self.cfg.ws_base, param))
            .context("parsing WebSocket URL")?;

        let proxy = self.proxy_url.clone();
        let name = self.cfg.name.to_string();
        let task_set = self.task_set.clone();
        let books = self.orderbooks.clone();
        let channels = self.channels.clone();
        let client = self.client.clone();
        let tls_config = self.tls_config.clone();
        let http_bucket = self.http_bucket.clone();
        let ws_bucket = self.ws_bucket.clone();
        let depth_base = depth_base.to_string();

        {
            let mut set = task_set.lock().await;
            set.spawn(async move {
                if proxy.is_empty() {
                    let url_for_connect = url.clone();
                    reconnect_loop(
                        move || {
                            let tls = tls_config.clone();
                            let url = url_for_connect.clone();
                            async move {
                                connect_async_tls_with_config(
                                    url,
                                    None,
                                    false,
                                    Some(Connector::Rustls(tls)),
                                )
                                .await
                                .context("WebSocket handshake")
                                .map(|(ws_stream, _)| ws_stream)
                            }
                        },
                        name.clone(),
                        url,
                        chunk_len,
                        ws_bucket,
                        books,
                        channels.clone(),
                        client,
                        depth_base,
                        http_bucket,
                    )
                    .await;
                } else {
                    let url_for_connect = url.clone();
                    let proxy_addr = proxy.clone();
                    reconnect_loop(
                        move || {
                            let url = url_for_connect.clone();
                            let proxy_addr = proxy_addr.clone();
                            let tls = tls_config.clone();
                            async move { connect_via_socks5(url, &proxy_addr, tls).await }
                        },
                        name.clone(),
                        url,
                        chunk_len,
                        ws_bucket,
                        books,
                        channels,
                        client,
                        depth_base,
                        http_bucket,
                    )
                    .await;
                }
            });
        }
        Ok(())
    }
}

#[async_trait]
impl ExchangeAdapter for BinanceAdapter {
    async fn subscribe(&mut self) -> Result<()> {
        for symbol in &self.symbols {
            let key = format!("{}:{}", self.cfg.name, symbol);
            // Ensure a channel exists for each subscribed symbol
            self.channels.get_or_create(&key);
        }
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

        // spawn periodic depth snapshot refresh tasks
        for symbol in self.symbols.clone() {
            let books = self.orderbooks.clone();
            let client = self.client.clone();
            let http_bucket = self.http_bucket.clone();
            let depth_base = depth_base.clone();
            let refresh_interval = self.book_refresh_interval;
            let task_set = self.task_set.clone();
            let symbol = symbol.clone();
            let mut set = task_set.lock().await;
            set.spawn(async move {
                let mut ticker = interval(refresh_interval);
                ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    if let Some(book) =
                        fetch_depth_snapshot(&client, &depth_base, &symbol, &http_bucket).await
                    {
                        if core::config::metrics_enabled() {
                            metrics::counter!("md_ws_resnapshot_total").increment(1);
                        }
                        books.insert(symbol.clone(), book);
                    }
                }
            });
        }

        for chunk in chunks {
            self.spawn_chunk_reader(chunk, &depth_base).await?;
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
        let books_map: DashMap<String, OrderBook> = DashMap::new();

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
        self.orderbooks = Arc::new(books_map);
        Ok(())
    }
}

// --- Internal helpers -----------------------------------------------------

pub async fn connect_via_socks5(
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

async fn reconnect_loop<F, Fut, S>(
    mut connect: F,
    name: String,
    url: Url,
    chunk_len: usize,
    ws_bucket: Arc<TokenBucket>,
    books: Arc<DashMap<String, OrderBook>>,
    channels: ChannelRegistry,
    client: Client,
    depth_base: String,
    http_bucket: Arc<TokenBucket>,
) where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<WebSocketStream<S>>> + Send,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
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
        if ws_bucket.acquire(1).await.is_err() {
            break;
        }
        tracing::info!(
            "\u{2192} opening WS ({}): {} ({} streams)",
            name,
            url,
            chunk_len
        );
        let start = Instant::now();
        let mut connected = false;
        let result = match connect().await {
            Ok(ws_stream) => {
                connected = true;
                backoff = Duration::from_secs(1);
                tracing::info!("subscribed {} topics to {}", chunk_len, url);
                run_ws(
                    ws_stream,
                    books.clone(),
                    channels.clone(),
                    client.clone(),
                    depth_base.clone(),
                    name.clone(),
                    http_bucket.clone(),
                )
                .await
            }
            Err(e) => Err(e),
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
            backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
        }

        let jitter: f32 = rand::thread_rng().gen_range(0.8..1.2);
        let sleep_dur = backoff.mul_f32(jitter);
        tracing::warn!("Reconnecting in {:?}...", sleep_dur);
        if core::config::metrics_enabled() {
            metrics::counter!("md_ws_reconnects_total").increment(1);
        }
        sleep(sleep_dur).await;
    }
}

async fn rate_limited_get(
    client: &Client,
    url: &str,
    bucket: &TokenBucket,
) -> Result<reqwest::Response> {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(64);
    loop {
        bucket.acquire(1).await?;
        match client.get(url).send().await {
            Ok(resp) => {
                if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                    tracing::warn!("HTTP 429 for {}", url);
                    sleep(backoff).await;
                    backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
                    continue;
                }
                return resp.error_for_status().map_err(|e| e.into());
            }
            Err(e) => {
                tracing::warn!("Request error for {}: {}", url, e);
                sleep(backoff).await;
                backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
                continue;
            }
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

async fn handle_ping_pong<S>(
    write: &mut SplitSink<WebSocketStream<S>, Message>,
    last_pong: &mut Instant,
    msg: Option<Message>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match msg {
        Some(Message::Ping(payload)) => {
            write.send(Message::Pong(payload)).await?;
        }
        Some(Message::Pong(_)) => {
            *last_pong = Instant::now();
        }
        Some(_) => {}
        None => {
            if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                let _ = write.close().await;
                return Err(e.into());
            }
            if last_pong.elapsed() > Duration::from_secs(60) {
                tracing::warn!("no pong received in 60s, closing socket");
                if core::config::metrics_enabled() {
                    metrics::counter!("ws_heartbeat_failures").increment(1);
                }
                let _ = write.close().await;
                return Err(anyhow!("heartbeat timeout"));
            }
        }
    }
    Ok(())
}

fn current_time() -> std::time::SystemTime {
    #[cfg(test)]
    {
        let f = *NOW_FN.lock().expect("now function mutex poisoned");
        f()
    }
    #[cfg(not(test))]
    {
        std::time::SystemTime::now()
    }
}

#[cfg(test)]
type NowFn = fn() -> std::time::SystemTime;

#[cfg(test)]
static NOW_FN: Lazy<Mutex<NowFn>> = Lazy::new(|| Mutex::new(std::time::SystemTime::now));

#[cfg(test)]
pub(crate) fn set_now_fn(f: NowFn) {
    *NOW_FN.lock().expect("now function mutex poisoned") = f;
}

fn lag_ns_from_event_time(ev_time: u64, now: std::time::SystemTime) -> u128 {
    match now.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration
            .as_nanos()
            .saturating_sub(ev_time as u128 * 1_000_000),
        Err(err) => {
            warn!("failed to compute lag: {}", err);
            0
        }
    }
}

fn log_and_metric_event(
    event: &StreamMessage<'static>,
    bytes: &[u8],
    exchange: &str,
) -> (String, Instant, Span) {
    let event_time = event.data.event_time();
    let symbol = event
        .data
        .symbol()
        .map(|s| s.to_string())
        .unwrap_or_else(|| event.stream.split('@').next().unwrap_or("").to_string());
    let span = tracing::info_span!("ws_event", exchange = %exchange, symbol = %symbol);
    let pipeline_start = Instant::now();
    span.in_scope(|| {
        let lag_ns = event_time.map(|ev_time| lag_ns_from_event_time(ev_time, current_time()));
        if core::config::metrics_enabled() {
            metrics::counter!("md_ws_events_total").increment(1);
            if let Some(lag_ns) = lag_ns {
                metrics::gauge!("md_ws_lag_ns").set(lag_ns as f64);
            }
        }
        #[cfg(feature = "debug-logs")]
        tracing::debug!(?event);
        // Use `from_utf8` instead of `from_utf8_unchecked` to avoid
        // potential UB from invalid payloads. The validation cost is
        // negligible, and we fall back to a lossy conversion only on
        // error, which should be rare.
        let raw = match std::str::from_utf8(bytes) {
            Ok(s) => std::borrow::Cow::Borrowed(s),
            Err(e) => {
                tracing::warn!("non-UTF8 WebSocket payload: {}", e);
                std::borrow::Cow::Owned(String::from_utf8_lossy(bytes).into_owned())
            }
        };
        handle_stream_event(event, raw.as_ref());
    });
    (symbol, pipeline_start, span)
}

async fn update_order_book(
    books: &Arc<DashMap<String, OrderBook>>,
    client: &Client,
    http_bucket: &Arc<TokenBucket>,
    depth_base: &str,
    update: &DepthUpdateEvent<'_>,
) {
    let symbol = update.symbol.clone();
    if let Some(mut book) = books.get_mut(&symbol) {
        match apply_depth_update(&mut book, update) {
            ApplyResult::Applied | ApplyResult::Outdated => {}
            ApplyResult::Gap => {
                let buffer = vec![update.clone()];
                drop(book);
                if let Some(mut new_book) =
                    fetch_depth_snapshot(client, depth_base, &symbol, http_bucket).await
                {
                    if core::config::metrics_enabled() {
                        metrics::counter!("md_ws_resnapshot_total").increment(1);
                    }
                    match fast_forward(&mut new_book, &buffer) {
                        ApplyResult::Applied => {
                            books.insert(symbol, new_book);
                        }
                        ApplyResult::Gap | ApplyResult::Outdated => {
                            if let Some(resynced) =
                                fetch_depth_snapshot(client, depth_base, &symbol, http_bucket).await
                            {
                                if core::config::metrics_enabled() {
                                    metrics::counter!("md_ws_resnapshot_total").increment(1);
                                }
                                books.insert(symbol, resynced);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn process_text_message(
    text: String,
    books: &Arc<DashMap<String, OrderBook>>,
    channels: &ChannelRegistry,
    client: &Client,
    depth_base: &str,
    exchange: &str,
    http_bucket: &Arc<TokenBucket>,
) -> Result<()> {
    let mut bytes = text.into_bytes();
    let event = match from_slice::<StreamMessage<'static>>(&mut bytes) {
        Ok(event) => event,
        Err(e) => {
            tracing::error!("failed to parse message: {}", e);
            return Ok(());
        }
    };

    let (symbol, pipeline_start, span) = log_and_metric_event(&event, &bytes, exchange);
    let _enter = span.enter();

    if let Event::DepthUpdate(ref update) = event.data {
        update_order_book(books, client, http_bucket, depth_base, update).await;
    }

    let key = format!("{}:{}", exchange, symbol);
    if let Some(tx) = channels.get(&key) {
        if let Err(e) = tx.send(event).await {
            tracing::warn!("failed to send event: {}", e);
        }
    } else {
        tracing::warn!("missing channel for {}", key);
    }

    if core::config::metrics_enabled() {
        metrics::gauge!("md_pipeline_p99_us").set(pipeline_start.elapsed().as_micros() as f64);
    }

    Ok(())
}

pub async fn run_ws<S>(
    ws_stream: WebSocketStream<S>,
    books: Arc<DashMap<String, OrderBook>>,
    channels: ChannelRegistry,
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
                handle_ping_pong(&mut write, &mut last_pong, None).await?;
            }
            msg = timeout(Duration::from_secs(60), read.next()) => {
                let msg = match msg {
                    Ok(Some(msg)) => msg,
                    Ok(None) => break,
                    Err(_) => {
                        tracing::warn!("WS read timeout, closing socket");
                        if core::config::metrics_enabled() {
                            metrics::counter!("ws_heartbeat_failures").increment(1);
                        }
                        let _ = write.close().await;
                        return Err(anyhow!("read timeout"));
                    }
                };

                match msg {
                    Ok(Message::Text(text)) => {
                        process_text_message(
                            text,
                            &books,
                            &channels,
                            &client,
                            &depth_base,
                            &exchange,
                            &http_bucket,
                        ).await?;
                    }
                    Ok(other) => {
                        handle_ping_pong(&mut write, &mut last_pong, Some(other)).await?;
                    }
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

#[cfg(test)]
mod tests {
    use super::{lag_ns_from_event_time, log_and_metric_event, set_now_fn};
    use arb_core::events::{Event, IndexPriceEvent, StreamMessage};
    use std::{
        borrow::Cow,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    use tracing_test::traced_test;

    #[test]
    fn lag_defaults_to_zero_on_error() {
        let past = UNIX_EPOCH - Duration::from_secs(1);
        let lag = lag_ns_from_event_time(0, past);
        assert_eq!(lag, 0);
    }

    #[traced_test]
    #[test]
    fn log_warning_on_pre_epoch_time() {
        fn before_epoch() -> SystemTime {
            UNIX_EPOCH - Duration::from_secs(1)
        }
        set_now_fn(before_epoch);
        let msg = StreamMessage {
            stream: "test@index".to_string(),
            data: Event::IndexPrice(IndexPriceEvent {
                event_time: 0,
                symbol: "TEST".to_string(),
                index_price: Cow::Borrowed("0"),
            }),
        };
        let _ = log_and_metric_event(&msg, b"{}", "binance");
        assert!(logs_contain("failed to compute lag"));
        set_now_fn(SystemTime::now);
    }
}
