use agents::adapter::binance::{run_ws, BinanceAdapter, BinanceConfig};
use agents::ExchangeAdapter;
use arb_core as core;
use arb_core::rate_limit::TokenBucket;
use dashmap::DashMap;
use futures::SinkExt;
use reqwest::Client;
use rustls::{ClientConfig, RootCertStore};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::{accept_async, connect_async, tungstenite::protocol::Message};

#[tokio::test]
async fn run_ws_emits_event() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let json = r#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":[["1.0","2.0"]],"a":[["2.0","1.0"]]}}"#;
        ws.send(Message::Text(json.to_string())).await.unwrap();
        ws.close(None).await.unwrap();
    });

    let url = format!("ws://{}", addr);
    let (ws_stream, _) = connect_async(url).await.unwrap();

    let books = Arc::new(DashMap::new());
    let (tx, mut rx) = mpsc::channel(1);
    let channels = Arc::new(DashMap::new());
    channels.insert("Test:BTCUSDT".into(), tx);
    let http_bucket = Arc::new(TokenBucket::new(1, 1, Duration::from_secs(1)));

    run_ws(
        ws_stream,
        books,
        channels,
        Client::new(),
        "http://localhost/".into(),
        "Test".into(),
        http_bucket,
    )
    .await
    .unwrap();

    let msg = rx.recv().await.expect("no event");
    assert_eq!(msg.stream, "btcusdt@depth");
    match msg.data {
        core::events::Event::DepthUpdate(ev) => assert_eq!(ev.symbol, "BTCUSDT"),
        _ => panic!("unexpected event"),
    }

    server.await.unwrap();
}

static TEST_CFG: BinanceConfig = BinanceConfig {
    name: "Test",
    info_url: "http://localhost/",
    ws_base: "wss://127.0.0.1:1/",
};

#[tokio::test]
async fn subscribe_handles_reconnect_failures() {
    std::env::set_var("API_KEY", "k");
    std::env::set_var("API_SECRET", "s");
    std::env::set_var("ENABLE_SPOT", "0");
    std::env::set_var("ENABLE_FUTURES", "0");
    core::config::load().unwrap();
    std::env::set_var("MAX_FAILURES", "1");
    let client = Client::new();
    let (task_tx, mut task_rx) = mpsc::unbounded_channel();
    let event_txs = Arc::new(DashMap::new());
    let (tx, _rx) = mpsc::channel(1);
    event_txs.insert("Test:BTCUSDT".into(), tx);
    let tls = Arc::new(
        ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );
    let mut adapter = BinanceAdapter::new(
        &TEST_CFG,
        client,
        1,
        String::new(),
        task_tx.clone(),
        event_txs,
        vec!["BTCUSDT".to_string()],
        tls,
    );
    adapter.subscribe().await.unwrap();
    let handle = task_rx.recv().await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("task did not complete");
}

#[tokio::test]
async fn token_bucket_enforces_rate_limit() {
    let bucket = TokenBucket::new(1, 0, Duration::from_secs(60));
    bucket.acquire(1).await;
    let fut = bucket.acquire(1);
    assert!(tokio::time::timeout(Duration::from_millis(50), fut)
        .await
        .is_err());
}
