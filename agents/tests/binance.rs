use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use agents::adapter::binance::{connect_via_socks5, process_text_message};
use arb_core as core;
use arb_core::rate_limit::TokenBucket;
use dashmap::DashMap;
use metrics_util::debugging::DebuggingRecorder;
use reqwest::Client;
use rustls::{ClientConfig, RootCertStore};
use tokio::sync::mpsc;
use url::Url;

#[tokio::test]
async fn connect_ws_handles_proxy_url() {
    let tls = Arc::new(
        ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );
    let url = Url::parse("wss://example.com/stream").unwrap();
    let res = connect_via_socks5(url, "127.0.0.1:1", tls).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn process_text_message_updates_book_and_metrics() {
    // Enable metrics
    std::env::set_var("API_KEY", "k");
    std::env::set_var("API_SECRET", "s");
    std::env::set_var("SPOT_SYMBOLS", "BTCUSDT");
    std::env::set_var("ENABLE_FUTURES", "0");
    core::config::load().unwrap();

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _ = recorder.install();

    let books = Arc::new(DashMap::new());
    books.insert(
        "BTCUSDT".to_string(),
        core::OrderBook {
            bids: HashMap::new(),
            asks: HashMap::new(),
            last_update_id: 1,
        },
    );

    let (tx, mut rx) = mpsc::channel(1);
    let event_txs = Arc::new(DashMap::new());
    event_txs.insert("Test:BTCUSDT".into(), tx);

    let http_bucket = Arc::new(TokenBucket::new(1, 1, Duration::from_secs(1)));

    let json = r#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":2,"pu":1,"b":[["1.0","2.0"]],"a":[["2.0","3.0"]]}}"#;

    process_text_message(
        json.to_string(),
        &books,
        &event_txs,
        &Client::new(),
        "http://localhost/",
        "Test",
        &http_bucket,
    )
    .await
    .unwrap();

    // event forwarded
    assert!(rx.recv().await.is_some());

    // orderbook updated
    let book = books.get("BTCUSDT").unwrap();
    assert_eq!(book.bids.get("1.0"), Some(&"2.0".to_string()));
    assert_eq!(book.asks.get("2.0"), Some(&"3.0".to_string()));

    // metrics emitted
    let metrics = snapshotter.snapshot().into_vec();
    assert!(metrics
        .iter()
        .any(|(k, _, _, _)| k.key().name() == "md_pipeline_p99_us"));
}
