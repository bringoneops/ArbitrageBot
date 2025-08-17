use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use agents::adapter::binance::{connect_via_socks5, fetch_symbols, process_text_message};
use agents::ChannelRegistry;
use arb_core as core;
use arb_core::rate_limit::TokenBucket;
use dashmap::DashMap;
use httpmock::prelude::*;
use metrics_util::debugging::DebuggingRecorder;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client,
};
use rustls::{ClientConfig, RootCertStore};
use serde_json::json;
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

    let channels = ChannelRegistry::new(1);
    let (_, rx) = channels.get_or_create("Test:BTCUSDT");
    let mut rx = rx.expect("receiver");

    let http_bucket = Arc::new(TokenBucket::new(1, 1, Duration::from_secs(1)));

    let json = r#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":2,"pu":1,"b":[["1.0","2.0"]],"a":[["2.0","3.0"]]}}"#;

    process_text_message(
        json.to_string(),
        &books,
        &channels,
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

#[tokio::test]
async fn fetch_symbols_uses_provided_client() {
    let server = MockServer::start();

    let mock = server.mock(|when, then| {
        when.method(GET).path("/exchangeInfo").header("x-test", "1");
        then.status(200).json_body(json!({
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING"},
                {"symbol": "ETHUSDT", "status": "BREAK"}
            ]
        }));
    });

    let mut headers = HeaderMap::new();
    headers.insert("x-test", HeaderValue::from_static("1"));
    let client = Client::builder().default_headers(headers).build().unwrap();

    let url = format!("{}/exchangeInfo", server.base_url());
    let symbols = fetch_symbols(&client, &url).await.unwrap();

    assert_eq!(symbols, vec!["BTCUSDT".to_string()]);
    mock.assert();
}
