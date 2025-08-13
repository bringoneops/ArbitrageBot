use agents::{spawn_adapters, BINANCE_EXCHANGES};

#[tokio::test]
async fn spawn_adapters_ok() {
    assert!(spawn_adapters().await.is_ok());
}

#[test]
fn binance_exchanges_present() {
    assert!(!BINANCE_EXCHANGES.is_empty());
}
