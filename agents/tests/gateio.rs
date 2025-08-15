use agents::adapter::gateio::{fetch_symbols, GATEIO_EXCHANGES};

#[tokio::test]
async fn fetch_symbols_returns_pairs() {
    let url = format!("{}?limit=1", GATEIO_EXCHANGES[0].info_url);
    let symbols = fetch_symbols(&url).await.unwrap();
    assert!(!symbols.is_empty());
}
