use agents::adapter::gateio::{fetch_symbols, GateioConfig};

#[tokio::test]
#[ignore]
async fn fetch_symbols_returns_pairs() {
    const INFO_URL: &str = "https://api.gateio.ws/api/v4/spot/currency_pairs?limit=1";
    let cfg = GateioConfig {
        id: "gateio_spot",
        name: "Gate.io Spot",
        info_url: INFO_URL,
        ws_base: "",
    };
    let symbols = fetch_symbols(&cfg).await.unwrap();
    assert!(!symbols.is_empty());
}
