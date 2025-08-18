use canonical::symbol::{get_spec, load_from_path, normalize_symbol};

#[test]
fn normalize_and_get_spec() {
    // Prepare a temporary file with minimal symbol table
    let mut path = std::env::temp_dir();
    path.push(format!(
        "symbols_{}.json",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let data = r#"
        [
            {
                "id": "BTC-USD",
                "spec": {
                    "venue": "spot",
                    "base": "BTC",
                    "quote": "USD",
                    "lot_step": 0.01,
                    "price_step": 0.1
                },
                "aliases": { "binance": ["btcusdt", "BTCUSDT"] }
            }
        ]
    "#;

    std::fs::write(&path, data).unwrap();

    load_from_path(path.to_str().unwrap()).unwrap();
    assert_eq!(normalize_symbol("binance", "btcusdt"), "BTC-USD");
    assert_eq!(normalize_symbol("binance", "BTCUSDT"), "BTC-USD");

    let spec = get_spec("BTC-USD").unwrap();
    assert_eq!(spec.lot_step, Some(0.01));
    assert_eq!(spec.price_step, Some(0.1));

    // Clean up temporary file
    let _ = std::fs::remove_file(path);
}
