use binance_us_and_global::config::{Config, Credentials};

#[test]
fn empty_spot_symbols_fails() {
    let cfg = Config {
        proxy_url: None,
        spot_symbols: vec![],
        futures_symbols: vec![],
        chunk_size: 1,
        event_buffer_size: 1,
        enable_spot: true,
        enable_futures: false,
        enable_metrics: false,
        credentials: Credentials {
            api_key: "k".into(),
            api_secret: "s".into(),
        },
        ca_bundle: None,
        cert_pins: Vec::new(),
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn empty_futures_symbols_fails() {
    let cfg = Config {
        proxy_url: None,
        spot_symbols: vec![],
        futures_symbols: vec![],
        chunk_size: 1,
        event_buffer_size: 1,
        enable_spot: false,
        enable_futures: true,
        enable_metrics: false,
        credentials: Credentials {
            api_key: "k".into(),
            api_secret: "s".into(),
        },
        ca_bundle: None,
        cert_pins: Vec::new(),
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn invalid_chunk_size_fails() {
    let cfg = Config {
        proxy_url: None,
        spot_symbols: vec!["BTCUSDT".into()],
        futures_symbols: vec![],
        chunk_size: 0,
        event_buffer_size: 1,
        enable_spot: true,
        enable_futures: false,
        enable_metrics: false,
        credentials: Credentials {
            api_key: "k".into(),
            api_secret: "s".into(),
        },
        ca_bundle: None,
        cert_pins: Vec::new(),
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn invalid_event_buffer_size_fails() {
    let cfg = Config {
        proxy_url: None,
        spot_symbols: vec!["BTCUSDT".into()],
        futures_symbols: vec![],
        chunk_size: 10,
        event_buffer_size: 0,
        enable_spot: true,
        enable_futures: false,
        enable_metrics: false,
        credentials: Credentials {
            api_key: "k".into(),
            api_secret: "s".into(),
        },
        ca_bundle: None,
        cert_pins: Vec::new(),
    };
    assert!(cfg.validate().is_err());
}
