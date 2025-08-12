use binance_us_and_global::stream_config_for_exchange;

#[test]
fn spot_config_excludes_futures_streams() {
    let cfg = stream_config_for_exchange("Binance.US Spot");
    assert!(cfg.global.iter().all(|s| !s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("indexPrice")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("forceOrder")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("continuousKline")));
}

#[test]
fn futures_config_includes_futures_streams() {
    let cfg = stream_config_for_exchange("Binance Futures");
    assert!(cfg.global.iter().any(|s| s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("forceOrder")));
}

#[test]
fn options_config_excludes_futures_streams() {
    let cfg = stream_config_for_exchange("Binance Options");
    assert!(cfg.global.iter().all(|s| !s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("forceOrder")));
}
