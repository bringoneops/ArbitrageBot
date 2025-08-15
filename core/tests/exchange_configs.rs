use arb_core as core;
use core::stream_config_for_exchange;

#[test]
fn spot_config_excludes_futures_streams() {
    let cfg = stream_config_for_exchange("Binance.US Spot");
    assert!(cfg.global.iter().all(|s| !s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("indexPrice")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("forceOrder")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("continuousKline")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("openInterest")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("topLongShortPositionRatio")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("topLongShortAccountRatio")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("takerBuySellVolume")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("depth5@100ms")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("depth10@100ms")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("depth20@100ms")));
}

#[test]
fn futures_config_includes_futures_streams() {
    let cfg = stream_config_for_exchange("Binance Futures");
    assert!(cfg.global.iter().any(|s| s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("forceOrder")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("openInterest")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("topLongShortPositionRatio")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("topLongShortAccountRatio")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("takerBuySellVolume")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("depth5@100ms")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("depth10@100ms")));
    assert!(cfg.per_symbol.iter().any(|s| s.contains("depth20@100ms")));
}

#[test]
fn options_config_includes_options_streams() {
    let cfg = stream_config_for_exchange("Binance Options");
    assert!(cfg.per_symbol.iter().any(|s| s == "greeks"));
    assert!(cfg.per_symbol.iter().any(|s| s == "openInterest"));
    assert!(cfg.per_symbol.iter().any(|s| s == "impliedVolatility"));
    assert!(cfg.global.iter().all(|s| !s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("markPrice")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("forceOrder")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("topLongShortPositionRatio")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("topLongShortAccountRatio")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("takerBuySellVolume")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("depth5@100ms")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("depth10@100ms")));
    assert!(cfg.per_symbol.iter().all(|s| !s.contains("depth20@100ms")));
}

#[test]
fn gateio_spot_streams() {
    let cfg = stream_config_for_exchange("Gate.io Spot");
    assert!(cfg.per_symbol.iter().any(|s| s == "order_book_update"));
    assert!(cfg.per_symbol.iter().any(|s| s == "trades"));
    assert!(cfg.per_symbol.iter().any(|s| s == "tickers"));
}

#[test]
fn gateio_futures_streams() {
    let cfg = stream_config_for_exchange("Gate.io Futures");
    assert!(cfg.per_symbol.iter().any(|s| s == "order_book_update"));
    assert!(cfg.per_symbol.iter().any(|s| s == "trades"));
    assert!(cfg.per_symbol.iter().any(|s| s == "tickers"));
}
