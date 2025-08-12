pub mod events;

pub fn chunk_streams(symbols: &[&str]) -> Vec<Vec<String>> {
    // Global streams including rolling-window tickers
    let mut streams = vec![
        "!miniTicker@arr".to_string(),
        "!ticker@arr".to_string(),
        "!bookTicker@arr".to_string(),
        "!ticker_1h@arr".to_string(),
        "!ticker_4h@arr".to_string(),
    ];

    // Per-symbol suffixes
    let suffixes = [
        "trade",
        "aggTrade",
        "depth",
        "depth5",
        "depth10",
        "depth20",
        "depth@100ms",
        "kline_1m",
        "kline_3m",
        "kline_5m",
        "kline_15m",
        "kline_30m",
        "kline_1h",
        "kline_2h",
        "kline_4h",
        "kline_6h",
        "kline_8h",
        "kline_12h",
        "kline_1d",
        "kline_3d",
        "kline_1w",
        "kline_1M",
        "miniTicker",
        "ticker",
        "bookTicker",
        "ticker_1h",
        "ticker_4h",
    ];

    for &sym in symbols {
        let sym_lower = sym.to_lowercase();
        for suffix in &suffixes {
            streams.push(format!("{}@{}", sym_lower, suffix));
        }
    }

    let chunk_size = 100;
    let mut result = Vec::new();
    for chunk in streams.chunks(chunk_size) {
        result.push(chunk.to_vec());
    }
    result
}
