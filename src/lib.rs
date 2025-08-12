pub mod events;

// Global streams including rolling-window tickers
const GLOBAL_STREAMS: &[&str] = &[
    "!miniTicker@arr",
    "!ticker@arr",
    "!bookTicker@arr",
    "!ticker_1h@arr",
    "!ticker_4h@arr",
];

// Per-symbol stream suffixes
const PER_SYMBOL_SUFFIXES: &[&str] = &[
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

pub fn chunk_streams(symbols: &[&str], chunk_size: usize) -> Vec<Vec<String>> {
    let mut streams =
        Vec::with_capacity(GLOBAL_STREAMS.len() + symbols.len() * PER_SYMBOL_SUFFIXES.len());
    streams.extend(GLOBAL_STREAMS.iter().map(|s| s.to_string()));

    for &sym in symbols {
        let sym_lower = sym.to_lowercase();
        for &suffix in PER_SYMBOL_SUFFIXES {
            streams.push(format!("{}@{}", sym_lower, suffix));
        }
    }

    let mut result = Vec::with_capacity(streams.len().div_ceil(chunk_size));
    for chunk in streams.chunks(chunk_size) {
        result.push(chunk.to_vec());
    }
    result
}
