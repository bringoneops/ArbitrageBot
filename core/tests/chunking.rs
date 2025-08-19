use arb_core as core;
use core::{chunk_streams, chunk_streams_with_config, StreamConfig};
use std::collections::HashSet;

#[test]
fn chunks_do_not_exceed_hundred_streams() {
    // Create mock symbols to generate more than one chunk
    let symbols: Vec<String> = (0..4).map(|i| format!("SYM{i}")).collect();
    let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let chunks = chunk_streams(&symbol_refs, 100);

    assert!(chunks.iter().all(|c| c.len() <= 100));
}

#[test]
fn includes_global_streams() {
    let chunks = chunk_streams(&[], 100);
    let streams: Vec<&String> = chunks.iter().flatten().collect();
    assert!(streams.iter().any(|s| *s == "!miniTicker@arr"));
    assert!(streams.iter().any(|s| *s == "!markPrice@arr"));
}

#[test]
fn includes_per_symbol_streams() {
    let chunks = chunk_streams(&["BTCUSDT"], 100);
    let streams: Vec<String> = chunks.into_iter().flatten().collect();
    assert!(streams.contains(&"btcusdt@bookTicker".to_string()));
    assert!(streams.contains(&"btcusdt@markPrice".to_string()));
    assert!(streams.contains(&"btcusdt@markPrice@1s".to_string()));
    assert!(streams.contains(&"btcusdt@forceOrder".to_string()));
}

#[test]
fn returns_expected_number_of_chunks() {
    let chunk_size = 50;
    let global_count = chunk_streams(&[], chunk_size)
        .iter()
        .map(|c| c.len())
        .sum::<usize>();
    let one_symbol_count = chunk_streams(&["SYM0"], chunk_size)
        .iter()
        .map(|c| c.len())
        .sum::<usize>();
    let per_symbol = one_symbol_count - global_count;

    let symbols: Vec<String> = (0..2).map(|i| format!("SYM{i}")).collect();
    let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let expected_total = global_count + symbol_refs.len() * per_symbol;
    let expected_chunks = expected_total.div_ceil(chunk_size);
    let chunks = chunk_streams(&symbol_refs, chunk_size);
    assert_eq!(chunks.len(), expected_chunks);
}

#[test]
fn zero_chunk_size_returns_empty_vec() {
    let chunks = chunk_streams(&[], 0);
    assert!(chunks.is_empty());
}

#[test]
fn supports_custom_stream_configuration() {
    let cfg = StreamConfig {
        global: vec!["custom".into()],
        per_symbol: vec!["suffix".into()],
    };
    let chunks = chunk_streams_with_config(&["ABC"], 10, &cfg);
    assert!(chunks.iter().flatten().any(|s| s == "custom"));
    assert!(chunks.iter().flatten().any(|s| s == "abc@suffix"));
}

#[test]
fn removes_duplicate_streams() {
    let symbols = ["SYM0", "SYM0"];
    let chunks = chunk_streams(&symbols, 100);
    let streams: Vec<String> = chunks.into_iter().flatten().collect();
    let unique: HashSet<&String> = streams.iter().collect();
    assert_eq!(streams.len(), unique.len());
}
