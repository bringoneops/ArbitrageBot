use binance_us_and_global::{chunk_streams, chunk_streams_with_config, StreamConfig};

#[test]
fn chunks_do_not_exceed_hundred_streams() {
    // Create mock symbols to generate more than one chunk
    let symbols: Vec<String> = (0..4).map(|i| format!("SYM{}", i)).collect();
    let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let chunks = chunk_streams(&symbol_refs, 100);

    assert!(chunks.iter().all(|c| c.len() <= 100));
}

#[test]
fn includes_global_streams() {
    let chunks = chunk_streams(&[], 100);
    assert!(chunks.iter().flatten().any(|s| s == "!miniTicker@arr"));
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

    let symbols: Vec<String> = (0..2).map(|i| format!("SYM{}", i)).collect();
    let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let expected_total = global_count + symbol_refs.len() * per_symbol;
    let expected_chunks = (expected_total + chunk_size - 1) / chunk_size;
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
