use binance_us_and_global::chunk_streams;

#[test]
fn chunks_do_not_exceed_hundred_streams() {
    // Create mock symbols to generate more than one chunk
    let symbols: Vec<String> = (0..4).map(|i| format!("SYM{}", i)).collect();
    let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let chunks = chunk_streams(&symbol_refs);

    assert!(chunks.iter().all(|c| c.len() <= 100));
}
