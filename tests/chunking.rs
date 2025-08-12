use binance_us_and_global::chunk_streams;

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
    let symbols: Vec<String> = (0..2).map(|i| format!("SYM{}", i)).collect();
    let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let chunks = chunk_streams(&symbol_refs, 50);
    assert_eq!(chunks.len(), 2); // 5 globals + 54 per-symbol streams = 59 total
}
