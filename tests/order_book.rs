use binance_us_and_global::{apply_depth_update, fast_forward, ApplyResult, DepthSnapshot, OrderBook};
use binance_us_and_global::events::DepthUpdateEvent;

#[test]
fn merges_snapshot_and_diff() {
    let snapshot_json = r#"{"lastUpdateId":1,"bids":[["1.0","1.0"]],"asks":[["2.0","2.0"]]}"#;
    let snapshot: DepthSnapshot = serde_json::from_str(snapshot_json).unwrap();
    let mut book: OrderBook = snapshot.into();

    let diff_json = r#"{"E":0,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":[["1.0","0.5"],["0.9","1.0"]],"a":[["2.0","0"],["2.1","2.5"]]}"#;
    let diff: DepthUpdateEvent<'_> = serde_json::from_str(diff_json).unwrap();

    assert_eq!(apply_depth_update(&mut book, &diff), ApplyResult::Applied);

    assert_eq!(book.last_update_id, 3);
    assert_eq!(book.bids.get("1.0").unwrap(), "0.5");
    assert_eq!(book.bids.get("0.9").unwrap(), "1.0");
    assert!(!book.asks.contains_key("2.0"));
    assert_eq!(book.asks.get("2.1").unwrap(), "2.5");
}

#[test]
fn ignores_outdated_update() {
    let snapshot_json = r#"{"lastUpdateId":3,"bids":[["1.0","1.0"]],"asks":[]}"#;
    let snapshot: DepthSnapshot = serde_json::from_str(snapshot_json).unwrap();
    let mut book: OrderBook = snapshot.into();

    // final_update_id (3) is less than next expected (4)
    let diff_json = r#"{"E":0,"s":"BTCUSDT","U":1,"u":3,"pu":1,"b":[["1.0","2.0"]],"a":[]}"#;
    let diff: DepthUpdateEvent<'_> = serde_json::from_str(diff_json).unwrap();

    assert_eq!(apply_depth_update(&mut book, &diff), ApplyResult::Outdated);

    // Book should remain unchanged
    assert_eq!(book.last_update_id, 3);
    assert_eq!(book.bids.get("1.0").unwrap(), "1.0");
}

#[test]
fn skips_update_with_gap() {
    let snapshot_json = r#"{"lastUpdateId":1,"bids":[],"asks":[]}"#;
    let snapshot: DepthSnapshot = serde_json::from_str(snapshot_json).unwrap();
    let mut book: OrderBook = snapshot.into();

    // first_update_id (3) is greater than expected (2)
    let diff_json = r#"{"E":0,"s":"BTCUSDT","U":3,"u":4,"pu":1,"b":[],"a":[]}"#;
    let diff: DepthUpdateEvent<'_> = serde_json::from_str(diff_json).unwrap();

    assert_eq!(apply_depth_update(&mut book, &diff), ApplyResult::Gap);

    // Book should remain unchanged
    assert_eq!(book.last_update_id, 1);
}

#[test]
fn resyncs_after_gap() {
    // initial book at update 1
    let snapshot_json = r#"{"lastUpdateId":1,"bids":[["1.0","1.0"]],"asks":[]}"#;
    let snapshot: DepthSnapshot = serde_json::from_str(snapshot_json).unwrap();
    let mut book: OrderBook = snapshot.into();

    // Contiguous update to 2
    let diff1_json = r#"{"E":0,"s":"BTCUSDT","U":2,"u":2,"pu":1,"b":[["1.1","1.0"]],"a":[]}"#;
    let diff1: DepthUpdateEvent<'_> = serde_json::from_str(diff1_json).unwrap();
    assert_eq!(apply_depth_update(&mut book, &diff1), ApplyResult::Applied);

    // Gap detected: previous_final_update_id (3) does not match last_update_id (2)
    let gap_json = r#"{"E":0,"s":"BTCUSDT","U":4,"u":4,"pu":3,"b":[["1.2","1.0"]],"a":[]}"#;
    let gap_diff: DepthUpdateEvent<'_> = serde_json::from_str(gap_json).unwrap();
    assert_eq!(apply_depth_update(&mut book, &gap_diff), ApplyResult::Gap);

    // Fetch snapshot at update 3 and fast-forward buffered delta
    let snap2_json = r#"{"lastUpdateId":3,"bids":[["1.0","1.0"],["1.1","1.0"]],"asks":[]}"#;
    let snap2: DepthSnapshot = serde_json::from_str(snap2_json).unwrap();
    let mut new_book: OrderBook = snap2.into();
    fast_forward(&mut new_book, &[gap_diff]);

    assert_eq!(new_book.last_update_id, 4);
    assert!(new_book.bids.contains_key("1.2"));
}
