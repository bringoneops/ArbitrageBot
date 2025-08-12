use binance_us_and_global::{apply_depth_update, DepthSnapshot, OrderBook};
use binance_us_and_global::events::DepthUpdateEvent;

#[test]
fn merges_snapshot_and_diff() {
    let snapshot_json = r#"{"lastUpdateId":1,"bids":[["1.0","1.0"]],"asks":[["2.0","2.0"]]}"#;
    let snapshot: DepthSnapshot = serde_json::from_str(snapshot_json).unwrap();
    let mut book: OrderBook = snapshot.into();

    let diff_json = r#"{"E":0,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":[["1.0","0.5"],["0.9","1.0"]],"a":[["2.0","0"],["2.1","2.5"]]}"#;
    let diff: DepthUpdateEvent = serde_json::from_str(diff_json).unwrap();

    apply_depth_update(&mut book, &diff);

    assert_eq!(book.last_update_id, 3);
    assert_eq!(book.bids.get("1.0").unwrap(), "0.5");
    assert_eq!(book.bids.get("0.9").unwrap(), "1.0");
    assert!(!book.asks.contains_key("2.0"));
    assert_eq!(book.asks.get("2.1").unwrap(), "2.5");
}
