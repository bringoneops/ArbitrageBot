use agents::adapter::lbank::{parse_depth_frame, parse_trade_frame};
use arb_core::events::Event;
use serde_json::Value;

#[test]
fn lbank_parse_trade_and_print() {
    let raw = r#"{
        "trade":{"volume":6.3607,"amount":77148.9303,"price":12129,"direction":"sell","TS":"2019-06-28T19:55:49.460"},
        "type":"trade",
        "pair":"btc_usdt",
        "SERVER":"V2",
        "TS":"2019-06-28T19:55:49.466"
    }"#;
    let v: Value = serde_json::from_str(raw).unwrap();
    let msg = parse_trade_frame(&v).unwrap();
    if let Event::Trade(ev) = &msg.data {
        assert_eq!(ev.price, "12129");
        assert_eq!(ev.quantity, "6.3607");
        let out = serde_json::json!({
            "stream": msg.stream,
            "data": {
                "e": "trade",
                "s": ev.symbol,
                "p": ev.price,
                "q": ev.quantity
            }
        });
        println!("{}", out);
    } else {
        panic!("expected trade event");
    }
}

#[test]
fn lbank_parse_depth_and_print() {
    let raw = r#"{
        "depth":{"asks":[[0.0252,0.5833],[0.025215,4.377]],"bids":[[0.025135,3.962],[0.025134,3.46]]},
        "count":100,
        "type":"depth",
        "pair":"eth_btc",
        "SERVER":"V2",
        "TS":"2019-06-28T17:49:22.722"
    }"#;
    let v: Value = serde_json::from_str(raw).unwrap();
    let msg = parse_depth_frame(&v).unwrap();
    if let Event::DepthUpdate(ev) = &msg.data {
        assert_eq!(ev.bids.len(), 2);
        assert_eq!(ev.asks.len(), 2);
        let out = serde_json::json!({
            "stream": msg.stream,
            "data": {
                "e": "depthUpdate",
                "s": ev.symbol,
                "b": ev.bids,
                "a": ev.asks
            }
        });
        println!("{}", out);
    } else {
        panic!("expected depth event");
    }
}
