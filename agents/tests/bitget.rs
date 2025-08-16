use agents::adapter::bitget::handle_text;
use arb_core::events::Event;
use serde_json::json;

#[test]
fn bitget_parse_trade_and_print() {
    let raw = r#"{
        "action":"snapshot",
        "arg":{"instType":"SP","channel":"trade","instId":"BTCUSDT"},
        "data":[{"ts":"1620000000000","price":"50000","size":"0.1","side":"buy"}]
    }"#;
    if let Some(msg) = handle_text(raw) {
        if let Event::Trade(ev) = msg.data {
            assert_eq!(ev.price, "50000");
            assert_eq!(ev.quantity, "0.1");
            println!(
                "{}",
                json!({
                    "stream": msg.stream,
                    "data": {"e":"trade","s":ev.symbol,"p":ev.price,"q":ev.quantity}
                })
            );
        } else {
            panic!("expected trade");
        }
    } else {
        panic!("no event");
    }
}

#[test]
fn bitget_parse_depth_and_print() {
    let raw = r#"{
        "action":"snapshot",
        "arg":{"instType":"SP","channel":"books","instId":"BTCUSDT"},
        "data":[{"ts":"1620000000000","bids":[["49900","1"],["49800","2"]],"asks":[["50100","1.5"],["50200","2"]]}]
    }"#;
    if let Some(msg) = handle_text(raw) {
        if let Event::DepthUpdate(ev) = msg.data {
            assert_eq!(ev.bids.len(), 2);
            assert_eq!(ev.asks.len(), 2);
            println!(
                "{}",
                json!({
                    "stream": msg.stream,
                    "data": {"e":"depthUpdate","s":ev.symbol,"b":ev.bids,"a":ev.asks}
                })
            );
        } else {
            panic!("expected depth");
        }
    } else {
        panic!("no event");
    }
}
