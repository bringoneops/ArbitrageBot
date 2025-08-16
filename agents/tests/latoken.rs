use agents::adapter::latoken::parse_message;
use arb_core::events::Event;

#[test]
fn latoken_parse_trade_message() {
    let json = r#"{
        "channel":"trades",
        "symbol":"BTCUSDT",
        "data":{"id":1,"price":"100.0","qty":"0.5","ts":1,"side":"buy"}
    }"#;
    let msg = parse_message(json).unwrap();
    match msg.data {
        Event::Trade(t) => {
            assert_eq!(t.price, "100.0");
            assert_eq!(t.quantity, "0.5");
            assert_eq!(t.trade_id, 1);
        }
        _ => panic!("expected trade event"),
    }
}

#[test]
fn latoken_parse_depth_message() {
    let json = r#"{
        "channel":"book",
        "symbol":"BTCUSDT",
        "data":{"bids":[["1","2"]],"asks":[["3","4"]],"ts":1,"seq_start":1,"seq_end":2}
    }"#;
    let msg = parse_message(json).unwrap();
    match msg.data {
        Event::DepthUpdate(d) => {
            assert_eq!(d.bids[0][0], "1");
            assert_eq!(d.asks[0][0], "3");
            assert_eq!(d.first_update_id, 1);
            assert_eq!(d.final_update_id, 2);
        }
        _ => panic!("expected depth update"),
    }
}
