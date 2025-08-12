use binance_us_and_global::events::{Event, StreamMessage};

#[test]
fn parses_trade_event() {
    let json = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":123456789,"s":"BTCUSDT","t":12345,"p":"0.001","q":"100","b":88,"a":50,"T":123456785,"m":true,"M":true}}"#;
    let msg: StreamMessage<Event> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::Trade(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.trade_id, 12345);
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn handles_unknown_event() {
    let json = r#"{"stream":"test","data":{"e":"mystery"}}"#;
    let msg: StreamMessage<Event> = serde_json::from_str(json).expect("failed to parse");
    assert!(matches!(msg.data, Event::Unknown));
}

#[test]
fn parses_agg_trade_event() {
    let json = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":2,"p":"0.1","q":"2","f":1,"l":2,"T":3,"m":true,"M":true}}"#;
    let msg: StreamMessage<Event> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::AggTrade(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.agg_trade_id, 2);
        }
        _ => panic!("unexpected event"),
    }
}
