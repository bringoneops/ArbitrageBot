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
