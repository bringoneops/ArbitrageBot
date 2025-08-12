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

#[test]
fn parses_depth_update_event() {
    let json = r#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"b":[["1.0","2.0"],["1.5","3.0"]],"a":[["2.0","1.0"],["2.5","4.0"]]}}"#;
    let msg: StreamMessage<Event> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::DepthUpdate(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.bids[0][0], "1.0");
            assert_eq!(ev.bids[0][1], "2.0");
            assert_eq!(ev.asks[0][0], "2.0");
            assert_eq!(ev.asks[0][1], "1.0");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_kline_event() {
    let json = r#"{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1,"s":"BTCUSDT","k":{"t":2,"T":3,"i":"1m","o":"0.1","c":"0.2","h":"0.3","l":"0.0","v":"100","n":10,"x":false,"q":"200","V":"50","Q":"25"}}}"#;
    let msg: StreamMessage<Event> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::Kline(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.kline.open, "0.1");
            assert_eq!(ev.kline.high, "0.3");
            assert_eq!(ev.kline.low, "0.0");
            assert_eq!(ev.kline.close, "0.2");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_mini_ticker_event() {
    let json = r#"{"stream":"btcusdt@miniTicker","data":{"e":"24hrMiniTicker","E":1,"s":"BTCUSDT","c":"1","o":"0.5","h":"1.2","l":"0.4","v":"100","q":"80"}}"#;
    let msg: StreamMessage<Event> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::MiniTicker(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.close_price, "1");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_ticker_event() {
    let json = r#"{"stream":"btcusdt@ticker","data":{"e":"24hrTicker","E":1,"s":"BTCUSDT","p":"0.1","P":"1","w":"0.6","c":"0.7","o":"0.6","h":"0.8","l":"0.5","v":"1000","q":"600"}}"#;
    let msg: StreamMessage<Event> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::Ticker(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.last_price, "0.7");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_book_ticker_event() {
    let json = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"BTCUSDT","b":"0.1","B":"2","a":"0.2","A":"3"}}"#;
    let msg: StreamMessage<Event> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::BookTicker(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.best_bid_price, "0.1");
        }
        _ => panic!("unexpected event"),
    }
}
