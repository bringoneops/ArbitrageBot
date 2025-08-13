use canonical::events::{Event, StreamMessage};
use canonical::{BookKind, MdEvent, Side};

#[test]
fn parses_trade_event() {
    let json = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":123456789,"s":"BTCUSDT","t":12345,"p":"0.001","q":"100","b":88,"a":50,"T":123456785,"m":true,"M":true}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    let md = MdEvent::try_from(msg.data).expect("failed to convert");
    match md {
        MdEvent::Trade(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.trade_id, Some(12345));
            assert_eq!(ev.side, Some(Side::Sell));
            assert_eq!(ev.timestamp, 123456785000000);
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn handles_unknown_event() {
    let json = r#"{"stream":"test","data":{"e":"mystery"}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    assert!(matches!(msg.data, Event::Unknown));
}

#[test]
fn parses_agg_trade_event() {
    let json = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":2,"p":"0.1","q":"2","f":1,"l":2,"T":3,"m":true,"M":true}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
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
    let json = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":[["1.0","2.0"],["1.5","3.0"]],"a":[["2.0","1.0"],["2.5","4.0"]]}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    let md = MdEvent::try_from(msg.data).expect("failed to convert");
    match md {
        MdEvent::Book(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.event_time, 1_000_000);
            assert_eq!(ev.bids[0].price, 1.0);
            assert_eq!(ev.bids[0].quantity, 2.0);
            assert_eq!(ev.bids[0].kind, BookKind::Bid);
            assert_eq!(ev.asks[0].price, 2.0);
            assert_eq!(ev.asks[0].quantity, 1.0);
            assert_eq!(ev.asks[0].kind, BookKind::Ask);
            assert_eq!(ev.previous_final_update_id, Some(1));
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_kline_event() {
    let json = r#"{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1,"s":"BTCUSDT","k":{"t":2,"T":3,"i":"1m","o":"0.1","c":"0.2","h":"0.3","l":"0.0","v":"100","n":10,"x":false,"q":"200","V":"50","Q":"25"}}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
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
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
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
    let json = r#"{"stream":"btcusdt@ticker","data":{"e":"24hrTicker","E":1,"s":"BTCUSDT","p":"0.1","P":"1","w":"0.6","x":"0.5","c":"0.7","Q":"4","b":"0.69","B":"5","a":"0.71","A":"6","o":"0.6","h":"0.8","l":"0.5","v":"1000","q":"600","O":10,"C":20,"F":100,"L":200,"n":300}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::Ticker(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.last_price, "0.7");
            assert_eq!(ev.prev_close_price, "0.5");
            assert_eq!(ev.last_qty, "4");
            assert_eq!(ev.best_bid_price, "0.69");
            assert_eq!(ev.best_ask_qty, "6");
            assert_eq!(ev.open_time, 10);
            assert_eq!(ev.close_time, 20);
            assert_eq!(ev.first_trade_id, 100);
            assert_eq!(ev.last_trade_id, 200);
            assert_eq!(ev.count, 300);
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_book_ticker_event() {
    let json = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"BTCUSDT","b":"0.1","B":"2","a":"0.2","A":"3"}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::BookTicker(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.best_bid_price, "0.1");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_mark_price_event() {
    let json = r#"{"stream":"btcusdt@markPrice","data":{"e":"markPriceUpdate","E":1,"s":"BTCUSDT","p":"1.0","i":"1.1","r":"0.001","T":2,"P":"1.0"}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::MarkPrice(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.mark_price, "1.0");
            assert_eq!(ev.index_price, "1.1");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_index_price_event() {
    let json = r#"{"stream":"btcusdt@indexPrice","data":{"e":"indexPriceUpdate","E":1,"s":"BTCUSDT","p":"1.1"}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::IndexPrice(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.index_price, "1.1");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_mark_price_kline_event() {
    let json = r#"{"stream":"btcusdt@markPriceKline_1m","data":{"e":"markPriceKline","E":1,"s":"BTCUSDT","k":{"t":2,"T":3,"i":"1m","o":"0.1","c":"0.2","h":"0.3","l":"0.0","v":"100","n":10,"x":false,"q":"200","V":"50","Q":"25"}}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::MarkPriceKline(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.kline.open, "0.1");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_index_price_kline_event() {
    let json = r#"{"stream":"btcusdt@indexPriceKline_1m","data":{"e":"indexPriceKline","E":1,"s":"BTCUSDT","k":{"t":2,"T":3,"i":"1m","o":"0.1","c":"0.2","h":"0.3","l":"0.0","v":"100","n":10,"x":false,"q":"200","V":"50","Q":"25"}}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::IndexPriceKline(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.kline.close, "0.2");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_continuous_kline_event() {
    let json = r#"{"stream":"btcusdt@continuousKline_1m_perpetual","data":{"e":"continuous_kline","E":1,"ps":"BTCUSDT","ct":"PERPETUAL","k":{"t":2,"T":3,"i":"1m","o":"0.1","c":"0.2","h":"0.3","l":"0.0","v":"100","n":10,"x":false,"q":"200","V":"50","Q":"25"}}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::ContinuousKline(ev) => {
            assert_eq!(ev.pair, "BTCUSDT");
            assert_eq!(ev.contract_type, "PERPETUAL");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_force_order_event() {
    let json = r#"{"stream":"btcusdt@forceOrder","data":{"e":"forceOrder","E":1,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.1","p":"10000","ap":"10000","X":"FILLED","l":"0.1","z":"0.1","T":2,"L":"10000","t":1,"b":"0","a":"0","m":false,"R":false}}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::ForceOrder(ev) => {
            assert_eq!(ev.order.symbol, "BTCUSDT");
            assert_eq!(ev.order.side, "SELL");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_force_order_arr_event() {
    let json = r#"{"stream":"forceOrder@arr","data":{"e":"forceOrder","E":1,"o":{"s":"ETHUSDT","S":"BUY","o":"LIMIT","f":"IOC","q":"1","p":"2000","ap":"2000","X":"FILLED","l":"1","z":"1","T":2,"L":"2000","t":2,"b":"0","a":"0","m":true,"R":false}}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::ForceOrder(ev) => {
            assert_eq!(ev.order.symbol, "ETHUSDT");
            assert!(ev.order.is_maker);
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_greeks_event() {
    let json = r#"{"stream":"btcusdt@greeks","data":{"e":"greeks","E":1,"s":"BTCUSDT","d":"0.1","g":"0.2","v":"0.3","t":"0.4","r":"0.5"}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::Greeks(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.delta, "0.1");
            assert_eq!(ev.gamma, "0.2");
            assert_eq!(ev.vega, "0.3");
            assert_eq!(ev.theta, "0.4");
            assert_eq!(ev.rho.unwrap(), "0.5");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_open_interest_event() {
    let json = r#"{"stream":"btcusdt@openInterest","data":{"e":"openInterest","E":1,"s":"BTCUSDT","o":"123"}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::OpenInterest(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.open_interest, "123");
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn parses_implied_volatility_event() {
    let json = r#"{"stream":"btcusdt@impliedVolatility","data":{"e":"impliedVolatility","E":1,"s":"BTCUSDT","v":"0.6"}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(json).expect("failed to parse");
    match msg.data {
        Event::ImpliedVolatility(ev) => {
            assert_eq!(ev.symbol, "BTCUSDT");
            assert_eq!(ev.implied_volatility, "0.6");
        }
        _ => panic!("unexpected event"),
    }
}
