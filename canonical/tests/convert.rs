use canonical::{
    events::{Event, MexcStreamMessage, TradeEvent},
    BookKind, MdEvent, Side, Trade,
};
use std::borrow::Cow;
use serde_json::json;

#[test]
fn trade_event_to_canonical() {
    let trade_event = TradeEvent {
        event_time: 1,
        symbol: "BTCUSD".to_string(),
        trade_id: 1,
        price: Cow::Borrowed("100.0"),
        quantity: Cow::Borrowed("1.5"),
        buyer_order_id: 10,
        seller_order_id: 20,
        trade_time: 1,
        buyer_is_maker: false,
        best_match: true,
    };
    let md: MdEvent = MdEvent::from(trade_event);
    match md {
        MdEvent::Trade(ref t) => {
            assert_eq!(t.symbol, "BTCUSD");
            assert_eq!(t.price, 100.0);
        }
        _ => panic!("expected trade"),
    }
    let trade_event2 = TradeEvent {
        event_time: 1,
        symbol: "BTCUSD".to_string(),
        trade_id: 1,
        price: Cow::Borrowed("100.0"),
        quantity: Cow::Borrowed("1.5"),
        buyer_order_id: 10,
        seller_order_id: 20,
        trade_time: 1,
        buyer_is_maker: false,
        best_match: true,
    };
    let event = Event::Trade(trade_event2);
    let trade = Trade::try_from(event).unwrap();
    assert_eq!(trade.quantity, 1.5);
}

#[test]
fn mexc_trade_event_to_canonical() {
    let msg: MexcStreamMessage<'_> = serde_json::from_value(json!({
        "channel": "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT",
        "publicdeals": {
            "dealsList": [{
                "price": "93220.00",
                "quantity": "0.04438243",
                "tradetype": 2,
                "time": 1736409765051u64
            }],
            "eventtype": "spot@public.aggre.deals.v3.api.pb@100ms"
        },
        "symbol": "BTCUSDT",
        "sendtime": 1736409765052u64
    })).unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md {
        MdEvent::Trade(t) => {
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.price, 93220.0);
            assert_eq!(t.side, Some(Side::Sell));
        }
        _ => panic!("expected trade"),
    }
}

#[test]
fn mexc_depth_event_to_canonical() {
    let msg: MexcStreamMessage<'_> = serde_json::from_value(json!({
        "channel": "spot@public.aggre.depth.v3.api.pb@100ms@BTCUSDT",
        "publicincreasedepths": {
            "asksList": [{"price": "92877.58", "quantity": "0.00000000"}],
            "bidsList": [{"price": "92876.00", "quantity": "0.10000000"}],
            "eventtype": "spot@public.aggre.depth.v3.api.pb@100ms",
            "fromVersion": "10589632359",
            "toVersion": "10589632359"
        },
        "symbol": "BTCUSDT",
        "sendtime": 1736411507002u64
    })).unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md {
        MdEvent::Book(b) => {
            assert_eq!(b.symbol, "BTCUSDT");
            assert_eq!(b.bids[0].price, 92876.0);
            assert_eq!(b.bids[0].kind, BookKind::Bid);
            assert_eq!(b.asks[0].kind, BookKind::Ask);
        }
        _ => panic!("expected book"),
    }
}

#[test]
fn mexc_ticker_event_to_canonical() {
    let msg: MexcStreamMessage<'_> = serde_json::from_value(json!({
        "channel": "spot@public.aggre.bookTicker.v3.api.pb@100ms@BTCUSDT",
        "publicbookticker": {
            "bidprice": "93387.28",
            "bidquantity": "3.73485",
            "askprice": "93387.29",
            "askquantity": "7.669875"
        },
        "symbol": "BTCUSDT",
        "sendtime": 1736412092433u64
    })).unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md {
        MdEvent::Ticker(t) => {
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.bid_price, 93387.28);
            assert_eq!(t.ask_price, 93387.29);
        }
        _ => panic!("expected ticker"),
    }
}
