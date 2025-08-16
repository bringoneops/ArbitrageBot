use arb_core::DepthSnapshot as CoreDepthSnapshot;
use canonical::{
    events::{
        Event, ForceOrder, ForceOrderEvent, FundingRateEvent, IndexPriceEvent, Kline as EventKline,
        KlineEvent, MarkPriceEvent, MexcStreamMessage, MiniTickerEvent, OpenInterestEvent,
        TickerEvent, TradeEvent, GateioStreamMessage,
    },
    AvgPrice, BookTicker, DepthL2Update, DepthSnapshot as CanonDepthSnapshot,
    FundingRate as CanonFundingRate, IndexPrice as CanonIndexPrice, Kline as CanonKline,
    Liquidation as CanonLiquidation, MarkPrice as CanonMarkPrice, MdEvent,
    MiniTicker as CanonMiniTicker, OpenInterest as CanonOpenInterest, Side, Trade,
};
use serde_json::json;
use std::borrow::Cow;

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
            assert_eq!(t.exchange, "binance");
            assert_eq!(t.symbol, "BTCUSD");
            assert_eq!(t.price, 100.0);
            let s = serde_json::to_string(t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(*t, de);
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
    assert_eq!(trade.exchange, "binance");
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
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md {
        MdEvent::Trade(t) => {
            assert_eq!(t.exchange, "mexc");
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.price, 93220.0);
            assert_eq!(t.side, Some(Side::Sell));
            let s = serde_json::to_string(&t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
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
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md {
        MdEvent::DepthL2Update(b) => {
            assert_eq!(b.exchange, "mexc");
            assert_eq!(b.symbol, "BTCUSDT");
            assert_eq!(b.bids[0].price, 92876.0);
            let s = serde_json::to_string(&b).unwrap();
            let de: DepthL2Update = serde_json::from_str(&s).unwrap();
            assert_eq!(b, de);
        }
        _ => panic!("expected depth"),
    }
}

#[test]
fn mexc_book_ticker_event_to_canonical() {
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
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md {
        MdEvent::BookTicker(t) => {
            assert_eq!(t.exchange, "mexc");
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.bid_price, 93387.28);
            let s = serde_json::to_string(&t).unwrap();
            let de: BookTicker = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected book ticker"),
    }
}

#[test]
fn mini_ticker_event_to_canonical() {
    let ev = MiniTickerEvent {
        event_time: 1,
        symbol: "BTCUSDT".to_string(),
        close_price: Cow::Borrowed("10"),
        open_price: Cow::Borrowed("9"),
        high_price: Cow::Borrowed("11"),
        low_price: Cow::Borrowed("8"),
        volume: Cow::Borrowed("100"),
        quote_volume: Cow::Borrowed("1000"),
    };
    let md = MdEvent::from(ev);
    match md {
        MdEvent::MiniTicker(t) => {
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.close, 10.0);
            let s = serde_json::to_string(&t).unwrap();
            let de: CanonMiniTicker = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected mini ticker"),
    }
}

#[test]
fn kline_event_to_canonical() {
    let kline = EventKline {
        start_time: 0,
        close_time: 1,
        interval: "1m".to_string(),
        open: Cow::Borrowed("9"),
        close: Cow::Borrowed("10"),
        high: Cow::Borrowed("11"),
        low: Cow::Borrowed("8"),
        volume: Cow::Borrowed("100"),
        trades: 1,
        is_closed: true,
        quote_volume: Cow::Borrowed("1000"),
        taker_buy_base_volume: Cow::Borrowed("50"),
        taker_buy_quote_volume: Cow::Borrowed("500"),
    };
    let ev = KlineEvent {
        event_time: 1,
        symbol: "BTCUSDT".to_string(),
        kline,
    };
    let md = MdEvent::from(ev);
    match md {
        MdEvent::Kline(k) => {
            assert_eq!(k.symbol, "BTCUSDT");
            assert_eq!(k.close, 10.0);
            let s = serde_json::to_string(&k).unwrap();
            let de: CanonKline = serde_json::from_str(&s).unwrap();
            assert_eq!(k, de);
        }
        _ => panic!("expected kline"),
    }
}

#[test]
fn depth_snapshot_to_canonical() {
    let snap = CoreDepthSnapshot {
        last_update_id: 1,
        bids: vec![["1".to_string(), "2".to_string()]],
        asks: vec![["3".to_string(), "4".to_string()]],
    };
    let ds: CanonDepthSnapshot = snap.into();
    assert_eq!(ds.last_update_id, 1);
    let s = serde_json::to_string(&ds).unwrap();
    let de: CanonDepthSnapshot = serde_json::from_str(&s).unwrap();
    assert_eq!(ds, de);
}

#[test]
fn avg_price_event_to_canonical() {
    let ev = TickerEvent {
        event_time: 1,
        symbol: "BTCUSDT".to_string(),
        price_change: Cow::Borrowed("0"),
        price_change_percent: Cow::Borrowed("0"),
        weighted_avg_price: Cow::Borrowed("10"),
        prev_close_price: Cow::Borrowed("0"),
        last_price: Cow::Borrowed("0"),
        last_qty: Cow::Borrowed("0"),
        best_bid_price: Cow::Borrowed("0"),
        best_bid_qty: Cow::Borrowed("0"),
        best_ask_price: Cow::Borrowed("0"),
        best_ask_qty: Cow::Borrowed("0"),
        open_price: Cow::Borrowed("0"),
        high_price: Cow::Borrowed("0"),
        low_price: Cow::Borrowed("0"),
        volume: Cow::Borrowed("0"),
        quote_volume: Cow::Borrowed("0"),
        open_time: 0,
        close_time: 0,
        first_trade_id: 0,
        last_trade_id: 0,
        count: 0,
    };
    let md = MdEvent::from(ev);
    match md {
        MdEvent::AvgPrice(p) => {
            assert_eq!(p.price, 10.0);
            let s = serde_json::to_string(&p).unwrap();
            let de: AvgPrice = serde_json::from_str(&s).unwrap();
            assert_eq!(p, de);
        }
        _ => panic!("expected avg price"),
    }
}

#[test]
fn mark_price_event_to_canonical() {
    let ev = MarkPriceEvent {
        event_time: 1,
        symbol: "BTCUSDT".to_string(),
        mark_price: Cow::Borrowed("100"),
        index_price: Cow::Borrowed("101"),
        funding_rate: Cow::Borrowed("0.01"),
        next_funding_time: 0,
        estimated_settle_price: None,
    };
    let md = MdEvent::from(ev);
    match md {
        MdEvent::MarkPrice(p) => {
            assert_eq!(p.price, 100.0);
            assert_eq!(p.symbol, "BTCUSDT");
            let s = serde_json::to_string(&p).unwrap();
            let de: CanonMarkPrice = serde_json::from_str(&s).unwrap();
            assert_eq!(p, de);
        }
        _ => panic!("expected mark price"),
    }
}

#[test]
fn index_price_event_to_canonical() {
    let ev = IndexPriceEvent {
        event_time: 1,
        symbol: "BTCUSDT".to_string(),
        index_price: Cow::Borrowed("101"),
    };
    let md = MdEvent::from(ev);
    match md {
        MdEvent::IndexPrice(p) => {
            assert_eq!(p.price, 101.0);
            let s = serde_json::to_string(&p).unwrap();
            let de: CanonIndexPrice = serde_json::from_str(&s).unwrap();
            assert_eq!(p, de);
        }
        _ => panic!("expected index price"),
    }
}

#[test]
fn funding_rate_event_to_canonical() {
    let ev = FundingRateEvent {
        event_time: 1,
        symbol: "BTCUSDT".to_string(),
        funding_rate: Cow::Borrowed("0.01"),
        funding_time: 0,
    };
    let md = MdEvent::from(ev);
    match md {
        MdEvent::FundingRate(f) => {
            assert_eq!(f.rate, 0.01);
            let s = serde_json::to_string(&f).unwrap();
            let de: CanonFundingRate = serde_json::from_str(&s).unwrap();
            assert_eq!(f, de);
        }
        _ => panic!("expected funding rate"),
    }
}

#[test]
fn open_interest_event_to_canonical() {
    let ev = OpenInterestEvent {
        event_time: 1,
        symbol: "BTCUSDT".to_string(),
        open_interest: Cow::Borrowed("1234"),
    };
    let md = MdEvent::from(ev);
    match md {
        MdEvent::OpenInterest(o) => {
            assert_eq!(o.open_interest, 1234.0);
            let s = serde_json::to_string(&o).unwrap();
            let de: CanonOpenInterest = serde_json::from_str(&s).unwrap();
            assert_eq!(o, de);
        }
        _ => panic!("expected open interest"),
    }
}

#[test]
fn force_order_event_to_canonical() {
    let ev = ForceOrderEvent {
        event_time: 1,
        order: ForceOrder {
            symbol: "BTCUSDT".to_string(),
            side: "SELL".to_string(),
            order_type: "LIMIT".to_string(),
            time_in_force: "GTC".to_string(),
            original_quantity: Cow::Borrowed("1"),
            price: Cow::Borrowed("100"),
            average_price: Cow::Borrowed("100"),
            status: "FILLED".to_string(),
            last_filled_quantity: Cow::Borrowed("1"),
            filled_accumulated_quantity: Cow::Borrowed("1"),
            trade_time: 1,
            last_filled_price: Cow::Borrowed("100"),
            trade_id: 1,
            bids_notional: Cow::Borrowed("100"),
            ask_notional: Cow::Borrowed("100"),
            is_maker: true,
            reduce_only: false,
        },
    };
    let md = MdEvent::from(ev);
    match md {
        MdEvent::Liquidation(l) => {
            assert_eq!(l.price, 100.0);
            assert_eq!(l.quantity, 1.0);
            let s = serde_json::to_string(&l).unwrap();
            let de: CanonLiquidation = serde_json::from_str(&s).unwrap();
            assert_eq!(l, de);
        }
        _ => panic!("expected liquidation"),
    }
}

#[test]
fn gateio_trade_message_to_canonical() {
    let s = json!({
        "method": "trades.update",
        "params": [
            "BTC_USDT",
            [{
                "id": 1,
                "create_time_ms": 1736409765000u64,
                "price": "93220.00",
                "amount": "0.044",
                "side": "sell"
            }]
        ]
    })
    .to_string();
    let msg: GateioStreamMessage<'_> = serde_json::from_str(&s).unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md {
        MdEvent::Trade(t) => {
            assert_eq!(t.exchange, "gateio");
            assert_eq!(t.symbol, "BTC_USDT");
            assert_eq!(t.price, 93220.0);
            assert_eq!(t.quantity, 0.044);
            assert_eq!(t.side, Some(Side::Sell));
            let s = serde_json::to_string(&t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected trade"),
    }
}

#[test]
fn gateio_depth_message_to_canonical() {
    let s = json!({
        "method": "depth.update",
        "params": [
            "BTC_USDT",
            {
                "t": 1736411507000u64,
                "bids": [["92876.00", "0.10000000"]],
                "asks": [["92877.58", "0.00000000"]],
                "id": 100
            }
        ]
    })
    .to_string();
    let msg: GateioStreamMessage<'_> = serde_json::from_str(&s).unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md {
        MdEvent::DepthL2Update(b) => {
            assert_eq!(b.exchange, "gateio");
            assert_eq!(b.symbol, "BTC_USDT");
            assert_eq!(b.bids[0].price, 92876.0);
            let s = serde_json::to_string(&b).unwrap();
            let de: DepthL2Update = serde_json::from_str(&s).unwrap();
            assert_eq!(b, de);
        }
        _ => panic!("expected depth"),
    }
}

#[test]
fn gateio_kline_message_to_canonical() {
    let s = json!({
        "method": "kline.update",
        "params": [
            "BTC_USDT",
            [{
                "t": 1736409765000u64,
                "o": "93000.0",
                "c": "93200.0",
                "h": "93300.0",
                "l": "92900.0",
                "v": "10"
            }],
            "1m"
        ]
    })
    .to_string();
    let msg: GateioStreamMessage<'_> = serde_json::from_str(&s).unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md {
        MdEvent::Kline(k) => {
            assert_eq!(k.exchange, "gateio");
            assert_eq!(k.symbol, "BTC_USDT");
            assert_eq!(k.open, 93000.0);
            let s = serde_json::to_string(&k).unwrap();
            let de: CanonKline = serde_json::from_str(&s).unwrap();
            assert_eq!(k, de);
        }
        _ => panic!("expected kline"),
    }
}
