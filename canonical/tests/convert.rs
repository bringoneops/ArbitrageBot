use arb_core::DepthSnapshot as CoreDepthSnapshot;
use canonical::{
    events::{
        BingxStreamMessage, BitgetStreamMessage, BitmartStreamMessage, CoinexStreamMessage, Event,
        DepthUpdateEvent, ForceOrder, ForceOrderEvent, FundingRateEvent, GateioStreamMessage, IndexPriceEvent,
        Kline as EventKline, KlineEvent, KucoinStreamMessage, LatokenStreamMessage,
        LbankStreamMessage, MarkPriceEvent, MexcStreamMessage, MiniTickerEvent, OpenInterestEvent,
        TickerEvent, TradeEvent, XtStreamMessage,
    },
    AvgPrice, BookTicker, DepthL2Update, DepthSnapshot as CanonDepthSnapshot,
    FundingRate as CanonFundingRate, IndexPrice as CanonIndexPrice, Kline as CanonKline,
    Liquidation as CanonLiquidation, MarkPrice as CanonMarkPrice, MdEvent, MdEventKind,
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
    match md.event {
        MdEventKind::Trade(ref t) => {
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
    match md.event {
        MdEventKind::Trade(t) => {
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
    match md.event {
        MdEventKind::DepthL2Update(b) => {
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
fn negative_quantity_rejected() {
    let trade_event = TradeEvent {
        event_time: 1,
        symbol: "BTCUSD".to_string(),
        trade_id: 1,
        price: Cow::Borrowed("100.0"),
        quantity: Cow::Borrowed("-1.0"),
        buyer_order_id: 10,
        seller_order_id: 20,
        trade_time: 1,
        buyer_is_maker: false,
        best_match: true,
    };
    let event = Event::Trade(trade_event);
    assert!(MdEvent::try_from(event).is_err());
}

#[test]
fn unsorted_book_rejected() {
    let depth_event = DepthUpdateEvent {
        event_time: 1,
        symbol: "BTCUSD".to_string(),
        first_update_id: 1,
        final_update_id: 1,
        previous_final_update_id: 0,
        bids: vec![
            [Cow::Borrowed("1.0"), Cow::Borrowed("1.0")],
            [Cow::Borrowed("2.0"), Cow::Borrowed("1.0")],
        ],
        asks: vec![
            [Cow::Borrowed("1.0"), Cow::Borrowed("1.0")],
            [Cow::Borrowed("0.5"), Cow::Borrowed("1.0")],
        ],
    };
    let event = Event::DepthUpdate(depth_event);
    assert!(MdEvent::try_from(event).is_err());
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
    match md.event {
        MdEventKind::BookTicker(t) => {
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
fn bingx_trade_event_to_canonical() {
    let msg: BingxStreamMessage<'_> = serde_json::from_value(json!({
        "e": "trade",
        "E": 1u64,
        "s": "BTCUSDT",
        "t": 10u64,
        "p": "93200.0",
        "q": "0.5",
        "b": 1u64,
        "a": 2u64,
        "T": 1u64,
        "m": true
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Trade(t) => {
            assert_eq!(t.exchange, "bingx");
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.price, 93200.0);
            assert_eq!(t.side, Some(Side::Sell));
            let s = serde_json::to_string(&t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected trade"),
    }
}

#[test]
fn bingx_depth_event_to_canonical() {
    let msg: BingxStreamMessage<'_> = serde_json::from_value(json!({
        "e": "depthUpdate",
        "E": 1u64,
        "s": "BTCUSDT",
        "U": 100u64,
        "u": 101u64,
        "b": [["93200.0", "1.0"]],
        "a": [["93300.0", "2.0"]]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::DepthL2Update(b) => {
            assert_eq!(b.exchange, "bingx");
            assert_eq!(b.symbol, "BTCUSDT");
            assert_eq!(b.bids[0].price, 93200.0);
            let s = serde_json::to_string(&b).unwrap();
            let de: DepthL2Update = serde_json::from_str(&s).unwrap();
            assert_eq!(b, de);
        }
        _ => panic!("expected depth"),
    }
}

#[test]
fn xt_trade_event_to_canonical() {
    let msg: XtStreamMessage<'_> = serde_json::from_value(json!({
        "topic": "BTC_USDT@trade",
        "data": [{
            "i": 1u64,
            "p": "100.0",
            "q": "0.5",
            "T": 1u64,
            "m": true
        }]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Trade(t) => {
            assert_eq!(t.exchange, "xt");
            assert_eq!(t.symbol, "BTC_USDT");
            assert_eq!(t.price, 100.0);
            assert_eq!(t.side, Some(Side::Sell));
            let s = serde_json::to_string(&t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected trade"),
    }
}

#[test]
fn xt_depth_event_to_canonical() {
    let msg: XtStreamMessage<'_> = serde_json::from_value(json!({
        "topic": "BTC_USDT@depth",
        "data": {
            "t": 1u64,
            "b": [["100.0", "1.0"]],
            "a": [["101.0", "2.0"]]
        }
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::DepthL2Update(b) => {
            assert_eq!(b.exchange, "xt");
            assert_eq!(b.symbol, "BTC_USDT");
            assert_eq!(b.bids[0].price, 100.0);
            let s = serde_json::to_string(&b).unwrap();
            let de: DepthL2Update = serde_json::from_str(&s).unwrap();
            assert_eq!(b, de);
        }
        _ => panic!("expected depth"),
    }
}

#[test]
fn xt_kline_event_to_canonical() {
    let msg: XtStreamMessage<'_> = serde_json::from_value(json!({
        "topic": "BTC_USDT@kline",
        "data": {
            "t": 1u64,
            "o": "90",
            "c": "100",
            "h": "110",
            "l": "80",
            "v": "1000"
        }
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Kline(k) => {
            assert_eq!(k.exchange, "xt");
            assert_eq!(k.symbol, "BTC_USDT");
            assert_eq!(k.open, 90.0);
            let s = serde_json::to_string(&k).unwrap();
            let de: CanonKline = serde_json::from_str(&s).unwrap();
            assert_eq!(k, de);
        }
        _ => panic!("expected kline"),
    }
}

#[test]
fn xt_ticker_event_to_canonical() {
    let msg: XtStreamMessage<'_> = serde_json::from_value(json!({
        "topic": "BTC_USDT@ticker",
        "data": {
            "t": 1u64,
            "bp": "100",
            "bq": "1",
            "ap": "101",
            "aq": "2"
        }
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::BookTicker(t) => {
            assert_eq!(t.exchange, "xt");
            assert_eq!(t.symbol, "BTC_USDT");
            assert_eq!(t.bid_price, 100.0);
            let s = serde_json::to_string(&t).unwrap();
            let de: BookTicker = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected book ticker"),
    }
}

#[test]
fn latoken_trade_event_to_canonical() {
    let msg: LatokenStreamMessage<'_> = serde_json::from_value(json!({
        "topic": "trade",
        "symbol": "BTCUSDT",
        "data": {"t": 1u64, "p": "100.0", "q": "0.5", "m": true}
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Trade(t) => {
            assert_eq!(t.exchange, "latoken");
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.price, 100.0);
            assert_eq!(t.side, Some(Side::Sell));
            let s = serde_json::to_string(&t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected trade"),
    }
}

#[test]
fn latoken_depth_event_to_canonical() {
    let msg: LatokenStreamMessage<'_> = serde_json::from_value(json!({
        "topic": "depth",
        "symbol": "BTCUSDT",
        "data": {"t": 1u64, "b": [["100.0", "1.0"]], "a": [["101.0", "2.0"]]}
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::DepthL2Update(b) => {
            assert_eq!(b.exchange, "latoken");
            assert_eq!(b.symbol, "BTCUSDT");
            assert_eq!(b.bids[0].price, 100.0);
            let s = serde_json::to_string(&b).unwrap();
            let de: DepthL2Update = serde_json::from_str(&s).unwrap();
            assert_eq!(b, de);
        }
        _ => panic!("expected depth"),
    }
}

#[test]
fn latoken_kline_event_to_canonical() {
    let msg: LatokenStreamMessage<'_> = serde_json::from_value(json!({
        "topic": "kline",
        "symbol": "BTCUSDT",
        "data": {"t": 1u64, "o": "90", "c": "100", "h": "110", "l": "80", "v": "1000"}
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Kline(k) => {
            assert_eq!(k.exchange, "latoken");
            assert_eq!(k.symbol, "BTCUSDT");
            assert_eq!(k.open, 90.0);
            let s = serde_json::to_string(&k).unwrap();
            let de: CanonKline = serde_json::from_str(&s).unwrap();
            assert_eq!(k, de);
        }
        _ => panic!("expected kline"),
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
    match md.event {
        MdEventKind::MiniTicker(t) => {
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
    match md.event {
        MdEventKind::Kline(k) => {
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
    match md.event {
        MdEventKind::AvgPrice(p) => {
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
    match md.event {
        MdEventKind::MarkPrice(p) => {
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
    match md.event {
        MdEventKind::IndexPrice(p) => {
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
    match md.event {
        MdEventKind::FundingRate(f) => {
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
    match md.event {
        MdEventKind::OpenInterest(o) => {
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
    match md.event {
        MdEventKind::Liquidation(l) => {
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
    match md.event {
        MdEventKind::Trade(t) => {
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
    match md.event {
        MdEventKind::DepthL2Update(b) => {
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
    match md.event {
        MdEventKind::Kline(k) => {
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

#[test]
fn kucoin_trade_message_to_canonical() {
    let msg: KucoinStreamMessage = serde_json::from_value(json!({
        "type": "message",
        "topic": "/market/match:BTC-USDT",
        "subject": "trade.l3match",
        "data": {
            "sequence": 1u64,
            "symbol": "BTC-USDT",
            "side": "buy",
            "size": "0.1",
            "price": "100.0",
            "takerOrderId": "1",
            "makerOrderId": "2",
            "time": 1u64
        }
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Trade(t) => {
            assert_eq!(t.exchange, "kucoin");
            assert_eq!(t.symbol, "BTC-USDT");
            assert_eq!(t.price, 100.0);
            assert_eq!(t.side, Some(Side::Buy));
            let s = serde_json::to_string(&t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected trade"),
    }
}

#[test]
fn kucoin_depth_message_to_canonical() {
    let msg: KucoinStreamMessage = serde_json::from_value(json!({
        "type": "message",
        "topic": "/market/level2:BTC-USDT",
        "subject": "trade.l2update",
        "data": {
            "sequenceStart": 1u64,
            "sequenceEnd": 2u64,
            "symbol": "BTC-USDT",
            "changes": {
                "bids": [["100.0", "1.0", "1"]],
                "asks": [["101.0", "2.0", "2"]]
            }
        }
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::DepthL2Update(b) => {
            assert_eq!(b.exchange, "kucoin");
            assert_eq!(b.symbol, "BTC-USDT");
            assert_eq!(b.bids[0].price, 100.0);
            let s = serde_json::to_string(&b).unwrap();
            let de: DepthL2Update = serde_json::from_str(&s).unwrap();
            assert_eq!(b, de);
        }
        _ => panic!("expected depth"),
    }
}

#[test]
fn kucoin_kline_message_to_canonical() {
    let msg: KucoinStreamMessage = serde_json::from_value(json!({
        "type": "message",
        "topic": "/market/candles:1min:BTC-USDT",
        "subject": "trade.candles.update",
        "data": {
            "candles": ["1", "10.0", "12.0", "13.0", "9.0", "100.0", "1000.0"],
            "symbol": "BTC-USDT",
            "time": 1u64
        }
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Kline(k) => {
            assert_eq!(k.exchange, "kucoin");
            assert_eq!(k.symbol, "BTC-USDT");
            assert_eq!(k.open, 10.0);
            let s = serde_json::to_string(&k).unwrap();
            let de: CanonKline = serde_json::from_str(&s).unwrap();
            assert_eq!(k, de);
        }
        _ => panic!("expected kline"),
    }
}

#[test]
fn bitget_trade_message_to_canonical() {
    let msg: BitgetStreamMessage<'_> = serde_json::from_value(json!({
        "arg": {"channel": "trade", "instId": "BTCUSDT"},
        "data": [{"price": "100.0", "vol": "0.5", "side": "buy", "ts": 1u64, "tradeId": "1"}]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Trade(t) => {
            assert_eq!(t.exchange, "bitget");
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.price, 100.0);
            assert_eq!(t.side, Some(Side::Buy));
            let s = serde_json::to_string(&t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected trade"),
    }
}

#[test]
fn bitget_depth_message_to_canonical() {
    let msg: BitgetStreamMessage<'_> = serde_json::from_value(json!({
        "arg": {"channel": "depth", "instId": "BTCUSDT"},
        "data": [{
            "bids": [["100.0", "1.0"]],
            "asks": [["101.0", "2.0"]],
            "ts": 1u64
        }]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::DepthL2Update(b) => {
            assert_eq!(b.exchange, "bitget");
            assert_eq!(b.symbol, "BTCUSDT");
            assert_eq!(b.bids[0].price, 100.0);
            let s = serde_json::to_string(&b).unwrap();
            let de: DepthL2Update = serde_json::from_str(&s).unwrap();
            assert_eq!(b, de);
        }
        _ => panic!("expected depth"),
    }
}

#[test]
fn bitget_ticker_message_to_canonical() {
    let msg: BitgetStreamMessage<'_> = serde_json::from_value(json!({
        "arg": {"channel": "ticker", "instId": "BTCUSDT"},
        "data": [{"bidPri": "100.0", "bidSz": "1.0", "askPri": "101.0", "askSz": "2.0", "ts": 1u64}]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::BookTicker(t) => {
            assert_eq!(t.exchange, "bitget");
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.bid_price, 100.0);
            let s = serde_json::to_string(&t).unwrap();
            let de: BookTicker = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected ticker"),
    }
}

#[test]
fn bitget_kline_message_to_canonical() {
    let msg: BitgetStreamMessage<'_> = serde_json::from_value(json!({
        "arg": {"channel": "candle1m", "instId": "BTCUSDT"},
        "data": [["1", "100.0", "110.0", "90.0", "105.0", "10.0"]]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Kline(k) => {
            assert_eq!(k.exchange, "bitget");
            assert_eq!(k.symbol, "BTCUSDT");
            assert_eq!(k.open, 100.0);
            let s = serde_json::to_string(&k).unwrap();
            let de: CanonKline = serde_json::from_str(&s).unwrap();
            assert_eq!(k, de);
        }
        _ => panic!("expected kline"),
    }
}

#[test]
fn bitmart_trade_message_to_canonical() {
    let msg: BitmartStreamMessage<'_> = serde_json::from_value(json!({
        "table": "spot/trade",
        "data": [{
            "symbol": "BTC_USDT",
            "price": "100.0",
            "size": "0.5",
            "side": "buy",
            "time": 1u64
        }]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Trade(t) => {
            assert_eq!(t.exchange, "bitmart");
            assert_eq!(t.symbol, "BTC_USDT");
            assert_eq!(t.price, 100.0);
            assert_eq!(t.side, Some(Side::Buy));
            let s = serde_json::to_string(&t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected trade"),
    }
}

#[test]
fn bitmart_depth_message_to_canonical() {
    let msg: BitmartStreamMessage<'_> = serde_json::from_value(json!({
        "table": "spot/depth5",
        "data": [{
            "symbol": "BTC_USDT",
            "ms_t": 1u64,
            "bids": [["100.0", "1.0"]],
            "asks": [["101.0", "2.0"]],
            "version": 2u64,
            "prev_version": 1u64
        }]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::DepthL2Update(b) => {
            assert_eq!(b.exchange, "bitmart");
            assert_eq!(b.symbol, "BTC_USDT");
            assert_eq!(b.bids[0].price, 100.0);
            let s = serde_json::to_string(&b).unwrap();
            let de: DepthL2Update = serde_json::from_str(&s).unwrap();
            assert_eq!(b, de);
        }
        _ => panic!("expected depth"),
    }
}

#[test]
fn bitmart_ticker_message_to_canonical() {
    let msg: BitmartStreamMessage<'_> = serde_json::from_value(json!({
        "table": "spot/ticker",
        "data": [{
            "symbol": "BTC_USDT",
            "ms_t": 1u64,
            "best_bid": "100.0",
            "best_bid_size": "1.0",
            "best_ask": "101.0",
            "best_ask_size": "2.0"
        }]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::BookTicker(t) => {
            assert_eq!(t.exchange, "bitmart");
            assert_eq!(t.symbol, "BTC_USDT");
            assert_eq!(t.bid_price, 100.0);
            let s = serde_json::to_string(&t).unwrap();
            let de: BookTicker = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected book ticker"),
    }
}

#[test]
fn bitmart_kline_message_to_canonical() {
    let msg: BitmartStreamMessage<'_> = serde_json::from_value(json!({
        "table": "spot/kline1m",
        "data": [{
            "symbol": "BTC_USDT",
            "ms_t": 1u64,
            "open_price": "90",
            "close_price": "100",
            "high_price": "110",
            "low_price": "80",
            "volume": "1000"
        }]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Kline(k) => {
            assert_eq!(k.exchange, "bitmart");
            assert_eq!(k.symbol, "BTC_USDT");
            assert_eq!(k.open, 90.0);
            let s = serde_json::to_string(&k).unwrap();
            let de: CanonKline = serde_json::from_str(&s).unwrap();
            assert_eq!(k, de);
        }
        _ => panic!("expected kline"),
    }
}

#[test]
fn bitmart_funding_rate_message_to_canonical() {
    let msg: BitmartStreamMessage<'_> = serde_json::from_value(json!({
        "table": "futures/fundingRate",
        "data": [{
            "symbol": "BTC_USDT",
            "fundingRate": "0.01",
            "fundingTime": 1u64
        }]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::FundingRate(f) => {
            assert_eq!(f.exchange, "bitmart");
            assert_eq!(f.symbol, "BTC_USDT");
            assert_eq!(f.rate, 0.01);
            let s = serde_json::to_string(&f).unwrap();
            let de: CanonFundingRate = serde_json::from_str(&s).unwrap();
            assert_eq!(f, de);
        }
        _ => panic!("expected funding rate"),
    }
}

#[test]
fn coinex_trade_event_to_canonical() {
    let msg: CoinexStreamMessage<'_> = serde_json::from_value(json!({
        "method": "deals.update",
        "params": [
            "BTCUSDT",
            [{"id": 1u64, "time": 1u64, "price": "93200.0", "amount": "0.5", "side": "sell"}]
        ]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Trade(t) => {
            assert_eq!(t.exchange, "coinex");
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.price, 93200.0);
            assert_eq!(t.side, Some(Side::Sell));
            let s = serde_json::to_string(&t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected trade"),
    }
}

#[test]
fn coinex_depth_event_to_canonical() {
    let msg: CoinexStreamMessage<'_> = serde_json::from_value(json!({
        "method": "depth.update",
        "params": [
            "BTCUSDT",
            {"bids": [["93200.0", "1.0"]], "asks": [["93300.0", "2.0"]], "ts": 1u64}
        ]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::DepthL2Update(b) => {
            assert_eq!(b.exchange, "coinex");
            assert_eq!(b.symbol, "BTCUSDT");
            assert_eq!(b.bids[0].price, 93200.0);
            let s = serde_json::to_string(&b).unwrap();
            let de: DepthL2Update = serde_json::from_str(&s).unwrap();
            assert_eq!(b, de);
        }
        _ => panic!("expected depth"),
    }
}

#[test]
fn coinex_bbo_event_to_canonical() {
    let msg: CoinexStreamMessage<'_> = serde_json::from_value(json!({
        "method": "bbo.update",
        "params": [
            "BTCUSDT",
            {"b": "93200.0", "B": "1.0", "a": "93201.0", "A": "2.0"}
        ]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::BookTicker(t) => {
            assert_eq!(t.exchange, "coinex");
            assert_eq!(t.symbol, "BTCUSDT");
            assert_eq!(t.bid_price, 93200.0);
            let s = serde_json::to_string(&t).unwrap();
            let de: BookTicker = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected book ticker"),
    }
}

#[test]
fn coinex_index_event_to_canonical() {
    let msg: CoinexStreamMessage<'_> = serde_json::from_value(json!({
        "method": "index.update",
        "params": ["BTCUSDT", "93200.0"]
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::IndexPrice(p) => {
            assert_eq!(p.exchange, "coinex");
            assert_eq!(p.symbol, "BTCUSDT");
            assert_eq!(p.price, 93200.0);
            let s = serde_json::to_string(&p).unwrap();
            let de: CanonIndexPrice = serde_json::from_str(&s).unwrap();
            assert_eq!(p, de);
        }
        _ => panic!("expected index price"),
    }
}

#[test]
fn lbank_trade_message_to_canonical() {
    let msg: LbankStreamMessage<'_> = serde_json::from_value(json!({
        "type": "trade",
        "pair": "btc_usdt",
        "trade": {"price": "93200.0", "volume": "0.5", "direction": "buy"}
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Trade(t) => {
            assert_eq!(t.exchange, "lbank");
            assert_eq!(t.symbol, "btc_usdt");
            assert_eq!(t.price, 93200.0);
            assert_eq!(t.quantity, 0.5);
            assert_eq!(t.side, Some(Side::Buy));
            let s = serde_json::to_string(&t).unwrap();
            let de: Trade = serde_json::from_str(&s).unwrap();
            assert_eq!(t, de);
        }
        _ => panic!("expected trade"),
    }
}

#[test]
fn lbank_depth_message_to_canonical() {
    let msg: LbankStreamMessage<'_> = serde_json::from_value(json!({
        "type": "depth",
        "pair": "btc_usdt",
        "depth": {
            "bids": [["93200.0", "1.0"]],
            "asks": [["93300.0", "2.0"]]
        }
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::DepthL2Update(b) => {
            assert_eq!(b.exchange, "lbank");
            assert_eq!(b.symbol, "btc_usdt");
            assert_eq!(b.bids[0].price, 93200.0);
            let s = serde_json::to_string(&b).unwrap();
            let de: DepthL2Update = serde_json::from_str(&s).unwrap();
            assert_eq!(b, de);
        }
        _ => panic!("expected depth"),
    }
}

#[test]
fn lbank_kbar_message_to_canonical() {
    let msg: LbankStreamMessage<'_> = serde_json::from_value(json!({
        "type": "kbar",
        "pair": "btc_usdt",
        "kbar": {
            "o": "93000.0",
            "h": "93300.0",
            "l": "92900.0",
            "c": "93200.0",
            "v": "10.0",
            "n": 100
        }
    }))
    .unwrap();
    let md = MdEvent::try_from(msg).unwrap();
    match md.event {
        MdEventKind::Kline(k) => {
            assert_eq!(k.exchange, "lbank");
            assert_eq!(k.symbol, "btc_usdt");
            assert_eq!(k.open, 93000.0);
            let s = serde_json::to_string(&k).unwrap();
            let de: CanonKline = serde_json::from_str(&s).unwrap();
            assert_eq!(k, de);
        }
        _ => panic!("expected kline"),
    }
}
