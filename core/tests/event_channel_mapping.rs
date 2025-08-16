use arb_core::events::Channel;
use canonical::{
    AvgPrice, BookKind, BookTicker, DepthL2Update, DepthSnapshot, FundingRate, IndexPrice, Kline,
    Level, Liquidation, MarkPrice, MdEvent, MiniTicker, OpenInterest, Trade,
};

fn sample_level(kind: BookKind) -> Level {
    Level {
        price: 0.0,
        quantity: 0.0,
        kind,
    }
}

#[test]
fn channel_enum_matches_events() {
    let trade = Trade {
        exchange: "ex".into(),
        symbol: "sym".into(),
        price: 0.0,
        quantity: 0.0,
        trade_id: None,
        buyer_order_id: None,
        seller_order_id: None,
        timestamp: 0,
        side: None,
    };
    assert_eq!(trade.channel(), Channel::Trade);

    let book = BookTicker {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        bid_price: 0.0,
        bid_quantity: 0.0,
        ask_price: 0.0,
        ask_quantity: 0.0,
    };
    assert_eq!(book.channel(), Channel::Book);

    let mini = MiniTicker {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        open: 0.0,
        high: 0.0,
        low: 0.0,
        close: 0.0,
        volume: 0.0,
        quote_volume: 0.0,
    };
    assert_eq!(mini.channel(), Channel::MiniTicker);

    let kline = Kline {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        open: 0.0,
        close: 0.0,
        high: 0.0,
        low: 0.0,
        volume: 0.0,
    };
    assert_eq!(kline.channel(), Channel::Kline);

    let depth_update = DepthL2Update {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        bids: vec![sample_level(BookKind::Bid)],
        asks: vec![sample_level(BookKind::Ask)],
        first_update_id: None,
        final_update_id: None,
        previous_final_update_id: None,
    };
    assert_eq!(depth_update.channel(), Channel::Depth);

    let depth_snapshot = DepthSnapshot {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        last_update_id: 0,
        bids: vec![sample_level(BookKind::Bid)],
        asks: vec![sample_level(BookKind::Ask)],
    };
    assert_eq!(depth_snapshot.channel(), Channel::Depth);

    let avg_price = AvgPrice {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        price: 0.0,
    };
    assert_eq!(avg_price.channel(), Channel::AvgPrice);

    let mark_price = MarkPrice {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        price: 0.0,
    };
    assert_eq!(mark_price.channel(), Channel::MarkPrice);

    let index_price = IndexPrice {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        price: 0.0,
    };
    assert_eq!(index_price.channel(), Channel::IndexPrice);

    let funding = FundingRate {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        rate: 0.0,
    };
    assert_eq!(funding.channel(), Channel::FundingRate);

    let oi = OpenInterest {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        open_interest: 0.0,
    };
    assert_eq!(oi.channel(), Channel::OpenInterest);

    let liq = Liquidation {
        exchange: "ex".into(),
        symbol: "sym".into(),
        ts: 0,
        price: 0.0,
        quantity: 0.0,
    };
    assert_eq!(liq.channel(), Channel::Liquidation);

    let md: MdEvent = MdEvent::Trade(trade);
    assert_eq!(md.channel(), Channel::Trade);
}
