use arb_core::events::{Event, TradeEvent};
use std::borrow::Cow;

#[test]
fn trade_event_helpers() {
    let trade = TradeEvent {
        event_time: 42,
        symbol: "BTCUSD".to_string(),
        trade_id: 1,
        price: Cow::Borrowed("1"),
        quantity: Cow::Borrowed("2"),
        buyer_order_id: 1,
        seller_order_id: 2,
        trade_time: 42,
        buyer_is_maker: false,
        best_match: true,
    };
    let event = Event::Trade(trade);
    assert_eq!(event.event_time(), Some(42));
    assert_eq!(event.symbol(), Some("BTCUSD"));
}
