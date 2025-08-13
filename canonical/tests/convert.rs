use canonical::{events::{Event, TradeEvent}, MdEvent, Trade};
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
