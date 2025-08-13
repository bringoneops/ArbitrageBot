use rust_decimal::prelude::ToPrimitive;
use std::convert::TryFrom;

pub use arb_core::events;
pub use arb_core::events::{Event, StreamMessage};
use events::{DepthUpdateEvent, TradeEvent};

#[derive(Debug, Clone, PartialEq)]
pub enum MdEvent {
    Trade(Trade),
    Book(Book),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    pub symbol: String,
    pub price: f64,
    pub quantity: f64,
    pub trade_id: Option<u64>,
    pub buyer_order_id: Option<u64>,
    pub seller_order_id: Option<u64>,
    pub timestamp: u64,
    pub side: Option<Side>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Book {
    pub symbol: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub event_time: u64,
    pub first_update_id: Option<u64>,
    pub final_update_id: Option<u64>,
    pub previous_final_update_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Level {
    pub price: f64,
    pub quantity: f64,
    pub kind: BookKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BookKind {
    Bid,
    Ask,
}

impl<'a> From<TradeEvent<'a>> for Trade {
    fn from(ev: TradeEvent<'a>) -> Self {
        let side = if ev.buyer_is_maker {
            Side::Sell
        } else {
            Side::Buy
        };
        let price = ev.price_decimal().to_f64().unwrap_or_default();
        let quantity = ev.quantity_decimal().to_f64().unwrap_or_default();
        Self {
            symbol: ev.symbol,
            price,
            quantity,
            trade_id: Some(ev.trade_id),
            buyer_order_id: Some(ev.buyer_order_id),
            seller_order_id: Some(ev.seller_order_id),
            timestamp: ev.trade_time.saturating_mul(1_000_000),
            side: Some(side),
        }
    }
}

impl<'a> From<TradeEvent<'a>> for MdEvent {
    fn from(ev: TradeEvent<'a>) -> Self {
        MdEvent::Trade(ev.into())
    }
}

impl<'a> TryFrom<Event<'a>> for Trade {
    type Error = ();
    fn try_from(ev: Event<'a>) -> Result<Self, Self::Error> {
        match ev {
            Event::Trade(e) => Ok(e.into()),
            _ => Err(()),
        }
    }
}

impl<'a> From<DepthUpdateEvent<'a>> for Book {
    fn from(ev: DepthUpdateEvent<'a>) -> Self {
        let bids = ev
            .bids
            .into_iter()
            .filter_map(|[p, q]| {
                Some(Level {
                    price: p.parse().ok()?,
                    quantity: q.parse().ok()?,
                    kind: BookKind::Bid,
                })
            })
            .collect();
        let asks = ev
            .asks
            .into_iter()
            .filter_map(|[p, q]| {
                Some(Level {
                    price: p.parse().ok()?,
                    quantity: q.parse().ok()?,
                    kind: BookKind::Ask,
                })
            })
            .collect();
        Self {
            symbol: ev.symbol,
            bids,
            asks,
            event_time: ev.event_time.saturating_mul(1_000_000),
            first_update_id: Some(ev.first_update_id),
            final_update_id: Some(ev.final_update_id),
            previous_final_update_id: Some(ev.previous_final_update_id),
        }
    }
}

impl<'a> From<DepthUpdateEvent<'a>> for MdEvent {
    fn from(ev: DepthUpdateEvent<'a>) -> Self {
        MdEvent::Book(ev.into())
    }
}

impl<'a> TryFrom<Event<'a>> for Book {
    type Error = ();
    fn try_from(ev: Event<'a>) -> Result<Self, Self::Error> {
        match ev {
            Event::DepthUpdate(e) => Ok(e.into()),
            _ => Err(()),
        }
    }
}

impl<'a> TryFrom<Event<'a>> for MdEvent {
    type Error = ();
    fn try_from(ev: Event<'a>) -> Result<Self, Self::Error> {
        match ev {
            Event::Trade(e) => Ok(MdEvent::from(e)),
            Event::DepthUpdate(e) => Ok(MdEvent::from(e)),
            _ => Err(()),
        }
    }
}
