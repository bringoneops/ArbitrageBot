use std::convert::TryFrom;

use crate::events;

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

impl From<events::TradeEvent> for Trade {
    fn from(ev: events::TradeEvent) -> Self {
        let side = if ev.buyer_is_maker {
            Side::Sell
        } else {
            Side::Buy
        };
        Self {
            symbol: ev.symbol,
            price: ev.price.parse().unwrap_or_default(),
            quantity: ev.quantity.parse().unwrap_or_default(),
            trade_id: Some(ev.trade_id),
            buyer_order_id: Some(ev.buyer_order_id),
            seller_order_id: Some(ev.seller_order_id),
            timestamp: ev.trade_time.saturating_mul(1_000_000),
            side: Some(side),
        }
    }
}

impl From<events::TradeEvent> for MdEvent {
    fn from(ev: events::TradeEvent) -> Self {
        MdEvent::Trade(ev.into())
    }
}

impl From<events::DepthUpdateEvent> for Book {
    fn from(ev: events::DepthUpdateEvent) -> Self {
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

impl From<events::DepthUpdateEvent> for MdEvent {
    fn from(ev: events::DepthUpdateEvent) -> Self {
        MdEvent::Book(ev.into())
    }
}

impl TryFrom<events::Event> for MdEvent {
    type Error = ();
    fn try_from(ev: events::Event) -> Result<Self, Self::Error> {
        match ev {
            events::Event::Trade(e) => Ok(MdEvent::from(e)),
            events::Event::DepthUpdate(e) => Ok(MdEvent::from(e)),
            _ => Err(()),
        }
    }
}
