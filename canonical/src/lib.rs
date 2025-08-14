use rust_decimal::prelude::ToPrimitive;
use std::convert::TryFrom;

pub use arb_core::events;
pub use arb_core::events::{Event, StreamMessage, MexcEvent, MexcStreamMessage, BookTickerEvent};
use events::{DepthUpdateEvent, TradeEvent};

#[derive(Debug, Clone, PartialEq)]
pub enum MdEvent {
    Trade(Trade),
    Book(Book),
    Ticker(Ticker),
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

#[derive(Debug, Clone, PartialEq)]
pub struct Ticker {
    pub symbol: String,
    pub bid_price: f64,
    pub bid_quantity: f64,
    pub ask_price: f64,
    pub ask_quantity: f64,
    pub event_time: u64,
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
            Event::BookTicker(e) => Ok(MdEvent::from(e)),
            _ => Err(()),
        }
    }
}

impl<'a> From<BookTickerEvent<'a>> for Ticker {
    fn from(ev: BookTickerEvent<'a>) -> Self {
        let bid_price = ev.best_bid_price_decimal().to_f64().unwrap_or_default();
        let bid_quantity = ev.best_bid_qty_decimal().to_f64().unwrap_or_default();
        let ask_price = ev.best_ask_price_decimal().to_f64().unwrap_or_default();
        let ask_quantity = ev.best_ask_qty_decimal().to_f64().unwrap_or_default();
        Self {
            symbol: ev.symbol,
            bid_price,
            bid_quantity,
            ask_price,
            ask_quantity,
            event_time: 0,
        }
    }
}

impl<'a> From<BookTickerEvent<'a>> for MdEvent {
    fn from(ev: BookTickerEvent<'a>) -> Self {
        MdEvent::Ticker(ev.into())
    }
}

impl<'a> TryFrom<MexcStreamMessage<'a>> for MdEvent {
    type Error = ();
    fn try_from(msg: MexcStreamMessage<'a>) -> Result<Self, Self::Error> {
        match msg.data {
            MexcEvent::Trades { data } => {
                let deal = data.deals.first().ok_or(())?;
                let price: f64 = deal.price.parse().ok().ok_or(())?;
                let quantity: f64 = deal.quantity.parse().ok().ok_or(())?;
                let side = match deal.trade_type {
                    1 => Some(Side::Buy),
                    2 => Some(Side::Sell),
                    _ => None,
                };
                Ok(MdEvent::Trade(Trade {
                    symbol: msg.symbol,
                    price,
                    quantity,
                    trade_id: None,
                    buyer_order_id: None,
                    seller_order_id: None,
                    timestamp: deal.trade_time.saturating_mul(1_000_000),
                    side,
                }))
            }
            MexcEvent::Depth { data } => {
                let bids = data
                    .bids
                    .into_iter()
                    .filter_map(|lvl| {
                        Some(Level {
                            price: lvl.price.parse().ok()?,
                            quantity: lvl.quantity.parse().ok()?,
                            kind: BookKind::Bid,
                        })
                    })
                    .collect();
                let asks = data
                    .asks
                    .into_iter()
                    .filter_map(|lvl| {
                        Some(Level {
                            price: lvl.price.parse().ok()?,
                            quantity: lvl.quantity.parse().ok()?,
                            kind: BookKind::Ask,
                        })
                    })
                    .collect();
                Ok(MdEvent::Book(Book {
                    symbol: msg.symbol,
                    bids,
                    asks,
                    event_time: msg.event_time.saturating_mul(1_000_000),
                    first_update_id: None,
                    final_update_id: None,
                    previous_final_update_id: None,
                }))
            }
            MexcEvent::BookTicker { data } => {
                let bid_price: f64 = data.bid_price.parse().ok().ok_or(())?;
                let bid_quantity: f64 = data.bid_qty.parse().ok().ok_or(())?;
                let ask_price: f64 = data.ask_price.parse().ok().ok_or(())?;
                let ask_quantity: f64 = data.ask_qty.parse().ok().ok_or(())?;
                Ok(MdEvent::Ticker(Ticker {
                    symbol: msg.symbol,
                    bid_price,
                    bid_quantity,
                    ask_price,
                    ask_quantity,
                    event_time: msg.event_time.saturating_mul(1_000_000),
                }))
            }
        }
    }
}
