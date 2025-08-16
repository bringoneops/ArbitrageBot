use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

pub use arb_core::events;
use arb_core::events::Channel;
pub use arb_core::events::{
    BookTickerEvent, Event, KlineEvent, MexcEvent, MexcStreamMessage, MiniTickerEvent,
    StreamMessage, TickerEvent,
};
use arb_core::DepthSnapshot as CoreDepthSnapshot;
use events::{
    DepthUpdateEvent, ForceOrderEvent, FundingRateEvent, IndexPriceEvent, MarkPriceEvent,
    OpenInterestEvent, TradeEvent,
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MdEvent {
    Trade(Trade),
    DepthL2Update(DepthL2Update),
    BookTicker(BookTicker),
    MiniTicker(MiniTicker),
    Kline(Kline),
    DepthSnapshot(DepthSnapshot),
    AvgPrice(AvgPrice),
    MarkPrice(MarkPrice),
    IndexPrice(IndexPrice),
    FundingRate(FundingRate),
    OpenInterest(OpenInterest),
    Liquidation(Liquidation),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Trade {
    pub exchange: String,
    pub symbol: String,
    pub price: f64,
    pub quantity: f64,
    pub trade_id: Option<u64>,
    pub buyer_order_id: Option<u64>,
    pub seller_order_id: Option<u64>,
    pub timestamp: u64,
    pub side: Option<Side>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Level {
    pub price: f64,
    pub quantity: f64,
    pub kind: BookKind,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum BookKind {
    Bid,
    Ask,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DepthL2Update {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub first_update_id: Option<u64>,
    pub final_update_id: Option<u64>,
    pub previous_final_update_id: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct BookTicker {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub bid_price: f64,
    pub bid_quantity: f64,
    pub ask_price: f64,
    pub ask_quantity: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MiniTicker {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Kline {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DepthSnapshot {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub last_update_id: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AvgPrice {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub price: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MarkPrice {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub price: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct IndexPrice {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub price: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FundingRate {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub rate: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct OpenInterest {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub open_interest: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Liquidation {
    pub exchange: String,
    pub symbol: String,
    pub ts: u64,
    pub price: f64,
    pub quantity: f64,
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
            exchange: "binance".to_string(),
            symbol: ev.symbol,
            price,
            quantity,
            trade_id: Some(ev.trade_id),
            buyer_order_id: Some(ev.buyer_order_id),
            seller_order_id: Some(ev.seller_order_id),
            timestamp: ev.trade_time * 1_000_000,
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

impl<'a> From<DepthUpdateEvent<'a>> for DepthL2Update {
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
            exchange: "binance".to_string(),
            symbol: ev.symbol,
            ts: ev.event_time * 1_000_000,
            bids,
            asks,
            first_update_id: Some(ev.first_update_id),
            final_update_id: Some(ev.final_update_id),
            previous_final_update_id: Some(ev.previous_final_update_id),
        }
    }
}

impl<'a> From<DepthUpdateEvent<'a>> for MdEvent {
    fn from(ev: DepthUpdateEvent<'a>) -> Self {
        MdEvent::DepthL2Update(ev.into())
    }
}

impl<'a> From<BookTickerEvent<'a>> for BookTicker {
    fn from(ev: BookTickerEvent<'a>) -> Self {
        let bid_price = ev.best_bid_price_decimal().to_f64().unwrap_or_default();
        let bid_quantity = ev.best_bid_qty_decimal().to_f64().unwrap_or_default();
        let ask_price = ev.best_ask_price_decimal().to_f64().unwrap_or_default();
        let ask_quantity = ev.best_ask_qty_decimal().to_f64().unwrap_or_default();
        Self {
            exchange: "binance".to_string(),
            symbol: ev.symbol,
            ts: 0,
            bid_price,
            bid_quantity,
            ask_price,
            ask_quantity,
        }
    }
}

impl<'a> From<BookTickerEvent<'a>> for MdEvent {
    fn from(ev: BookTickerEvent<'a>) -> Self {
        MdEvent::BookTicker(ev.into())
    }
}

impl<'a> From<MiniTickerEvent<'a>> for MiniTicker {
    fn from(ev: MiniTickerEvent<'a>) -> Self {
        let open = ev.open_price_decimal().to_f64().unwrap_or_default();
        let high = ev.high_price_decimal().to_f64().unwrap_or_default();
        let low = ev.low_price_decimal().to_f64().unwrap_or_default();
        let close = ev.close_price_decimal().to_f64().unwrap_or_default();
        let volume = ev.volume_decimal().to_f64().unwrap_or_default();
        let quote_volume = ev.quote_volume_decimal().to_f64().unwrap_or_default();
        let symbol = ev.symbol;
        Self {
            exchange: "binance".to_string(),
            symbol,
            ts: ev.event_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
        }
    }
}

impl<'a> From<MiniTickerEvent<'a>> for MdEvent {
    fn from(ev: MiniTickerEvent<'a>) -> Self {
        MdEvent::MiniTicker(ev.into())
    }
}

impl<'a> From<KlineEvent<'a>> for Kline {
    fn from(ev: KlineEvent<'a>) -> Self {
        let k = ev.kline;
        Self {
            exchange: "binance".to_string(),
            symbol: ev.symbol,
            ts: ev.event_time,
            open: k.open_decimal().to_f64().unwrap_or_default(),
            close: k.close_decimal().to_f64().unwrap_or_default(),
            high: k.high_decimal().to_f64().unwrap_or_default(),
            low: k.low_decimal().to_f64().unwrap_or_default(),
            volume: k.volume_decimal().to_f64().unwrap_or_default(),
        }
    }
}

impl<'a> From<KlineEvent<'a>> for MdEvent {
    fn from(ev: KlineEvent<'a>) -> Self {
        MdEvent::Kline(ev.into())
    }
}

impl From<CoreDepthSnapshot> for DepthSnapshot {
    fn from(s: CoreDepthSnapshot) -> Self {
        let bids = s
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
        let asks = s
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
            exchange: "binance".to_string(),
            symbol: String::new(),
            ts: 0,
            last_update_id: s.last_update_id,
            bids,
            asks,
        }
    }
}

impl From<CoreDepthSnapshot> for MdEvent {
    fn from(s: CoreDepthSnapshot) -> Self {
        MdEvent::DepthSnapshot(s.into())
    }
}

impl<'a> From<TickerEvent<'a>> for AvgPrice {
    fn from(ev: TickerEvent<'a>) -> Self {
        let price = ev.weighted_avg_price_decimal().to_f64().unwrap_or_default();
        let symbol = ev.symbol;
        Self {
            exchange: "binance".to_string(),
            symbol,
            ts: ev.event_time,
            price,
        }
    }
}

impl<'a> From<TickerEvent<'a>> for MdEvent {
    fn from(ev: TickerEvent<'a>) -> Self {
        MdEvent::AvgPrice(ev.into())
    }
}

impl<'a> From<MarkPriceEvent<'a>> for MarkPrice {
    fn from(ev: MarkPriceEvent<'a>) -> Self {
        let price = ev.mark_price_decimal().to_f64().unwrap_or_default();
        Self {
            exchange: "binance".to_string(),
            symbol: ev.symbol,
            ts: ev.event_time,
            price,
        }
    }
}

impl<'a> From<MarkPriceEvent<'a>> for MdEvent {
    fn from(ev: MarkPriceEvent<'a>) -> Self {
        MdEvent::MarkPrice(ev.into())
    }
}

impl<'a> From<IndexPriceEvent<'a>> for IndexPrice {
    fn from(ev: IndexPriceEvent<'a>) -> Self {
        let price = ev.index_price_decimal().to_f64().unwrap_or_default();
        Self {
            exchange: "binance".to_string(),
            symbol: ev.symbol,
            ts: ev.event_time,
            price,
        }
    }
}

impl<'a> From<IndexPriceEvent<'a>> for MdEvent {
    fn from(ev: IndexPriceEvent<'a>) -> Self {
        MdEvent::IndexPrice(ev.into())
    }
}

impl<'a> From<FundingRateEvent<'a>> for FundingRate {
    fn from(ev: FundingRateEvent<'a>) -> Self {
        let rate = ev.funding_rate_decimal().to_f64().unwrap_or_default();
        Self {
            exchange: "binance".to_string(),
            symbol: ev.symbol,
            ts: ev.event_time,
            rate,
        }
    }
}

impl<'a> From<FundingRateEvent<'a>> for MdEvent {
    fn from(ev: FundingRateEvent<'a>) -> Self {
        MdEvent::FundingRate(ev.into())
    }
}

impl<'a> From<OpenInterestEvent<'a>> for OpenInterest {
    fn from(ev: OpenInterestEvent<'a>) -> Self {
        let open_interest = ev.open_interest_decimal().to_f64().unwrap_or_default();
        Self {
            exchange: "binance".to_string(),
            symbol: ev.symbol,
            ts: ev.event_time,
            open_interest,
        }
    }
}

impl<'a> From<OpenInterestEvent<'a>> for MdEvent {
    fn from(ev: OpenInterestEvent<'a>) -> Self {
        MdEvent::OpenInterest(ev.into())
    }
}

impl<'a> From<ForceOrderEvent<'a>> for Liquidation {
    fn from(ev: ForceOrderEvent<'a>) -> Self {
        let price = ev.order.price_decimal().to_f64().unwrap_or_default();
        let quantity = ev
            .order
            .original_quantity_decimal()
            .to_f64()
            .unwrap_or_default();
        Self {
            exchange: "binance".to_string(),
            symbol: ev.order.symbol,
            ts: ev.event_time,
            price,
            quantity,
        }
    }
}

impl<'a> From<ForceOrderEvent<'a>> for MdEvent {
    fn from(ev: ForceOrderEvent<'a>) -> Self {
        MdEvent::Liquidation(ev.into())
    }
}

impl<'a> TryFrom<Event<'a>> for MdEvent {
    type Error = ();
    fn try_from(ev: Event<'a>) -> Result<Self, Self::Error> {
        match ev {
            Event::Trade(e) => Ok(MdEvent::from(e)),
            Event::DepthUpdate(e) => Ok(MdEvent::from(e)),
            Event::BookTicker(e) => Ok(MdEvent::from(e)),
            Event::MiniTicker(e) => Ok(MdEvent::from(e)),
            Event::Kline(e) => Ok(MdEvent::from(e)),
            Event::Ticker(e) => Ok(MdEvent::from(e)),
            Event::MarkPrice(e) => Ok(MdEvent::from(e)),
            Event::IndexPrice(e) => Ok(MdEvent::from(e)),
            Event::FundingRate(e) => Ok(MdEvent::from(e)),
            Event::OpenInterest(e) => Ok(MdEvent::from(e)),
            Event::ForceOrder(e) => Ok(MdEvent::from(e)),
            _ => Err(()),
        }
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
                    exchange: "mexc".to_string(),
                    symbol: msg.symbol,
                    price,
                    quantity,
                    trade_id: None,
                    buyer_order_id: None,
                    seller_order_id: None,
                    timestamp: deal.trade_time,
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
                Ok(MdEvent::DepthL2Update(DepthL2Update {
                    exchange: "mexc".to_string(),
                    symbol: msg.symbol,
                    ts: msg.event_time,
                    bids,
                    asks,
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
                Ok(MdEvent::BookTicker(BookTicker {
                    exchange: "mexc".to_string(),
                    symbol: msg.symbol,
                    ts: msg.event_time,
                    bid_price,
                    bid_quantity,
                    ask_price,
                    ask_quantity,
                }))
            }
        }
    }
}

impl MdEvent {
    pub fn channel(&self) -> Channel {
        match self {
            MdEvent::Trade(e) => e.channel(),
            MdEvent::DepthL2Update(e) => e.channel(),
            MdEvent::BookTicker(e) => e.channel(),
            MdEvent::MiniTicker(e) => e.channel(),
            MdEvent::Kline(e) => e.channel(),
            MdEvent::DepthSnapshot(e) => e.channel(),
            MdEvent::AvgPrice(e) => e.channel(),
            MdEvent::MarkPrice(e) => e.channel(),
            MdEvent::IndexPrice(e) => e.channel(),
            MdEvent::FundingRate(e) => e.channel(),
            MdEvent::OpenInterest(e) => e.channel(),
            MdEvent::Liquidation(e) => e.channel(),
        }
    }
}

impl Trade {
    pub fn channel(&self) -> Channel {
        Channel::Trade
    }
}

impl DepthL2Update {
    pub fn channel(&self) -> Channel {
        Channel::Depth
    }
}

impl BookTicker {
    pub fn channel(&self) -> Channel {
        Channel::Book
    }
}

impl MiniTicker {
    pub fn channel(&self) -> Channel {
        Channel::MiniTicker
    }
}

impl Kline {
    pub fn channel(&self) -> Channel {
        Channel::Kline
    }
}

impl DepthSnapshot {
    pub fn channel(&self) -> Channel {
        Channel::Depth
    }
}

impl AvgPrice {
    pub fn channel(&self) -> Channel {
        Channel::AvgPrice
    }
}

impl MarkPrice {
    pub fn channel(&self) -> Channel {
        Channel::MarkPrice
    }
}

impl IndexPrice {
    pub fn channel(&self) -> Channel {
        Channel::IndexPrice
    }
}

impl FundingRate {
    pub fn channel(&self) -> Channel {
        Channel::FundingRate
    }
}

impl OpenInterest {
    pub fn channel(&self) -> Channel {
        Channel::OpenInterest
    }
}

impl Liquidation {
    pub fn channel(&self) -> Channel {
        Channel::Liquidation
    }
}
