use serde::Deserialize;
use std::borrow::Cow;
use std::str::FromStr;
use rust_decimal::Decimal;

#[derive(Debug, Deserialize)]
pub struct StreamMessage<'a> {
    pub stream: String,
    pub data: Event<'a>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
pub enum Event<'a> {
    #[serde(rename = "trade")]
    Trade(TradeEvent<'a>),
    #[serde(rename = "aggTrade")]
    AggTrade(AggTradeEvent<'a>),
    #[serde(rename = "depthUpdate")]
    DepthUpdate(DepthUpdateEvent<'a>),
    #[serde(rename = "kline")]
    Kline(KlineEvent<'a>),
    #[serde(rename = "24hrMiniTicker")]
    MiniTicker(MiniTickerEvent<'a>),
    #[serde(rename = "24hrTicker")]
    Ticker(TickerEvent<'a>),
    #[serde(rename = "bookTicker")]
    BookTicker(BookTickerEvent<'a>),
    #[serde(rename = "indexPriceUpdate")]
    IndexPrice(IndexPriceEvent<'a>),
    #[serde(rename = "markPriceUpdate")]
    MarkPrice(MarkPriceEvent<'a>),
    #[serde(rename = "markPriceKline")]
    MarkPriceKline(MarkPriceKlineEvent<'a>),
    #[serde(rename = "indexPriceKline")]
    IndexPriceKline(IndexPriceKlineEvent<'a>),
    #[serde(rename = "continuous_kline")]
    ContinuousKline(ContinuousKlineEvent<'a>),
    #[serde(rename = "forceOrder")]
    ForceOrder(ForceOrderEvent<'a>),
    #[serde(rename = "greeks")]
    Greeks(GreeksEvent<'a>),
    #[serde(rename = "openInterest")]
    OpenInterest(OpenInterestEvent<'a>),
    #[serde(rename = "impliedVolatility")]
    ImpliedVolatility(ImpliedVolatilityEvent<'a>),
    #[serde(other)]
    Unknown,
}

impl<'a> Event<'a> {
    /// Returns the event timestamp if available.
    pub fn event_time(&self) -> Option<u64> {
        match self {
            Event::Trade(e) => Some(e.event_time),
            Event::AggTrade(e) => Some(e.event_time),
            Event::DepthUpdate(e) => Some(e.event_time),
            Event::Kline(e) => Some(e.event_time),
            Event::MiniTicker(e) => Some(e.event_time),
            Event::Ticker(e) => Some(e.event_time),
            Event::IndexPrice(e) => Some(e.event_time),
            Event::MarkPrice(e) => Some(e.event_time),
            Event::MarkPriceKline(e) => Some(e.event_time),
            Event::IndexPriceKline(e) => Some(e.event_time),
            Event::ContinuousKline(e) => Some(e.event_time),
            Event::ForceOrder(e) => Some(e.event_time),
            Event::Greeks(e) => Some(e.event_time),
            Event::OpenInterest(e) => Some(e.event_time),
            Event::ImpliedVolatility(e) => Some(e.event_time),
            Event::BookTicker(_) | Event::Unknown => None,
        }
    }

    /// Returns the symbol or pair identifier if available.
    pub fn symbol(&self) -> Option<&str> {
        match self {
            Event::Trade(e) => Some(&e.symbol),
            Event::AggTrade(e) => Some(&e.symbol),
            Event::DepthUpdate(e) => Some(&e.symbol),
            Event::Kline(e) => Some(&e.symbol),
            Event::MiniTicker(e) => Some(&e.symbol),
            Event::Ticker(e) => Some(&e.symbol),
            Event::BookTicker(e) => Some(&e.symbol),
            Event::IndexPrice(e) => Some(&e.symbol),
            Event::MarkPrice(e) => Some(&e.symbol),
            Event::MarkPriceKline(e) => Some(&e.symbol),
            Event::IndexPriceKline(e) => Some(&e.symbol),
            Event::ContinuousKline(e) => Some(&e.pair),
            Event::ForceOrder(e) => Some(&e.order.symbol),
            Event::Greeks(e) => Some(&e.symbol),
            Event::OpenInterest(e) => Some(&e.symbol),
            Event::ImpliedVolatility(e) => Some(&e.symbol),
            Event::Unknown => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TradeEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub trade_id: u64,
    #[serde(rename = "p")]
    pub price: Cow<'a, str>,
    #[serde(rename = "q")]
    pub quantity: Cow<'a, str>,
    #[serde(rename = "b")]
    pub buyer_order_id: u64,
    #[serde(rename = "a")]
    pub seller_order_id: u64,
    #[serde(rename = "T")]
    pub trade_time: u64,
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
    #[serde(rename = "M")]
    pub best_match: bool,
}

#[derive(Debug, Deserialize)]
pub struct AggTradeEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "a")]
    pub agg_trade_id: u64,
    #[serde(rename = "p")]
    pub price: Cow<'a, str>,
    #[serde(rename = "q")]
    pub quantity: Cow<'a, str>,
    #[serde(rename = "f")]
    pub first_trade_id: u64,
    #[serde(rename = "l")]
    pub last_trade_id: u64,
    #[serde(rename = "T")]
    pub trade_time: u64,
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
    #[serde(rename = "M")]
    pub best_match: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DepthUpdateEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub final_update_id: u64,
    #[serde(rename = "pu")]
    pub previous_final_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<[Cow<'a, str>; 2]>,
    #[serde(rename = "a")]
    pub asks: Vec<[Cow<'a, str>; 2]>,
}

#[derive(Debug, Deserialize)]
pub struct KlineEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: Kline<'a>,
}

#[derive(Debug, Deserialize)]
pub struct Kline<'a> {
    #[serde(rename = "t")]
    pub start_time: u64,
    #[serde(rename = "T")]
    pub close_time: u64,
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "o")]
    pub open: Cow<'a, str>,
    #[serde(rename = "c")]
    pub close: Cow<'a, str>,
    #[serde(rename = "h")]
    pub high: Cow<'a, str>,
    #[serde(rename = "l")]
    pub low: Cow<'a, str>,
    #[serde(rename = "v")]
    pub volume: Cow<'a, str>,
    #[serde(rename = "n")]
    pub trades: u64,
    #[serde(rename = "x")]
    pub is_closed: bool,
    #[serde(rename = "q")]
    pub quote_volume: Cow<'a, str>,
    #[serde(rename = "V")]
    pub taker_buy_base_volume: Cow<'a, str>,
    #[serde(rename = "Q")]
    pub taker_buy_quote_volume: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
pub struct MiniTickerEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub close_price: Cow<'a, str>,
    #[serde(rename = "o")]
    pub open_price: Cow<'a, str>,
    #[serde(rename = "h")]
    pub high_price: Cow<'a, str>,
    #[serde(rename = "l")]
    pub low_price: Cow<'a, str>,
    #[serde(rename = "v")]
    pub volume: Cow<'a, str>,
    #[serde(rename = "q")]
    pub quote_volume: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
pub struct TickerEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub price_change: Cow<'a, str>,
    #[serde(rename = "P")]
    pub price_change_percent: Cow<'a, str>,
    #[serde(rename = "w")]
    pub weighted_avg_price: Cow<'a, str>,
    #[serde(rename = "x")]
    pub prev_close_price: Cow<'a, str>,
    #[serde(rename = "c")]
    pub last_price: Cow<'a, str>,
    #[serde(rename = "Q")]
    pub last_qty: Cow<'a, str>,
    #[serde(rename = "b")]
    pub best_bid_price: Cow<'a, str>,
    #[serde(rename = "B")]
    pub best_bid_qty: Cow<'a, str>,
    #[serde(rename = "a")]
    pub best_ask_price: Cow<'a, str>,
    #[serde(rename = "A")]
    pub best_ask_qty: Cow<'a, str>,
    #[serde(rename = "o")]
    pub open_price: Cow<'a, str>,
    #[serde(rename = "h")]
    pub high_price: Cow<'a, str>,
    #[serde(rename = "l")]
    pub low_price: Cow<'a, str>,
    #[serde(rename = "v")]
    pub volume: Cow<'a, str>,
    #[serde(rename = "q")]
    pub quote_volume: Cow<'a, str>,
    #[serde(rename = "O")]
    pub open_time: u64,
    #[serde(rename = "C")]
    pub close_time: u64,
    #[serde(rename = "F")]
    pub first_trade_id: u64,
    #[serde(rename = "L")]
    pub last_trade_id: u64,
    #[serde(rename = "n")]
    pub count: u64,
}

#[derive(Debug, Deserialize)]
pub struct BookTickerEvent<'a> {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub best_bid_price: Cow<'a, str>,
    #[serde(rename = "B")]
    pub best_bid_qty: Cow<'a, str>,
    #[serde(rename = "a")]
    pub best_ask_price: Cow<'a, str>,
    #[serde(rename = "A")]
    pub best_ask_qty: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
pub struct IndexPriceEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p", alias = "i")]
    pub index_price: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
pub struct MarkPriceEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub mark_price: Cow<'a, str>,
    #[serde(rename = "i")]
    pub index_price: Cow<'a, str>,
    #[serde(rename = "r")]
    pub funding_rate: Cow<'a, str>,
    #[serde(rename = "T")]
    pub next_funding_time: u64,
    #[serde(rename = "P", default)]
    pub estimated_settle_price: Option<Cow<'a, str>>,
}

#[derive(Debug, Deserialize)]
pub struct MarkPriceKlineEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: Kline<'a>,
}

#[derive(Debug, Deserialize)]
pub struct IndexPriceKlineEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: Kline<'a>,
}

#[derive(Debug, Deserialize)]
pub struct ContinuousKlineEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "ps")]
    pub pair: String,
    #[serde(rename = "ct")]
    pub contract_type: String,
    #[serde(rename = "k")]
    pub kline: Kline<'a>,
}

#[derive(Debug, Deserialize)]
pub struct ForceOrderEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "o")]
    pub order: ForceOrder<'a>,
}

#[derive(Debug, Deserialize)]
pub struct ForceOrder<'a> {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "o")]
    pub order_type: String,
    #[serde(rename = "f")]
    pub time_in_force: String,
    #[serde(rename = "q")]
    pub original_quantity: Cow<'a, str>,
    #[serde(rename = "p")]
    pub price: Cow<'a, str>,
    #[serde(rename = "ap")]
    pub average_price: Cow<'a, str>,
    #[serde(rename = "X")]
    pub status: String,
    #[serde(rename = "l")]
    pub last_filled_quantity: Cow<'a, str>,
    #[serde(rename = "z")]
    pub filled_accumulated_quantity: Cow<'a, str>,
    #[serde(rename = "T")]
    pub trade_time: u64,
    #[serde(rename = "L")]
    pub last_filled_price: Cow<'a, str>,
    #[serde(rename = "t")]
    pub trade_id: u64,
    #[serde(rename = "b")]
    pub bids_notional: Cow<'a, str>,
    #[serde(rename = "a")]
    pub ask_notional: Cow<'a, str>,
    #[serde(rename = "m")]
    pub is_maker: bool,
    #[serde(rename = "R")]
    pub reduce_only: bool,
}

#[derive(Debug, Deserialize)]
pub struct GreeksEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "d")]
    pub delta: Cow<'a, str>,
    #[serde(rename = "g")]
    pub gamma: Cow<'a, str>,
    #[serde(rename = "v")]
    pub vega: Cow<'a, str>,
    #[serde(rename = "t")]
    pub theta: Cow<'a, str>,
    #[serde(rename = "r", default)]
    pub rho: Option<Cow<'a, str>>,
}

#[derive(Debug, Deserialize)]
pub struct OpenInterestEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "o")]
    pub open_interest: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
pub struct ImpliedVolatilityEvent<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "v")]
    pub implied_volatility: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
pub struct MexcStreamMessage<'a> {
    pub channel: String,
    #[serde(flatten)]
    pub data: MexcEvent<'a>,
    pub symbol: String,
    #[serde(rename = "sendtime")]
    pub event_time: u64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum MexcEvent<'a> {
    Trades { #[serde(rename = "publicdeals")] data: MexcTrades<'a> },
    Depth { #[serde(rename = "publicincreasedepths")] data: MexcDepth<'a> },
    BookTicker { #[serde(rename = "publicbookticker")] data: MexcBookTicker<'a> },
}

#[derive(Debug, Deserialize)]
pub struct MexcTrades<'a> {
    #[serde(rename = "dealsList")]
    pub deals: Vec<MexcDeal<'a>>,
}

#[derive(Debug, Deserialize)]
pub struct MexcDeal<'a> {
    pub price: Cow<'a, str>,
    pub quantity: Cow<'a, str>,
    #[serde(rename = "tradetype")]
    pub trade_type: u8,
    #[serde(rename = "time")]
    pub trade_time: u64,
}

#[derive(Debug, Deserialize)]
pub struct MexcDepth<'a> {
    #[serde(rename = "bidsList")]
    pub bids: Vec<MexcLevel<'a>>,
    #[serde(rename = "asksList")]
    pub asks: Vec<MexcLevel<'a>>,
    #[serde(rename = "fromVersion", default)]
    pub from_version: Option<String>,
    #[serde(rename = "toVersion", default)]
    pub to_version: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MexcLevel<'a> {
    pub price: Cow<'a, str>,
    pub quantity: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
pub struct MexcBookTicker<'a> {
    #[serde(rename = "bidprice")]
    pub bid_price: Cow<'a, str>,
    #[serde(rename = "bidquantity")]
    pub bid_qty: Cow<'a, str>,
    #[serde(rename = "askprice")]
    pub ask_price: Cow<'a, str>,
    #[serde(rename = "askquantity")]
    pub ask_qty: Cow<'a, str>,
}

fn parse_decimal(s: &Cow<'_, str>) -> Decimal {
    Decimal::from_str(s.as_ref()).unwrap_or_default()
}

macro_rules! decimal_accessors {
    ($ty:ident { $($field:ident => $name:ident),* $(,)? }) => {
        impl<'a> $ty<'a> {
            $(pub fn $name(&self) -> Decimal { parse_decimal(&self.$field) })*
        }
    };
}

macro_rules! decimal_option_accessors {
    ($ty:ident { $($field:ident => $name:ident),* $(,)? }) => {
        impl<'a> $ty<'a> {
            $(pub fn $name(&self) -> Option<Decimal> {
                self.$field.as_ref().map(|s| parse_decimal(s))
            })*
        }
    };
}

decimal_accessors!(TradeEvent { price => price_decimal, quantity => quantity_decimal });
decimal_accessors!(AggTradeEvent { price => price_decimal, quantity => quantity_decimal });
decimal_accessors!(Kline { open => open_decimal, close => close_decimal, high => high_decimal, low => low_decimal, volume => volume_decimal, quote_volume => quote_volume_decimal, taker_buy_base_volume => taker_buy_base_volume_decimal, taker_buy_quote_volume => taker_buy_quote_volume_decimal });
decimal_accessors!(MiniTickerEvent { close_price => close_price_decimal, open_price => open_price_decimal, high_price => high_price_decimal, low_price => low_price_decimal, volume => volume_decimal, quote_volume => quote_volume_decimal });
decimal_accessors!(TickerEvent { price_change => price_change_decimal, price_change_percent => price_change_percent_decimal, weighted_avg_price => weighted_avg_price_decimal, prev_close_price => prev_close_price_decimal, last_price => last_price_decimal, last_qty => last_qty_decimal, best_bid_price => best_bid_price_decimal, best_bid_qty => best_bid_qty_decimal, best_ask_price => best_ask_price_decimal, best_ask_qty => best_ask_qty_decimal, open_price => open_price_decimal, high_price => high_price_decimal, low_price => low_price_decimal, volume => volume_decimal, quote_volume => quote_volume_decimal });
decimal_accessors!(BookTickerEvent { best_bid_price => best_bid_price_decimal, best_bid_qty => best_bid_qty_decimal, best_ask_price => best_ask_price_decimal, best_ask_qty => best_ask_qty_decimal });
decimal_accessors!(IndexPriceEvent { index_price => index_price_decimal });
decimal_accessors!(MarkPriceEvent { mark_price => mark_price_decimal, index_price => index_price_decimal, funding_rate => funding_rate_decimal });
decimal_option_accessors!(MarkPriceEvent { estimated_settle_price => estimated_settle_price_decimal });
decimal_accessors!(ForceOrder { original_quantity => original_quantity_decimal, price => price_decimal, average_price => average_price_decimal, last_filled_quantity => last_filled_quantity_decimal, filled_accumulated_quantity => filled_accumulated_quantity_decimal, last_filled_price => last_filled_price_decimal, bids_notional => bids_notional_decimal, ask_notional => ask_notional_decimal });
decimal_accessors!(GreeksEvent { delta => delta_decimal, gamma => gamma_decimal, vega => vega_decimal, theta => theta_decimal });
decimal_option_accessors!(GreeksEvent { rho => rho_decimal });
decimal_accessors!(OpenInterestEvent { open_interest => open_interest_decimal });
decimal_accessors!(ImpliedVolatilityEvent { implied_volatility => implied_volatility_decimal });

