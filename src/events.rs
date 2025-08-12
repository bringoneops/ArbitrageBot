use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct StreamMessage<T> {
    pub stream: String,
    pub data: T,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
pub enum Event {
    #[serde(rename = "trade")]
    Trade(TradeEvent),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
pub struct TradeEvent {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub trade_id: u64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
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
