use anyhow::Result;
use async_trait::async_trait;

/// Trait implemented by exchange adapters to provide a unified interface
/// for connecting to different exchanges.
#[async_trait]
pub trait ExchangeAdapter {
    /// Subscribe to the necessary streams for the exchange.
    async fn subscribe(&mut self) -> Result<()>;

    /// Run the adapter. This typically performs authentication, backfilling
    /// and subscription.
    async fn run(&mut self) -> Result<()>;

    /// Perform any heartbeat handling required by the exchange.
    async fn heartbeat(&mut self) -> Result<()>;

    /// Authenticate with the exchange if required.
    async fn auth(&mut self) -> Result<()>;

    /// Backfill initial state before streaming live updates.
    async fn backfill(&mut self) -> Result<()>;
}

pub mod binance;
pub mod coinex;
pub mod gateio;
pub mod bitmart;
pub mod bitget;
pub mod latoken;
pub mod mexc;
