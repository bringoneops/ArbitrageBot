use anyhow::Result;

pub mod adapter;
pub use adapter::binance::{BinanceAdapter, BINANCE_EXCHANGES};
pub use adapter::ExchangeAdapter;

/// Spawns exchange adapters.
///
/// This is a placeholder implementation to illustrate how the ingestor crate
/// will rely on the `agents` crate for adapter management.
pub async fn spawn_adapters() -> Result<()> {
    Ok(())
}
