use anyhow::Result;
use async_trait::async_trait;
use canonical::MdEvent;

use super::traits::Sink;

/// Placeholder NATS sink.
pub struct NatsSink;

impl NatsSink {
    pub fn new() -> Self {
        NatsSink
    }
}

#[async_trait]
impl Sink for NatsSink {
    async fn publish_batch(&self, _events: &[MdEvent]) -> Result<()> {
        // In a real implementation this would publish to NATS.
        Ok(())
    }
}
