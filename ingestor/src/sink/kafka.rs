use anyhow::Result;
use async_trait::async_trait;
use canonical::MdEvent;

use super::traits::Sink;

/// Placeholder Kafka sink.
pub struct KafkaSink;

impl KafkaSink {
    pub fn new() -> Self {
        KafkaSink
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn publish_batch(&self, _events: &[MdEvent]) -> Result<()> {
        // In a real implementation this would produce to Kafka.
        Ok(())
    }
}
