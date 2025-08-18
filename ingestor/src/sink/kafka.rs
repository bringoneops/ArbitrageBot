use anyhow::Result;
use async_trait::async_trait;
use canonical::MdEvent;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use std::time::Duration;

use super::Sink;

pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
}

impl KafkaSink {
    pub fn new(brokers: &str, topic: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn publish(&self, event: &MdEvent) -> Result<()> {
        let payload = serde_json::to_vec(event)?;
        self
            .producer
            .send(
                FutureRecord::<(), _>::to(&self.topic).payload(&payload),
                Duration::from_secs(0),
            )
            .await
            .map_err(|(e, _)| e)?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        self.producer.flush(Duration::from_secs(1))?;
        Ok(())
    }
}

