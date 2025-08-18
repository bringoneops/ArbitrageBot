use super::wal::Wal;
use anyhow::Result;
use async_trait::async_trait;
use canonical::MdEvent;
use tokio::time::{sleep, Duration};

#[async_trait]
pub trait Sink: Send + Sync {
    async fn publish_batch(&self, events: &[MdEvent]) -> Result<()>;
}

/// Wrapper providing at-least-once delivery semantics with optional
/// write-ahead logging and dead-letter queue handling.
pub struct AtLeastOnceSink<S: Sink> {
    inner: S,
    wal: Option<Wal>,
    dlq: Option<Wal>,
    max_retries: usize,
}

impl<S: Sink> AtLeastOnceSink<S> {
    pub fn new(inner: S, wal: Option<Wal>, dlq: Option<Wal>, max_retries: usize) -> Self {
        Self {
            inner,
            wal,
            dlq,
            max_retries,
        }
    }
}

#[async_trait]
impl<S: Sink> Sink for AtLeastOnceSink<S> {
    async fn publish_batch(&self, events: &[MdEvent]) -> Result<()> {
        if let Some(wal) = &self.wal {
            let raw = serde_json::to_vec(events)?;
            wal.append(&raw).await?;
        }

        let mut attempts = 0usize;
        let mut delay = Duration::from_millis(100);
        let max_delay = Duration::from_secs(1);
        loop {
            match self.inner.publish_batch(events).await {
                Ok(()) => return Ok(()),
                Err(_e) if attempts < self.max_retries => {
                    attempts += 1;
                    sleep(delay).await;
                    delay = std::cmp::min(delay.saturating_mul(2), max_delay);
                }
                Err(e) => {
                    if let Some(dlq) = &self.dlq {
                        let raw = serde_json::to_vec(events)?;
                        let _ = dlq.append(&raw).await;
                    }
                    return Err(e);
                }
            }
        }
    }
}
