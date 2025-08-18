use anyhow::Result;
use async_trait::async_trait;
use canonical::MdEvent;
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

#[async_trait]
pub trait Sink: Send + Sync {
    async fn publish(&self, event: &MdEvent) -> Result<()>;
    async fn flush(&self) -> Result<()>;
}

pub struct FileSink {
    inner: Mutex<State>,
    batch_size: usize,
}

struct State {
    writer: BufWriter<tokio::fs::File>,
    buf: Vec<String>,
}

impl FileSink {
    const DEFAULT_BATCH_SIZE: usize = 1024;

    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            inner: Mutex::new(State {
                writer: BufWriter::new(file),
                buf: Vec::with_capacity(Self::DEFAULT_BATCH_SIZE),
            }),
            batch_size: Self::DEFAULT_BATCH_SIZE,
        })
    }

    async fn flush_locked(state: &mut State) -> Result<()> {
        for line in state.buf.drain(..) {
            state.writer.write_all(line.as_bytes()).await?;
            state.writer.write_all(b"\n").await?;
        }
        state.writer.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn publish(&self, event: &MdEvent) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let json = serde_json::to_string(event)?;
        inner.buf.push(json);
        if inner.buf.len() >= self.batch_size {
            Self::flush_locked(&mut inner).await?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.buf.is_empty() {
            inner.writer.flush().await?;
            return Ok(());
        }
        Self::flush_locked(&mut inner).await
    }
}
