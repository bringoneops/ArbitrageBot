use anyhow::Result;
use async_trait::async_trait;
use canonical::MdEvent;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

mod kafka;
pub use kafka::KafkaSink;

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

pub struct Wal<T: Sink> {
    inner: T,
    wal: Mutex<tokio::fs::File>,
    dlq: Mutex<tokio::fs::File>,
}

impl<T: Sink> Wal<T> {
    pub async fn new(path: impl AsRef<Path>, inner: T) -> Result<Self> {
        let path = path.as_ref();
        let mut dlq_path = path.to_path_buf();
        dlq_path.set_extension("dlq");

        let mut dlq = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&dlq_path)
            .await?;

        if let Ok(existing) = tokio::fs::read_to_string(path).await {
            for line in existing.lines().filter(|l| !l.trim().is_empty()) {
                if let Ok(ev) = serde_json::from_str::<MdEvent>(line) {
                    if inner.publish(&ev).await.is_err() {
                        dlq.write_all(line.as_bytes()).await?;
                        dlq.write_all(b"\n").await?;
                    }
                } else {
                    dlq.write_all(line.as_bytes()).await?;
                    dlq.write_all(b"\n").await?;
                }
            }
            inner.flush().await?;
        }

        let wal_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .await?;
        wal_file.sync_data().await?;

        Ok(Self {
            inner,
            wal: Mutex::new(wal_file),
            dlq: Mutex::new(dlq),
        })
    }
}

#[async_trait]
impl<T: Sink> Sink for Wal<T> {
    async fn publish(&self, event: &MdEvent) -> Result<()> {
        let json = serde_json::to_string(event)?;
        {
            let mut wal = self.wal.lock().await;
            wal.write_all(json.as_bytes()).await?;
            wal.write_all(b"\n").await?;
            wal.sync_data().await?;
        }
        if let Err(e) = self.inner.publish(event).await {
            let mut dlq = self.dlq.lock().await;
            dlq.write_all(json.as_bytes()).await?;
            dlq.write_all(b"\n").await?;
            dlq.sync_data().await?;
            return Err(e);
        }
        let mut wal = self.wal.lock().await;
        wal.set_len(0).await?;
        wal.seek(SeekFrom::Start(0)).await?;
        wal.sync_data().await?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }
}
