use anyhow::Result;
use async_trait::async_trait;
use canonical::MdEvent;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use std::path::PathBuf;

use super::traits::Sink;

pub struct FileSink {
    path: PathBuf,
}

impl FileSink {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn publish_batch(&self, events: &[MdEvent]) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        for ev in events {
            let line = serde_json::to_vec(ev)?;
            file.write_all(&line).await?;
            file.write_all(b"\n").await?;
        }
        file.flush().await?;
        Ok(())
    }
}
