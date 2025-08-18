use anyhow::Result;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use std::path::PathBuf;

/// Simple write-ahead log that appends raw frames to a file.
#[derive(Clone)]
pub struct Wal {
    path: PathBuf,
}

impl Wal {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub async fn append(&self, data: &[u8]) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        file.write_all(data).await?;
        file.write_all(b"\n").await?;
        file.flush().await?;
        Ok(())
    }
}
