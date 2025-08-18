pub mod traits;
pub mod kafka;
pub mod nats;
pub mod file;
pub mod wal;

pub use traits::{Sink, AtLeastOnceSink};
pub use file::FileSink;
pub use kafka::KafkaSink;
pub use nats::NatsSink;
pub use wal::Wal;

use anyhow::Result;
use std::env;
use std::sync::Arc;

/// Initialize a sink implementation from environment variables.
///
/// Supported environment variables:
/// - `SINK_TYPE`: "kafka", "nats", or "file" (default).
/// - `SINK_FILE_PATH`: path for file sink (default "events.log").
/// - `DLQ_PATH`: path for dead-letter queue file (default "dlq.log").
/// - `WAL_PATH`: optional path for write-ahead log.
/// - `SINK_MAX_RETRIES`: number of publish retries (default 3).
pub async fn init_from_env() -> Result<Arc<dyn Sink>> {
    let sink_type = env::var("SINK_TYPE").unwrap_or_else(|_| "file".to_string());
    let file_path = env::var("SINK_FILE_PATH").unwrap_or_else(|_| "events.log".to_string());
    let dlq_path = env::var("DLQ_PATH").unwrap_or_else(|_| "dlq.log".to_string());
    let wal = env::var("WAL_PATH").ok().map(Wal::new);
    let max_retries = env::var("SINK_MAX_RETRIES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(3);

    let dlq = Wal::new(dlq_path);

    let sink: Arc<dyn Sink> = match sink_type.to_lowercase().as_str() {
        "kafka" => Arc::new(AtLeastOnceSink::new(
            KafkaSink::new(),
            wal,
            Some(dlq),
            max_retries,
        )),
        "nats" => Arc::new(AtLeastOnceSink::new(
            NatsSink::new(),
            wal,
            Some(dlq),
            max_retries,
        )),
        _ => Arc::new(AtLeastOnceSink::new(
            FileSink::new(file_path),
            wal,
            Some(dlq),
            max_retries,
        )),
    };

    Ok(sink)
}
