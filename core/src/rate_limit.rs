use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time;

/// A simple token bucket implemented with a [`Semaphore`].
///
/// Tokens are represented by permits on the semaphore. A background task
/// periodically replenishes permits up to the configured capacity.
#[derive(Debug)]
pub struct TokenBucket {
    semaphore: Arc<Semaphore>,
}

impl TokenBucket {
    /// Create a new [`TokenBucket`].
    ///
    /// `capacity` is the maximum burst size. `tokens_per_interval` are added
    /// every `interval` until the bucket reaches full capacity.
    pub fn new(capacity: u32, tokens_per_interval: u32, interval: Duration) -> Self {
        let semaphore = Arc::new(Semaphore::new(capacity as usize));
        let refill_sem = semaphore.clone();
        let cap = capacity as usize;
        let add = tokens_per_interval as usize;
        tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            loop {
                ticker.tick().await;
                let available = refill_sem.available_permits();
                if available < cap {
                    let to_add = (cap - available).min(add);
                    refill_sem.add_permits(to_add);
                }
            }
        });
        Self { semaphore }
    }

    /// Acquire `tokens` from the bucket, waiting if necessary.
    pub async fn acquire(&self, tokens: u32) {
        // `acquire_many` returns a permit guard that releases permits when
        // dropped. We "forget" it to permanently consume the permits.
        self.semaphore
            .acquire_many(tokens)
            .await
            .expect("semaphore closed")
            .forget();
    }

    /// Return the current number of available tokens.
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }
}
