use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time;

/// Rate limiter using the classic token bucket algorithm.
///
/// # Algorithm
/// Tokens are represented as permits on a [`tokio::sync::Semaphore`]. A
/// background task ticks at a fixed `interval` and replenishes up to
/// `tokens_per_interval` permits, never allowing the bucket to exceed the
/// configured `capacity`.
///
/// # Concurrency
/// Cloning the bucket is cheap because the semaphore is wrapped in an
/// [`Arc`], allowing it to be shared across tasks. Waiting for permits relies on
/// the asynchronous [`Semaphore`] rather than a manual `Mutex`-guarded counter.
/// The semaphore parks tasks until permits become available, avoiding holding a
/// lock across `.await` points and providing fair wakeups. A naïve `Mutex` based
/// implementation (see benchmarks) would require custom bookkeeping and
/// sleeping, increasing contention and complexity. Using the semaphore trades a
/// small amount of runtime overhead for a simpler and more robust design.
#[derive(Debug)]
pub struct TokenBucket {
    semaphore: Arc<Semaphore>,
}

impl TokenBucket {
    /// Create a new [`TokenBucket`].
    ///
    /// `capacity` is the maximum burst size. `tokens_per_interval` are added
    /// every `interval` until the bucket reaches full capacity.
    ///
    /// A background task spawned on the Tokio runtime performs the periodic
    /// refills. The task holds the only mutable reference to the semaphore and
    /// therefore no external synchronization is required.
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
    ///
    /// This method asynchronously waits for permits. Tasks are parked by the
    /// semaphore without blocking threads until the refill task adds enough
    /// tokens.
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
