use arb_core::rate_limit::TokenBucket as SemaphoreBucket;
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::time::sleep;

// Legacy mutex-based token bucket used for comparison in the benchmark.
#[derive(Debug)]
struct MutexBucket {
    capacity: f64,
    tokens_per_interval: f64,
    interval: Duration,
    state: Mutex<TokenState>,
}

#[derive(Debug)]
struct TokenState {
    tokens: f64,
    last_refill: Instant,
}

impl MutexBucket {
    fn new(capacity: u32, tokens_per_interval: u32, interval: Duration) -> Self {
        Self {
            capacity: capacity as f64,
            tokens_per_interval: tokens_per_interval as f64,
            interval,
            state: Mutex::new(TokenState {
                tokens: capacity as f64,
                last_refill: Instant::now(),
            }),
        }
    }

    async fn acquire(&self, tokens: u32) {
        let needed = tokens as f64;
        loop {
            let wait = {
                let mut state = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(state.last_refill);
                if elapsed >= self.interval {
                    let intervals = elapsed.as_secs_f64() / self.interval.as_secs_f64();
                    state.tokens =
                        (state.tokens + intervals * self.tokens_per_interval).min(self.capacity);
                    state.last_refill = now;
                }
                if state.tokens >= needed {
                    state.tokens -= needed;
                    return;
                }
                let missing = needed - state.tokens;
                let rate = self.tokens_per_interval / self.interval.as_secs_f64();
                Duration::from_secs_f64(missing / rate)
            };
            sleep(wait).await;
        }
    }
}

fn bench_mutex_bucket(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bucket = Arc::new(MutexBucket::new(
        u32::MAX / 2,
        u32::MAX / 2,
        Duration::from_secs(1),
    ));
    c.bench_function("mutex_bucket", |b| {
        let bucket = bucket.clone();
        b.to_async(&rt).iter(|| async {
            bucket.acquire(1).await;
        });
    });
}

fn bench_semaphore_bucket(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bucket = Arc::new(rt.block_on(async {
        SemaphoreBucket::new(u32::MAX / 2, u32::MAX / 2, Duration::from_secs(1))
    }));
    c.bench_function("semaphore_bucket", |b| {
        let bucket = bucket.clone();
        b.to_async(&rt).iter(|| async {
            bucket.acquire(1).await.unwrap();
        });
    });
}

criterion_group!(benches, bench_mutex_bucket, bench_semaphore_bucket);
criterion_main!(benches);
