use std::time::{Duration, Instant};
use tokio::{sync::Mutex, time::sleep};

#[derive(Debug)]
pub struct TokenBucket {
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

impl TokenBucket {
    pub fn new(capacity: u32, tokens_per_interval: u32, interval: Duration) -> Self {
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

    pub async fn acquire(&self, tokens: u32) {
        let needed = tokens as f64;
        loop {
            let wait_duration = {
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
            sleep(wait_duration).await;
        }
    }
}
