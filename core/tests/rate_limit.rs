use arb_core::rate_limit::TokenBucket;
use std::time::Duration;

#[tokio::test]
async fn acquire_clamps_to_capacity() {
    let bucket = TokenBucket::new(5, 0, Duration::from_secs(1));
    tokio::time::timeout(Duration::from_secs(1), bucket.acquire(10))
        .await
        .expect("acquire should not time out");
    assert_eq!(bucket.available(), 0);
}
