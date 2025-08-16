use std::time::Duration;

use arb_core as core;
use core::next_backoff;

#[test]
fn backoff_resets_after_stable_run() {
    let max_backoff = Duration::from_secs(64);
    let min_stable = Duration::from_secs(5);
    let mut backoff = Duration::from_secs(1);

    backoff = next_backoff(
        backoff,
        Duration::from_secs(0),
        false,
        max_backoff,
        min_stable,
    );
    assert_eq!(backoff, Duration::from_secs(2));

    backoff = next_backoff(
        backoff,
        Duration::from_secs(10),
        true,
        max_backoff,
        min_stable,
    );
    assert_eq!(backoff, Duration::from_secs(1));
}
