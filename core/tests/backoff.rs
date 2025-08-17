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

#[test]
fn backoff_handles_large_previous_without_overflow() {
    let max_backoff = Duration::from_secs(64);
    let min_stable = Duration::from_secs(5);

    let backoff = next_backoff(
        Duration::MAX,
        Duration::from_secs(0),
        false,
        max_backoff,
        min_stable,
    );

    assert_eq!(backoff, max_backoff);
}

#[test]
fn backoff_handles_previous_near_overflow() {
    let max_backoff = Duration::from_secs(64);
    let min_stable = Duration::from_secs(5);

    let large_previous = Duration::from_secs(u64::MAX / 2 + 1);
    let backoff = next_backoff(
        large_previous,
        Duration::from_secs(0),
        false,
        max_backoff,
        min_stable,
    );

    assert_eq!(backoff, max_backoff);
}
