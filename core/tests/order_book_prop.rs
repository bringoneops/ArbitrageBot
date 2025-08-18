use arb_core as core;
use core::events::DepthUpdateEvent;
use core::{apply_depth_update, ApplyResult, DepthSnapshot, OrderBook};
use proptest::prelude::*;
use std::borrow::Cow;

fn snapshot_strategy() -> impl Strategy<Value = DepthSnapshot> {
    (
        any::<u64>(),
        prop::collection::btree_map(1u32..1000, 1u32..1000, 0..5),
        prop::collection::btree_map(1u32..1000, 1u32..1000, 0..5),
    )
        .prop_map(|(id, bids_map, asks_map)| DepthSnapshot {
            last_update_id: id,
            bids: bids_map
                .into_iter()
                .rev()
                .map(|(p, q)| [p.to_string(), q.to_string()])
                .collect(),
            asks: asks_map
                .into_iter()
                .map(|(p, q)| [p.to_string(), q.to_string()])
                .collect(),
        })
}

fn snapshot_and_diffs() -> impl Strategy<Value = (DepthSnapshot, Vec<DepthUpdateEvent<'static>>)> {
    (
        snapshot_strategy(),
        prop::collection::vec(
            (
                prop::collection::btree_map(1u32..1000, 0u32..1000, 0..3),
                prop::collection::btree_map(1u32..1000, 0u32..1000, 0..3),
            ),
            0..5,
        ),
    )
        .prop_map(|(snapshot, mods_vec)| {
            let mut last_id = snapshot.last_update_id;
            let diffs = mods_vec
                .into_iter()
                .map(|(bids_map, asks_map)| {
                    let first_update_id = last_id + 1;
                    let final_update_id = first_update_id;
                    let previous_final_update_id = last_id;
                    last_id = final_update_id;

                    let bids = bids_map
                        .into_iter()
                        .rev()
                        .map(|(p, q)| {
                            [Cow::Owned(p.to_string()), Cow::Owned(q.to_string())]
                        })
                        .collect();
                    let asks = asks_map
                        .into_iter()
                        .map(|(p, q)| {
                            [Cow::Owned(p.to_string()), Cow::Owned(q.to_string())]
                        })
                        .collect();

                    DepthUpdateEvent {
                        event_time: 0,
                        symbol: "TEST".to_string(),
                        first_update_id,
                        final_update_id,
                        previous_final_update_id,
                        bids,
                        asks,
                    }
                })
                .collect();
            (snapshot, diffs)
        })
}

proptest! {
    #[test]
    fn order_book_properties((snapshot, diffs) in snapshot_and_diffs()) {
        let mut book: OrderBook = snapshot.into();
        let mut prev_id = book.last_update_id;

        for diff in &diffs {
            prop_assert_eq!(apply_depth_update(&mut book, diff), ApplyResult::Applied);
            prop_assert!(book.last_update_id > prev_id);
            prev_id = book.last_update_id;

            for [price, qty] in &diff.bids {
                let p: f64 = price.parse().unwrap();
                let q: f64 = qty.parse().unwrap();
                prop_assert!(q >= 0.0);
                prop_assert!(p >= 0.0);
            }
            for [price, qty] in &diff.asks {
                let p: f64 = price.parse().unwrap();
                let q: f64 = qty.parse().unwrap();
                prop_assert!(q >= 0.0);
                prop_assert!(p >= 0.0);
            }

            prop_assert!(
                diff.bids.windows(2).all(|w| {
                    let p0: f64 = w[0][0].parse().unwrap();
                    let p1: f64 = w[1][0].parse().unwrap();
                    p0 >= p1
                }),
                "bids not sorted"
            );
            prop_assert!(
                diff.asks.windows(2).all(|w| {
                    let p0: f64 = w[0][0].parse().unwrap();
                    let p1: f64 = w[1][0].parse().unwrap();
                    p0 <= p1
                }),
                "asks not sorted"
            );
        }

        for qty in book.bids.values().chain(book.asks.values()) {
            let q: f64 = qty.parse().unwrap();
            prop_assert!(q >= 0.0);
        }

        let mut bid_prices: Vec<f64> = book
            .bids
            .keys()
            .map(|p| p.parse().unwrap())
            .collect();
        bid_prices.sort_by(|a, b| b.partial_cmp(a).unwrap());
        prop_assert!(bid_prices.windows(2).all(|w| w[0] >= w[1]));

        let mut ask_prices: Vec<f64> = book
            .asks
            .keys()
            .map(|p| p.parse().unwrap())
            .collect();
        ask_prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
        prop_assert!(ask_prices.windows(2).all(|w| w[0] <= w[1]));
    }
}

