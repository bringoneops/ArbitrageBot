use arb_core::OrderBook;
use criterion::{criterion_group, criterion_main, Criterion};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

async fn run_mutex_updates(concurrency: usize, updates: usize) {
    let books = Arc::new(Mutex::new(HashMap::new()));
    {
        let mut map = books.lock().await;
        map.insert(
            "BTCUSDT".to_string(),
            OrderBook {
                bids: HashMap::new(),
                asks: HashMap::new(),
                last_update_id: 0,
            },
        );
    }

    let mut tasks = Vec::new();
    for _ in 0..concurrency {
        let books = books.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..updates {
                let mut map = books.lock().await;
                if let Some(book) = map.get_mut("BTCUSDT") {
                    book.last_update_id += 1;
                }
            }
        }));
    }

    for t in tasks {
        let _ = t.await;
    }
}

async fn run_dashmap_updates(concurrency: usize, updates: usize) {
    let books = Arc::new(DashMap::new());
    books.insert(
        "BTCUSDT".to_string(),
        OrderBook {
            bids: HashMap::new(),
            asks: HashMap::new(),
            last_update_id: 0,
        },
    );

    let mut tasks = Vec::new();
    for _ in 0..concurrency {
        let books = books.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..updates {
                if let Some(mut book) = books.get_mut("BTCUSDT") {
                    book.last_update_id += 1;
                }
            }
        }));
    }

    for t in tasks {
        let _ = t.await;
    }
}

fn bench_update_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let concurrency = 4;
    let updates = 1000;

    c.bench_function("mutex_hashmap_update", |b| {
        b.to_async(&rt)
            .iter(|| run_mutex_updates(concurrency, updates))
    });

    c.bench_function("dashmap_update", |b| {
        b.to_async(&rt)
            .iter(|| run_dashmap_updates(concurrency, updates))
    });
}

criterion_group!(benches, bench_update_latency);
criterion_main!(benches);
