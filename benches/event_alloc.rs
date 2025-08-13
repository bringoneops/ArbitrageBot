use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde::Deserialize;
use binance_us_and_global::events::StreamMessage;

struct CountingAlloc;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::SeqCst);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

#[derive(Deserialize)]
struct StreamMessageOwned {
    stream: String,
    data: OwnedEvent,
}

#[derive(Deserialize)]
#[serde(tag = "e")]
enum OwnedEvent {
    #[serde(rename = "trade")]
    Trade(OwnedTrade),
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
struct OwnedTrade {
    #[serde(rename = "E")]
    _event_time: u64,
    #[serde(rename = "s")]
    _symbol: String,
    #[serde(rename = "t")]
    _trade_id: u64,
    #[serde(rename = "p")]
    _price: String,
    #[serde(rename = "q")]
    _quantity: String,
    #[serde(rename = "b")]
    _buyer_order_id: u64,
    #[serde(rename = "a")]
    _seller_order_id: u64,
    #[serde(rename = "T")]
    _trade_time: u64,
    #[serde(rename = "m")]
    _buyer_is_maker: bool,
    #[serde(rename = "M")]
    _best_match: bool,
}

const RAW: &str = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":123,"s":"BTCUSDT","t":1,"p":"0.001","q":"100","b":1,"a":2,"T":123,"m":true,"M":true}}"#;

fn parse_cow() -> usize {
    ALLOCS.store(0, Ordering::SeqCst);
    let _: StreamMessage<'_> = serde_json::from_str(RAW).unwrap();
    ALLOCS.load(Ordering::SeqCst)
}

fn parse_owned() -> usize {
    ALLOCS.store(0, Ordering::SeqCst);
    let _: StreamMessageOwned = serde_json::from_str(RAW).unwrap();
    ALLOCS.load(Ordering::SeqCst)
}

fn benchmark_alloc(c: &mut Criterion) {
    c.bench_function("parse_cow", |b| b.iter(|| black_box(parse_cow())));
    c.bench_function("parse_owned", |b| b.iter(|| black_box(parse_owned())));
}

criterion_group!(benches, benchmark_alloc);
criterion_main!(benches);
