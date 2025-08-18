use arb_core as core;
use canonical::MdEvent;
use core::events::StreamMessage;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use futures::executor::block_on;
use hdrhistogram::Histogram;
use simd_json::serde::from_slice as simd_from_slice;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

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

struct NoopSink;

impl NoopSink {
    async fn publish(&self, _ev: &MdEvent) {}
}

const RAW: &str = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":123,"s":"BTCUSDT","t":1,"p":"0.001","q":"100","b":1,"a":2,"T":123,"m":true,"M":true}}"#;
const BATCH_SIZE: usize = 1024;

fn bench_ingestor(c: &mut Criterion) {
    let batch: Vec<Vec<u8>> = (0..BATCH_SIZE).map(|_| RAW.as_bytes().to_vec()).collect();
    let sink = NoopSink;
    let latency = RefCell::new(Histogram::<u64>::new(3).unwrap());
    let allocs = RefCell::new(Histogram::<u64>::new(3).unwrap());

    let mut group = c.benchmark_group("ingestor");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));
    group.bench_function("parse_canonicalize_sink", |b| {
        b.iter(|| {
            for raw in &batch {
                ALLOCS.store(0, Ordering::SeqCst);
                let mut bytes = raw.clone();
                let start = Instant::now();
                let msg: StreamMessage<'_> = simd_from_slice(&mut bytes).unwrap();
                let ev = MdEvent::try_from(msg.data).unwrap();
                block_on(sink.publish(black_box(&ev)));
                latency
                    .borrow_mut()
                    .record(start.elapsed().as_nanos() as u64)
                    .unwrap();
                allocs
                    .borrow_mut()
                    .record(ALLOCS.load(Ordering::SeqCst) as u64)
                    .unwrap();
            }
        });
    });
    group.finish();

    let p99 = latency.borrow().value_at_percentile(99.0);
    let alloc_p99 = allocs.borrow().value_at_percentile(99.0);
    println!("p99 latency: {} ns", p99);
    println!("p99 allocations: {}", alloc_p99);
}

criterion_group!(benches, bench_ingestor);
criterion_main!(benches);
