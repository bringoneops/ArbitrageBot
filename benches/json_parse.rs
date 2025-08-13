use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use binance_us_and_global::events::StreamMessage;
use simd_json::serde::from_slice as simd_from_slice;
use serde_json;

const RAW: &str = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":123,"s":"BTCUSDT","t":1,"p":"0.001","q":"100","b":1,"a":2,"T":123,"m":true,"M":true}}"#;

fn bench_json_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parse");
    group.throughput(Throughput::Bytes(RAW.len() as u64));
    group.bench_function("simd_json", |b| {
        b.iter(|| {
            let mut bytes = RAW.as_bytes().to_vec();
            let _: StreamMessage<'_> = simd_from_slice(&mut bytes).unwrap();
        })
    });
    group.bench_function("serde_json", |b| {
        b.iter(|| {
            let text = String::from(RAW);
            let _: StreamMessage<'_> = serde_json::from_str(&text).unwrap();
        })
    });
    group.finish();
}

criterion_group!(benches, bench_json_parse);
criterion_main!(benches);
