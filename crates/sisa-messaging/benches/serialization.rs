use std::hint::black_box;
use std::str::FromStr;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use sisa_messaging::{
    Envelope, HeaderName, HeaderValue, Headers, JsonSerializer, Message, MessageId, Metadata,
    OrderingKey, Serializer,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct BenchMessage {
    order_id: Option<String>,
    payload: Vec<u8>,
}

impl Message for BenchMessage {
    const TYPE: &'static str = "bench.message";
    const VERSION: u32 = 1;

    fn order_by(&self) -> Option<OrderingKey> {
        self.order_id
            .as_ref()
            .and_then(|key| OrderingKey::new(key.clone()).ok())
    }
}

fn envelope(payload_size: usize, header_count: usize, ordered: bool) -> Envelope<BenchMessage> {
    let mut headers = Headers::new();

    for index in 0..header_count {
        headers.insert(
            HeaderName::new(format!("x-bench-{index}")).unwrap(),
            HeaderValue::new(format!("value-{index}")).unwrap(),
        );
    }

    Envelope::new(
        MessageId::from_str("0198f3e2-40f0-7b15-8a4a-843d24f68d20").unwrap(),
        BenchMessage {
            order_id: ordered.then(|| "order-42".to_owned()),
            payload: vec![b'x'; payload_size],
        },
        Metadata {
            headers,
            ..Metadata::default()
        },
    )
    .unwrap()
}

fn serialization_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("json_envelope");

    for (name, payload_size, header_count, ordered) in [
        ("small", 256, 0, false),
        ("typical", 4 * 1_024, 8, true),
        ("large", 64 * 1_024, 32, false),
    ] {
        let envelope = envelope(payload_size, header_count, ordered);

        let serialized = JsonSerializer.serialize(&envelope).unwrap();

        group.bench_with_input(BenchmarkId::new("serialize", name), &name, |bencher, _| {
            bencher.iter(|| JsonSerializer.serialize(black_box(&envelope)))
        });
        group.bench_with_input(
            BenchmarkId::new("deserialize", name),
            &name,
            |bencher, _| {
                bencher.iter_batched(
                    || serialized.clone(),
                    |serialized| {
                        black_box(<JsonSerializer as Serializer<BenchMessage>>::deserialize(
                            &JsonSerializer,
                            serialized,
                        ))
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("metadata_encode", name),
            &name,
            |bencher, _| bencher.iter(|| serde_json::to_vec(black_box(envelope.metadata()))),
        );

        let metadata = serde_json::to_vec(envelope.metadata()).unwrap();

        group.bench_with_input(
            BenchmarkId::new("metadata_decode", name),
            &name,
            |bencher, _| bencher.iter(|| serde_json::from_slice::<Metadata>(black_box(&metadata))),
        );
    }
    group.finish();
}

criterion_group!(benches, serialization_benchmarks);
criterion_main!(benches);
