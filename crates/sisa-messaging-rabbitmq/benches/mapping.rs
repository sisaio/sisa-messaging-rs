//! Broker-free AMQP projection benchmarks for small, typical, and large bodies.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use sisa_messaging::{
    ContentType, EnvelopeMapper, HeaderName, HeaderValue, MessageId, MessageType, Metadata,
    OrderingKey, SerializedEnvelope,
};
use sisa_messaging_rabbitmq::{ExchangeName, RabbitMqMapper, TypeRouteResolver};

fn fixture(size: usize, header_count: usize) -> SerializedEnvelope {
    let mut metadata = Metadata::default();

    for index in 0..header_count {
        metadata
            .headers
            .insert(
                HeaderName::new(format!("x-bench-{index}")).unwrap(),
                HeaderValue::new(format!("value-{index}")).unwrap(),
            )
            .unwrap();
    }

    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("order_created").unwrap(),
        message_version: 1,
        content_type: ContentType::new("application/json").unwrap(),
        payload: vec![b'x'; size],
        metadata,
        ordering_key: (header_count > 0).then(|| OrderingKey::new("bench-key").unwrap()),
    }
}

fn mapping(c: &mut Criterion) {
    let mapper = RabbitMqMapper::new(TypeRouteResolver::new(ExchangeName::new("events").unwrap()));

    let mut group = c.benchmark_group("mapping");

    for (profile, size, header_count) in [
        ("small", 256, 0),
        ("typical", 4096, 8),
        ("large", 65536, 32),
    ] {
        let envelope = fixture(size, header_count);
        let wire = mapper.encode(&envelope).unwrap();

        group.bench_function(format!("encode_{profile}"), |b| {
            b.iter(|| mapper.encode(black_box(&envelope)).unwrap())
        });

        group.bench_function(format!("decode_{profile}"), |b| {
            b.iter_batched(
                || wire.clone(),
                |wire| mapper.decode(black_box(wire)).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(benches, mapping);
criterion_main!(benches);
