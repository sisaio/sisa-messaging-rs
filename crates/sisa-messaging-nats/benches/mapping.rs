//! Broker-free NATS projection benchmarks for small, typical, and large bodies.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use sisa_messaging::{
    ContentType, EnvelopeMapper, HeaderName, HeaderValue, MessageId, MessageType, Metadata,
    OrderingKey, SerializedEnvelope,
};
use sisa_messaging_nats::{NatsMapper, Subject, TypeSubjectResolver};

fn fixture(size: usize, header_count: usize, ordered: bool) -> SerializedEnvelope {
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
        ordering_key: ordered.then(|| OrderingKey::new("bench-key").unwrap()),
    }
}

fn mapping(c: &mut Criterion) {
    let mapper = NatsMapper::new(TypeSubjectResolver::new(Subject::new("events").unwrap()));

    c.bench_function("nats_subject_validation", |b| {
        b.iter(|| Subject::new(black_box("events.order_created.v1")))
    });

    for (profile, size, header_count) in [
        ("small", 256, 0),
        ("typical", 4096, 8),
        ("large", 65536, 32),
    ] {
        let envelope = fixture(size, header_count, false);
        let ordered_envelope = (profile == "typical").then(|| fixture(size, header_count, true));
        let mut sequence = 0usize;

        c.bench_function(&format!("nats_encode_{profile}"), |b| {
            b.iter_batched(
                || {
                    sequence += 1;

                    if sequence.is_multiple_of(2) {
                        ordered_envelope.as_ref().unwrap_or(&envelope).clone()
                    } else {
                        envelope.clone()
                    }
                },
                |envelope| mapper.encode(black_box(&envelope)).unwrap(),
                BatchSize::SmallInput,
            )
        });

        let wire = mapper.encode(&envelope).unwrap();

        let ordered_wire = ordered_envelope
            .as_ref()
            .map(|envelope| mapper.encode(envelope).unwrap());

        c.bench_function(&format!("nats_decode_{profile}"), |b| {
            b.iter_batched(
                || {
                    sequence += 1;

                    if sequence.is_multiple_of(2) {
                        ordered_wire.as_ref().unwrap_or(&wire).clone()
                    } else {
                        wire.clone()
                    }
                },
                |wire| mapper.decode(black_box(wire)).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }
}

criterion_group!(benches, mapping);
criterion_main!(benches);
