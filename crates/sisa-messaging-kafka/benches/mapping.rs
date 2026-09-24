use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use sisa_messaging::{
    ContentType, CorrelationMetadata, DeliveryMetadata, EnvelopeMapper, HeaderName, HeaderValue,
    Headers, MessageId, MessageType, Metadata, MetadataValue, OrderingKey, RequestId,
    RoutingMetadata, SerializedEnvelope, TraceMetadata,
};
use sisa_messaging_kafka::KafkaEnvelopeMapper;

fn typical_envelope() -> SerializedEnvelope {
    let mut headers = Headers::new();

    for index in 0..8 {
        headers
            .insert(
                HeaderName::new(format!("x-bench-{index}"))
                    .expect("static benchmark header name is valid"),
                HeaderValue::new(format!("value-{index}"))
                    .expect("static benchmark header value is valid"),
            )
            .expect("benchmark header fixture fits");
    }

    SerializedEnvelope {
        message_id: "01890f52-7b00-7000-8000-000000000001"
            .parse::<MessageId>()
            .expect("static benchmark message id is valid"),
        message_type: MessageType::new("orders.created")
            .expect("static benchmark message type is valid"),
        message_version: 4,
        content_type: ContentType::new("application/json")
            .expect("static benchmark content type is valid"),
        payload: vec![b'x'; 4 * 1024],
        metadata: Metadata {
            correlation: CorrelationMetadata {
                correlation_id: Some(
                    MetadataValue::new("benchmark-correlation")
                        .expect("static benchmark correlation is valid"),
                ),
                conversation_id: Some(
                    "01890f52-7b00-7000-8000-000000000002"
                        .parse()
                        .expect("static benchmark conversation id is valid"),
                ),
                causation_id: Some(
                    "01890f52-7b00-7000-8000-000000000003"
                        .parse::<MessageId>()
                        .expect("static benchmark causation id is valid"),
                ),
                request_id: Some(
                    "01890f52-7b00-7000-8000-000000000004"
                        .parse::<RequestId>()
                        .expect("static benchmark request id is valid"),
                ),
            },
            trace: TraceMetadata {
                traceparent: Some(
                    HeaderValue::new("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                        .expect("static benchmark traceparent is valid"),
                ),
                tracestate: Some(
                    HeaderValue::new("vendor=value").expect("static benchmark tracestate is valid"),
                ),
            },
            routing: RoutingMetadata {
                source: Some(
                    MetadataValue::new("orders-api").expect("static benchmark source is valid"),
                ),
                destination: Some(
                    MetadataValue::new("orders.created")
                        .expect("static benchmark destination is valid"),
                ),
                reply_to: Some(
                    MetadataValue::new("orders.results")
                        .expect("static benchmark reply-to is valid"),
                ),
            },
            delivery: DeliveryMetadata {
                sent_at_ms: Some(1_700_000_000_000),
                deduplication_id: Some(
                    MetadataValue::new("benchmark-dedup")
                        .expect("static benchmark deduplication id is valid"),
                ),
            },
            tenant_id: Some(
                MetadataValue::new("benchmark-tenant").expect("static benchmark tenant is valid"),
            ),
            headers,
        },
        ordering_key: Some(
            OrderingKey::new("order-42").expect("static benchmark ordering key is valid"),
        ),
    }
}

fn mapping_benchmarks(criterion: &mut Criterion) {
    let mapper = KafkaEnvelopeMapper;
    let envelope = typical_envelope();
    let wire_record = mapper.encode(&envelope).expect("benchmark fixture encodes");
    let mut group = criterion.benchmark_group("kafka_mapping/typical-4k-8-ordered");

    group.bench_function("encode", |bencher| {
        bencher.iter(|| black_box(mapper.encode(black_box(&envelope))))
    });
    group.bench_function("decode", |bencher| {
        bencher.iter_batched(
            || wire_record.clone(),
            |record| black_box(mapper.decode(record)),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, mapping_benchmarks);
criterion_main!(benches);
