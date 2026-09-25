use criterion::{Criterion, criterion_group, criterion_main};
use sisa_messaging::{
    ContentType, EnvelopeMapper, MessageId, MessageType, Metadata, SerializedEnvelope,
};
use sisa_messaging_redis::RedisMapper;

fn benchmark(c: &mut Criterion) {
    let envelope = SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("benchmark").unwrap(),
        message_version: 1,
        content_type: ContentType::new("application/json").unwrap(),
        payload: vec![42; 256],
        metadata: Metadata::default(),
        ordering_key: None,
    };

    let mapper = RedisMapper;
    let wire = mapper.encode(&envelope).unwrap();

    c.bench_function("redis_mapper_encode_256b", |b| {
        b.iter(|| mapper.encode(&envelope).unwrap())
    });

    c.bench_function("redis_mapper_decode_256b", |b| {
        b.iter(|| mapper.decode(wire.clone()).unwrap())
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
