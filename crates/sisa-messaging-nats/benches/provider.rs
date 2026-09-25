//! Application-observed JetStream latency and throughput. Set NATS_URL to enable.

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use async_nats::jetstream::{self, consumer::pull, stream};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use sisa_messaging::{
    ContentType, Delivery, HeaderName, HeaderValue, IndividualDeliverySource, IndividualSettlement,
    IndividualSourceRequirements, MessageId, MessageType, Metadata, OrderingKey, Publisher,
    SerializedEnvelope,
};
use sisa_messaging_nats::{
    MappingError, NatsDeliverySource, NatsPublisher, NatsPublisherSettings, Subject,
    SubjectResolver,
};

#[derive(Clone)]
struct BenchResolver {
    prefix: Subject,
    ordered: bool,
}

impl SubjectResolver for BenchResolver {
    fn resolve(&self, envelope: &SerializedEnvelope) -> Result<Subject, MappingError> {
        let base = format!(
            "{}.{}.v{}",
            self.prefix.as_str(),
            envelope.message_type.as_str(),
            envelope.message_version
        );
        if self.ordered {
            let key = envelope
                .ordering_key
                .as_ref()
                .ok_or(MappingError::InvalidEnvelope)?;
            Subject::new(format!("{base}.{}", key.as_str()))
        } else {
            Subject::new(base)
        }
    }
}

fn envelope(size: usize, header_count: usize, ordered: bool) -> SerializedEnvelope {
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
        message_type: MessageType::new("event").unwrap(),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream").unwrap(),
        payload: vec![b'x'; size],
        metadata,
        ordering_key: ordered.then(|| OrderingKey::new("bench-key").unwrap()),
    }
}

fn provider(c: &mut Criterion) {
    let Ok(url) = std::env::var("NATS_URL") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _runtime_guard = runtime.enter();
    let context =
        runtime.block_on(async { jetstream::new(async_nats::connect(url).await.unwrap()) });
    let suffix = MessageId::new().to_string().replace('-', "");
    let prefix = Subject::new(format!("bench_{suffix}")).unwrap();
    let stream_name = format!("BENCH{suffix}");
    let stream = runtime.block_on(async {
        context
            .create_stream(stream::Config {
                name: stream_name,
                subjects: vec![format!("{}.>", prefix.as_str())],
                max_messages: 2_000,
                ..Default::default()
            })
            .await
            .unwrap()
    });
    let consumer = runtime.block_on(async {
        stream
            .create_consumer(pull::Config {
                durable_name: Some("individual".into()),
                ack_wait: Duration::from_millis(200),
                max_deliver: 5,
                ..Default::default()
            })
            .await
            .unwrap()
    });
    let settings = NatsPublisherSettings {
        publish_timeout: Duration::from_secs(5),
    };
    let unordered = NatsPublisher::new(
        context.clone(),
        BenchResolver {
            prefix: prefix.clone(),
            ordered: false,
        },
        settings,
    )
    .unwrap();
    let ordered = NatsPublisher::new(
        context,
        BenchResolver {
            prefix,
            ordered: true,
        },
        settings,
    )
    .unwrap();
    let mut source = NatsDeliverySource::new(consumer);
    runtime
        .block_on(source.open(IndividualSourceRequirements::new()))
        .unwrap();

    for (profile, size, header_count) in [
        ("small", 256, 0),
        ("typical", 4096, 8),
        ("large", 65536, 32),
    ] {
        for (subject_mode, publisher) in [("unordered", &unordered), ("ordered", &ordered)] {
            let mut group = c.benchmark_group(format!("nats_publish_ack_{profile}_{subject_mode}"));
            for concurrency in [1usize, 8, 32, 128] {
                group.throughput(Throughput::Elements(concurrency as u64));
                group.bench_function(format!("concurrency_{concurrency}"), |b| {
                    b.iter_batched(
                        || {
                            (0..concurrency)
                                .map(|_| envelope(size, header_count, subject_mode == "ordered"))
                                .collect::<Vec<_>>()
                        },
                        |messages| {
                            runtime.block_on(async {
                                let results = futures_util::future::join_all(
                                    messages
                                        .iter()
                                        .map(|message| publisher.publish(black_box(message))),
                                )
                                .await;
                                for result in results {
                                    result.unwrap();
                                }
                            });
                        },
                        BatchSize::SmallInput,
                    )
                });
                runtime.block_on(async { stream.purge().await }).unwrap();
            }
            group.finish();
        }

        let mut group = c.benchmark_group(format!("nats_settlement_{profile}"));
        group.throughput(Throughput::Elements(1));
        group.bench_function("receive_confirmed_ack", |b| {
            b.iter_batched(
                || {
                    runtime
                        .block_on(unordered.publish(&envelope(size, header_count, false)))
                        .unwrap()
                },
                |_| {
                    runtime.block_on(async {
                        source
                            .receive()
                            .await
                            .unwrap()
                            .unwrap()
                            .into_parts()
                            .1
                            .ack()
                            .await
                            .unwrap();
                    });
                },
                BatchSize::SmallInput,
            )
        });
        group.bench_function("delayed_nak_confirmation", |b| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    runtime
                        .block_on(unordered.publish(&envelope(size, header_count, false)))
                        .unwrap();
                    let settlement = runtime
                        .block_on(source.receive())
                        .unwrap()
                        .unwrap()
                        .into_parts()
                        .1;
                    let start = Instant::now();
                    runtime
                        .block_on(settlement.nak(Duration::from_millis(20)))
                        .unwrap();
                    elapsed += start.elapsed();
                    let redelivery = runtime.block_on(source.receive()).unwrap().unwrap();
                    runtime.block_on(redelivery.into_parts().1.ack()).unwrap();
                }
                elapsed
            })
        });
        group.bench_function("terminate_confirmation", |b| {
            b.iter_batched(
                || {
                    runtime
                        .block_on(unordered.publish(&envelope(size, header_count, false)))
                        .unwrap();
                    runtime
                        .block_on(source.receive())
                        .unwrap()
                        .unwrap()
                        .into_parts()
                        .1
                },
                |settlement| runtime.block_on(settlement.terminate()).unwrap(),
                BatchSize::SmallInput,
            )
        });
        group.bench_function("heartbeat_confirmation", |b| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    runtime
                        .block_on(unordered.publish(&envelope(size, header_count, false)))
                        .unwrap();
                    let mut settlement = runtime
                        .block_on(source.receive())
                        .unwrap()
                        .unwrap()
                        .into_parts()
                        .1;
                    let start = Instant::now();
                    runtime.block_on(settlement.heartbeat()).unwrap();
                    elapsed += start.elapsed();
                    runtime.block_on(settlement.ack()).unwrap();
                }
                elapsed
            })
        });
        group.bench_function("redelivery_after_withheld_ack", |b| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    runtime
                        .block_on(unordered.publish(&envelope(size, header_count, false)))
                        .unwrap();
                    let settlement = runtime
                        .block_on(source.receive())
                        .unwrap()
                        .unwrap()
                        .into_parts()
                        .1;
                    let start = Instant::now();
                    drop(settlement);
                    let redelivery = runtime.block_on(source.receive()).unwrap().unwrap();
                    elapsed += start.elapsed();
                    runtime.block_on(redelivery.into_parts().1.ack()).unwrap();
                }
                elapsed
            })
        });
        group.finish();
        runtime.block_on(async { stream.purge().await }).unwrap();
    }
}

criterion_group!(benches, provider);
criterion_main!(benches);
