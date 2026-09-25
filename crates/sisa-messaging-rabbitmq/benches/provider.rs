//! Application-observed RabbitMQ publish-confirm and settlement latency. Set `RABBITMQ_URL` to
//! enable; without it the benchmark registers nothing.

use std::{
    hint::black_box,
    num::{NonZeroU16, NonZeroU32},
    time::{Duration, Instant},
};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use lapin::{
    Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{
        ConfirmSelectOptions, ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions,
        QueueDeclareOptions, QueueDeleteOptions, QueuePurgeOptions,
    },
    types::{AMQPValue, FieldTable},
};
use sisa_messaging::{
    ContentType, Delivery, HeaderName, HeaderValue, IndividualDeliverySource, IndividualSettlement,
    IndividualSourceRequirements, MessageId, MessageType, Metadata, Publisher, SerializedEnvelope,
};
use sisa_messaging_rabbitmq::{
    ExchangeName, RabbitMqDeliverySource, RabbitMqPublisher, RabbitMqPublisherSettings,
    RabbitMqSettlement, RabbitMqSourceSettings, TypeRouteResolver,
};

/// Typical profile: 4 KiB body and eight custom headers.
const PAYLOAD_BYTES: usize = 4096;
const HEADER_COUNT: usize = 8;

/// Bounds broker memory during publish benchmarks; drop-head overflow still confirms.
const QUEUE_MAX_LENGTH: i32 = 10_000;

struct Topology<'a> {
    runtime: &'a tokio::runtime::Runtime,

    admin: Channel,

    exchange: String,

    queue: String,
}

impl Drop for Topology<'_> {
    fn drop(&mut self) {
        let _ = self.runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let _ = self
                    .admin
                    .queue_delete(self.queue.as_str().into(), QueueDeleteOptions::default())
                    .await;

                let _ = self
                    .admin
                    .exchange_delete(
                        self.exchange.as_str().into(),
                        ExchangeDeleteOptions::default(),
                    )
                    .await;
            })
            .await
        });
    }
}

impl Topology<'_> {
    fn purge(&self) {
        self.runtime
            .block_on(
                self.admin
                    .queue_purge(self.queue.as_str().into(), QueuePurgeOptions::default()),
            )
            .unwrap();
    }
}

fn envelope() -> SerializedEnvelope {
    let mut metadata = Metadata::default();

    for index in 0..HEADER_COUNT {
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
        payload: vec![b'x'; PAYLOAD_BYTES],
        metadata,
        ordering_key: None,
    }
}

async fn declare(connection: &Connection, name: &str) -> Channel {
    let admin = connection.create_channel().await.unwrap();

    admin
        .exchange_declare(
            name.into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let mut arguments = FieldTable::default();
    arguments.insert("x-max-length".into(), AMQPValue::LongInt(QUEUE_MAX_LENGTH));

    admin
        .queue_declare(name.into(), QueueDeclareOptions::default(), arguments)
        .await
        .unwrap();

    admin
        .queue_bind(
            name.into(),
            name.into(),
            "event.v1".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    admin
}

fn provider(c: &mut Criterion) {
    let Ok(url) = std::env::var("RABBITMQ_URL") else {
        return;
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _runtime_guard = runtime.enter();

    let name = format!(
        "sisa.bench.{}",
        MessageId::new().to_string().replace('-', "")
    );

    let connection = runtime
        .block_on(Connection::connect(&url, ConnectionProperties::default()))
        .unwrap_or_else(|_| panic!("RabbitMQ connection failed"));

    let topology = Topology {
        runtime: &runtime,
        admin: runtime.block_on(declare(&connection, &name)),
        exchange: name.clone(),
        queue: name.clone(),
    };

    let publisher = runtime.block_on(async {
        let channel = connection.create_channel().await.unwrap();

        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();

        RabbitMqPublisher::new(
            channel,
            TypeRouteResolver::new(ExchangeName::new(name.clone()).unwrap()),
            RabbitMqPublisherSettings {
                publish_timeout: Duration::from_secs(5),
                max_message_size: NonZeroU32::new(1 << 20).unwrap(),
            },
        )
        .unwrap()
    });

    let mut group = c.benchmark_group("provider");

    for concurrency in [1usize, 8, 32, 128] {
        group.throughput(Throughput::Elements(concurrency as u64));

        group.bench_function(format!("publish_confirmed_c{concurrency}"), |b| {
            b.iter_batched(
                || (0..concurrency).map(|_| envelope()).collect::<Vec<_>>(),
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

        topology.purge();
    }

    let mut source = RabbitMqDeliverySource::new(
        runtime.block_on(connection.create_channel()).unwrap(),
        RabbitMqSourceSettings {
            queue: name.clone(),
            prefetch: NonZeroU16::new(16).unwrap(),
        },
    )
    .unwrap();

    runtime
        .block_on(source.open(IndividualSourceRequirements::new().requiring_terminal_discard()))
        .unwrap();

    let received = |source: &mut RabbitMqDeliverySource| -> RabbitMqSettlement {
        runtime.block_on(publisher.publish(&envelope())).unwrap();

        runtime
            .block_on(source.receive())
            .unwrap()
            .unwrap()
            .into_parts()
            .1
    };

    group.throughput(Throughput::Elements(1));

    group.bench_function("receive_ack", |b| {
        b.iter_batched(
            || runtime.block_on(publisher.publish(&envelope())).unwrap(),
            |()| {
                runtime.block_on(async {
                    let settlement = source.receive().await.unwrap().unwrap().into_parts().1;
                    settlement.ack().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("requeue", |b| {
        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let settlement = received(&mut source);
                let start = Instant::now();

                let redelivery = runtime.block_on(async {
                    settlement.nak(Duration::ZERO).await.unwrap();

                    source.receive().await.unwrap().unwrap()
                });

                elapsed += start.elapsed();
                runtime.block_on(redelivery.into_parts().1.ack()).unwrap();
            }

            elapsed
        })
    });

    // Settlements are created one at a time: batching them would exceed the prefetch window.
    group.bench_function("terminate", |b| {
        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let settlement = received(&mut source);
                let start = Instant::now();
                runtime.block_on(settlement.terminate()).unwrap();
                elapsed += start.elapsed();
            }

            elapsed
        })
    });

    group.finish();

    let _ = runtime.block_on(source.close());
    drop(topology);
    let _ = runtime.block_on(connection.close(200, "OK".into()));
}

criterion_group!(benches, provider);
criterion_main!(benches);
