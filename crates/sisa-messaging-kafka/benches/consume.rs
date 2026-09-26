//! Generic partitioned consumer over a real Kafka broker with a no-I/O inbox.
//!
//! Set `SISA_KAFKA_BOOTSTRAP_SERVERS` and `SISA_KAFKA_BENCH_TOPIC` (a pre-provisioned topic with
//! eight partitions) to enable it. Each iteration publishes its records, then starts a fresh
//! static member in a fresh group seeded at every partition's high watermark. Timing starts when
//! the first record reaches the runtime's payload decode, which excludes client creation, the
//! transactional producer's initialization, the topic check, and the group join. It ends when a
//! separate `read_committed` observer sees every partition's committed offset past its last
//! record. After each timed region the iteration asserts that no record was handled twice.

#[path = "../tests/support/inbox.rs"]
mod inbox;

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use criterion::{Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer as _};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use sisa_messaging::{
    ContentType, Envelope, EnvelopeMapper, ErrorClassifier, FailureKind, Message, MessageId,
    Metadata, SerializedEnvelope, Serializer,
};
use sisa_messaging_consumer::{Consumer, ConsumerHandler, ConsumerSettings};
use sisa_messaging_inbox::InboxScope;
use sisa_messaging_kafka::{
    KafkaClient, KafkaClientSettings, KafkaConsumerSettings, KafkaEnvelopeMapper,
    KafkaShutdownOutcome,
};
use tokio_util::sync::CancellationToken;

use inbox::{FakeInbox, FakeTransaction};

/// Records per timed iteration of the single-partition settlement paths.
const PATH_MESSAGES: usize = 64;

/// Records per timed iteration of the multi-partition concurrency sweep.
const SWEEP_MESSAGES: usize = 256;

/// Partitions the sweep spreads its records over.
const SWEEP_PARTITIONS: i32 = 8;

/// Bound for one iteration; exceeding it fails the benchmark instead of reporting a sample.
const ITERATION_TIMEOUT: Duration = Duration::from_secs(120);

struct Order;

impl Message for Order {
    const TYPE: &'static str = "order-created";
    const VERSION: u32 = 1;
}

#[derive(Debug)]
struct BenchError(FailureKind);

impl fmt::Display for BenchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("benchmark operation failed")
    }
}

impl Error for BenchError {}

impl ErrorClassifier for BenchError {
    fn classify(&self) -> FailureKind {
        self.0
    }
}

/// Empty-body codec that records when the first record reaches the runtime.
#[derive(Clone, Default)]
struct OrderCodec {
    first_decode: Arc<OnceLock<Instant>>,
}

impl Serializer<Order> for OrderCodec {
    type Error = BenchError;

    fn serialize(&self, envelope: &Envelope<Order>) -> Result<SerializedEnvelope, Self::Error> {
        Ok(SerializedEnvelope {
            message_id: envelope.message_id(),
            message_type: envelope.message_type().clone(),
            message_version: envelope.message_version(),
            content_type: ContentType::new("application/octet-stream")
                .map_err(|_| BenchError(FailureKind::Permanent))?,
            payload: Vec::new(),
            metadata: envelope.metadata().clone(),
            ordering_key: None,
        })
    }

    fn deserialize(&self, envelope: SerializedEnvelope) -> Result<Envelope<Order>, Self::Error> {
        let _ = self.first_decode.set(Instant::now());

        Envelope::new(envelope.message_id, Order, envelope.metadata)
            .map_err(|_| BenchError(FailureKind::Permanent))
    }
}

/// Counts handler invocations per record.
#[derive(Clone, Default)]
struct BenchHandler {
    handled: Arc<Mutex<HashMap<MessageId, u32>>>,
}

impl ConsumerHandler<Order, FakeTransaction> for BenchHandler {
    type Error = BenchError;

    async fn handle(
        &self,
        _tx: &mut FakeTransaction,
        envelope: &Envelope<Order>,
    ) -> Result<(), Self::Error> {
        *self
            .handled
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(envelope.message_id())
            .or_default() += 1;

        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Scenario {
    messages: usize,

    partitions: i32,

    max_in_flight: usize,

    /// Pre-completes every record in the inbox, so each advances without the handler.
    completed_duplicates: bool,
}

struct Bench {
    runtime: tokio::runtime::Runtime,

    brokers: String,

    topic: String,

    client: KafkaClient,

    producer: FutureProducer,

    iteration: u64,
}

impl Bench {
    fn observer(&self, group: &str) -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .set("allow.auto.create.topics", "false")
            .create()
            .unwrap()
    }

    /// Commits each partition's high watermark for `group`; returns the seeded offsets.
    fn seed(&self, observer: &BaseConsumer, partitions: i32) -> HashMap<i32, i64> {
        let mut offsets = TopicPartitionList::new();
        let mut seeded = HashMap::new();

        for partition in 0..partitions {
            let (_, high) = observer
                .fetch_watermarks(&self.topic, partition, Duration::from_secs(10))
                .unwrap();

            offsets
                .add_partition_offset(&self.topic, partition, Offset::Offset(high))
                .unwrap();

            seeded.insert(partition, high);
        }

        observer.commit(&offsets, CommitMode::Sync).unwrap();

        seeded
    }

    async fn publish(&self, message_id: MessageId, partition: i32) -> i64 {
        let envelope = Envelope::new(message_id, Order, Metadata::default()).unwrap();
        let serialized = OrderCodec::default().serialize(&envelope).unwrap();
        let record = KafkaEnvelopeMapper.encode(&serialized).unwrap();

        let headers = record.headers.iter().fold(
            OwnedHeaders::new_with_capacity(record.headers.len()),
            |headers, header| {
                headers.insert(Header {
                    key: &header.name,
                    value: header.value.as_deref(),
                })
            },
        );

        let delivery = self
            .producer
            .send(
                FutureRecord::<(), [u8]>::to(&self.topic)
                    .partition(partition)
                    .payload(&record.payload)
                    .headers(headers),
                Duration::from_secs(5),
            )
            .await
            .unwrap_or_else(|_| panic!("benchmark record was not confirmed"));

        delivery.offset
    }

    /// Whether every partition's committed offset reached its target.
    fn committed(&self, observer: &BaseConsumer, targets: &HashMap<i32, i64>) -> bool {
        let mut partitions = TopicPartitionList::new();

        for partition in targets.keys() {
            partitions.add_partition(&self.topic, *partition);
        }

        let Ok(committed) = observer.committed_offsets(partitions, Duration::from_secs(10)) else {
            return false;
        };

        targets.iter().all(|(partition, target)| {
            committed
                .find_partition(&self.topic, *partition)
                .is_some_and(|entry| entry.offset() == Offset::Offset(*target))
        })
    }

    /// Runs one timed iteration; setup and teardown are excluded.
    fn iterate(&mut self, scenario: Scenario) -> Duration {
        self.iteration += 1;
        let group = format!("sisa-kafka-bench-{}", MessageId::new());
        let observer = self.observer(&group);
        let _ = self.seed(&observer, SWEEP_PARTITIONS);
        let scope = InboxScope::new("bench").unwrap();
        let inbox = FakeInbox::new(1_000);
        let handler = BenchHandler::default();
        let codec = OrderCodec::default();

        let mut targets = HashMap::new();
        let mut ids = Vec::with_capacity(scenario.messages);

        for index in 0..scenario.messages {
            let message_id = MessageId::new();
            let partition = i32::try_from(index).unwrap() % scenario.partitions;

            if scenario.completed_duplicates {
                inbox.mark_completed(&scope, message_id);
            }

            let offset = self.runtime.block_on(self.publish(message_id, partition));
            targets.insert(partition, offset + 1);
            ids.push(message_id);
        }

        let source = self
            .client
            .delivery_source(
                KafkaConsumerSettings::new(group.as_str(), "bench-member", [self.topic.as_str()])
                    .unwrap(),
            )
            .unwrap();

        let shutdown = source.shutdown_handle();
        let mut settings = ConsumerSettings::default();
        settings.max_in_flight = NonZeroUsize::new(scenario.max_in_flight).unwrap();
        settings.source_timeout = Duration::from_secs(30);

        let consumer = Consumer::new_partitioned(
            source,
            KafkaEnvelopeMapper,
            codec.clone(),
            inbox.clone(),
            scope.clone(),
            handler.clone(),
            settings,
        )
        .unwrap();

        let cancel = CancellationToken::new();
        let task = self.runtime.spawn(consumer.run_partitioned(cancel.clone()));

        let deadline = Instant::now() + ITERATION_TIMEOUT;

        let started = loop {
            if let Some(started) = codec.first_decode.get() {
                break *started;
            }

            assert!(Instant::now() < deadline, "no record reached the consumer");

            assert!(
                !task.is_finished(),
                "the consumer stopped before its first record"
            );

            std::thread::sleep(Duration::from_micros(200));
        };

        while !self.committed(&observer, &targets) {
            assert!(
                Instant::now() < deadline,
                "committed offsets did not advance"
            );

            assert!(
                !task.is_finished(),
                "the consumer stopped before committing"
            );
        }

        let elapsed = started.elapsed();

        cancel.cancel();
        self.runtime.block_on(task).unwrap().unwrap();

        let closed = self
            .runtime
            .block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(30),
                    std::future::IntoFuture::into_future(shutdown),
                )
                .await
            })
            .unwrap();

        assert_eq!(closed, KafkaShutdownOutcome::Closed);

        // Untimed oracle: every record was handled exactly once, or never for completed ones.
        let handled = handler
            .handled
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();

        if scenario.completed_duplicates {
            assert!(
                handled.is_empty(),
                "a completed duplicate invoked the handler"
            );
        } else {
            assert_eq!(handled.len(), ids.len(), "a record was not handled");

            assert!(
                handled.values().all(|count| *count == 1),
                "a record was handled twice"
            );
        }

        elapsed
    }
}

fn run(c: &mut Criterion, bench: &mut Bench, group: &str, name: &str, scenario: Scenario) {
    let mut group = c.benchmark_group(group);
    group.throughput(Throughput::Elements(scenario.messages as u64));
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function(name, |b| {
        b.iter_custom(|iterations| (0..iterations).map(|_| bench.iterate(scenario)).sum());
    });

    group.finish();
}

fn consume(c: &mut Criterion) {
    let (Ok(brokers), Ok(topic)) = (
        std::env::var("SISA_KAFKA_BOOTSTRAP_SERVERS"),
        std::env::var("SISA_KAFKA_BENCH_TOPIC"),
    ) else {
        return;
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();

    let client = KafkaClient::start(
        KafkaClientSettings::new([brokers.as_str()])
            .with_advanced_property("allow.auto.create.topics", "false")
            .unwrap(),
    )
    .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("linger.ms", "0")
        .create()
        .unwrap();

    let mut bench = Bench {
        runtime,
        brokers,
        topic,
        client,
        producer,
        iteration: 0,
    };

    let path = "kafka_consumer_path_partition_1";

    let advance = Scenario {
        messages: PATH_MESSAGES,
        partitions: 1,
        max_in_flight: 1,
        completed_duplicates: false,
    };

    run(c, &mut bench, path, "noop_commit_advance", advance);

    let duplicate = Scenario {
        completed_duplicates: true,
        ..advance
    };

    run(
        c,
        &mut bench,
        path,
        "completed_duplicate_advance",
        duplicate,
    );

    for max_in_flight in [1, 8, 32, 128] {
        run(
            c,
            &mut bench,
            "kafka_consumer_partitions_8_noop",
            &format!("max_in_flight_{max_in_flight}"),
            Scenario {
                messages: SWEEP_MESSAGES,
                partitions: SWEEP_PARTITIONS,
                max_in_flight,
                completed_duplicates: false,
            },
        );
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5));
    targets = consume
}

criterion_main!(benches);
