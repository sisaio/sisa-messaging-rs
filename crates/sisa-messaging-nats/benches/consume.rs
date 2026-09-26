//! Generic consumer runtime over real JetStream with a no-I/O inbox. Set NATS_URL to enable.
//!
//! Each iteration publishes its messages and creates a fresh filtered durable before timing.
//! Timing starts when the source's first pull request is observed on the broker API subject,
//! which excludes connection setup, the runtime's source-opening consumer lookup, and startup
//! validation, and ends when every message is settled with the scenario's expected operation, as
//! observed on the broker's acknowledgement subjects. After each timed region the iteration
//! asserts that the broker delivered every message exactly once.

#[path = "../tests/jetstream/inbox.rs"]
mod inbox;

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use async_nats::jetstream::{self, consumer::pull, stream};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures_util::StreamExt;
use sisa_messaging::{
    ContentType, Envelope, ErrorClassifier, FailureKind, Message, MessageId, Metadata, Publisher,
    SerializedEnvelope, Serializer,
};
use sisa_messaging_consumer::{Consumer, ConsumerHandler, ConsumerSettings};
use sisa_messaging_inbox::InboxScope;
use sisa_messaging_nats::{
    NatsDeliverySource, NatsMapper, NatsPublisher, NatsPublisherSettings, Subject,
    TypeSubjectResolver,
};
use tokio_util::sync::CancellationToken;

use inbox::{FakeInbox, FakeTransaction};

/// Messages per timed iteration of the settlement-path scenarios.
const PATH_MESSAGES: usize = 64;

/// Messages per timed iteration of the concurrency sweep.
const SWEEP_MESSAGES: usize = 256;

/// Bound for one iteration; exceeding it fails the benchmark instead of reporting a sample.
const ITERATION_TIMEOUT: Duration = Duration::from_secs(60);

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

/// Empty-body codec so the benchmark measures the runtime and provider, not payload decoding.
struct OrderCodec;

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
        Envelope::new(envelope.message_id, Order, envelope.metadata)
            .map_err(|_| BenchError(FailureKind::Permanent))
    }
}

#[derive(Clone, Copy)]
enum Behavior {
    Succeed,
    Fail(FailureKind),
    Sleep(Duration),
}

#[derive(Clone, Copy)]
struct BenchHandler(Behavior);

impl ConsumerHandler<Order, FakeTransaction> for BenchHandler {
    type Error = BenchError;

    async fn handle(
        &self,
        _tx: &mut FakeTransaction,
        _envelope: &Envelope<Order>,
    ) -> Result<(), Self::Error> {
        match self.0 {
            Behavior::Succeed => Ok(()),
            Behavior::Fail(kind) => Err(BenchError(kind)),
            Behavior::Sleep(duration) => {
                tokio::time::sleep(duration).await;

                Ok(())
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Scenario {
    messages: usize,

    max_in_flight: usize,

    behavior: Behavior,

    /// Pre-completes every message in the inbox before timing.
    completed_duplicates: bool,

    /// Settlement payload prefix counted toward completion: `+ACK`, `-NAK`, or `+TERM`.
    settled_with: &'static str,

    ack_wait: Duration,

    heartbeat_interval: Option<Duration>,
}

impl Scenario {
    const fn new(messages: usize, max_in_flight: usize, behavior: Behavior) -> Self {
        Self {
            messages,
            max_in_flight,
            behavior,
            completed_duplicates: false,
            settled_with: "+ACK",
            ack_wait: Duration::from_secs(30),
            heartbeat_interval: None,
        }
    }
}

struct Bench {
    runtime: tokio::runtime::Runtime,

    client: async_nats::Client,

    context: jetstream::Context,

    stream: stream::Stream,

    stream_name: String,

    base: String,

    iteration: u64,
}

impl Bench {
    fn settings(scenario: Scenario) -> ConsumerSettings {
        let mut settings = ConsumerSettings::default();
        settings.max_in_flight = NonZeroUsize::new(scenario.max_in_flight).unwrap();
        settings.heartbeat_interval = scenario.heartbeat_interval;
        // Retried deliveries stay delayed for the whole iteration so each is settled once.
        settings.nak_delay = Duration::from_secs(30);

        settings
    }

    /// Runs one timed iteration; setup and teardown are excluded.
    fn iterate(&mut self, scenario: Scenario) -> Duration {
        self.iteration += 1;
        let prefix = format!("{}.i{}", self.base, self.iteration);
        let durable = format!("bench{}", self.iteration);
        let inbox = FakeInbox::new(1_000);
        let scope = InboxScope::new("bench").unwrap();

        self.runtime.block_on(async {
            let publisher = NatsPublisher::new(
                self.context.clone(),
                TypeSubjectResolver::new(Subject::new(prefix.clone()).unwrap()),
                NatsPublisherSettings {
                    publish_timeout: Duration::from_secs(5),
                },
            )
            .unwrap();

            for _ in 0..scenario.messages {
                let message_id = MessageId::new();

                if scenario.completed_duplicates {
                    inbox.mark_completed(&scope, message_id);
                }

                let envelope = Envelope::new(message_id, Order, Metadata::default()).unwrap();
                let serialized = OrderCodec.serialize(&envelope).unwrap();
                publisher.publish(&serialized).await.unwrap();
            }

            let pull_consumer = self
                .stream
                .create_consumer(pull::Config {
                    durable_name: Some(durable.clone()),
                    filter_subject: format!("{prefix}.>"),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: scenario.ack_wait,
                    max_ack_pending: 1_024,
                    ..Default::default()
                })
                .await
                .unwrap();

            let mut settlements = self
                .client
                .subscribe(format!("$JS.ACK.{}.{durable}.>", self.stream_name))
                .await
                .unwrap();

            let mut pull_requests = self
                .client
                .subscribe(format!(
                    "$JS.API.CONSUMER.MSG.NEXT.{}.{durable}",
                    self.stream_name
                ))
                .await
                .unwrap();

            // Only the first pull request is needed; later ones must not load the connection.
            pull_requests.unsubscribe_after(1).await.unwrap();
            self.client.flush().await.unwrap();

            let source = NatsDeliverySource::new(pull_consumer);
            let mapper = NatsMapper::new(TypeSubjectResolver::new(Subject::new("orders").unwrap()));

            let consumer = Consumer::<Order, _>::new(
                source,
                mapper,
                OrderCodec,
                inbox,
                scope,
                BenchHandler(scenario.behavior),
                Self::settings(scenario),
            )
            .unwrap();

            let cancel = CancellationToken::new();
            let task = tokio::spawn(consumer.run(cancel.child_token()));

            // The first pull request follows the source's consumer lookup inside `run`.
            tokio::time::timeout(ITERATION_TIMEOUT, pull_requests.next())
                .await
                .unwrap()
                .unwrap();

            let started = Instant::now();
            let mut settled = 0;

            tokio::time::timeout(ITERATION_TIMEOUT, async {
                while settled < scenario.messages {
                    let message = settlements.next().await.unwrap();

                    if message
                        .payload
                        .starts_with(scenario.settled_with.as_bytes())
                    {
                        settled += 1;
                    }
                }
            })
            .await
            .unwrap();

            let elapsed = started.elapsed();
            cancel.cancel();
            task.await.unwrap().unwrap();

            // Untimed oracle: each delivery, including any redelivery, advances the consumer
            // sequence, so equality means no message was redelivered during the iteration.
            let info = self.stream.consumer_info(&durable).await.unwrap();

            assert_eq!(
                info.delivered.consumer_sequence, scenario.messages as u64,
                "a message was redelivered during the iteration"
            );

            self.stream.delete_consumer(&durable).await.unwrap();
            self.stream.purge().await.unwrap();

            elapsed
        })
    }
}

impl Drop for Bench {
    fn drop(&mut self) {
        let context = self.context.clone();
        let stream_name = self.stream_name.clone();

        let _ = self.runtime.block_on(async move {
            tokio::time::timeout(Duration::from_secs(5), context.delete_stream(stream_name)).await
        });
    }
}

fn run(c: &mut Criterion, bench: &mut Bench, group: &str, name: &str, scenario: Scenario) {
    let mut group = c.benchmark_group(group);
    group.throughput(Throughput::Elements(scenario.messages as u64));

    group.bench_function(name, |b| {
        b.iter_custom(|iterations| (0..iterations).map(|_| bench.iterate(scenario)).sum());
    });

    group.finish();
}

fn consume(c: &mut Criterion) {
    let Ok(url) = std::env::var("NATS_URL") else {
        return;
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async { async_nats::connect(url).await.unwrap() });
    // The JetStream context spawns its acknowledgement task on the current runtime.
    let context = runtime.block_on(async { jetstream::new(client.clone()) });
    let suffix = MessageId::new().to_string().replace('-', "");
    let base = format!("bench_{suffix}");
    let stream_name = format!("BENCH{suffix}");

    let stream = runtime.block_on(async {
        context
            .create_stream(stream::Config {
                name: stream_name.clone(),
                subjects: vec![format!("{base}.>")],
                ..Default::default()
            })
            .await
            .unwrap()
    });

    let mut bench = Bench {
        runtime,
        client,
        context,
        stream,
        stream_name,
        base,
        iteration: 0,
    };

    let path = "nats_consumer_path_concurrency_1";
    let noop = Scenario::new(PATH_MESSAGES, 1, Behavior::Succeed);
    run(c, &mut bench, path, "noop_commit_ack", noop);

    let duplicate = Scenario {
        completed_duplicates: true,
        ..noop
    };

    run(c, &mut bench, path, "completed_duplicate_ack", duplicate);

    let transient = Scenario {
        settled_with: "-NAK",
        ..Scenario::new(PATH_MESSAGES, 1, Behavior::Fail(FailureKind::Transient))
    };

    run(
        c,
        &mut bench,
        path,
        "transient_failure_delayed_nak",
        transient,
    );

    let permanent = Scenario {
        settled_with: "+TERM",
        ..Scenario::new(PATH_MESSAGES, 1, Behavior::Fail(FailureKind::Permanent))
    };

    run(
        c,
        &mut bench,
        path,
        "permanent_failure_terminate",
        permanent,
    );

    // Each handler outlives the acknowledgement deadline and survives only through heartbeats.
    let slow = Scenario {
        ack_wait: Duration::from_millis(500),
        heartbeat_interval: Some(Duration::from_millis(200)),
        ..Scenario::new(16, 16, Behavior::Sleep(Duration::from_millis(600)))
    };

    run(
        c,
        &mut bench,
        "nats_consumer_slow_handler",
        "sleep_600ms_heartbeat_200ms_x16",
        slow,
    );

    for concurrency in [1, 8, 32, 128] {
        run(
            c,
            &mut bench,
            "nats_consumer_independent_noop",
            &format!("concurrency_{concurrency}"),
            Scenario::new(SWEEP_MESSAGES, concurrency, Behavior::Succeed),
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
