//! Real PostgreSQL and Kafka fixtures, a test-owned effect table, and a recording handler.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer as _};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use sisa_messaging::{
    Envelope, ErrorClassifier, FailureKind, JsonSerializer, Message, MessageId, Metadata,
    Publisher, SerializedEnvelope, Serializer,
};
use sisa_messaging_consumer::{
    Consumer, ConsumerError, ConsumerExit, ConsumerHandler, ConsumerSettings,
};
use sisa_messaging_inbox::{InboxScope, InboxSettings};
use sisa_messaging_kafka::{
    KafkaClient, KafkaClientSettings, KafkaConsumerSettings, KafkaEnvelopeMapper, KafkaPublisher,
    KafkaPublisherSettings, KafkaShutdownOutcome, KafkaSourceShutdown, KafkaTopicResolver,
    RoutingDestinationError,
};
use sisa_messaging_postgres::{PostgresInboxStore, PostgresInboxTransaction};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

/// Bound for every wait on broker, database, or consumer progress.
pub(super) const PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);

/// The single-partition topic every scenario publishes to; each uses its own consumer group.
const TOPIC_ENV: &str = "SISA_KAFKA_TEST_TOPIC";

const BROKERS_ENV: &str = "SISA_KAFKA_BOOTSTRAP_SERVERS";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct OrderCreated {
    pub(super) label: String,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "order-created";
    const VERSION: u32 = 1;
}

fn connect_options() -> PgConnectOptions {
    match std::env::var("DATABASE_URL") {
        Ok(url) => PgConnectOptions::from_str(&url)
            .unwrap_or_else(|_| panic!("PostgreSQL system test connection configuration failed")),
        Err(std::env::VarError::NotPresent) => {
            let port = required("PGPORT").parse::<u16>().unwrap_or_else(|_| {
                panic!("PostgreSQL system test connection configuration failed")
            });

            PgConnectOptions::new()
                .host(&required("PGHOST"))
                .port(port)
                .username(&required("PGUSER"))
                .database(&required("PGDATABASE"))
        }
        Err(_) => panic!("PostgreSQL system test connection configuration failed"),
    }
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("system test configuration is incomplete"))
}

/// Connects and creates the test-owned effect table once across concurrent tests.
pub(super) async fn pool() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(connect_options())
        .await
        .unwrap_or_else(|_| panic!("PostgreSQL system test connection failed"));

    let mut transaction = pool.begin().await.unwrap();

    // Serializes concurrent `CREATE TABLE IF NOT EXISTS` across parallel tests.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('sisa-messaging-system-tests'))")
        .execute(&mut *transaction)
        .await
        .unwrap();

    // No uniqueness constraint: a duplicate business effect must stay observable.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS system_test_effects (
            scope text NOT NULL,
            message_id uuid NOT NULL,
            label text NOT NULL
        )",
    )
    .execute(&mut *transaction)
    .await
    .unwrap();

    transaction.commit().await.unwrap();

    pool
}

pub(super) async fn effect_count(pool: &PgPool, scope: &InboxScope, message_id: MessageId) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM system_test_effects WHERE scope = $1 AND message_id = $2",
    )
    .bind(scope.as_str())
    .bind(message_id.into_uuid())
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Durable inbox receipt state observed from an independent connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Receipt {
    pub(super) attempts: i32,

    pub(super) completed: bool,

    pub(super) dead: bool,
}

pub(super) async fn receipt(
    pool: &PgPool,
    scope: &InboxScope,
    message_id: MessageId,
) -> Option<Receipt> {
    sqlx::query_as::<_, (i32, bool, bool)>(
        "SELECT attempts, completed_at IS NOT NULL, dead_at IS NOT NULL
         FROM inbox_receipts WHERE scope = $1 AND message_id = $2",
    )
    .bind(scope.as_str())
    .bind(message_id.into_uuid())
    .fetch_optional(pool)
    .await
    .unwrap()
    .map(|(attempts, completed, dead)| Receipt {
        attempts,
        completed,
        dead,
    })
}

/// Removes this test's rows through a short-lived pool; every test uses its own scope.
///
/// Failures are returned rather than raised so they never replace a test body's own panic.
async fn cleanup(scope: &InboxScope) -> Result<(), &'static str> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options())
        .await
        .map_err(|_| "PostgreSQL system test cleanup connection failed")?;

    let mut result = Ok(());

    for statement in [
        "DELETE FROM system_test_effects WHERE scope = $1",
        "DELETE FROM inbox_receipts WHERE scope = $1",
    ] {
        if sqlx::query(statement)
            .bind(scope.as_str())
            .execute(&pool)
            .await
            .is_err()
        {
            result = Err("PostgreSQL system test cleanup delete failed");
            break;
        }
    }

    pool.close().await;

    result
}

pub(super) fn scope() -> InboxScope {
    InboxScope::new(format!("system-kafka-{}", MessageId::new())).unwrap()
}

#[derive(Debug)]
pub(super) struct EffectError;

impl fmt::Display for EffectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("effect handler failed")
    }
}

impl Error for EffectError {}

impl ErrorClassifier for EffectError {
    fn classify(&self) -> FailureKind {
        FailureKind::Transient
    }
}

struct HandlerState {
    /// Invocations per message id; other scenarios may share the topic.
    invocations: Mutex<HashMap<MessageId, usize>>,

    fail_first_after_write: bool,
}

/// Writes one effect row in the inbox transaction through the transaction's connection.
#[derive(Clone)]
pub(super) struct EffectHandler {
    scope: InboxScope,

    state: Arc<HandlerState>,
}

impl EffectHandler {
    /// Succeeds on every invocation.
    pub(super) fn succeeding(scope: &InboxScope) -> Self {
        Self::new(scope, false)
    }

    /// Fails transiently after writing on each message's first invocation.
    pub(super) fn failing_once_after_write(scope: &InboxScope) -> Self {
        Self::new(scope, true)
    }

    fn new(scope: &InboxScope, fail_first_after_write: bool) -> Self {
        Self {
            scope: scope.clone(),
            state: Arc::new(HandlerState {
                invocations: Mutex::new(HashMap::new()),
                fail_first_after_write,
            }),
        }
    }

    pub(super) fn invocations(&self, message_id: MessageId) -> usize {
        self.state
            .invocations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&message_id)
            .copied()
            .unwrap_or(0)
    }
}

impl ConsumerHandler<OrderCreated, PostgresInboxTransaction> for EffectHandler {
    type Error = EffectError;

    async fn handle(
        &self,
        tx: &mut PostgresInboxTransaction,
        envelope: &Envelope<OrderCreated>,
    ) -> Result<(), Self::Error> {
        let invocation = {
            let mut invocations = self
                .state
                .invocations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);

            let count = invocations.entry(envelope.message_id()).or_default();
            *count += 1;

            *count
        };

        sqlx::query(
            "INSERT INTO system_test_effects (scope, message_id, label) VALUES ($1, $2, $3)",
        )
        .bind(self.scope.as_str())
        .bind(envelope.message_id().into_uuid())
        .bind(&envelope.payload().label)
        .execute(&mut **tx)
        .await
        .map_err(|_| EffectError)?;

        if self.state.fail_first_after_write && invocation == 1 {
            return Err(EffectError);
        }

        Ok(())
    }
}

/// Routes every order to the test topic.
struct FixedTopic(String);

impl KafkaTopicResolver for FixedTopic {
    type Error = RoutingDestinationError;

    fn resolve(&self, _envelope: &SerializedEnvelope) -> Result<String, Self::Error> {
        Ok(self.0.clone())
    }
}

/// A fresh consumer group on the shared topic, seeded at the topic's end.
pub(super) struct Broker {
    brokers: String,

    topic: String,

    group: String,

    client: KafkaClient,

    publisher: KafkaPublisher<FixedTopic>,
}

impl Broker {
    pub(super) fn new() -> Self {
        let brokers = required(BROKERS_ENV);
        let topic = required(TOPIC_ENV);
        let group = format!("sisa-system-{}", MessageId::new());

        let settings = KafkaClientSettings::new([brokers.as_str()])
            .with_advanced_property("allow.auto.create.topics", "false")
            .and_then(|settings| settings.with_advanced_property("enable.idempotence", "true"))
            .and_then(|settings| settings.with_advanced_property("session.timeout.ms", "6000"))
            .and_then(|settings| settings.with_advanced_property("heartbeat.interval.ms", "500"))
            .unwrap();

        let client = KafkaClient::start(settings).unwrap();

        let publisher = KafkaPublisher::new(
            client.clone(),
            FixedTopic(topic.clone()),
            KafkaPublisherSettings::default(),
        );

        let broker = Self {
            brokers,
            topic,
            group,
            client,
            publisher,
        };

        broker.seed_at_end();

        broker
    }

    fn reader(&self) -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group)
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .set("allow.auto.create.topics", "false")
            .create()
            .unwrap()
    }

    fn seed_at_end(&self) {
        let reader = self.reader();

        let (_, high) = reader
            .fetch_watermarks(&self.topic, 0, PROGRESS_TIMEOUT)
            .unwrap();

        let mut offsets = TopicPartitionList::new();

        offsets
            .add_partition_offset(&self.topic, 0, Offset::Offset(high))
            .unwrap();

        reader.commit(&offsets, CommitMode::Sync).unwrap();
    }

    /// Publishes through the provider's publisher; returns the log end after the record.
    pub(super) async fn publish(&self, message_id: MessageId, label: &str) -> i64 {
        let envelope = Envelope::new(
            message_id,
            OrderCreated {
                label: label.to_owned(),
            },
            Metadata::default(),
        )
        .unwrap();

        let serialized = JsonSerializer.serialize(&envelope).unwrap();
        self.publisher.publish(&serialized).await.unwrap();

        self.reader()
            .fetch_watermarks(&self.topic, 0, PROGRESS_TIMEOUT)
            .unwrap()
            .1
    }

    /// The group's stable committed cursor.
    pub(super) fn committed(&self) -> i64 {
        let mut partitions = TopicPartitionList::new();
        partitions.add_partition(&self.topic, 0);

        let committed = self
            .reader()
            .committed_offsets(partitions, PROGRESS_TIMEOUT)
            .unwrap();

        match committed
            .find_partition(&self.topic, 0)
            .map(|entry| entry.offset())
        {
            Some(Offset::Offset(offset)) => offset,
            _ => panic!("the seeded group has a committed cursor"),
        }
    }

    /// Waits until the committed cursor reaches `next`.
    pub(super) async fn wait_committed(&self, next: i64) {
        let deadline = Instant::now() + PROGRESS_TIMEOUT;

        while self.committed() < next {
            assert!(Instant::now() < deadline, "committed cursor not advanced");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

pub(super) fn settings() -> ConsumerSettings {
    let mut settings = ConsumerSettings::default();
    settings.max_in_flight = NonZeroUsize::new(4).unwrap();
    settings.source_timeout = Duration::from_secs(30);
    settings.database_timeout = Duration::from_secs(5);
    settings.settlement_timeout = Duration::from_secs(15);
    settings.drain_timeout = Duration::from_secs(5);

    settings
}

/// A spawned consumer composed exactly as the application call site composes it.
pub(super) struct Running {
    cancel: CancellationToken,

    /// Aborted if a failing test unwinds, releasing its transactions before cleanup.
    task: AbortOnDropHandle<Result<ConsumerExit, ConsumerError>>,

    shutdown: KafkaSourceShutdown,
}

impl Running {
    pub(super) fn spawn(
        pool: &PgPool,
        broker: &Broker,
        scope: &InboxScope,
        handler: EffectHandler,
    ) -> Self {
        let inbox = PostgresInboxStore::new(pool.clone(), InboxSettings::default());

        let source = broker
            .client
            .delivery_source(
                KafkaConsumerSettings::new(
                    broker.group.as_str(),
                    "system-member",
                    [broker.topic.as_str()],
                )
                .unwrap(),
            )
            .unwrap();

        let shutdown = source.shutdown_handle();

        let consumer = Consumer::<OrderCreated, _>::new_partitioned(
            source,
            KafkaEnvelopeMapper,
            JsonSerializer,
            inbox,
            scope.clone(),
            handler,
            settings(),
        )
        .unwrap();

        let cancel = CancellationToken::new();

        let task =
            AbortOnDropHandle::new(tokio::spawn(consumer.run_partitioned(cancel.child_token())));

        Self {
            cancel,
            task,
            shutdown,
        }
    }

    /// Waits for the consumer to stop on its own, then for its Kafka member to close.
    pub(super) async fn finish(self) -> Result<ConsumerExit, ConsumerError> {
        let exit = tokio::time::timeout(PROGRESS_TIMEOUT, self.task)
            .await
            .expect("consumer did not stop")
            .expect("consumer task panicked");

        let closed = tokio::time::timeout(PROGRESS_TIMEOUT, self.shutdown)
            .await
            .expect("Kafka member did not close");

        assert_eq!(closed, KafkaShutdownOutcome::Closed);

        exit
    }

    pub(super) async fn stop(self) -> Result<ConsumerExit, ConsumerError> {
        self.cancel.cancel();

        self.finish().await
    }
}

/// Per-test resources handed to a test body.
pub(super) struct Fixture {
    pub(super) pool: PgPool,

    pub(super) broker: Arc<Broker>,

    pub(super) scope: InboxScope,
}

/// Serializes the Kafka scenarios within this test process; the lock never poisons.
static SCENARIOS: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Runs `body` with a fresh pool, consumer group, and scope, and removes the scope's rows even
/// when it panics.
///
/// Asynchronous cleanup cannot run in `Drop`, so the body runs as a task: a panic surfaces as a
/// `JoinError`, cleanup runs, and the original panic then resumes.
pub(super) async fn run_with_cleanup<F, Fut>(body: F)
where
    F: FnOnce(Fixture) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    // The scenarios share one partition, so a concurrent scenario's records would reach this
    // scenario's consumer group; run them one at a time, cleanup included.
    let _serial = SCENARIOS.lock().await;

    let pool = pool().await;
    let broker = Arc::new(Broker::new());
    let scope = scope();

    let outcome = tokio::spawn(body(Fixture {
        pool: pool.clone(),
        broker,
        scope: scope.clone(),
    }))
    .await;

    // Closing the test pool waits until no checked-out connection remains, so aborted
    // transactions are rolled back or finished before the deletes run on a separate pool.
    let drained = tokio::time::timeout(PROGRESS_TIMEOUT, pool.close())
        .await
        .is_ok();

    let cleaned = if drained {
        cleanup(&scope).await
    } else {
        Err("test connections did not return to the pool; cleanup was skipped")
    };

    // The body's own failure takes precedence over any cleanup failure.
    if let Err(error) = outcome {
        match error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_) => panic!("system test body was cancelled"),
        }
    }

    if let Err(message) = cleaned {
        panic!("{message}");
    }
}
