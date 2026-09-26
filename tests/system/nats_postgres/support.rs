//! Real PostgreSQL and JetStream fixtures, a test-owned effect table, and a recording handler.

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_nats::jetstream::{self, consumer::PullConsumer, consumer::pull, stream};
use serde::{Deserialize, Serialize};
use sisa_messaging::{
    Envelope, ErrorClassifier, FailureKind, JsonSerializer, Message, MessageId, Metadata,
    Publisher, Serializer,
};
use sisa_messaging_consumer::{
    Consumer, ConsumerError, ConsumerExit, ConsumerHandler, ConsumerSettings,
};
use sisa_messaging_inbox::{InboxScope, InboxSettings};
use sisa_messaging_nats::{
    NatsDeliverySource, NatsMapper, NatsPublisher, NatsPublisherSettings, Subject,
    TypeSubjectResolver,
};
use sisa_messaging_postgres::{PostgresInboxStore, PostgresInboxTransaction};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Bound for every wait on broker, database, or consumer progress.
pub(super) const PROGRESS_TIMEOUT: Duration = Duration::from_secs(15);

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
    std::env::var(name)
        .unwrap_or_else(|_| panic!("PostgreSQL system test connection configuration failed"))
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

/// Waits until the receipt satisfies `condition`.
pub(super) async fn wait_for_receipt(
    pool: &PgPool,
    scope: &InboxScope,
    message_id: MessageId,
    condition: impl Fn(Receipt) -> bool,
) -> Receipt {
    let deadline = Instant::now() + PROGRESS_TIMEOUT;

    loop {
        if let Some(receipt) = receipt(pool, scope, message_id).await
            && condition(receipt)
        {
            return receipt;
        }

        assert!(Instant::now() < deadline, "inbox receipt state not reached");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Removes this test's rows; every test uses its own scope.
pub(super) async fn cleanup(pool: &PgPool, scope: &InboxScope) {
    for statement in [
        "DELETE FROM system_test_effects WHERE scope = $1",
        "DELETE FROM inbox_receipts WHERE scope = $1",
    ] {
        sqlx::query(statement)
            .bind(scope.as_str())
            .execute(pool)
            .await
            .unwrap();
    }

    pool.close().await;
}

pub(super) fn scope() -> InboxScope {
    InboxScope::new(format!("system-{}", MessageId::new())).unwrap()
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
    invocations: Mutex<Vec<Instant>>,

    fail_first_after_write: bool,

    retry_gate: Option<Semaphore>,
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
        Self::new(scope, false, None)
    }

    /// Fails transiently after writing on the first invocation; later invocations first wait for
    /// [`Self::release_retry`].
    pub(super) fn failing_once_after_write(scope: &InboxScope) -> Self {
        Self::new(scope, true, Some(Semaphore::new(0)))
    }

    fn new(
        scope: &InboxScope,
        fail_first_after_write: bool,
        retry_gate: Option<Semaphore>,
    ) -> Self {
        Self {
            scope: scope.clone(),
            state: Arc::new(HandlerState {
                invocations: Mutex::new(Vec::new()),
                fail_first_after_write,
                retry_gate,
            }),
        }
    }

    pub(super) fn release_retry(&self) {
        if let Some(gate) = &self.state.retry_gate {
            gate.add_permits(1);
        }
    }

    pub(super) fn invocations(&self) -> Vec<Instant> {
        self.state
            .invocations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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

            invocations.push(Instant::now());

            invocations.len()
        };

        if invocation > 1
            && let Some(gate) = &self.state.retry_gate
            && let Ok(permit) = gate.acquire().await
        {
            permit.forget();
        }

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

/// A unique stream, durable pull consumer, and publisher on the server at `NATS_URL`.
pub(super) struct Broker {
    context: jetstream::Context,

    stream: stream::Stream,

    stream_name: String,

    durable: String,

    publisher: NatsPublisher<TypeSubjectResolver>,
}

impl Broker {
    pub(super) async fn new(ack_wait: Duration) -> (Self, PullConsumer) {
        let url = std::env::var("NATS_URL").expect("NATS_URL is required for this ignored test");
        let context = jetstream::new(async_nats::connect(url).await.unwrap());
        let suffix = MessageId::new().to_string().replace('-', "");
        let prefix = format!("system_{suffix}");
        let stream_name = format!("SYSTEM{suffix}");
        let durable = format!("durable{suffix}");

        let stream = context
            .create_stream(stream::Config {
                name: stream_name.clone(),
                subjects: vec![format!("{prefix}.>")],
                ..Default::default()
            })
            .await
            .unwrap();

        let pull_consumer = stream
            .create_consumer(pull::Config {
                durable_name: Some(durable.clone()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait,
                ..Default::default()
            })
            .await
            .unwrap();

        let publisher = NatsPublisher::new(
            context.clone(),
            TypeSubjectResolver::new(Subject::new(prefix).unwrap()),
            NatsPublisherSettings {
                publish_timeout: Duration::from_secs(5),
            },
        )
        .unwrap();

        let broker = Self {
            context,
            stream,
            stream_name,
            durable,
            publisher,
        };

        (broker, pull_consumer)
    }

    pub(super) async fn publish(&self, message_id: MessageId, label: &str) {
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
    }

    pub(super) async fn info(&self) -> jetstream::consumer::Info {
        self.stream.consumer_info(&self.durable).await.unwrap()
    }

    /// Waits until the broker has acknowledged every one of the first `count` stream messages.
    pub(super) async fn wait_acked(&self, count: u64) -> jetstream::consumer::Info {
        let deadline = Instant::now() + PROGRESS_TIMEOUT;

        loop {
            let info = self.info().await;

            if info.ack_floor.stream_sequence >= count && info.num_ack_pending == 0 {
                return info;
            }

            assert!(
                Instant::now() < deadline,
                "broker acknowledgement not observed"
            );

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub(super) async fn delete(self) {
        let _ = self.context.delete_stream(&self.stream_name).await;
    }
}

pub(super) fn settings(nak_delay: Duration) -> ConsumerSettings {
    let mut settings = ConsumerSettings::default();
    settings.max_in_flight = NonZeroUsize::new(4).unwrap();
    settings.source_timeout = Duration::from_secs(5);
    settings.database_timeout = Duration::from_secs(5);
    settings.settlement_timeout = Duration::from_secs(5);
    settings.nak_delay = nak_delay;
    settings.drain_timeout = Duration::from_secs(5);

    settings
}

/// A spawned consumer composed exactly as the application call site composes it.
pub(super) struct Running {
    cancel: CancellationToken,

    task: JoinHandle<Result<ConsumerExit, ConsumerError>>,
}

impl Running {
    pub(super) fn spawn(
        pool: &PgPool,
        pull_consumer: PullConsumer,
        scope: &InboxScope,
        handler: EffectHandler,
        settings: ConsumerSettings,
    ) -> Self {
        let inbox = PostgresInboxStore::new(pool.clone(), InboxSettings::default());
        let source = NatsDeliverySource::new(pull_consumer);
        let mapper = NatsMapper::new(TypeSubjectResolver::new(Subject::new("orders").unwrap()));

        let consumer = Consumer::<OrderCreated, _>::new(
            source,
            mapper,
            JsonSerializer,
            inbox,
            scope.clone(),
            handler,
            settings,
        )
        .unwrap();

        let cancel = CancellationToken::new();
        let task = tokio::spawn(consumer.run(cancel.child_token()));

        Self { cancel, task }
    }

    pub(super) async fn stop(self) -> Result<ConsumerExit, ConsumerError> {
        self.cancel.cancel();

        tokio::time::timeout(PROGRESS_TIMEOUT, self.task)
            .await
            .expect("consumer did not stop")
            .expect("consumer task panicked")
    }
}
