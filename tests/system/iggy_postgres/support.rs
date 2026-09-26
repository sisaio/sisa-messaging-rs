//! Real PostgreSQL and Apache Iggy fixtures, a test-owned effect table, and an effect handler.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use iggy::prelude::{
    AutoLogin, Client, ClientWrapper, ConsumerGroupClient, Credentials,
    IggyClient as RawIggyClient, StreamClient, TcpClient, TcpClientConfig,
    TcpClientReconnectionConfig, TopicClient, TopicCreateOptions,
};
use serde::{Deserialize, Serialize};
use sisa_messaging::{
    Envelope, ErrorClassifier, FailureKind, JsonSerializer, Message, MessageId, Metadata,
    MetadataValue, Publisher, RoutingMetadata, Serializer,
};
use sisa_messaging_consumer::{
    Consumer, ConsumerError, ConsumerExit, ConsumerHandler, ConsumerSettings,
};
use sisa_messaging_iggy::{
    Identifier, IggyClient, IggyClientSettings, IggyCredentials, IggyDeliverySource,
    IggyEnvelopeMapper, IggyPublisher, IggyPublisherSettings, IggySourceSettings,
    RoutingDestinationResolver,
};
use sisa_messaging_inbox::{InboxScope, InboxSettings};
use sisa_messaging_postgres::{PostgresInboxStore, PostgresInboxTransaction};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

/// Bound for every wait on broker, database, or consumer progress.
pub(super) const PROGRESS_TIMEOUT: Duration = Duration::from_secs(60);

/// Default address and root credentials of the throwaway local broker the ignored tests target.
const DEFAULT_SERVER_ADDRESS: &str = "127.0.0.1:8090";
const DEFAULT_USERNAME: &str = "iggy";
const DEFAULT_PASSWORD: &str = "iggy";
const DEFAULT_STREAM: &str = "sisa-iggy-test-stream";

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

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn server_address() -> String {
    env_or("SISA_IGGY_SERVER_ADDRESS", DEFAULT_SERVER_ADDRESS)
}

fn username() -> String {
    env_or("SISA_IGGY_USERNAME", DEFAULT_USERNAME)
}

fn password() -> String {
    env_or("SISA_IGGY_PASSWORD", DEFAULT_PASSWORD)
}

fn identifier(name: &str) -> Identifier {
    Identifier::from_str_value(name).unwrap_or_else(|_| panic!("invalid test identifier"))
}

/// Connects and creates the test-owned effect table once across concurrent tests.
pub(super) async fn pool() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(16)
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

/// Effect rows per message id for one scope.
pub(super) async fn effects(pool: &PgPool, scope: &InboxScope) -> Vec<(MessageId, i64)> {
    sqlx::query_as::<_, (sqlx::types::Uuid, i64)>(
        "SELECT message_id, count(*) FROM system_test_effects WHERE scope = $1 GROUP BY message_id",
    )
    .bind(scope.as_str())
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|(id, count)| (MessageId::from_uuid(id), count))
    .collect()
}

/// Members that wrote at least one effect row for one scope.
pub(super) async fn effect_members(pool: &PgPool, scope: &InboxScope) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT DISTINCT split_part(label, '/', 1) FROM system_test_effects WHERE scope = $1",
    )
    .bind(scope.as_str())
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Completed inbox receipts for one scope.
pub(super) async fn completed(pool: &PgPool, scope: &InboxScope) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM inbox_receipts WHERE scope = $1 AND completed_at IS NOT NULL",
    )
    .bind(scope.as_str())
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Waits until at least `count` receipts of the scope are completed.
pub(super) async fn wait_completed(pool: &PgPool, scope: &InboxScope, count: i64) {
    let deadline = Instant::now() + PROGRESS_TIMEOUT;

    while completed(pool, scope).await < count {
        assert!(Instant::now() < deadline, "inbox completions not reached");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
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

/// Writes one effect row labeled with its member in the inbox transaction, after a short delay
/// that widens the window in which a rebalance lands while records are in flight.
#[derive(Clone)]
pub(super) struct EffectHandler {
    scope: InboxScope,

    member: &'static str,
}

impl ConsumerHandler<OrderCreated, PostgresInboxTransaction> for EffectHandler {
    type Error = EffectError;

    async fn handle(
        &self,
        tx: &mut PostgresInboxTransaction,
        envelope: &Envelope<OrderCreated>,
    ) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_millis(5)).await;

        sqlx::query(
            "INSERT INTO system_test_effects (scope, message_id, label) VALUES ($1, $2, $3)",
        )
        .bind(self.scope.as_str())
        .bind(envelope.message_id().into_uuid())
        .bind(format!("{}/{}", self.member, envelope.payload().label))
        .execute(&mut **tx)
        .await
        .map_err(|_| EffectError)?;

        Ok(())
    }
}

/// A unique topic and pre-provisioned consumer group, with a raw provisioning client and a
/// publisher.
pub(super) struct Broker {
    raw: RawIggyClient,

    publisher_client: IggyClient,

    publisher: IggyPublisher<RoutingDestinationResolver>,

    stream: String,

    topic: String,

    group: String,
}

async fn iggy_client() -> IggyClient {
    let settings = IggyClientSettings::new(
        server_address(),
        IggyCredentials::UsernamePassword {
            username: username(),
            password: password(),
        },
    );

    IggyClient::start(settings)
        .await
        .unwrap_or_else(|_| panic!("Iggy system test client failed to connect"))
}

impl Broker {
    pub(super) async fn new(partitions: u32) -> Self {
        let config = TcpClientConfig {
            server_address: server_address(),
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                username(),
                password().into(),
            )),
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        };

        let tcp = TcpClient::create(Arc::new(config))
            .unwrap_or_else(|_| panic!("Iggy system test client configuration is invalid"));

        let raw = RawIggyClient::create(ClientWrapper::Tcp(tcp), None, None);

        Client::connect(&raw)
            .await
            .unwrap_or_else(|_| panic!("Iggy system test provisioning client failed to connect"));

        let suffix = MessageId::new();
        let stream = env_or("SISA_IGGY_TEST_STREAM", DEFAULT_STREAM);
        let topic = format!("system-topic-{suffix}");
        let group = format!("system-group-{suffix}");
        let stream_id = identifier(&stream);

        if raw.get_stream(&stream_id).await.unwrap().is_none() {
            let _ = raw.create_stream(&stream).await;
        }

        raw.create_topic(
            &stream_id,
            &topic,
            &TopicCreateOptions {
                partitions_count: Some(partitions),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();

        raw.create_consumer_group(&stream_id, &identifier(&topic), &group)
            .await
            .unwrap();

        let publisher_client = iggy_client().await;

        let publisher = IggyPublisher::new(
            publisher_client.clone(),
            RoutingDestinationResolver,
            IggyPublisherSettings::default(),
        );

        Self {
            raw,
            publisher_client,
            publisher,
            stream,
            topic,
            group,
        }
    }

    /// Publishes one order without an ordering key, so the publisher spreads records across
    /// partitions round-robin.
    pub(super) async fn publish(&self, message_id: MessageId, label: &str) {
        let destination = MetadataValue::new(format!("{}/{}", self.stream, self.topic)).unwrap();

        let envelope = Envelope::new(
            message_id,
            OrderCreated {
                label: label.to_owned(),
            },
            Metadata {
                routing: RoutingMetadata {
                    destination: Some(destination),
                    ..RoutingMetadata::default()
                },
                ..Metadata::default()
            },
        )
        .unwrap();

        let serialized = JsonSerializer.serialize(&envelope).unwrap();
        self.publisher.publish(&serialized).await.unwrap();
    }

    fn source_settings(&self) -> IggySourceSettings {
        IggySourceSettings::new(
            identifier(&self.stream),
            identifier(&self.topic),
            identifier(&self.group),
        )
        // Small batches keep records in flight on every partition when the second member joins.
        .with_batch_length(4)
        .and_then(|settings| settings.with_poll_interval(Duration::from_millis(50)))
        .and_then(|settings| settings.with_assignment_refresh_interval(Duration::from_millis(250)))
        .unwrap()
    }

    async fn delete(&self) {
        let stream = identifier(&self.stream);
        let topic = identifier(&self.topic);

        let _ = self
            .raw
            .delete_consumer_group(&stream, &topic, &identifier(&self.group))
            .await;

        let _ = self.raw.delete_topic(&stream, &topic).await;
        let _ = self.publisher_client.shutdown().await;
        let _ = Client::shutdown(&self.raw).await;
    }
}

pub(super) fn settings() -> ConsumerSettings {
    let mut settings = ConsumerSettings::default();
    settings.max_in_flight = NonZeroUsize::new(4).unwrap();
    settings.source_timeout = Duration::from_secs(5);
    settings.database_timeout = Duration::from_secs(5);
    settings.settlement_timeout = Duration::from_secs(5);
    settings.drain_timeout = Duration::from_secs(5);

    settings
}

/// One group member composed exactly as the application call site composes it, with its own
/// dedicated Iggy client.
pub(super) struct Member {
    client: IggyClient,

    cancel: CancellationToken,

    /// Aborted if a failing test unwinds, releasing its transactions before cleanup.
    task: AbortOnDropHandle<Result<ConsumerExit, ConsumerError>>,
}

impl Member {
    pub(super) async fn spawn(
        member: &'static str,
        broker: &Broker,
        pool: &PgPool,
        scope: &InboxScope,
    ) -> Self {
        let client = iggy_client().await;
        let source = IggyDeliverySource::new(client.clone(), broker.source_settings());
        let inbox = PostgresInboxStore::new(pool.clone(), InboxSettings::default());

        let consumer = Consumer::<OrderCreated, _>::new_partitioned(
            source,
            IggyEnvelopeMapper,
            JsonSerializer,
            inbox,
            scope.clone(),
            EffectHandler {
                scope: scope.clone(),
                member,
            },
            settings(),
        )
        .unwrap();

        let cancel = CancellationToken::new();

        let task =
            AbortOnDropHandle::new(tokio::spawn(consumer.run_partitioned(cancel.child_token())));

        Self {
            client,
            cancel,
            task,
        }
    }

    /// Cancels the member, waits for its drain, and then ends its group membership.
    pub(super) async fn stop(self) -> Result<ConsumerExit, ConsumerError> {
        self.cancel.cancel();

        let exit = tokio::time::timeout(PROGRESS_TIMEOUT, self.task)
            .await
            .expect("consumer did not stop")
            .expect("consumer task panicked");

        let _ = self.client.shutdown().await;

        exit
    }
}

/// Per-test resources handed to a test body.
pub(super) struct Fixture {
    pub(super) pool: PgPool,

    pub(super) broker: Arc<Broker>,

    pub(super) scope: InboxScope,
}

/// Runs `body` with a fresh pool, topic, group, and scope, and removes them even when it panics.
///
/// Asynchronous cleanup cannot run in `Drop`, so the body runs as a task: a panic surfaces as a
/// `JoinError`, cleanup runs, and the original panic then resumes.
pub(super) async fn run_with_cleanup<F, Fut>(partitions: u32, body: F)
where
    F: FnOnce(Fixture) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let pool = pool().await;
    let broker = Arc::new(Broker::new(partitions).await);
    let scope = InboxScope::new(format!("system-{}", MessageId::new())).unwrap();

    let outcome = tokio::spawn(body(Fixture {
        pool: pool.clone(),
        broker: Arc::clone(&broker),
        scope: scope.clone(),
    }))
    .await;

    broker.delete().await;

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
