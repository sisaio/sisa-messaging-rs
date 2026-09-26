//! Typed Kafka partitioned consumer that records each order in PostgreSQL.
//!
//! The application composes the generic partitioned `Consumer` with the Kafka delivery source
//! and mapper and the PostgreSQL inbox, as described in `docs/consumer-framework.md` section 4.
//! It reads `SISA_KAFKA_BOOTSTRAP_SERVERS`, `SISA_KAFKA_INSTANCE_ID`, and `SISA_POSTGRES_URL`
//! and provisions nothing. Before it starts, the operator creates:
//!
//! - the `orders` topic; the source never creates topics;
//! - the repository messaging schema and the application-owned projection table
//!   `order_projection (message_id uuid NOT NULL, order_id text NOT NULL, amount_cents bigint NOT
//!   NULL)`.
//!
//! Each running instance needs a distinct, stable `SISA_KAFKA_INSTANCE_ID`: it is the static
//! group member identity and derives the transactional identity that fences offset commits.
//! Ctrl-C cancels the consumer, which drains in-flight records for at most its drain timeout.

#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt;
use std::process::ExitCode;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sisa_messaging::{Envelope, ErrorClassifier, FailureKind, JsonSerializer, Message};
use sisa_messaging_consumer::{
    Consumer, ConsumerConfigError, ConsumerError, ConsumerErrorKind, ConsumerExit, ConsumerHandler,
    ConsumerSettings,
};
use sisa_messaging_inbox::{InboxScope, InboxScopeError, InboxSettings};
use sisa_messaging_kafka::{
    KafkaClient, KafkaClientSettings, KafkaConsumerSettings, KafkaEnvelopeMapper,
    KafkaShutdownOutcome, KafkaSourceShutdown,
};
use sisa_messaging_postgres::{PostgresInboxStore, PostgresInboxTransaction};
use sqlx::postgres::PgPoolOptions;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

/// Application-provisioned topic; the source verifies it exists and never creates it.
const TOPIC: &str = "orders";

/// Stable consumer group; the library never generates one.
const GROUP: &str = "orders-projection";

/// One connection per in-flight transaction at the default `max_in_flight` of 32, plus one for
/// failure recording after a rollback.
const POOL_CONNECTIONS: u32 = 33;

/// Bound for the Kafka member to close its consumer after the source is dropped.
const SOURCE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound for closing the pool and the Kafka member after a forced abort. The operator asked to
/// stop immediately; aborted workflows return their connections as they unwind, PostgreSQL
/// rolls back any transaction whose connection closes at process exit, and an unadvanced
/// record is replayed from the committed offset, so a short wait loses nothing.
const FORCED_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// Exit status after a forced abort, following the shell convention for an interrupt.
const FORCED_ABORT_EXIT: u8 = 130;

/// Sample message contract carried as JSON.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct OrderCreated {
    order_id: String,

    amount_cents: i64,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "order-created";
    const VERSION: u32 = 1;
}

/// Writes the projection row inside the transaction that also completes the inbox receipt.
struct RecordOrder;

/// A projection write failure whose rendering carries no payload or database text.
#[derive(Debug)]
struct RecordOrderError;

impl fmt::Display for RecordOrderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("order projection write failed")
    }
}

impl Error for RecordOrderError {}

impl ErrorClassifier for RecordOrderError {
    fn classify(&self) -> FailureKind {
        // Treat database failures as retryable; an application classifies its own schema errors.
        FailureKind::Transient
    }
}

impl ConsumerHandler<OrderCreated, PostgresInboxTransaction> for RecordOrder {
    type Error = RecordOrderError;

    async fn handle(
        &self,
        tx: &mut PostgresInboxTransaction,
        envelope: &Envelope<OrderCreated>,
    ) -> Result<(), Self::Error> {
        let order = envelope.payload();

        sqlx::query(
            "INSERT INTO order_projection (message_id, order_id, amount_cents) VALUES ($1, $2, $3)",
        )
        .bind(envelope.message_id().into_uuid())
        .bind(&order.order_id)
        .bind(order.amount_cents)
        .execute(&mut **tx)
        .await
        .map_err(|_| RecordOrderError)?;

        Ok(())
    }
}

/// Startup or supervision failure; rendering names only the failed stage.
#[derive(Debug)]
enum ExampleError {
    MissingEnvironment(&'static str),
    KafkaClient,
    DatabaseConnect,
    InvalidScope(InboxScopeError),
    InvalidSettings(ConsumerConfigError),
    Signal,
    Consumer(ConsumerErrorKind),
    ConsumerTask,
    ForcedAbort,
}

impl fmt::Display for ExampleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment(name) => write!(formatter, "{name} must be set"),
            Self::KafkaClient => formatter.write_str("Kafka client configuration failed"),
            Self::DatabaseConnect => formatter.write_str("database connection failed"),
            Self::InvalidScope(error) => write!(formatter, "inbox scope is invalid: {error}"),
            Self::InvalidSettings(error) => write!(formatter, "consumer settings: {error}"),
            Self::Signal => formatter.write_str("shutdown signal listener failed"),
            Self::Consumer(kind) => write!(formatter, "consumer stopped: {kind:?}"),
            Self::ConsumerTask => formatter.write_str("consumer task failed"),
            Self::ForcedAbort => formatter.write_str(
                "consumer aborted by a second interrupt; unfinished records remain unadvanced",
            ),
        }
    }
}

impl Error for ExampleError {}

impl From<InboxScopeError> for ExampleError {
    fn from(error: InboxScopeError) -> Self {
        Self::InvalidScope(error)
    }
}

impl From<ConsumerConfigError> for ExampleError {
    fn from(error: ConsumerConfigError) -> Self {
        Self::InvalidSettings(error)
    }
}

fn required_env(name: &'static str) -> Result<String, ExampleError> {
    std::env::var(name).map_err(|_| ExampleError::MissingEnvironment(name))
}

/// Prints only the stage-level `Display` rendering on failure; no broker, URL, or provider text.
#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");

            match error {
                ExampleError::ForcedAbort => ExitCode::from(FORCED_ABORT_EXIT),
                _ => ExitCode::FAILURE,
            }
        }
    }
}

async fn run() -> Result<(), ExampleError> {
    let bootstrap_servers = required_env("SISA_KAFKA_BOOTSTRAP_SERVERS")?;
    let instance_id = required_env("SISA_KAFKA_INSTANCE_ID")?;
    let database_url = required_env("SISA_POSTGRES_URL")?;

    // Authentication and TLS belong in advanced properties here; consumer sources inherit them.
    let client = KafkaClient::start(KafkaClientSettings::new([bootstrap_servers]))
        .map_err(|_| ExampleError::KafkaClient)?;

    let source_settings = KafkaConsumerSettings::new(GROUP, instance_id, [TOPIC])
        .and_then(|settings| settings.with_shutdown_timeout(SOURCE_SHUTDOWN_TIMEOUT))
        .map_err(|_| ExampleError::KafkaClient)?;

    let source = client
        .delivery_source(source_settings)
        .map_err(|_| ExampleError::KafkaClient)?;

    let source_shutdown = source.shutdown_handle();

    let pool = PgPoolOptions::new()
        .max_connections(POOL_CONNECTIONS)
        .connect(&database_url)
        .await
        .map_err(|_| ExampleError::DatabaseConnect)?;

    let cancel = CancellationToken::new();
    let inbox = PostgresInboxStore::new(pool.clone(), InboxSettings::default());

    let consumer = Consumer::<OrderCreated, _>::new_partitioned(
        source,
        KafkaEnvelopeMapper,
        JsonSerializer,
        inbox,
        InboxScope::new("orders-projection")?,
        RecordOrder,
        ConsumerSettings::default(),
    )?;

    let task = tokio::spawn(consumer.run_partitioned(cancel.child_token()));

    let result = supervise(task, &cancel).await;

    // The consumer dropped its source when it stopped; the member thread now closes the Kafka
    // consumer within its own bound, or within the forced bound after a second interrupt.
    if matches!(result, Err(ExampleError::ForcedAbort)) {
        let _ = tokio::time::timeout(FORCED_CLOSE_TIMEOUT, pool.close()).await;
        let _ = close_source(source_shutdown, FORCED_CLOSE_TIMEOUT).await;
    } else {
        pool.close().await;

        if !close_source(
            source_shutdown,
            SOURCE_SHUTDOWN_TIMEOUT + Duration::from_secs(1),
        )
        .await
        {
            eprintln!(
                "Kafka consumer close timed out; its group membership expires with the session"
            );
        }
    }

    result
}

/// Waits for the Kafka member thread to finish; returns whether the consumer closed cleanly.
async fn close_source(shutdown: KafkaSourceShutdown, bound: Duration) -> bool {
    matches!(
        tokio::time::timeout(bound, std::future::IntoFuture::into_future(shutdown)).await,
        Ok(KafkaShutdownOutcome::Closed)
    )
}

type ConsumerTask = JoinHandle<Result<ConsumerExit, ConsumerError>>;

/// Waits for Ctrl-C or an early consumer exit, then returns only after the consumer has stopped.
///
/// The first Ctrl-C starts the consumer's drain, which `drain_timeout` bounds. A second Ctrl-C
/// aborts the drain; records still in flight stay unadvanced and replay from the committed offset.
async fn supervise(mut task: ConsumerTask, cancel: &CancellationToken) -> Result<(), ExampleError> {
    let signal = tokio::select! {
        joined = &mut task => return exit(joined),
        signal = tokio::signal::ctrl_c() => signal,
    };

    cancel.cancel();

    let joined = tokio::select! {
        joined = &mut task => joined,
        Ok(()) = tokio::signal::ctrl_c(), if signal.is_ok() => {
            task.abort();

            match task.await {
                Err(error) if error.is_cancelled() => return Err(ExampleError::ForcedAbort),
                // The consumer finished before the abort took effect.
                joined => joined,
            }
        }
    };

    signal.map_err(|_| ExampleError::Signal)?;

    exit(joined)
}

fn exit(
    joined: Result<Result<ConsumerExit, ConsumerError>, JoinError>,
) -> Result<(), ExampleError> {
    match joined {
        Ok(Ok(_exit)) => Ok(()),
        // Only the stable kind is rendered; the provider source may hold sensitive text.
        Ok(Err(error)) => Err(ExampleError::Consumer(error.kind())),
        Err(_) => Err(ExampleError::ConsumerTask),
    }
}
