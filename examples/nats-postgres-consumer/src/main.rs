//! Typed NATS JetStream consumer that records each order in PostgreSQL.
//!
//! The application composes the generic `Consumer` with the NATS delivery source and mapper and
//! the PostgreSQL inbox, as described in `docs/consumer-framework.md` section 4. It reads
//! `NATS_URL` and `DATABASE_URL` and provisions nothing. Before it starts, the operator creates:
//!
//! - the `ORDERS` stream capturing `orders.>` and its durable pull consumer `orders-projection`
//!   with explicit acknowledgement and an unlimited `max_deliver` or one of at least the inbox
//!   `max_attempts`;
//! - the repository messaging schema and the application-owned projection table
//!   `order_projection (message_id uuid NOT NULL, order_id text NOT NULL, amount_cents bigint NOT
//!   NULL)`.
//!
//! Ctrl-C cancels the consumer, which drains in-flight deliveries for at most its drain timeout.

#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt;
use std::process::ExitCode;

use async_nats::jetstream::{self, consumer::PullConsumer};
use serde::{Deserialize, Serialize};
use sisa_messaging::{Envelope, ErrorClassifier, FailureKind, JsonSerializer, Message};
use sisa_messaging_consumer::{
    Consumer, ConsumerConfigError, ConsumerError, ConsumerErrorKind, ConsumerExit, ConsumerHandler,
    ConsumerSettings,
};
use sisa_messaging_inbox::{InboxScope, InboxScopeError, InboxSettings};
use sisa_messaging_nats::{NatsDeliverySource, NatsMapper, Subject, TypeSubjectResolver};
use sisa_messaging_postgres::{PostgresInboxStore, PostgresInboxTransaction};
use sqlx::postgres::PgPoolOptions;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

/// Application-provisioned stream looked up at startup.
const STREAM: &str = "ORDERS";

/// Stable durable pull consumer name; the library never generates one.
const DURABLE: &str = "orders-projection";

/// Subject prefix the mapper would use to encode; decoding reads the delivered subject.
const SUBJECT_PREFIX: &str = "orders";

/// One connection per in-flight transaction at the default `max_in_flight` of 32, plus one for
/// failure recording after a rollback.
const POOL_CONNECTIONS: u32 = 33;

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
    NatsConnect,
    ConsumerLookup,
    DatabaseConnect,
    InvalidSubject,
    InvalidScope(InboxScopeError),
    InvalidSettings(ConsumerConfigError),
    Signal,
    Consumer(ConsumerErrorKind),
    ConsumerTask,
}

impl fmt::Display for ExampleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment(name) => write!(formatter, "{name} must be set"),
            Self::NatsConnect => formatter.write_str("NATS connection failed"),
            Self::ConsumerLookup => formatter.write_str("durable consumer lookup failed"),
            Self::DatabaseConnect => formatter.write_str("database connection failed"),
            Self::InvalidSubject => formatter.write_str("subject prefix is invalid"),
            Self::InvalidScope(error) => write!(formatter, "inbox scope is invalid: {error}"),
            Self::InvalidSettings(error) => write!(formatter, "consumer settings: {error}"),
            Self::Signal => formatter.write_str("shutdown signal listener failed"),
            Self::Consumer(kind) => write!(formatter, "consumer stopped: {kind:?}"),
            Self::ConsumerTask => formatter.write_str("consumer task failed"),
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

/// Prints only the stage-level `Display` rendering on failure; no URL or provider text.
#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");

            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), ExampleError> {
    let nats_url = required_env("NATS_URL")?;
    let database_url = required_env("DATABASE_URL")?;

    let client = async_nats::connect(nats_url)
        .await
        .map_err(|_| ExampleError::NatsConnect)?;

    // Look up, never create: the stream and durable consumer are application-provisioned.
    let pull_consumer: PullConsumer = jetstream::new(client)
        .get_consumer_from_stream(DURABLE, STREAM)
        .await
        .map_err(|_| ExampleError::ConsumerLookup)?;

    let pool = PgPoolOptions::new()
        .max_connections(POOL_CONNECTIONS)
        .connect(&database_url)
        .await
        .map_err(|_| ExampleError::DatabaseConnect)?;

    let subject_resolver = TypeSubjectResolver::new(
        Subject::new(SUBJECT_PREFIX).map_err(|_| ExampleError::InvalidSubject)?,
    );

    let cancel = CancellationToken::new();

    let inbox = PostgresInboxStore::new(pool.clone(), InboxSettings::default());
    let source = NatsDeliverySource::new(pull_consumer);
    let mapper = NatsMapper::new(subject_resolver);

    let consumer = Consumer::<OrderCreated, _>::new(
        source,
        mapper,
        JsonSerializer,
        inbox,
        InboxScope::new("orders-projection")?,
        RecordOrder,
        ConsumerSettings::default(),
    )?;

    let task = tokio::spawn(consumer.run(cancel.child_token()));

    let result = supervise(task, &cancel).await;
    pool.close().await;

    result
}

type ConsumerTask = JoinHandle<Result<ConsumerExit, ConsumerError>>;

/// Waits for Ctrl-C or an early consumer exit, then returns only after the consumer has drained.
async fn supervise(mut task: ConsumerTask, cancel: &CancellationToken) -> Result<(), ExampleError> {
    let signal = tokio::select! {
        joined = &mut task => return exit(joined),
        signal = tokio::signal::ctrl_c() => signal,
    };

    cancel.cancel();
    let joined = task.await;
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
