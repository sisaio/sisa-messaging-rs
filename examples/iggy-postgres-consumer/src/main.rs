//! Typed Apache Iggy consumer-group consumer that records each order in PostgreSQL.
//!
//! The application composes the generic partitioned `Consumer` with the Iggy delivery source and
//! mapper and the PostgreSQL inbox, as described in `docs/consumer-framework.md` section 4. It
//! reads `SISA_IGGY_SERVER_ADDRESS`, `SISA_IGGY_USERNAME`, `SISA_IGGY_PASSWORD`, and
//! `SISA_POSTGRES_URL`, and provisions nothing. Before it starts, the operator creates:
//!
//! - the `orders` stream, its `order-created` topic, and the topic's `orders-projection`
//!   consumer group;
//! - the repository messaging schema and the application-owned projection table
//!   `order_projection (message_id uuid NOT NULL, order_id text NOT NULL, amount_cents bigint NOT
//!   NULL)`.
//!
//! Every process running this consumer group must use the same PostgreSQL inbox: the Iggy source
//! is replay-only, so a rebalance or an indeterminate offset store can redeliver a record that
//! the shared inbox then recognizes as already completed.
//!
//! Ctrl-C cancels the consumer, which drains in-flight records for at most its drain timeout.
//! Shutting the Iggy client down afterwards ends this member's group membership.

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
use sisa_messaging_iggy::{
    Identifier, IggyClient, IggyClientSettings, IggyCredentials, IggyDeliverySource,
    IggyEnvelopeMapper, IggySourceSettings,
};
use sisa_messaging_inbox::{InboxScope, InboxScopeError, InboxSettings};
use sisa_messaging_postgres::{PostgresInboxStore, PostgresInboxTransaction};
use sqlx::postgres::PgPoolOptions;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

/// Application-provisioned stream, topic, and consumer group looked up at startup.
const STREAM: &str = "orders";
const TOPIC: &str = "order-created";
const GROUP: &str = "orders-projection";

/// One connection per in-flight transaction at the default `max_in_flight` of 32, plus one for
/// failure recording after a rollback.
const POOL_CONNECTIONS: u32 = 33;

/// Bound for closing the pool after a forced abort. The operator asked to stop immediately;
/// aborted workflows return their connections as they unwind, and PostgreSQL rolls back any
/// transaction whose connection closes at process exit, so a short wait loses nothing.
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
    IggyConnect,
    InvalidIdentifier,
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
            Self::IggyConnect => formatter.write_str("Iggy connection failed"),
            Self::InvalidIdentifier => formatter.write_str("Iggy resource name is invalid"),
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

fn identifier(name: &str) -> Result<Identifier, ExampleError> {
    Identifier::from_str_value(name).map_err(|_| ExampleError::InvalidIdentifier)
}

/// Prints only the stage-level `Display` rendering on failure; no address, credential, or
/// provider text.
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
    let server_address = required_env("SISA_IGGY_SERVER_ADDRESS")?;
    let username = required_env("SISA_IGGY_USERNAME")?;
    let password = required_env("SISA_IGGY_PASSWORD")?;
    let database_url = required_env("SISA_POSTGRES_URL")?;

    // The source's group membership belongs to this dedicated client session.
    let client = IggyClient::start(IggyClientSettings::new(
        server_address,
        IggyCredentials::UsernamePassword { username, password },
    ))
    .await
    .map_err(|_| ExampleError::IggyConnect)?;

    let pool = PgPoolOptions::new()
        .max_connections(POOL_CONNECTIONS)
        .connect(&database_url)
        .await
        .map_err(|_| ExampleError::DatabaseConnect)?;

    // Look up and join, never create: the stream, topic, and group are application-provisioned.
    let source = IggyDeliverySource::new(
        client.clone(),
        IggySourceSettings::new(identifier(STREAM)?, identifier(TOPIC)?, identifier(GROUP)?),
    );

    let cancel = CancellationToken::new();
    let inbox = PostgresInboxStore::new(pool.clone(), InboxSettings::default());

    let consumer = Consumer::<OrderCreated, _>::new_partitioned(
        source,
        IggyEnvelopeMapper,
        JsonSerializer,
        inbox,
        InboxScope::new("orders-projection")?,
        RecordOrder,
        ConsumerSettings::default(),
    )?;

    let task = tokio::spawn(consumer.run_partitioned(cancel.child_token()));

    let result = supervise(task, &cancel).await;

    // Ends this member's group membership; the server hands its partitions to the others.
    let _ = client.shutdown().await;

    if matches!(result, Err(ExampleError::ForcedAbort)) {
        let _ = tokio::time::timeout(FORCED_CLOSE_TIMEOUT, pool.close()).await;
    } else {
        pool.close().await;
    }

    result
}

type ConsumerTask = JoinHandle<Result<ConsumerExit, ConsumerError>>;

/// Waits for Ctrl-C or an early consumer exit, then returns only after the consumer has stopped.
///
/// The first Ctrl-C starts the consumer's drain, which `drain_timeout` bounds. A second Ctrl-C
/// aborts the drain; records still in flight stay unadvanced and are replayed by the group.
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
