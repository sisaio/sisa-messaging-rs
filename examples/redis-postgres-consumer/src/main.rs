//! Application-owned Redis and PostgreSQL construction for one typed consumer.

use std::{error::Error, fmt, num::NonZeroUsize};

use serde::{Deserialize, Serialize};
use sisa_messaging::{Envelope, ErrorClassifier, FailureKind, JsonSerializer, Message};
use sisa_messaging_consumer::{
    Consumer, ConsumerError, ConsumerHandler, ConsumerSettings, SettlementMode,
};
use sisa_messaging_inbox::{InboxScope, InboxSettings};
use sisa_messaging_postgres::{PostgresError, PostgresInboxStore, PostgresInboxTransaction};
use sisa_messaging_redis::{RedisDeliverySource, RedisMapper, SourceSettings};
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, Serialize)]
struct OrderCreated {
    order_id: String,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u32 = 1;
}

/// This error deliberately has no source: inbox failure summaries persist its error chain.
#[derive(Debug)]
struct HandlerError(FailureKind);

impl fmt::Display for HandlerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("order receipt write failed")
    }
}

impl Error for HandlerError {}

impl ErrorClassifier for HandlerError {
    fn classify(&self) -> FailureKind {
        self.0
    }
}

struct RecordOrder;

impl ConsumerHandler<OrderCreated, PostgresInboxTransaction> for RecordOrder {
    type Error = HandlerError;

    async fn handle(
        &self,
        tx: &mut PostgresInboxTransaction,
        envelope: &Envelope<OrderCreated>,
    ) -> Result<(), HandlerError> {
        sqlx::query(
            "INSERT INTO example_order_receipts (order_id) VALUES ($1) ON CONFLICT (order_id) DO NOTHING",
        )
        .bind(&envelope.payload().order_id)
        .execute(&mut **tx)
        .await
        .map_err(|error| HandlerError(PostgresError::from(error).classify()))?;

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(()) => {}
        Err(RunError::Stage(stage)) => {
            eprintln!("consumer failed at {stage}");
            std::process::exit(1);
        }
        Err(RunError::Consumer(error)) => {
            eprintln!("consumer stopped: {error} (kind: {:?})", error.kind());
            std::process::exit(1);
        }
    }
}

enum RunError {
    Stage(&'static str),
    Consumer(ConsumerError),
}

async fn run() -> Result<(), RunError> {
    let postgres_url =
        std::env::var("SISA_POSTGRES_URL").map_err(|_| RunError::Stage("SISA_POSTGRES_URL"))?;

    let redis_url =
        std::env::var("SISA_REDIS_URL").map_err(|_| RunError::Stage("SISA_REDIS_URL"))?;

    let stream =
        std::env::var("SISA_REDIS_STREAM").map_err(|_| RunError::Stage("SISA_REDIS_STREAM"))?;

    let group =
        std::env::var("SISA_REDIS_GROUP").map_err(|_| RunError::Stage("SISA_REDIS_GROUP"))?;

    let consumer_name =
        std::env::var("SISA_REDIS_CONSUMER").map_err(|_| RunError::Stage("SISA_REDIS_CONSUMER"))?;

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&postgres_url)
        .await
        .map_err(|_| RunError::Stage("PostgreSQL connection"))?;

    let inbox = PostgresInboxStore::new(pool, InboxSettings::default());

    let client = redis::Client::open(redis_url).map_err(|_| RunError::Stage("Redis client"))?;

    let read_connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|_| RunError::Stage("Redis read connection"))?;

    let command_connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|_| RunError::Stage("Redis command connection"))?;

    let source = RedisDeliverySource::new(
        read_connection,
        command_connection,
        stream,
        group,
        consumer_name,
        SourceSettings::default(),
    )
    .map_err(|_| RunError::Stage("Redis source configuration"))?;

    let mut settings = ConsumerSettings::default();
    settings.mode = SettlementMode::PendingRecovery;
    settings.max_in_flight = NonZeroUsize::new(4).ok_or(RunError::Stage("consumer concurrency"))?;

    let consumer = Consumer::<OrderCreated, _>::new(
        source,
        RedisMapper,
        JsonSerializer,
        inbox,
        InboxScope::new("orders-projection")
            .map_err(|_| RunError::Stage("inbox scope configuration"))?,
        RecordOrder,
        settings,
    )
    .map_err(|_| RunError::Stage("consumer configuration"))?;

    let cancel = CancellationToken::new();
    let run = consumer.run(cancel.clone());
    tokio::pin!(run);

    tokio::select! {
        result = &mut run => { result.map_err(RunError::Consumer)?; }
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|_| RunError::Stage("shutdown signal"))?;
            cancel.cancel();
            run.await.map_err(RunError::Consumer)?;
        }
    }

    Ok(())
}
