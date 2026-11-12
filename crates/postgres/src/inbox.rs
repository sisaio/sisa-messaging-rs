//! The inbox in `inbox_receipts`.

use std::collections::BTreeMap;
use std::fmt;

use sisa_eventing_core::{Envelope, InboxStore, MessageId, StoreError};
use sqlx::types::Json;
use sqlx::{PgPool, PgTransaction};

// The attributes that have a column of their own, so `attributes` doesn't repeat them.
// `specversion` is always `1.0`.
const COLUMN_ATTRIBUTES: [&str; 5] = ["specversion", "id", "type", "source", "correlationid"];

// The CloudEvents attributes without a column: time, subject, content type, causation, trace and
// extensions.
fn attributes(envelope: &Envelope) -> BTreeMap<String, String> {
    let mut attributes = envelope.to_attributes();
    for name in COLUMN_ATTRIBUTES {
        attributes.remove(name);
    }
    attributes
}

// A database error, saying what the inbox was doing.
fn database(context: &'static str) -> impl FnOnce(sqlx::Error) -> StoreError {
    move |error| StoreError::Database {
        context,
        source: Box::new(error),
    }
}

/// The inbox in the `inbox_receipts` table, found through the connection's `search_path`.
///
/// An endpoint with this inbox handles each delivery in a transaction on the pool: it records a
/// receipt for the message, runs the handler, and commits before it acks. A redelivered message
/// finds its receipt and is acked without running the handler again. The handler does its own
/// writes in the same transaction, through `HandlerContext::transaction` from
/// `sisa-eventing-runtime`:
///
/// ```no_run
/// # use serde::{Deserialize, Serialize};
/// # use sisa_eventing_core::Message;
/// # use sisa_eventing_postgres::{PostgresInbox, PostgresOutbox};
/// # use sisa_eventing_runtime::{Bus, HandlerContext, HandlerError, InMemoryTransport};
/// # use sqlx::PgTransaction;
/// # #[derive(Serialize, Deserialize)]
/// # struct OrderPlaced { id: i64 }
/// # impl Message for OrderPlaced { const TYPE: &'static str = "orders.order_placed.v1"; }
/// async fn charge_order(
///     order: OrderPlaced,
///     context: &mut HandlerContext<PgTransaction<'static>>,
/// ) -> Result<(), HandlerError> {
///     sqlx::query("INSERT INTO charges (order_id) VALUES ($1)")
///         .bind(order.id)
///         .execute(context.transaction())
///         .await?;
///     Ok(())
/// }
///
/// # async fn run(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// let bus = Bus::builder("/billing", InMemoryTransport::new())
///     .outbox(PostgresOutbox::new(pool.clone()))
///     .endpoint("billing", |endpoint| {
///         endpoint
///             .inbox(PostgresInbox::new(pool.clone()))
///             .handle::<OrderPlaced>(charge_order)
///     })
///     .start()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// A receipt is unique per endpoint, message id and message time, and its partition comes from
/// the message's time, so a redelivery days later still finds it while its partition is kept.
#[derive(Clone)]
pub struct PostgresInbox {
    pool: PgPool,
    keep_data: bool,
}

impl PostgresInbox {
    /// An inbox on the database of `pool`, whose `search_path` names the schema the tables are
    /// in. Receipts keep the message's attributes, not its data.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            keep_data: false,
        }
    }

    /// Keeps each message's data in its receipt too, when `keep_data` is true.
    #[must_use]
    pub fn keep_data(mut self, keep_data: bool) -> Self {
        self.keep_data = keep_data;
        self
    }
}

// Records receipts in a transaction on the pool, which the handler then works in.
impl InboxStore for PostgresInbox {
    type Transaction = PgTransaction<'static>;

    async fn begin(&self) -> Result<PgTransaction<'static>, StoreError> {
        self.pool
            .begin()
            .await
            .map_err(database("beginning an inbox transaction"))
    }

    async fn record(
        &self,
        transaction: &mut PgTransaction<'static>,
        endpoint: &str,
        envelope: &Envelope,
    ) -> Result<bool, StoreError> {
        let message_time = envelope.time.to_string();
        let data = self.keep_data.then_some(&envelope.data[..]);
        let inserted = sqlx::query(
            "INSERT INTO inbox_receipts
                 (id, message_time, endpoint, message_id, type, source, correlation_id,
                  attributes, data)
             VALUES ($1, $2::timestamptz, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT DO NOTHING",
        )
        // The receipt's own id: a UUID version 7, like a message id.
        .bind(MessageId::new().as_uuid())
        .bind(message_time)
        .bind(endpoint)
        .bind(envelope.id.as_uuid())
        .bind(envelope.message_type.as_str())
        .bind(envelope.source.as_str())
        .bind(
            envelope
                .correlation
                .correlation_id
                .as_ref()
                .map(MessageId::as_uuid),
        )
        .bind(Json(attributes(envelope)))
        .bind(data)
        .execute(&mut **transaction)
        .await
        .map_err(database("inserting an inbox receipt"))?;
        Ok(inserted.rows_affected() == 1)
    }

    async fn commit(&self, mut transaction: PgTransaction<'static>) -> Result<(), StoreError> {
        // PostgreSQL answers COMMIT on an aborted transaction with a rollback and no error, which
        // happens when a handler swallows a failed statement. A statement first fails there
        // instead, so the delivery isn't acked for work that was never written.
        sqlx::query("SELECT 1")
            .execute(&mut *transaction)
            .await
            .map_err(database("checking the inbox transaction before its commit"))?;
        transaction
            .commit()
            .await
            .map_err(database("committing an inbox transaction"))
    }
}

// Shows the table and whether data is kept, never a connection.
impl fmt::Debug for PostgresInbox {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresInbox")
            .field("table", &"inbox_receipts")
            .field("keep_data", &self.keep_data)
            .finish_non_exhaustive()
    }
}
