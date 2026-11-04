//! The outbox in `outbox_messages`.

use std::collections::BTreeMap;
use std::fmt;

use sisa_messaging_core::{Envelope, OutboxStore, StoreError};
use sqlx::types::Json;
use sqlx::{PgConnection, PgPool};

// The attributes that have a column of their own, so `attributes` doesn't repeat them.
// `specversion` is always `1.0`.
const COLUMN_ATTRIBUTES: [&str; 8] = [
    "specversion",
    "id",
    "type",
    "source",
    "subject",
    "partitionkey",
    "datacontenttype",
    "dataschema",
];

// The CloudEvents attributes without a column: time, correlation, trace and extensions.
fn attributes(envelope: &Envelope) -> BTreeMap<String, String> {
    let mut attributes = envelope.to_attributes();
    for name in COLUMN_ATTRIBUTES {
        attributes.remove(name);
    }
    attributes
}

/// The outbox in the `outbox_messages` table, found through the connection's `search_path`.
///
/// Rows are written on the application's own connection, inside its transaction, with
/// `Bus::outbox` from `sisa-messaging-runtime`:
///
/// ```no_run
/// # use serde::{Deserialize, Serialize};
/// # use sisa_messaging_core::{Message, Publisher};
/// # use sisa_messaging_postgres::PostgresOutbox;
/// # use sisa_messaging_runtime::{Bus, InMemoryTransport};
/// # #[derive(Serialize, Deserialize)]
/// # struct OrderPlaced { id: i64 }
/// # impl Message for OrderPlaced { const TYPE: &'static str = "orders.order_placed.v1"; }
/// # async fn run(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// let bus = Bus::builder("/orders", InMemoryTransport::new())
///     .outbox(PostgresOutbox::new(pool.clone()))
///     .start()
///     .await?;
///
/// let mut tx = pool.begin().await?;
/// sqlx::query("INSERT INTO orders (id) VALUES ($1)").bind(42).execute(&mut *tx).await?;
/// bus.outbox(&mut tx).publish(&OrderPlaced { id: 42 }).await?;
/// tx.commit().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct PostgresOutbox {
    // The dispatcher's own connections, for claiming rows after the commit. Rows are never
    // written on them.
    #[expect(dead_code, reason = "the dispatcher claims rows on it")]
    pool: PgPool,
}

impl PostgresOutbox {
    /// An outbox on the database of `pool`, whose `search_path` names the schema the tables are
    /// in.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

// Writes the rows on the caller's connection, so they commit with its transaction.
impl OutboxStore for PostgresOutbox {
    type Connection = PgConnection;

    async fn insert(
        &self,
        connection: &mut PgConnection,
        envelopes: &[Envelope],
    ) -> Result<(), StoreError> {
        for envelope in envelopes {
            sqlx::query(
                "INSERT INTO outbox_messages
                     (id, type, source, subject, partition_key, data_content_type, data_schema,
                      attributes, data)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            )
            .bind(envelope.id.as_uuid())
            .bind(envelope.message_type.as_str())
            .bind(envelope.source.as_str())
            .bind(envelope.subject.as_deref())
            .bind(envelope.partition_key.as_deref())
            .bind(envelope.content_type.as_str())
            .bind(envelope.data_schema.as_deref())
            .bind(Json(attributes(envelope)))
            .bind(&envelope.data[..])
            .execute(&mut *connection)
            .await
            .map_err(|error| StoreError::Database {
                context: "inserting an outbox row",
                source: Box::new(error),
            })?;
        }
        Ok(())
    }
}

// Shows the table, never a connection.
impl fmt::Debug for PostgresOutbox {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresOutbox")
            .field("table", &"outbox_messages")
            .finish_non_exhaustive()
    }
}
