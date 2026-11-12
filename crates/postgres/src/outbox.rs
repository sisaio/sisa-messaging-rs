//! The outbox in `outbox_messages`.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use sisa_eventing_core::{Envelope, OutboxRecord, OutboxStore, StoreError, Timestamp};
use sqlx::types::{Json, JsonValue, Uuid};
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

// The columns of a claimed row, as `claim` selects them.
type ClaimedRow = (
    Uuid,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Json<BTreeMap<String, JsonValue>>,
    Vec<u8>,
    i32,
    String,
);

fn timestamp(column: &str, text: &str) -> Result<Timestamp, String> {
    text.parse().map_err(|error| format!("{column}: {error}"))
}

// Puts a claimed row back together as a record: the envelope from its columns and its other
// attributes, which are all strings unless something other than this crate wrote the row.
fn record(row: ClaimedRow) -> Result<OutboxRecord, String> {
    let (id, created_at, message_type, source, subject, partition_key, content_type, schema) =
        (row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7);
    let mut attributes = BTreeMap::new();
    for (name, value) in row.8.0 {
        match value {
            JsonValue::String(value) => attributes.insert(name, value),
            other => return Err(format!("attribute `{name}` is {other}, not a string")),
        };
    }
    let columns = [
        ("specversion", Some("1.0".to_owned())),
        ("id", Some(id.to_string())),
        ("type", Some(message_type)),
        ("source", Some(source)),
        ("datacontenttype", Some(content_type)),
        ("subject", subject),
        ("partitionkey", partition_key),
        ("dataschema", schema),
    ];
    for (name, value) in columns {
        if let Some(value) = value {
            attributes.insert(name.to_owned(), value);
        }
    }
    let envelope =
        Envelope::from_attributes(attributes, row.9.into()).map_err(|error| error.to_string())?;
    // A count of failed attempts is never negative.
    let attempts = u32::try_from(row.10).unwrap_or_default();
    Ok(OutboxRecord::new(
        envelope,
        timestamp("created_at", &created_at)?,
        attempts,
        timestamp("available_at", &row.11)?,
    ))
}

fn database(context: &'static str) -> impl FnOnce(sqlx::Error) -> StoreError {
    move |error| StoreError::Database {
        context,
        source: Box::new(error),
    }
}

/// The outbox in the `outbox_messages` table, found through the connection's `search_path`.
///
/// Rows are written on the application's own connection, inside its transaction, with
/// `Bus::outbox` from `sisa-eventing-runtime`:
///
/// ```no_run
/// # use serde::{Deserialize, Serialize};
/// # use sisa_eventing_core::{Message, Publisher};
/// # use sisa_eventing_postgres::PostgresOutbox;
/// # use sisa_eventing_runtime::{Bus, InMemoryTransport};
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
    // The dispatcher's own connections, for claiming and marking rows after the commit. Rows
    // are never written on them.
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

// Writes the rows on the caller's connection, so they commit with its transaction, and claims
// and marks them on its own pool, one committed statement each. A mark matches the due time its
// claim set, so a dispatcher whose lease ran out can't overwrite a newer claim.
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
            .map_err(database("inserting an outbox row"))?;
        }
        Ok(())
    }

    async fn claim(&self, limit: usize, lease: Duration) -> Result<Vec<OutboxRecord>, StoreError> {
        // `RETURNING` has no order, so the outer query sorts. `to_json` writes the times in
        // RFC 3339, whatever the session's date style, so they read back as a `Timestamp`.
        let rows: Vec<ClaimedRow> = sqlx::query_as(
            "WITH claimed AS (
                 UPDATE outbox_messages
                 SET available_at = now() + $2
                 WHERE (created_at, id) IN (
                     SELECT created_at, id FROM outbox_messages
                     WHERE status = 'pending' AND available_at <= now()
                     ORDER BY id
                     LIMIT $1
                     FOR UPDATE SKIP LOCKED
                 )
                 RETURNING id, created_at, type, source, subject, partition_key,
                           data_content_type, data_schema, attributes, data, attempts,
                           available_at
             )
             SELECT id, to_json(created_at) #>> '{}', type, source, subject, partition_key,
                    data_content_type, data_schema, attributes, data, attempts,
                    to_json(available_at) #>> '{}'
             FROM claimed ORDER BY id",
        )
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .bind(lease)
        .fetch_all(&self.pool)
        .await
        .map_err(database("claiming outbox rows"))?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let (id, written_at, leased_until) = (row.0, row.1.clone(), row.11.clone());
            match record(row) {
                Ok(record) => records.push(record),
                // No retry makes the row readable, so it waits for a person, and the other rows
                // still go out.
                Err(error) => {
                    sqlx::query(
                        "UPDATE outbox_messages
                         SET status = 'failed', attempts = attempts + 1, last_error = $3
                         WHERE created_at = $1::timestamptz AND id = $2 AND status = 'pending'
                           AND available_at = $4::timestamptz",
                    )
                    .bind(written_at)
                    .bind(id)
                    .bind(format!("not a valid envelope: {error}"))
                    .bind(leased_until)
                    .execute(&self.pool)
                    .await
                    .map_err(database("marking an unreadable outbox row failed"))?;
                }
            }
        }
        Ok(records)
    }

    async fn mark_dispatched(&self, records: &[OutboxRecord]) -> Result<(), StoreError> {
        let created_at: Vec<String> = records
            .iter()
            .map(|record| record.created_at.to_string())
            .collect();
        let ids: Vec<Uuid> = records
            .iter()
            .map(|record| *record.envelope.id.as_uuid())
            .collect();
        let leased_until: Vec<String> = records
            .iter()
            .map(|record| record.leased_until.to_string())
            .collect();
        sqlx::query(
            "UPDATE outbox_messages SET status = 'dispatched', dispatched_at = now()
             WHERE (created_at, id, available_at) IN (
                 SELECT * FROM unnest($1::text[]::timestamptz[], $2::uuid[],
                                      $3::text[]::timestamptz[]))
               AND status = 'pending'",
        )
        .bind(created_at)
        .bind(ids)
        .bind(leased_until)
        .execute(&self.pool)
        .await
        .map_err(database("marking outbox rows dispatched"))?;
        Ok(())
    }

    async fn mark_retry(
        &self,
        record: &OutboxRecord,
        error: &str,
        delay: Duration,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE outbox_messages
             SET attempts = attempts + 1, last_error = $3, available_at = now() + $4
             WHERE created_at = $1::timestamptz AND id = $2 AND status = 'pending'
               AND available_at = $5::timestamptz",
        )
        .bind(record.created_at.to_string())
        .bind(record.envelope.id.as_uuid())
        .bind(error)
        .bind(delay)
        .bind(record.leased_until.to_string())
        .execute(&self.pool)
        .await
        .map_err(database("putting an outbox row back"))?;
        Ok(())
    }

    async fn mark_failed(&self, record: &OutboxRecord, error: &str) -> Result<(), StoreError> {
        // Due at once, so setting the row back to pending sends it on the next claim.
        sqlx::query(
            "UPDATE outbox_messages
             SET status = 'failed', attempts = attempts + 1, last_error = $3, available_at = now()
             WHERE created_at = $1::timestamptz AND id = $2 AND status = 'pending'
               AND available_at = $4::timestamptz",
        )
        .bind(record.created_at.to_string())
        .bind(record.envelope.id.as_uuid())
        .bind(error)
        .bind(record.leased_until.to_string())
        .execute(&self.pool)
        .await
        .map_err(database("marking an outbox row failed"))?;
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
