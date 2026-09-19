//! Inbox dead-letter statements.

use chrono::{DateTime, Utc};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::PostgresError;

pub(super) struct ListParams {
    pub(super) after_dead_at: Option<DateTime<Utc>>,
    pub(super) after_id: Option<Uuid>,
    pub(super) limit: i64,
}

pub(super) struct RetryParams<'a> {
    pub(super) ids: &'a [Uuid],
}

pub(super) struct DeleteParams<'a> {
    pub(super) ids: &'a [Uuid],
}

pub(super) struct DeadLetterRecord {
    pub(super) id: Uuid,
    pub(super) scope: String,
    pub(super) message_id: Uuid,
    pub(super) message_type: String,
    pub(super) message_version: i32,
    pub(super) metadata: serde_json::Value,
    pub(super) attempts: i32,
    pub(super) received_at: DateTime<Utc>,
    pub(super) dead_at: DateTime<Utc>,
    pub(super) dead_reason: String,
    pub(super) last_error: Option<String>,
}

pub(super) struct InboxIdRecord {
    pub(super) id: Uuid,
}

pub(super) async fn list<'e, E>(
    executor: E,
    params: ListParams,
) -> Result<Vec<DeadLetterRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        DeadLetterRecord,
        r#"
            -- The stable keyset order matches the terminal receipt access path.
            SELECT
                id AS "id!",
                scope AS "scope!",
                message_id AS "message_id!",
                message_type AS "message_type!",
                message_version AS "message_version!",
                metadata AS "metadata!",
                attempts AS "attempts!",
                received_at AS "received_at!",
                dead_at AS "dead_at!",
                dead_reason AS "dead_reason!",
                last_error
            FROM inbox_receipts
            WHERE dead_at IS NOT NULL
              -- The exclusive cursor cannot return its preceding page boundary twice.
              AND (
                  $1::timestamptz IS NULL
                  OR (dead_at, id) > ($1, $2)
              )
            ORDER BY dead_at, id
            LIMIT $3
        "#,
        params.after_dead_at,
        params.after_id,
        params.limit,
    )
    .fetch_all(executor)
    .await
    .map_err(PostgresError::from)
}

pub(super) async fn retry<'e, E>(
    executor: E,
    params: RetryParams<'_>,
) -> Result<Vec<InboxIdRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        InboxIdRecord,
        r#"
            -- Retrying preserves identity while resetting only still-terminal receipts.
            UPDATE inbox_receipts
            SET
                attempts = 0,
                dead_at = NULL,
                dead_reason = NULL,
                last_error = NULL
            WHERE id = ANY($1)
              AND dead_at IS NOT NULL
            RETURNING
                id AS "id!"
        "#,
        params.ids,
    )
    .fetch_all(executor)
    .await
    .map_err(PostgresError::from)
}

pub(super) async fn delete<'e, E>(
    executor: E,
    params: DeleteParams<'_>,
) -> Result<Vec<InboxIdRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        InboxIdRecord,
        r#"
            -- Delete is restricted to the caller-bounded terminal identity batch.
            DELETE FROM inbox_receipts
            -- A concurrently retried receipt cannot be removed from a stale batch.
            WHERE id = ANY($1)
              AND dead_at IS NOT NULL
            RETURNING
                id AS "id!"
        "#,
        params.ids,
    )
    .fetch_all(executor)
    .await
    .map_err(PostgresError::from)
}
