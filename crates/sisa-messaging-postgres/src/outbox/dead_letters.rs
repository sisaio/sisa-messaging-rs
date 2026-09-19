//! Outbox dead-letter statements.

use chrono::{DateTime, Utc};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::PostgresError;

/// Bound keyset cursor and page size for one dead-letter listing.
pub(super) struct ListParams {
    pub(super) after_dead_at: Option<DateTime<Utc>>,
    pub(super) after_id: Option<Uuid>,
    pub(super) limit: i64,
}

/// Bound terminal row identities for an operator retry.
pub(super) struct RetryParams {
    pub(super) ids: Vec<Uuid>,
}

/// Bound terminal row identities for an operator delete.
pub(super) struct DeleteParams {
    pub(super) ids: Vec<Uuid>,
}

/// Persisted dead-letter row used to build the portable API record.
pub(super) struct DeadLetterListRecord {
    pub(super) id: Uuid,
    pub(super) message_id: Uuid,
    pub(super) message_type: String,
    pub(super) message_version: i32,
    pub(super) content_type: String,
    pub(super) payload: Vec<u8>,
    pub(super) metadata: serde_json::Value,
    pub(super) ordering_key: Option<String>,
    pub(super) attempts: i32,
    pub(super) dead_at: DateTime<Utc>,
    pub(super) dead_reason: String,
    pub(super) last_error: Option<String>,
}

/// One terminal row identity changed by an operator action.
pub(super) struct OutboxIdRecord {
    pub(super) id: Uuid,
}

pub(super) async fn list<'e, E>(
    executor: E,
    params: ListParams,
) -> Result<Vec<DeadLetterListRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        DeadLetterListRecord,
        r#"
            -- Stable keyset order matches the dead-letter cursor index.
            SELECT
                id AS "id!",
                message_id AS "message_id!",
                message_type AS "message_type!",
                message_version AS "message_version!",
                content_type AS "content_type!",
                payload AS "payload!",
                metadata AS "metadata!",
                ordering_key,
                attempts AS "attempts!",
                dead_at AS "dead_at!",
                dead_reason AS "dead_reason!",
                last_error
            FROM outbox_messages
            WHERE dead_at IS NOT NULL
              -- The cursor is exclusive, so adjacent pages cannot repeat their boundary row.
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
    params: RetryParams,
) -> Result<Vec<OutboxIdRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        OutboxIdRecord,
        r#"
            -- Operator retry resets only terminal rows in the caller-bounded identity batch.
            UPDATE outbox_messages
            SET
                claimable_at = now(),
                -- An expired prior deadline must not immediately re-terminalize this new attempt.
                expires_at = NULL,
                attempts = 0,
                dead_at = NULL,
                dead_reason = NULL,
                last_error = NULL,
                claim_token = NULL,
                locked_by = NULL
            -- An operator retry may revive only rows that are still terminal.
            WHERE id = ANY($1)
              AND dead_at IS NOT NULL
            RETURNING
                id AS "id!"
        "#,
        &params.ids,
    )
    .fetch_all(executor)
    .await
    .map_err(PostgresError::from)
}

pub(super) async fn delete<'e, E>(
    executor: E,
    params: DeleteParams,
) -> Result<Vec<OutboxIdRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        OutboxIdRecord,
        r#"
            -- Delete is restricted to the caller-bounded terminal identity batch.
            DELETE FROM outbox_messages
            -- A row revived by another operator cannot be deleted from this stale batch.
            WHERE id = ANY($1)
              AND dead_at IS NOT NULL
            RETURNING
                id AS "id!"
        "#,
        &params.ids,
    )
    .fetch_all(executor)
    .await
    .map_err(PostgresError::from)
}
