//! Inbox completion and failure statements.

use chrono::{DateTime, Utc};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::PostgresError;

use super::claim::LockParams;

pub(super) struct CompleteParams {
    pub(super) id: Uuid,
}

pub(super) struct FailParams<'a> {
    pub(super) scope: &'a str,
    pub(super) message_id: Uuid,
    pub(super) message_type: &'a str,
    pub(super) message_version: i32,
    pub(super) metadata: &'a serde_json::Value,
    pub(super) permanent: bool,
    pub(super) max_attempts: i32,
    pub(super) error: &'a str,
}

pub(super) struct FailRecord {
    pub(super) attempts: i32,
    pub(super) completed_at: Option<DateTime<Utc>>,
    pub(super) dead_at: Option<DateTime<Utc>>,
    pub(super) dead_reason: Option<String>,
}

pub(super) async fn complete<'e, E>(
    executor: E,
    params: CompleteParams,
) -> Result<u64, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        r#"
            -- Completion is valid only while the receipt remains active in this business transaction.
            UPDATE inbox_receipts
            SET completed_at = now()
            WHERE id = $1
              AND completed_at IS NULL
              AND dead_at IS NULL
        "#,
        params.id,
    )
    .execute(executor)
    .await
    .map(|result| result.rows_affected())
    .map_err(PostgresError::from)
}

pub(super) async fn blocking_lock<'e, E>(
    executor: E,
    params: LockParams<'_>,
) -> Result<(), PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        r#"
            -- Failure serialization uses the same transaction-scoped identity lock as claiming.
            SELECT pg_advisory_xact_lock(
                hashtextextended($1 || chr(31) || ($2::uuid)::text, 0)
            )
        "#,
        params.scope,
        params.message_id,
    )
    .execute(executor)
    .await
    .map(|_| ())
    .map_err(PostgresError::from)
}

pub(super) async fn fail<'e, E>(
    executor: E,
    params: FailParams<'_>,
) -> Result<FailRecord, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        FailRecord,
        r#"
            -- The advisory lock serializes provider writers for this durable receipt identity.
            WITH existing AS (
                SELECT
                    id,
                    attempts,
                    completed_at,
                    dead_at,
                    dead_reason
                FROM inbox_receipts
                WHERE scope = $1
                  AND message_id = $2
                FOR UPDATE
            ),
            next_state AS (
                SELECT
                    id,
                    completed_at,
                    dead_at,
                    dead_reason,
                    -- Saturate before adding so schema-valid i32::MAX cannot overflow.
                    CASE
                        WHEN attempts < 2147483647 THEN attempts + 1
                        ELSE attempts
                    END AS next_attempt
                FROM existing
            ),
            updated AS (
                -- Completed and terminal receipts are immutable; active receipts advance once.
                UPDATE inbox_receipts AS receipt
                SET
                    attempts = state.next_attempt,
                    dead_at = CASE
                        WHEN $6 OR state.next_attempt >= $7 THEN now()
                    END,
                    dead_reason = CASE
                        WHEN $6 THEN 'permanent'
                        WHEN state.next_attempt >= $7 THEN 'exhausted'
                    END,
                    last_error = $8
                FROM next_state AS state
                WHERE receipt.id = state.id
                  AND state.completed_at IS NULL
                  AND state.dead_at IS NULL
                RETURNING
                    receipt.attempts,
                    receipt.completed_at,
                    receipt.dead_at,
                    receipt.dead_reason
            ),
            terminal AS (
                SELECT
                    attempts,
                    completed_at,
                    dead_at,
                    dead_reason
                FROM existing
                WHERE completed_at IS NOT NULL
                   OR dead_at IS NOT NULL
            ),
            inserted AS (
                INSERT INTO inbox_receipts (
                    scope,
                    message_id,
                    message_type,
                    message_version,
                    metadata,
                    attempts,
                    dead_at,
                    dead_reason,
                    last_error
                )
                SELECT
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    1,
                    CASE WHEN $6 OR 1 >= $7 THEN now() END,
                    CASE
                        WHEN $6 THEN 'permanent'
                        WHEN 1 >= $7 THEN 'exhausted'
                    END,
                    $8
                -- Existing rows are already returned by the update or terminal CTE.
                WHERE NOT EXISTS (SELECT 1 FROM existing)
                RETURNING
                    attempts,
                    completed_at,
                    dead_at,
                    dead_reason
            )
            SELECT
                attempts AS "attempts!",
                completed_at,
                dead_at,
                dead_reason
            FROM updated
            UNION ALL
            SELECT
                attempts AS "attempts!",
                completed_at,
                dead_at,
                dead_reason
            FROM terminal
            UNION ALL
            SELECT
                attempts AS "attempts!",
                completed_at,
                dead_at,
                dead_reason
            FROM inserted
        "#,
        params.scope,
        params.message_id,
        params.message_type,
        params.message_version,
        params.metadata,
        params.permanent,
        params.max_attempts,
        params.error,
    )
    .fetch_one(executor)
    .await
    .map_err(PostgresError::from)
}
