//! Inbox claim statements.

use chrono::{DateTime, Utc};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::PostgresError;

pub(super) struct LockParams<'a> {
    pub(super) scope: &'a str,

    pub(super) message_id: Uuid,
}

pub(super) struct ClaimParams<'a> {
    pub(super) scope: &'a str,

    pub(super) message_id: Uuid,

    pub(super) message_type: &'a str,

    pub(super) message_version: i32,

    pub(super) metadata: &'a serde_json::Value,
}

pub(super) struct ClaimRecord {
    pub(super) id: Uuid,

    pub(super) attempts: i32,

    pub(super) completed_at: Option<DateTime<Utc>>,

    pub(super) dead_at: Option<DateTime<Utc>>,

    pub(super) dead_reason: Option<String>,
}

pub(super) async fn try_lock<'e, E>(
    executor: E,
    params: LockParams<'_>,
) -> Result<bool, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar!(
        r#"
            -- The separator prevents ambiguous scope/message lock-key concatenation.
            SELECT pg_try_advisory_xact_lock(
                hashtextextended($1 || chr(31) || ($2::uuid)::text, 0)
            ) AS "locked!"
        "#,
        params.scope,
        params.message_id,
    )
    .fetch_one(executor)
    .await
    .map_err(PostgresError::from)
}

pub(super) async fn claim<'e, E>(
    executor: E,
    params: ClaimParams<'_>,
) -> Result<ClaimRecord, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        ClaimRecord,
        r#"
            INSERT INTO inbox_receipts (
                scope,
                message_id,
                message_type,
                message_version,
                metadata
            )
            VALUES ($1, $2, $3, $4, $5)
            -- Preserve original receipt diagnostics while reading its durable state.
            ON CONFLICT (scope, message_id) DO UPDATE
            SET scope = inbox_receipts.scope
            RETURNING
                id AS "id!",
                attempts AS "attempts!",
                completed_at,
                dead_at,
                dead_reason
        "#,
        params.scope,
        params.message_id,
        params.message_type,
        params.message_version,
        params.metadata,
    )
    .fetch_one(executor)
    .await
    .map_err(PostgresError::from)
}
