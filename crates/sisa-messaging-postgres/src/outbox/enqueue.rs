//! Transactional enqueue statement.

use chrono::{DateTime, Utc};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::PostgresError;

/// Bound values for one transactional outbox insert.
pub(super) struct EnqueueParams<'a> {
    pub(super) message_id: Uuid,
    pub(super) message_type: &'a str,
    pub(super) message_version: i32,
    pub(super) content_type: &'a str,
    pub(super) payload: &'a [u8],
    pub(super) metadata: &'a serde_json::Value,
    pub(super) ordering_key: Option<&'a str>,
    pub(super) expires_at: Option<DateTime<Utc>>,
}

/// Provider-generated row identity returned by enqueue.
pub(super) struct EnqueueRecord {
    pub(super) id: Uuid,
}

pub(super) async fn enqueue<'e, E>(
    executor: E,
    params: EnqueueParams<'_>,
) -> Result<EnqueueRecord, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        EnqueueRecord,
        r#"
            -- One insert remains inside the caller-owned business transaction.
            INSERT INTO outbox_messages (
                message_id, message_type, message_version, content_type,
                payload, metadata, ordering_key, expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id AS "id!"
        "#,
        params.message_id,
        params.message_type,
        params.message_version,
        params.content_type,
        params.payload,
        params.metadata,
        params.ordering_key,
        params.expires_at,
    )
    .fetch_one(executor)
    .await
    .map_err(PostgresError::from)
}
