//! Claim and poison-isolation statements.

use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::PostgresError;

pub(super) struct ClaimParams<'a> {
    pub(super) limit: i64,
    pub(super) worker_id: &'a str,
    pub(super) lease_micros: i64,
}

pub(super) struct PoisonParams<'a> {
    pub(super) ids: &'a [Uuid],
    pub(super) tokens: &'a [Uuid],
}

pub(super) struct ClaimRecord {
    pub(super) id: Uuid,
    pub(super) claim_token: Uuid,
    pub(super) message_id: Uuid,
    pub(super) message_type: String,
    pub(super) message_version: i32,
    pub(super) content_type: String,
    pub(super) payload: Vec<u8>,
    pub(super) metadata: serde_json::Value,
    pub(super) ordering_key: Option<String>,
    pub(super) attempts: i32,
}

pub(super) async fn claim<'e, E>(
    executor: E,
    params: ClaimParams<'_>,
) -> Result<Vec<ClaimRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        ClaimRecord,
        r#"
            -- Candidates are active rows whose initial availability or backoff deadline is due.
            WITH candidates AS (
                SELECT o.id
                FROM outbox_messages AS o
                WHERE o.published_at IS NULL
                  AND o.dead_at IS NULL
                  AND o.claimable_at <= now()
                  AND (o.expires_at IS NULL OR o.expires_at > now())
                -- Only the ordering-key head may be claimed while predecessors remain active.
                  AND NOT EXISTS (
                      SELECT 1
                      FROM outbox_messages AS p
                      WHERE p.ordering_key = o.ordering_key
                        AND p.ordering_key IS NOT NULL
                        AND p.published_at IS NULL
                        AND p.dead_at IS NULL
                        AND (p.expires_at IS NULL OR p.expires_at > now())
                        AND p.id < o.id
                  )
                ORDER BY o.claimable_at, o.id
                -- Lock only this bounded selection; concurrent workers skip these rows.
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            ),
            claimed AS (
                -- Atomically mint each fence and move its availability to the lease deadline.
                UPDATE outbox_messages AS o
                SET
                    claim_token = uuidv7(),
                    locked_by = $2,
                    claimable_at = now() + ($3::bigint * interval '1 microsecond')
                FROM candidates AS c
                WHERE o.id = c.id
                RETURNING
                    o.id,
                    o.claim_token,
                    o.message_id,
                    o.message_type,
                    o.message_version,
                    o.content_type,
                    o.payload,
                    o.metadata,
                    o.ordering_key,
                    o.attempts
            )
            SELECT
                id AS "id!",
                claim_token AS "claim_token!",
                message_id AS "message_id!",
                message_type AS "message_type!",
                message_version AS "message_version!",
                content_type AS "content_type!",
                payload AS "payload!",
                metadata AS "metadata!",
                ordering_key,
                attempts AS "attempts!"
            FROM claimed
        "#,
        params.limit,
        params.worker_id,
        params.lease_micros,
    )
    .fetch_all(executor)
    .await
    .map_err(PostgresError::from)
}

pub(super) async fn poison<'e, E>(
    executor: E,
    params: PoisonParams<'_>,
) -> Result<u64, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        r#"
            -- Fenced set update isolates poison rows without reprocessing healthy records.
            UPDATE outbox_messages AS o
            SET
                dead_at = now(),
                dead_reason = 'undecodable',
                last_error = 'persisted provider data is invalid',
                claim_token = NULL,
                locked_by = NULL
            -- Pair each undecodable row with the claim fence that permits this transition.
            FROM unnest($1::uuid[], $2::uuid[]) AS poisoned(id, token)
            -- A successor's claim remains intact when poison handling is stale.
            WHERE o.id = poisoned.id
              AND o.claim_token = poisoned.token
              AND o.published_at IS NULL
              AND o.dead_at IS NULL
        "#,
        params.ids,
        params.tokens,
    )
    .execute(executor)
    .await
    .map(|result| result.rows_affected())
    .map_err(PostgresError::from)
}
