//! Bounded outbox maintenance statements.

use sqlx::{Executor, Postgres};

use crate::PostgresError;

/// Bound input for one expired-row transition pass.
pub(super) struct ExpireParams {
    pub(super) batch_size: i64,
}

/// Bound input for one published-retention deletion pass.
pub(super) struct PurgePublishedParams {
    pub(super) retention_micros: i64,

    pub(super) batch_size: i64,
}

/// Bound input for one dead-letter-retention deletion pass.
pub(super) struct PurgeDeadParams {
    pub(super) retention_micros: i64,

    pub(super) batch_size: i64,
}

/// Empty input that keeps the aggregate read at the helper boundary.
pub(super) struct StatsParams;

/// Aggregate outbox counts and the oldest claimable head age in seconds.
pub(super) struct StatsRecord {
    pub(super) pending: i64,

    pub(super) expired: i64,

    pub(super) dead: i64,

    pub(super) oldest_pending_age_seconds: f64,
}

pub(super) async fn expire<'e, E>(executor: E, params: ExpireParams) -> Result<u64, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        r#"
            -- Expire only due rows that are unclaimed or whose stale lease has elapsed.
            WITH candidates AS (
                SELECT id
                FROM outbox_messages
                WHERE published_at IS NULL
                  AND dead_at IS NULL
                  AND expires_at <= now()
                  AND (claim_token IS NULL OR claimable_at <= now())
                ORDER BY expires_at, id
                -- Keep this bounded pass disjoint from concurrent maintenance workers.
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE outbox_messages AS o
            SET
                dead_at = now(),
                dead_reason = 'expired',
                claim_token = NULL,
                locked_by = NULL
            FROM candidates AS c
            WHERE o.id = c.id
        "#,
        params.batch_size,
    )
    .execute(executor)
    .await
    .map(|result| result.rows_affected())
    .map_err(PostgresError::from)
}

pub(super) async fn purge_published<'e, E>(
    executor: E,
    params: PurgePublishedParams,
) -> Result<u64, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        r#"
            -- Delete one locked retention batch through the published-time access path.
            DELETE FROM outbox_messages
            WHERE id IN (
                SELECT id
                FROM outbox_messages
                WHERE published_at < now() - ($1::bigint * interval '1 microsecond')
                ORDER BY published_at, id
                -- Preserve concurrent retention and operator work on rows outside this batch.
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
        "#,
        params.retention_micros,
        params.batch_size,
    )
    .execute(executor)
    .await
    .map(|result| result.rows_affected())
    .map_err(PostgresError::from)
}

pub(super) async fn purge_dead<'e, E>(
    executor: E,
    params: PurgeDeadParams,
) -> Result<u64, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        r#"
            -- Delete one locked retention batch through the dead-letter access path.
            DELETE FROM outbox_messages
            WHERE id IN (
                SELECT id
                FROM outbox_messages
                WHERE dead_at < now() - ($1::bigint * interval '1 microsecond')
                ORDER BY dead_at, id
                -- Preserve concurrent retention and operator work on rows outside this batch.
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
        "#,
        params.retention_micros,
        params.batch_size,
    )
    .execute(executor)
    .await
    .map(|result| result.rows_affected())
    .map_err(PostgresError::from)
}

pub(super) async fn stats<'e, E>(
    executor: E,
    _params: StatsParams,
) -> Result<StatsRecord, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        StatsRecord,
        r#"
            SELECT
                count(*) FILTER (
                    WHERE published_at IS NULL
                      AND dead_at IS NULL
                      AND (expires_at IS NULL OR expires_at > now())
                ) AS "pending!",
                count(*) FILTER (
                    WHERE published_at IS NULL
                      AND dead_at IS NULL
                      AND expires_at <= now()
                ) AS "expired!",
                count(*) FILTER (
                    WHERE dead_at IS NOT NULL
                ) AS "dead!",
                COALESCE(
                    EXTRACT(
                        EPOCH FROM now() - min(created_at) FILTER (
                            WHERE published_at IS NULL
                              AND dead_at IS NULL
                              AND (expires_at IS NULL OR expires_at > now())
                              AND claimable_at <= now()
                              -- The oldest age is limited to an ordering-key head claim can take.
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM outbox_messages AS p
                                  WHERE p.ordering_key = outbox_messages.ordering_key
                                    AND p.ordering_key IS NOT NULL
                                    AND p.published_at IS NULL
                                    AND p.dead_at IS NULL
                                    -- An expired predecessor owns its key until its current
                                    -- lease ends.
                                    AND (
                                        p.expires_at IS NULL
                                        OR p.expires_at > now()
                                        OR (
                                            p.claim_token IS NOT NULL
                                            AND p.claimable_at > now()
                                        )
                                    )
                                    AND p.id < outbox_messages.id
                              )
                        )
                    ),
                    0
                )::float8 AS "oldest_pending_age_seconds!"
            FROM outbox_messages
        "#,
    )
    .fetch_one(executor)
    .await
    .map_err(PostgresError::from)
}
