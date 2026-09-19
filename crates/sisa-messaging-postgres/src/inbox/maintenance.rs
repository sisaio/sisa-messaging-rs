//! Bounded inbox maintenance statements.

use sqlx::{Executor, Postgres};

use crate::PostgresError;

pub(super) struct PurgeParams {
    pub(super) retention_micros: i64,
    pub(super) batch_size: i64,
}

pub(super) struct StatsParams;

pub(super) struct StatsRecord {
    pub(super) pending: i64,
    pub(super) retrying: i64,
    pub(super) completed: i64,
    pub(super) dead: i64,
}

pub(super) async fn purge_completed<'e, E>(
    executor: E,
    params: PurgeParams,
) -> Result<u64, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        r#"
            -- Retention locks one completed-only batch; nonterminal receipts cannot be selected.
            DELETE FROM inbox_receipts
            WHERE id IN (
                SELECT id
                FROM inbox_receipts
                WHERE completed_at < now() - ($1::bigint * interval '1 microsecond')
                  AND dead_at IS NULL
                ORDER BY completed_at, id
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
    params: PurgeParams,
) -> Result<u64, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        r#"
            -- Retention locks one dead-only batch; nonterminal receipts cannot be selected.
            DELETE FROM inbox_receipts
            WHERE id IN (
                SELECT id
                FROM inbox_receipts
                WHERE dead_at < now() - ($1::bigint * interval '1 microsecond')
                  AND completed_at IS NULL
                ORDER BY dead_at, id
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
                    WHERE completed_at IS NULL
                      AND dead_at IS NULL
                      AND attempts = 0
                ) AS "pending!",
                count(*) FILTER (
                    WHERE completed_at IS NULL
                      AND dead_at IS NULL
                      AND attempts > 0
                ) AS "retrying!",
                count(*) FILTER (
                    WHERE completed_at IS NOT NULL
                ) AS "completed!",
                count(*) FILTER (
                    WHERE dead_at IS NOT NULL
                ) AS "dead!"
            FROM inbox_receipts
        "#,
    )
    .fetch_one(executor)
    .await
    .map_err(PostgresError::from)
}
