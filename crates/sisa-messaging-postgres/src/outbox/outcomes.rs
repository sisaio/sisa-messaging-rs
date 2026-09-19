#![allow(
    clippy::manual_async_fn,
    reason = "trait implementations mirror portable native-async signatures"
)]
//! Fenced publication outcome implementation.

use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::PostgresError;

/// Bound claims for an acknowledged publication batch.
pub(super) struct CompleteParams {
    pub(super) ids: Vec<Uuid>,
    pub(super) tokens: Vec<Uuid>,
}

/// Bound outcomes for one retry or terminal-failure batch.
pub(super) struct FailParams<'a> {
    pub(super) ids: Vec<Uuid>,
    pub(super) tokens: Vec<Uuid>,
    pub(super) dead: Vec<bool>,
    pub(super) delays_micros: Vec<i64>,
    pub(super) reasons: Vec<&'a str>,
    pub(super) errors: Vec<&'a str>,
}

/// Bound claims for a voluntary release batch.
pub(super) struct ReleaseParams {
    pub(super) ids: Vec<Uuid>,
    pub(super) tokens: Vec<Uuid>,
}

/// Bound claims and duration for a lease-renewal batch.
pub(super) struct ExtendLeaseParams {
    pub(super) ids: Vec<Uuid>,
    pub(super) tokens: Vec<Uuid>,
    pub(super) lease_micros: i64,
}

/// One claim whose fence matched an outcome statement.
pub(super) struct FencedClaimRecord {
    pub(super) id: Uuid,
    pub(super) claim_token: Uuid,
}

pub(super) async fn complete<'e, E>(
    executor: E,
    params: CompleteParams,
) -> Result<Vec<FencedClaimRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        FencedClaimRecord,
        r#"
            -- Match id and token so a stale publisher cannot complete a newer lease.
            UPDATE outbox_messages AS o
            SET
                attempts = o.attempts + 1,
                published_at = now(),
                claim_token = NULL,
                locked_by = NULL
            -- Pair each row identity with its original claim fence.
            FROM unnest($1::uuid[], $2::uuid[]) AS requested(id, token)
            -- A matching fence is required before a terminal publish transition.
            WHERE o.id = requested.id
              AND o.claim_token = requested.token
              AND o.published_at IS NULL
              AND o.dead_at IS NULL
            RETURNING
                o.id AS "id!",
                requested.token AS "claim_token!"
        "#,
        &params.ids,
        &params.tokens,
    )
    .fetch_all(executor)
    .await
    .map_err(PostgresError::from)
}

pub(super) async fn fail<'e, E>(
    executor: E,
    params: FailParams<'_>,
) -> Result<Vec<FencedClaimRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    // SQLx's checked PostgreSQL `text[]` binding requires owned strings at the query boundary.
    let reasons: Vec<String> = params.reasons.into_iter().map(str::to_owned).collect();
    let errors: Vec<String> = params.errors.into_iter().map(str::to_owned).collect();
    sqlx::query_as!(
        FencedClaimRecord,
        r#"
            -- One bounded array update records each fenced retry or terminal failure.
            UPDATE outbox_messages AS o
            SET
                attempts = o.attempts + 1,
                -- Retried rows use database time; terminal rows retain their prior availability.
                claimable_at = CASE
                    WHEN requested.dead THEN o.claimable_at
                    ELSE now() + (requested.delay * interval '1 microsecond')
                END,
                -- Terminal failures alone receive death metadata.
                dead_at = CASE WHEN requested.dead THEN now() ELSE NULL END,
                dead_reason = CASE WHEN requested.dead THEN requested.reason ELSE NULL END,
                last_error = requested.error,
                claim_token = NULL,
                locked_by = NULL
            -- Align each failure action with the claim it is allowed to settle.
            FROM unnest(
                $1::uuid[],
                $2::uuid[],
                $3::bool[],
                $4::bigint[],
                $5::text[],
                $6::text[]
            ) AS requested(id, token, dead, delay, reason, error)
            -- Fencing preserves a successor's claim when this publisher is stale.
            WHERE o.id = requested.id
              AND o.claim_token = requested.token
              AND o.published_at IS NULL
              AND o.dead_at IS NULL
            RETURNING
                o.id AS "id!",
                requested.token AS "claim_token!"
        "#,
        &params.ids,
        &params.tokens,
        &params.dead,
        &params.delays_micros,
        &reasons,
        &errors,
    )
    .fetch_all(executor)
    .await
    .map_err(PostgresError::from)
}

pub(super) async fn release<'e, E>(
    executor: E,
    params: ReleaseParams,
) -> Result<Vec<FencedClaimRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        FencedClaimRecord,
        r#"
            -- Release only the worker's still-current claims without incrementing attempts.
            UPDATE outbox_messages AS o
            SET
                claimable_at = now(),
                claim_token = NULL,
                locked_by = NULL
            -- Pair each release request with the fence minted at claim time.
            FROM unnest($1::uuid[], $2::uuid[]) AS requested(id, token)
            -- Never release a claim that another worker has superseded.
            WHERE o.id = requested.id
              AND o.claim_token = requested.token
              AND o.published_at IS NULL
              AND o.dead_at IS NULL
            RETURNING
                o.id AS "id!",
                requested.token AS "claim_token!"
        "#,
        &params.ids,
        &params.tokens,
    )
    .fetch_all(executor)
    .await
    .map_err(PostgresError::from)
}

pub(super) async fn extend_lease<'e, E>(
    executor: E,
    params: ExtendLeaseParams,
) -> Result<Vec<FencedClaimRecord>, PostgresError>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        FencedClaimRecord,
        r#"
            -- Renewal retains the same fence token while moving the database-time lease.
            UPDATE outbox_messages AS o
            SET
                claimable_at = now() + ($3::bigint * interval '1 microsecond')
            -- Pair each renewal request with its currently held fence.
            FROM unnest($1::uuid[], $2::uuid[]) AS requested(id, token)
            -- A stale renewal must not extend the successor's lease.
            WHERE o.id = requested.id
              AND o.claim_token = requested.token
              AND o.published_at IS NULL
              AND o.dead_at IS NULL
            RETURNING
                o.id AS "id!",
                o.claim_token AS "claim_token!"
        "#,
        &params.ids,
        &params.tokens,
        params.lease_micros,
    )
    .fetch_all(executor)
    .await
    .map_err(PostgresError::from)
}
