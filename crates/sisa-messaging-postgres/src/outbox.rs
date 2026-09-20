//! PostgreSQL outbox provider façade.

#![allow(
    clippy::manual_async_fn,
    reason = "trait signatures use native async futures"
)]

mod claim;
mod dead_letters;
mod enqueue;
mod maintenance;
mod outcomes;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sisa_messaging::{
    ContentType, Envelope, ErrorSummary, Message, MessageId, MessageType, OrderingKey,
    SerializedEnvelope, Serializer,
};
use sisa_messaging_outbox::{
    Claim, ClaimBatch, ClaimRequest, DeadLetterBatch, DeadLetterQuery, DeadLetterRecord,
    EnqueueOptions, FailureRecord, FencedClaims, OutboxDeadLetters, OutboxEnqueue,
    OutboxMaintenance, OutboxPurgeReport, OutboxPurgeRequest, OutboxStats, OutboxStore,
};
use sqlx::PgPool;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::PostgresError;

/// PostgreSQL implementation of all outbox capabilities using an application-owned pool.
#[derive(Clone, Debug)]
pub struct PostgresOutboxStore<Ser> {
    pub(super) pool: PgPool,

    pub(super) serializer: Ser,
}

impl<Ser> PostgresOutboxStore<Ser> {
    /// Constructs without opening connections, probing schema, or running migrations.
    #[must_use]
    pub fn new(pool: PgPool, serializer: Ser) -> Self {
        Self { pool, serializer }
    }
}

impl<Ser> OutboxEnqueue<Transaction<'_, Postgres>> for PostgresOutboxStore<Ser>
where
    Ser: Send + Sync,
{
    type Error = PostgresError;
    type Serializer = Ser;

    #[rustfmt::skip]
    fn enqueue<M>(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        source: &Envelope<M>,
        options: EnqueueOptions,
    ) -> impl std::future::Future<
        Output = Result<sisa_messaging_outbox::OutboxId, Self::Error>,
    > + Send
    where
        M: Message,
        Ser: Serializer<M>,
    {
        async move {
            let serialized = self
                .serializer
                .serialize(source)
                .map_err(|_| PostgresError::InvalidData)?;
            let metadata = crate::metadata::encode(&serialized.metadata)?;
            let params = enqueue::EnqueueParams {
                message_id: serialized.message_id.into_uuid(),
                message_type: serialized.message_type.as_str(),
                message_version: i32::try_from(serialized.message_version)
                    .map_err(|_| PostgresError::InvalidData)?,
                content_type: serialized.content_type.as_str(),
                payload: &serialized.payload,
                metadata: &metadata,
                ordering_key: serialized.ordering_key.as_ref().map(|key| key.as_str()),
                expires_at: options
                    .expires_at
                    .map(crate::metadata::system_time_to_utc)
                    .transpose()?,
            };
            let record = enqueue::enqueue(&mut **transaction, params).await?;
            Ok(sisa_messaging_outbox::OutboxId::from_uuid(record.id))
        }
    }
}

impl<Ser> OutboxStore for PostgresOutboxStore<Ser>
where
    Ser: Send + Sync + 'static,
{
    type Error = PostgresError;

    fn claim(
        &self,
        request: ClaimRequest,
    ) -> impl std::future::Future<Output = Result<ClaimBatch, Self::Error>> + Send {
        async move {
            let rows = claim::claim(
                &self.pool,
                claim::ClaimParams {
                    limit: i64::from(request.limit.get()),
                    worker_id: &request.worker_id,
                    lease_micros: duration_micros(request.lease)?,
                },
            )
            .await?;
            let mut records = Vec::with_capacity(rows.len());
            let mut poison = sisa_messaging_outbox::PoisonReport::default();
            let mut ids = Vec::new();
            let mut tokens = Vec::new();
            for row in rows {
                match decode_envelope(
                    row.message_id,
                    row.message_type,
                    row.message_version,
                    row.content_type,
                    row.payload,
                    row.metadata,
                    row.ordering_key,
                ) {
                    Ok(envelope) => records.push(sisa_messaging_outbox::ClaimedRecord {
                        claim: Claim {
                            id: sisa_messaging_outbox::OutboxId::from_uuid(row.id),
                            token: sisa_messaging_outbox::ClaimToken::from_uuid(row.claim_token),
                        },
                        envelope,
                        attempts: nonnegative_u32(row.attempts)?,
                    }),
                    Err(_) => {
                        poison.observed += 1;
                        ids.push(row.id);
                        tokens.push(row.claim_token);
                    }
                }
            }
            if !ids.is_empty()
                && let Ok(updated) = claim::poison(
                    &self.pool,
                    claim::PoisonParams {
                        ids: &ids,
                        tokens: &tokens,
                    },
                )
                .await
            {
                poison.marked_dead = updated.min(u64::from(poison.observed)) as u32;
            }
            Ok(ClaimBatch { records, poison })
        }
    }

    fn complete(
        &self,
        requested: &[Claim],
    ) -> impl std::future::Future<Output = Result<FencedClaims, Self::Error>> + Send {
        async move {
            let (ids, tokens) = claim_parts(requested);
            let records =
                outcomes::complete(&self.pool, outcomes::CompleteParams { ids, tokens }).await?;
            Ok(fenced_claims(records))
        }
    }

    fn fail(
        &self,
        requested: &[FailureRecord],
    ) -> impl std::future::Future<Output = Result<FencedClaims, Self::Error>> + Send {
        async move {
            let mut params = outcomes::FailParams {
                ids: Vec::with_capacity(requested.len()),
                tokens: Vec::with_capacity(requested.len()),
                dead: Vec::with_capacity(requested.len()),
                delays_micros: Vec::with_capacity(requested.len()),
                reasons: Vec::with_capacity(requested.len()),
                errors: Vec::with_capacity(requested.len()),
            };
            for failure in requested {
                let (dead, delay_micros, reason) = match failure.action {
                    sisa_messaging_outbox::FailureAction::Retry { delay } => {
                        (false, duration_micros(delay)?, "")
                    }
                    sisa_messaging_outbox::FailureAction::Dead { reason } => {
                        (true, 0, reason.as_str())
                    }
                    _ => return Err(PostgresError::InvalidData),
                };
                params.ids.push(failure.claim.id.into_uuid());
                params.tokens.push(failure.claim.token.into_uuid());
                params.dead.push(dead);
                params.delays_micros.push(delay_micros);
                params.reasons.push(reason);
                params.errors.push(failure.error.as_str());
            }
            let records = outcomes::fail(&self.pool, params).await?;
            Ok(fenced_claims(records))
        }
    }

    fn release(
        &self,
        requested: &[Claim],
    ) -> impl std::future::Future<Output = Result<FencedClaims, Self::Error>> + Send {
        async move {
            let (ids, tokens) = claim_parts(requested);
            let records =
                outcomes::release(&self.pool, outcomes::ReleaseParams { ids, tokens }).await?;
            Ok(fenced_claims(records))
        }
    }

    fn extend_lease(
        &self,
        requested: &[Claim],
        lease: std::time::Duration,
    ) -> impl std::future::Future<Output = Result<FencedClaims, Self::Error>> + Send {
        async move {
            let (ids, tokens) = claim_parts(requested);
            let records = outcomes::extend_lease(
                &self.pool,
                outcomes::ExtendLeaseParams {
                    ids,
                    tokens,
                    lease_micros: duration_micros(lease)?,
                },
            )
            .await?;
            Ok(fenced_claims(records))
        }
    }
}

fn claim_parts(requested: &[Claim]) -> (Vec<uuid::Uuid>, Vec<uuid::Uuid>) {
    let mut ids = Vec::with_capacity(requested.len());
    let mut tokens = Vec::with_capacity(requested.len());
    for claim in requested {
        ids.push(claim.id.into_uuid());
        tokens.push(claim.token.into_uuid());
    }
    (ids, tokens)
}

fn fenced_claims(records: Vec<outcomes::FencedClaimRecord>) -> FencedClaims {
    fenced_claims_from_rows(
        records
            .into_iter()
            .map(|record| (record.id, record.claim_token)),
    )
}

impl<Ser> OutboxMaintenance for PostgresOutboxStore<Ser>
where
    Ser: Send + Sync,
{
    type Error = PostgresError;

    fn purge(
        &self,
        request: OutboxPurgeRequest,
    ) -> impl std::future::Future<Output = Result<OutboxPurgeReport, Self::Error>> + Send {
        async move {
            let published_retention = duration_micros(request.published_retention)?;
            let dead_retention = duration_micros(request.dead_retention)?;
            let batch_size = i64::from(request.batch_size.get());
            let mut transaction = self.pool.begin().await.map_err(PostgresError::from)?;
            let expired =
                maintenance::expire(&mut *transaction, maintenance::ExpireParams { batch_size })
                    .await?;
            let published_deleted = maintenance::purge_published(
                &mut *transaction,
                maintenance::PurgePublishedParams {
                    retention_micros: published_retention,
                    batch_size,
                },
            )
            .await?;
            let dead_deleted = maintenance::purge_dead(
                &mut *transaction,
                maintenance::PurgeDeadParams {
                    retention_micros: dead_retention,
                    batch_size,
                },
            )
            .await?;
            transaction.commit().await.map_err(PostgresError::from)?;

            Ok(OutboxPurgeReport {
                expired,
                published_deleted,
                dead_deleted,
            })
        }
    }

    fn stats(&self) -> impl std::future::Future<Output = Result<OutboxStats, Self::Error>> + Send {
        async move {
            let record = maintenance::stats(&self.pool, maintenance::StatsParams).await?;
            Ok(OutboxStats {
                pending: count(record.pending)?,
                expired: count(record.expired)?,
                dead: count(record.dead)?,
                oldest_pending_age: duration_seconds(record.oldest_pending_age_seconds)?,
            })
        }
    }
}

impl<Ser> OutboxDeadLetters for PostgresOutboxStore<Ser>
where
    Ser: Send + Sync,
{
    type Error = PostgresError;

    fn list(
        &self,
        query: DeadLetterQuery,
    ) -> impl std::future::Future<Output = Result<Vec<DeadLetterRecord>, Self::Error>> + Send {
        async move {
            let (after_dead_at, after_id) = match query.after {
                Some(cursor) => (
                    Some(crate::metadata::system_time_to_utc(cursor.dead_at)?),
                    Some(cursor.id.into_uuid()),
                ),
                None => (None, None),
            };
            let records = dead_letters::list(
                &self.pool,
                dead_letters::ListParams {
                    after_dead_at,
                    after_id,
                    limit: i64::from(query.limit.get()),
                },
            )
            .await?;
            records.into_iter().map(dead_letter_record).collect()
        }
    }

    #[rustfmt::skip]
    fn retry(
        &self,
        batch: DeadLetterBatch<'_>,
    ) -> impl std::future::Future<
        Output = Result<Vec<sisa_messaging_outbox::OutboxId>, Self::Error>,
    > + Send {
        async move {
            let records = dead_letters::retry(
                &self.pool,
                dead_letters::RetryParams {
                    ids: batch.ids().iter().map(|id| id.into_uuid()).collect(),
                },
            )
            .await?;
            Ok(records
                .into_iter()
                .map(|record| sisa_messaging_outbox::OutboxId::from_uuid(record.id))
                .collect())
        }
    }

    #[rustfmt::skip]
    fn delete(
        &self,
        batch: DeadLetterBatch<'_>,
    ) -> impl std::future::Future<
        Output = Result<Vec<sisa_messaging_outbox::OutboxId>, Self::Error>,
    > + Send {
        async move {
            let records = dead_letters::delete(
                &self.pool,
                dead_letters::DeleteParams {
                    ids: batch.ids().iter().map(|id| id.into_uuid()).collect(),
                },
            )
            .await?;
            Ok(records
                .into_iter()
                .map(|record| sisa_messaging_outbox::OutboxId::from_uuid(record.id))
                .collect())
        }
    }
}

fn count(value: i64) -> Result<u64, PostgresError> {
    u64::try_from(value).map_err(|_| PostgresError::InvalidData)
}

fn dead_letter_record(
    row: dead_letters::DeadLetterListRecord,
) -> Result<DeadLetterRecord, PostgresError> {
    Ok(DeadLetterRecord {
        id: sisa_messaging_outbox::OutboxId::from_uuid(row.id),
        message_id: MessageId::from_uuid(row.message_id),
        envelope: decode_envelope(
            row.message_id,
            row.message_type,
            row.message_version,
            row.content_type,
            row.payload,
            row.metadata,
            row.ordering_key,
        )
        .ok(),
        attempts: nonnegative_u32(row.attempts)?,
        dead_at: utc_to_system_time(row.dead_at),
        reason: decode_dead_reason(row.dead_reason.as_str())?,
        last_error: row
            .last_error
            .map(|error| ErrorSummary::from_safe_text(&error)),
    })
}

fn duration_micros(duration: Duration) -> Result<i64, PostgresError> {
    const MAX_MICROS: u128 = (i32::MAX as u128) * 1_000_000;
    let micros = duration.as_micros();
    if micros > MAX_MICROS {
        return Err(PostgresError::InvalidData);
    }
    i64::try_from(micros).map_err(|_| PostgresError::InvalidData)
}

fn duration_seconds(value: f64) -> Result<Duration, PostgresError> {
    if !value.is_finite() || value.is_sign_negative() {
        return Err(PostgresError::InvalidData);
    }
    Duration::try_from_secs_f64(value).map_err(|_| PostgresError::InvalidData)
}

fn utc_to_system_time(value: DateTime<Utc>) -> std::time::SystemTime {
    value.into()
}

fn nonnegative_u32(value: i32) -> Result<u32, PostgresError> {
    u32::try_from(value).map_err(|_| PostgresError::InvalidData)
}

fn decode_dead_reason(value: &str) -> Result<sisa_messaging_outbox::DeadReason, PostgresError> {
    match value {
        "expired" => Ok(sisa_messaging_outbox::DeadReason::Expired),
        "undecodable" => Ok(sisa_messaging_outbox::DeadReason::Undecodable),
        "permanent" => Ok(sisa_messaging_outbox::DeadReason::Permanent),
        "exhausted" => Ok(sisa_messaging_outbox::DeadReason::Exhausted),
        _ => Err(PostgresError::InvalidData),
    }
}

fn decode_envelope(
    message_id: Uuid,
    message_type: String,
    message_version: i32,
    content_type: String,
    payload: Vec<u8>,
    metadata: serde_json::Value,
    ordering_key: Option<String>,
) -> Result<SerializedEnvelope, PostgresError> {
    Ok(SerializedEnvelope {
        message_id: MessageId::from_uuid(message_id),
        message_type: MessageType::new(message_type).map_err(|_| PostgresError::InvalidData)?,
        message_version: nonnegative_u32(message_version)?,
        content_type: ContentType::new(content_type).map_err(|_| PostgresError::InvalidData)?,
        payload,
        metadata: crate::metadata::decode(metadata)?,
        ordering_key: ordering_key
            .map(|value| OrderingKey::new(value).map_err(|_| PostgresError::InvalidData))
            .transpose()?,
    })
}

fn fenced_claims_from_rows(rows: impl IntoIterator<Item = (Uuid, Uuid)>) -> FencedClaims {
    let confirmed = rows
        .into_iter()
        .map(|(id, claim_token)| Claim {
            id: sisa_messaging_outbox::OutboxId::from_uuid(id),
            token: sisa_messaging_outbox::ClaimToken::from_uuid(claim_token),
        })
        .collect();
    FencedClaims { confirmed }
}
