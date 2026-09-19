//! PostgreSQL inbox claim, completion, failure, and unit-of-work façade.

#![allow(
    clippy::manual_async_fn,
    reason = "trait implementations mirror portable native-async signatures"
)]

mod claim;
mod dead_letters;
mod maintenance;
mod outcomes;

use std::num::NonZeroU32;
use std::time::Duration;

use sisa_messaging::{ErrorSummary, MessageId, MessageType};
use sisa_messaging_inbox::{
    DeadLetterBatch, DeadLetterQuery, DeadLetterRecord, DeadReason, InboxClaimOutcome,
    InboxDeadLetters, InboxFailure, InboxFailureOutcome, InboxId, InboxMaintenance,
    InboxPurgeReport, InboxPurgeRequest, InboxReceipt, InboxRecord, InboxScope, InboxSettings,
    InboxStats, InboxStore, InboxUnitOfWork,
};
use sqlx::{PgPool, Postgres, Transaction};

use crate::{PostgresError, metadata};

/// Transaction type used by [`PostgresInboxStore`] with the portable inbox capabilities.
pub type PostgresInboxTransaction = Transaction<'static, Postgres>;

/// Provider-minted completion evidence. It is intentionally neither cloneable nor constructible.
#[derive(Debug)]
pub struct PostgresInboxReceipt {
    id: InboxId,
    attempts: u32,
}

impl InboxReceipt for PostgresInboxReceipt {
    fn id(&self) -> InboxId {
        self.id
    }

    fn recorded_failures(&self) -> u32 {
        self.attempts
    }
}

/// PostgreSQL implementation of inbox receipt claim and failure lifecycle.
#[derive(Clone, Debug)]
pub struct PostgresInboxStore {
    pool: PgPool,
    settings: InboxSettings,
}

impl PostgresInboxStore {
    /// Constructs without opening connections, probing schema, or running migrations.
    #[must_use]
    pub fn new(pool: PgPool, settings: InboxSettings) -> Self {
        Self { pool, settings }
    }
}

impl InboxStore<PostgresInboxTransaction> for PostgresInboxStore {
    type Error = PostgresError;
    type Receipt = PostgresInboxReceipt;

    fn max_attempts(&self) -> NonZeroU32 {
        self.settings.max_attempts()
    }

    fn claim(
        &self,
        transaction: &mut PostgresInboxTransaction,
        record: &InboxRecord,
    ) -> impl std::future::Future<Output = Result<InboxClaimOutcome<Self::Receipt>, Self::Error>> + Send
    {
        async move {
            if !claim::try_lock(
                &mut **transaction,
                claim::LockParams {
                    scope: record.scope.as_str(),
                    message_id: record.message_id.into_uuid(),
                },
            )
            .await?
            {
                return Ok(InboxClaimOutcome::InProgressDuplicate);
            }

            let encoded_metadata = metadata::encode(&record.metadata)?;
            let row = claim::claim(
                &mut **transaction,
                claim::ClaimParams {
                    scope: record.scope.as_str(),
                    message_id: record.message_id.into_uuid(),
                    message_type: record.message_type.as_str(),
                    message_version: i32::try_from(record.version)
                        .map_err(|_| PostgresError::InvalidData)?,
                    metadata: &encoded_metadata,
                },
            )
            .await?;

            if row.completed_at.is_some() {
                return Ok(InboxClaimOutcome::CompletedDuplicate);
            }
            if row.dead_at.is_some() {
                return Ok(InboxClaimOutcome::DeadDuplicate {
                    reason: dead_reason(row.dead_reason.as_deref())?,
                });
            }

            Ok(InboxClaimOutcome::Claimed(PostgresInboxReceipt {
                id: InboxId::from_uuid(row.id),
                attempts: u32_value(row.attempts)?,
            }))
        }
    }

    fn complete(
        &self,
        transaction: &mut PostgresInboxTransaction,
        receipt: Self::Receipt,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let updated = outcomes::complete(
                &mut **transaction,
                outcomes::CompleteParams {
                    id: receipt.id.into_uuid(),
                },
            )
            .await?;
            if updated == 1 {
                Ok(())
            } else {
                Err(PostgresError::InvalidData)
            }
        }
    }

    fn fail(
        &self,
        record: &InboxRecord,
        failure: InboxFailure,
    ) -> impl std::future::Future<Output = Result<InboxFailureOutcome, Self::Error>> + Send {
        async move {
            let mut transaction = self.pool.begin().await.map_err(PostgresError::from)?;
            outcomes::blocking_lock(
                &mut *transaction,
                claim::LockParams {
                    scope: record.scope.as_str(),
                    message_id: record.message_id.into_uuid(),
                },
            )
            .await?;

            let encoded_metadata = metadata::encode(&record.metadata)?;
            let row = outcomes::fail(
                &mut *transaction,
                outcomes::FailParams {
                    scope: record.scope.as_str(),
                    message_id: record.message_id.into_uuid(),
                    message_type: record.message_type.as_str(),
                    message_version: i32::try_from(record.version)
                        .map_err(|_| PostgresError::InvalidData)?,
                    metadata: &encoded_metadata,
                    // Any future non-retryable classification remains terminal like the portable reducer.
                    permanent: !failure.kind.is_retryable(),
                    max_attempts: i32::try_from(self.settings.max_attempts().get())
                        .map_err(|_| PostgresError::InvalidData)?,
                    error: failure.error.as_str(),
                },
            )
            .await?;
            transaction.commit().await.map_err(PostgresError::from)?;

            let attempts = u32_value(row.attempts)?;
            if row.completed_at.is_some() {
                Ok(InboxFailureOutcome::CompletedDuplicate)
            } else if row.dead_at.is_some() {
                Ok(InboxFailureOutcome::Dead {
                    attempts,
                    reason: dead_reason(row.dead_reason.as_deref())?,
                })
            } else {
                Ok(InboxFailureOutcome::Retry { attempts })
            }
        }
    }
}

impl InboxUnitOfWork for PostgresInboxStore {
    type Transaction = PostgresInboxTransaction;
    type Error = PostgresError;

    fn begin(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::Transaction, Self::Error>> + Send {
        async move { self.pool.begin().await.map_err(PostgresError::from) }
    }

    fn commit(
        &self,
        transaction: Self::Transaction,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move { transaction.commit().await.map_err(PostgresError::from) }
    }

    fn rollback(
        &self,
        transaction: Self::Transaction,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move { transaction.rollback().await.map_err(PostgresError::from) }
    }
}

impl InboxMaintenance for PostgresInboxStore {
    type Error = PostgresError;

    fn purge(
        &self,
        request: InboxPurgeRequest,
    ) -> impl std::future::Future<Output = Result<InboxPurgeReport, Self::Error>> + Send {
        async move {
            let mut transaction = self.pool.begin().await.map_err(PostgresError::from)?;
            let batch_size = i64::from(request.batch_size.get());
            let completed_deleted = match request.completed_retention {
                Some(retention) => {
                    maintenance::purge_completed(
                        &mut *transaction,
                        maintenance::PurgeParams {
                            retention_micros: micros(retention),
                            batch_size,
                        },
                    )
                    .await?
                }
                None => 0,
            };
            let dead_deleted = match request.dead_retention {
                Some(retention) => {
                    maintenance::purge_dead(
                        &mut *transaction,
                        maintenance::PurgeParams {
                            retention_micros: micros(retention),
                            batch_size,
                        },
                    )
                    .await?
                }
                None => 0,
            };
            transaction.commit().await.map_err(PostgresError::from)?;
            Ok(InboxPurgeReport {
                completed_deleted,
                dead_deleted,
            })
        }
    }

    fn stats(&self) -> impl std::future::Future<Output = Result<InboxStats, Self::Error>> + Send {
        async move {
            let record = maintenance::stats(&self.pool, maintenance::StatsParams).await?;
            Ok(InboxStats {
                pending: count(record.pending)?,
                retrying: count(record.retrying)?,
                completed: count(record.completed)?,
                dead: count(record.dead)?,
            })
        }
    }
}

impl InboxDeadLetters for PostgresInboxStore {
    type Error = PostgresError;

    fn list(
        &self,
        query: DeadLetterQuery,
    ) -> impl std::future::Future<Output = Result<Vec<DeadLetterRecord>, Self::Error>> + Send {
        async move {
            let (after_dead_at, after_id) = match query.after {
                Some(cursor) => (
                    Some(metadata::system_time_to_utc(cursor.dead_at)?),
                    Some(cursor.id.into_uuid()),
                ),
                None => (None, None),
            };
            dead_letters::list(
                &self.pool,
                dead_letters::ListParams {
                    after_dead_at,
                    after_id,
                    limit: i64::from(query.limit.get()),
                },
            )
            .await?
            .into_iter()
            .map(dead_letter_record)
            .collect()
        }
    }

    fn retry(
        &self,
        batch: DeadLetterBatch<'_>,
    ) -> impl std::future::Future<Output = Result<Vec<InboxId>, Self::Error>> + Send {
        async move {
            let ids: Vec<_> = batch.ids().iter().map(|id| id.into_uuid()).collect();
            let records =
                dead_letters::retry(&self.pool, dead_letters::RetryParams { ids: &ids }).await?;
            Ok(records
                .into_iter()
                .map(|record| InboxId::from_uuid(record.id))
                .collect())
        }
    }

    fn delete(
        &self,
        batch: DeadLetterBatch<'_>,
    ) -> impl std::future::Future<Output = Result<Vec<InboxId>, Self::Error>> + Send {
        async move {
            let ids: Vec<_> = batch.ids().iter().map(|id| id.into_uuid()).collect();
            let records =
                dead_letters::delete(&self.pool, dead_letters::DeleteParams { ids: &ids }).await?;
            Ok(records
                .into_iter()
                .map(|record| InboxId::from_uuid(record.id))
                .collect())
        }
    }
}

fn u32_value(value: i32) -> Result<u32, PostgresError> {
    u32::try_from(value).map_err(|_| PostgresError::InvalidData)
}

fn count(value: i64) -> Result<u64, PostgresError> {
    u64::try_from(value).map_err(|_| PostgresError::InvalidData)
}

fn micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

fn dead_letter_record(
    row: dead_letters::DeadLetterRecord,
) -> Result<DeadLetterRecord, PostgresError> {
    Ok(DeadLetterRecord {
        id: InboxId::from_uuid(row.id),
        scope: InboxScope::new(row.scope).map_err(|_| PostgresError::InvalidData)?,
        message_id: MessageId::from_uuid(row.message_id),
        message_type: MessageType::new(row.message_type).map_err(|_| PostgresError::InvalidData)?,
        version: u32_value(row.message_version)?,
        // Dead-letter inspection deliberately tolerates poisoned diagnostics metadata.
        metadata: metadata::decode(row.metadata).ok(),
        attempts: u32_value(row.attempts)?,
        received_at: row.received_at.into(),
        dead_at: row.dead_at.into(),
        reason: dead_reason(Some(row.dead_reason.as_str()))?,
        last_error: row
            .last_error
            .map(|error| ErrorSummary::from_safe_text(&error)),
    })
}

fn dead_reason(value: Option<&str>) -> Result<DeadReason, PostgresError> {
    match value {
        Some("permanent") => Ok(DeadReason::Permanent),
        Some("exhausted") => Ok(DeadReason::Exhausted),
        _ => Err(PostgresError::InvalidData),
    }
}
