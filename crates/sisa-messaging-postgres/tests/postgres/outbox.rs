use std::{
    borrow::Cow,
    error::Error,
    fmt,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::support::{
    OutboxLookupParams, TestMessage, TestSerializer, insert_outbox_row,
    insert_outbox_row_with_attempts, isolated_concurrent_outbox_pool, isolated_outbox_pool,
    outbox_record, test_envelope,
};
use sisa_messaging::{
    ErrorClassifier, ErrorSummary, FailureKind, HeaderName, HeaderValue, Headers, Message,
    MessageId, Metadata, MetadataValue, RoutingMetadata,
};
use sisa_messaging_inbox::{InboxMaintenance, InboxPurgeRequest, InboxSettings};
use sisa_messaging_outbox::{
    Claim, ClaimRequest, ClaimToken, DeadLetterBatch, DeadLetterCursor, DeadLetterQuery,
    DeadReason, EnqueueOptions, FailureAction, FailureRecord, OutboxDeadLetters, OutboxEnqueue,
    OutboxMaintenance, OutboxPurgeRequest, OutboxStore,
};
use sisa_messaging_postgres::{PostgresError, PostgresInboxStore, PostgresOutboxStore};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Barrier;
use uuid::Uuid;

#[tokio::test]
async fn purge_expires_a_stale_crash_lease_and_clears_its_fence() {
    let pool = isolated_outbox_pool().await;
    let id = sqlx::query_scalar!(
        r#"
            -- A stale lease remains eligible for expiry even while its token remains persisted.
            INSERT INTO outbox_messages (
                id, message_id, message_type, message_version, content_type, payload, metadata,
                created_at, claimable_at, expires_at, claim_token, locked_by, attempts
            )
            VALUES (
                uuidv7(), uuidv7(), 'postgres.stale-lease', 1, 'application/test',
                ''::bytea, '{}'::jsonb, now(), now() - interval '1 second',
                now() - interval '1 second', uuidv7(), 'crashed-worker', 0
            )
            RETURNING id
        "#
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("stale lease setup failed"));
    let store = PostgresOutboxStore::new(pool.clone(), ());
    let report = store
        .purge(OutboxPurgeRequest {
            published_retention: Duration::from_secs(86_400),
            dead_retention: Duration::from_secs(86_400),
            batch_size: NonZeroU32::MIN,
        })
        .await
        .unwrap_or_else(|_| panic!("stale lease purge failed"));
    assert_eq!(report.expired, 1);
    let row = outbox_record(&pool, OutboxLookupParams::by_id(id))
        .await
        .unwrap_or_else(|| panic!("stale lease lookup failed"));
    assert_eq!(row.dead_reason.as_deref(), Some("expired"));
    assert_eq!(row.claim_token, None);
    assert_eq!(row.locked_by, None);
}

#[tokio::test]
async fn claim_returns_healthy_rows_and_marks_a_poison_row_dead() {
    let pool = isolated_outbox_pool().await;
    insert_outbox_row(&pool, "postgres.healthy").await;
    let poison_id = insert_outbox_row(&pool, "").await;
    let store = PostgresOutboxStore::new(pool.clone(), ());
    let batch = store
        .claim(ClaimRequest {
            worker_id: "postgres-test".into(),
            limit: NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("mixed claim failed"));
    assert_eq!(
        (
            batch.records.len(),
            batch.poison.observed,
            batch.poison.marked_dead
        ),
        (1, 1, 1)
    );
    let dead = outbox_record(&pool, OutboxLookupParams::by_id(poison_id))
        .await
        .unwrap_or_else(|| panic!("poison lookup failed"));
    assert_eq!(dead.dead_reason.as_deref(), Some("undecodable"));
    assert!(dead.claim_token.is_none() && dead.locked_by.is_none());
}

#[tokio::test]
async fn claim_keeps_healthy_rows_when_the_poison_follow_up_fails() {
    let pool = isolated_outbox_pool().await;
    insert_outbox_row(&pool, "postgres.healthy-after-poison-error").await;
    insert_outbox_row(&pool, "").await;
    sqlx::query!(
        r#"
            -- One temporary trigger makes only post-claim poison cleanup fail in this fixture.
            DO $$
            BEGIN
                CREATE FUNCTION pg_temp.fail_poison_transition()
                RETURNS trigger LANGUAGE plpgsql AS $fn$
                BEGIN
                    IF NEW.dead_at IS NOT NULL THEN
                        RAISE EXCEPTION 'poison transition rejected' USING ERRCODE = '57014';
                    END IF;
                    RETURN NEW;
                END
                $fn$;
                CREATE TRIGGER fail_poison_transition BEFORE UPDATE ON outbox_messages
                FOR EACH ROW EXECUTE FUNCTION pg_temp.fail_poison_transition();
            END
            $$
        "#
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("trigger setup failed"));
    let store = PostgresOutboxStore::new(pool, ());
    let batch = store
        .claim(ClaimRequest {
            worker_id: "postgres-test".into(),
            limit: NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("claim should survive poison error"));
    assert_eq!(
        (
            batch.records.len(),
            batch.poison.observed,
            batch.poison.marked_dead
        ),
        (1, 1, 0)
    );
}

#[tokio::test]
async fn stats_excludes_a_due_successor_and_clamps_a_future_created_age() {
    let pool = isolated_outbox_pool().await;
    sqlx::query!(
        r#"
            -- An expired predecessor blocks its successor while the predecessor lease is current.
            INSERT INTO outbox_messages (
                id, message_id, message_type, message_version, content_type, payload, metadata,
                ordering_key, created_at, claimable_at, expires_at, claim_token, locked_by,
                attempts
            )
            VALUES
                (
                    '00000000-0000-0000-0000-000000000001', uuidv7(),
                    'postgres.ordering-head', 1, 'application/test', ''::bytea, '{}'::jsonb,
                    'postgres-test-key', now() - interval '2 hours', now() + interval '1 hour',
                    now() - interval '1 hour', uuidv7(), 'postgres-test-worker', 0
                ),
                (
                    '00000000-0000-0000-0000-000000000002', uuidv7(),
                    'postgres.ordering-successor', 1, 'application/test', ''::bytea,
                    '{}'::jsonb, 'postgres-test-key', now() - interval '2 hours',
                    now() - interval '1 hour', NULL, NULL, NULL, 0
                ),
                -- Clock skew is schema-valid; the derived finite negative age clamps to zero.
                (
                    '00000000-0000-0000-0000-000000000003', uuidv7(),
                    'postgres.future-created-at', 1, 'application/test', ''::bytea,
                    '{}'::jsonb, NULL, now() + interval '1 hour', now(), NULL, NULL, NULL, 0
                )
        "#
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("ordering stats setup failed"));
    let stats = PostgresOutboxStore::new(pool, ())
        .stats()
        .await
        .unwrap_or_else(|_| panic!("stats failed"));
    assert_eq!((stats.pending, stats.expired), (2, 1));
    // A schema-valid clock-skewed row must not make maintenance fail.
    assert_eq!(stats.oldest_pending_age, Duration::ZERO);
}

#[tokio::test]
async fn stats_rejects_a_non_finite_database_derived_age() {
    let pool = isolated_outbox_pool().await;
    sqlx::query!(
        r#"
            -- PostgreSQL permits infinity timestamps, but their derived age is not a Duration.
            INSERT INTO outbox_messages (
                id,
                message_id,
                message_type,
                message_version,
                content_type,
                payload,
                metadata,
                created_at,
                claimable_at,
                attempts
            )
            VALUES (
                uuidv7(),
                uuidv7(),
                'postgres.infinite-age',
                1,
                'application/test',
                ''::bytea,
                '{}'::jsonb,
                '-infinity',
                now(),
                0
            )
        "#
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("infinite age fixture setup failed"));
    let result = PostgresOutboxStore::new(pool, ()).stats().await;
    assert!(matches!(result, Err(PostgresError::InvalidData)));
}

#[tokio::test]
async fn purge_bounds_each_maintenance_phase_and_stats_observes_remaining_states() {
    let pool = isolated_outbox_pool().await;
    sqlx::query!(
        r#"
            -- Two old rows per phase prove batch one leaves one matching row for every phase.
            INSERT INTO outbox_messages (
                id, message_id, message_type, message_version, content_type, payload, metadata,
                created_at, claimable_at, expires_at, published_at, dead_at, dead_reason
            )
            VALUES
                (uuidv7(), uuidv7(), 'postgres.maintenance-expire', 1, 'application/test',
                 ''::bytea, '{}'::jsonb, now(), now(), now() - interval '1 minute', NULL,
                 NULL, NULL),
                (uuidv7(), uuidv7(), 'postgres.maintenance-expire-remaining', 1,
                 'application/test', ''::bytea, '{}'::jsonb, now(), now(),
                 now() - interval '1 minute', NULL, NULL, NULL),
                (uuidv7(), uuidv7(), 'postgres.maintenance-published', 1, 'application/test',
                 ''::bytea, '{}'::jsonb, now(), now(), NULL, now() - interval '1 minute',
                 NULL, NULL),
                (uuidv7(), uuidv7(), 'postgres.maintenance-published-remaining', 1,
                 'application/test', ''::bytea, '{}'::jsonb, now(), now(), NULL,
                 now() - interval '1 minute', NULL, NULL),
                (uuidv7(), uuidv7(), 'postgres.maintenance-dead', 1, 'application/test',
                 ''::bytea, '{}'::jsonb, now(), now(), NULL, NULL,
                 now() - interval '1 minute', 'permanent'),
                (uuidv7(), uuidv7(), 'postgres.maintenance-dead-remaining', 1,
                 'application/test', ''::bytea, '{}'::jsonb, now(), now(), NULL, NULL,
                 now() - interval '1 minute', 'permanent'),
                (uuidv7(), uuidv7(), 'postgres.maintenance-pending', 1, 'application/test',
                 ''::bytea, '{}'::jsonb, now() - interval '1 minute', now(), NULL, NULL,
                 NULL, NULL)
        "#
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("maintenance fixture setup failed"));
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let report = store
        .purge(OutboxPurgeRequest {
            published_retention: Duration::from_secs(1),
            dead_retention: Duration::from_secs(1),
            batch_size: NonZeroU32::MIN,
        })
        .await
        .unwrap_or_else(|_| panic!("bounded maintenance failed"));
    assert_eq!(
        (
            report.expired,
            report.published_deleted,
            report.dead_deleted
        ),
        (1, 1, 1)
    );
    let stats = store
        .stats()
        .await
        .unwrap_or_else(|_| panic!("stats failed"));
    assert_eq!((stats.pending, stats.expired, stats.dead), (1, 1, 2));
    assert!(stats.oldest_pending_age >= Duration::from_secs(50));
}

#[tokio::test]
async fn dead_letters_page_exclusively_retry_preserves_identity_and_delete_returns_dead_matches() {
    let pool = isolated_outbox_pool().await;
    sqlx::query!(
        r#"
            -- Fixed death order makes the exclusive cursor boundary observable.
            INSERT INTO outbox_messages (
                id, message_id, message_type, message_version, content_type, payload, metadata,
                created_at, claimable_at, expires_at, attempts, dead_at, dead_reason, last_error
            )
            VALUES
                ('00000000-0000-0000-0000-000000000201',
                 '00000000-0000-0000-0000-000000000301', 'postgres.dead-first', 1,
                 'application/test', ''::bytea, '{}'::jsonb, now(), now(),
                 now() - interval '1 hour', 4, now() - interval '3 minutes', 'permanent',
                 'safe test error'),
                ('00000000-0000-0000-0000-000000000202',
                 '00000000-0000-0000-0000-000000000302', 'postgres.dead-second', 1,
                 'application/test', ''::bytea, '{}'::jsonb, now(), now(), NULL, 2,
                 now() - interval '2 minutes', 'exhausted', 'safe test error'),
                ('00000000-0000-0000-0000-000000000203',
                 '00000000-0000-0000-0000-000000000303', 'postgres.live', 1,
                 'application/test', ''::bytea, '{}'::jsonb, now(),
                 now() + interval '1 hour', NULL, 0, NULL, NULL, NULL)
        "#
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("dead-letter fixture setup failed"));
    let first_id = Uuid::from_u128(0x201);
    let first_message_id = Uuid::from_u128(0x301);
    let second_id = Uuid::from_u128(0x202);
    let live_id = Uuid::from_u128(0x203);
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let first = store
        .list(DeadLetterQuery {
            after: None,
            limit: NonZeroU32::MIN,
        })
        .await
        .unwrap_or_else(|_| panic!("first dead-letter page failed"));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].id.into_uuid(), first_id);
    let second = store
        .list(DeadLetterQuery {
            after: Some(DeadLetterCursor {
                dead_at: first[0].dead_at,
                id: first[0].id,
            }),
            limit: NonZeroU32::MIN,
        })
        .await
        .unwrap_or_else(|_| panic!("second dead-letter page failed"));
    assert_eq!(
        second
            .iter()
            .map(|row| row.id.into_uuid())
            .collect::<Vec<_>>(),
        vec![second_id]
    );
    let retried = store
        .retry(
            DeadLetterBatch::new(&[first[0].id]).unwrap_or_else(|_| panic!("retry batch failed")),
        )
        .await
        .unwrap_or_else(|_| panic!("dead-letter retry failed"));
    assert_eq!(retried, vec![first[0].id]);
    let revived = outbox_record(&pool, OutboxLookupParams::by_id(first_id))
        .await
        .unwrap_or_else(|| panic!("revived dead-letter lookup failed"));
    assert_eq!(
        (revived.id, revived.message_id, revived.attempts),
        (first_id, first_message_id, 0)
    );
    assert!(
        revived.dead_at.is_none() && revived.dead_reason.is_none() && revived.last_error.is_none()
    );
    assert!(revived.claim_token.is_none() && revived.locked_by.is_none());
    let reclaimed = store
        .claim(ClaimRequest {
            worker_id: "postgres-retry-expired".into(),
            limit: NonZeroU32::MIN,
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("expired dead-letter re-claim failed"));
    assert_eq!(reclaimed.records.len(), 1);
    assert_eq!(reclaimed.records[0].claim.id.into_uuid(), first_id);
    assert_eq!(
        reclaimed.records[0].envelope.message_id.into_uuid(),
        first_message_id
    );
    assert_eq!(reclaimed.records[0].envelope.metadata, Metadata::default());
    let deleted = store
        .delete(
            DeadLetterBatch::new(&[
                sisa_messaging_outbox::OutboxId::from_uuid(second_id),
                sisa_messaging_outbox::OutboxId::from_uuid(live_id),
            ])
            .unwrap_or_else(|_| panic!("delete batch failed")),
        )
        .await
        .unwrap_or_else(|_| panic!("dead-letter delete failed"));
    assert_eq!(
        deleted,
        vec![sisa_messaging_outbox::OutboxId::from_uuid(second_id)]
    );
}

#[derive(Debug)]
struct SentinelDatabaseError(&'static str);

impl fmt::Display for SentinelDatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("postgres-secret-sentinel")
    }
}

impl Error for SentinelDatabaseError {}

impl sqlx::error::DatabaseError for SentinelDatabaseError {
    fn message(&self) -> &str {
        "postgres-secret-sentinel"
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.0))
    }

    fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> sqlx::error::ErrorKind {
        sqlx::error::ErrorKind::Other
    }
}

#[test]
fn postgres_error_redacts_database_diagnostics_but_retains_the_source() {
    let error = PostgresError::from(sqlx::Error::Database(Box::new(SentinelDatabaseError(
        "55P03",
    ))));
    assert_eq!(error.to_string(), "database operation failed");
    assert_eq!(format!("{error:?}"), "Database");
    assert!(!error.to_string().contains("postgres-secret-sentinel"));
    assert!(!format!("{error:?}").contains("postgres-secret-sentinel"));
    let source = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<sqlx::Error>())
        .unwrap_or_else(|| panic!("database error must retain the SQLx source"));
    assert_eq!(
        source
            .as_database_error()
            .and_then(|database| database.code())
            .as_deref(),
        Some("55P03")
    );
    assert_eq!(error.classify(), FailureKind::Transient);
}

#[test]
fn postgres_error_redacts_permanent_driver_diagnostics() {
    let error = PostgresError::from(sqlx::Error::Protocol("postgres-secret-sentinel".into()));
    assert_eq!(error.to_string(), "database operation failed");
    assert_eq!(format!("{error:?}"), "Database");
    assert!(!error.to_string().contains("postgres-secret-sentinel"));
    assert!(!format!("{error:?}").contains("postgres-secret-sentinel"));
    let source = std::error::Error::source(&error);
    assert!(source.is_some_and(|source| source.is::<sqlx::Error>()));
    assert_eq!(error.classify(), FailureKind::Permanent);
}

#[test]
fn postgres_error_classifies_sqlx_variants_without_message_parsing() {
    let transient_states = [
        "08006", "40001", "40P01", "53100", "55P03", "57014", "57P01",
    ];
    for state in transient_states {
        let error = PostgresError::from(sqlx::Error::Database(Box::new(SentinelDatabaseError(
            state,
        ))));
        assert!(matches!(error, PostgresError::Database { .. }));
        assert_eq!(error.classify(), FailureKind::Transient, "state {state}");
    }

    for error in [
        sqlx::Error::PoolTimedOut,
        sqlx::Error::Io(std::io::Error::other("transient sentinel")),
        sqlx::Error::WorkerCrashed,
        sqlx::Error::BeginFailed,
    ] {
        let error = PostgresError::from(error);
        assert!(matches!(error, PostgresError::Database { .. }));
        assert_eq!(error.classify(), FailureKind::Transient);
    }

    let permanent_database: Vec<sqlx::Error> = vec![
        sqlx::Error::Database(Box::new(SentinelDatabaseError("23503"))),
        sqlx::Error::Database(Box::new(SentinelDatabaseError("40002"))),
        sqlx::Error::Database(Box::new(SentinelDatabaseError("40003"))),
        sqlx::Error::Configuration(Box::new(std::io::Error::other("configuration sentinel"))),
        sqlx::Error::Tls(Box::new(std::io::Error::other("tls sentinel"))),
        sqlx::Error::Protocol("protocol sentinel".into()),
        sqlx::Error::PoolClosed,
    ];
    for error in permanent_database {
        let error = PostgresError::from(error);
        assert!(matches!(error, PostgresError::Database { .. }));
        assert_eq!(error.classify(), FailureKind::Permanent);
        assert_eq!(error.to_string(), "database operation failed");
        assert_eq!(format!("{error:?}"), "Database");
        let source = std::error::Error::source(&error);
        assert!(source.is_some_and(|source| source.is::<sqlx::Error>()));
    }

    let permanent_structured: Vec<sqlx::Error> = vec![
        sqlx::Error::InvalidArgument("invalid argument sentinel".into()),
        sqlx::Error::RowNotFound,
        sqlx::Error::TypeNotFound {
            type_name: "missing_type".into(),
        },
        sqlx::Error::ColumnIndexOutOfBounds { index: 1, len: 0 },
        sqlx::Error::ColumnNotFound("missing_column".into()),
        sqlx::Error::ColumnDecode {
            index: "decode_column".into(),
            source: Box::new(std::io::Error::other("decode sentinel")),
        },
        sqlx::Error::Encode(Box::new(std::io::Error::other("encode sentinel"))),
        sqlx::Error::Decode(Box::new(std::io::Error::other("decode sentinel"))),
        sqlx::Error::AnyDriverError(Box::new(std::io::Error::other("any sentinel"))),
        sqlx::Error::InvalidSavePointStatement,
    ];
    for error in permanent_structured {
        let error = PostgresError::from(error);
        assert!(matches!(error, PostgresError::Database { .. }));
        assert_eq!(error.classify(), FailureKind::Permanent);
        assert_eq!(error.to_string(), "database operation failed");
        assert_eq!(format!("{error:?}"), "Database");
        let source = std::error::Error::source(&error);
        assert!(source.is_some_and(|source| source.is::<sqlx::Error>()));
    }
}

#[test]
fn postgres_error_is_static_send_and_sync() {
    fn assert_error<T: std::error::Error + Send + Sync + 'static>() {}

    assert_error::<PostgresError>();
}

#[tokio::test]
async fn outbox_duration_bounds_fail_before_closed_pool_io() {
    let pool = PgPoolOptions::new().connect_lazy_with(crate::support::connect_options());
    pool.close().await;
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let inbox = PostgresInboxStore::new(
        pool,
        InboxSettings::new(NonZeroU32::MIN)
            .unwrap_or_else(|_| panic!("inbox duration settings rejected")),
    );
    let claim = Claim {
        id: sisa_messaging_outbox::OutboxId::from_uuid(Uuid::now_v7()),
        token: ClaimToken::from_uuid(Uuid::now_v7()),
    };
    let too_large = Duration::from_secs(i32::MAX as u64 + 1);
    for result in [
        store
            .claim(ClaimRequest {
                worker_id: "closed-pool-duration".into(),
                limit: NonZeroU32::MIN,
                lease: too_large,
            })
            .await
            .map(|_| ()),
        store
            .fail(&[FailureRecord {
                claim,
                failure_kind: FailureKind::Transient,
                error: ErrorSummary::from_safe_text("too-large retry delay"),
                action: FailureAction::Retry { delay: too_large },
            }])
            .await
            .map(|_| ()),
        store.extend_lease(&[claim], too_large).await.map(|_| ()),
        store
            .purge(OutboxPurgeRequest {
                published_retention: too_large,
                dead_retention: Duration::ZERO,
                batch_size: NonZeroU32::MIN,
            })
            .await
            .map(|_| ()),
    ] {
        assert!(matches!(result, Err(PostgresError::InvalidData)));
    }
    let exact_limit = Duration::from_secs(i32::MAX as u64);
    let result = store
        .claim(ClaimRequest {
            worker_id: "closed-pool-exact-duration".into(),
            limit: NonZeroU32::MIN,
            lease: exact_limit,
        })
        .await;
    assert!(matches!(result, Err(PostgresError::Database { .. })));
    let duration_max = store.extend_lease(&[claim], Duration::MAX).await;
    assert!(matches!(duration_max, Err(PostgresError::InvalidData)));

    for request in [
        InboxPurgeRequest {
            completed_retention: Some(too_large),
            dead_retention: None,
            batch_size: NonZeroU32::MIN,
        },
        InboxPurgeRequest {
            completed_retention: None,
            dead_retention: Some(Duration::MAX),
            batch_size: NonZeroU32::MIN,
        },
    ] {
        assert!(matches!(
            inbox.purge(request).await,
            Err(PostgresError::InvalidData)
        ));
    }
    let inbox_exact_limit = inbox
        .purge(InboxPurgeRequest {
            completed_retention: Some(exact_limit),
            dead_retention: Some(Duration::ZERO),
            batch_size: NonZeroU32::MIN,
        })
        .await;
    assert!(matches!(
        inbox_exact_limit,
        Err(PostgresError::Database { .. })
    ));
}

#[tokio::test]
async fn enqueue_commits_a_serialized_envelope() {
    let pool = isolated_outbox_pool().await;
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let message_id = MessageId::new();
    let mut transaction = pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("enqueue transaction start failed"));
    let outbox_id = store
        .enqueue(
            &mut transaction,
            &test_envelope(message_id, Metadata::default()),
            EnqueueOptions::default(),
        )
        .await
        .unwrap_or_else(|_| panic!("enqueue failed"));
    transaction
        .commit()
        .await
        .unwrap_or_else(|_| panic!("enqueue transaction commit failed"));
    let row = outbox_record(&pool, OutboxLookupParams::by_id(outbox_id.into_uuid()))
        .await
        .unwrap_or_else(|| panic!("committed enqueue lookup failed"));
    assert_eq!(row.message_id, message_id.into_uuid());
    assert_eq!(row.message_type, TestMessage::TYPE);
}

#[tokio::test]
async fn enqueue_rollback_leaves_no_durable_row() {
    let pool = isolated_outbox_pool().await;
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let message_id = MessageId::new();
    let mut transaction = pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("rollback transaction start failed"));
    store
        .enqueue(
            &mut transaction,
            &test_envelope(message_id, Metadata::default()),
            EnqueueOptions::default(),
        )
        .await
        .unwrap_or_else(|_| panic!("enqueue before rollback failed"));
    transaction
        .rollback()
        .await
        .unwrap_or_else(|_| panic!("enqueue transaction rollback failed"));
    assert!(
        outbox_record(
            &pool,
            OutboxLookupParams::by_message_id(message_id.into_uuid())
        )
        .await
        .is_none()
    );
}

#[tokio::test]
async fn outbox_rejects_out_of_range_system_times_before_running_sql() {
    let pool = isolated_outbox_pool().await;
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let out_of_range = out_of_chrono_range_system_time();
    let mut transaction = pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("out-of-range enqueue transaction start failed"));
    let enqueue = store
        .enqueue(
            &mut transaction,
            &test_envelope(MessageId::new(), Metadata::default()),
            EnqueueOptions {
                expires_at: Some(out_of_range),
            },
        )
        .await;
    assert!(matches!(enqueue, Err(PostgresError::InvalidData)));
    transaction
        .rollback()
        .await
        .unwrap_or_else(|_| panic!("out-of-range enqueue transaction rollback failed"));

    let cursor = sisa_messaging_outbox::DeadLetterCursor {
        dead_at: out_of_range,
        id: sisa_messaging_outbox::OutboxId::from_uuid(Uuid::nil()),
    };
    let list = store
        .list(DeadLetterQuery {
            after: Some(cursor),
            limit: NonZeroU32::MIN,
        })
        .await;
    assert!(matches!(list, Err(PostgresError::InvalidData)));
}

fn out_of_chrono_range_system_time() -> SystemTime {
    match SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(200_000_000_000_000)) {
        Some(value) => value,
        None => panic!("test platform cannot represent the intended out-of-range timestamp"),
    }
}

#[tokio::test]
async fn enqueue_duplicate_message_identity_aborts_and_maps_the_postgres_error() {
    let pool = isolated_outbox_pool().await;
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let message_id = MessageId::new();
    let envelope = test_envelope(message_id, Metadata::default());
    let mut first = pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("first duplicate transaction start failed"));
    store
        .enqueue(&mut first, &envelope, EnqueueOptions::default())
        .await
        .unwrap_or_else(|_| panic!("first duplicate enqueue failed"));
    first
        .commit()
        .await
        .unwrap_or_else(|_| panic!("first duplicate transaction commit failed"));
    let mut duplicate = pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("duplicate transaction start failed"));
    let error = store
        .enqueue(&mut duplicate, &envelope, EnqueueOptions::default())
        .await
        .expect_err("duplicate message identity must fail");
    assert!(matches!(error, PostgresError::DuplicateMessageId { .. }));
    assert_eq!(error.classify(), FailureKind::Permanent);
    assert_eq!(error.to_string(), "duplicate outbox message identity");
    assert_eq!(format!("{error:?}"), "DuplicateMessageId");
    let source = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<sqlx::Error>())
        .unwrap_or_else(|| panic!("duplicate error must retain the SQLx source"));
    assert_eq!(
        source
            .as_database_error()
            .and_then(|database| database.code())
            .as_deref(),
        Some("23505")
    );
    let aborted = sqlx::query_scalar!(
        r#"
            -- PostgreSQL rejects later work in the caller-owned transaction after the duplicate.
            SELECT 1 AS "value!"
        "#
    )
    .fetch_one(&mut *duplicate)
    .await
    .expect_err("duplicate enqueue must abort the caller-owned transaction");
    assert_eq!(
        aborted
            .as_database_error()
            .and_then(|database| database.code())
            .as_deref(),
        Some("25P02")
    );
    duplicate
        .rollback()
        .await
        .unwrap_or_else(|_| panic!("duplicate transaction rollback failed"));
}

#[tokio::test]
async fn claim_round_trips_envelope_metadata() {
    let pool = isolated_outbox_pool().await;
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let message_id = MessageId::new();
    let mut headers = Headers::new();
    headers
        .insert(
            HeaderName::new("x-test-id").unwrap_or_else(|_| panic!("header name failed")),
            HeaderValue::new("metadata-round-trip")
                .unwrap_or_else(|_| panic!("header value failed")),
        )
        .unwrap_or_else(|_| panic!("header insert failed"));
    let metadata = Metadata {
        routing: RoutingMetadata {
            source: Some(
                MetadataValue::new("postgres-integration")
                    .unwrap_or_else(|_| panic!("metadata value failed")),
            ),
            ..RoutingMetadata::default()
        },
        headers,
        ..Metadata::default()
    };
    let mut transaction = pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("metadata transaction start failed"));
    store
        .enqueue(
            &mut transaction,
            &test_envelope(message_id, metadata.clone()),
            EnqueueOptions::default(),
        )
        .await
        .unwrap_or_else(|_| panic!("metadata enqueue failed"));
    transaction
        .commit()
        .await
        .unwrap_or_else(|_| panic!("metadata transaction commit failed"));
    let batch = store
        .claim(ClaimRequest {
            worker_id: "postgres-metadata-test".into(),
            limit: NonZeroU32::MIN,
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("metadata claim failed"));
    assert_eq!(batch.poison.observed, 0);
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].envelope.message_id, message_id);
    assert_eq!(batch.records[0].envelope.metadata, metadata);
}

#[tokio::test]
async fn claim_marks_malformed_persisted_metadata_as_poison() {
    let pool = isolated_outbox_pool().await;
    let malformed_id = sqlx::query_scalar!(
        r#"
            -- This JSON object passes the table check, but headers must be a metadata map.
            INSERT INTO outbox_messages (
                message_id, message_type, message_version, content_type, payload, metadata
            )
            VALUES (uuidv7(), 'postgres.malformed-metadata', 1, 'application/test', ''::bytea,
                    '{"headers": []}'::jsonb)
            RETURNING id
        "#
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("malformed metadata setup failed"));
    let batch = PostgresOutboxStore::new(pool.clone(), TestSerializer)
        .claim(ClaimRequest {
            worker_id: "postgres-metadata-poison-test".into(),
            limit: NonZeroU32::MIN,
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("malformed metadata claim failed"));
    assert!(batch.records.is_empty());
    assert_eq!((batch.poison.observed, batch.poison.marked_dead), (1, 1));
    let dead = outbox_record(&pool, OutboxLookupParams::by_id(malformed_id))
        .await
        .unwrap_or_else(|| panic!("malformed metadata poison lookup failed"));
    assert_eq!(dead.dead_reason.as_deref(), Some("undecodable"));
    assert!(dead.claim_token.is_none() && dead.locked_by.is_none());
}

#[tokio::test]
async fn concurrent_disjoint_unordered_claims_do_not_overlap() {
    let fixture = isolated_concurrent_outbox_pool().await;
    let pool = fixture.pool.clone();
    let marker = format!("postgres.concurrent-disjoint-{}", Uuid::now_v7());
    let ids = sqlx::query_scalar!(
        r#"
            -- Schema-isolated rows let separate connections exercise SKIP LOCKED concurrently.
            INSERT INTO outbox_messages (
                message_id, message_type, message_version, content_type, payload, metadata
            )
            VALUES (uuidv7(), $1, 1, 'application/test', ''::bytea, '{}'::jsonb),
                   (uuidv7(), $1, 1, 'application/test', ''::bytea, '{}'::jsonb),
                   (uuidv7(), $1, 1, 'application/test', ''::bytea, '{}'::jsonb)
            RETURNING id
        "#,
        marker
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_else(|_| panic!("concurrent claim setup failed"));
    let locked_id = ids[0];
    let mut lock_transaction = pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("concurrent lock transaction start failed"));
    sqlx::query!(
        r#"
            -- The held row lock forces both workers to traverse the SKIP LOCKED branch.
            SELECT id FROM outbox_messages WHERE id = $1 FOR UPDATE
        "#,
        locked_id
    )
    .fetch_one(&mut *lock_transaction)
    .await
    .unwrap_or_else(|_| panic!("concurrent lock acquisition failed"));
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let barrier = Arc::new(Barrier::new(3));
    let first_store = store.clone();
    let first_barrier = barrier.clone();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_store
            .claim(ClaimRequest {
                worker_id: "postgres-concurrent-first".into(),
                limit: NonZeroU32::MIN,
                lease: Duration::from_secs(30),
            })
            .await
    });
    let second_store = store.clone();
    let second_barrier = barrier.clone();
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_store
            .claim(ClaimRequest {
                worker_id: "postgres-concurrent-second".into(),
                limit: NonZeroU32::MIN,
                lease: Duration::from_secs(30),
            })
            .await
    });
    barrier.wait().await;
    let first = first
        .await
        .unwrap_or_else(|_| panic!("first concurrent task failed"))
        .unwrap_or_else(|_| panic!("first concurrent claim failed"));
    let second = second
        .await
        .unwrap_or_else(|_| panic!("second concurrent task failed"))
        .unwrap_or_else(|_| panic!("second concurrent claim failed"));
    assert_eq!((first.records.len(), second.records.len()), (1, 1));
    let mut claimed = first
        .records
        .iter()
        .chain(&second.records)
        .map(|record| record.claim.id.into_uuid())
        .collect::<Vec<_>>();
    claimed.sort_unstable();
    let mut expected = ids
        .into_iter()
        .filter(|id| *id != locked_id)
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(claimed, expected);
    lock_transaction
        .rollback()
        .await
        .unwrap_or_else(|_| panic!("concurrent lock transaction rollback failed"));
    fixture.cleanup().await;
}

#[tokio::test]
async fn claim_serializes_an_ordering_key_head_while_progressing_a_distinct_key() {
    let pool = isolated_outbox_pool().await;
    sqlx::query!(
        r#"
            -- Fixed identities make the same-key predecessor relationship deterministic.
            INSERT INTO outbox_messages (
                id, message_id, message_type, message_version, content_type, payload, metadata,
                ordering_key
            )
            VALUES
                ('00000000-0000-0000-0000-000000000011', uuidv7(), 'postgres.ordering-head',
                 1, 'application/test', ''::bytea, '{}'::jsonb, 'postgres-ordering-key'),
                ('00000000-0000-0000-0000-000000000012', uuidv7(),
                 'postgres.ordering-successor', 1, 'application/test', ''::bytea, '{}'::jsonb,
                 'postgres-ordering-key'),
                ('00000000-0000-0000-0000-000000000013', uuidv7(),
                 'postgres.ordering-independent', 1, 'application/test', ''::bytea,
                 '{}'::jsonb, 'postgres-other-key')
        "#
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("ordering claim setup failed"));
    let batch = PostgresOutboxStore::new(pool, TestSerializer)
        .claim(ClaimRequest {
            worker_id: "postgres-ordering-test".into(),
            limit: NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN),
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("ordering claim failed"));
    let mut types = batch
        .records
        .iter()
        .map(|record| record.envelope.message_type.as_str())
        .collect::<Vec<_>>();
    types.sort_unstable();
    assert_eq!(
        types,
        ["postgres.ordering-head", "postgres.ordering-independent"]
    );
}

#[tokio::test]
async fn claim_keeps_an_expired_currently_leased_predecessor_as_the_ordering_barrier() {
    let pool = isolated_outbox_pool().await;
    let predecessor_id = Uuid::from_u128(0x21);
    let successor_id = Uuid::from_u128(0x22);
    sqlx::query!(
        r#"
            -- The expired predecessor's unexpired lease must still fence its same-key successor.
            INSERT INTO outbox_messages (
                id,
                message_id,
                message_type,
                message_version,
                content_type,
                payload,
                metadata,
                ordering_key,
                claimable_at,
                expires_at,
                claim_token,
                locked_by
            )
            VALUES
                (
                    $1,
                    uuidv7(),
                    'postgres.expired-leased-predecessor',
                    1,
                    'application/test',
                    ''::bytea,
                    '{}'::jsonb,
                    'postgres-expired-lease-key',
                    now() + interval '1 hour',
                    now() - interval '1 minute',
                    uuidv7(),
                    'current-worker'
                ),
                (
                    $2,
                    uuidv7(),
                    'postgres.expired-leased-successor',
                    1,
                    'application/test',
                    ''::bytea,
                    '{}'::jsonb,
                    'postgres-expired-lease-key',
                    now(),
                    NULL,
                    NULL,
                    NULL
                )
        "#,
        predecessor_id,
        successor_id,
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("expired leased ordering fixture setup failed"));
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let blocked = store
        .claim(ClaimRequest {
            worker_id: "postgres-expired-lease-ordering".into(),
            limit: NonZeroU32::MIN,
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("blocked ordering claim failed"));
    assert!(blocked.records.is_empty());

    sqlx::query!(
        r#"
            -- Once the predecessor's lease is stale, its elapsed expiry no longer blocks the key.
            UPDATE outbox_messages
            SET claimable_at = now() - interval '1 second'
            WHERE id = $1
        "#,
        predecessor_id,
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("expired predecessor lease expiry setup failed"));
    let unblocked = store
        .claim(ClaimRequest {
            worker_id: "postgres-expired-lease-ordering".into(),
            limit: NonZeroU32::MIN,
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("unblocked ordering claim failed"));
    assert_eq!(unblocked.records.len(), 1);
    assert_eq!(unblocked.records[0].claim.id.into_uuid(), successor_id);
}

#[tokio::test]
async fn fenced_outcomes_confirm_only_current_claims_and_increment_attempts_selectively() {
    let pool = isolated_outbox_pool().await;
    for (message_type, attempts) in [
        ("postgres.outcome-complete", i32::MAX),
        ("postgres.outcome-fail", i32::MAX),
        ("postgres.outcome-release", 0),
        ("postgres.outcome-renew", 0),
    ] {
        insert_outbox_row_with_attempts(&pool, message_type, attempts).await;
    }
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let batch = store
        .claim(ClaimRequest {
            worker_id: "postgres-outcome-test".into(),
            limit: NonZeroU32::new(4).unwrap_or(NonZeroU32::MIN),
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("outcome claim failed"));
    let claim_for = |message_type| {
        batch
            .records
            .iter()
            .find(|record| record.envelope.message_type.as_str() == message_type)
            .map(|record| record.claim)
            .unwrap_or_else(|| panic!("expected claimed fixture row"))
    };
    let complete = claim_for("postgres.outcome-complete");
    let fail = claim_for("postgres.outcome-fail");
    let release = claim_for("postgres.outcome-release");
    let renew = claim_for("postgres.outcome-renew");
    let stale = |claim: Claim| Claim {
        id: claim.id,
        token: ClaimToken::from_uuid(Uuid::now_v7()),
    };
    let confirmed = store
        .complete(&[complete, stale(fail)])
        .await
        .unwrap_or_else(|_| panic!("complete outcome failed"));
    assert_eq!(confirmed.confirmed, vec![complete]);
    let confirmed = store
        .fail(&[FailureRecord {
            claim: fail,
            failure_kind: FailureKind::Permanent,
            error: ErrorSummary::from_safe_text("terminal failure requested by test"),
            action: FailureAction::Dead {
                reason: DeadReason::Permanent,
            },
        }])
        .await
        .unwrap_or_else(|_| panic!("fail outcome failed"));
    assert_eq!(confirmed.confirmed, vec![fail]);
    let confirmed = store
        .release(&[release, stale(renew)])
        .await
        .unwrap_or_else(|_| panic!("release outcome failed"));
    assert_eq!(confirmed.confirmed, vec![release]);
    let confirmed = store
        .extend_lease(&[renew, stale(complete)], Duration::from_secs(60))
        .await
        .unwrap_or_else(|_| panic!("lease renewal failed"));
    assert_eq!(confirmed.confirmed, vec![renew]);
    let complete_row = outbox_record(&pool, OutboxLookupParams::by_id(complete.id.into_uuid()))
        .await
        .unwrap_or_else(|| panic!("complete outcome lookup failed"));
    let fail_row = outbox_record(&pool, OutboxLookupParams::by_id(fail.id.into_uuid()))
        .await
        .unwrap_or_else(|| panic!("fail outcome lookup failed"));
    let release_row = outbox_record(&pool, OutboxLookupParams::by_id(release.id.into_uuid()))
        .await
        .unwrap_or_else(|| panic!("release outcome lookup failed"));
    let renew_row = outbox_record(&pool, OutboxLookupParams::by_id(renew.id.into_uuid()))
        .await
        .unwrap_or_else(|| panic!("renew outcome lookup failed"));
    assert_eq!(
        (complete_row.message_type.as_str(), complete_row.attempts),
        ("postgres.outcome-complete", i32::MAX)
    );
    assert!(complete_row.published_at.is_some());
    assert_eq!(
        (fail_row.message_type.as_str(), fail_row.attempts),
        ("postgres.outcome-fail", i32::MAX)
    );
    assert_eq!(fail_row.dead_reason.as_deref(), Some("permanent"));
    assert!(fail_row.dead_at.is_some());
    assert_eq!(
        (release_row.message_type.as_str(), release_row.attempts),
        ("postgres.outcome-release", 0)
    );
    assert_eq!(
        (renew_row.message_type.as_str(), renew_row.attempts),
        ("postgres.outcome-renew", 0)
    );
    assert!(renew_row.claim_token.is_some());
}

#[tokio::test]
async fn failure_actions_use_database_time_and_preserve_permanent_and_exhausted_reasons() {
    let pool = isolated_outbox_pool().await;
    for message_type in [
        "postgres.failure-retry",
        "postgres.failure-permanent",
        "postgres.failure-exhausted",
    ] {
        insert_outbox_row(&pool, message_type).await;
    }
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let batch = store
        .claim(ClaimRequest {
            worker_id: "postgres-failure-test".into(),
            limit: NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN),
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("failure claim failed"));
    let claim_for = |message_type| {
        batch
            .records
            .iter()
            .find(|record| record.envelope.message_type.as_str() == message_type)
            .map(|record| record.claim)
            .unwrap_or_else(|| panic!("expected claimed failure row"))
    };
    let failures = [
        FailureRecord {
            claim: claim_for("postgres.failure-retry"),
            failure_kind: FailureKind::Transient,
            error: ErrorSummary::from_safe_text("retry test failure"),
            action: FailureAction::Retry {
                delay: Duration::from_secs(30),
            },
        },
        FailureRecord {
            claim: claim_for("postgres.failure-permanent"),
            failure_kind: FailureKind::Permanent,
            error: ErrorSummary::from_safe_text("permanent test failure"),
            action: FailureAction::Dead {
                reason: DeadReason::Permanent,
            },
        },
        FailureRecord {
            claim: claim_for("postgres.failure-exhausted"),
            failure_kind: FailureKind::Transient,
            error: ErrorSummary::from_safe_text("exhausted test failure"),
            action: FailureAction::Dead {
                reason: DeadReason::Exhausted,
            },
        },
    ];
    let before_transition = outbox_record(
        &pool,
        OutboxLookupParams::by_id(failures[0].claim.id.into_uuid()),
    )
    .await
    .unwrap_or_else(|| panic!("pre-transition database time lookup failed"))
    .observed_at;
    let confirmed = store
        .fail(&failures)
        .await
        .unwrap_or_else(|_| panic!("failure transition failed"));
    assert_eq!(confirmed.confirmed.len(), 3);
    let retry = outbox_record(
        &pool,
        OutboxLookupParams::by_id(failures[0].claim.id.into_uuid()),
    )
    .await
    .unwrap_or_else(|| panic!("retry state lookup failed"));
    let permanent = outbox_record(
        &pool,
        OutboxLookupParams::by_id(failures[1].claim.id.into_uuid()),
    )
    .await
    .unwrap_or_else(|| panic!("permanent state lookup failed"));
    let exhausted = outbox_record(
        &pool,
        OutboxLookupParams::by_id(failures[2].claim.id.into_uuid()),
    )
    .await
    .unwrap_or_else(|| panic!("exhausted state lookup failed"));
    assert_eq!(
        (
            exhausted.message_type.as_str(),
            exhausted.dead_reason.as_deref()
        ),
        ("postgres.failure-exhausted", Some("exhausted"))
    );
    assert_eq!(
        (
            permanent.message_type.as_str(),
            permanent.dead_reason.as_deref()
        ),
        ("postgres.failure-permanent", Some("permanent"))
    );
    assert_eq!(
        (retry.message_type.as_str(), retry.dead_reason.as_deref()),
        ("postgres.failure-retry", None)
    );
    let retry_delay = chrono::Duration::seconds(30);
    assert!(retry.claimable_at >= before_transition + retry_delay);
    assert!(retry.claimable_at <= retry.observed_at + retry_delay);
    assert_eq!(
        (retry.attempts, permanent.attempts, exhausted.attempts),
        (1, 1, 1)
    );
}

#[tokio::test]
async fn renewal_keeps_a_current_lease_safe_while_a_stale_lease_expires() {
    let pool = isolated_outbox_pool().await;
    sqlx::query!(
        r#"
            -- Both fixtures begin with a future deadline before their expiry is forced.
            INSERT INTO outbox_messages (
                message_id, message_type, message_version, content_type, payload, metadata,
                expires_at
            )
            VALUES
                (uuidv7(), 'postgres.renew-current', 1, 'application/test', ''::bytea,
                 '{}'::jsonb, now() + interval '1 hour'),
                (uuidv7(), 'postgres.renew-stale', 1, 'application/test', ''::bytea,
                 '{}'::jsonb, now() + interval '1 hour')
        "#
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("renewal expiry setup failed"));
    let store = PostgresOutboxStore::new(pool.clone(), TestSerializer);
    let batch = store
        .claim(ClaimRequest {
            worker_id: "postgres-renewal-test".into(),
            limit: NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap_or_else(|_| panic!("renewal claim failed"));
    let current = batch
        .records
        .iter()
        .find(|record| record.envelope.message_type.as_str() == "postgres.renew-current")
        .map(|record| record.claim)
        .unwrap_or_else(|| panic!("current renewal claim missing"));
    let pre_renewal_deadline =
        outbox_record(&pool, OutboxLookupParams::by_id(current.id.into_uuid()))
            .await
            .unwrap_or_else(|| panic!("pre-renewal deadline lookup failed"))
            .claimable_at;
    let confirmed = store
        .extend_lease(&[current], Duration::from_secs(60))
        .await
        .unwrap_or_else(|_| panic!("current lease renewal failed"));
    assert_eq!(confirmed.confirmed, vec![current]);
    let renewed_deadline = outbox_record(&pool, OutboxLookupParams::by_id(current.id.into_uuid()))
        .await
        .unwrap_or_else(|| panic!("renewed deadline lookup failed"))
        .claimable_at;
    assert!(renewed_deadline > pre_renewal_deadline);
    sqlx::query!(
        r#"
            -- Database-time control makes one unrenewed lease stale without a process sleep.
            UPDATE outbox_messages
            SET
                expires_at = now() - interval '1 second',
                claimable_at = CASE
                    WHEN message_type = 'postgres.renew-stale' THEN now() - interval '1 second'
                    ELSE claimable_at
                END
        "#
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("renewal expiry transition setup failed"));
    let report = store
        .purge(OutboxPurgeRequest {
            published_retention: Duration::from_secs(86_400),
            dead_retention: Duration::from_secs(86_400),
            batch_size: NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
        })
        .await
        .unwrap_or_else(|_| panic!("renewal expiry purge failed"));
    assert_eq!(report.expired, 1);
    let current_row = outbox_record(&pool, OutboxLookupParams::by_id(current.id.into_uuid()))
        .await
        .unwrap_or_else(|| panic!("current renewal lookup failed"));
    let stale_row = outbox_record(
        &pool,
        OutboxLookupParams::by_message_id(
            batch
                .records
                .iter()
                .find(|record| record.envelope.message_type.as_str() == "postgres.renew-stale")
                .unwrap_or_else(|| panic!("stale renewal claim missing"))
                .envelope
                .message_id
                .into_uuid(),
        ),
    )
    .await
    .unwrap_or_else(|| panic!("stale renewal lookup failed"));
    assert_eq!(current_row.message_type, "postgres.renew-current");
    assert_eq!(current_row.dead_reason, None);
    assert_eq!(current_row.claim_token, Some(current.token.into_uuid()));
    assert_eq!(stale_row.message_type, "postgres.renew-stale");
    assert_eq!(stale_row.dead_reason.as_deref(), Some("expired"));
    assert_eq!(stale_row.claim_token, None);
}
