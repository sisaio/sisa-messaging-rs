use std::{
    num::NonZeroU32,
    sync::OnceLock,
    time::{Duration, SystemTime},
};

use sisa_messaging::{ErrorSummary, FailureKind, MessageId, MessageType, Metadata};
use sisa_messaging_inbox::{
    DeadLetterBatch, DeadLetterCursor, DeadLetterQuery, DeadLetterRecord, DeadReason,
    InboxClaimOutcome, InboxDeadLetters, InboxFailure, InboxFailureOutcome, InboxId,
    InboxMaintenance, InboxPurgeRequest, InboxReceipt, InboxRecord, InboxScope, InboxSettings,
    InboxStore, InboxUnitOfWork,
};
use sisa_messaging_postgres::{PostgresError, PostgresInboxStore};
use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

use crate::support::pool;

fn record(scope: &str, id: u128) -> InboxRecord {
    InboxRecord {
        scope: InboxScope::new(test_scope(scope))
            .unwrap_or_else(|_| panic!("valid scope rejected")),
        message_id: MessageId::from_uuid(Uuid::from_u128(id)),
        message_type: MessageType::new("postgres.inbox-test")
            .unwrap_or_else(|_| panic!("valid type rejected")),
        version: 1,
        metadata: Metadata::default(),
    }
}

fn test_scope(scope: &str) -> String {
    static RUN: OnceLock<String> = OnceLock::new();
    let run = RUN.get_or_init(|| Uuid::now_v7().simple().to_string());

    format!("{scope}-{run}")
}

fn failure(kind: FailureKind) -> InboxFailure {
    failure_with_error(kind, "safe test failure")
}

fn failure_with_error(kind: FailureKind, error: &str) -> InboxFailure {
    InboxFailure {
        kind,
        error: ErrorSummary::from_safe_text(error),
    }
}

fn out_of_chrono_range_system_time() -> SystemTime {
    match SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(200_000_000_000_000)) {
        Some(value) => value,
        None => panic!("test platform cannot represent the intended out-of-range timestamp"),
    }
}

fn store(pool: sqlx::PgPool, attempts: u32) -> PostgresInboxStore {
    PostgresInboxStore::new(
        pool,
        InboxSettings::new(NonZeroU32::new(attempts).unwrap_or(NonZeroU32::MIN))
            .unwrap_or_else(|_| panic!("settings rejected")),
    )
}

struct InboxLookupParams {
    id: Option<Uuid>,

    scope: Option<String>,

    message_id: Option<Uuid>,
}

impl InboxLookupParams {
    fn by_id(id: Uuid) -> Self {
        Self {
            id: Some(id),
            scope: None,
            message_id: None,
        }
    }

    fn by_identity(scope: &str, message_id: Uuid) -> Self {
        Self {
            id: None,
            scope: Some(scope.to_owned()),
            message_id: Some(message_id),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InboxReceiptRecord {
    id: Uuid,

    scope: String,

    message_id: Uuid,

    message_type: String,

    message_version: i32,

    metadata: serde_json::Value,

    attempts: i32,

    received_at: chrono::DateTime<chrono::Utc>,

    completed_at: Option<chrono::DateTime<chrono::Utc>>,

    dead_at: Option<chrono::DateTime<chrono::Utc>>,

    dead_reason: Option<String>,

    last_error: Option<String>,
}

async fn inbox_record(pool: &PgPool, params: InboxLookupParams) -> Option<InboxReceiptRecord> {
    sqlx::query_as!(
        InboxReceiptRecord,
        r#"
            -- Plan assertions observe one provider-owned durable receipt by its opaque handle.
            SELECT
                id AS "id!",
                scope AS "scope!",
                message_id AS "message_id!",
                message_type AS "message_type!",
                message_version AS "message_version!",
                metadata AS "metadata!",
                attempts AS "attempts!",
                received_at AS "received_at!",
                completed_at,
                dead_at,
                dead_reason,
                last_error
            FROM inbox_receipts
            WHERE (id = $1 AND $2::text IS NULL AND $3::uuid IS NULL)
               OR (scope = $2 AND message_id = $3 AND $1::uuid IS NULL)
        "#,
        params.id,
        params.scope,
        params.message_id,
    )
    .fetch_optional(pool)
    .await
    .unwrap_or_else(|_| panic!("inbox receipt lookup failed"))
}

struct InboxFixture {
    pool: PgPool,

    schema: String,
}

impl InboxFixture {
    async fn cleanup(self) {
        self.pool.close().await;
        let control = pool().await;

        // The generated identifier contains only a fixed prefix and UUID hex digits.
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&control)
        .await
        .unwrap_or_else(|_| panic!("inbox schema cleanup failed"));
    }
}

async fn isolated_inbox_fixture() -> InboxFixture {
    let control = pool().await;
    let schema = format!("inbox_test_{}", Uuid::now_v7().simple());

    // PostgreSQL identifiers cannot be parameters; this UUID-derived schema name is safe.
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&control)
        .await
        .unwrap_or_else(|_| panic!("inbox schema setup failed"));

    sqlx::query(AssertSqlSafe(format!(
        "CREATE TABLE {schema}.inbox_receipts (LIKE public.inbox_receipts INCLUDING ALL)"
    )))
    .execute(&control)
    .await
    .unwrap_or_else(|_| panic!("inbox table setup failed"));

    for (source, target) in [
        ("inbox_receipts_pkey", "pk_inbox_receipts"),
        (
            "inbox_receipts_scope_message_id_idx",
            "ix_inbox_receipts_scope_message_id",
        ),
        (
            "inbox_receipts_completed_at_idx",
            "ix_inbox_receipts_completed",
        ),
        ("inbox_receipts_dead_at_id_idx", "ix_inbox_receipts_dead"),
    ] {
        sqlx::query(AssertSqlSafe(format!(
            "ALTER INDEX {schema}.{source} RENAME TO {target}"
        )))
        .execute(&control)
        .await
        .unwrap_or_else(|_| panic!("inbox index setup failed"));
    }

    let search_path = format!("{schema}, public");

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(move |connection, _| {
            let search_path = search_path.clone();

            Box::pin(async move {
                sqlx::query!(
                    r#"
                        -- Every pool connection resolves the provider table name to this schema.
                        SELECT set_config('search_path', $1, false) AS "search_path!"
                    "#,
                    search_path
                )
                .fetch_one(connection)
                .await
                .map(|_| ())
            })
        })
        .connect_with(crate::support::connect_options())
        .await
        .unwrap_or_else(|_| panic!("isolated inbox PostgreSQL connection failed"));

    InboxFixture { pool, schema }
}

#[tokio::test]
async fn inbox_dead_letter_cursor_rejects_an_out_of_range_system_time_before_sql() {
    let fixture = isolated_inbox_fixture().await;
    let store = store(fixture.pool.clone(), 2);

    let list = store
        .list(DeadLetterQuery {
            after: Some(DeadLetterCursor {
                dead_at: out_of_chrono_range_system_time(),
                id: InboxId::from_uuid(Uuid::nil()),
            }),
            limit: NonZeroU32::MIN,
        })
        .await;

    assert!(matches!(list, Err(PostgresError::InvalidData)));
    fixture.cleanup().await;
}

#[tokio::test]
async fn claim_complete_and_rollback_make_effects_atomic_and_terminal_duplicates_inert() {
    let fixture = isolated_inbox_fixture().await;
    let pool = fixture.pool.clone();
    let store = store(pool.clone(), 2);
    let effect_table = format!("inbox_effect_{}", Uuid::now_v7().simple());

    // Identifiers cannot be bound; this name is constrained to a fixed prefix and UUID hex.
    sqlx::query(AssertSqlSafe(format!(
        "CREATE TABLE {effect_table} (message_id uuid PRIMARY KEY)"
    )))
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("effect table setup failed"));

    let completed = record("inbox-atomic-v4", 0x901);

    let mut transaction = store
        .begin()
        .await
        .unwrap_or_else(|_| panic!("begin failed"));

    let receipt = match store
        .claim(&mut transaction, &completed)
        .await
        .unwrap_or_else(|_| panic!("claim failed"))
    {
        InboxClaimOutcome::Claimed(receipt) => receipt,
        outcome => panic!("unexpected claim outcome: {outcome:?}"),
    };

    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {effect_table} VALUES ($1)"
    )))
    .bind(completed.message_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .unwrap_or_else(|_| panic!("effect insert failed"));

    store
        .complete(&mut transaction, receipt)
        .await
        .unwrap_or_else(|_| panic!("complete failed"));

    store
        .commit(transaction)
        .await
        .unwrap_or_else(|_| panic!("commit failed"));

    assert_eq!(
        sqlx::query_scalar::<_, i64>(AssertSqlSafe(format!(
            "SELECT count(*) FROM {effect_table}"
        )))
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|_| panic!("effect read failed")),
        1
    );

    let mut transaction = store
        .begin()
        .await
        .unwrap_or_else(|_| panic!("begin failed"));

    assert!(matches!(
        store
            .claim(&mut transaction, &completed)
            .await
            .unwrap_or_else(|_| panic!("duplicate claim failed")),
        InboxClaimOutcome::CompletedDuplicate
    ));

    store
        .rollback(transaction)
        .await
        .unwrap_or_else(|_| panic!("rollback failed"));

    let rolled = record("inbox-atomic-v4", 0x902);

    let mut transaction = store
        .begin()
        .await
        .unwrap_or_else(|_| panic!("begin failed"));

    let _ = store
        .claim(&mut transaction, &rolled)
        .await
        .unwrap_or_else(|_| panic!("claim failed"));

    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {effect_table} VALUES ($1)"
    )))
    .bind(rolled.message_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .unwrap_or_else(|_| panic!("rollback effect insert failed"));

    store
        .rollback(transaction)
        .await
        .unwrap_or_else(|_| panic!("rollback failed"));

    assert_eq!(
        sqlx::query_scalar::<_, i64>(AssertSqlSafe(format!(
            "SELECT count(*) FROM {effect_table}"
        )))
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|_| panic!("effect read failed")),
        1
    );

    let mut transaction = store
        .begin()
        .await
        .unwrap_or_else(|_| panic!("begin failed"));

    assert!(matches!(
        store
            .claim(&mut transaction, &rolled)
            .await
            .unwrap_or_else(|_| panic!("reclaim failed")),
        InboxClaimOutcome::Claimed(_)
    ));

    store
        .rollback(transaction)
        .await
        .unwrap_or_else(|_| panic!("rollback failed"));

    // The first open transaction owns this key; a second transaction sees an immediate duplicate.
    let contested = record("inbox-lock-v3", 0x903);
    let distinct = record("inbox-lock-v3", 0x904);

    let mut holder = store
        .begin()
        .await
        .unwrap_or_else(|_| panic!("begin failed"));

    assert!(matches!(
        store
            .claim(&mut holder, &contested)
            .await
            .unwrap_or_else(|_| panic!("holder claim failed")),
        InboxClaimOutcome::Claimed(_)
    ));

    let mut contender = store
        .begin()
        .await
        .unwrap_or_else(|_| panic!("begin failed"));

    assert!(matches!(
        store
            .claim(&mut contender, &contested)
            .await
            .unwrap_or_else(|_| panic!("contended claim failed")),
        InboxClaimOutcome::InProgressDuplicate
    ));

    assert!(matches!(
        store
            .claim(&mut contender, &distinct)
            .await
            .unwrap_or_else(|_| panic!("distinct claim failed")),
        InboxClaimOutcome::Claimed(_)
    ));

    store
        .rollback(contender)
        .await
        .unwrap_or_else(|_| panic!("rollback failed"));

    store
        .rollback(holder)
        .await
        .unwrap_or_else(|_| panic!("rollback failed"));

    sqlx::query(AssertSqlSafe(format!("DROP TABLE {effect_table}")))
        .execute(&pool)
        .await
        .unwrap_or_else(|_| panic!("effect table cleanup failed"));

    fixture.cleanup().await;
}

#[tokio::test]
async fn failures_transition_exactly_and_dead_letters_page_retry_and_delete() {
    let fixture = isolated_inbox_fixture().await;
    let pool = fixture.pool.clone();
    let store = store(pool.clone(), 2);
    let retry = record("inbox-failures", 0x911);

    assert_eq!(
        store
            .fail(&retry, failure(FailureKind::Transient))
            .await
            .unwrap_or_else(|_| panic!("retry failure failed")),
        InboxFailureOutcome::Retry { attempts: 1 }
    );

    assert_eq!(
        store
            .fail(&retry, failure(FailureKind::Transient))
            .await
            .unwrap_or_else(|_| panic!("exhaustion failed")),
        InboxFailureOutcome::Dead {
            attempts: 2,
            reason: DeadReason::Exhausted
        }
    );

    let terminal_before = inbox_record(
        &pool,
        InboxLookupParams::by_identity(retry.scope.as_str(), retry.message_id.into_uuid()),
    )
    .await
    .unwrap_or_else(|| panic!("terminal receipt missing"));

    let terminal_snapshot = terminal_before;

    assert_eq!(
        store
            .fail(
                &retry,
                failure_with_error(FailureKind::Permanent, "distinct terminal re-fail"),
            )
            .await
            .unwrap_or_else(|_| panic!("terminal failure failed")),
        InboxFailureOutcome::Dead {
            attempts: 2,
            reason: DeadReason::Exhausted
        }
    );

    let terminal_after = inbox_record(
        &pool,
        InboxLookupParams::by_identity(retry.scope.as_str(), retry.message_id.into_uuid()),
    )
    .await
    .unwrap_or_else(|| panic!("terminal receipt disappeared"));

    assert_eq!(
        terminal_after, terminal_snapshot,
        "a distinguishable repeated failure changed a durable terminal receipt column"
    );

    let permanent = record("inbox-failures", 0x912);

    assert_eq!(
        store
            .fail(&permanent, failure(FailureKind::Permanent))
            .await
            .unwrap_or_else(|_| panic!("permanent failure failed")),
        InboxFailureOutcome::Dead {
            attempts: 1,
            reason: DeadReason::Permanent
        }
    );

    let page = store
        .list(DeadLetterQuery {
            after: None,
            limit: NonZeroU32::new(10).unwrap_or(NonZeroU32::MIN),
        })
        .await
        .unwrap_or_else(|_| panic!("list failed"));

    assert_eq!(page.len(), 2);
    let first = &page[0];
    let second = &page[1];
    assert_eq!(first.scope, retry.scope);
    assert_eq!(first.message_type, retry.message_type);
    assert_eq!(first.metadata, Some(Metadata::default()));
    assert_eq!(first.attempts, 2);
    assert_eq!(first.reason, DeadReason::Exhausted);

    assert_eq!(
        first.last_error,
        Some(ErrorSummary::from_safe_text("safe test failure"))
    );

    assert_eq!(second.scope, permanent.scope);

    let permanent_durable = inbox_record(
        &pool,
        InboxLookupParams::by_identity(permanent.scope.as_str(), permanent.message_id.into_uuid()),
    )
    .await
    .unwrap_or_else(|| panic!("permanent terminal receipt missing"));

    let expected_second = DeadLetterRecord {
        id: InboxId::from_uuid(permanent_durable.id),
        scope: permanent.scope.clone(),
        message_id: permanent.message_id,
        message_type: permanent.message_type.clone(),
        version: u32::try_from(permanent_durable.message_version)
            .unwrap_or_else(|_| panic!("permanent message version must be non-negative")),
        metadata: Some(Metadata::default()),
        attempts: u32::try_from(permanent_durable.attempts)
            .unwrap_or_else(|_| panic!("permanent attempts must be non-negative")),
        received_at: permanent_durable.received_at.into(),
        dead_at: permanent_durable
            .dead_at
            .unwrap_or_else(|| panic!("permanent terminal receipt has no dead time"))
            .into(),
        reason: DeadReason::Permanent,
        last_error: Some(ErrorSummary::from_safe_text("safe test failure")),
    };

    assert_eq!(
        *second, expected_second,
        "initial dead-letter page did not return the complete permanent receipt"
    );

    let after = DeadLetterCursor {
        dead_at: first.dead_at,
        id: first.id,
    };

    let next_page = store
        .list(DeadLetterQuery {
            after: Some(after),
            limit: NonZeroU32::MIN,
        })
        .await
        .unwrap_or_else(|_| panic!("keyset failed"));

    assert_eq!(
        next_page,
        vec![expected_second],
        "exclusive keyset page did not return the complete second receipt"
    );

    assert_eq!(
        store
            .retry(DeadLetterBatch::new(&[first.id]).unwrap_or_else(|_| panic!("batch rejected")),)
            .await
            .unwrap_or_else(|_| panic!("retry failed")),
        vec![first.id]
    );

    assert_eq!(
        store
            .delete(
                DeadLetterBatch::new(&[second.id])
                    .unwrap_or_else(|_| panic!("batch rejected")),
            )
            .await
            .unwrap_or_else(|_| panic!("delete failed")),
        vec![second.id]
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn retention_is_terminal_only_and_bounded_per_phase() {
    let fixture = isolated_inbox_fixture().await;
    let pool = fixture.pool.clone();
    let store = store(pool.clone(), 2);

    for id in [0x921, 0x922] {
        let row = record("inbox-purge-v3", id);

        let mut tx = store
            .begin()
            .await
            .unwrap_or_else(|_| panic!("begin failed"));

        let receipt = match store
            .claim(&mut tx, &row)
            .await
            .unwrap_or_else(|_| panic!("claim failed"))
        {
            InboxClaimOutcome::Claimed(r) => r,
            other => panic!("unexpected {other:?}"),
        };

        store
            .complete(&mut tx, receipt)
            .await
            .unwrap_or_else(|_| panic!("complete failed"));

        store
            .commit(tx)
            .await
            .unwrap_or_else(|_| panic!("commit failed"));
    }

    for id in [0x923, 0x924] {
        let row = record("inbox-purge-v3", id);

        let _ = store
            .fail(&row, failure(FailureKind::Permanent))
            .await
            .unwrap_or_else(|_| panic!("dead setup failed"));
    }

    let pending = record("inbox-purge-v3", 0x925);

    let mut pending_transaction = store
        .begin()
        .await
        .unwrap_or_else(|_| panic!("pending setup begin failed"));

    assert!(matches!(
        store
            .claim(&mut pending_transaction, &pending)
            .await
            .unwrap_or_else(|_| panic!("pending setup claim failed")),
        InboxClaimOutcome::Claimed(_)
    ));

    store
        .commit(pending_transaction)
        .await
        .unwrap_or_else(|_| panic!("pending setup commit failed"));

    let retrying = record("inbox-purge-v3", 0x926);

    assert_eq!(
        store
            .fail(&retrying, failure(FailureKind::Transient))
            .await
            .unwrap_or_else(|_| panic!("retrying setup failed")),
        InboxFailureOutcome::Retry { attempts: 1 }
    );

    let report = store
        .purge(InboxPurgeRequest {
            completed_retention: Some(Duration::ZERO),
            dead_retention: Some(Duration::ZERO),
            batch_size: NonZeroU32::MIN,
        })
        .await
        .unwrap_or_else(|_| panic!("purge failed"));

    assert_eq!((report.completed_deleted, report.dead_deleted), (1, 1));

    let stats = store
        .stats()
        .await
        .unwrap_or_else(|_| panic!("stats failed"));

    assert_eq!(
        (stats.pending, stats.retrying, stats.completed, stats.dead),
        (1, 1, 1, 1),
        "zero-retention batch one must preserve pending and retrying receipts"
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn saturated_attempts_become_dead_without_overflow_and_poisoned_metadata_is_tolerated() {
    let fixture = isolated_inbox_fixture().await;
    let pool = fixture.pool.clone();
    let store = store(pool.clone(), 2_147_483_647);
    let row = record("inbox-boundaries-v2", 0x931);

    sqlx::query!(
        r#"
            -- A schema-valid maximum proves the failure transition saturates before incrementing.
            INSERT INTO inbox_receipts (
                scope, message_id, message_type, message_version, metadata, attempts
            )
            VALUES (
                $1,
                $2,
                'postgres.inbox-test',
                1,
                '{"correlation":{"correlation_id":42}}'::jsonb,
                2147483647
            )
        "#,
        row.scope.as_str(),
        row.message_id.into_uuid()
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("boundary fixture setup failed"));

    assert_eq!(
        store
            .fail(&row, failure(FailureKind::Transient))
            .await
            .unwrap_or_else(|_| panic!("saturated failure failed")),
        InboxFailureOutcome::Dead {
            attempts: 2_147_483_647,
            reason: DeadReason::Exhausted
        }
    );

    let dead = store
        .list(DeadLetterQuery {
            after: None,
            limit: NonZeroU32::new(100).unwrap_or(NonZeroU32::MIN),
        })
        .await
        .unwrap_or_else(|_| panic!("list failed"));

    assert!(
        dead.iter()
            .any(|entry| entry.scope == row.scope && entry.metadata.is_none())
    );

    fixture.cleanup().await;
}

#[derive(Debug)]
struct InboxPlanNode {
    node_type: String,

    index_name: Option<String>,

    actual_rows: Option<f64>,

    actual_loops: Option<f64>,

    rows_removed_by_filter: Option<f64>,

    conflict_arbiter_indexes: Vec<String>,
}

fn inbox_plan_nodes(plan: Option<serde_json::Value>) -> Vec<InboxPlanNode> {
    fn visit(value: &serde_json::Value, nodes: &mut Vec<InboxPlanNode>) {
        match value {
            serde_json::Value::Object(object) => {
                if let Some(serde_json::Value::String(node_type)) = object.get("Node Type") {
                    nodes.push(InboxPlanNode {
                        node_type: node_type.clone(),
                        index_name: object
                            .get("Index Name")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned),
                        actual_rows: object
                            .get("Actual Rows")
                            .and_then(serde_json::Value::as_f64),
                        actual_loops: object
                            .get("Actual Loops")
                            .and_then(serde_json::Value::as_f64),
                        rows_removed_by_filter: object
                            .get("Rows Removed by Filter")
                            .and_then(serde_json::Value::as_f64),
                        conflict_arbiter_indexes: object
                            .get("Conflict Arbiter Indexes")
                            .and_then(serde_json::Value::as_array)
                            .map(|indexes| {
                                indexes
                                    .iter()
                                    .filter_map(serde_json::Value::as_str)
                                    .map(str::to_owned)
                                    .collect()
                            })
                            .unwrap_or_default(),
                    });
                }

                for child in object.values() {
                    visit(child, nodes);
                }
            }
            serde_json::Value::Array(values) => {
                for child in values {
                    visit(child, nodes);
                }
            }
            _ => {}
        }
    }

    let mut nodes = Vec::new();

    visit(
        &plan.unwrap_or_else(|| panic!("inbox plan payload was null")),
        &mut nodes,
    );

    nodes
}

fn assert_inbox_bounded_node(node: &InboxPlanNode, description: &str) {
    let rows = node
        .actual_rows
        .unwrap_or_else(|| panic!("{description} has no Actual Rows: {node:?}"));

    let loops = node
        .actual_loops
        .unwrap_or_else(|| panic!("{description} has no Actual Loops: {node:?}"));

    assert!(rows <= 1.0, "{description} examined {rows} rows");
    assert!(loops <= 1.0, "{description} ran {loops} loops");

    if let Some(filtered) = node.rows_removed_by_filter {
        assert!(filtered <= 1.0, "{description} removed {filtered} rows");
    }
}

fn assert_inbox_named_index(nodes: &[InboxPlanNode], index: &str) {
    let matches: Vec<_> = nodes
        .iter()
        .filter(|node| {
            node.index_name.as_deref() == Some(index)
                && matches!(node.node_type.as_str(), "Index Scan" | "Index Only Scan")
        })
        .collect();

    assert_eq!(
        matches.len(),
        1,
        "expected one bounded index node for {index}: {nodes:?}"
    );

    assert_inbox_bounded_node(matches[0], index);
}

fn assert_inbox_mutation(nodes: &[InboxPlanNode], locking: bool) {
    let mutations: Vec<_> = nodes
        .iter()
        .filter(|node| node.node_type == "ModifyTable")
        .collect();

    assert_eq!(mutations.len(), 1, "missing ModifyTable: {nodes:?}");
    assert_inbox_bounded_node(mutations[0], "ModifyTable");

    if locking {
        let locks: Vec<_> = nodes
            .iter()
            .filter(|node| node.node_type == "LockRows")
            .collect();

        assert_eq!(locks.len(), 1, "missing LockRows: {nodes:?}");
        assert_inbox_bounded_node(locks[0], "LockRows");
    }
}

#[tokio::test]
async fn postgres_18_inbox_query_shapes_use_bounded_named_index_access_paths() {
    let fixture = isolated_inbox_fixture().await;
    let pool = fixture.pool.clone();
    let store = store(pool.clone(), 2);
    let completed = record("inbox-plan-completed", 0xa01);

    let mut transaction = store
        .begin()
        .await
        .unwrap_or_else(|_| panic!("begin failed"));

    let completed_receipt = match store
        .claim(&mut transaction, &completed)
        .await
        .unwrap_or_else(|_| panic!("completed plan setup claim failed"))
    {
        InboxClaimOutcome::Claimed(receipt) => receipt,
        outcome => panic!("unexpected completed plan setup outcome: {outcome:?}"),
    };

    let completed_id = completed_receipt.id().into_uuid();

    store
        .complete(&mut transaction, completed_receipt)
        .await
        .unwrap_or_else(|_| panic!("completed plan setup completion failed"));

    store
        .commit(transaction)
        .await
        .unwrap_or_else(|_| panic!("completed plan setup commit failed"));

    let dead = record("inbox-plan-dead", 0xa02);

    assert!(matches!(
        store
            .fail(&dead, failure(FailureKind::Permanent))
            .await
            .unwrap_or_else(|_| panic!("dead plan setup failed")),
        InboxFailureOutcome::Dead { .. }
    ));

    let dead_id = inbox_record(
        &pool,
        InboxLookupParams::by_identity(dead.scope.as_str(), dead.message_id.into_uuid()),
    )
    .await
    .unwrap_or_else(|| panic!("dead plan setup receipt missing"))
    .id;

    // Dead noise makes the partial cursor index preferable without unchecked fixture SQL.
    for sequence in 0..1024_u128 {
        let noise = record("inbox-plan-noise", 0xb000 + sequence);

        let _ = store
            .fail(&noise, failure(FailureKind::Permanent))
            .await
            .unwrap_or_else(|_| panic!("dead plan noise setup failed"));
    }

    let retry = record("inbox-plan-retry", 0xa03);

    assert!(matches!(
        store
            .fail(&retry, failure(FailureKind::Permanent))
            .await
            .unwrap_or_else(|_| panic!("retry plan setup failed")),
        InboxFailureOutcome::Dead { .. }
    ));

    let retry_id = inbox_record(
        &pool,
        InboxLookupParams::by_identity(retry.scope.as_str(), retry.message_id.into_uuid()),
    )
    .await
    .unwrap_or_else(|| panic!("retry plan setup receipt missing"))
    .id;

    sqlx::query!("ANALYZE inbox_receipts")
        .execute(&pool)
        .await
        .unwrap_or_else(|_| panic!("inbox plan analyze failed"));

    let claim_record = record("inbox-plan-claim", 0xa04);

    let mut transaction = store
        .begin()
        .await
        .unwrap_or_else(|_| panic!("claim plan setup begin failed"));

    assert!(matches!(
        store
            .claim(&mut transaction, &claim_record)
            .await
            .unwrap_or_else(|_| panic!("claim plan setup claim failed")),
        InboxClaimOutcome::Claimed(_)
    ));

    store
        .commit(transaction)
        .await
        .unwrap_or_else(|_| panic!("claim plan setup commit failed"));

    let claim: Option<serde_json::Value> = sqlx::query_scalar!(
        r#"
            EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
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
        claim_record.scope.as_str(),
        claim_record.message_id.into_uuid(),
        "postgres.inbox-test",
        1_i32,
        serde_json::json!({}),
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("claim plan failed"));

    let claim_nodes = inbox_plan_nodes(claim);
    assert_inbox_mutation(&claim_nodes, false);

    assert!(
        claim_nodes.iter().any(|node| {
            node.conflict_arbiter_indexes
                .iter()
                .any(|index| index == "ix_inbox_receipts_scope_message_id")
        }),
        "claim plan omitted ix_inbox_receipts_scope_message_id: {claim_nodes:?}"
    );

    let claim_persisted = inbox_record(
        &pool,
        InboxLookupParams::by_identity(
            claim_record.scope.as_str(),
            claim_record.message_id.into_uuid(),
        ),
    )
    .await
    .unwrap_or_else(|| panic!("claim plan receipt missing"));

    assert!(
        claim_persisted.scope == claim_record.scope.as_str()
            && claim_persisted.message_id == claim_record.message_id.into_uuid()
            && claim_persisted.attempts == 0
            && claim_persisted.completed_at.is_none()
            && claim_persisted.dead_at.is_none()
            && claim_persisted.dead_reason.is_none()
            && claim_persisted.last_error.is_none(),
        "claim plan did not persist an active receipt"
    );

    let completed_purge: Option<serde_json::Value> = sqlx::query_scalar!(
        r#"
            EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
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
        0_i64,
        1_i64,
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("completed purge plan failed"));

    let completed_nodes = inbox_plan_nodes(completed_purge);
    assert_inbox_named_index(&completed_nodes, "ix_inbox_receipts_completed");
    assert_inbox_mutation(&completed_nodes, true);

    assert!(
        inbox_record(&pool, InboxLookupParams::by_id(completed_id))
            .await
            .is_none(),
        "completed purge plan did not delete its receipt"
    );

    let dead_purge: Option<serde_json::Value> = sqlx::query_scalar!(
        r#"
            EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
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
        0_i64,
        1_i64,
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("dead purge plan failed"));

    let dead_nodes = inbox_plan_nodes(dead_purge);
    assert_inbox_named_index(&dead_nodes, "ix_inbox_receipts_dead");
    assert_inbox_mutation(&dead_nodes, true);

    assert!(
        inbox_record(&pool, InboxLookupParams::by_id(dead_id))
            .await
            .is_none(),
        "dead purge plan did not delete its oldest receipt"
    );

    let cursor: Option<serde_json::Value> = sqlx::query_scalar!(
        r#"
            EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
            -- The stable keyset order matches the terminal receipt access path.
            SELECT
                id AS "id!",
                scope AS "scope!",
                message_id AS "message_id!",
                message_type AS "message_type!",
                message_version AS "message_version!",
                metadata AS "metadata!",
                attempts AS "attempts!",
                received_at AS "received_at!",
                dead_at AS "dead_at!",
                dead_reason AS "dead_reason!",
                last_error
            FROM inbox_receipts
            WHERE dead_at IS NOT NULL
              -- The exclusive cursor cannot return its preceding page boundary twice.
              AND (
                  $1::timestamptz IS NULL
                  OR (dead_at, id) > ($1, $2)
              )
            ORDER BY dead_at, id
            LIMIT $3
        "#,
        Option::<chrono::DateTime<chrono::Utc>>::None,
        Option::<Uuid>::None,
        1_i64,
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("dead cursor plan failed"));

    let cursor_nodes = inbox_plan_nodes(cursor);
    assert_inbox_named_index(&cursor_nodes, "ix_inbox_receipts_dead");

    let retry_plan: Option<serde_json::Value> = sqlx::query_scalar!(
        r#"
            EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
            -- Retrying preserves identity while resetting only still-terminal receipts.
            UPDATE inbox_receipts
            SET
                attempts = 0,
                dead_at = NULL,
                dead_reason = NULL,
                last_error = NULL
            WHERE id = ANY($1)
              AND dead_at IS NOT NULL
            RETURNING
                id AS "id!"
        "#,
        &vec![retry_id],
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("retry plan failed"));

    let retry_nodes = inbox_plan_nodes(retry_plan);
    assert_inbox_named_index(&retry_nodes, "pk_inbox_receipts");
    assert_inbox_mutation(&retry_nodes, false);

    let retry_persisted = inbox_record(&pool, InboxLookupParams::by_id(retry_id))
        .await
        .unwrap_or_else(|| panic!("retry plan receipt missing"));

    assert!(
        retry_persisted.attempts == 0
            && retry_persisted.completed_at.is_none()
            && retry_persisted.dead_at.is_none()
            && retry_persisted.dead_reason.is_none()
            && retry_persisted.last_error.is_none(),
        "retry plan did not persist its active transition"
    );

    fixture.cleanup().await;
}
