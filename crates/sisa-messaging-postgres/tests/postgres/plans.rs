use crate::support::{OutboxLookupParams, isolated_outbox_pool, outbox_record};
use uuid::Uuid;

#[derive(Debug)]
struct PlanNode {
    node_type: String,

    index_name: Option<String>,

    actual_rows: Option<f64>,

    actual_loops: Option<f64>,

    rows_removed_by_filter: Option<f64>,
}

fn assert_bounded_plan_node(node: &PlanNode, description: &str) {
    let actual_rows = node
        .actual_rows
        .unwrap_or_else(|| panic!("{description} has no Actual Rows: {node:?}"));
    let actual_loops = node
        .actual_loops
        .unwrap_or_else(|| panic!("{description} has no Actual Loops: {node:?}"));
    assert!(
        actual_rows <= 1.0,
        "{description} examined {actual_rows} rows"
    );
    assert!(
        actual_loops <= 1.0,
        "{description} ran {actual_loops} loops"
    );
    if let Some(rows_removed_by_filter) = node.rows_removed_by_filter {
        assert!(
            rows_removed_by_filter <= 1.0,
            "{description} removed {rows_removed_by_filter} rows"
        );
    }
}

fn assert_bounded_index_plan(
    plan: Option<serde_json::Value>,
    indexes: &[&str],
    mutation: bool,
    locking_candidate: bool,
) {
    fn visit(value: &serde_json::Value, nodes: &mut Vec<PlanNode>) {
        match value {
            serde_json::Value::Object(object) => {
                if let Some(serde_json::Value::String(node)) = object.get("Node Type") {
                    nodes.push(PlanNode {
                        node_type: node.clone(),
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
                    });
                }
                for value in object.values() {
                    visit(value, nodes);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    visit(value, nodes);
                }
            }
            _ => {}
        }
    }
    let plan = plan.unwrap_or_else(|| panic!("plan payload was null"));
    let mut nodes = Vec::new();
    visit(&plan, &mut nodes);
    for index in indexes {
        let matching_nodes: Vec<_> = nodes
            .iter()
            .filter(|node| {
                node.index_name.as_deref() == Some(*index)
                    && matches!(node.node_type.as_str(), "Index Scan" | "Index Only Scan")
            })
            .collect();
        assert!(
            !matching_nodes.is_empty(),
            "missing bounded index scan for {index} in {nodes:?}"
        );
        for node in matching_nodes {
            assert_bounded_plan_node(node, index);
        }
    }
    if mutation {
        let mutation_node = nodes
            .iter()
            .find(|node| node.node_type == "ModifyTable")
            .unwrap_or_else(|| panic!("missing ModifyTable: {nodes:?}"));
        assert_bounded_plan_node(mutation_node, "ModifyTable");
    }
    if locking_candidate {
        let candidate_node = nodes
            .iter()
            .find(|node| node.node_type == "LockRows")
            .unwrap_or_else(|| panic!("missing LockRows candidate: {nodes:?}"));
        assert_bounded_plan_node(candidate_node, "LockRows candidate");
    }
}

#[tokio::test]
async fn postgres_18_outbox_query_shapes_use_bounded_named_index_access_paths() {
    let pool = isolated_outbox_pool().await;
    let claim_id = Uuid::from_u128(0x401);
    let claim_token = Uuid::from_u128(0x501);
    let poison_id = Uuid::from_u128(0x402);
    let poison_token = Uuid::from_u128(0x502);
    let outcome_id = Uuid::from_u128(0x403);
    let outcome_token = Uuid::from_u128(0x503);
    let expiry_id = Uuid::from_u128(0x404);
    let published_id = Uuid::from_u128(0x405);
    let dead_id = Uuid::from_u128(0x406);
    sqlx::query!(
        r#"
            -- Noise plus matching rows make every final query shape selective in one checked setup.
            WITH noise AS (
                INSERT INTO outbox_messages (id, message_id, message_type, message_version, content_type, payload, metadata, ordering_key, created_at, claimable_at, expires_at, claim_token, locked_by, published_at, dead_at, dead_reason)
                SELECT uuidv7(), uuidv7(), 'postgres.plan-noise', 1, 'application/test', ''::bytea, '{}'::jsonb, NULL, now(), now() + interval '1 hour', NULL, NULL, NULL, NULL, NULL, NULL
                FROM generate_series(1, 2048)
                RETURNING id
            )
            INSERT INTO outbox_messages (id, message_id, message_type, message_version, content_type, payload, metadata, ordering_key, created_at, claimable_at, expires_at, claim_token, locked_by, published_at, dead_at, dead_reason)
            VALUES
                ($1, uuidv7(), 'postgres.plan-claim', 1, 'application/test', ''::bytea, '{}'::jsonb, 'plan-key', now(), now(), NULL, $2, 'plan-worker', NULL, NULL, NULL),
                ($3, uuidv7(), 'postgres.plan-poison', 1, 'application/test', ''::bytea, '{}'::jsonb, NULL, now(), now() + interval '1 hour', NULL, $4, 'plan-worker', NULL, NULL, NULL),
                ($5, uuidv7(), 'postgres.plan-outcome', 1, 'application/test', ''::bytea, '{}'::jsonb, NULL, now(), now() + interval '1 hour', NULL, $6, 'plan-worker', NULL, NULL, NULL),
                ($7, uuidv7(), 'postgres.plan-expire', 1, 'application/test', ''::bytea, '{}'::jsonb, NULL, now(), now(), now() - interval '1 minute', NULL, NULL, NULL, NULL, NULL),
                ($8, uuidv7(), 'postgres.plan-published', 1, 'application/test', ''::bytea, '{}'::jsonb, NULL, now(), now(), NULL, NULL, NULL, now() - interval '1 minute', NULL, NULL),
                ($9, uuidv7(), 'postgres.plan-dead', 1, 'application/test', ''::bytea, '{}'::jsonb, NULL, now(), now(), NULL, NULL, NULL, NULL, now() - interval '1 minute', 'permanent')
        "#,
        claim_id,
        claim_token,
        poison_id,
        poison_token,
        outcome_id,
        outcome_token,
        expiry_id,
        published_id,
        dead_id,
    ).execute(&pool).await.unwrap_or_else(|_| panic!("plan fixture setup failed"));
    sqlx::query!("ANALYZE outbox_messages")
        .execute(&pool)
        .await
        .unwrap_or_else(|_| panic!("plan fixture analyze failed"));

    let claim = sqlx::query_scalar!(
        r#"
        EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
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
                    -- An expired predecessor still owns its key until its current lease ends.
                    AND (
                        p.expires_at IS NULL
                        OR p.expires_at > now()
                        OR (
                            p.claim_token IS NOT NULL
                            AND p.claimable_at > now()
                        )
                    )
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
        1_i64,
        "plan-worker",
        1_000_000_i64
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("claim plan failed"));
    assert_bounded_index_plan(
        claim,
        &[
            "ix_outbox_messages_claimable",
            "ix_outbox_messages_ordering_key",
        ],
        true,
        true,
    );
    let claim_persisted = outbox_record(&pool, OutboxLookupParams::by_id(claim_id))
        .await
        .unwrap_or_else(|| panic!("claim plan fixture row was deleted"));
    assert!(
        claim_persisted.id == claim_id
            && claim_persisted.message_type == "postgres.plan-claim"
            && claim_persisted
                .claim_token
                .is_some_and(|token| token != claim_token)
            && claim_persisted.locked_by.as_deref() == Some("plan-worker")
            && claim_persisted.claimable_at > claim_persisted.observed_at
            && claim_persisted.attempts == 0
            && claim_persisted.published_at.is_none()
            && claim_persisted.dead_at.is_none()
            && claim_persisted.dead_reason.is_none()
            && claim_persisted.last_error.is_none(),
        "claim plan did not persist its fenced lease"
    );

    let poison = sqlx::query_scalar!(
        r#"
        EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
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
        &vec![poison_id],
        &vec![poison_token]
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("poison plan failed"));
    assert_bounded_index_plan(poison, &["outbox_messages_pkey"], true, false);
    let poison_persisted = outbox_record(&pool, OutboxLookupParams::by_id(poison_id))
        .await
        .unwrap_or_else(|| panic!("poison plan fixture row was deleted"));
    assert!(
        poison_persisted.id == poison_id
            && poison_persisted.message_type == "postgres.plan-poison"
            && poison_persisted.attempts == 0
            && poison_persisted.published_at.is_none()
            && poison_persisted.dead_at.is_some()
            && poison_persisted.dead_reason.as_deref() == Some("undecodable")
            && poison_persisted.last_error.as_deref() == Some("persisted provider data is invalid")
            && poison_persisted.claim_token.is_none()
            && poison_persisted.locked_by.is_none(),
        "poison plan did not persist its fenced transition"
    );

    let expiry = sqlx::query_scalar!(
        r#"
        EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
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
        1_i64
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("expiry plan failed"));
    assert_bounded_index_plan(expiry, &["ix_outbox_messages_expires"], true, true);
    let expiry_persisted = outbox_record(&pool, OutboxLookupParams::by_id(expiry_id))
        .await
        .unwrap_or_else(|| panic!("expiry plan fixture row was deleted"));
    assert!(
        expiry_persisted.id == expiry_id
            && expiry_persisted.message_type == "postgres.plan-expire"
            && expiry_persisted.attempts == 0
            && expiry_persisted.published_at.is_none()
            && expiry_persisted.dead_at.is_some()
            && expiry_persisted.dead_reason.as_deref() == Some("expired")
            && expiry_persisted.last_error.is_none()
            && expiry_persisted.claim_token.is_none()
            && expiry_persisted.locked_by.is_none(),
        "expiry plan did not persist its terminal transition"
    );

    let published = sqlx::query_scalar!(
        r#"
        EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
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
        1_000_000_i64,
        1_i64
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("published plan failed"));
    assert_bounded_index_plan(published, &["ix_outbox_messages_published"], true, true);
    assert!(
        outbox_record(&pool, OutboxLookupParams::by_id(published_id))
            .await
            .is_none(),
        "published retention plan did not delete its fixture row"
    );

    let dead = sqlx::query_scalar!(
        r#"
        EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
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
        1_000_000_i64,
        1_i64
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("dead retention plan failed"));
    assert_bounded_index_plan(dead, &["ix_outbox_messages_dead_cursor"], true, true);
    assert!(
        outbox_record(&pool, OutboxLookupParams::by_id(dead_id))
            .await
            .is_none(),
        "dead retention plan did not delete its fixture row"
    );

    let cursor = sqlx::query_scalar!(
        r#"
        EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
        -- Stable keyset order matches the dead-letter cursor index.
        SELECT
            id AS "id!",
            message_id AS "message_id!",
            message_type AS "message_type!",
            message_version AS "message_version!",
            content_type AS "content_type!",
            payload AS "payload!",
            metadata AS "metadata!",
            ordering_key,
            attempts AS "attempts!",
            dead_at AS "dead_at!",
            dead_reason AS "dead_reason!",
            last_error
        FROM outbox_messages
        WHERE dead_at IS NOT NULL
          -- The cursor is exclusive, so adjacent pages cannot repeat their boundary row.
          AND (
              $1::timestamptz IS NULL
              OR (dead_at, id) > ($1, $2)
          )
        ORDER BY dead_at, id
        LIMIT $3
    "#,
        Option::<chrono::DateTime<chrono::Utc>>::None,
        Option::<Uuid>::None,
        1_i64
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("dead cursor plan failed"));
    assert_bounded_index_plan(cursor, &["ix_outbox_messages_dead_cursor"], false, false);

    let outcome = sqlx::query_scalar!(
        r#"
        EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
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
        &vec![outcome_id],
        &vec![outcome_token]
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| panic!("outcome plan failed"));
    assert_bounded_index_plan(outcome, &["outbox_messages_pkey"], true, false);
    let outcome_persisted = outbox_record(&pool, OutboxLookupParams::by_id(outcome_id))
        .await
        .unwrap_or_else(|| panic!("outcome plan fixture row was deleted"));
    assert!(
        outcome_persisted.id == outcome_id
            && outcome_persisted.message_type == "postgres.plan-outcome"
            && outcome_persisted.attempts == 1
            && outcome_persisted.published_at.is_some()
            && outcome_persisted.dead_at.is_none()
            && outcome_persisted.dead_reason.is_none()
            && outcome_persisted.last_error.is_none()
            && outcome_persisted.claim_token.is_none()
            && outcome_persisted.locked_by.is_none(),
        "outcome plan did not persist its fenced completion"
    );
}
