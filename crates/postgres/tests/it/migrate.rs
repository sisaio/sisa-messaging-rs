use std::path::{Path, PathBuf};

use sisa_messaging_testing::Postgres;
use sqlx::{PgPool, migrate::Migrator};

fn migrations() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations/postgresql")
}

/// A pool whose `search_path` is the new schema `messaging`, as an application configures it.
async fn messaging_pool(postgres: &Postgres) -> PgPool {
    sqlx::query("CREATE SCHEMA messaging")
        .execute(&postgres.pool().await)
        .await
        .unwrap();

    PgPool::connect(&format!(
        "{}?options=-c%20search_path%3Dmessaging",
        postgres.url()
    ))
    .await
    .unwrap()
}

/// Runs the migrations as `sqlx migrate run --source migrations/postgresql` does.
async fn migrated(postgres: &Postgres) -> PgPool {
    let pool = messaging_pool(postgres).await;
    Migrator::new(migrations().as_path())
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();
    pool
}

async fn tables(pool: &PgPool, schema: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT relname::text FROM pg_class
         JOIN pg_namespace ON pg_namespace.oid = relnamespace
         WHERE nspname = $1 AND relkind IN ('r', 'p') ORDER BY 1",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn today(pool: &PgPool) -> String {
    sqlx::query_scalar("SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYYMMDD')")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn the_migrations_create_both_tables_in_the_search_path_schema() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;

    let names = tables(&pool, "messaging").await;
    let today = today(&pool).await;
    for table in ["outbox_messages", "inbox_receipts"] {
        let partitions: Vec<_> = names
            .iter()
            .filter(|name| name.starts_with(&format!("{table}_")))
            .collect();
        assert_eq!(partitions.len(), 9, "a default and 8 days: {partitions:?}");
        assert!(names.contains(&format!("{table}_default")), "{names:?}");
        assert!(names.contains(&format!("{table}_p{today}")), "{names:?}");
    }
    assert!(names.contains(&"_sqlx_migrations".to_owned()), "{names:?}");
    assert!(tables(&pool, "public").await.is_empty());
}

#[tokio::test]
async fn the_script_run_by_hand_creates_the_same_tables() {
    let postgres = Postgres::start().await;
    let pool = messaging_pool(&postgres).await;
    let script = migrations().join("20261104000000_create_outbox_and_inbox.sql");

    sqlx::raw_sql(sqlx::AssertSqlSafe(
        std::fs::read_to_string(script).unwrap(),
    ))
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(tables(&pool, "messaging").await.len(), 20);
}

#[tokio::test]
async fn every_constraint_and_index_is_named() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;

    let names: Vec<String> = sqlx::query_scalar(
        "SELECT conname::text FROM pg_constraint
         JOIN pg_class ON pg_class.oid = conrelid
         WHERE relname IN ('outbox_messages', 'inbox_receipts')
           AND contype <> 'n' -- PostgreSQL 18 records NOT NULL as a constraint too
         UNION
         SELECT indexname::text FROM pg_indexes
         WHERE schemaname = 'messaging' AND tablename IN ('outbox_messages', 'inbox_receipts')
         ORDER BY 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        names,
        [
            "ck_outbox_messages_status",
            "ix_outbox_messages_due",
            "ix_outbox_messages_id",
            "pk_inbox_receipts",
            "pk_outbox_messages",
            "uq_inbox_receipts_endpoint_message",
        ]
    );
}

#[tokio::test]
async fn a_row_lands_in_the_partition_of_its_utc_day() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;

    // A session 14 hours ahead of UTC, so a bound in local time would pick tomorrow's partition.
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("SET TIME ZONE 'Pacific/Kiritimati'")
        .execute(&mut *connection)
        .await
        .unwrap();
    let partition: String = sqlx::query_scalar(
        "INSERT INTO inbox_receipts
             (id, message_time, endpoint, message_id, type, source, attributes)
         VALUES (gen_random_uuid(), date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
                 'billing', gen_random_uuid(), 'orders.order_placed.v1', '/orders', '{}')
         RETURNING tableoid::regclass::text",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();

    assert_eq!(partition, format!("inbox_receipts_p{}", today(&pool).await));
}
