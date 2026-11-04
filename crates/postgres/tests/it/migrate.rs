use sisa_messaging_postgres::{PostgresSettings, migrate};
use sisa_messaging_testing::Postgres;
use sqlx::PgPool;

/// The partitions of `table` in the default schema, by name.
async fn partitions(pool: &PgPool, table: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT child.relname::text FROM pg_inherits
         JOIN pg_class child ON child.oid = pg_inherits.inhrelid
         JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
         JOIN pg_namespace ON pg_namespace.oid = parent.relnamespace
         WHERE pg_namespace.nspname = 'sisa_messaging' AND parent.relname = $1
         ORDER BY 1",
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn versions(pool: &PgPool, schema: &str) -> Vec<i32> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT version FROM \"{schema}\".schema_version ORDER BY 1"
    )))
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn migrate_creates_both_tables_with_a_default_and_eight_daily_partitions() {
    let postgres = Postgres::start().await;
    let pool = postgres.pool().await;

    migrate(&pool, &PostgresSettings::default()).await.unwrap();

    let today: String = sqlx::query_scalar("SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYYMMDD')")
        .fetch_one(&pool)
        .await
        .unwrap();
    for table in ["outbox_messages", "inbox_receipts"] {
        let names = partitions(&pool, table).await;
        assert_eq!(names.len(), 9, "{names:?}");
        assert!(names.contains(&format!("{table}_default")), "{names:?}");
        assert!(names.contains(&format!("{table}_p{today}")), "{names:?}");
    }
    assert_eq!(versions(&pool, "sisa_messaging").await, [1]);
}

#[tokio::test]
async fn migrate_names_every_constraint_and_index() {
    let postgres = Postgres::start().await;
    let pool = postgres.pool().await;

    migrate(&pool, &PostgresSettings::default()).await.unwrap();

    let names: Vec<String> = sqlx::query_scalar(
        "SELECT conname::text FROM pg_constraint
         JOIN pg_class ON pg_class.oid = conrelid
         WHERE relname IN ('outbox_messages', 'inbox_receipts', 'schema_version')
           AND contype <> 'n' -- PostgreSQL 18 records NOT NULL as a constraint too
         UNION
         SELECT indexname::text FROM pg_indexes
         WHERE schemaname = 'sisa_messaging'
           AND tablename IN ('outbox_messages', 'inbox_receipts', 'schema_version')
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
            "pk_schema_version",
            "uq_inbox_receipts_endpoint_message",
        ]
    );
}

#[tokio::test]
async fn a_second_migrate_changes_nothing() {
    let postgres = Postgres::start().await;
    let pool = postgres.pool().await;
    migrate(&pool, &PostgresSettings::default()).await.unwrap();

    migrate(&pool, &PostgresSettings::default()).await.unwrap();

    assert_eq!(versions(&pool, "sisa_messaging").await, [1]);
    assert_eq!(partitions(&pool, "outbox_messages").await.len(), 9);
}

#[tokio::test]
async fn concurrent_migrates_both_succeed_and_leave_one_schema() {
    let postgres = Postgres::start().await;
    let pool = postgres.pool().await;
    let settings = PostgresSettings::default();

    let (first, second) = tokio::join!(migrate(&pool, &settings), migrate(&pool, &settings));

    first.unwrap();
    second.unwrap();
    assert_eq!(versions(&pool, "sisa_messaging").await, [1]);
}

#[tokio::test]
async fn migrate_uses_the_configured_schema() {
    let postgres = Postgres::start().await;
    let pool = postgres.pool().await;
    let mut settings = PostgresSettings::default();
    settings.schema = "Billing Messages".into();

    migrate(&pool, &settings).await.unwrap();

    assert_eq!(versions(&pool, "Billing Messages").await, [1]);
    sqlx::query("SELECT * FROM \"Billing Messages\".outbox_messages")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_row_lands_in_the_partition_of_its_utc_day() {
    let postgres = Postgres::start().await;
    let pool = postgres.pool().await;
    migrate(&pool, &PostgresSettings::default()).await.unwrap();

    // A session 14 hours ahead of UTC, so a bound in local time would pick tomorrow's partition.
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("SET TIME ZONE 'Pacific/Kiritimati'")
        .execute(&mut *connection)
        .await
        .unwrap();
    let partition: String = sqlx::query_scalar(
        "INSERT INTO sisa_messaging.inbox_receipts
             (id, message_time, endpoint, message_id, type, source, attributes)
         VALUES (gen_random_uuid(), date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
                 'billing', gen_random_uuid(), 'orders.order_placed.v1', '/orders', '{}')
         RETURNING tableoid::regclass::text",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();

    let today: String = sqlx::query_scalar("SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYYMMDD')")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(partition, format!("sisa_messaging.inbox_receipts_p{today}"));
}
