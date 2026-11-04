//! The partitions of both tables, kept by the bus's maintenance worker.

use sisa_messaging_core::{Date, Expiry, PartitionInterval, PartitionStore, Timestamp};
use sisa_messaging_postgres::PostgresPartitions;
use sisa_messaging_runtime::{Bus, InMemoryTransport, PartitionSettings};
use sisa_messaging_testing::{Postgres, eventually};
use sqlx::PgPool;

use crate::migrate::migrated;

fn today() -> Date {
    Timestamp::now().date()
}

fn partition(table: &str, day: Date) -> String {
    format!("{table}_p{}", day.to_string().replace('-', ""))
}

async fn exists(pool: &PgPool, table: &str) -> bool {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(table)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn attached(pool: &PgPool, table: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT FROM pg_inherits WHERE inhrelid = to_regclass($1))")
        .bind(table)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn maintained(
    store: PostgresPartitions,
    settings: PartitionSettings,
) -> Bus<InMemoryTransport> {
    Bus::builder("/orders", InMemoryTransport::new())
        .partitions(store, settings)
        .start()
        .await
        .unwrap()
}

/// Writes an outbox row with `status`, created at noon UTC on `day`.
async fn outbox_row(pool: &PgPool, day: Date, status: &str) {
    sqlx::query(
        "INSERT INTO outbox_messages
             (id, created_at, type, source, data_content_type, attributes, data, status)
         VALUES (gen_random_uuid(), $1::timestamptz, 'orders.order_placed.v1', '/orders',
                 'application/json', '{}', '\\x7b7d', $2)",
    )
    .bind(format!("{day} 12:00+00"))
    .bind(status)
    .execute(pool)
    .await
    .unwrap();
}

/// Three old daily partitions of `table`, 30 to 28 days ago, as a long-running service has.
async fn old_partitions(store: &PostgresPartitions) -> [Date; 3] {
    let first = today().add_days(-30);
    store
        .ensure_partitions(PartitionInterval::Day, first, 3)
        .await
        .unwrap();
    [first, first.add_days(1), first.add_days(2)]
}

fn retention(days: u32, on_expiry: Expiry) -> PartitionSettings {
    let mut settings = PartitionSettings::default();
    settings.retention_days = Some(days);
    settings.on_expiry = on_expiry;
    settings
}

#[tokio::test]
async fn the_worker_creates_the_partitions_ahead() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let mut settings = PartitionSettings::default();
    settings.ahead = 10;

    let bus = maintained(PostgresPartitions::inbox(pool.clone()), settings).await;

    let last = partition("inbox_receipts", today().add_days(10));
    eventually("the tenth partition ahead", async || {
        exists(&pool, &last).await
    })
    .await;
    assert!(!exists(&pool, &partition("inbox_receipts", today().add_days(11))).await);
    bus.shutdown().await;
}

#[tokio::test]
async fn a_new_interval_starts_where_the_existing_partitions_end() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let store = PostgresPartitions::outbox(pool.clone());
    let month = PartitionInterval::Month;

    store.ensure_partitions(month, today(), 2).await.unwrap();

    // Every partition starts where the one before it ends, from the first of this month to the
    // end of the next, around the daily ones the migration made.
    let partitions = store
        .expirable(Date::new(9_999, 1, 1).unwrap())
        .await
        .unwrap();
    for pair in partitions.windows(2) {
        assert_eq!(pair[0].to, pair[1].from, "{partitions:?}");
    }
    assert_eq!(partitions.first().unwrap().from, month.start(today()));
    assert_eq!(partitions.last().unwrap().to, month.end(month.end(today())));
}

#[tokio::test]
async fn one_instance_holds_the_lock_at_a_time() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let first = PostgresPartitions::outbox(pool.clone());
    let second = PostgresPartitions::outbox(pool.clone());

    let lock = first.try_lock().await.unwrap();
    assert!(lock.is_some());
    assert!(second.try_lock().await.unwrap().is_none());
    assert!(
        PostgresPartitions::inbox(pool)
            .try_lock()
            .await
            .unwrap()
            .is_some(),
        "each table has a lock of its own"
    );

    drop(lock);
    eventually("the lock to be free", async || {
        second.try_lock().await.unwrap().is_some()
    })
    .await;
}

#[tokio::test]
async fn expired_partitions_are_detached_by_default() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let store = PostgresPartitions::inbox(pool.clone());
    let days = old_partitions(&store).await;

    let bus = maintained(store, retention(7, Expiry::default())).await;

    for day in days {
        let name = partition("inbox_receipts", day);
        eventually("the partition to leave the table", async || {
            !attached(&pool, &name).await
        })
        .await;
        assert!(exists(&pool, &name).await, "{name} is kept as a table");
    }
    assert!(attached(&pool, &partition("inbox_receipts", today())).await);
    bus.shutdown().await;
}

#[tokio::test]
async fn expired_partitions_are_dropped_only_when_asked() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let store = PostgresPartitions::outbox(pool.clone());
    let days = old_partitions(&store).await;

    let bus = maintained(store, retention(7, Expiry::Drop)).await;

    for day in days {
        let name = partition("outbox_messages", day);
        eventually("the partition to be dropped", async || {
            !exists(&pool, &name).await
        })
        .await;
    }
    assert!(attached(&pool, &partition("outbox_messages", today())).await);
    bus.shutdown().await;
}

#[tokio::test]
async fn a_partition_with_pending_or_failed_outbox_rows_survives_its_retention() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let store = PostgresPartitions::outbox(pool.clone());
    let [pending, failed, dispatched] = old_partitions(&store).await;
    outbox_row(&pool, pending, "pending").await;
    outbox_row(&pool, failed, "failed").await;
    outbox_row(&pool, dispatched, "dispatched").await;

    let bus = maintained(store, retention(7, Expiry::Drop)).await;

    let name = partition("outbox_messages", dispatched);
    eventually("the dispatched partition to be dropped", async || {
        !exists(&pool, &name).await
    })
    .await;
    assert!(attached(&pool, &partition("outbox_messages", pending)).await);
    assert!(attached(&pool, &partition("outbox_messages", failed)).await);
    bus.shutdown().await;
}

#[tokio::test]
async fn the_store_counts_the_rows_of_the_default_partition() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let store = PostgresPartitions::outbox(pool.clone());

    assert_eq!(store.default_rows().await.unwrap(), 0);
    outbox_row(&pool, Date::new(2000, 1, 1).unwrap(), "dispatched").await;

    assert_eq!(store.default_rows().await.unwrap(), 1);
}
