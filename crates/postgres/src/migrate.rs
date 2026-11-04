//! Creating the tables, once per database, safely from every instance of the application.

use sqlx::{AssertSqlSafe, PgConnection, PgPool};

use crate::PostgresSettings;

/// The version of the tables this release creates, kept in `schema_version`.
const VERSION: i32 = 1;

/// How many days after today get a partition, so the first writes of each day have one.
const DAYS_AHEAD: i32 = 7;

/// `name` as a PostgreSQL identifier, quoted so that any name works and none can inject SQL.
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The version 1 SQL with `schema` in place of the documented `sisa_messaging`.
fn schema_sql(schema: &str) -> String {
    include_str!("schema.sql").replace("sisa_messaging.", &format!("{}.", quote(schema)))
}

/// Creates the partitions of both tables for today and the next [`DAYS_AHEAD`] days, by UTC, that
/// don't exist yet. The database's clock picks today, so every instance agrees on it.
async fn create_partitions(connection: &mut PgConnection, schema: &str) -> sqlx::Result<()> {
    let days: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT to_char(day, 'YYYYMMDD'), day::text, (day + 1)::text
         FROM generate_series(0, $1) AS ahead,
              LATERAL (SELECT (now() AT TIME ZONE 'UTC')::date + ahead AS day) AS days",
    )
    .bind(DAYS_AHEAD)
    .fetch_all(&mut *connection)
    .await?;

    for table in ["outbox_messages", "inbox_receipts"] {
        for (suffix, from, to) in &days {
            let sql = format!(
                "CREATE TABLE IF NOT EXISTS {schema}.{partition} PARTITION OF {schema}.{table}
                 FOR VALUES FROM ('{from} 00:00+00') TO ('{to} 00:00+00')",
                schema = quote(schema),
                partition = quote(&format!("{table}_p{suffix}")),
                table = quote(table),
                from = from,
                to = to,
            );
            // The names are quoted and the bounds are dates the database made.
            sqlx::raw_sql(AssertSqlSafe(sql))
                .execute(&mut *connection)
                .await?;
        }
    }
    Ok(())
}

/// Creates the outbox and inbox tables in the configured schema, with their default partitions
/// and the daily partitions for today and the next 7 days, by UTC, and records the version in
/// `schema_version`.
///
/// Every instance can call it at startup: it runs in one transaction under an advisory lock, so
/// concurrent calls wait for each other, and a call on an up to date schema only adds the
/// partitions that are missing. It uses its own table for the version, not sqlx's
/// `_sqlx_migrations`, which belongs to the application.
///
/// Teams that run their own migrations create the same tables with this SQL, shown for the default
/// schema, followed by a partition per day such as:
///
/// ```sql
/// CREATE TABLE sisa_messaging.outbox_messages_p20261102 PARTITION OF sisa_messaging.outbox_messages
///     FOR VALUES FROM ('2026-11-02 00:00+00') TO ('2026-11-03 00:00+00');
/// ```
///
/// ```sql
#[doc = include_str!("schema.sql")]
/// ```
///
/// # Errors
///
/// The database's error when a statement fails, such as when the user may not create a schema.
pub async fn migrate(pool: &PgPool, settings: &PostgresSettings) -> sqlx::Result<()> {
    let schema = quote(&settings.schema);
    let mut tx = pool.begin().await?;

    // One lock per schema, held until the commit.
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('sisa_messaging.migrate.' || $1, 0))",
    )
    .bind(&settings.schema)
    .execute(&mut *tx)
    .await?;

    // Here and below, the only text put into the SQL is the quoted schema name.
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE SCHEMA IF NOT EXISTS {schema};
         CREATE TABLE IF NOT EXISTS {schema}.schema_version (
             version integer NOT NULL,
             CONSTRAINT pk_schema_version PRIMARY KEY (version)
         );"
    )))
    .execute(&mut *tx)
    .await?;

    let version: Option<i32> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT max(version) FROM {schema}.schema_version"
    )))
    .fetch_one(&mut *tx)
    .await?;
    if version.unwrap_or(0) < VERSION {
        sqlx::raw_sql(AssertSqlSafe(schema_sql(&settings.schema)))
            .execute(&mut *tx)
            .await?;
    }

    create_partitions(&mut tx, &settings.schema).await?;
    tx.commit().await
}
