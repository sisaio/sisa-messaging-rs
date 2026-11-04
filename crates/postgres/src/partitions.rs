//! The partitions of the outbox and inbox tables: their DDL and the maintenance lock.

use std::fmt;

use sisa_messaging_core::{
    Date, Expiry, PartitionInfo, PartitionInterval, PartitionLock, PartitionStore, StoreError,
};
use sqlx::{AssertSqlSafe, PgPool};

// Every range partition of the table, oldest first, with its bounds as UTC days. The bounds come
// back in the session's time zone with their offset, so the cast reads them as the same instant.
const PARTITIONS: &str = r"
    SELECT child.relname::text,
           to_char(bounds.lower AT TIME ZONE 'UTC', 'YYYY-MM-DD'),
           to_char(bounds.upper AT TIME ZONE 'UTC', 'YYYY-MM-DD')
    FROM pg_inherits
    JOIN pg_class child ON child.oid = inhrelid
    CROSS JOIN LATERAL (SELECT pg_get_expr(child.relpartbound, child.oid) AS bound) AS expression
    CROSS JOIN LATERAL (
        SELECT substring(expression.bound FROM 'FROM \(''([^'']*)''\)')::timestamptz AS lower,
               substring(expression.bound FROM 'TO \(''([^'']*)''\)')::timestamptz AS upper
    ) AS bounds
    WHERE inhparent = $1::regclass AND bounds.upper IS NOT NULL
    ORDER BY bounds.lower";

// How long expiring waits for the table's lock, which also holds up its writers meanwhile.
const LOCK_TIMEOUT: &str = "SET LOCAL lock_timeout = '10s'";

fn database(context: &'static str) -> impl FnOnce(sqlx::Error) -> StoreError {
    move |error| StoreError::Database {
        context,
        source: Box::new(error),
    }
}

fn date(text: &str) -> Result<Date, StoreError> {
    text.parse().map_err(|error| StoreError::Database {
        context: "reading a partition's bounds",
        source: Box::new(error),
    })
}

// A name in double quotes, for SQL that can't take it as a parameter.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Table {
    Outbox,
    Inbox,
}

impl Table {
    fn name(self) -> &'static str {
        match self {
            Self::Outbox => "outbox_messages",
            Self::Inbox => "inbox_receipts",
        }
    }
}

/// The partitions of the `outbox_messages` or the `inbox_receipts` table, which the bus's
/// maintenance worker keeps through [`PartitionStore`].
///
/// A partition holds whole UTC days and is named after its first one, such as
/// `outbox_messages_p20261102`. The tables are found through the connection's `search_path`, and
/// the maintenance lock is a session advisory lock on the table's object id, so two schemas in one
/// database are maintained apart.
///
/// ```no_run
/// use sisa_messaging_postgres::PostgresPartitions;
/// use sisa_messaging_runtime::{Bus, InMemoryTransport, PartitionSettings};
///
/// # async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// let mut outbox = PartitionSettings::default();
/// outbox.retention_days = Some(90);
///
/// let bus = Bus::builder("/orders", InMemoryTransport::new())
///     .partitions(PostgresPartitions::outbox(pool.clone()), outbox)
///     .partitions(PostgresPartitions::inbox(pool), PartitionSettings::default())
///     .start()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct PostgresPartitions {
    pool: PgPool,
    table: Table,
}

impl PostgresPartitions {
    /// The partitions of `outbox_messages`, on `pool`.
    #[must_use]
    pub fn outbox(pool: PgPool) -> Self {
        Self {
            pool,
            table: Table::Outbox,
        }
    }

    /// The partitions of `inbox_receipts`, on `pool`.
    #[must_use]
    pub fn inbox(pool: PgPool) -> Self {
        Self {
            pool,
            table: Table::Inbox,
        }
    }

    async fn partitions(&self) -> Result<Vec<PartitionInfo>, StoreError> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(PARTITIONS)
            .bind(self.table.name())
            .fetch_all(&self.pool)
            .await
            .map_err(database("listing the partitions"))?;
        rows.into_iter()
            .map(|(name, from, to)| Ok(PartitionInfo::new(name, date(&from)?, date(&to)?)))
            .collect()
    }

    async fn create(&self, from: Date, to: Date) -> Result<(), StoreError> {
        let name = format!(
            "{}_p{}",
            self.table.name(),
            from.to_string().replace('-', "")
        );
        let sql = format!(
            "CREATE TABLE {} PARTITION OF {} FOR VALUES FROM ('{from} 00:00+00') TO ('{to} 00:00+00')",
            quoted(&name),
            self.table.name(),
        );
        sqlx::raw_sql(AssertSqlSafe(sql))
            .execute(&self.pool)
            .await
            .map_err(database("creating a partition"))?;
        Ok(())
    }

    async fn count_unsent(&self, partition: &mut PartitionInfo) -> Result<(), StoreError> {
        let sql = format!(
            "SELECT count(*) FILTER (WHERE status = 'pending'), count(*) FILTER (WHERE status = 'failed') FROM {}",
            quoted(&partition.name)
        );
        let (pending, failed): (i64, i64) = sqlx::query_as(AssertSqlSafe(sql))
            .fetch_one(&self.pool)
            .await
            .map_err(database("counting a partition's unsent rows"))?;
        partition.pending = pending.unsigned_abs();
        partition.failed = failed.unsigned_abs();
        Ok(())
    }
}

// Shows the table; the pool stays out of it.
impl fmt::Debug for PostgresPartitions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresPartitions")
            .field("table", &self.table.name())
            .finish_non_exhaustive()
    }
}

// Runs the DDL for the bus's maintenance worker.
impl PartitionStore for PostgresPartitions {
    async fn try_lock(&self) -> Result<Option<PartitionLock>, StoreError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(database("taking the partition lock"))?;
        let locked: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_lock($1::regclass::oid::bigint)")
                .bind(self.table.name())
                .fetch_one(&mut *connection)
                .await
                .map_err(database("taking the partition lock"))?;
        if !locked {
            return Ok(None);
        }
        // The lock belongs to the session, so the connection closes when the lock drops instead of
        // going back to the pool still holding it.
        connection.close_on_drop();
        Ok(Some(PartitionLock::new(connection)))
    }

    async fn ensure_partitions(
        &self,
        interval: PartitionInterval,
        from: Date,
        count: u32,
    ) -> Result<(), StoreError> {
        let mut existing = self.partitions().await?;
        let mut first_error = None;
        let mut start = interval.start(from);
        for _ in 0..count {
            let end = interval.end(start);
            // Fill each stretch of [start, end) that no partition holds yet.
            let mut day = start;
            while day < end {
                if let Some(holder) = existing
                    .iter()
                    .find(|partition| partition.from <= day && day < partition.to)
                {
                    day = holder.to;
                    continue;
                }
                let gap_end = existing
                    .iter()
                    .map(|partition| partition.from)
                    .filter(|&next| next > day)
                    .min()
                    .map_or(end, |next| next.min(end));
                match self.create(day, gap_end).await {
                    Ok(()) => existing.push(PartitionInfo::new(String::new(), day, gap_end)),
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
                day = gap_end;
            }
            start = end;
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn default_rows(&self) -> Result<u64, StoreError> {
        let default: Option<String> = sqlx::query_scalar(
            "SELECT partdefid::regclass::text FROM pg_partitioned_table
             WHERE partrelid = $1::regclass AND partdefid <> 0",
        )
        .bind(self.table.name())
        .fetch_optional(&self.pool)
        .await
        .map_err(database("finding the default partition"))?;
        let Some(default) = default else {
            return Ok(0);
        };
        // The regclass text is already quoted where it needs to be.
        let rows: i64 =
            sqlx::query_scalar(AssertSqlSafe(format!("SELECT count(*) FROM {default}")))
                .fetch_one(&self.pool)
                .await
                .map_err(database("counting the default partition's rows"))?;
        Ok(rows.unsigned_abs())
    }

    async fn expirable(&self, older_than: Date) -> Result<Vec<PartitionInfo>, StoreError> {
        let mut expirable: Vec<PartitionInfo> = self
            .partitions()
            .await?
            .into_iter()
            .filter(|partition| partition.to <= older_than)
            .collect();
        if self.table == Table::Outbox {
            for partition in &mut expirable {
                self.count_unsent(partition).await?;
            }
        }
        Ok(expirable)
    }

    async fn expire(&self, partition: &PartitionInfo, mode: Expiry) -> Result<(), StoreError> {
        let statement = match mode {
            Expiry::Drop => format!("DROP TABLE {}", quoted(&partition.name)),
            _ => format!(
                "ALTER TABLE {} DETACH PARTITION {}",
                self.table.name(),
                quoted(&partition.name)
            ),
        };
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(database("expiring a partition"))?;
        sqlx::raw_sql(LOCK_TIMEOUT)
            .execute(&mut *transaction)
            .await
            .map_err(database("expiring a partition"))?;
        sqlx::raw_sql(AssertSqlSafe(statement))
            .execute(&mut *transaction)
            .await
            .map_err(database("expiring a partition"))?;
        transaction
            .commit()
            .await
            .map_err(database("expiring a partition"))
    }
}
