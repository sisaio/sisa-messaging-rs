//! The partition contract: creating a table's partitions ahead and expiring old ones.

use std::any::Any;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{Date, StoreError};

/// How much time one partition of a table holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum PartitionInterval {
    /// A UTC day, the default.
    #[default]
    Day,
    /// An ISO 8601 week, from Monday.
    Week,
    /// A calendar month.
    Month,
}

impl PartitionInterval {
    /// The first day of the interval that holds `date`.
    ///
    /// ```
    /// use sisa_eventing_core::{Date, PartitionInterval};
    ///
    /// let thursday: Date = "2026-11-05".parse()?;
    /// assert_eq!(PartitionInterval::Week.start(thursday).to_string(), "2026-11-02");
    /// assert_eq!(PartitionInterval::Month.end(thursday).to_string(), "2026-12-01");
    /// # Ok::<(), sisa_eventing_core::EnvelopeError>(())
    /// ```
    #[must_use]
    pub fn start(self, date: Date) -> Date {
        match self {
            Self::Day => date,
            Self::Week => date.monday(),
            Self::Month => date.first_of_month(),
        }
    }

    /// The first day after the interval that holds `date`: the start of the next one.
    #[must_use]
    pub fn end(self, date: Date) -> Date {
        match self {
            Self::Day => date.add_days(1),
            Self::Week => date.monday().add_days(7),
            Self::Month => date.first_of_next_month(),
        }
    }
}

/// What happens to a partition once it is older than the retention.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Expiry {
    /// It leaves the table but stays as a table of its own, to archive or query. The default.
    #[default]
    Detach,
    /// It is dropped with its rows, which nothing can undo.
    Drop,
}

/// A partition the store found, with the outbox rows it still holds.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartitionInfo {
    /// The partition's table, such as `outbox_messages_p20261102`.
    pub name: String,
    /// The first day it holds.
    pub from: Date,
    /// The first day after it.
    pub to: Date,
    /// How many of its outbox rows are still `pending`; 0 for the inbox.
    pub pending: u64,
    /// How many of its outbox rows are `failed`; 0 for the inbox.
    pub failed: u64,
}

impl PartitionInfo {
    /// A partition holding the days from `from` up to, not including, `to`, with no pending or
    /// failed rows.
    #[must_use]
    pub fn new(name: impl Into<String>, from: Date, to: Date) -> Self {
        Self {
            name: name.into(),
            from,
            to,
            pending: 0,
            failed: 0,
        }
    }
}

/// Proof that this instance maintains a table's partitions: no other instance gets the lock
/// until this one is dropped.
pub struct PartitionLock {
    // What the store holds the lock with, such as the database session; dropping it unlocks.
    _guard: Box<dyn Any + Send>,
}

impl PartitionLock {
    /// A lock held as long as `guard` lives.
    #[must_use]
    pub fn new(guard: impl Any + Send) -> Self {
        Self {
            _guard: Box::new(guard),
        }
    }
}

// Shows only the type; the guard is the store's own.
impl fmt::Debug for PartitionLock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PartitionLock")
            .finish_non_exhaustive()
    }
}

/// The partitions of one time-partitioned table, such as the outbox or the inbox. A database
/// crate implements it with DDL; when to create and expire partitions is the runtime's.
///
/// A partition holds whole UTC days, from its first day up to, not including, its `to` day.
/// Rows are never deleted one by one: data leaves only when a whole partition expires.
pub trait PartitionStore: Send + Sync + 'static {
    /// Takes the table's maintenance lock, or returns `None` when another instance holds it.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when the database couldn't be asked.
    fn try_lock(&self) -> impl Future<Output = Result<Option<PartitionLock>, StoreError>> + Send;

    /// Makes sure `count` intervals of partitions exist, starting with the one that holds `from`.
    /// Days an existing partition already holds are left as they are, so a changed interval
    /// takes over where the old partitions end.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when a partition couldn't be created, such as when the default partition
    /// already holds rows of its days. The others are still created.
    fn ensure_partitions(
        &self,
        interval: PartitionInterval,
        from: Date,
        count: u32,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// How many rows the default partition holds: rows whose time had no partition.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when the database couldn't be asked.
    fn default_rows(&self) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// The partitions that hold only days before `older_than`, with their pending and failed
    /// outbox rows counted.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when the database couldn't be asked.
    fn expirable(
        &self,
        older_than: Date,
    ) -> impl Future<Output = Result<Vec<PartitionInfo>, StoreError>> + Send;

    /// Detaches or drops `partition`, one that [`expirable`](Self::expirable) returned. The
    /// runtime calls it only for a partition with no pending or failed rows.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when the partition couldn't be expired.
    fn expire(
        &self,
        partition: &PartitionInfo,
        mode: Expiry,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}
