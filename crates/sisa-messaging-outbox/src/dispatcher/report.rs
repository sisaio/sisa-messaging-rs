//! Database-confirmed dispatcher accounting.

/// Summary of locally observed, database-confirmed dispatcher work.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutboxRunReport {
    /// Healthy records accepted from successful claims.
    pub claimed: u64,

    /// Poison rows observed by provider claim operations.
    pub poisoned: u64,

    /// Broker acknowledgements whose completion transition was confirmed.
    pub completed: u64,

    /// Retry transitions confirmed by the store.
    pub retried: u64,

    /// Dead transitions confirmed by the store.
    pub dead: u64,

    /// Requested fenced writes that no longer matched ownership.
    pub fenced: u64,

    /// Claims successfully released during ambiguity or shutdown cleanup.
    pub released: u64,

    /// Publish tasks aborted after lease loss or drain expiry.
    pub aborted: u64,

    /// Timed-out or transient store calls suppressed for lease recovery.
    pub store_failures: u64,
}
