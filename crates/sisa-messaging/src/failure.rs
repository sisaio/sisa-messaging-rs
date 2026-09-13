//! Explicit retry classification for errors crossing retry boundaries.

/// Whether an operation failure can be retried safely.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[non_exhaustive]
pub enum FailureKind {
    /// The operation may succeed when attempted again.
    Transient,

    /// Retrying the same operation is not expected to succeed.
    Permanent,
}

impl FailureKind {
    /// Reports whether retry is permitted.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Transient)
    }
}

/// Attaches an explicit retry decision to an error crossing a retry boundary.
pub trait Classify {
    /// Returns the structured retry classification.
    fn classify(&self) -> FailureKind;
}
