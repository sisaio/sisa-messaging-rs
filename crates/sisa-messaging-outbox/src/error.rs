//! Safe construction and terminal dispatcher errors.

use std::error::Error;
use std::fmt;

use crate::RetryPolicyError;

/// Invalid dispatcher construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum SettingsError {
    /// A required timeout or scheduling duration was zero.
    #[error("dispatcher setting {field} must be non-zero")]
    ZeroDuration {
        /// Stable field name; no caller data is rendered.
        field: &'static str,
    },

    /// Store calls could consume the complete safe renewal window.
    #[error("store timeout must be below half the lease")]
    StoreTimeoutNotBelowHalfLease,

    /// Capacity could not be represented by the bounded provider request.
    #[error("dispatcher capacity exceeds the provider request bound")]
    CapacityNotRepresentable,

    /// A configured monotonic timer deadline overflowed.
    #[error("dispatcher duration cannot be represented as a monotonic deadline")]
    DeadlineNotRepresentable,

    /// Worker identity was empty, excessive, or contained control bytes.
    #[error("dispatcher worker identity is invalid")]
    InvalidWorkerId,

    /// Retry-policy invariants were not satisfied.
    #[error("retry policy is invalid")]
    RetryPolicy(#[source] RetryPolicyError),
}

/// Terminal dispatcher failure returned to its supervisor.
///
/// Its outer [`Display`](fmt::Display) and [`Debug`](fmt::Debug) representations are bounded,
/// stable categories that never format provider or task errors. The original error remains
/// available through [`Error::source`] for typed inspection and downcasting. Callers that
/// recursively render foreign source chains are responsible for applying their own redaction.
#[derive(thiserror::Error)]
#[non_exhaustive]
pub enum DispatcherError<E>
where
    E: Error + Send + Sync + 'static,
{
    /// A permanently classified store failure stopped the worker.
    #[error("outbox store operation failed permanently")]
    Store(#[source] E),

    /// A publisher task panicked or was cancelled unexpectedly.
    #[error("outbox publisher task failed")]
    PublisherTask(#[source] tokio::task::JoinError),
}

impl<E> fmt::Debug for DispatcherError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(_) => formatter.write_str("DispatcherError::Store(permanent)"),
            Self::PublisherTask(_) => {
                formatter.write_str("DispatcherError::PublisherTask(unexpected)")
            }
        }
    }
}
