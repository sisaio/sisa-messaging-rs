//! Classified failure inputs and deterministic state reduction.

use sisa_messaging::{ErrorSummary, FailureKind};
#[cfg(test)]
use std::num::NonZeroU32;

/// Stable terminal category for a dead inbox receipt.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum DeadReason {
    /// A non-retryable handler failure was recorded.
    Permanent,

    /// A retryable failure reached the configured recorded-failure limit.
    Exhausted,
}

impl DeadReason {
    /// Returns the stable value intended for persistence and telemetry.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Permanent => "permanent",
            Self::Exhausted => "exhausted",
        }
    }
}

/// Classified and safely summarized handler failure recorded after transaction rollback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboxFailure {
    /// Explicit retry classification; providers never infer it from diagnostic text.
    pub kind: FailureKind,

    /// Caller-reviewed, UTF-8-boundary-bounded diagnostic summary.
    pub error: ErrorSummary,
}

/// Result of atomically recording one classified failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InboxFailureOutcome {
    /// The receipt remains eligible for a future delivery.
    Retry {
        /// Recorded failure count after this transition.
        attempts: u32,
    },

    /// The receipt became terminal.
    Dead {
        /// Recorded failure count after this transition.
        attempts: u32,

        /// Stable terminal category.
        reason: DeadReason,
    },

    /// Another transaction completed the receipt before this failure was recorded.
    CompletedDuplicate,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReceiptState {
    Pending,
    Retrying { attempts: u32 },
    Completed,
    Dead { attempts: u32, reason: DeadReason },
}

#[cfg(test)]
pub(crate) fn reduce_failure(
    state: ReceiptState,
    kind: FailureKind,
    max_attempts: NonZeroU32,
) -> (ReceiptState, InboxFailureOutcome) {
    match state {
        ReceiptState::Completed => (
            ReceiptState::Completed,
            InboxFailureOutcome::CompletedDuplicate,
        ),
        ReceiptState::Dead { attempts, reason } => (
            ReceiptState::Dead { attempts, reason },
            InboxFailureOutcome::Dead { attempts, reason },
        ),
        ReceiptState::Pending => reduce_active_failure(1, kind, max_attempts.get()),
        ReceiptState::Retrying { attempts } => {
            reduce_active_failure(attempts.saturating_add(1), kind, max_attempts.get())
        }
    }
}

#[cfg(test)]
fn reduce_active_failure(
    attempts: u32,
    kind: FailureKind,
    max_attempts: u32,
) -> (ReceiptState, InboxFailureOutcome) {
    let reason = match kind {
        FailureKind::Permanent => Some(DeadReason::Permanent),
        FailureKind::Transient if attempts >= max_attempts => Some(DeadReason::Exhausted),
        FailureKind::Transient => None,
        _ => Some(DeadReason::Permanent),
    };

    match reason {
        Some(reason) => (
            ReceiptState::Dead { attempts, reason },
            InboxFailureOutcome::Dead { attempts, reason },
        ),
        None => (
            ReceiptState::Retrying { attempts },
            InboxFailureOutcome::Retry { attempts },
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_reduction_preserves_terminal_states_and_exhausts_active_states() {
        let limits = [
            NonZeroU32::MIN,
            NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            NonZeroU32::new(u32::MAX).unwrap_or(NonZeroU32::MIN),
        ];

        let kinds = [FailureKind::Transient, FailureKind::Permanent];

        for limit in limits {
            for kind in kinds {
                let completed = ReceiptState::Completed;

                assert_eq!(
                    reduce_failure(completed, kind, limit),
                    (completed, InboxFailureOutcome::CompletedDuplicate)
                );

                let dead = ReceiptState::Dead {
                    attempts: 7,
                    reason: DeadReason::Permanent,
                };

                assert_eq!(
                    reduce_failure(dead, kind, limit),
                    (
                        dead,
                        InboxFailureOutcome::Dead {
                            attempts: 7,
                            reason: DeadReason::Permanent,
                        },
                    )
                );
            }
        }

        for limit in limits {
            for state in [
                ReceiptState::Pending,
                ReceiptState::Retrying { attempts: 1 },
                ReceiptState::Retrying {
                    attempts: limit.get().saturating_sub(1),
                },
                ReceiptState::Retrying { attempts: u32::MAX },
            ] {
                let attempts = match state {
                    ReceiptState::Pending => 1,
                    ReceiptState::Retrying { attempts } => attempts.saturating_add(1),
                    ReceiptState::Completed | ReceiptState::Dead { .. } => continue,
                };

                for kind in kinds {
                    let expected_reason = if kind.is_retryable() && attempts < limit.get() {
                        None
                    } else if kind.is_retryable() {
                        Some(DeadReason::Exhausted)
                    } else {
                        Some(DeadReason::Permanent)
                    };

                    let expected = match expected_reason {
                        Some(reason) => (
                            ReceiptState::Dead { attempts, reason },
                            InboxFailureOutcome::Dead { attempts, reason },
                        ),
                        None => (
                            ReceiptState::Retrying { attempts },
                            InboxFailureOutcome::Retry { attempts },
                        ),
                    };

                    assert_eq!(reduce_failure(state, kind, limit), expected);
                }
            }
        }
    }
}
