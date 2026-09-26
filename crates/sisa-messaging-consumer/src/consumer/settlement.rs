//! Centralized, pure settlement decision table.
//!
//! The workflow reduces every database and handler path to a profile-neutral [`Resolution`]. Each
//! delivery profile maps that resolution to its own private plan; the individual profile maps it
//! to [`IndividualPlan`] under the selected [`SettlementMode`]. No function here performs I/O.

use std::time::Duration;

use sisa_messaging::FailureKind;
use sisa_messaging_inbox::DeadReason;

use crate::{ConsumerErrorKind, OperatorReason, SettlementMode};

/// The durable state a workflow established for one delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Resolution {
    /// A commit succeeded, or the inbox reports a committed completion elsewhere.
    Completed,

    /// Another live transaction owns the delivery.
    InProgress,

    /// The rollback and a retryable failure record were both confirmed.
    RetryRecorded,

    /// The inbox holds a durable dead receipt.
    Dead(DeadReason),

    /// The wire value had no trustworthy identity; nothing was recorded.
    Malformed,

    /// A commit failed or timed out; its durable effect is unknown.
    CommitAmbiguous { kind: FailureKind },

    /// Begin, claim, or completion failed before any durable effect.
    Unresolved { kind: FailureKind },

    /// A handler failure was not confirmed rolled back and recorded.
    NotRecorded { kind: FailureKind },
}

impl Resolution {
    /// Returns the stable outcome label used in telemetry.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::InProgress => "in_progress",
            Self::RetryRecorded => "retry_recorded",
            Self::Dead(_) => "dead",
            Self::Malformed => "malformed",
            Self::CommitAmbiguous { .. } => "commit_ambiguous",
            Self::Unresolved { .. } => "unresolved",
            Self::NotRecorded { .. } => "not_recorded",
        }
    }

    /// Returns the classification of the failure that left this resolution, when one did.
    pub(crate) const fn failure_kind(self) -> Option<FailureKind> {
        match self {
            Self::CommitAmbiguous { kind }
            | Self::Unresolved { kind }
            | Self::NotRecorded { kind } => Some(kind),
            Self::Completed
            | Self::InProgress
            | Self::RetryRecorded
            | Self::Dead(_)
            | Self::Malformed => None,
        }
    }
}

/// A private individual-delivery settlement operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndividualAction {
    /// Confirm successful processing.
    Ack,

    /// Request delayed redelivery.
    Nak { delay: Duration },

    /// Terminally discard the delivery.
    Terminate,

    /// Drop the settlement handle without a broker operation.
    Leave,
}

impl IndividualAction {
    /// Returns the stable action label used in telemetry.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ack => "ack",
            Self::Nak { .. } => "nak",
            Self::Terminate => "terminate",
            Self::Leave => "leave",
        }
    }
}

/// Why a delivery stops the consumer after its settlement action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StopCause {
    /// A unit-of-work or inbox operation failed without a safe disposition.
    Inbox,

    /// A handler failure was not confirmed recorded.
    FailureNotRecorded,

    /// The delivery needs operator action.
    Operator(OperatorReason),
}

impl StopCause {
    pub(crate) const fn kind(self) -> ConsumerErrorKind {
        match self {
            Self::Inbox => ConsumerErrorKind::Inbox,
            Self::FailureNotRecorded => ConsumerErrorKind::FailureNotRecorded,
            Self::Operator(reason) => ConsumerErrorKind::OperatorActionRequired(reason),
        }
    }
}

/// One individual-delivery settlement plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndividualPlan {
    pub(crate) action: IndividualAction,

    pub(crate) stop: Option<StopCause>,
}

impl IndividualPlan {
    const fn settle(action: IndividualAction) -> Self {
        Self { action, stop: None }
    }

    const fn stop(cause: StopCause) -> Self {
        Self {
            action: IndividualAction::Leave,
            stop: Some(cause),
        }
    }
}

/// Maps a resolution to its individual-delivery plan.
///
/// Acknowledgement is planned only for [`Resolution::Completed`]. Non-transient failure kinds,
/// including unknown future kinds, fail closed by leaving the delivery and stopping.
pub(crate) fn decide_individual(
    mode: SettlementMode,
    nak_delay: Duration,
    resolution: Resolution,
) -> IndividualPlan {
    let retry = match mode {
        SettlementMode::Broker => IndividualAction::Nak { delay: nak_delay },
        SettlementMode::PendingRecovery => IndividualAction::Leave,
    };

    match resolution {
        Resolution::Completed => IndividualPlan::settle(IndividualAction::Ack),
        Resolution::InProgress | Resolution::RetryRecorded => IndividualPlan::settle(retry),
        Resolution::CommitAmbiguous { kind } | Resolution::Unresolved { kind } => {
            if kind.is_retryable() {
                IndividualPlan::settle(retry)
            } else {
                IndividualPlan::stop(StopCause::Inbox)
            }
        }
        Resolution::NotRecorded { kind } => match mode {
            SettlementMode::Broker if kind.is_retryable() => IndividualPlan::settle(retry),
            SettlementMode::Broker | SettlementMode::PendingRecovery => {
                IndividualPlan::stop(StopCause::FailureNotRecorded)
            }
        },
        Resolution::Dead(reason) => match mode {
            SettlementMode::Broker => IndividualPlan::settle(IndividualAction::Terminate),
            SettlementMode::PendingRecovery => {
                IndividualPlan::stop(StopCause::Operator(OperatorReason::Dead(reason)))
            }
        },
        Resolution::Malformed => match mode {
            SettlementMode::Broker => IndividualPlan::settle(IndividualAction::Terminate),
            SettlementMode::PendingRecovery => {
                IndividualPlan::stop(StopCause::Operator(OperatorReason::Malformed))
            }
        },
    }
}

/// How a settlement operation that did not confirm success ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettlementFailure {
    /// The operation exceeded `settlement_timeout`; its broker outcome is unknown.
    TimedOut,

    /// The source reported the operation as unsupported.
    Unsupported,

    /// The provider operation failed with this classification.
    Failed(FailureKind),
}

impl SettlementFailure {
    /// Reports whether this failure stops the consumer.
    ///
    /// A transient failure or timeout leaves the attempt to broker redelivery; a permanent,
    /// unsupported, or unknown classification stops receiving.
    pub(crate) const fn stops(self) -> bool {
        match self {
            Self::TimedOut => false,
            Self::Unsupported => true,
            Self::Failed(kind) => !kind.is_retryable(),
        }
    }

    pub(crate) const fn failure_kind(self) -> FailureKind {
        match self {
            Self::TimedOut => FailureKind::Transient,
            Self::Unsupported => FailureKind::Permanent,
            Self::Failed(kind) => kind,
        }
    }
}
