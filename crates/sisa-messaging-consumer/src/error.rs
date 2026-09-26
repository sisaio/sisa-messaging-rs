//! Construction errors, clean exits, and fatal supervision failures.

use std::error::Error;
use std::fmt;

use sisa_messaging::{ErrorClassifier, FailureKind, IndividualSourceRequirement};
use sisa_messaging_inbox::DeadReason;

/// A consumer setting validated by [`Consumer::new`](crate::Consumer::new).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SettingsField {
    /// [`ConsumerSettings::source_timeout`](crate::ConsumerSettings::source_timeout).
    SourceTimeout,

    /// [`ConsumerSettings::database_timeout`](crate::ConsumerSettings::database_timeout).
    DatabaseTimeout,

    /// [`ConsumerSettings::settlement_timeout`](crate::ConsumerSettings::settlement_timeout).
    SettlementTimeout,

    /// [`ConsumerSettings::heartbeat_interval`](crate::ConsumerSettings::heartbeat_interval).
    HeartbeatInterval,

    /// [`ConsumerSettings::nak_delay`](crate::ConsumerSettings::nak_delay).
    NakDelay,

    /// [`ConsumerSettings::drain_timeout`](crate::ConsumerSettings::drain_timeout).
    DrainTimeout,
}

impl SettingsField {
    /// Returns the stable field name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceTimeout => "source_timeout",
            Self::DatabaseTimeout => "database_timeout",
            Self::SettlementTimeout => "settlement_timeout",
            Self::HeartbeatInterval => "heartbeat_interval",
            Self::NakDelay => "nak_delay",
            Self::DrainTimeout => "drain_timeout",
        }
    }
}

/// Invalid constructor-known consumer settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConsumerConfigError {
    /// A duration that must bound an operation or delay was zero.
    ZeroDuration(SettingsField),

    /// Immediate requeue was selected with a nonzero negative-acknowledgement delay.
    ImmediateRequeueRequiresZeroDelay,

    /// Individual acknowledgement heartbeat was selected for a partitioned source.
    HeartbeatRequiresIndividualSource,
}

impl fmt::Display for ConsumerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroDuration(field) => {
                write!(
                    formatter,
                    "consumer setting {} must be non-zero",
                    field.as_str()
                )
            }
            Self::ImmediateRequeueRequiresZeroDelay => {
                formatter.write_str("immediate requeue requires a zero nak_delay")
            }
            Self::HeartbeatRequiresIndividualSource => {
                formatter.write_str("heartbeat requires an individual delivery source")
            }
        }
    }
}

impl Error for ConsumerConfigError {}

impl ErrorClassifier for ConsumerConfigError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// A clean end of [`Consumer::run`](crate::Consumer::run).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ConsumerExit {
    /// The caller cancelled the consumer and the bounded drain finished.
    Cancelled,

    /// The source reported a clean close and the bounded drain finished.
    SourceClosed,
}

/// Why a delivery needs operator action before the consumer can continue.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum OperatorReason {
    /// The wire value had no trustworthy message identity, so no inbox row could record it.
    Malformed,

    /// The inbox holds a durable dead receipt for the delivery.
    Dead(DeadReason),
}

/// The decision boundary that ended [`Consumer::run`](crate::Consumer::run).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ConsumerErrorKind {
    /// Opening the source failed.
    SourceOpen,

    /// Opening the source exceeded `source_timeout`.
    SourceOpenTimeout,

    /// The opened source does not satisfy a requirement of the selected settlement mode.
    Unsupported(IndividualSourceRequirement),

    /// The inbox attempt bound exceeds the source's finite delivery bound.
    AttemptBoundExceedsMaxDeliver,

    /// The opened acknowledgement deadline is too short for the heartbeat interval.
    HeartbeatDeadlineTooShort,

    /// Receiving from the source failed.
    Source,

    /// A unit-of-work or inbox operation failed without a safe delivery disposition.
    Inbox,

    /// A handler failure could not be rolled back and recorded.
    FailureNotRecorded,

    /// A settlement operation failed permanently or is unsupported.
    Settlement,

    /// A later record arrived while its partition still had an unresolved record.
    PartitionOrder,

    /// An ordered record could not be durably resolved.
    PartitionUnresolved,

    /// Advance timed out or failed; the provider must fence and reconcile the cursor.
    PartitionAdvanceUncertain,

    /// A delivery requires operator action; it was left unsettled.
    OperatorActionRequired(OperatorReason),

    /// The application handler panicked; its transaction was dropped and its delivery left
    /// unsettled.
    HandlerPanicked,

    /// A processing task panicked outside handler execution, including framework and message
    /// drop code. Its transaction was dropped and its delivery left unsettled.
    ProviderPanicked,

    /// A consumer-internal task failed unexpectedly.
    Runtime,
}

type ProviderSource = Box<dyn Error + Send + Sync + 'static>;

/// A fatal consumer failure returned to the application supervisor.
///
/// [`Display`](fmt::Display) and [`Debug`](fmt::Debug) render only the stable kind and failure
/// classification, and [`Error::source`] is `None`, because provider errors may contain
/// credentials, payloads, or wire headers. The typed provider error remains available through
/// [`ConsumerError::provider_source`] for deliberate inspection or downcasting.
pub struct ConsumerError {
    kind: ConsumerErrorKind,

    failure: FailureKind,

    source: Option<ProviderSource>,
}

impl ConsumerError {
    pub(crate) fn new(
        kind: ConsumerErrorKind,
        failure: FailureKind,
        source: Option<ProviderSource>,
    ) -> Self {
        Self {
            kind,
            failure,
            source,
        }
    }

    /// Returns the decision boundary that ended the run.
    #[must_use]
    pub const fn kind(&self) -> ConsumerErrorKind {
        self.kind
    }

    /// Returns the retry classification of the underlying failure.
    #[must_use]
    pub const fn failure_kind(&self) -> FailureKind {
        self.failure
    }

    /// Borrows the typed provider error, when one caused this failure.
    ///
    /// Its rendering is provider-owned and may contain sensitive values; apply redaction before
    /// logging it.
    #[must_use]
    pub fn provider_source(&self) -> Option<&(dyn Error + Send + Sync + 'static)> {
        self.source.as_deref()
    }
}

impl fmt::Debug for ConsumerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerError")
            .field("kind", &self.kind)
            .field("failure", &self.failure)
            .field(
                "source",
                &self.source.as_ref().map(|_| "<redacted>").unwrap_or("None"),
            )
            .finish()
    }
}

impl fmt::Display for ConsumerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ConsumerErrorKind::SourceOpen => "consumer source failed to open",
            ConsumerErrorKind::SourceOpenTimeout => "consumer source open timed out",
            ConsumerErrorKind::Unsupported(_) => {
                "consumer source does not satisfy a settlement-mode requirement"
            }
            ConsumerErrorKind::AttemptBoundExceedsMaxDeliver => {
                "inbox attempt bound exceeds the source delivery bound"
            }
            ConsumerErrorKind::HeartbeatDeadlineTooShort => {
                "source acknowledgement deadline is too short for heartbeat"
            }
            ConsumerErrorKind::Source => "consumer source failed",
            ConsumerErrorKind::Inbox => "consumer inbox operation failed",
            ConsumerErrorKind::FailureNotRecorded => "consumer could not record a handler failure",
            ConsumerErrorKind::Settlement => "consumer settlement failed",
            ConsumerErrorKind::PartitionOrder => "partition delivered an overlapping record",
            ConsumerErrorKind::PartitionUnresolved => "partition record remains unresolved",
            ConsumerErrorKind::PartitionAdvanceUncertain => "partition advancement is uncertain",
            ConsumerErrorKind::OperatorActionRequired(_) => {
                "consumer delivery requires operator action"
            }
            ConsumerErrorKind::HandlerPanicked => "consumer handler panicked",
            ConsumerErrorKind::ProviderPanicked => "consumer processing task panicked",
            ConsumerErrorKind::Runtime => "consumer runtime failed",
        })
    }
}

impl Error for ConsumerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Provider errors stay reachable through `provider_source` but are not exposed to generic
        // error-chain renderers.
        None
    }
}

impl ErrorClassifier for ConsumerError {
    fn classify(&self) -> FailureKind {
        self.failure
    }
}
