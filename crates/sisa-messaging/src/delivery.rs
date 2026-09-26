//! Transport-neutral inbound delivery and settlement capabilities.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::num::NonZeroU64;
use std::time::Duration;

use crate::{ErrorClassifier, FailureKind};

/// One owned transport delivery that can be split exactly once.
pub trait Delivery: Send + 'static {
    /// Owned transport wire representation consumed by an envelope mapper.
    type Wire: Send + 'static;
    /// Settlement handle retained independently from the wire value.
    type Settlement: Send + 'static;

    /// Splits wire data from its settlement handle.
    fn into_parts(self) -> (Self::Wire, Self::Settlement);
}

/// An operation that an individual-delivery transport may not support.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum IndividualCapability {
    /// Immediate requeue without a caller-selected delay.
    ImmediateRequeue,

    /// Redelivery after a caller-selected delay.
    DelayedRetry,

    /// Terminally discarding a delivery.
    TerminalDiscard,

    /// Extending the delivery deadline while processing continues.
    Heartbeat,
}

/// An individual-delivery source requirement that its descriptor did not satisfy.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum IndividualSourceRequirement {
    /// Immediate requeue is supported.
    ImmediateRequeue,

    /// A finite acknowledgement deadline is available.
    AckWait,

    /// A finite maximum delivery count is available.
    MaxDeliver,

    /// Delayed retry is supported.
    DelayedRetry,

    /// Terminal discard is supported.
    TerminalDiscard,

    /// Heartbeat acknowledgement is supported.
    Heartbeat,
}

/// A bounded error identifying a required individual-delivery capability that is absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnsupportedIndividualRequirement {
    requirement: IndividualSourceRequirement,
}

impl UnsupportedIndividualRequirement {
    /// Returns the unsatisfied requirement.
    #[must_use]
    pub const fn requirement(self) -> IndividualSourceRequirement {
        self.requirement
    }
}

impl fmt::Display for UnsupportedIndividualRequirement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("individual source does not satisfy a required capability")
    }
}

impl Error for UnsupportedIndividualRequirement {}

impl ErrorClassifier for UnsupportedIndividualRequirement {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// Requirements checked against an individual source before it begins receiving.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndividualSourceRequirements {
    ack_wait: bool,

    max_deliver: bool,

    delayed_retry: bool,

    immediate_requeue: bool,

    terminal_discard: bool,

    heartbeat: bool,
}

impl IndividualSourceRequirements {
    /// Creates a requirement set with no optional requirements.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ack_wait: false,
            max_deliver: false,
            delayed_retry: false,
            immediate_requeue: false,
            terminal_discard: false,
            heartbeat: false,
        }
    }

    /// Requires a finite acknowledgement deadline.
    #[must_use]
    pub const fn requiring_ack_wait(mut self) -> Self {
        self.ack_wait = true;

        self
    }

    /// Requires a finite delivery-count bound.
    #[must_use]
    pub const fn requiring_max_deliver(mut self) -> Self {
        self.max_deliver = true;

        self
    }

    /// Requires delayed retry.
    #[must_use]
    pub const fn requiring_delayed_retry(mut self) -> Self {
        self.delayed_retry = true;

        self
    }

    /// Requires immediate requeue.
    #[must_use]
    pub const fn requiring_immediate_requeue(mut self) -> Self {
        self.immediate_requeue = true;

        self
    }

    /// Requires terminal discard.
    #[must_use]
    pub const fn requiring_terminal_discard(mut self) -> Self {
        self.terminal_discard = true;

        self
    }

    /// Requires heartbeat acknowledgement.
    #[must_use]
    pub const fn requiring_heartbeat(mut self) -> Self {
        self.heartbeat = true;

        self
    }
}

/// Validated, immutable limits and operation support for an opened individual source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndividualSourceDescriptor {
    ack_wait: Option<Duration>,

    max_deliver: Option<NonZeroU64>,

    supports_delayed_retry: bool,

    supports_immediate_requeue: bool,

    supports_terminal_discard: bool,

    supports_heartbeat: bool,
}

impl IndividualSourceDescriptor {
    /// Constructs a descriptor. A configured acknowledgement deadline must be non-zero.
    pub fn new(
        ack_wait: Option<Duration>,
        max_deliver: Option<NonZeroU64>,
        supports_delayed_retry: bool,
        supports_terminal_discard: bool,
        supports_heartbeat: bool,
    ) -> Result<Self, IndividualSourceDescriptorError> {
        if ack_wait.is_some_and(|duration| duration.is_zero()) {
            return Err(IndividualSourceDescriptorError::ZeroAckWait);
        }

        Ok(Self {
            ack_wait,
            max_deliver,
            supports_delayed_retry,
            supports_immediate_requeue: false,
            supports_terminal_discard,
            supports_heartbeat,
        })
    }

    /// Returns the configured acknowledgement deadline, when one exists.
    #[must_use]
    pub const fn ack_wait(self) -> Option<Duration> {
        self.ack_wait
    }

    /// Returns the configured finite delivery-count bound, when one exists.
    #[must_use]
    pub const fn max_deliver(self) -> Option<NonZeroU64> {
        self.max_deliver
    }

    /// Reports whether delayed retry is supported.
    #[must_use]
    pub const fn supports_delayed_retry(self) -> bool {
        self.supports_delayed_retry
    }

    /// Advertises existing support for immediate requeue on this source.
    #[must_use]
    pub const fn with_immediate_requeue(mut self) -> Self {
        self.supports_immediate_requeue = true;

        self
    }

    /// Reports whether immediate requeue is supported.
    #[must_use]
    pub const fn supports_immediate_requeue(self) -> bool {
        self.supports_immediate_requeue
    }

    /// Reports whether terminal discard is supported.
    #[must_use]
    pub const fn supports_terminal_discard(self) -> bool {
        self.supports_terminal_discard
    }

    /// Reports whether heartbeat acknowledgement is supported.
    #[must_use]
    pub const fn supports_heartbeat(self) -> bool {
        self.supports_heartbeat
    }

    /// Checks startup requirements before the source begins receiving.
    pub fn validate(
        self,
        requirements: IndividualSourceRequirements,
    ) -> Result<(), UnsupportedIndividualRequirement> {
        let requirement = if requirements.ack_wait && self.ack_wait.is_none() {
            Some(IndividualSourceRequirement::AckWait)
        } else if requirements.max_deliver && self.max_deliver.is_none() {
            Some(IndividualSourceRequirement::MaxDeliver)
        } else if requirements.delayed_retry && !self.supports_delayed_retry {
            Some(IndividualSourceRequirement::DelayedRetry)
        } else if requirements.immediate_requeue && !self.supports_immediate_requeue {
            Some(IndividualSourceRequirement::ImmediateRequeue)
        } else if requirements.terminal_discard && !self.supports_terminal_discard {
            Some(IndividualSourceRequirement::TerminalDiscard)
        } else if requirements.heartbeat && !self.supports_heartbeat {
            Some(IndividualSourceRequirement::Heartbeat)
        } else {
            None
        };

        requirement.map_or(Ok(()), |requirement| {
            Err(UnsupportedIndividualRequirement { requirement })
        })
    }
}

/// An invalid individual source descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IndividualSourceDescriptorError {
    /// An acknowledgement deadline was configured as zero.
    ZeroAckWait,
}

impl fmt::Display for IndividualSourceDescriptorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("individual source descriptor contains an invalid limit")
    }
}

impl Error for IndividualSourceDescriptorError {}

impl ErrorClassifier for IndividualSourceDescriptorError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// An individual-source open error with a bounded capability failure or provider failure.
#[non_exhaustive]
pub enum IndividualSourceOpenError<E> {
    /// Opening the transport source failed.
    Source(E),

    /// The opened source did not meet a requested capability or finite limit.
    Unsupported(UnsupportedIndividualRequirement),
}

impl<E> fmt::Debug for IndividualSourceOpenError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(_) => formatter
                .debug_tuple("Source")
                .field(&"<redacted>")
                .finish(),
            Self::Unsupported(error) => formatter.debug_tuple("Unsupported").field(error).finish(),
        }
    }
}

impl<E: fmt::Display> fmt::Display for IndividualSourceOpenError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(_) => formatter.write_str("individual source failed to open"),
            Self::Unsupported(error) => error.fmt(formatter),
        }
    }
}

impl<E: Error + 'static> Error for IndividualSourceOpenError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            // Provider errors may contain credentials, payloads, or wire headers. Keep typed
            // access through the enum variant without exposing them to generic error renderers.
            Self::Source(_) => None,
            Self::Unsupported(error) => Some(error),
        }
    }
}

impl<E: ErrorClassifier> ErrorClassifier for IndividualSourceOpenError<E> {
    fn classify(&self) -> FailureKind {
        match self {
            Self::Source(error) => error.classify(),
            Self::Unsupported(error) => error.classify(),
        }
    }
}

/// A contract error for an unsupported individual-delivery operation or provider failure.
#[non_exhaustive]
pub enum IndividualSettlementError<E> {
    /// The opened transport does not provide the requested operation.
    Unsupported(IndividualCapability),

    /// The provider operation failed.
    Operation(E),
}

impl<E> fmt::Debug for IndividualSettlementError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(capability) => formatter
                .debug_tuple("Unsupported")
                .field(capability)
                .finish(),
            Self::Operation(_) => formatter
                .debug_tuple("Operation")
                .field(&"<redacted>")
                .finish(),
        }
    }
}

impl<E: fmt::Display> fmt::Display for IndividualSettlementError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(_) => {
                formatter.write_str("individual settlement operation is unsupported")
            }
            Self::Operation(_) => formatter.write_str("individual settlement operation failed"),
        }
    }
}

impl<E: Error + 'static> Error for IndividualSettlementError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            // Provider errors remain available through `Operation`, but are not safe to expose
            // through a generic error-chain renderer.
            Self::Unsupported(_) | Self::Operation(_) => None,
        }
    }
}

impl<E: ErrorClassifier> ErrorClassifier for IndividualSettlementError<E> {
    fn classify(&self) -> FailureKind {
        match self {
            Self::Unsupported(_) => FailureKind::Permanent,
            Self::Operation(error) => error.classify(),
        }
    }
}

/// An individual-delivery settlement handle.
///
/// Callers should acknowledge only after the consumer transaction has committed, and only a
/// broker-confirmed success confirms settlement to the caller. An error, timeout, or dropped
/// operation leaves its broker outcome unknown to the caller; the broker may have applied the
/// settlement without its confirmation reaching the caller, so redelivery may or may not occur.
/// Unsupported operations return a permanent, bounded contract error. Implementations must not
/// emulate an unsupported operation with a different broker action.
pub trait IndividualSettlement: Send + 'static {
    /// Provider error with an explicit retry decision.
    type Error: Error + Send + Sync + 'static + ErrorClassifier;

    /// Extends the delivery deadline while processing continues.
    ///
    /// Call periodically while work is in progress when the source descriptor advertises heartbeat
    /// support. Failure or cancellation does not resolve the delivery.
    fn heartbeat(
        &mut self,
    ) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send;

    /// Confirms successful processing after the consumer transaction has committed.
    ///
    /// A successful result means the broker confirmed settlement. An error, timeout, or dropped
    /// future leaves the outcome unknown to the caller: the broker may have applied the ack without
    /// its confirmation reaching the caller, so redelivery may or may not occur.
    fn ack(self)
    -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send;

    /// Requests redelivery after the supplied delay.
    ///
    /// Use only when delayed retry is advertised by the source descriptor. Unsupported delayed
    /// retry must return the bounded unsupported-operation error, not emulate a delay with another
    /// broker action. An error, timeout, or dropped future leaves settlement indeterminate.
    fn nak(
        self,
        delay: Duration,
    ) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send;

    /// Terminates redelivery for a poison or permanently failed delivery.
    ///
    /// Use only when terminal discard is advertised by the source descriptor. A successful result
    /// means the broker confirmed the terminal disposition. An error, timeout, or dropped future
    /// leaves settlement indeterminate.
    fn terminate(
        self,
    ) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send;
}

/// An inbound source with per-delivery settlement semantics.
pub trait IndividualDeliverySource: Send {
    /// Delivery yielded by the source, with its settlement statically bound to this profile.
    type Delivery: Delivery<Settlement: IndividualSettlement>;
    /// Fatal source error with an explicit retry decision.
    type Error: Error + Send + Sync + 'static + ErrorClassifier;

    /// Opens the source and validates its immutable descriptor against caller requirements.
    fn open(
        &mut self,
        requirements: IndividualSourceRequirements,
    ) -> impl Future<
        Output = Result<IndividualSourceDescriptor, IndividualSourceOpenError<Self::Error>>,
    > + Send;

    /// Waits cancel-safely for the next delivery; `None` means a clean source close.
    ///
    /// Dropping this future before it yields must not lose or settle a delivery.
    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<Option<Self::Delivery>, Self::Error>> + Send;
}

/// The result of attempting to advance a partitioned log position.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum PartitionAdvance {
    /// The resolved record advanced the committed position.
    Advanced,

    /// Fencing conclusively proved this operation did not advance the committed cursor.
    OwnershipLost,
}

/// A settlement handle for an ordered partition and offset.
///
/// The implementation owns the opaque partition, offset, and fencing generation. It may advance
/// only after the record has committed or received a durable terminal disposition. One unresolved
/// record may exist per partition; a later offset must not advance ahead of it. An error, timeout,
/// or dropped advance future is indeterminate and requires the source to pause that partition. The
/// source must not reconcile or redeliver while the corresponding operation can still take effect,
/// including while a pending or unpolled future remains live. It may resume only after the operation
/// is conclusively quiescent or an authoritative generation fence prevents any late effect, and
/// then only after reconciling the committed cursor and ownership generation. Reconciliation replays
/// when the cursor did not advance, continues when it did, and keeps the partition paused if either
/// fact cannot be established. Providers unable to fence and reconcile this state cannot implement
/// this profile.
pub trait PartitionedLogSettlement: Send + 'static {
    /// Partition identifier surfaced by the corresponding source.
    type Partition: Clone + Eq + Hash + Send + Sync + 'static;
    /// Advance error with an explicit retry decision.
    type Error: Error + Send + Sync + 'static + ErrorClassifier;

    /// Returns `Advanced` only after confirmed advancement. `OwnershipLost` is valid only when
    /// fencing proves that the cursor did not advance. Errors and cancellation are indeterminate;
    /// the source must pause and reconcile before delivering a later offset for this partition.
    fn advance(self) -> impl Future<Output = Result<PartitionAdvance, Self::Error>> + Send;

    /// Returns the opaque partition associated with this settlement handle.
    fn partition(&self) -> &Self::Partition;
}

/// One result from a partitioned log source.
#[derive(Debug)]
#[non_exhaustive]
pub enum PartitionedLogReceive<D, P> {
    /// A record with a settlement handle bound to its partition and offset.
    Delivery(D),

    /// The source lost ownership of this partition; no record was advanced.
    OwnershipLost(P),

    /// The source closed cleanly.
    Closed,
}

/// An inbound source with ordered, partition-scoped settlement semantics.
pub trait PartitionedLogDeliverySource: Send {
    /// Partition identifier returned with ownership-loss events.
    type Partition: Clone + Eq + Hash + Send + Sync + 'static;
    /// Delivery yielded by the source, with its settlement statically bound to this profile.
    type Delivery: Delivery<Settlement: PartitionedLogSettlement<Partition = Self::Partition>>;
    /// Fatal source error with an explicit retry decision.
    type Error: Error + Send + Sync + 'static + ErrorClassifier;

    /// Performs one-time source initialization.
    fn open(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Waits cancel-safely for a record, ownership loss, or clean source close.
    ///
    /// Dropping this readiness future must not lose or advance a record. The source must not emit a
    /// delivery for a new generation of a partition while an earlier advance is being reconciled.
    fn receive(
        &mut self,
    ) -> impl Future<
        Output = Result<PartitionedLogReceive<Self::Delivery, Self::Partition>, Self::Error>,
    > + Send;
}
