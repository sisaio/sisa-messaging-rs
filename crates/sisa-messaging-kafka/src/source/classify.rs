//! Error mapping for transactional offset commits and consumer events.
//!
//! The mapping is pure so each row is testable without a broker. Only a broker fence that
//! proves the commit did not apply may yield an ownership loss; every other uncertain outcome
//! reconciles behind a producer epoch fence.

use rdkafka::error::RDKafkaErrorCode;

/// The transactional step that failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Stage {
    Begin,

    SendOffsets,

    Commit,
}

/// The safe facts of one librdkafka transaction error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TxnFailure {
    pub(crate) code: RDKafkaErrorCode,

    pub(crate) fatal: bool,

    pub(crate) abortable: bool,
}

/// The conclusion for every advance in a failed transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Verdict {
    /// The group generation fence proves the offsets did not commit.
    OwnershipLost,

    /// The outcome is unknown until the committed cursor is read behind an epoch fence.
    Reconcile,

    /// The failure cannot resolve by retrying.
    Permanent,
}

/// What the member thread does next.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Disposition {
    /// Conclude without touching the transaction.
    Conclude(Verdict),

    /// Abort the transaction, then conclude with `then` when the abort succeeds or with
    /// `on_abort_failure` when it does not.
    Abort {
        then: Verdict,

        on_abort_failure: Verdict,
    },
}

/// Consumer-group fences returned for a stale transactional offset commit.
pub(crate) const GENERATION_FENCES: [RDKafkaErrorCode; 3] = [
    RDKafkaErrorCode::IllegalGeneration,
    RDKafkaErrorCode::UnknownMemberId,
    RDKafkaErrorCode::FencedInstanceId,
];

pub(crate) fn is_authorization(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::TransactionalIdAuthorizationFailed
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed
    )
}

/// Maps a failed transaction step to the next action.
pub(crate) fn transaction(stage: Stage, failure: TxnFailure) -> Disposition {
    if is_authorization(failure.code) {
        return if failure.abortable && !failure.fatal {
            Disposition::Abort {
                then: Verdict::Permanent,
                on_abort_failure: Verdict::Permanent,
            }
        } else {
            Disposition::Conclude(Verdict::Permanent)
        };
    }

    match stage {
        // Nothing was sent, but a failed begin leaves the producer state unknown; reconciling
        // replaces the producer and proves the cursor.
        Stage::Begin => Disposition::Conclude(Verdict::Reconcile),
        Stage::SendOffsets
            if GENERATION_FENCES.contains(&failure.code) && failure.abortable && !failure.fatal =>
        {
            Disposition::Abort {
                then: Verdict::OwnershipLost,
                on_abort_failure: Verdict::Reconcile,
            }
        }
        Stage::SendOffsets | Stage::Commit if failure.abortable && !failure.fatal => {
            Disposition::Abort {
                then: Verdict::Reconcile,
                on_abort_failure: Verdict::Reconcile,
            }
        }
        // Timeouts, retriable end-transaction failures, and fatal producer fences.
        Stage::SendOffsets | Stage::Commit => Disposition::Conclude(Verdict::Reconcile),
    }
}

/// The resolution of an aborted transaction.
pub(crate) const fn after_abort(disposition: Disposition, abort_succeeded: bool) -> Verdict {
    match disposition {
        Disposition::Conclude(verdict) => verdict,
        Disposition::Abort {
            then,
            on_abort_failure,
        } => {
            if abort_succeeded {
                then
            } else {
                on_abort_failure
            }
        }
    }
}

/// How the source treats an error reported by consumer polling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerVerdict {
    /// librdkafka recovers on its own; keep polling.
    Continue,

    /// Another live member with the same static identity replaced this one.
    InstanceFenced,

    /// The broker rejected group or topic authorization.
    Authorization,

    /// The consumer instance is unusable.
    Fatal,
}

pub(crate) fn consumer(code: RDKafkaErrorCode, fatal: bool) -> ConsumerVerdict {
    if code == RDKafkaErrorCode::FencedInstanceId {
        ConsumerVerdict::InstanceFenced
    } else if is_authorization(code) {
        ConsumerVerdict::Authorization
    } else if fatal || code == RDKafkaErrorCode::Fatal {
        ConsumerVerdict::Fatal
    } else {
        ConsumerVerdict::Continue
    }
}

/// Whether an error during open can resolve by retrying the open.
pub(crate) fn open_is_transient(code: RDKafkaErrorCode, fatal: bool) -> bool {
    !(fatal
        || is_authorization(code)
        || matches!(
            code,
            RDKafkaErrorCode::Fenced
                | RDKafkaErrorCode::ProducerFenced
                | RDKafkaErrorCode::InvalidTransactionTimeout
                | RDKafkaErrorCode::InvalidArgument
                | RDKafkaErrorCode::NotImplemented
                | RDKafkaErrorCode::UnsupportedVersion
        ))
}
