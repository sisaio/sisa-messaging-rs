//! Bounded, redacted tracing events.
//!
//! Events carry only stable labels, the consumer's static message type and version, message
//! identities, attempt counts, failure classifications, and dead reasons. Payloads, metadata and
//! header values, scope, stream, or consumer names, and every provider, handler, mapper, codec, or
//! source rendering are never recorded.

use sisa_messaging::{FailureKind, MessageId};

use crate::ConsumerErrorKind;
use crate::consumer::settlement::{IndividualAction, Resolution, SettlementFailure};

const TARGET: &str = "messaging.consumer";

/// Static identity of the message type a consumer handles.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MessageLabels {
    pub(crate) message_type: &'static str,

    pub(crate) version: u32,
}

/// Per-delivery identity safe to emit.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DeliveryLabels {
    pub(crate) message: MessageLabels,

    pub(crate) message_id: Option<MessageId>,

    pub(crate) attempt: Option<u32>,
}

pub(crate) fn started(message: MessageLabels) {
    tracing::info!(
        target: TARGET,
        {
            "message.version" = message.version,
            "message.type" = message.message_type,
        },
        "consumer started"
    );
}

pub(crate) fn stopped(message: MessageLabels, outcome: &'static str) {
    tracing::info!(
        target: TARGET,
        {
            "message.version" = message.version,
            "message.type" = message.message_type,
            outcome,
        },
        "consumer stopped"
    );
}

pub(crate) fn resolved(
    labels: DeliveryLabels,
    resolution: Resolution,
    stage: &'static str,
    action: IndividualAction,
) {
    let failure = resolution.failure_kind().map(failure_label);

    let dead_reason = match resolution {
        Resolution::Dead(reason) => Some(reason.as_str()),
        _ => None,
    };

    tracing::debug!(
        target: TARGET,
        {
            "message.version" = labels.message.version,
            "message.type" = labels.message.message_type,
            "message.id" = labels.message_id.as_ref().map(tracing::field::display),
            attempt = labels.attempt,
            outcome = resolution.as_str(),
            stage,
            "failure.kind" = failure,
            "dead.reason" = dead_reason,
            action = action.as_str(),
        },
        "delivery resolved"
    );
}

pub(crate) fn settled(labels: DeliveryLabels, action: IndividualAction) {
    tracing::debug!(
        target: TARGET,
        {
            "message.version" = labels.message.version,
            "message.type" = labels.message.message_type,
            "message.id" = labels.message_id.as_ref().map(tracing::field::display),
            action = action.as_str(),
        },
        "delivery settled"
    );
}

pub(crate) fn settlement_failed(
    labels: DeliveryLabels,
    action: IndividualAction,
    failure: SettlementFailure,
    error_type: &'static str,
) {
    let reason = match failure {
        SettlementFailure::TimedOut => "timeout",
        SettlementFailure::Unsupported => "unsupported",
        SettlementFailure::Failed(_) => "failed",
    };

    tracing::warn!(
        target: TARGET,
        {
            "message.version" = labels.message.version,
            "message.type" = labels.message.message_type,
            "message.id" = labels.message_id.as_ref().map(tracing::field::display),
            action = action.as_str(),
            outcome = reason,
            "failure.kind" = failure_label(failure.failure_kind()),
            "error.type" = error_type,
        },
        "delivery settlement unconfirmed"
    );
}

pub(crate) fn cleanup_failed(labels: DeliveryLabels, stage: &'static str, kind: FailureKind) {
    tracing::warn!(
        target: TARGET,
        {
            "message.version" = labels.message.version,
            "message.type" = labels.message.message_type,
            "message.id" = labels.message_id.as_ref().map(tracing::field::display),
            stage,
            "failure.kind" = failure_label(kind),
        },
        "transaction rollback unconfirmed"
    );
}

pub(crate) fn task_failed(message: MessageLabels, kind: ConsumerErrorKind) {
    tracing::error!(
        target: TARGET,
        {
            "message.version" = message.version,
            "message.type" = message.message_type,
            "error.kind" = error_label(kind),
        },
        "consumer task failed; delivery left unsettled"
    );
}

pub(crate) fn stopping(message: MessageLabels, kind: ConsumerErrorKind, failure: FailureKind) {
    tracing::error!(
        target: TARGET,
        {
            "message.version" = message.version,
            "message.type" = message.message_type,
            "error.kind" = error_label(kind),
            "failure.kind" = failure_label(failure),
        },
        "consumer stopping"
    );
}

const fn failure_label(kind: FailureKind) -> &'static str {
    match kind {
        FailureKind::Transient => "transient",
        FailureKind::Permanent => "permanent",
        _ => "unknown",
    }
}

const fn error_label(kind: ConsumerErrorKind) -> &'static str {
    match kind {
        ConsumerErrorKind::SourceOpen => "source_open",
        ConsumerErrorKind::SourceOpenTimeout => "source_open_timeout",
        ConsumerErrorKind::Unsupported(_) => "unsupported",
        ConsumerErrorKind::AttemptBoundExceedsMaxDeliver => "attempt_bound_exceeds_max_deliver",
        ConsumerErrorKind::Source => "source",
        ConsumerErrorKind::Inbox => "inbox",
        ConsumerErrorKind::FailureNotRecorded => "failure_not_recorded",
        ConsumerErrorKind::Settlement => "settlement",
        ConsumerErrorKind::OperatorActionRequired(_) => "operator_action_required",
        ConsumerErrorKind::HandlerPanicked => "handler_panicked",
        ConsumerErrorKind::ProviderPanicked => "provider_panicked",
        ConsumerErrorKind::Runtime => "runtime",
    }
}
