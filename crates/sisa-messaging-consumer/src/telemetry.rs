//! Direct OpenTelemetry metrics and bounded, redacted tracing events.
//!
//! Events carry only stable labels, the consumer's static message type and version, message
//! identities, attempt counts, failure classifications, and dead reasons. Payloads, metadata and
//! header values, scope, stream, or consumer names, and every provider, handler, mapper, codec, or
//! source rendering are never recorded.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use opentelemetry::Context;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use opentelemetry::trace::{
    Link, SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags, TraceId, TraceState, Tracer,
};
use sisa_messaging::{FailureKind, MessageId, Metadata};
use sisa_messaging_inbox::DeadReason;

use crate::ConsumerErrorKind;
use crate::consumer::settlement::{IndividualAction, Resolution, SettlementFailure};

const TARGET: &str = "messaging.consumer";
const IN_FLIGHT_METRIC: &str = "consumer.in.flight";

struct Instruments {
    processed: Counter<u64>,

    duplicate: Counter<u64>,

    dead: Counter<u64>,

    process_duration: Histogram<f64>,

    in_flight: UpDownCounter<i64>,
}

static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

fn instruments() -> &'static Instruments {
    INSTRUMENTS.get_or_init(|| {
        let meter = opentelemetry::global::meter(TARGET);

        Instruments {
            processed: meter
                .u64_counter("consumer.processed.messages")
                .with_unit("{message}")
                .build(),
            duplicate: meter
                .u64_counter("consumer.duplicate.messages")
                .with_unit("{message}")
                .build(),
            dead: meter
                .u64_counter("consumer.dead.messages")
                .with_unit("{message}")
                .build(),
            process_duration: meter
                .f64_histogram("messaging.process.duration")
                .with_unit("s")
                .build(),
            in_flight: meter
                .i64_up_down_counter(IN_FLIGHT_METRIC)
                .with_unit("{message}")
                .build(),
        }
    })
}

pub(crate) fn processed() {
    instruments().processed.add(1, &[]);
}

pub(crate) fn duplicate(state: &'static str) {
    instruments()
        .duplicate
        .add(1, &[KeyValue::new("duplicate.state", state)]);
}

pub(crate) fn dead(reason: DeadReason) {
    instruments()
        .dead
        .add(1, &[KeyValue::new("dead.reason", reason.as_str())]);
}

fn process_duration(duration: Duration, error_type: Option<&'static str>) {
    let operation = KeyValue::new("messaging.operation.name", "process");

    match error_type {
        Some(error_type) => instruments().process_duration.record(
            duration.as_secs_f64(),
            &[operation, KeyValue::new("error.type", error_type)],
        ),
        None => instruments()
            .process_duration
            .record(duration.as_secs_f64(), &[operation]),
    }
}

fn remote_span_context(metadata: &Metadata) -> Option<SpanContext> {
    let parent = metadata.trace.traceparent.as_ref()?.as_str();
    let mut parts = parent.split('-');

    let (Some("00"), Some(trace), Some(span), Some(flags), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };

    let hex = |text: &str, length: usize| {
        text.len() == length
            && text
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };

    if !hex(trace, 32) || !hex(span, 16) || !hex(flags, 2) {
        return None;
    }

    let trace_id = TraceId::from_hex(trace).ok()?;
    let span_id = SpanId::from_hex(span).ok()?;
    let flags = u8::from_str_radix(flags, 16).ok()?;

    let state = metadata
        .trace
        .tracestate
        .as_ref()
        .and_then(|value| value.as_str().parse::<TraceState>().ok())
        .unwrap_or_default();

    let context = SpanContext::new(trace_id, span_id, TraceFlags::new(flags), true, state);

    context.is_valid().then_some(context)
}

/// One process span per received delivery. A static message contract stands in for the
/// destination template, so dynamic broker routing never enters its name or attributes.
pub(crate) struct ProcessingSpan {
    context: Context,
}

impl ProcessingSpan {
    pub(crate) fn new(
        message: MessageLabels,
        metadata: Option<&Metadata>,
        ambient: &Context,
    ) -> Self {
        let remote = metadata.and_then(remote_span_context);
        let ambient_binding = ambient.span();
        let ambient_span = ambient_binding.span_context();
        let valid_ambient = ambient_span.is_valid().then(|| ambient_span.clone());

        let parent = remote.as_ref().map_or_else(
            || ambient.clone(),
            |remote| Context::new().with_remote_span_context(remote.clone()),
        );

        let mut builder = opentelemetry::global::tracer(TARGET)
            .span_builder(format!("process {}", message.message_type))
            .with_kind(SpanKind::Consumer)
            .with_attributes([
                KeyValue::new("messaging.operation.name", "process"),
                KeyValue::new("message.type", message.message_type),
                KeyValue::new("message.version", i64::from(message.version)),
            ]);

        if remote.is_some()
            && let Some(ambient_span) = valid_ambient
        {
            builder = builder.with_links(vec![Link::with_context(ambient_span)]);
        }

        let tracer = opentelemetry::global::tracer(TARGET);
        let span = builder.start_with_context(&tracer, &parent);

        Self {
            context: parent.with_span(span),
        }
    }

    pub(crate) fn context(&self) -> Context {
        self.context.clone()
    }
}

impl Drop for ProcessingSpan {
    fn drop(&mut self) {
        self.context.span().end();
    }
}

/// A bounded child operation. The owning future keeps this guard alive until the operation ends.
pub(crate) struct OperationSpan {
    context: Context,
}

impl OperationSpan {
    pub(crate) fn child(name: &'static str, kind: SpanKind) -> Self {
        Self::start(name, kind, &Context::current())
    }

    pub(crate) fn from_parent(name: &'static str, kind: SpanKind, parent: &Context) -> Self {
        Self::start(name, kind, parent)
    }

    fn start(name: &'static str, kind: SpanKind, parent: &Context) -> Self {
        let tracer = opentelemetry::global::tracer(TARGET);

        let span = tracer
            .span_builder(name)
            .with_kind(kind)
            .start_with_context(&tracer, parent);

        Self {
            context: parent.clone().with_span(span),
        }
    }

    pub(crate) fn context(&self) -> Context {
        self.context.clone()
    }
}

impl Drop for OperationSpan {
    fn drop(&mut self) {
        self.context.span().end();
    }
}

/// Counts a received delivery until its coordinator ends, including abort and panic paths.
pub(crate) struct InFlightGuard;

impl InFlightGuard {
    pub(crate) fn new() -> Self {
        instruments().in_flight.add(1, &[]);

        Self
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        instruments().in_flight.add(-1, &[]);
    }
}

/// Measures the database and handler workflow even when its task is aborted or panics.
pub(crate) struct ProcessingTimer {
    started: Instant,

    finished: bool,
}

impl ProcessingTimer {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            finished: false,
        }
    }

    pub(crate) fn finish(mut self, error_type: Option<&'static str>) {
        process_duration(self.started.elapsed(), error_type);
        self.finished = true;
    }
}

impl Drop for ProcessingTimer {
    fn drop(&mut self) {
        if !self.finished {
            process_duration(
                self.started.elapsed(),
                Some(if std::thread::panicking() {
                    "panic"
                } else {
                    "cancelled"
                }),
            );
        }
    }
}

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

/// Partition coordination reports only a fixed event name, never a partition or offset.
pub(crate) fn partition_event(message: MessageLabels, event: &'static str) {
    tracing::debug!(
        target: TARGET,
        {
            "message.version" = message.version,
            "message.type" = message.message_type,
            event,
        },
        "partition coordination"
    );
}

/// A failed heartbeat does not resolve or cancel the running delivery.
pub(crate) fn heartbeat_failed(
    message: MessageLabels,
    outcome: &'static str,
    failure: FailureKind,
) {
    tracing::warn!(
        target: TARGET,
        {
            "message.version" = message.version,
            "message.type" = message.message_type,
            outcome,
            "failure.kind" = failure_label(failure),
        },
        "delivery heartbeat unconfirmed"
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
        ConsumerErrorKind::HeartbeatDeadlineTooShort => "heartbeat_deadline_too_short",
        ConsumerErrorKind::Source => "source",
        ConsumerErrorKind::Inbox => "inbox",
        ConsumerErrorKind::FailureNotRecorded => "failure_not_recorded",
        ConsumerErrorKind::Settlement => "settlement",
        ConsumerErrorKind::PartitionOrder => "partition_order",
        ConsumerErrorKind::PartitionUnresolved => "partition_unresolved",
        ConsumerErrorKind::PartitionAdvanceUncertain => "partition_advance_uncertain",
        ConsumerErrorKind::OperatorActionRequired(_) => "operator_action_required",
        ConsumerErrorKind::HandlerPanicked => "handler_panicked",
        ConsumerErrorKind::ProviderPanicked => "provider_panicked",
        ConsumerErrorKind::Runtime => "runtime",
    }
}

#[cfg(test)]
mod tests {
    use super::{IN_FLIGHT_METRIC, remote_span_context};
    use sisa_messaging::{HeaderValue, Metadata};

    #[test]
    fn in_flight_name_and_remote_context_are_bounded() {
        assert_eq!(IN_FLIGHT_METRIC, "consumer.in.flight");
        let mut metadata = Metadata::default();

        metadata.trace.traceparent = Some(
            HeaderValue::new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01").unwrap(),
        );

        let parent = remote_span_context(&metadata).unwrap();
        assert!(parent.is_valid());
        assert!(parent.is_remote());

        metadata.trace.traceparent = Some(HeaderValue::new("00-secret-01-01").unwrap());
        assert!(remote_span_context(&metadata).is_none());
    }
}
