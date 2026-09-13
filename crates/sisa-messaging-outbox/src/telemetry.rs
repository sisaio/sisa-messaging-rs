//! Direct OpenTelemetry API instruments and bounded tracing events.

use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};

use sisa_messaging::FailureKind;

use crate::DeadReason;

struct Instruments {
    claimed: Counter<u64>,

    published: Counter<u64>,

    retried: Counter<u64>,

    dead: Counter<u64>,

    publish_duration: Histogram<f64>,

    in_flight: UpDownCounter<i64>,
}

static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

fn instruments() -> &'static Instruments {
    INSTRUMENTS.get_or_init(|| {
        let meter = opentelemetry::global::meter("messaging.outbox");

        Instruments {
            claimed: meter
                .u64_counter("outbox.claimed.messages")
                .with_unit("{message}")
                .build(),
            published: meter
                .u64_counter("outbox.published.messages")
                .with_unit("{message}")
                .build(),
            retried: meter
                .u64_counter("outbox.retried.messages")
                .with_unit("{message}")
                .build(),
            dead: meter
                .u64_counter("outbox.dead.messages")
                .with_unit("{message}")
                .build(),
            publish_duration: meter
                .f64_histogram("outbox.publish.duration")
                .with_unit("s")
                .build(),
            in_flight: meter
                .i64_up_down_counter("outbox.in.flight")
                .with_unit("{message}")
                .build(),
        }
    })
}

pub(crate) fn claimed(count: usize) {
    instruments().claimed.add(count as u64, &[]);
}

pub(crate) fn publish_started() {
    instruments().in_flight.add(1, &[]);
}

pub(crate) fn publish_stopped() {
    instruments().in_flight.add(-1, &[]);
}

pub(crate) fn publish_finished(duration: Duration, error_type: Option<&'static str>) {
    let attributes = error_type.map(|value| KeyValue::new("error.type", value));
    instruments()
        .publish_duration
        .record(duration.as_secs_f64(), attributes.as_slice());
}

pub(crate) fn published(count: usize) {
    instruments().published.add(count as u64, &[]);
}

pub(crate) fn retried(count: usize, kind: FailureKind) {
    instruments()
        .retried
        .add(count as u64, &[failure_attribute(kind)]);
}

pub(crate) fn dead(count: usize, reason: DeadReason) {
    instruments().dead.add(
        count as u64,
        &[KeyValue::new("dead.reason", reason.as_str())],
    );
}

fn failure_attribute(kind: FailureKind) -> KeyValue {
    let value = match kind {
        FailureKind::Transient => "transient",
        FailureKind::Permanent => "permanent",
        _ => "unknown",
    };

    KeyValue::new("failure.kind", value)
}
