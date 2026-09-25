//! Direct OTel API instruments with a closed transport attribute vocabulary.
//!
//! Attributes never include exchange names, routing keys, queue names, or header values.

use std::{sync::OnceLock, time::Duration};

use opentelemetry::{
    InstrumentationScope, KeyValue,
    metrics::{Counter, Histogram},
};

use crate::RabbitMqError;

struct Instruments {
    sent: Counter<u64>,

    consumed: Counter<u64>,

    duration: Histogram<f64>,
}

static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

fn instruments() -> &'static Instruments {
    INSTRUMENTS.get_or_init(|| {
        let scope = InstrumentationScope::builder("messaging.rabbitmq")
            .with_schema_url("https://opentelemetry.io/schemas/1.42.0")
            .build();

        let meter = opentelemetry::global::meter_with_scope(scope);

        Instruments {
            sent: meter
                .u64_counter("messaging.client.sent.messages")
                .with_unit("{message}")
                .build(),
            consumed: meter
                .u64_counter("messaging.client.consumed.messages")
                .with_unit("{message}")
                .build(),
            duration: meter
                .f64_histogram("messaging.client.operation.duration")
                .with_unit("s")
                .build(),
        }
    })
}

/// A bounded `error.type` for a failed operation.
#[derive(Clone, Copy)]
pub(crate) enum Failure {
    /// A provider error.
    Provider(RabbitMqError),

    /// The profile does not support the requested settlement operation.
    Unsupported,
}

fn attributes(operation: &'static str, failure: Option<Failure>) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new("messaging.system", "rabbitmq"),
        KeyValue::new("messaging.operation.name", operation),
    ];

    if let Some(failure) = failure {
        let kind = match failure {
            Failure::Unsupported => "unsupported",
            Failure::Provider(error) => error_type(error),
        };

        attributes.push(KeyValue::new("error.type", kind));
    }

    attributes
}

fn error_type(error: RabbitMqError) -> &'static str {
    match error {
        RabbitMqError::Settings => "settings",
        RabbitMqError::Mapping => "mapping",
        RabbitMqError::PayloadTooLarge => "payload_too_large",
        RabbitMqError::Unroutable => "unroutable",
        RabbitMqError::Rejected => "rejected",
        RabbitMqError::Publish => "publish",
        RabbitMqError::Timeout => "timeout",
        RabbitMqError::Source => "source",
        RabbitMqError::Settlement => "settlement",
    }
}

pub(crate) fn finished(
    operation: &'static str,
    elapsed: Duration,
    result: Result<(), RabbitMqError>,
) {
    finished_with(operation, elapsed, result.map_err(Failure::Provider));
}

pub(crate) fn finished_with(
    operation: &'static str,
    elapsed: Duration,
    result: Result<(), Failure>,
) {
    let instruments = instruments();
    let succeeded = result.is_ok();
    let attributes = attributes(operation, result.err());

    instruments
        .duration
        .record(elapsed.as_secs_f64(), &attributes);

    if succeeded && operation == "receive" {
        instruments.consumed.add(1, &attributes);
    }
}

pub(crate) fn sent_attempted() {
    instruments().sent.add(1, &attributes("publish", None));
}

pub(crate) fn receive_closed(elapsed: Duration) {
    instruments()
        .duration
        .record(elapsed.as_secs_f64(), &attributes("receive", None));
}
