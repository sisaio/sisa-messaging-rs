//! Direct OTel API instruments with a closed transport attribute vocabulary.

use std::{sync::OnceLock, time::Duration};

use opentelemetry::{
    InstrumentationScope, KeyValue,
    metrics::{Counter, Histogram},
};

use crate::NatsError;

struct Instruments {
    sent: Counter<u64>,

    consumed: Counter<u64>,

    duration: Histogram<f64>,
}

static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

fn instruments() -> &'static Instruments {
    INSTRUMENTS.get_or_init(|| {
        let scope = InstrumentationScope::builder("messaging.nats")
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

fn attributes(operation: &'static str, error: Option<NatsError>) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new("messaging.system", "nats"),
        KeyValue::new("messaging.operation.name", operation),
    ];

    if let Some(error) = error {
        let kind = match error {
            NatsError::Settings => "settings",
            NatsError::Mapping => "mapping",
            NatsError::PayloadTooLarge => "payload_too_large",
            NatsError::Publish => "publish",
            NatsError::Timeout => "timeout",
            NatsError::Source => "source",
            NatsError::Settlement => "settlement",
        };

        attributes.push(KeyValue::new("error.type", kind));
    }

    attributes
}

pub(crate) fn finished(operation: &'static str, elapsed: Duration, result: Result<(), NatsError>) {
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
