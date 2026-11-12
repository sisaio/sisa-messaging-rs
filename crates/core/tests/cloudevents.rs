//! The envelope's attributes checked against the CloudEvents reference SDK: what we write is a
//! valid CloudEvent to it, and what it writes reads as an envelope.

use std::collections::BTreeMap;

use bytes::Bytes;
use cloudevents::{AttributesReader, Event, EventBuilder, EventBuilderV10};
use serde_json::{Map, Value};
use sisa_eventing_core::{
    ContentType, Correlation, Envelope, MessageId, MessageType, Source, TraceContext,
};

fn envelope() -> Envelope {
    let mut envelope = Envelope::new(
        MessageType::new("orders.order_placed.v1").unwrap(),
        Source::new("/orders").unwrap(),
        ContentType::new("application/json").unwrap(),
        Bytes::from_static(br#"{"id":42}"#),
    );
    envelope.subject = Some("Bestellung № 42".into());
    envelope.data_schema = Some("https://example.com/schemas/order_placed.v1.json".into());
    envelope.partition_key = Some("customer-7".into());
    envelope.correlation = Correlation::new(Some(MessageId::new()), Some(MessageId::new()));
    envelope.trace = Some(TraceContext::new(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        Some("rojo=00f067aa0ba902b7, congo=t61rcWkgMzE".into()),
    ));
    envelope.extensions.insert("tenant", "acme").unwrap();
    envelope
}

// The event in the SDK's JSON format, whose members are the attributes.
fn to_sdk(attributes: BTreeMap<String, String>) -> Event {
    let mut json: Map<String, Value> = attributes
        .into_iter()
        .map(|(name, value)| (name, Value::String(value)))
        .collect();
    json.insert("data".into(), serde_json::json!({ "id": 42 }));
    serde_json::from_value(Value::Object(json)).expect("the SDK reads the attributes")
}

// The attributes of an SDK event, from its JSON format without the data.
fn from_sdk(event: &Event) -> BTreeMap<String, String> {
    let Value::Object(json) = serde_json::to_value(event).unwrap() else {
        panic!("an event is a JSON object");
    };
    json.into_iter()
        .filter(|(name, _)| name != "data" && name != "data_base64")
        .map(|(name, value)| match value {
            Value::String(value) => (name, value),
            value => panic!("{name} is {value}, not a string"),
        })
        .collect()
}

#[test]
fn the_sdk_reads_every_attribute_we_write() {
    let envelope = envelope();

    let event = to_sdk(envelope.to_attributes());

    assert_eq!(event.id(), envelope.id.to_string());
    assert_eq!(event.source(), "/orders");
    assert_eq!(event.ty(), "orders.order_placed.v1");
    assert_eq!(event.datacontenttype(), Some("application/json"));
    assert_eq!(event.subject(), envelope.subject.as_deref());
    assert_eq!(
        event.dataschema().map(ToString::to_string),
        envelope.data_schema
    );
    assert_eq!(
        event.time().unwrap().timestamp_nanos_opt().map(i128::from),
        Some(envelope.time.unix_nanos())
    );
    for (name, value) in envelope.to_attributes() {
        if let Some(extension) = event.extension(&name) {
            assert_eq!(extension.to_string(), value, "{name}");
        }
    }
    // partitionkey, correlationid, causationid, traceparent, tracestate and tenant.
    assert_eq!(event.iter_extensions().count(), 6);
}

#[test]
fn what_the_sdk_writes_back_reads_as_the_same_envelope() {
    let envelope = envelope();
    let event = to_sdk(envelope.to_attributes());

    let read = Envelope::from_attributes(from_sdk(&event), envelope.data.clone()).unwrap();

    assert_eq!(read, envelope);
}

#[test]
fn an_event_built_by_the_sdk_reads_as_an_envelope() {
    let id = MessageId::new().to_string();
    let event = EventBuilderV10::new()
        .id(&id)
        .source("https://example.com/billing")
        .ty("billing.invoice_sent.v1")
        .time("2026-11-12T08:30:15.123456789+07:00")
        .subject("invoice 7")
        .extension("tenant", "acme")
        .data("application/json", serde_json::json!({ "id": 7 }))
        .build()
        .unwrap();

    let envelope = Envelope::from_attributes(from_sdk(&event), Bytes::new()).unwrap();

    assert_eq!(envelope.id.to_string(), id);
    assert_eq!(envelope.source.as_str(), "https://example.com/billing");
    assert_eq!(envelope.message_type.to_string(), "billing.invoice_sent.v1");
    assert_eq!(envelope.content_type.as_str(), "application/json");
    assert_eq!(envelope.subject.as_deref(), Some("invoice 7"));
    assert_eq!(envelope.extensions.get("tenant"), Some("acme"));
    assert_eq!(
        Some(envelope.time.unix_nanos()),
        event.time().unwrap().timestamp_nanos_opt().map(i128::from)
    );
    assert_eq!(envelope.time.to_string(), "2026-11-12T01:30:15.123456789Z");
}
