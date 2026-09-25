use sisa_messaging::{
    ContentType, EnvelopeMapper, HeaderName, HeaderValue, MessageId, MessageType, Metadata,
    MetadataValue, SerializedEnvelope,
};
use sisa_messaging_nats::{NatsMapper, Subject, TypeSubjectResolver};

fn fixture() -> SerializedEnvelope {
    let mut metadata = Metadata::default();
    metadata.delivery.deduplication_id = Some(MetadataValue::new("stable-retry-id").unwrap());

    metadata.trace.traceparent =
        Some(HeaderValue::new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01").unwrap());

    metadata
        .headers
        .insert(
            HeaderName::new("x-tenant-label").unwrap(),
            HeaderValue::new("public").unwrap(),
        )
        .unwrap();

    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("order_created").unwrap(),
        message_version: 2,
        content_type: ContentType::new("application/json").unwrap(),
        payload: br#"{"order":1}"#.to_vec(),
        metadata,
        ordering_key: None,
    }
}

#[test]
fn projection_round_trip_and_broker_dedup_id() {
    let mapper = NatsMapper::new(TypeSubjectResolver::new(Subject::new("events").unwrap()));
    let envelope = fixture();
    let wire = mapper.encode(&envelope).unwrap();
    assert_eq!(wire.subject.as_str(), "events.order_created.v2");

    assert_eq!(
        wire.headers.get("Nats-Msg-Id").unwrap().as_str(),
        "stable-retry-id"
    );

    assert_eq!(mapper.decode(wire).unwrap(), envelope);
}

#[test]
fn malformed_subjects_and_duplicate_identity_are_rejected_without_leakage() {
    for subject in [
        "",
        ".events",
        "events.",
        "events.*",
        "events.>",
        "events.bad token",
        "events.\nsecret",
    ] {
        let error = Subject::new(subject).err().unwrap();

        if !subject.is_empty() {
            assert!(!format!("{error:?} {error}").contains(subject));
        }
    }

    let mapper = NatsMapper::new(TypeSubjectResolver::new(Subject::new("events").unwrap()));
    let mut wire = mapper.encode(&fixture()).unwrap();
    wire.headers.append("Nats-Msg-Id", "forged");
    assert!(mapper.decode(wire).is_err());
}
