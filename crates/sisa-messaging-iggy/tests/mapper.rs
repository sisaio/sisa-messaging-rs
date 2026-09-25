use sisa_messaging::{
    ContentType, ConversationId, CorrelationMetadata, DeliveryMetadata, EnvelopeMapper, HeaderName,
    HeaderValue, Headers, MessageId, MessageType, Metadata, MetadataValue, OrderingKey, RequestId,
    RoutingMetadata, SerializedEnvelope, TraceMetadata,
};
use sisa_messaging_iggy::{IggyEnvelopeMapper, IggyHeader, IggyMappingError, IggyRecord};

fn envelope() -> SerializedEnvelope {
    let mut headers = Headers::new();
    headers
        .insert(
            HeaderName::new("x-request-zone").expect("valid fixture name"),
            HeaderValue::new("west").expect("valid fixture value"),
        )
        .expect("fixture header fits");

    SerializedEnvelope {
        message_id: "01890f52-7b00-7000-8000-000000000001"
            .parse::<MessageId>()
            .expect("valid fixed message id"),
        message_type: MessageType::new("orders.created").expect("valid fixture type"),
        message_version: 4,
        content_type: ContentType::new("application/json").expect("valid fixture type"),
        payload: br#"{"order":7}"#.to_vec(),
        metadata: Metadata {
            correlation: CorrelationMetadata {
                correlation_id: Some(MetadataValue::new("corr-1").expect("valid fixture id")),
                conversation_id: Some(
                    "01890f52-7b00-7000-8000-000000000002"
                        .parse::<ConversationId>()
                        .expect("valid fixed conversation id"),
                ),
                causation_id: Some(
                    "01890f52-7b00-7000-8000-000000000003"
                        .parse::<MessageId>()
                        .expect("valid fixed causation id"),
                ),
                request_id: Some(
                    "01890f52-7b00-7000-8000-000000000004"
                        .parse::<RequestId>()
                        .expect("valid fixed request id"),
                ),
            },
            trace: TraceMetadata {
                traceparent: Some(
                    HeaderValue::new("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                        .expect("valid traceparent"),
                ),
                tracestate: Some(HeaderValue::new("vendor=value").expect("valid tracestate")),
            },
            routing: RoutingMetadata {
                source: Some(MetadataValue::new("orders-api").expect("valid source")),
                destination: Some(MetadataValue::new("orders.created").expect("valid destination")),
                reply_to: Some(MetadataValue::new("orders.results").expect("valid reply-to")),
            },
            delivery: DeliveryMetadata {
                sent_at_ms: Some(1_700_000_000_000),
                deduplication_id: Some(MetadataValue::new("dedup-7").expect("valid dedup id")),
            },
            tenant_id: Some(MetadataValue::new("tenant-1").expect("valid tenant")),
            headers,
        },
        ordering_key: Some(OrderingKey::new("order-7").expect("valid fixture key")),
    }
}

fn expected_headers() -> Vec<IggyHeader> {
    [
        ("message-id", "01890f52-7b00-7000-8000-000000000001"),
        ("message-type", "orders.created"),
        ("message-version", "4"),
        ("content-type", "application/json"),
        ("ordering-key", "order-7"),
        ("correlation-id", "corr-1"),
        ("conversation-id", "01890f52-7b00-7000-8000-000000000002"),
        ("causation-id", "01890f52-7b00-7000-8000-000000000003"),
        ("request-id", "01890f52-7b00-7000-8000-000000000004"),
        ("source", "orders-api"),
        ("destination", "orders.created"),
        ("reply-to", "orders.results"),
        ("sent-at-ms", "1700000000000"),
        ("deduplication-id", "dedup-7"),
        ("tenant-id", "tenant-1"),
        (
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ),
        ("tracestate", "vendor=value"),
        ("x-request-zone", "west"),
    ]
    .into_iter()
    .map(|(name, value)| IggyHeader {
        name: name.to_owned(),
        value: value.as_bytes().to_vec(),
    })
    .collect()
}

#[test]
fn iggy_mapping_uses_stable_wire_headers_and_decodes_an_independent_record() {
    let expected = envelope();
    let mapper = IggyEnvelopeMapper;

    let encoded = mapper.encode(&expected).expect("fixture maps");

    assert_eq!(encoded.id, expected.message_id.as_uuid().as_u128());
    assert_eq!(encoded.payload, br#"{"order":7}"#);
    assert_eq!(encoded.headers, expected_headers());
    assert_eq!(encoded.key.as_deref(), Some(b"order-7".as_slice()));

    let hand_built_record = IggyRecord {
        id: expected.message_id.as_uuid().as_u128(),
        payload: br#"{"order":7}"#.to_vec(),
        headers: expected_headers(),
        key: Some(b"order-7".to_vec()),
    };
    let decoded = mapper
        .decode(hand_built_record)
        .expect("hand-built record decodes");

    assert_eq!(decoded, expected);
}

#[test]
fn iggy_mapping_rejects_duplicate_and_empty_headers_permanently() {
    let mapper = IggyEnvelopeMapper;
    let encoded = mapper.encode(&envelope()).expect("fixture maps");
    let mut duplicate = encoded.clone();
    duplicate.headers.push(duplicate.headers[0].clone());

    let Err(error) = mapper.decode(duplicate) else {
        panic!("duplicate framework header must fail");
    };

    assert_eq!(error, IggyMappingError::DuplicateHeader);
    assert_eq!(
        sisa_messaging::ErrorClassifier::classify(&error),
        sisa_messaging::FailureKind::Permanent
    );

    let mut empty_header = encoded;
    empty_header.headers[0].value = Vec::new();

    let empty_result = mapper.decode(empty_header);

    assert_eq!(empty_result, Err(IggyMappingError::InvalidHeader));
}

#[test]
fn iggy_mapping_rejects_a_record_id_that_disagrees_with_the_message_id_header() {
    let mapper = IggyEnvelopeMapper;
    let mut wire = mapper.encode(&envelope()).expect("fixture maps");
    wire.id ^= 1;

    let result = mapper.decode(wire);

    assert_eq!(result, Err(IggyMappingError::InvalidRecordId));
}

#[test]
fn iggy_mapping_rejects_an_ordering_key_over_the_messages_key_bound() {
    let mapper = IggyEnvelopeMapper;
    let mut oversized = envelope();
    oversized.ordering_key =
        Some(sisa_messaging::OrderingKey::new("x".repeat(256)).expect("fixture key is valid"));

    let result = mapper.encode(&oversized);

    assert_eq!(result, Err(IggyMappingError::InvalidOrderingKey));
}

#[test]
fn iggy_mapping_rejects_an_empty_payload() {
    let mapper = IggyEnvelopeMapper;
    let mut empty_payload = envelope();
    empty_payload.payload = Vec::new();

    let result = mapper.encode(&empty_payload);

    assert_eq!(result, Err(IggyMappingError::EmptyPayload));
}

#[test]
fn iggy_mapping_rejects_a_payload_over_the_message_bound() {
    let mapper = IggyEnvelopeMapper;
    let mut oversized_payload = envelope();
    oversized_payload.payload = vec![b'x'; 64_000_001];

    let result = mapper.encode(&oversized_payload);

    assert_eq!(result, Err(IggyMappingError::PayloadTooLarge));
}

#[test]
fn iggy_mapping_rejects_a_custom_header_value_over_the_iggy_bound_at_encode() {
    let mapper = IggyEnvelopeMapper;
    let mut oversized = envelope();
    let mut headers = Headers::new();
    headers
        .insert(
            HeaderName::new("x-oversized").expect("valid fixture name"),
            HeaderValue::new("x".repeat(300))
                .expect("fixture value fits the shared 8192-byte bound"),
        )
        .expect("fixture header fits the shared collection bound");
    oversized.metadata.headers = headers;

    let result = mapper.encode(&oversized);

    assert_eq!(result, Err(IggyMappingError::InvalidHeader));
}

#[test]
fn iggy_mapping_omits_an_oversized_tracestate_and_keeps_traceparent_required() {
    let mapper = IggyEnvelopeMapper;
    let mut long_tracestate = envelope();
    long_tracestate.metadata.trace.tracestate =
        Some(HeaderValue::new("v".repeat(300)).expect("fixture tracestate fits the shared bound"));

    let encoded = mapper
        .encode(&long_tracestate)
        .expect("an oversized tracestate is omitted, not rejected");

    assert!(
        !encoded
            .headers
            .iter()
            .any(|header| header.name == "tracestate"),
        "an oversized tracestate must not be written to the wire record"
    );
    assert!(
        encoded
            .headers
            .iter()
            .any(|header| header.name == "traceparent"),
        "traceparent stays required regardless of tracestate"
    );

    let decoded = mapper
        .decode(encoded)
        .expect("a record without tracestate decodes");

    assert_eq!(decoded.metadata.trace.tracestate, None);
    assert!(decoded.metadata.trace.traceparent.is_some());
}

#[test]
fn iggy_mapping_rejects_decode_of_a_record_missing_a_required_header() {
    let expected = envelope();
    let mapper = IggyEnvelopeMapper;
    let mut headers = expected_headers();
    headers.retain(|header| header.name != "content-type");

    let wire = IggyRecord {
        id: expected.message_id.as_uuid().as_u128(),
        payload: br#"{"order":7}"#.to_vec(),
        headers,
        key: Some(b"order-7".to_vec()),
    };

    let result = mapper.decode(wire);

    assert_eq!(result, Err(IggyMappingError::MissingRequiredHeader));
}

#[test]
fn iggy_mapping_rejects_a_non_numeric_message_version_permanently() {
    let expected = envelope();
    let mapper = IggyEnvelopeMapper;
    let mut headers = expected_headers();
    let version = headers
        .iter_mut()
        .find(|header| header.name == "message-version")
        .expect("fixture carries a message-version header");
    version.value = b"not-a-number".to_vec();

    let wire = IggyRecord {
        id: expected.message_id.as_uuid().as_u128(),
        payload: br#"{"order":7}"#.to_vec(),
        headers,
        key: Some(b"order-7".to_vec()),
    };

    let result = mapper.decode(wire);

    assert_eq!(result, Err(IggyMappingError::InvalidFrameworkValue));
}

#[test]
fn iggy_mapping_rejects_a_malformed_message_id_permanently() {
    let mapper = IggyEnvelopeMapper;
    let mut headers = expected_headers();
    let message_id = headers
        .iter_mut()
        .find(|header| header.name == "message-id")
        .expect("fixture carries a message-id header");
    message_id.value = b"not-a-uuid".to_vec();

    let wire = IggyRecord {
        id: 0,
        payload: br#"{"order":7}"#.to_vec(),
        headers,
        key: Some(b"order-7".to_vec()),
    };

    let result = mapper.decode(wire);

    assert_eq!(result, Err(IggyMappingError::InvalidFrameworkValue));
}

#[test]
fn iggy_mapping_rejects_more_than_the_combined_header_count_bound() {
    let mapper = IggyEnvelopeMapper;
    let headers: Vec<IggyHeader> = (0..85)
        .map(|index| IggyHeader {
            name: format!("x-bulk-{index}"),
            value: b"1".to_vec(),
        })
        .collect();

    let wire = IggyRecord {
        id: 0,
        payload: b"payload".to_vec(),
        headers,
        key: None,
    };

    let result = mapper.decode(wire);

    assert_eq!(result, Err(IggyMappingError::HeaderBoundsExceeded));
}

#[test]
fn iggy_mapping_rejects_custom_headers_exceeding_the_aggregate_byte_bound() {
    let mapper = IggyEnvelopeMapper;
    let headers: Vec<IggyHeader> = (0..40)
        .map(|index| IggyHeader {
            name: format!("x-big-{index}"),
            value: vec![b'x'; 2000],
        })
        .collect();

    let wire = IggyRecord {
        id: 0,
        payload: b"payload".to_vec(),
        headers,
        key: None,
    };

    let result = mapper.decode(wire);

    assert_eq!(result, Err(IggyMappingError::HeaderBoundsExceeded));
}

#[test]
fn iggy_mapping_rejects_an_uppercase_framework_header_name() {
    let mapper = IggyEnvelopeMapper;
    let mut headers = expected_headers();
    let message_type = headers
        .iter_mut()
        .find(|header| header.name == "message-type")
        .expect("fixture carries a message-type header");
    message_type.name = "Message-Type".to_owned();

    let wire = IggyRecord {
        id: 0,
        payload: br#"{"order":7}"#.to_vec(),
        headers,
        key: Some(b"order-7".to_vec()),
    };

    let result = mapper.decode(wire);

    assert_eq!(result, Err(IggyMappingError::InvalidHeader));
}

#[test]
fn iggy_mapping_rejects_case_insensitive_duplicate_custom_headers() {
    let mapper = IggyEnvelopeMapper;
    let mut headers = expected_headers();
    headers.push(IggyHeader {
        name: "X-A".to_owned(),
        value: b"first".to_vec(),
    });
    headers.push(IggyHeader {
        name: "x-a".to_owned(),
        value: b"second".to_vec(),
    });

    let wire = IggyRecord {
        id: 0,
        payload: br#"{"order":7}"#.to_vec(),
        headers,
        key: Some(b"order-7".to_vec()),
    };

    let result = mapper.decode(wire);

    assert_eq!(result, Err(IggyMappingError::DuplicateHeader));
}
