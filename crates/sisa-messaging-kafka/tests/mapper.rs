use sisa_messaging::{
    ContentType, ConversationId, CorrelationMetadata, DeliveryMetadata, EnvelopeMapper, HeaderName,
    HeaderValue, Headers, MessageId, MessageType, Metadata, MetadataValue, OrderingKey, RequestId,
    RoutingMetadata, SerializedEnvelope, TraceMetadata,
};
use sisa_messaging_kafka::{KafkaEnvelopeMapper, KafkaHeader, KafkaMappingError, KafkaRecord};

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

#[test]
fn kafka_mapping_uses_stable_wire_headers_and_decodes_an_independent_record() {
    let expected = envelope();
    let mapper = KafkaEnvelopeMapper;

    let encoded = mapper.encode(&expected).expect("fixture maps");
    let expected_headers = vec![
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
    .map(|(name, value)| KafkaHeader {
        name: name.to_owned(),
        value: Some(value.as_bytes().to_vec()),
    })
    .collect::<Vec<_>>();

    assert_eq!(encoded.key.as_deref(), Some(b"order-7".as_slice()));
    assert_eq!(encoded.payload, br#"{"order":7}"#);
    assert_eq!(encoded.headers, expected_headers);

    let hand_built_record = KafkaRecord {
        key: Some(b"order-7".to_vec()),
        payload: br#"{"order":7}"#.to_vec(),
        headers: expected_headers,
    };
    let decoded = mapper
        .decode(hand_built_record)
        .expect("hand-built record decodes");

    assert_eq!(decoded, expected);
}

#[test]
fn kafka_mapping_rejects_duplicate_and_null_headers_permanently() {
    let mapper = KafkaEnvelopeMapper;
    let encoded = mapper.encode(&envelope()).expect("fixture maps");
    let mut duplicate = encoded.clone();
    duplicate.headers.push(duplicate.headers[0].clone());

    let Err(error) = mapper.decode(duplicate) else {
        panic!("duplicate framework header must fail");
    };

    assert_eq!(error, KafkaMappingError::DuplicateHeader);
    assert_eq!(
        sisa_messaging::ErrorClassifier::classify(&error),
        sisa_messaging::FailureKind::Permanent
    );

    let mut null_header = encoded;
    null_header.headers[0].value = None;

    let null_result = mapper.decode(null_header);

    assert_eq!(null_result, Err(KafkaMappingError::InvalidHeader));
}

#[test]
fn kafka_mapping_rejects_a_key_that_disagrees_with_ordering_metadata() {
    let mapper = KafkaEnvelopeMapper;
    let mut wire = mapper.encode(&envelope()).expect("fixture maps");
    wire.key = Some(b"another-order".to_vec());

    let result = mapper.decode(wire);

    assert_eq!(result, Err(KafkaMappingError::InvalidRecordKey));
}

#[test]
fn kafka_wire_header_keeps_nullable_values_for_decode_validation() {
    let _empty = KafkaRecord {
        key: None,
        payload: Vec::new(),
        headers: Vec::new(),
    };

    let header = KafkaHeader {
        name: "x-nullable".to_owned(),
        value: None,
    };
    let mut wire = KafkaEnvelopeMapper
        .encode(&envelope())
        .expect("fixture maps");
    wire.headers.push(header);

    let result = KafkaEnvelopeMapper.decode(wire);

    assert_eq!(result, Err(KafkaMappingError::InvalidHeader));
}
