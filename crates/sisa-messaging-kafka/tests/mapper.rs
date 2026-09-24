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
        message_id: MessageId::new(),
        message_type: MessageType::new("orders.created").expect("valid fixture type"),
        message_version: 4,
        content_type: ContentType::new("application/json").expect("valid fixture type"),
        payload: br#"{"order":7}"#.to_vec(),
        metadata: Metadata {
            correlation: CorrelationMetadata {
                correlation_id: Some(MetadataValue::new("corr-1").expect("valid fixture id")),
                conversation_id: Some(ConversationId::new()),
                causation_id: Some(MessageId::new()),
                request_id: Some(RequestId::new()),
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
fn kafka_mapping_round_trips_shared_envelope_and_ordering_key() {
    let expected = envelope();
    let mapper = KafkaEnvelopeMapper;

    let wire = mapper.encode(&expected).expect("fixture maps");
    let key = wire.key.clone();
    let has_message_id = wire
        .headers
        .iter()
        .any(|header| header.name == "message-id");
    let has_custom_header = wire
        .headers
        .iter()
        .any(|header| header.name == "x-request-zone");
    let decoded = mapper.decode(wire).expect("wire decodes");

    assert_eq!(key.as_deref(), Some(b"order-7".as_slice()));
    assert!(has_message_id);
    assert!(has_custom_header);
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
