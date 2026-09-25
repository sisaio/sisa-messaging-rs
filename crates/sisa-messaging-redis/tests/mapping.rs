use sisa_messaging::{
    ContentType, EnvelopeMapper, ErrorClassifier, FailureKind, HeaderName, HeaderValue, Headers,
    MAX_CUSTOM_HEADER_BYTES, MessageId, MessageType, Metadata, SerializedEnvelope,
};
use sisa_messaging_redis::{RedisError, RedisMapper, RedisWire};

#[test]
fn unsupported_stream_command_is_permanent_and_redacted() {
    let error = RedisError::Unsupported;
    assert_eq!(error.classify(), FailureKind::Permanent);
    assert!(!error.to_string().contains("sensitive-payload"));
    assert_eq!(RedisError::Command.classify(), FailureKind::Transient);
}

#[test]
fn maximum_valid_headers_with_escaped_values_round_trip() {
    let mut headers = Headers::new();

    for index in 0..64 {
        let name = HeaderName::new(format!("k{index:03}")).unwrap();
        let value = HeaderValue::new("\\\"".repeat(510)).unwrap();
        headers.insert(name, value).unwrap();
    }

    assert_eq!(64 * (4 + 1020), MAX_CUSTOM_HEADER_BYTES);

    let envelope = SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("redis_boundary").unwrap(),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream").unwrap(),
        payload: vec![0, 1, 255],
        metadata: Metadata {
            headers,
            ..Metadata::default()
        },
        ordering_key: None,
    };

    let wire = RedisMapper.encode(&envelope).unwrap();
    assert!(wire.envelope.len() > 64 * 1024);
    assert!(wire.envelope.len() <= 1024 * 1024);
    assert_eq!(RedisMapper.decode(wire).unwrap(), envelope);
}

#[test]
fn malformed_wire_has_bounded_error() {
    let bad = RedisWire {
        version: b"2".to_vec(),
        envelope: b"sensitive raw header".to_vec(),
        payload: b"sensitive payload".to_vec(),
    };

    let error = RedisMapper.decode(bad).err().unwrap();
    assert!(!error.to_string().contains("sensitive"));
}
