use std::error::Error;
use std::future::{Future, ready};
use std::num::NonZeroU64;
use std::str::FromStr;
use std::time::Duration;

use sisa_messaging::{
    ContentType, ConversationId, Delivery, DeliverySource, Envelope, EnvelopeMapper,
    ErrorClassifier, ErrorSummary, FailureKind, FrameworkHeader, HeaderName, HeaderNameError,
    HeaderValue, HeaderValueError, MAX_ERROR_SUMMARY_BYTES, Message, MessageId, MessageType,
    Metadata, MetadataValue, OrderingKey, Publisher, RequestId, SerializedEnvelope, Settlement,
    ValidationError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct OrderedMessage {
    order_id: OrderingKey,
}

impl Message for OrderedMessage {
    const TYPE: &'static str = "orders.created";
    const VERSION: u32 = 1;

    fn order_by(&self) -> Option<OrderingKey> {
        Some(self.order_id.clone())
    }
}

#[test]
fn semantic_ids_round_trip_without_becoming_interchangeable() {
    let raw = uuid::Uuid::parse_str("0198f3e2-40f0-7b15-8a4a-843d24f68d10").unwrap();
    let message = MessageId::from_uuid(raw);
    let conversation = ConversationId::from_uuid(raw);
    let request = RequestId::from_uuid(raw);

    assert_eq!(MessageId::from_str(&message.to_string()).unwrap(), message);
    assert_eq!(conversation.into_uuid(), raw);
    assert_eq!(*request.as_uuid(), raw);
}

#[test]
fn envelope_freezes_contract_and_ordering_values_at_construction() {
    let order_id = OrderingKey::new("order-42").unwrap();
    let envelope = Envelope::new(
        MessageId::new(),
        OrderedMessage {
            order_id: order_id.clone(),
        },
        Metadata::default(),
    )
    .unwrap();

    assert_eq!(envelope.message_type().as_str(), "orders.created");
    assert_eq!(envelope.message_version(), 1);
    assert_eq!(envelope.ordering_key(), Some(&order_id));
}

#[test]
fn bounded_values_reject_empty_control_and_oversized_input_without_echoing_it() {
    assert_eq!(MessageType::new(""), Err(ValidationError::Empty));
    assert_eq!(
        ContentType::new("application/json\rhidden"),
        Err(ValidationError::InvalidCharacter)
    );
    assert_eq!(
        MetadataValue::new("x".repeat(1_025)),
        Err(ValidationError::TooLong { max_bytes: 1_024 })
    );
    assert!(
        !ValidationError::InvalidCharacter
            .to_string()
            .contains("hidden")
    );

    for byte in (0_u8..=31).chain(std::iter::once(127)) {
        let value = String::from_utf8(vec![b'a', byte]).unwrap();

        assert_eq!(
            MessageType::new(value),
            Err(ValidationError::InvalidCharacter)
        );
    }
    assert!(OrderingKey::new("x".repeat(512)).is_ok());
    assert_eq!(
        OrderingKey::new("x".repeat(513)),
        Err(ValidationError::TooLong { max_bytes: 512 })
    );
}

#[test]
fn custom_headers_reject_injection_and_framework_collisions() {
    assert_eq!(
        HeaderName::new("message-id"),
        Err(HeaderNameError::Reserved)
    );
    assert_eq!(
        HeaderName::new("traceparent"),
        Err(HeaderNameError::Reserved)
    );
    assert_eq!(
        HeaderName::new("contains space"),
        Err(HeaderNameError::InvalidCharacter)
    );
    assert_eq!(
        HeaderValue::new("safe\r\nunsafe"),
        Err(HeaderValueError::Newline)
    );
    assert!(HeaderValue::new("Zażółć gęślą").is_ok());
    assert_eq!(
        HeaderName::new("X-Import-Batch").unwrap().as_str(),
        "x-import-batch"
    );
}

#[test]
fn framework_header_mapping_matches_the_frozen_fixture() {
    let actual = FrameworkHeader::ALL
        .iter()
        .map(|header| header.name())
        .collect::<Vec<_>>()
        .join("\n");
    let expected = include_str!("fixtures/framework-headers.txt").trim_end();

    assert_eq!(actual, expected);
}

#[test]
fn safe_error_summary_truncates_at_a_utf8_boundary() {
    let source = "é".repeat(MAX_ERROR_SUMMARY_BYTES);
    let summary = ErrorSummary::from_safe_text(&source);

    assert!(summary.as_str().len() <= MAX_ERROR_SUMMARY_BYTES);
    assert!(summary.as_str().is_char_boundary(summary.as_str().len()));
}

#[derive(Debug)]
struct SafeOuter(SafeInner);

impl std::fmt::Display for SafeOuter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("outer category")
    }
}

impl Error for SafeOuter {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

#[derive(Debug)]
struct SafeInner;

impl std::fmt::Display for SafeInner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("inner category")
    }
}

impl Error for SafeInner {}

#[test]
fn safe_error_summary_includes_a_bounded_source_chain() {
    let summary = ErrorSummary::from_safe_error(&SafeOuter(SafeInner));

    assert_eq!(summary.as_str(), "outer category: inner category");
}

#[derive(Debug)]
struct VeryLargeSafeDisplay;

impl std::fmt::Display for VeryLargeSafeDisplay {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for _ in 0..10_000 {
            formatter.write_str("é")?;
        }
        Ok(())
    }
}

impl Error for VeryLargeSafeDisplay {}

#[test]
fn safe_error_summary_stops_a_very_large_display_at_the_utf8_byte_bound() {
    let summary = ErrorSummary::from_safe_error(&VeryLargeSafeDisplay);

    assert_eq!(summary.as_str().len(), MAX_ERROR_SUMMARY_BYTES);
    assert!(summary.as_str().is_char_boundary(summary.as_str().len()));
}

#[derive(Debug)]
struct DeepSafeChain {
    source: Option<Box<Self>>,
}

impl std::fmt::Display for DeepSafeChain {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("node-é")
    }
}

impl Error for DeepSafeChain {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[test]
fn safe_error_summary_bounds_deep_source_chains_and_their_separators() {
    let mut chain = DeepSafeChain { source: None };
    for _ in 0..2_000 {
        chain = DeepSafeChain {
            source: Some(Box::new(chain)),
        };
    }

    let summary = ErrorSummary::from_safe_error(&chain);

    assert!(summary.as_str().len() <= MAX_ERROR_SUMMARY_BYTES);
    assert!(summary.as_str().is_char_boundary(summary.as_str().len()));
    assert!(summary.as_str().contains(": node-é"));
}

#[derive(Debug)]
struct ContractError;

impl std::fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("contract category")
    }
}

impl Error for ContractError {}

impl ErrorClassifier for ContractError {
    fn classify(&self) -> FailureKind {
        FailureKind::Transient
    }
}

struct ContractPublisher;

impl Publisher for ContractPublisher {
    type Error = ContractError;

    fn publish(
        &self,
        _envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }
}

struct ContractMapper;

impl EnvelopeMapper<Vec<u8>> for ContractMapper {
    type Error = ContractError;

    fn encode(&self, envelope: &SerializedEnvelope) -> Result<Vec<u8>, Self::Error> {
        Ok(envelope.payload.clone())
    }

    fn decode(&self, _wire: Vec<u8>) -> Result<SerializedEnvelope, Self::Error> {
        Err(ContractError)
    }
}

struct ContractSettlement;

impl Settlement for ContractSettlement {
    type Error = ContractError;

    fn heartbeat(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn ack(self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn nak(self, _delay: Duration) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn terminate(self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }
}

struct ContractDelivery(Vec<u8>, ContractSettlement);

impl Delivery for ContractDelivery {
    type Wire = Vec<u8>;
    type Settlement = ContractSettlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.0, self.1)
    }
}

struct ContractSource;

impl DeliverySource for ContractSource {
    type Delivery = ContractDelivery;
    type Error = ContractError;

    fn open(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<Option<Self::Delivery>, Self::Error>> + Send {
        ready(Ok(None))
    }

    fn ack_wait(&self) -> Option<Duration> {
        Some(Duration::from_secs(30))
    }

    fn max_deliver(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(5)
    }
}

fn assert_send<T: Send>(_: T) {}

#[test]
fn async_capabilities_use_send_native_futures_and_static_dispatch() {
    let serialized = SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("contracts.test").unwrap(),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream").unwrap(),
        payload: Vec::new(),
        metadata: Metadata::default(),
        ordering_key: None,
    };
    assert_send(ContractPublisher.publish(&serialized));

    let mut source = ContractSource;

    assert_send(source.open());
    assert_send(source.receive());
    assert_eq!(source.ack_wait(), Some(Duration::from_secs(30)));
    assert_eq!(source.max_deliver(), NonZeroU64::new(5));

    let mut settlement = ContractSettlement;

    assert_send(settlement.heartbeat());
    assert_send(ContractSettlement.ack());
    assert_send(ContractSettlement.nak(Duration::from_secs(1)));
    assert_send(ContractSettlement.terminate());

    let delivery = ContractDelivery(Vec::new(), ContractSettlement);

    let (wire, _settlement) = delivery.into_parts();

    assert!(wire.is_empty());
    assert_eq!(
        ContractMapper.encode(&serialized).unwrap(),
        serialized.payload
    );
    assert!(ContractMapper.decode(Vec::new()).is_err());
    assert!(ContractError.classify().is_retryable());
}

#[cfg(feature = "json")]
mod json_contract {
    use serde::{Deserialize, Serialize};
    use sisa_messaging::{
        CorrelationMetadata, DeliveryMetadata, Headers, JsonSerializer, JsonSerializerError,
        RoutingMetadata, SerializedEnvelope, Serializer, TraceMetadata,
    };

    use super::*;

    fn full_metadata() -> Metadata {
        let mut headers = Headers::new();
        headers.insert(
            HeaderName::new("x-import-batch").unwrap(),
            HeaderValue::new("2026-09-11").unwrap(),
        );
        Metadata {
            correlation: CorrelationMetadata {
                correlation_id: Some(MetadataValue::new("checkout-42").unwrap()),
                conversation_id: Some(
                    ConversationId::from_str("0198f3e2-40f0-7b15-8a4a-843d24f68d10").unwrap(),
                ),
                causation_id: Some(
                    MessageId::from_str("0198f3e2-40f0-7b15-8a4a-843d24f68d11").unwrap(),
                ),
                request_id: Some(
                    RequestId::from_str("0198f3e2-40f0-7b15-8a4a-843d24f68d12").unwrap(),
                ),
            },
            trace: TraceMetadata {
                traceparent: Some(
                    HeaderValue::new("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                        .unwrap(),
                ),
                tracestate: Some(HeaderValue::new("vendor=value").unwrap()),
            },
            routing: RoutingMetadata {
                source: Some(MetadataValue::new("orders-api").unwrap()),
                destination: Some(MetadataValue::new("fulfilment").unwrap()),
                reply_to: Some(MetadataValue::new("orders-api").unwrap()),
            },
            delivery: DeliveryMetadata {
                sent_at_ms: Some(1_789_056_000_000),
                deduplication_id: Some(MetadataValue::new("order-42-v1").unwrap()),
            },
            tenant_id: Some(MetadataValue::new("acme").unwrap()),
            headers,
        }
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct JsonMessage {
        order_id: String,
        secret: String,
    }

    impl Message for JsonMessage {
        const TYPE: &'static str = "orders.created";
        const VERSION: u32 = 1;

        fn order_by(&self) -> Option<OrderingKey> {
            OrderingKey::new(self.order_id.clone()).ok()
        }
    }

    #[test]
    fn metadata_json_matches_the_additive_contract_fixture() {
        let fixture = include_str!("fixtures/metadata.json");
        let expected = full_metadata();
        let decoded: Metadata = serde_json::from_str(fixture).unwrap();
        let encoded = serde_json::to_string_pretty(&decoded).unwrap() + "\n";

        assert_eq!(decoded, expected);
        assert_eq!(encoded, fixture);
    }

    #[test]
    fn metadata_json_accepts_missing_and_unknown_additive_fields() {
        let decoded: Metadata = serde_json::from_str(
            r#"{
                "correlation": {"correlation_id": "checkout-42", "future": true},
                "future_group": {"field": "ignored"}
            }"#,
        )
        .unwrap();

        assert_eq!(
            decoded
                .correlation
                .correlation_id
                .as_ref()
                .map(|id| id.as_str()),
            Some("checkout-42")
        );
        assert!(decoded.headers.is_empty());
    }

    #[test]
    fn json_serializer_round_trips_supported_envelopes() {
        let envelope = Envelope::new(
            MessageId::from_str("0198f3e2-40f0-7b15-8a4a-843d24f68d20").unwrap(),
            JsonMessage {
                order_id: "order-42".to_owned(),
                secret: "never render me".to_owned(),
            },
            full_metadata(),
        )
        .unwrap();
        let serialized = JsonSerializer.serialize(&envelope).unwrap();
        let decoded: Envelope<JsonMessage> = JsonSerializer.deserialize(serialized).unwrap();

        assert_eq!(decoded, envelope);
    }

    #[test]
    fn json_failures_do_not_render_payload_or_header_values() {
        let malformed = SerializedEnvelope {
            message_id: MessageId::new(),
            message_type: MessageType::new(JsonMessage::TYPE).unwrap(),
            message_version: JsonMessage::VERSION,
            content_type: ContentType::new("application/json").unwrap(),
            payload: br#"{"secret":"never render me""#.to_vec(),
            metadata: full_metadata(),
            ordering_key: Some(OrderingKey::new("order-42").unwrap()),
        };
        let error =
            <JsonSerializer as Serializer<JsonMessage>>::deserialize(&JsonSerializer, malformed)
                .unwrap_err();

        assert_eq!(error, JsonSerializerError::Decode);
        assert_eq!(error.classify(), FailureKind::Permanent);
        assert!(!error.to_string().contains("never render me"));
        assert!(!error.to_string().contains("2026-09-11"));
    }

    #[test]
    fn json_serializer_rejects_a_mismatched_content_type_before_decoding() {
        let serialized = SerializedEnvelope {
            message_id: MessageId::new(),
            message_type: MessageType::new(JsonMessage::TYPE).unwrap(),
            message_version: JsonMessage::VERSION,
            content_type: ContentType::new("application/octet-stream").unwrap(),
            payload: br#"{"order_id":"order-42","secret":"hidden"}"#.to_vec(),
            metadata: Metadata::default(),
            ordering_key: Some(OrderingKey::new("order-42").unwrap()),
        };

        let error =
            <JsonSerializer as Serializer<JsonMessage>>::deserialize(&JsonSerializer, serialized)
                .unwrap_err();
        assert_eq!(error, JsonSerializerError::ContentTypeMismatch);
    }

    #[test]
    fn json_serializer_rejects_a_mismatched_type_before_payload_decode() {
        let serialized = SerializedEnvelope {
            message_id: MessageId::new(),
            message_type: MessageType::new("orders.cancelled").unwrap(),
            message_version: JsonMessage::VERSION,
            content_type: ContentType::new("application/json").unwrap(),
            payload: br#"{"secret":"must not be decoded""#.to_vec(),
            metadata: Metadata::default(),
            ordering_key: None,
        };

        let error =
            <JsonSerializer as Serializer<JsonMessage>>::deserialize(&JsonSerializer, serialized)
                .unwrap_err();
        assert_eq!(error, JsonSerializerError::MessageTypeMismatch);
        assert_eq!(error.classify(), FailureKind::Permanent);
    }

    #[test]
    fn json_serializer_rejects_a_mismatched_version_before_payload_decode() {
        let serialized = SerializedEnvelope {
            message_id: MessageId::new(),
            message_type: MessageType::new(JsonMessage::TYPE).unwrap(),
            message_version: JsonMessage::VERSION + 1,
            content_type: ContentType::new("application/json").unwrap(),
            payload: br#"{"secret":"must not be decoded""#.to_vec(),
            metadata: Metadata::default(),
            ordering_key: None,
        };

        let error =
            <JsonSerializer as Serializer<JsonMessage>>::deserialize(&JsonSerializer, serialized)
                .unwrap_err();
        assert_eq!(error, JsonSerializerError::MessageVersionMismatch);
        assert_eq!(error.classify(), FailureKind::Permanent);
    }

    #[test]
    fn json_serializer_rejects_a_decoded_message_with_a_mismatched_ordering_key() {
        let serialized = SerializedEnvelope {
            message_id: MessageId::new(),
            message_type: MessageType::new(JsonMessage::TYPE).unwrap(),
            message_version: JsonMessage::VERSION,
            content_type: ContentType::new("application/json").unwrap(),
            payload: serde_json::to_vec(&JsonMessage {
                order_id: "order-42".to_owned(),
                secret: "must not be rendered".to_owned(),
            })
            .unwrap(),
            metadata: Metadata::default(),
            ordering_key: Some(OrderingKey::new("different-order").unwrap()),
        };

        let error =
            <JsonSerializer as Serializer<JsonMessage>>::deserialize(&JsonSerializer, serialized)
                .unwrap_err();
        assert_eq!(error, JsonSerializerError::OrderingKeyMismatch);
        assert_eq!(error.classify(), FailureKind::Permanent);
        assert!(!error.to_string().contains("must not be rendered"));
    }
}
