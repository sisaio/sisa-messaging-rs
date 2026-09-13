use std::collections::BTreeMap;
use std::error::Error;
use std::future::{Future, ready};
use std::num::NonZeroU64;
use std::str::FromStr;
use std::time::Duration;

use sisa_messaging::{
    ContentType, ConversationId, Delivery, DeliverySource, Envelope, EnvelopeMapper,
    ErrorClassifier, ErrorSummary, FailureKind, FrameworkHeader, HeaderName, HeaderNameError,
    HeaderValue, HeaderValueError, Headers, HeadersError, MAX_CUSTOM_HEADER_BYTES,
    MAX_CUSTOM_HEADER_COUNT, MAX_ERROR_SUMMARY_BYTES, Message, MessageId, MessageType, Metadata,
    MetadataValue, OrderingKey, Publisher, RequestId, SerializedEnvelope, Settlement,
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
        HeaderValueError::ControlCharacter.to_string(),
        "header value contains a forbidden control character"
    );
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
    assert_eq!(
        HeaderValue::new("safe\x01"),
        Err(HeaderValueError::ControlCharacter)
    );

    for byte in (0_u8..=31).chain(std::iter::once(127)) {
        let expected = if byte == b'\r' || byte == b'\n' {
            HeaderValueError::Newline
        } else {
            HeaderValueError::ControlCharacter
        };
        assert_eq!(
            HeaderValue::new(
                String::from_utf8(vec![
                    b's', b'a', b'f', b'e', byte, b'u', b'n', b's', b'a', b'f', b'e'
                ])
                .unwrap()
            ),
            Err(expected)
        );
    }

    assert!(HeaderValue::new("Zażółć gęślą").is_ok());
    assert_eq!(
        HeaderName::new("X-Import-Batch").unwrap().as_str(),
        "x-import-batch"
    );
}

fn header_name(index: usize) -> HeaderName {
    HeaderName::new(format!("x-{index}")).unwrap()
}

#[test]
fn custom_headers_enforce_exact_count_boundaries_atomically() {
    let mut headers = Headers::new();

    for index in 0..MAX_CUSTOM_HEADER_COUNT {
        assert_eq!(
            headers
                .insert(header_name(index), HeaderValue::new("v").unwrap())
                .unwrap(),
            None
        );
    }

    let snapshot = headers.clone();
    let error = headers
        .insert(
            header_name(MAX_CUSTOM_HEADER_COUNT),
            HeaderValue::new("sensitive-count-sentinel").unwrap(),
        )
        .unwrap_err();

    assert_eq!(headers.len(), MAX_CUSTOM_HEADER_COUNT);
    assert_eq!(headers, snapshot);
    assert_eq!(error, HeadersError::TooManyHeaders);
    assert_eq!(error.to_string(), "too many custom headers");
    assert!(!error.to_string().contains("sensitive-count-sentinel"));
}

#[test]
fn custom_headers_enforce_exact_aggregate_byte_boundaries_atomically() {
    const VALUE_BYTES: usize = 8_191;

    let mut headers = Headers::new();
    let full_value = HeaderValue::new("a".repeat(VALUE_BYTES)).unwrap();

    for index in 0..7 {
        headers
            .insert(
                HeaderName::new(format!("x{index}")).unwrap(),
                full_value.clone(),
            )
            .unwrap();
    }

    let retained_bytes = 7 * (2 + VALUE_BYTES);
    let boundary_name = HeaderName::new("x").unwrap();
    let boundary_value =
        HeaderValue::new("b".repeat(MAX_CUSTOM_HEADER_BYTES - retained_bytes - 1)).unwrap();
    headers.insert(boundary_name, boundary_value).unwrap();

    let snapshot = headers.clone();
    let error = headers
        .insert(HeaderName::new("y").unwrap(), HeaderValue::new("").unwrap())
        .unwrap_err();

    assert_eq!(headers, snapshot);
    assert_eq!(error, HeadersError::TooManyBytes);
    assert_eq!(error.to_string(), "custom headers exceed the byte limit");
}

#[test]
fn custom_header_byte_accounting_uses_utf8_bytes_and_replacement_delta() {
    let mut headers = Headers::new();
    let name = HeaderName::new("x-label").unwrap();
    let utf8_value = HeaderValue::new("é").unwrap();

    assert_eq!(
        headers.insert(name.clone(), utf8_value.clone()).unwrap(),
        None
    );
    assert_eq!(
        headers
            .insert(name.clone(), HeaderValue::new("ab").unwrap())
            .unwrap(),
        Some(utf8_value)
    );

    let smaller = HeaderValue::new("").unwrap();
    assert_eq!(
        headers.insert(name.clone(), smaller.clone()).unwrap(),
        Some(HeaderValue::new("ab").unwrap())
    );
    assert_eq!(headers.get(&name), Some(&smaller));
}

#[test]
fn replacement_at_count_and_byte_limits_succeeds_or_rolls_back() {
    const MAX_VALUE_BYTES: usize = 8_192;

    let mut count_limited = Headers::new();

    for index in 0..MAX_CUSTOM_HEADER_COUNT {
        count_limited
            .insert(header_name(index), HeaderValue::new("").unwrap())
            .unwrap();
    }

    let first = header_name(0);
    assert_eq!(
        count_limited
            .insert(first, HeaderValue::new("replacement").unwrap())
            .unwrap(),
        Some(HeaderValue::new("").unwrap())
    );

    let mut byte_limited = Headers::new();
    let full_value = HeaderValue::new("a".repeat(MAX_VALUE_BYTES)).unwrap();

    for index in 0..7 {
        byte_limited
            .insert(header_name(index), full_value.clone())
            .unwrap();
    }

    let retained_bytes = 7 * (3 + MAX_VALUE_BYTES);
    let boundary_name = HeaderName::new("x").unwrap();
    let boundary_value =
        HeaderValue::new("b".repeat(MAX_CUSTOM_HEADER_BYTES - retained_bytes - 1)).unwrap();
    byte_limited
        .insert(boundary_name.clone(), boundary_value.clone())
        .unwrap();

    assert_eq!(
        byte_limited
            .insert(boundary_name.clone(), boundary_value.clone())
            .unwrap(),
        Some(boundary_value.clone())
    );

    let snapshot = byte_limited.clone();
    let larger_value = HeaderValue::new(format!("{}c", boundary_value.as_str())).unwrap();
    let error = byte_limited
        .insert(boundary_name.clone(), larger_value)
        .unwrap_err();

    assert_eq!(error, HeadersError::TooManyBytes);
    assert_eq!(byte_limited, snapshot);
    assert_eq!(byte_limited.get(&boundary_name), Some(&boundary_value));

    let smaller = HeaderValue::new("small").unwrap();
    assert_eq!(
        byte_limited
            .insert(boundary_name.clone(), smaller.clone())
            .unwrap(),
        Some(boundary_value)
    );
    assert_eq!(byte_limited.get(&boundary_name), Some(&smaller));
}

#[test]
fn custom_header_operations_match_a_deterministic_bounded_model() {
    let mut seed = 0x5eed_cafe_f00d_beef_u64;
    let mut headers = Headers::new();
    let mut model = BTreeMap::<String, String>::new();

    for _ in 0..256 {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);

        let index = (seed.rotate_left(17) % 72) as usize;
        let input_name = if seed & 1 == 0 {
            format!("X-{index}")
        } else {
            format!("x-{index}")
        };
        let value = "v".repeat((seed.rotate_right(11) % 8_193) as usize);
        let name = HeaderName::new(input_name).unwrap();
        let header_value = HeaderValue::new(value.clone()).unwrap();
        let canonical_name = name.as_str().to_owned();
        let previous = model.get(&canonical_name);
        let current_bytes = model
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum::<usize>();
        let previous_bytes = previous.map_or(0, |value| canonical_name.len() + value.len());
        let candidate_bytes = current_bytes - previous_bytes + canonical_name.len() + value.len();
        let expected_error = if previous.is_none() && model.len() == MAX_CUSTOM_HEADER_COUNT {
            Some(HeadersError::TooManyHeaders)
        } else if candidate_bytes > MAX_CUSTOM_HEADER_BYTES {
            Some(HeadersError::TooManyBytes)
        } else {
            None
        };

        let result = headers.insert(name, header_value);

        match expected_error {
            Some(error) => assert_eq!(result, Err(error)),
            None => {
                let expected_previous = model
                    .insert(canonical_name.clone(), value)
                    .map(|value| HeaderValue::new(value).unwrap());
                assert_eq!(result.unwrap(), expected_previous);
            }
        }

        assert_eq!(headers.len(), model.len());
        for (name, value) in &model {
            assert_eq!(
                headers
                    .get(&HeaderName::new(name.clone()).unwrap())
                    .map(HeaderValue::as_str),
                Some(value.as_str())
            );
        }
    }
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

struct ContractDelivery {
    wire: Vec<u8>,

    settlement: ContractSettlement,
}

impl Delivery for ContractDelivery {
    type Wire = Vec<u8>;
    type Settlement = ContractSettlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.wire, self.settlement)
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

    let delivery = ContractDelivery {
        wire: Vec::new(),
        settlement: ContractSettlement,
    };

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

        headers
            .insert(
                HeaderName::new("x-import-batch").unwrap(),
                HeaderValue::new("2026-09-11").unwrap(),
            )
            .unwrap();

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
    fn metadata_json_rejects_reserved_header_names_and_control_characters() {
        assert!(
            serde_json::from_str::<Metadata>(
                r#"{
                    "headers": {
                        "message-id": "forbidden"
                    }
                }"#
            )
            .is_err()
        );

        assert!(
            serde_json::from_str::<Metadata>(
                r#"{
                    "headers": {
                        "x-safe": "safe\r\nunsafe"
                    }
                }"#
            )
            .is_err()
        );

        assert!(
            serde_json::from_str::<Metadata>(
                r#"{
                    "headers": {
                        "x-safe": "safe\u0001value"
                    }
                }"#
            )
            .is_err()
        );

        assert!(
            serde_json::from_str::<Metadata>(
                r#"{
                    "headers": {
                        "x-safe": "safe\u007fvalue"
                    }
                }"#
            )
            .is_err()
        );
    }

    #[test]
    fn custom_header_json_round_trips_exact_count_and_byte_boundaries() {
        const MAX_VALUE_BYTES: usize = 8_192;

        let mut count_limited = Headers::new();

        for index in 0..MAX_CUSTOM_HEADER_COUNT {
            count_limited
                .insert(header_name(index), HeaderValue::new("v").unwrap())
                .unwrap();
        }

        let count_json = serde_json::to_string(&count_limited).unwrap();
        let count_decoded: Headers = serde_json::from_str(&count_json).unwrap();

        assert_eq!(count_decoded, count_limited);

        let mut byte_limited = Headers::new();
        let full_value = HeaderValue::new("a".repeat(MAX_VALUE_BYTES)).unwrap();

        for index in 0..7 {
            byte_limited
                .insert(header_name(index), full_value.clone())
                .unwrap();
        }

        let retained_bytes = 7 * (3 + MAX_VALUE_BYTES);
        byte_limited
            .insert(
                HeaderName::new("x").unwrap(),
                HeaderValue::new(
                    "é".repeat((MAX_CUSTOM_HEADER_BYTES - retained_bytes - 1) / "é".len()),
                )
                .unwrap(),
            )
            .unwrap();

        let byte_json = serde_json::to_string(&byte_limited).unwrap();
        let byte_decoded: Headers = serde_json::from_str(&byte_json).unwrap();

        assert_eq!(byte_decoded, byte_limited.clone());
        assert_eq!(byte_limited, byte_limited.clone());
        assert_eq!(serde_json::to_string(&Headers::default()).unwrap(), "{}");
    }

    #[test]
    fn custom_header_json_rejects_a_sixty_fifth_key_before_its_value() {
        let mut entries = (0..MAX_CUSTOM_HEADER_COUNT)
            .map(|index| format!(r#""x-{index}":"v""#))
            .collect::<Vec<_>>();
        entries.push(r#""x-64":{"secret":"sensitive-unconsumed-value-sentinel"}"#.to_owned());
        let input = format!("{{{}}}", entries.join(","));

        let error = serde_json::from_str::<Headers>(&input).unwrap_err();
        let rendered = error.to_string();

        assert!(rendered.contains("too many custom headers"));
        assert!(!rendered.contains("sensitive-unconsumed-value-sentinel"));
    }

    #[test]
    fn custom_header_json_preserves_canonical_duplicate_replacement() {
        let decoded: Headers =
            serde_json::from_str(r#"{"X-Label":"first","x-label":"second"}"#).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded
                .get(&HeaderName::new("x-label").unwrap())
                .map(HeaderValue::as_str),
            Some("second")
        );
        assert_eq!(
            serde_json::to_string(&decoded).unwrap(),
            r#"{"x-label":"second"}"#
        );
    }

    #[test]
    fn custom_header_json_errors_do_not_echo_oversized_or_malformed_inputs() {
        let oversized_name = format!("sensitive-name-sentinel-{}", "x".repeat(255));
        let oversized_value = format!("sensitive-value-sentinel-{}", "x".repeat(8_192));
        let cases = [
            format!(
                "{{{}:\"safe\"}}",
                serde_json::to_string(&oversized_name).unwrap()
            ),
            format!(
                "{{\"x-safe\":{}}}",
                serde_json::to_string(&oversized_value).unwrap()
            ),
            r#"{"x-safe":{"secret":"malformed-sensitive-sentinel"}}"#.to_owned(),
        ];

        for input in cases {
            let rendered = serde_json::from_str::<Headers>(&input)
                .unwrap_err()
                .to_string();

            assert!(!rendered.contains("sensitive-name-sentinel"));
            assert!(!rendered.contains("sensitive-value-sentinel"));
            assert!(!rendered.contains("malformed-sensitive-sentinel"));
        }
    }

    #[test]
    fn custom_header_json_rejects_the_aggregate_byte_limit_plus_one() {
        const MAX_VALUE_BYTES: usize = 8_192;

        let mut entries = (0..7)
            .map(|index| {
                format!(
                    r#""x-{index}":{}"#,
                    serde_json::to_string(&"a".repeat(MAX_VALUE_BYTES)).unwrap()
                )
            })
            .collect::<Vec<_>>();
        let retained_bytes = 7 * (3 + MAX_VALUE_BYTES);
        let sentinel = "sensitive-byte-sentinel-";
        let oversized = format!(
            "{sentinel}{}",
            "b".repeat(MAX_CUSTOM_HEADER_BYTES - retained_bytes - sentinel.len())
        );
        entries.push(format!(
            r#""x":{}"#,
            serde_json::to_string(&oversized).unwrap()
        ));
        let input = format!("{{{}}}", entries.join(","));

        let error = serde_json::from_str::<Headers>(&input).unwrap_err();
        let rendered = error.to_string();

        assert!(rendered.contains("custom headers exceed the byte limit"));
        assert!(!rendered.contains("sensitive-byte-sentinel"));
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
