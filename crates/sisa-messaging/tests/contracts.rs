use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::future::{Future, poll_fn, ready};
use std::num::NonZeroU64;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use sisa_messaging::{
    ContentType, ConversationId, Delivery, Envelope, EnvelopeMapper, ErrorClassifier, ErrorSummary,
    FailureKind, FrameworkHeader, HeaderName, HeaderNameError, HeaderValue, HeaderValueError,
    Headers, HeadersError, IndividualCapability, IndividualDeliverySource, IndividualSettlement,
    IndividualSettlementError, IndividualSourceDescriptor, IndividualSourceDescriptorError,
    IndividualSourceOpenError, IndividualSourceRequirement, IndividualSourceRequirements,
    MAX_CUSTOM_HEADER_BYTES, MAX_CUSTOM_HEADER_COUNT, MAX_ERROR_SUMMARY_BYTES, Message, MessageId,
    MessageType, Metadata, MetadataValue, OrderingKey, PartitionAdvance,
    PartitionedLogDeliverySource, PartitionedLogReceive, PartitionedLogSettlement, Publisher,
    RequestId, SerializedEnvelope, UnsupportedIndividualRequirement, ValidationError,
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

struct SecretProviderError;

impl std::fmt::Debug for SecretProviderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretProviderError(SECRET_SENTINEL)")
    }
}

impl std::fmt::Display for SecretProviderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SECRET_SENTINEL payload/header/url")
    }
}

impl Error for SecretProviderError {}

impl ErrorClassifier for SecretProviderError {
    fn classify(&self) -> FailureKind {
        FailureKind::Transient
    }
}

#[test]
fn individual_provider_errors_redact_formatting_and_safe_summaries_but_retain_typed_access() {
    let open = IndividualSourceOpenError::Source(SecretProviderError);
    let open_debug = format!("{open:?}");
    let open_display = open.to_string();
    assert!(!open_debug.contains("SECRET_SENTINEL"));
    assert!(!open_display.contains("SECRET_SENTINEL"));
    assert!(Error::source(&open).is_none());
    let open_summary = ErrorSummary::from_safe_error(&open);
    assert!(!open_summary.as_str().contains("SECRET_SENTINEL"));
    assert!(!open_summary.to_string().contains("SECRET_SENTINEL"));
    assert!(matches!(
        &open,
        IndividualSourceOpenError::Source(error)
            if error.to_string().contains("SECRET_SENTINEL")
    ));
    assert_eq!(open.classify(), FailureKind::Transient);

    let settlement = IndividualSettlementError::Operation(SecretProviderError);
    let settlement_debug = format!("{settlement:?}");
    let settlement_display = settlement.to_string();
    assert!(!settlement_debug.contains("SECRET_SENTINEL"));
    assert!(!settlement_display.contains("SECRET_SENTINEL"));
    assert!(Error::source(&settlement).is_none());
    let settlement_summary = ErrorSummary::from_safe_error(&settlement);
    assert!(!settlement_summary.as_str().contains("SECRET_SENTINEL"));
    assert!(!settlement_summary.to_string().contains("SECRET_SENTINEL"));
    assert!(matches!(
        &settlement,
        IndividualSettlementError::Operation(error)
            if error.to_string().contains("SECRET_SENTINEL")
    ));
    assert_eq!(settlement.classify(), FailureKind::Transient);
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

#[derive(Clone, Copy)]
struct IndividualSupport {
    delayed_retry: bool,
    terminal_discard: bool,
    heartbeat: bool,
}

impl IndividualSupport {
    fn descriptor(
        self,
        ack_wait: Option<Duration>,
        max_deliver: Option<NonZeroU64>,
    ) -> IndividualSourceDescriptor {
        IndividualSourceDescriptor::new(
            ack_wait,
            max_deliver,
            self.delayed_retry,
            self.terminal_discard,
            self.heartbeat,
        )
        .unwrap()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IndividualEvent {
    Heartbeat,
    Ack,
    Nak,
    Terminate,
}

struct IndividualFixtureSettlement {
    support: IndividualSupport,
    events: Arc<Mutex<Vec<IndividualEvent>>>,
    progress: Arc<Mutex<IndividualProgress>>,
}

impl IndividualFixtureSettlement {
    fn record(&self, event: IndividualEvent) {
        self.events.lock().unwrap().push(event);
    }

    fn settled(&self, event: IndividualEvent) {
        self.record(event);
        self.progress.lock().unwrap().settled = true;
    }
}

#[derive(Default)]
struct IndividualProgress {
    outstanding: bool,
    settled: bool,
}

impl IndividualSettlement for IndividualFixtureSettlement {
    type Error = ContractError;

    fn heartbeat(
        &mut self,
    ) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send {
        let support = self.support;
        let events = Arc::clone(&self.events);
        poll_fn(move |_| {
            if !support.heartbeat {
                return Poll::Ready(Err(IndividualSettlementError::Unsupported(
                    IndividualCapability::Heartbeat,
                )));
            }
            events.lock().unwrap().push(IndividualEvent::Heartbeat);
            Poll::Ready(Ok(()))
        })
    }

    fn ack(
        self,
    ) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send {
        poll_fn(move |_| {
            self.settled(IndividualEvent::Ack);
            Poll::Ready(Ok(()))
        })
    }

    fn nak(
        self,
        _delay: Duration,
    ) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send {
        poll_fn(move |_| {
            if !self.support.delayed_retry {
                return Poll::Ready(Err(IndividualSettlementError::Unsupported(
                    IndividualCapability::DelayedRetry,
                )));
            }
            self.settled(IndividualEvent::Nak);
            Poll::Ready(Ok(()))
        })
    }

    fn terminate(
        self,
    ) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send {
        poll_fn(move |_| {
            if !self.support.terminal_discard {
                return Poll::Ready(Err(IndividualSettlementError::Unsupported(
                    IndividualCapability::TerminalDiscard,
                )));
            }
            self.settled(IndividualEvent::Terminate);
            Poll::Ready(Ok(()))
        })
    }
}

struct IndividualFixtureDelivery {
    wire: Vec<u8>,
    settlement: IndividualFixtureSettlement,
}

impl Delivery for IndividualFixtureDelivery {
    type Wire = Vec<u8>;
    type Settlement = IndividualFixtureSettlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.wire, self.settlement)
    }
}

struct IndividualSourceFixture {
    descriptor: IndividualSourceDescriptor,
    next: Option<IndividualFixtureDelivery>,
    progress: Arc<Mutex<IndividualProgress>>,
}

impl IndividualSourceFixture {
    fn new(
        descriptor: IndividualSourceDescriptor,
        support: IndividualSupport,
        events: Arc<Mutex<Vec<IndividualEvent>>>,
    ) -> Self {
        let progress = Arc::new(Mutex::new(IndividualProgress::default()));
        Self {
            descriptor,
            next: Some(IndividualFixtureDelivery {
                wire: vec![1, 2, 3],
                settlement: IndividualFixtureSettlement {
                    support,
                    events,
                    progress: Arc::clone(&progress),
                },
            }),
            progress,
        }
    }

    fn poll_receive(&mut self) -> Poll<Result<Option<IndividualFixtureDelivery>, ContractError>> {
        if let Some(delivery) = self.next.take() {
            self.progress.lock().unwrap().outstanding = true;
            return Poll::Ready(Ok(Some(delivery)));
        }

        let progress = self.progress.lock().unwrap();
        if progress.outstanding && !progress.settled {
            Poll::Pending
        } else {
            Poll::Ready(Ok(None))
        }
    }

    fn open(
        &mut self,
        requirements: IndividualSourceRequirements,
    ) -> impl Future<
        Output = Result<IndividualSourceDescriptor, IndividualSourceOpenError<ContractError>>,
    > + Send {
        let descriptor = self.descriptor;
        ready(
            descriptor
                .validate(requirements)
                .map(|()| descriptor)
                .map_err(IndividualSourceOpenError::Unsupported),
        )
    }

    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<Option<IndividualFixtureDelivery>, ContractError>> + Send {
        poll_fn(move |_| self.poll_receive())
    }
}

struct NatsFixture(IndividualSourceFixture);
struct RabbitMqFixture(IndividualSourceFixture);
struct RedisStreamsFixture(IndividualSourceFixture);

impl NatsFixture {
    fn new() -> Self {
        let support = IndividualSupport {
            delayed_retry: true,
            terminal_discard: true,
            heartbeat: true,
        };
        Self(IndividualSourceFixture::new(
            support.descriptor(Some(Duration::from_secs(30)), NonZeroU64::new(5)),
            support,
            Arc::default(),
        ))
    }
}

impl RabbitMqFixture {
    fn new() -> Self {
        let support = IndividualSupport {
            delayed_retry: false,
            terminal_discard: true,
            heartbeat: false,
        };
        Self(IndividualSourceFixture::new(
            support.descriptor(None, None),
            support,
            Arc::default(),
        ))
    }
}

impl RedisStreamsFixture {
    fn new() -> Self {
        let support = IndividualSupport {
            delayed_retry: false,
            terminal_discard: false,
            heartbeat: false,
        };
        Self(IndividualSourceFixture::new(
            support.descriptor(None, None),
            support,
            Arc::default(),
        ))
    }
}

macro_rules! impl_individual_source {
    ($fixture:ty) => {
        impl IndividualDeliverySource for $fixture {
            type Delivery = IndividualFixtureDelivery;
            type Error = ContractError;

            fn open(
                &mut self,
                requirements: IndividualSourceRequirements,
            ) -> impl Future<
                Output = Result<IndividualSourceDescriptor, IndividualSourceOpenError<Self::Error>>,
            > + Send {
                self.0.open(requirements)
            }

            fn receive(
                &mut self,
            ) -> impl Future<Output = Result<Option<Self::Delivery>, Self::Error>> + Send {
                self.0.receive()
            }
        }
    };
}

impl_individual_source!(NatsFixture);
impl_individual_source!(RabbitMqFixture);
impl_individual_source!(RedisStreamsFixture);

struct ReadinessFixture {
    ready: Arc<AtomicBool>,
    source: IndividualSourceFixture,
}

impl IndividualDeliverySource for ReadinessFixture {
    type Delivery = IndividualFixtureDelivery;
    type Error = ContractError;

    fn open(
        &mut self,
        requirements: IndividualSourceRequirements,
    ) -> impl Future<
        Output = Result<IndividualSourceDescriptor, IndividualSourceOpenError<Self::Error>>,
    > + Send {
        self.source.open(requirements)
    }

    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<Option<Self::Delivery>, Self::Error>> + Send {
        let ready = Arc::clone(&self.ready);
        poll_fn(move |_context| {
            if ready.load(AtomicOrdering::SeqCst) {
                self.source.poll_receive()
            } else {
                Poll::Pending
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PartitionId(u8);

#[derive(Debug)]
struct PartitionState {
    partition: PartitionId,
    next_offset: u64,
    committed_offset: u64,
    unresolved: Option<u64>,
    generation: u64,
    owned: bool,
    paused: bool,
    reconcile_required: bool,
    loss_pending: bool,
    max_offset: u64,
}

impl PartitionState {
    fn new(partition: PartitionId, max_offset: u64) -> Self {
        Self {
            partition,
            next_offset: 1,
            committed_offset: 0,
            unresolved: None,
            generation: 7,
            owned: true,
            paused: false,
            reconcile_required: false,
            loss_pending: false,
            max_offset,
        }
    }
}

#[derive(Clone, Copy)]
enum AdvanceMode {
    Success,
    OwnershipLost,
    ErrorBeforeEffect,
    ErrorAfterEffect,
    PendingAfterEffect,
}

struct PartitionFixtureSettlement {
    partition: PartitionId,
    offset: u64,
    generation: u64,
    state: Arc<Mutex<PartitionState>>,
    resolved: Arc<AtomicBool>,
    mode: AdvanceMode,
}

impl PartitionFixtureSettlement {
    fn mark_durably_resolved(&self) {
        self.resolved.store(true, AtomicOrdering::SeqCst);
    }
}

impl PartitionedLogSettlement for PartitionFixtureSettlement {
    type Partition = PartitionId;
    type Error = ContractError;

    fn advance(self) -> impl Future<Output = Result<PartitionAdvance, Self::Error>> + Send {
        let mut effect_applied = false;
        poll_fn(move |_context| {
            if matches!(self.mode, AdvanceMode::PendingAfterEffect) && effect_applied {
                return Poll::Pending;
            }
            let mut state = self.state.lock().unwrap();
            if state.partition != self.partition {
                return Poll::Ready(Err(ContractError));
            }
            if !state.owned || state.generation != self.generation {
                state.paused = true;
                state.loss_pending = true;
                return Poll::Ready(Ok(PartitionAdvance::OwnershipLost));
            }
            if matches!(self.mode, AdvanceMode::OwnershipLost) {
                state.owned = false;
                state.paused = true;
                state.loss_pending = true;
                return Poll::Ready(Ok(PartitionAdvance::OwnershipLost));
            }
            if !self.resolved.load(AtomicOrdering::SeqCst)
                || state.unresolved != Some(self.offset)
                || self.offset != state.committed_offset + 1
            {
                return Poll::Ready(Err(ContractError));
            }
            if matches!(self.mode, AdvanceMode::ErrorBeforeEffect) {
                state.paused = true;
                state.reconcile_required = true;
                return Poll::Ready(Err(ContractError));
            }

            state.committed_offset = self.offset;
            match self.mode {
                AdvanceMode::Success => {
                    state.unresolved = None;
                    Poll::Ready(Ok(PartitionAdvance::Advanced))
                }
                AdvanceMode::ErrorAfterEffect => {
                    state.paused = true;
                    state.reconcile_required = true;
                    Poll::Ready(Err(ContractError))
                }
                AdvanceMode::PendingAfterEffect => {
                    state.paused = true;
                    state.reconcile_required = true;
                    effect_applied = true;
                    Poll::Pending
                }
                AdvanceMode::OwnershipLost | AdvanceMode::ErrorBeforeEffect => {
                    Poll::Ready(Err(ContractError))
                }
            }
        })
    }

    fn partition(&self) -> &Self::Partition {
        &self.partition
    }
}

struct PartitionFixtureDelivery {
    partition: PartitionId,
    offset: u64,
    settlement: PartitionFixtureSettlement,
}

impl Delivery for PartitionFixtureDelivery {
    type Wire = u64;
    type Settlement = PartitionFixtureSettlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.offset, self.settlement)
    }
}

struct PartitionedFixtureSource {
    partitions: BTreeMap<PartitionId, Arc<Mutex<PartitionState>>>,
    ownership_events: VecDeque<PartitionId>,
    next_advance_mode: AdvanceMode,
}

impl PartitionedFixtureSource {
    fn new(partitions: &[(PartitionId, u64)], ownership_events: &[PartitionId]) -> Self {
        Self {
            partitions: partitions
                .iter()
                .map(|(partition, max_offset)| {
                    (
                        *partition,
                        Arc::new(Mutex::new(PartitionState::new(*partition, *max_offset))),
                    )
                })
                .collect(),
            ownership_events: ownership_events.iter().copied().collect(),
            next_advance_mode: AdvanceMode::Success,
        }
    }

    fn open(&mut self) -> impl Future<Output = Result<(), ContractError>> + Send {
        ready(Ok(()))
    }

    fn restart_partition(&mut self, partition: PartitionId) {
        if let Some(state) = self.partitions.get(&partition) {
            let mut state = state.lock().unwrap();
            state.generation += 1;
            state.owned = true;
            state.paused = state.unresolved.is_some();
            state.reconcile_required = state.unresolved.is_some();
            state.loss_pending = false;
        }
    }

    fn poll_receive(
        &mut self,
    ) -> Poll<Result<PartitionedLogReceive<PartitionFixtureDelivery, PartitionId>, ContractError>>
    {
        if let Some(partition) = self.ownership_events.pop_front() {
            if let Some(shared_state) = self.partitions.get(&partition) {
                let mut state = shared_state.lock().unwrap();
                state.owned = false;
                state.paused = true;
                state.loss_pending = false;
            }
            return Poll::Ready(Ok(PartitionedLogReceive::OwnershipLost(partition)));
        }

        for (partition, shared_state) in &self.partitions {
            let mut state = shared_state.lock().unwrap();
            if state.loss_pending {
                state.loss_pending = false;
                state.owned = false;
                state.paused = true;
                return Poll::Ready(Ok(PartitionedLogReceive::OwnershipLost(*partition)));
            }
            if state.reconcile_required && state.owned {
                if let Some(offset) = state.unresolved {
                    if state.committed_offset >= offset {
                        state.unresolved = None;
                    } else {
                        state.next_offset = offset;
                        state.unresolved = None;
                    }
                }
                state.reconcile_required = false;
                state.paused = false;
            }
        }

        for (partition, shared_state) in &self.partitions {
            let mut state = shared_state.lock().unwrap();
            if !state.owned
                || state.paused
                || state.unresolved.is_some()
                || state.next_offset > state.max_offset
            {
                continue;
            }

            let offset = state.next_offset;
            state.next_offset += 1;
            state.unresolved = Some(offset);
            let generation = state.generation;
            let state = Arc::clone(shared_state);
            let mode = self.next_advance_mode;
            self.next_advance_mode = AdvanceMode::Success;
            return Poll::Ready(Ok(PartitionedLogReceive::Delivery(
                PartitionFixtureDelivery {
                    partition: *partition,
                    offset,
                    settlement: PartitionFixtureSettlement {
                        partition: *partition,
                        offset,
                        generation,
                        state,
                        resolved: Arc::new(AtomicBool::new(false)),
                        mode,
                    },
                },
            )));
        }

        if self.partitions.values().any(|shared_state| {
            let state = shared_state.lock().unwrap();
            state.unresolved.is_some() || (state.owned && state.paused)
        }) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(PartitionedLogReceive::Closed))
        }
    }

    fn receive(
        &mut self,
    ) -> impl Future<
        Output = Result<
            PartitionedLogReceive<PartitionFixtureDelivery, PartitionId>,
            ContractError,
        >,
    > + Send {
        poll_fn(move |_| self.poll_receive())
    }
}

struct KafkaFixture(PartitionedFixtureSource);
struct IggyFixture(PartitionedFixtureSource);

impl KafkaFixture {
    fn new() -> Self {
        Self(PartitionedFixtureSource::new(
            &[(PartitionId(0), 2), (PartitionId(1), 1)],
            &[],
        ))
    }
}

impl IggyFixture {
    fn new() -> Self {
        Self(PartitionedFixtureSource::new(&[(PartitionId(0), 2)], &[]))
    }
}

macro_rules! impl_partitioned_source {
    ($fixture:ty) => {
        impl PartitionedLogDeliverySource for $fixture {
            type Partition = PartitionId;
            type Delivery = PartitionFixtureDelivery;
            type Error = ContractError;

            fn open(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
                self.0.open()
            }

            fn receive(
                &mut self,
            ) -> impl Future<
                Output = Result<
                    PartitionedLogReceive<Self::Delivery, Self::Partition>,
                    Self::Error,
                >,
            > + Send {
                self.0.receive()
            }
        }
    };
}

impl_partitioned_source!(KafkaFixture);
impl_partitioned_source!(IggyFixture);

struct PartitionReadinessFixture {
    ready: Arc<AtomicBool>,
    source: PartitionedFixtureSource,
}

impl PartitionedLogDeliverySource for PartitionReadinessFixture {
    type Partition = PartitionId;
    type Delivery = PartitionFixtureDelivery;
    type Error = ContractError;

    fn open(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.source.open()
    }

    fn receive(
        &mut self,
    ) -> impl Future<
        Output = Result<PartitionedLogReceive<Self::Delivery, Self::Partition>, Self::Error>,
    > + Send {
        let ready = Arc::clone(&self.ready);
        poll_fn(move |_| {
            if ready.load(AtomicOrdering::SeqCst) {
                self.source.poll_receive()
            } else {
                Poll::Pending
            }
        })
    }
}

fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.as_mut().poll(&mut context)
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match poll_once(future.as_mut()) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("in-memory contract future unexpectedly remained pending"),
    }
}

fn assert_send<T: Send>(_: T) {}

fn assert_send_sync<T: Send + Sync>() {}

fn assert_individual_source<T: IndividualDeliverySource + Sync>() {}

fn assert_individual_settlement<T: IndividualSettlement + Sync>() {}

fn assert_partitioned_source<T: PartitionedLogDeliverySource + Sync>() {}

fn assert_partitioned_settlement<T: PartitionedLogSettlement + Sync>() {}

#[test]
fn inbound_public_types_and_native_futures_are_send_sync_and_statically_dispatched() {
    assert_send_sync::<IndividualCapability>();
    assert_send_sync::<IndividualSourceRequirement>();
    assert_send_sync::<IndividualSourceRequirements>();
    assert_send_sync::<IndividualSourceDescriptor>();
    assert_send_sync::<IndividualSourceDescriptorError>();
    assert_send_sync::<UnsupportedIndividualRequirement>();
    assert_send_sync::<IndividualSourceOpenError<ContractError>>();
    assert_send_sync::<IndividualSettlementError<ContractError>>();
    assert_send_sync::<PartitionAdvance>();
    assert_send_sync::<PartitionedLogReceive<PartitionFixtureDelivery, PartitionId>>();

    assert_individual_source::<NatsFixture>();
    assert_individual_source::<RabbitMqFixture>();
    assert_individual_source::<RedisStreamsFixture>();
    assert_individual_source::<ReadinessFixture>();
    assert_individual_settlement::<IndividualFixtureSettlement>();
    assert_partitioned_source::<KafkaFixture>();
    assert_partitioned_source::<IggyFixture>();
    assert_partitioned_source::<PartitionReadinessFixture>();
    assert_partitioned_settlement::<PartitionFixtureSettlement>();

    let support = IndividualSupport {
        delayed_retry: true,
        terminal_discard: true,
        heartbeat: true,
    };
    let mut settlement = IndividualFixtureSettlement {
        support,
        events: Arc::default(),
        progress: Arc::default(),
    };
    let mut individual = NatsFixture::new();
    let mut kafka = KafkaFixture::new();
    let mut iggy = IggyFixture::new();
    let mut rabbit = RabbitMqFixture::new();
    let mut redis = RedisStreamsFixture::new();

    // Return-position `impl Future` keeps these generic calls on concrete source and settlement types.
    assert_send(individual.open(IndividualSourceRequirements::new()));
    assert_send(individual.receive());
    assert_send(rabbit.open(IndividualSourceRequirements::new()));
    assert_send(rabbit.receive());
    assert_send(redis.open(IndividualSourceRequirements::new()));
    assert_send(redis.receive());
    assert_send(settlement.heartbeat());
    assert_send(
        IndividualFixtureSettlement {
            support,
            events: Arc::default(),
            progress: Arc::default(),
        }
        .ack(),
    );
    assert_send(
        IndividualFixtureSettlement {
            support,
            events: Arc::default(),
            progress: Arc::default(),
        }
        .nak(Duration::from_secs(1)),
    );
    assert_send(
        IndividualFixtureSettlement {
            support,
            events: Arc::default(),
            progress: Arc::default(),
        }
        .terminate(),
    );
    assert_send(kafka.open());
    assert_send(kafka.receive());
    assert_send(iggy.open());
    assert_send(iggy.receive());
    let mut partition_readiness = PartitionReadinessFixture {
        ready: Arc::default(),
        source: PartitionedFixtureSource::new(&[(PartitionId(0), 1)], &[]),
    };
    assert_send(partition_readiness.open());
    assert_send(partition_readiness.receive());
    assert_send(
        PartitionFixtureSettlement {
            partition: PartitionId(0),
            offset: 1,
            generation: 7,
            state: Arc::new(Mutex::new(PartitionState::new(PartitionId(0), 1))),
            resolved: Arc::new(AtomicBool::new(true)),
            mode: AdvanceMode::Success,
        }
        .advance(),
    );
}

#[test]
fn individual_fixtures_report_truthful_snapshots_and_reject_missing_requirements() {
    let all_capabilities = IndividualSourceRequirements::new()
        .requiring_ack_wait()
        .requiring_max_deliver()
        .requiring_delayed_retry()
        .requiring_terminal_discard()
        .requiring_heartbeat();
    let mut nats = NatsFixture::new();
    let nats_descriptor = block_on(nats.open(all_capabilities)).unwrap();

    assert_eq!(nats_descriptor.ack_wait(), Some(Duration::from_secs(30)));
    assert_eq!(nats_descriptor.max_deliver(), NonZeroU64::new(5));
    assert!(nats_descriptor.supports_delayed_retry());
    assert!(nats_descriptor.supports_terminal_discard());
    assert!(nats_descriptor.supports_heartbeat());

    let mut rabbit = RabbitMqFixture::new();
    let rabbit_descriptor = block_on(rabbit.open(IndividualSourceRequirements::new())).unwrap();
    assert_eq!(rabbit_descriptor.ack_wait(), None);
    assert_eq!(rabbit_descriptor.max_deliver(), None);
    assert!(!rabbit_descriptor.supports_delayed_retry());
    assert!(rabbit_descriptor.supports_terminal_discard());
    assert!(!rabbit_descriptor.supports_heartbeat());
    let error =
        block_on(rabbit.open(IndividualSourceRequirements::new().requiring_delayed_retry()))
            .unwrap_err();
    assert_eq!(error.classify(), FailureKind::Permanent);
    assert!(matches!(
        error,
        IndividualSourceOpenError::Unsupported(error)
            if error.requirement() == IndividualSourceRequirement::DelayedRetry
    ));

    let mut redis = RedisStreamsFixture::new();
    let redis_descriptor = block_on(redis.open(IndividualSourceRequirements::new())).unwrap();
    assert_eq!(redis_descriptor.ack_wait(), None);
    assert_eq!(redis_descriptor.max_deliver(), None);
    assert!(!redis_descriptor.supports_delayed_retry());
    assert!(!redis_descriptor.supports_terminal_discard());
    assert!(!redis_descriptor.supports_heartbeat());
    let error =
        block_on(redis.open(IndividualSourceRequirements::new().requiring_terminal_discard()))
            .unwrap_err();
    assert!(matches!(
        error,
        IndividualSourceOpenError::Unsupported(error)
            if error.requirement() == IndividualSourceRequirement::TerminalDiscard
    ));

    assert_eq!(
        IndividualSourceDescriptor::new(Some(Duration::ZERO), NonZeroU64::new(1), true, true, true,),
        Err(IndividualSourceDescriptorError::ZeroAckWait)
    );
}

#[test]
fn individual_operations_fail_classified_without_emulation_and_delivery_splits_once() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let support = IndividualSupport {
        delayed_retry: false,
        terminal_discard: true,
        heartbeat: false,
    };
    let mut settlement = IndividualFixtureSettlement {
        support,
        events: Arc::clone(&events),
        progress: Arc::default(),
    };

    let heartbeat_error = block_on(settlement.heartbeat()).unwrap_err();
    assert!(matches!(
        heartbeat_error,
        IndividualSettlementError::Unsupported(IndividualCapability::Heartbeat)
    ));
    assert_eq!(heartbeat_error.classify(), FailureKind::Permanent);
    let retry_error = block_on(
        IndividualFixtureSettlement {
            support,
            events: Arc::clone(&events),
            progress: Arc::default(),
        }
        .nak(Duration::from_secs(10)),
    )
    .unwrap_err();
    assert!(matches!(
        retry_error,
        IndividualSettlementError::Unsupported(IndividualCapability::DelayedRetry)
    ));
    assert_eq!(
        retry_error.to_string(),
        "individual settlement operation is unsupported"
    );
    assert!(events.lock().unwrap().is_empty());

    let mut nats = NatsFixture::new();
    let unpolled_receive = Box::pin(nats.receive());
    drop(unpolled_receive);
    let delivery = block_on(nats.receive()).unwrap().unwrap();
    let mut unresolved_receive = Box::pin(nats.receive());
    assert!(poll_once(unresolved_receive.as_mut()).is_pending());
    drop(unresolved_receive);
    let (wire, settlement) = delivery.into_parts();
    assert_eq!(wire, [1, 2, 3]);
    block_on(settlement.ack()).unwrap();
    assert!(block_on(nats.receive()).unwrap().is_none());

    let mut redis = RedisStreamsFixture::new();
    let (wire, settlement) = block_on(redis.receive()).unwrap().unwrap().into_parts();
    assert_eq!(wire, [1, 2, 3]);
    let error = block_on(settlement.terminate()).unwrap_err();
    assert!(matches!(
        error,
        IndividualSettlementError::Unsupported(IndividualCapability::TerminalDiscard)
    ));
}

#[test]
fn cancel_safe_individual_readiness_keeps_the_delivery_available() {
    let support = IndividualSupport {
        delayed_retry: true,
        terminal_discard: true,
        heartbeat: true,
    };
    let ready = Arc::new(AtomicBool::new(false));
    let mut source = ReadinessFixture {
        ready: Arc::clone(&ready),
        source: IndividualSourceFixture::new(
            support.descriptor(Some(Duration::from_secs(30)), NonZeroU64::new(5)),
            support,
            Arc::default(),
        ),
    };
    let unpolled = Box::pin(source.receive());
    drop(unpolled);
    let mut pending = Box::pin(source.receive());
    assert!(poll_once(pending.as_mut()).is_pending());
    drop(pending);

    ready.store(true, AtomicOrdering::SeqCst);
    let delivery = block_on(source.receive()).unwrap().unwrap();
    assert_eq!(delivery.into_parts().0, [1, 2, 3]);
}

fn take_partition_delivery(
    source: &mut impl PartitionedLogDeliverySource<
        Partition = PartitionId,
        Delivery = PartitionFixtureDelivery,
        Error = ContractError,
    >,
) -> PartitionFixtureDelivery {
    match block_on(source.receive()).unwrap() {
        PartitionedLogReceive::Delivery(delivery) => delivery,
        PartitionedLogReceive::OwnershipLost(partition) => {
            panic!("unexpected ownership loss for partition {partition:?}")
        }
        PartitionedLogReceive::Closed => panic!("unexpected closed partition source"),
    }
}

fn take_partition_and_settlement<D, P>(delivery: D) -> (P, <D as Delivery>::Settlement)
where
    D: Delivery,
    <D as Delivery>::Settlement: PartitionedLogSettlement<Partition = P>,
    P: Clone,
{
    let (_, settlement) = delivery.into_parts();
    let partition = settlement.partition().clone();
    (partition, settlement)
}

#[test]
fn partitioned_sources_hold_gaps_and_allow_other_partitions_to_progress() {
    let mut kafka = KafkaFixture::new();
    block_on(kafka.open()).unwrap();

    let first = take_partition_delivery(&mut kafka);
    assert_eq!(first.partition, PartitionId(0));
    assert_eq!(first.offset, 1);
    let second_partition = take_partition_delivery(&mut kafka);
    assert_eq!(second_partition.partition, PartitionId(1));
    assert_eq!(second_partition.offset, 1);

    let (offset, second_settlement) = second_partition.into_parts();
    assert_eq!(offset, 1);
    second_settlement.mark_durably_resolved();
    assert_eq!(
        block_on(second_settlement.advance()).unwrap(),
        PartitionAdvance::Advanced
    );

    let mut blocked_by_gap = Box::pin(kafka.receive());
    assert!(poll_once(blocked_by_gap.as_mut()).is_pending());
    drop(blocked_by_gap);
    let (first_offset, first_settlement) = first.into_parts();
    assert_eq!(first_offset, 1);
    first_settlement.mark_durably_resolved();
    assert_eq!(
        block_on(first_settlement.advance()).unwrap(),
        PartitionAdvance::Advanced
    );

    let later = take_partition_delivery(&mut kafka);
    assert_eq!(later.partition, PartitionId(0));
    assert_eq!(later.offset, 2);

    let mut iggy = IggyFixture::new();
    block_on(iggy.open()).unwrap();
    let iggy_delivery = take_partition_delivery(&mut iggy);
    assert_eq!(iggy_delivery.partition, PartitionId(0));
    assert_eq!(iggy_delivery.offset, 1);
}

#[test]
fn partition_receive_only_consumes_after_poll_and_preserves_pending_readiness() {
    let mut unpolled_source = IggyFixture::new();
    let unpolled_receive = Box::pin(unpolled_source.receive());
    drop(unpolled_receive);
    let unpolled_delivery = take_partition_delivery(&mut unpolled_source);
    assert_eq!(unpolled_delivery.offset, 1);

    let ready = Arc::new(AtomicBool::new(false));
    let mut readiness_source = PartitionReadinessFixture {
        ready: Arc::clone(&ready),
        source: PartitionedFixtureSource::new(&[(PartitionId(0), 1)], &[]),
    };
    let mut pending_receive = Box::pin(readiness_source.receive());
    assert!(poll_once(pending_receive.as_mut()).is_pending());
    drop(pending_receive);
    {
        let state = readiness_source.source.partitions[&PartitionId(0)]
            .lock()
            .unwrap();
        assert_eq!(state.next_offset, 1);
        assert_eq!(state.unresolved, None);
    }

    ready.store(true, AtomicOrdering::SeqCst);
    let delivery = take_partition_delivery(&mut readiness_source);
    assert_eq!(delivery.offset, 1);
}

#[test]
fn partition_ownership_loss_and_indeterminate_advances_reconcile_before_restart() {
    let mut lost_source = KafkaFixture(PartitionedFixtureSource::new(
        &[(PartitionId(3), 1)],
        &[PartitionId(3)],
    ));
    assert!(matches!(
        block_on(lost_source.receive()).unwrap(),
        PartitionedLogReceive::OwnershipLost(PartitionId(3))
    ));

    let mut unpolled_advance_source = IggyFixture::new();
    let (_, unpolled_advance) = take_partition_delivery(&mut unpolled_advance_source).into_parts();
    let state = Arc::clone(&unpolled_advance_source.0.partitions[&PartitionId(0)]);
    unpolled_advance.mark_durably_resolved();
    let unpolled_advance = Box::pin(unpolled_advance.advance());
    drop(unpolled_advance);
    {
        let state = state.lock().unwrap();
        assert_eq!(state.committed_offset, 0);
        assert_eq!(state.unresolved, Some(1));
    }
    unpolled_advance_source.0.restart_partition(PartitionId(0));
    let replay = take_partition_delivery(&mut unpolled_advance_source);
    assert_eq!(replay.offset, 1);

    let mut before_effect_source = IggyFixture::new();
    before_effect_source.0.next_advance_mode = AdvanceMode::ErrorBeforeEffect;
    let before_effect = take_partition_delivery(&mut before_effect_source).settlement;
    let state = Arc::clone(&before_effect_source.0.partitions[&PartitionId(0)]);
    before_effect.mark_durably_resolved();
    let error = block_on(before_effect.advance()).unwrap_err();
    assert_eq!(error.classify(), FailureKind::Transient);
    {
        let state = state.lock().unwrap();
        assert_eq!(state.committed_offset, 0);
        assert_eq!(state.unresolved, Some(1));
        assert!(state.paused);
        assert!(state.reconcile_required);
    }
    before_effect_source.0.restart_partition(PartitionId(0));
    let replay = take_partition_delivery(&mut before_effect_source);
    assert_eq!(replay.offset, 1);

    let mut after_effect_error_source = IggyFixture::new();
    after_effect_error_source.0.next_advance_mode = AdvanceMode::ErrorAfterEffect;
    let after_effect_error = take_partition_delivery(&mut after_effect_error_source).settlement;
    let state = Arc::clone(&after_effect_error_source.0.partitions[&PartitionId(0)]);
    after_effect_error.mark_durably_resolved();
    let error = block_on(after_effect_error.advance()).unwrap_err();
    assert_eq!(error.classify(), FailureKind::Transient);
    {
        let state = state.lock().unwrap();
        assert_eq!(state.committed_offset, 1);
        assert_eq!(state.unresolved, Some(1));
        assert!(state.paused);
        assert!(state.reconcile_required);
    }
    let next = take_partition_delivery(&mut after_effect_error_source);
    assert_eq!(next.offset, 2);

    let mut after_effect_pending_source = IggyFixture::new();
    after_effect_pending_source.0.next_advance_mode = AdvanceMode::PendingAfterEffect;
    let after_effect_pending = take_partition_delivery(&mut after_effect_pending_source).settlement;
    let state = Arc::clone(&after_effect_pending_source.0.partitions[&PartitionId(0)]);
    after_effect_pending.mark_durably_resolved();
    let mut pending_advance = Box::pin(after_effect_pending.advance());
    assert!(poll_once(pending_advance.as_mut()).is_pending());
    drop(pending_advance);
    {
        let state = state.lock().unwrap();
        assert_eq!(state.committed_offset, 1);
        assert_eq!(state.unresolved, Some(1));
        assert!(state.paused);
        assert!(state.reconcile_required);
    }
    after_effect_pending_source
        .0
        .restart_partition(PartitionId(0));
    let next = take_partition_delivery(&mut after_effect_pending_source);
    assert_eq!(next.offset, 2);

    let mut relinquished_source = IggyFixture::new();
    let mut relinquished = take_partition_delivery(&mut relinquished_source).settlement;
    let state = Arc::clone(&relinquished_source.0.partitions[&PartitionId(0)]);
    relinquished.mark_durably_resolved();
    relinquished.mode = AdvanceMode::OwnershipLost;
    assert_eq!(
        block_on(relinquished.advance()).unwrap(),
        PartitionAdvance::OwnershipLost
    );
    {
        let state = state.lock().unwrap();
        assert_eq!(state.committed_offset, 0);
        assert_eq!(state.unresolved, Some(1));
        assert!(!state.owned);
    }

    let mut stale_source = IggyFixture::new();
    let (delivery_partition, stale) =
        take_partition_and_settlement(take_partition_delivery(&mut stale_source));
    let state = Arc::clone(&stale_source.0.partitions[&PartitionId(0)]);
    stale.mark_durably_resolved();
    state.lock().unwrap().generation += 1;
    assert_eq!(
        block_on(stale.advance()).unwrap(),
        PartitionAdvance::OwnershipLost
    );
    assert!(matches!(
        block_on(stale_source.receive()).unwrap(),
        PartitionedLogReceive::OwnershipLost(partition) if partition == delivery_partition
    ));
    {
        let state = state.lock().unwrap();
        assert_eq!(state.committed_offset, 0);
        assert_eq!(state.unresolved, Some(1));
    }
    stale_source.0.restart_partition(delivery_partition);
    let replay = take_partition_delivery(&mut stale_source);
    assert_eq!(replay.partition, delivery_partition);
    assert_eq!(replay.offset, 1);
}

#[test]
fn contract_futures_retain_send_and_existing_publication_contracts() {
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
