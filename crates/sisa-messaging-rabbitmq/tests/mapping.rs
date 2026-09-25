//! Broker-free AMQP projection, validation, boundary, and redaction tests.

use lapin::{
    BasicProperties,
    protocol::basic::gen_properties,
    types::{AMQPValue, FieldArray, FieldTable, LongString, ShortString},
};
use sisa_messaging::{
    ContentType, ConversationId, EnvelopeMapper, ErrorClassifier, FailureKind, HeaderName,
    HeaderValue, MessageId, MessageType, Metadata, MetadataValue, OrderingKey, RequestId,
    SerializedEnvelope,
};
use sisa_messaging_rabbitmq::{
    ExchangeName, MappingError, RabbitMqError, RabbitMqMapper, RabbitMqSourceSettings,
    RabbitMqWire, Route, RouteResolver, RoutingKey, TypeRouteResolver,
};
use std::num::NonZeroU16;

const SECRET: &str = "secret-token-value";

fn mapper() -> RabbitMqMapper<TypeRouteResolver> {
    RabbitMqMapper::new(TypeRouteResolver::new(ExchangeName::new("events").unwrap()))
}

fn minimal(payload: Vec<u8>) -> SerializedEnvelope {
    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("order_created").unwrap(),
        message_version: 2,
        content_type: ContentType::new("application/json").unwrap(),
        payload,
        metadata: Metadata::default(),
        ordering_key: None,
    }
}

fn full() -> SerializedEnvelope {
    let mut envelope = minimal(br#"{"order":1}"#.to_vec());
    let metadata = &mut envelope.metadata;
    metadata.correlation.correlation_id = Some(MetadataValue::new("correlation").unwrap());
    metadata.correlation.conversation_id = Some(ConversationId::new());
    metadata.correlation.causation_id = Some(MessageId::new());
    metadata.correlation.request_id = Some(RequestId::new());
    metadata.routing.source = Some(MetadataValue::new("billing").unwrap());
    metadata.routing.destination = Some(MetadataValue::new("orders").unwrap());
    metadata.routing.reply_to = Some(MetadataValue::new("replies").unwrap());
    metadata.delivery.sent_at_ms = Some(1_700_000_000_000);
    metadata.delivery.deduplication_id = Some(MetadataValue::new("stable-retry-id").unwrap());
    metadata.tenant_id = Some(MetadataValue::new("tenant").unwrap());

    metadata.trace.traceparent =
        Some(HeaderValue::new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01").unwrap());

    metadata.trace.tracestate = Some(HeaderValue::new("vendor=value").unwrap());

    for (name, value) in [("x-tenant-label", "public"), ("priority-class", "gold")] {
        metadata
            .headers
            .insert(
                HeaderName::new(name).unwrap(),
                HeaderValue::new(value).unwrap(),
            )
            .unwrap();
    }

    envelope.ordering_key = Some(OrderingKey::new("customer-7").unwrap());

    envelope
}

fn table(wire: &RabbitMqWire) -> FieldTable {
    wire.properties.headers().clone().unwrap()
}

fn with_table(wire: &RabbitMqWire, table: FieldTable) -> RabbitMqWire {
    RabbitMqWire {
        properties: wire.properties.clone().with_headers(table),
        ..wire.clone()
    }
}

fn long(value: &str) -> AMQPValue {
    AMQPValue::LongString(LongString::from(value))
}

/// Encoded content-header frame payload: class id, weight, and body size, then the flags and
/// property list exactly as the AMQP client serializes them.
fn content_header_len(properties: &BasicProperties) -> usize {
    let context = gen_properties::<Vec<u8>>(properties)(Vec::new().into()).unwrap();

    2 + 2 + 8 + usize::try_from(context.position).unwrap()
}

#[test]
fn full_envelope_round_trips_through_headers() {
    let mapper = mapper();
    let envelope = full();

    let wire = mapper.encode(&envelope).unwrap();

    assert_eq!(wire.exchange, "events");
    assert_eq!(wire.routing_key, "order_created.v2");
    assert_eq!(wire.payload, envelope.payload);
    assert_eq!(mapper.decode(wire).unwrap(), envelope);
}

#[test]
fn every_header_is_a_namespaced_long_string() {
    let wire = mapper().encode(&full()).unwrap();

    let headers = table(&wire);

    assert_eq!(headers.inner().len(), 17 + 2);

    for (name, value) in &headers {
        assert!(name.as_str().starts_with("sisa-"));
        assert!(matches!(value, AMQPValue::LongString(_)));
    }

    assert!(headers.contains_key("sisa-custom-x-tenant-label"));
    assert!(headers.contains_key("sisa-ordering-key"));
}

#[test]
fn identity_content_type_and_persistence_are_mirrored_into_properties() {
    let envelope = minimal(vec![1, 2, 3]);

    let wire = mapper().encode(&envelope).unwrap();

    let message_id = envelope.message_id.to_string();

    assert_eq!(
        wire.properties.message_id().as_ref().unwrap().as_str(),
        message_id
    );

    assert_eq!(
        wire.properties.content_type().as_ref().unwrap().as_str(),
        "application/json"
    );

    assert_eq!(*wire.properties.delivery_mode(), Some(2));
}

#[test]
fn encoding_is_deterministic() {
    let mapper = mapper();
    let envelope = full();

    let first = mapper.encode(&envelope).unwrap();
    let second = mapper.encode(&envelope).unwrap();

    assert_eq!(first.properties, second.properties);
    assert_eq!(first.routing_key, second.routing_key);
}

#[test]
fn decoding_ignores_properties_and_route() {
    let mapper = mapper();
    let envelope = full();
    let wire = mapper.encode(&envelope).unwrap();

    let forged = RabbitMqWire {
        exchange: "other".into(),
        routing_key: "other.key".into(),
        properties: wire
            .properties
            .clone()
            .with_message_id(ShortString::from("forged"))
            .with_content_type(ShortString::from("text/plain"))
            .with_delivery_mode(1),
        payload: wire.payload.clone(),
    };

    assert_eq!(mapper.decode(forged).unwrap(), envelope);
}

#[test]
fn broker_headers_are_ignored() {
    let mapper = mapper();
    let envelope = full();
    let wire = mapper.encode(&envelope).unwrap();
    let mut headers = table(&wire);

    headers.insert(
        "x-death".into(),
        AMQPValue::FieldArray(FieldArray::from(vec![AMQPValue::FieldTable(
            FieldTable::default(),
        )])),
    );

    headers.insert("x-delivery-count".into(), AMQPValue::LongLongInt(3));
    headers.insert("x-first-death-reason".into(), long("rejected"));
    headers.insert("application".into(), AMQPValue::Boolean(true));

    assert_eq!(mapper.decode(with_table(&wire, headers)).unwrap(), envelope);
}

#[test]
fn missing_table_or_required_header_is_invalid_envelope() {
    let mapper = mapper();
    let wire = mapper.encode(&minimal(vec![])).unwrap();

    let no_table = RabbitMqWire {
        properties: BasicProperties::default(),
        ..wire.clone()
    };

    assert_eq!(
        mapper.decode(no_table).err(),
        Some(MappingError::InvalidEnvelope)
    );

    for required in [
        "sisa-message-id",
        "sisa-message-type",
        "sisa-message-version",
        "sisa-content-type",
    ] {
        let mut entries = table(&wire).inner().clone();
        entries.remove(required);

        assert_eq!(
            mapper.decode(with_table(&wire, entries.into())).err(),
            Some(MappingError::InvalidEnvelope)
        );
    }
}

#[test]
fn malformed_framework_values_are_invalid_envelope() {
    let mapper = mapper();
    let wire = mapper.encode(&minimal(vec![])).unwrap();

    for (name, value) in [
        ("sisa-message-id", "not-a-uuid"),
        ("sisa-message-version", "two"),
        ("sisa-sent-at-ms", "-1"),
        ("sisa-conversation-id", SECRET),
    ] {
        let mut headers = table(&wire);
        headers.insert(ShortString::from(name), long(value));

        let error = mapper.decode(with_table(&wire, headers)).err().unwrap();

        assert_eq!(error, MappingError::InvalidEnvelope);
        assert!(!format!("{error:?} {error}").contains(value));
    }
}

#[test]
fn non_long_string_framework_or_custom_values_are_rejected() {
    let mapper = mapper();
    let wire = mapper.encode(&full()).unwrap();

    for (name, value) in [
        (
            "sisa-message-id",
            AMQPValue::ShortString(ShortString::from("x")),
        ),
        ("sisa-message-version", AMQPValue::LongUInt(2)),
        ("sisa-custom-x-tenant-label", AMQPValue::Boolean(true)),
        ("sisa-unknown-future", AMQPValue::Void),
        (
            "sisa-tenant-id",
            AMQPValue::LongString(vec![0xff, 0xfe].into()),
        ),
    ] {
        let mut headers = table(&wire);
        headers.insert(ShortString::from(name), value);

        assert_eq!(
            mapper.decode(with_table(&wire, headers)).err(),
            Some(MappingError::InvalidHeaders)
        );
    }
}

#[test]
fn case_variant_duplicates_are_rejected() {
    let mapper = mapper();
    let envelope = full();
    let wire = mapper.encode(&envelope).unwrap();

    for (name, value) in [
        ("Sisa-Message-Id", envelope.message_id.to_string()),
        ("SISA-TENANT-ID", "tenant".to_owned()),
        ("sisa-custom-X-Tenant-Label", "public".to_owned()),
        ("Sisa-Custom-x-tenant-label", "other".to_owned()),
    ] {
        let mut headers = table(&wire);
        headers.insert(ShortString::from(name), long(&value));

        assert_eq!(
            mapper.decode(with_table(&wire, headers)).err(),
            Some(MappingError::InvalidHeaders)
        );
    }
}

#[test]
fn reserved_or_invalid_custom_names_are_rejected() {
    let mapper = mapper();
    let wire = mapper.encode(&minimal(vec![])).unwrap();

    for name in [
        "sisa-custom-message-id",
        "sisa-custom-",
        "sisa-custom-bad name",
    ] {
        let mut headers = table(&wire);
        headers.insert(ShortString::from(name), long("value"));

        assert_eq!(
            mapper.decode(with_table(&wire, headers)).err(),
            Some(MappingError::InvalidHeaders)
        );
    }
}

#[test]
fn unknown_framework_names_are_ignored_for_forward_compatibility() {
    let mapper = mapper();
    let envelope = minimal(vec![]);
    let wire = mapper.encode(&envelope).unwrap();
    let mut headers = table(&wire);

    headers.insert("sisa-future-field".into(), long("value"));

    assert_eq!(mapper.decode(with_table(&wire, headers)).unwrap(), envelope);
}

#[test]
fn route_components_are_bounded_at_255_bytes() {
    let at_limit = "e".repeat(255);
    let over_limit = "e".repeat(256);

    assert_eq!(
        ExchangeName::new(at_limit.clone()).unwrap().as_str(),
        at_limit
    );

    assert_eq!(
        ExchangeName::new(over_limit.clone()).err(),
        Some(MappingError::InvalidRoute)
    );

    assert_eq!(
        RoutingKey::new(at_limit.clone()).unwrap().as_str(),
        at_limit
    );

    assert_eq!(
        RoutingKey::new(over_limit).err(),
        Some(MappingError::InvalidRoute)
    );
}

#[test]
fn route_components_reject_control_bytes_and_implicit_default_exchange() {
    assert_eq!(
        ExchangeName::new("").err(),
        Some(MappingError::InvalidRoute)
    );

    assert_eq!(
        ExchangeName::new("ev\nents").err(),
        Some(MappingError::InvalidRoute)
    );

    assert_eq!(
        RoutingKey::new("key\u{7f}").err(),
        Some(MappingError::InvalidRoute)
    );

    assert!(RoutingKey::new("").unwrap().as_str().is_empty());

    assert!(ExchangeName::default_exchange().as_str().is_empty());
    assert_eq!(ExchangeName::new("events").unwrap().as_str(), "events");
}

#[test]
fn resolved_routing_key_over_255_bytes_is_a_mapping_error() {
    let mut envelope = minimal(vec![]);
    envelope.message_type = MessageType::new("t".repeat(250)).unwrap();
    envelope.message_version = 12_345;

    assert_eq!(
        mapper().encode(&envelope).err(),
        Some(MappingError::InvalidRoute)
    );

    envelope.message_version = 1;

    assert_eq!(mapper().encode(&envelope).unwrap().routing_key.len(), 253);
}

#[test]
fn custom_resolvers_route_through_the_default_exchange() {
    struct QueueResolver;

    impl RouteResolver for QueueResolver {
        fn resolve(&self, _: &SerializedEnvelope) -> Result<Route, MappingError> {
            Ok(Route {
                exchange: ExchangeName::default_exchange(),
                routing_key: RoutingKey::new("orders")?,
            })
        }
    }

    let wire = RabbitMqMapper::new(QueueResolver)
        .encode(&minimal(vec![]))
        .unwrap();

    assert_eq!(wire.exchange, "");
    assert_eq!(wire.routing_key, "orders");
}

#[test]
fn content_header_frame_is_bounded_at_4088_bytes() {
    let mapper = mapper();
    let name = HeaderName::new("padding").unwrap();

    let padded = |length: usize| {
        let mut envelope = minimal(vec![]);

        envelope
            .metadata
            .headers
            .insert(name.clone(), HeaderValue::new("p".repeat(length)).unwrap())
            .unwrap();

        envelope
    };

    let baseline = content_header_len(&mapper.encode(&padded(0)).unwrap().properties);
    let at_limit = padded(4_088 - baseline);

    let wire = mapper.encode(&at_limit).unwrap();

    assert_eq!(content_header_len(&wire.properties), 4_088);
    assert_eq!(mapper.decode(wire).unwrap(), at_limit);

    assert_eq!(
        mapper.encode(&padded(4_088 - baseline + 1)).err(),
        Some(MappingError::HeadersTooLarge)
    );
}

#[test]
fn debug_and_display_never_render_routes_or_wire_data() {
    let exchange = ExchangeName::new(format!("exchange-{SECRET}")).unwrap();
    let routing_key = RoutingKey::new(format!("key-{SECRET}")).unwrap();

    let route = Route {
        exchange: exchange.clone(),
        routing_key: routing_key.clone(),
    };

    let resolver = TypeRouteResolver::new(exchange.clone());

    let mut envelope = full();
    envelope.payload = SECRET.as_bytes().to_vec();
    let wire = mapper().encode(&envelope).unwrap();

    let settings = RabbitMqSourceSettings {
        queue: format!("queue-{SECRET}"),
        prefetch: NonZeroU16::new(8).unwrap(),
    };

    let rendered = format!(
        "{exchange:?} {exchange} {routing_key:?} {routing_key} {route:?} {route} {resolver:?} \
         {wire:?} {settings:?} {:?} {} {:?} {}",
        MappingError::InvalidRoute,
        MappingError::HeadersTooLarge,
        RabbitMqError::Unroutable,
        RabbitMqError::Publish,
    );

    assert!(!rendered.contains(SECRET));
    assert!(!rendered.contains("public"));
}

#[test]
fn only_local_invariant_failures_are_permanent() {
    for (error, expected) in [
        (RabbitMqError::Settings, FailureKind::Permanent),
        (RabbitMqError::Mapping, FailureKind::Permanent),
        (RabbitMqError::PayloadTooLarge, FailureKind::Permanent),
        (RabbitMqError::Unroutable, FailureKind::Transient),
        (RabbitMqError::Rejected, FailureKind::Transient),
        (RabbitMqError::Publish, FailureKind::Transient),
        (RabbitMqError::Timeout, FailureKind::Transient),
        (RabbitMqError::Source, FailureKind::Transient),
        (RabbitMqError::Settlement, FailureKind::Transient),
    ] {
        assert_eq!(error.classify(), expected, "{error:?}");
    }

    for error in [
        MappingError::InvalidRoute,
        MappingError::InvalidEnvelope,
        MappingError::InvalidHeaders,
        MappingError::HeadersTooLarge,
    ] {
        assert_eq!(error.classify(), FailureKind::Permanent);
    }
}
