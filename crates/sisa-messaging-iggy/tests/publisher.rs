use std::error::Error as StdError;
use std::time::Duration;

use iggy::prelude::{Identifier, IggyError};
use sisa_messaging::{
    ContentType, ErrorClassifier, FailureKind, MessageId, MessageType, Metadata, MetadataValue,
    RoutingMetadata, SerializedEnvelope,
};
use sisa_messaging_iggy::{
    IggyClient, IggyClientError, IggyClientErrorKind, IggyClientSettings, IggyCredentials,
    IggyDestinationResolver, IggyPublishError, IggyPublishErrorKind, IggyPublisherSettings,
    IggyTlsSettings, RoutingDestinationResolver,
};

fn valid_settings() -> IggyClientSettings {
    IggyClientSettings::new(
        "localhost:8090",
        IggyCredentials::UsernamePassword {
            username: "iggy".to_owned(),
            password: "iggy".to_owned(),
        },
    )
}

#[tokio::test]
async fn empty_server_address_is_rejected_before_any_connection_attempt() {
    let settings = IggyClientSettings::new(
        "   ",
        IggyCredentials::UsernamePassword {
            username: "iggy".to_owned(),
            password: "iggy".to_owned(),
        },
    );

    let error = IggyClient::start(settings)
        .await
        .expect_err("blank server address must be rejected");

    assert_eq!(error.kind(), IggyClientErrorKind::InvalidSettings);
    assert_eq!(error.classify(), FailureKind::Permanent);
    assert!(StdError::source(&error).is_none());
}

#[tokio::test]
async fn empty_username_or_password_is_rejected() {
    let settings = IggyClientSettings::new(
        "localhost:8090",
        IggyCredentials::UsernamePassword {
            username: String::new(),
            password: "iggy".to_owned(),
        },
    );

    let error = IggyClient::start(settings)
        .await
        .expect_err("empty username must be rejected");

    assert_eq!(error.kind(), IggyClientErrorKind::InvalidSettings);

    let settings = IggyClientSettings::new(
        "localhost:8090",
        IggyCredentials::UsernamePassword {
            username: "iggy".to_owned(),
            password: String::new(),
        },
    );

    let error = IggyClient::start(settings)
        .await
        .expect_err("empty password must be rejected");

    assert_eq!(error.kind(), IggyClientErrorKind::InvalidSettings);
}

#[tokio::test]
async fn empty_personal_access_token_is_rejected() {
    let settings = IggyClientSettings::new(
        "localhost:8090",
        IggyCredentials::PersonalAccessToken(String::new()),
    );

    let error = IggyClient::start(settings)
        .await
        .expect_err("empty personal access token must be rejected");

    assert_eq!(error.kind(), IggyClientErrorKind::InvalidSettings);
}

#[tokio::test]
async fn zero_connect_timeout_is_rejected() {
    let settings = valid_settings().with_connect_timeout(Duration::ZERO);

    let error = IggyClient::start(settings)
        .await
        .expect_err("a zero connect timeout must be rejected");

    assert_eq!(error.kind(), IggyClientErrorKind::InvalidSettings);
}

#[tokio::test]
async fn zero_heartbeat_interval_is_rejected() {
    let settings = valid_settings().with_heartbeat_interval(Duration::ZERO);

    let error = IggyClient::start(settings)
        .await
        .expect_err("a zero heartbeat interval must be rejected");

    assert_eq!(error.kind(), IggyClientErrorKind::InvalidSettings);
}

#[tokio::test]
async fn malformed_server_address_is_rejected() {
    // Missing a port, matching the SDK's own `validate_server_address` rejection.
    let settings = IggyClientSettings::new(
        "127.0.0.1",
        IggyCredentials::UsernamePassword {
            username: "iggy".to_owned(),
            password: "iggy".to_owned(),
        },
    );

    let error = IggyClient::start(settings)
        .await
        .expect_err("a malformed server address must be rejected");

    assert_eq!(error.kind(), IggyClientErrorKind::InvalidSettings);
    assert_eq!(error.classify(), FailureKind::Permanent);
}

#[test]
fn zero_send_timeout_is_rejected() {
    let error = IggyPublisherSettings::new(Duration::ZERO)
        .expect_err("a zero send timeout must be rejected");

    assert_eq!(error.classify(), FailureKind::Permanent);
}

#[test]
fn nonzero_send_timeout_is_accepted() {
    let settings = IggyPublisherSettings::new(Duration::from_secs(1))
        .expect("a positive send timeout is valid");

    assert_eq!(settings.send_timeout(), Duration::from_secs(1));
}

#[test]
fn client_settings_debug_never_leaks_credentials_address_or_tls_paths() {
    const SENSITIVE_ADDRESS: &str = "SENSITIVE_ADDRESS_MARKER:8090";
    const SENSITIVE_PASSWORD: &str = "SENSITIVE_PASSWORD_MARKER";
    const SENSITIVE_TOKEN: &str = "SENSITIVE_TOKEN_MARKER";
    const SENSITIVE_DOMAIN: &str = "SENSITIVE_DOMAIN_MARKER";
    const SENSITIVE_CA_FILE: &str = "SENSITIVE_CA_FILE_MARKER";

    let credentials = IggyCredentials::UsernamePassword {
        username: "iggy".to_owned(),
        password: SENSITIVE_PASSWORD.to_owned(),
    };
    let tls = IggyTlsSettings::new(SENSITIVE_DOMAIN).with_ca_file(SENSITIVE_CA_FILE);
    let settings =
        IggyClientSettings::new(SENSITIVE_ADDRESS, credentials.clone()).with_tls(tls.clone());

    let settings_debug = format!("{settings:?}");
    let credentials_debug = format!("{credentials:?}");
    let token_debug = format!(
        "{:?}",
        IggyCredentials::PersonalAccessToken(SENSITIVE_TOKEN.to_owned())
    );
    let tls_debug = format!("{tls:?}");

    for haystack in [
        &settings_debug,
        &credentials_debug,
        &token_debug,
        &tls_debug,
    ] {
        assert!(
            !haystack.contains(SENSITIVE_ADDRESS),
            "server address leaked in: {haystack}"
        );
        assert!(
            !haystack.contains(SENSITIVE_PASSWORD),
            "password leaked in: {haystack}"
        );
        assert!(
            !haystack.contains(SENSITIVE_TOKEN),
            "token leaked in: {haystack}"
        );
        assert!(
            !haystack.contains(SENSITIVE_DOMAIN),
            "TLS domain leaked in: {haystack}"
        );
        assert!(
            !haystack.contains(SENSITIVE_CA_FILE),
            "TLS CA file path leaked in: {haystack}"
        );
    }
}

#[test]
fn iggy_client_error_from_covers_every_known_arm() {
    let cases = [
        (
            IggyError::Unauthenticated,
            IggyClientErrorKind::Authentication,
        ),
        (IggyError::Unauthorized, IggyClientErrorKind::Authentication),
        (
            IggyError::InvalidCredentials,
            IggyClientErrorKind::Authentication,
        ),
        (
            IggyError::InvalidUsername,
            IggyClientErrorKind::Authentication,
        ),
        (
            IggyError::InvalidPassword,
            IggyClientErrorKind::Authentication,
        ),
        (IggyError::InvalidTlsDomain, IggyClientErrorKind::Tls),
        (
            IggyError::InvalidTlsCertificatePath,
            IggyClientErrorKind::Tls,
        ),
        (IggyError::InvalidTlsCertificate, IggyClientErrorKind::Tls),
        (IggyError::FailedToAddCertificate, IggyClientErrorKind::Tls),
        (IggyError::Disconnected, IggyClientErrorKind::Connect),
        (
            IggyError::CannotEstablishConnection,
            IggyClientErrorKind::Connect,
        ),
        (IggyError::TcpError, IggyClientErrorKind::Connect),
        (IggyError::InvalidCommand, IggyClientErrorKind::Connect),
    ];

    for (error, expected_kind) in cases {
        assert_eq!(IggyClientError::from(error).kind(), expected_kind);
    }
}

/// Covers every arm of the crate's `IggyError` classification for publish failures.
///
/// `IggyError::RequestAlreadyApplied` is deliberately absent from this table: `publish` never
/// routes it through this conversion. It intercepts that variant beforehand and reports success,
/// since the server's own deduplication confirms the request already committed. Proving that
/// interception needs a real duplicate request from a broker, so it is left to the opt-in
/// real-broker test rather than asserted here.
#[test]
fn iggy_publish_error_from_covers_every_known_arm() {
    let cases = [
        (
            IggyError::Unauthorized,
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::InvalidCredentials,
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::StreamIdNotFound(Identifier::default()),
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::StreamNameNotFound(String::new()),
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::TopicIdNotFound(Identifier::default(), Identifier::default()),
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::TopicNameNotFound(String::new(), String::new()),
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::TooBigMessagePayload,
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::TooBigUserHeaders,
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::InvalidMessagePayloadLength,
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::EmptyMessagePayload,
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::InvalidHeaderValue,
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::InvalidHeaderKey,
            IggyPublishErrorKind::Rejected,
            FailureKind::Permanent,
        ),
        (
            IggyError::TransientNotAccepted,
            IggyPublishErrorKind::ServerTransient,
            FailureKind::Transient,
        ),
        (
            IggyError::TransientNotCommitted,
            IggyPublishErrorKind::OutcomeUnknown,
            FailureKind::Transient,
        ),
        (
            IggyError::Unauthenticated,
            IggyPublishErrorKind::OutcomeUnknown,
            FailureKind::Transient,
        ),
        (
            IggyError::NotConnected,
            IggyPublishErrorKind::OutcomeUnknown,
            FailureKind::Transient,
        ),
        (
            IggyError::Disconnected,
            IggyPublishErrorKind::OutcomeUnknown,
            FailureKind::Transient,
        ),
        (
            IggyError::ClientShutdown,
            IggyPublishErrorKind::OutcomeUnknown,
            FailureKind::Transient,
        ),
        (
            IggyError::CannotEstablishConnection,
            IggyPublishErrorKind::OutcomeUnknown,
            FailureKind::Transient,
        ),
        (
            IggyError::TcpError,
            IggyPublishErrorKind::OutcomeUnknown,
            FailureKind::Transient,
        ),
        (
            IggyError::StaleClient,
            IggyPublishErrorKind::OutcomeUnknown,
            FailureKind::Transient,
        ),
        (
            IggyError::InvalidCommand,
            IggyPublishErrorKind::OutcomeUnknown,
            FailureKind::Transient,
        ),
    ];

    for (error, expected_kind, expected_failure) in cases {
        let classified = IggyPublishError::from(error);
        assert_eq!(classified.kind(), expected_kind);
        assert_eq!(classified.classify(), expected_failure);
    }
}

fn envelope_with_destination(destination: Option<&str>) -> SerializedEnvelope {
    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("resolver.test").expect("fixture type is valid"),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream")
            .expect("fixture content type is valid"),
        payload: b"payload".to_vec(),
        metadata: Metadata {
            routing: RoutingMetadata {
                destination: destination
                    .map(|value| MetadataValue::new(value).expect("fixture destination is valid")),
                ..RoutingMetadata::default()
            },
            ..Metadata::default()
        },
        ordering_key: None,
    }
}

#[test]
fn routing_resolver_accepts_numeric_and_named_stream_topic_pairs() {
    let resolver = RoutingDestinationResolver;

    resolver
        .resolve(&envelope_with_destination(Some("1/2")))
        .expect("numeric stream and topic must resolve");
    resolver
        .resolve(&envelope_with_destination(Some("orders/created")))
        .expect("named stream and topic must resolve");
    resolver
        .resolve(&envelope_with_destination(Some("1/created")))
        .expect("mixed numeric and named identifiers must resolve");
}

#[test]
fn routing_resolver_rejects_missing_or_malformed_destinations_permanently() {
    let resolver = RoutingDestinationResolver;

    let missing = resolver
        .resolve(&envelope_with_destination(None))
        .expect_err("missing destination must be rejected");
    assert_eq!(missing.classify(), FailureKind::Permanent);
    assert!(StdError::source(&missing).is_none());

    let no_separator = resolver
        .resolve(&envelope_with_destination(Some("orders-created")))
        .expect_err("destination without a separator must be rejected");
    assert_eq!(no_separator.classify(), FailureKind::Permanent);

    let extra_segment = resolver
        .resolve(&envelope_with_destination(Some("orders/created/extra")))
        .expect_err("destination with more than one separator must be rejected");
    assert_eq!(extra_segment.classify(), FailureKind::Permanent);

    let empty_stream = resolver
        .resolve(&envelope_with_destination(Some("/created")))
        .expect_err("an empty stream segment must be rejected");
    assert_eq!(empty_stream.classify(), FailureKind::Permanent);

    let empty_topic = resolver
        .resolve(&envelope_with_destination(Some("orders/")))
        .expect_err("an empty topic segment must be rejected");
    assert_eq!(empty_topic.classify(), FailureKind::Permanent);
}
