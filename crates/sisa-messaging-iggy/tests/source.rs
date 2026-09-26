//! Deterministic delivery-source settings bounds, error classification, and redaction.

use std::time::Duration;

use iggy::prelude::{Identifier, IggyError};
use sisa_messaging::{ErrorClassifier, FailureKind};
use sisa_messaging_iggy::{IggyDeliveryError, IggyDeliveryErrorKind, IggySourceSettings};

fn settings() -> IggySourceSettings {
    IggySourceSettings::new(
        Identifier::from_str_value("orders").unwrap(),
        Identifier::from_str_value("order-created").unwrap(),
        Identifier::from_str_value("orders-projection").unwrap(),
    )
}

fn assert_invalid(result: Result<IggySourceSettings, IggyDeliveryError>) {
    let error = result.unwrap_err();

    assert_eq!(error.kind(), IggyDeliveryErrorKind::InvalidSettings);
    assert_eq!(error.classify(), FailureKind::Permanent);
    assert_eq!(error.code(), None);
}

#[test]
fn batch_length_accepts_only_one_through_1024() {
    assert_invalid(settings().with_batch_length(0));
    assert_invalid(settings().with_batch_length(1_025));
    assert_invalid(settings().with_batch_length(u32::MAX));

    for batch_length in [1, 64, 1_024] {
        assert!(settings().with_batch_length(batch_length).is_ok());
    }
}

#[test]
fn intervals_and_the_request_timeout_are_bounded() {
    assert_invalid(settings().with_poll_interval(Duration::ZERO));
    assert_invalid(settings().with_assignment_refresh_interval(Duration::ZERO));
    assert_invalid(settings().with_request_timeout(Duration::ZERO));

    // Past one hour the source's deadlines could overflow.
    for too_long in [
        Duration::from_secs(60 * 60) + Duration::from_nanos(1),
        Duration::MAX,
    ] {
        assert_invalid(settings().with_poll_interval(too_long));
        assert_invalid(settings().with_assignment_refresh_interval(too_long));
    }

    let hour = Duration::from_secs(60 * 60);

    assert!(settings().with_poll_interval(hour).is_ok());
    assert!(settings().with_assignment_refresh_interval(hour).is_ok());
    assert!(settings().with_request_timeout(Duration::MAX).is_ok());

    let tiny = Duration::from_nanos(1);

    assert!(settings().with_poll_interval(tiny).is_ok());
    assert!(settings().with_assignment_refresh_interval(tiny).is_ok());
    assert!(settings().with_request_timeout(tiny).is_ok());
}

#[test]
fn a_rejected_setter_leaves_no_partial_value_behind() {
    let valid = settings().with_batch_length(8).unwrap();

    assert_eq!(valid.clone().with_batch_length(0).ok(), None);
    assert_eq!(valid.clone(), settings().with_batch_length(8).unwrap());
    assert_ne!(valid, settings());
}

fn classified(error: IggyError) -> (IggyDeliveryErrorKind, FailureKind, Option<u32>) {
    let code = error.as_code();
    let classified = IggyDeliveryError::from(error);

    assert_eq!(
        classified.code(),
        Some(code),
        "the numeric code is preserved"
    );

    (classified.kind(), classified.classify(), classified.code())
}

#[test]
fn credential_and_permission_errors_are_permanent() {
    for error in [
        IggyError::Unauthorized,
        IggyError::InvalidCredentials,
        IggyError::InvalidUsername,
        IggyError::InvalidPassword,
    ] {
        let (kind, failure, _) = classified(error);

        assert_eq!(kind, IggyDeliveryErrorKind::Unauthorized);
        assert_eq!(failure, FailureKind::Permanent);
    }
}

#[test]
fn missing_resources_are_permanent() {
    let stream = Identifier::from_str_value("orders").unwrap();
    let topic = Identifier::from_str_value("order-created").unwrap();

    for error in [
        IggyError::StreamIdNotFound(stream.clone()),
        IggyError::StreamNameNotFound("orders".to_owned()),
        IggyError::TopicIdNotFound(topic.clone(), stream.clone()),
        IggyError::TopicNameNotFound("order-created".to_owned(), "orders".to_owned()),
        IggyError::PartitionNotFound(3, topic.clone(), stream.clone()),
        IggyError::ConsumerGroupIdNotFound(topic.clone(), stream.clone()),
        IggyError::ConsumerGroupNameNotFound("orders-projection".to_owned(), topic.clone()),
    ] {
        let (kind, failure, _) = classified(error);

        assert_eq!(kind, IggyDeliveryErrorKind::NotFound);
        assert_eq!(failure, FailureKind::Permanent);
    }
}

#[test]
fn offset_rejections_are_permanent() {
    for error in [
        IggyError::InvalidOffset(99),
        IggyError::TooManyConsumerOffsets,
    ] {
        let (kind, failure, _) = classified(error);

        assert_eq!(kind, IggyDeliveryErrorKind::Rejected);
        assert_eq!(failure, FailureKind::Permanent);
    }
}

#[test]
fn lost_sessions_are_transient_disconnections() {
    for error in [
        IggyError::Disconnected,
        IggyError::NotConnected,
        IggyError::ClientShutdown,
        IggyError::CannotEstablishConnection,
        IggyError::TcpError,
        IggyError::StaleClient,
        IggyError::Unauthenticated,
    ] {
        let (kind, failure, _) = classified(error);

        assert_eq!(kind, IggyDeliveryErrorKind::Disconnected);
        assert_eq!(failure, FailureKind::Transient);
    }
}

#[test]
fn ownership_fences_and_unrecognized_errors_on_a_store_stay_transient() {
    let topic = Identifier::from_str_value("order-created").unwrap();
    let stream = Identifier::from_str_value("orders").unwrap();

    for error in [
        IggyError::ConsumerGroupPartitionNotOwned(7, 0),
        IggyError::ConsumerGroupMemberNotFound(7, topic.clone(), stream.clone()),
        IggyError::TransientNotCommitted,
        IggyError::TransientNotAccepted,
        IggyError::Error,
        IggyError::InvalidCommand,
    ] {
        let (kind, failure, _) = classified(error);

        assert_eq!(kind, IggyDeliveryErrorKind::Unavailable);
        assert_eq!(failure, FailureKind::Transient);
    }
}

#[test]
fn rendered_errors_never_carry_sdk_text_or_resource_names() {
    let stream = Identifier::from_str_value("secret-stream").unwrap();
    let topic = Identifier::from_str_value("secret-topic").unwrap();

    for error in [
        IggyError::StreamNameNotFound("secret-stream".to_owned()),
        IggyError::TopicNameNotFound("secret-topic".to_owned(), "secret-stream".to_owned()),
        IggyError::ConsumerGroupNameNotFound("secret-group".to_owned(), topic.clone()),
        IggyError::ConsumerGroupMemberNotFound(7, topic.clone(), stream.clone()),
        IggyError::ResourceNotFound("secret-resource".to_owned()),
    ] {
        let sdk_text = error.to_string();
        let classified = IggyDeliveryError::from(error);
        let rendered = format!("{classified} {classified:?}");

        assert!(!rendered.contains("secret"), "rendered: {rendered}");
        assert!(!rendered.contains(&sdk_text));
    }
}
