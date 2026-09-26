//! Redacted, structured Iggy provider errors.

use std::error::Error;
use std::fmt;

use iggy::prelude::IggyError;
use sisa_messaging::{ErrorClassifier, FailureKind};

/// A redacted reason why Iggy client construction or connection failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IggyClientErrorKind {
    /// The supplied settings were incomplete or malformed.
    InvalidSettings,

    /// The server rejected the configured credentials.
    Authentication,

    /// TLS configuration was rejected before or during the handshake.
    Tls,

    /// The transport could not establish or complete the connection.
    Connect,

    /// The connect-and-login sequence did not complete within the configured timeout.
    Timeout,
}

/// A redacted Iggy client construction or connection error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IggyClientError {
    kind: IggyClientErrorKind,
}

impl IggyClientError {
    pub(crate) const fn new(kind: IggyClientErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the structured client construction failure.
    #[must_use]
    pub const fn kind(self) -> IggyClientErrorKind {
        self.kind
    }
}

impl fmt::Display for IggyClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            IggyClientErrorKind::InvalidSettings => "Iggy client settings are invalid",
            IggyClientErrorKind::Authentication => "Iggy server rejected the credentials",
            IggyClientErrorKind::Tls => "Iggy TLS configuration was rejected",
            IggyClientErrorKind::Connect => "Iggy client could not connect",
            IggyClientErrorKind::Timeout => "Iggy connect-and-login did not complete in time",
        })
    }
}

impl Error for IggyClientError {}

impl ErrorClassifier for IggyClientError {
    fn classify(&self) -> FailureKind {
        match self.kind {
            IggyClientErrorKind::InvalidSettings
            | IggyClientErrorKind::Authentication
            | IggyClientErrorKind::Tls => FailureKind::Permanent,
            IggyClientErrorKind::Connect | IggyClientErrorKind::Timeout => FailureKind::Transient,
        }
    }
}

/// Classifies an SDK error encountered while connecting into this crate's client failure
/// taxonomy. This is the crate's single mapping from SDK error to [`IggyClientErrorKind`]; the
/// connect path uses it directly rather than duplicating the match.
impl From<IggyError> for IggyClientError {
    fn from(error: IggyError) -> Self {
        let kind = match error {
            IggyError::Unauthenticated
            | IggyError::Unauthorized
            | IggyError::InvalidCredentials
            | IggyError::InvalidUsername
            | IggyError::InvalidPassword => IggyClientErrorKind::Authentication,
            IggyError::InvalidTlsDomain
            | IggyError::InvalidTlsCertificatePath
            | IggyError::InvalidTlsCertificate
            | IggyError::FailedToAddCertificate => IggyClientErrorKind::Tls,
            _ => IggyClientErrorKind::Connect,
        };

        Self::new(kind)
    }
}

/// A malformed or unrepresentable Iggy envelope projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IggyMappingError {
    /// The envelope payload was empty.
    EmptyPayload,

    /// The envelope payload exceeded the Iggy message payload bound.
    PayloadTooLarge,

    /// A header name or value was malformed or outside shared or Iggy header bounds.
    InvalidHeader,

    /// Two wire headers mapped to the same case-insensitive name.
    DuplicateHeader,

    /// The wire record exceeded shared or Iggy header bounds.
    HeaderBoundsExceeded,

    /// A required framework header was absent.
    MissingRequiredHeader,

    /// A framework header had a value that could not be represented or parsed.
    InvalidFrameworkValue,

    /// The shared ordering key exceeded the 255-byte Iggy messages-key bound.
    InvalidOrderingKey,

    /// The wire record identity did not match the decoded message-id header.
    InvalidRecordId,

    /// The wire record's Iggy messages-key did not match its ordering-key header.
    InvalidRecordKey,
}

impl fmt::Display for IggyMappingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyPayload => "Iggy record payload must not be empty",
            Self::PayloadTooLarge => "Iggy record payload exceeds the message payload bound",
            Self::InvalidHeader => "Iggy record contains an invalid header",
            Self::DuplicateHeader => "Iggy record contains duplicate headers",
            Self::HeaderBoundsExceeded => "Iggy record headers exceed shared or Iggy bounds",
            Self::MissingRequiredHeader => "Iggy record is missing a required framework header",
            Self::InvalidFrameworkValue => "Iggy record contains an invalid framework value",
            Self::InvalidOrderingKey => "Iggy ordering key exceeds the messages-key byte bound",
            Self::InvalidRecordId => "Iggy record id does not match its message-id header",
            Self::InvalidRecordKey => "Iggy record key does not match its ordering-key header",
        })
    }
}

impl Error for IggyMappingError {}

impl ErrorClassifier for IggyMappingError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// The envelope did not contain a well-formed Iggy routing destination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingDestinationError;

impl fmt::Display for RoutingDestinationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Iggy destination is missing or malformed in routing metadata")
    }
}

impl Error for RoutingDestinationError {}

impl ErrorClassifier for RoutingDestinationError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// The publisher settings' send timeout was zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidSendTimeout;

impl fmt::Display for InvalidSendTimeout {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Iggy publisher send timeout must not be zero")
    }
}

impl Error for InvalidSendTimeout {}

impl ErrorClassifier for InvalidSendTimeout {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// Safe, structured class of an Iggy publication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IggyPublishErrorKind {
    /// The application destination resolver rejected the envelope.
    Routing,

    /// The envelope could not be projected into an Iggy message.
    Mapping,

    /// The server rejected the request (permission, unknown stream/topic, or oversize).
    Rejected,

    /// The server refused admission before the write; safe to retry.
    ServerTransient,

    /// The client's session was already disconnected or shut down, so the request was not sent.
    ///
    /// The message can succeed on a rebuilt client, so this is transient, but retrying through
    /// the same client cannot succeed: reconnection is disabled. A supervisor that observes this
    /// kind, or [`IggyClient::is_connected`](crate::IggyClient::is_connected) returning `false`,
    /// must build a new [`IggyClient`](crate::IggyClient) and publisher.
    ClientDisconnected,

    /// The server's reply for this request was not observed, or the connection was lost after
    /// the request may have reached the server. The outcome is unknown: the message may already
    /// be committed. See the crate documentation for how a retry can observe this.
    OutcomeUnknown,
}

/// A redacted Iggy publication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IggyPublishError {
    kind: IggyPublishErrorKind,

    failure_kind: FailureKind,
}

impl IggyPublishError {
    pub(crate) fn new(kind: IggyPublishErrorKind, failure_kind: FailureKind) -> Self {
        Self { kind, failure_kind }
    }

    /// Returns the structured operation stage that failed.
    #[must_use]
    pub const fn kind(self) -> IggyPublishErrorKind {
        self.kind
    }
}

impl fmt::Display for IggyPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            IggyPublishErrorKind::Routing => "Iggy destination resolution failed",
            IggyPublishErrorKind::Mapping => "Iggy envelope mapping failed",
            IggyPublishErrorKind::Rejected => "Iggy server rejected the publish request",
            IggyPublishErrorKind::ServerTransient => {
                "Iggy server refused admission before the write"
            }
            IggyPublishErrorKind::ClientDisconnected => {
                "Iggy client session is closed; the request was not sent"
            }
            IggyPublishErrorKind::OutcomeUnknown => "Iggy publish outcome is unknown",
        })
    }
}

impl Error for IggyPublishError {}

impl ErrorClassifier for IggyPublishError {
    fn classify(&self) -> FailureKind {
        self.failure_kind
    }
}

/// Classifies an SDK error from `send_messages` into this crate's publish failure taxonomy. This
/// is the crate's single mapping from SDK error to [`IggyPublishErrorKind`]; the publish path
/// uses it directly rather than duplicating the match.
///
/// No SDK error maps to [`IggyPublishErrorKind::ClientDisconnected`]. With reconnection disabled,
/// the SDK reports a request it refused before writing (its own session was already closed) with
/// the same `Disconnected` error it uses for a request lost after writing, so an SDK error never
/// proves the request was unsent. The publisher instead reads the session state before sending
/// and reports a known-closed session as `ClientDisconnected` without calling the SDK; every
/// connection error the SDK does return stays [`IggyPublishErrorKind::OutcomeUnknown`].
///
/// `IggyError::RequestAlreadyApplied` falls through to the general `OutcomeUnknown` arm here, but
/// the publish path never actually constructs this variant from it: it intercepts
/// `RequestAlreadyApplied` beforehand and reports success, since the server's own
/// deduplication confirms the request already committed. No test currently exercises this path;
/// a deterministic fake-reply test is tracked in #63.
impl From<IggyError> for IggyPublishError {
    fn from(error: IggyError) -> Self {
        let (kind, failure_kind) = match error {
            IggyError::Unauthorized
            | IggyError::InvalidCredentials
            | IggyError::StreamIdNotFound(_)
            | IggyError::StreamNameNotFound(_)
            | IggyError::TopicIdNotFound(_, _)
            | IggyError::TopicNameNotFound(_, _)
            | IggyError::TooBigMessagePayload
            | IggyError::TooBigUserHeaders
            | IggyError::InvalidMessagePayloadLength
            | IggyError::EmptyMessagePayload
            | IggyError::InvalidHeaderValue
            | IggyError::InvalidHeaderKey => {
                (IggyPublishErrorKind::Rejected, FailureKind::Permanent)
            }

            // The server explicitly refused admission before accepting the write.
            IggyError::TransientNotAccepted => (
                IggyPublishErrorKind::ServerTransient,
                FailureKind::Transient,
            ),

            // `TransientNotCommitted` is returned only after the SDK's own same-session VSR
            // replay budget is exhausted, so by the time it reaches here the request's outcome is
            // already ambiguous. `Disconnected`, `Unauthenticated`, `NotConnected`, and `TcpError`
            // can each occur after the request was already written to the wire (a reply timeout
            // or a broken frame), and with reconnection disabled the SDK collapses several of
            // those conditions into `Disconnected`. None of these distinguish "never sent" from
            // "sent, outcome unknown", so all of them, plus a lost or shut-down client, are
            // reported the same way.
            IggyError::TransientNotCommitted
            | IggyError::Unauthenticated
            | IggyError::NotConnected
            | IggyError::Disconnected
            | IggyError::ClientShutdown
            | IggyError::CannotEstablishConnection
            | IggyError::TcpError
            | IggyError::StaleClient => {
                (IggyPublishErrorKind::OutcomeUnknown, FailureKind::Transient)
            }

            // An error this crate does not recognize by name is treated the same way: never
            // assumed rejected or pre-admission-refused, since either claim could be wrong. This
            // also covers `RequestAlreadyApplied`, which `publish` never routes here (see above).
            _ => (IggyPublishErrorKind::OutcomeUnknown, FailureKind::Transient),
        };

        Self::new(kind, failure_kind)
    }
}

/// Safe, structured class of an Iggy delivery-source or offset-advance failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum IggyDeliveryErrorKind {
    /// A source setting was outside its documented bound.
    InvalidSettings,

    /// The server rejected the session's credentials or permissions.
    Unauthorized,

    /// The stream, topic, consumer group, or partition does not exist.
    ///
    /// The source never creates any of them, and a group deleted while the source runs is
    /// reported the same way when the source next tries to rejoin it.
    NotFound,

    /// The server permanently rejected the request, such as an out-of-range offset.
    Rejected,

    /// The client session was lost or was never connected.
    ///
    /// Reconnection is application-owned: build a new [`IggyClient`](crate::IggyClient) and a
    /// new source. A deliberately shut-down client is reported as a clean source close instead.
    Disconnected,

    /// A server request did not complete within the source's request timeout.
    ///
    /// For an offset advance the outcome is unknown: the store may still be applied later.
    Timeout,

    /// The server refused or could not settle the request for a retryable reason, or returned
    /// an error this crate does not classify by name.
    ///
    /// For an offset advance the outcome is unknown: the store may still be applied later.
    Unavailable,

    /// A poll returned a record past the partition's next expected offset.
    ///
    /// The source never skips forward over a record it has not resolved. This reports records
    /// removed above the source's position before they were resolved, for example by segment
    /// deletion or retention, which requires operator action. A topic purge is not detected as a
    /// gap: it restarts offsets at 0, and the reused offsets are skipped instead (see the crate
    /// documentation's purge precondition).
    OffsetGap,
}

/// A redacted Iggy delivery-source or offset-advance failure.
///
/// It carries only its structured kind, its retry classification, and the numeric Iggy error
/// code when the server or SDK reported one: never SDK error text, resource names, credentials,
/// addresses, or payload bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IggyDeliveryError {
    kind: IggyDeliveryErrorKind,

    failure_kind: FailureKind,

    code: Option<u32>,
}

impl IggyDeliveryError {
    pub(crate) const fn new(kind: IggyDeliveryErrorKind, failure_kind: FailureKind) -> Self {
        Self {
            kind,
            failure_kind,
            code: None,
        }
    }

    pub(crate) const fn invalid_settings() -> Self {
        Self::new(
            IggyDeliveryErrorKind::InvalidSettings,
            FailureKind::Permanent,
        )
    }

    pub(crate) const fn timeout() -> Self {
        Self::new(IggyDeliveryErrorKind::Timeout, FailureKind::Transient)
    }

    /// Returns the structured failure class.
    #[must_use]
    pub const fn kind(self) -> IggyDeliveryErrorKind {
        self.kind
    }

    /// Returns the numeric Iggy error code, when the failure came from an SDK or server error.
    #[must_use]
    pub const fn code(self) -> Option<u32> {
        self.code
    }
}

impl fmt::Display for IggyDeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            IggyDeliveryErrorKind::InvalidSettings => "Iggy delivery source settings are invalid",
            IggyDeliveryErrorKind::Unauthorized => {
                "Iggy server rejected the session's credentials or permissions"
            }
            IggyDeliveryErrorKind::NotFound => {
                "Iggy stream, topic, consumer group, or partition not found"
            }
            IggyDeliveryErrorKind::Rejected => "Iggy server rejected the delivery request",
            IggyDeliveryErrorKind::Disconnected => "Iggy client session is not connected",
            IggyDeliveryErrorKind::Timeout => "Iggy delivery request did not complete in time",
            IggyDeliveryErrorKind::Unavailable => {
                "Iggy server could not complete the delivery request"
            }
            IggyDeliveryErrorKind::OffsetGap => {
                "Iggy poll skipped past the partition's next expected offset"
            }
        })
    }
}

impl Error for IggyDeliveryError {}

impl ErrorClassifier for IggyDeliveryError {
    fn classify(&self) -> FailureKind {
        self.failure_kind
    }
}

/// Classifies an SDK error into this crate's delivery failure taxonomy. This is the crate's single
/// mapping from SDK error to [`IggyDeliveryErrorKind`]; the source and settlement paths use it
/// after handling the outcomes that are not failures for them (an ownership fence on a poll, or
/// a deliberately shut-down client).
///
/// An ownership fence (`ConsumerGroupPartitionNotOwned`) or missing membership
/// (`ConsumerGroupMemberNotFound`) that reaches this mapping comes from an offset store, where it
/// does not prove the SDK's own replay of the same request was not applied earlier, so both are
/// [`IggyDeliveryErrorKind::Unavailable`] and transient rather than a conclusive ownership loss.
impl From<IggyError> for IggyDeliveryError {
    fn from(error: IggyError) -> Self {
        let (kind, failure_kind) = match &error {
            IggyError::Unauthorized
            | IggyError::InvalidCredentials
            | IggyError::InvalidUsername
            | IggyError::InvalidPassword => {
                (IggyDeliveryErrorKind::Unauthorized, FailureKind::Permanent)
            }

            IggyError::StreamIdNotFound(_)
            | IggyError::StreamNameNotFound(_)
            | IggyError::TopicIdNotFound(_, _)
            | IggyError::TopicNameNotFound(_, _)
            | IggyError::PartitionNotFound(_, _, _)
            | IggyError::NoPartitions(_, _)
            | IggyError::ConsumerGroupIdNotFound(_, _)
            | IggyError::ConsumerGroupNameNotFound(_, _)
            | IggyError::ResourceNotFound(_) => {
                (IggyDeliveryErrorKind::NotFound, FailureKind::Permanent)
            }

            IggyError::InvalidOffset(_)
            | IggyError::TooManyConsumerOffsets
            | IggyError::InvalidIdentifier
            | IggyError::InvalidConsumerGroupId
            | IggyError::InvalidConsumerGroupName
            | IggyError::FeatureUnavailable => {
                (IggyDeliveryErrorKind::Rejected, FailureKind::Permanent)
            }

            // With reconnection disabled a lost session does not recover; the application builds
            // a new client. `Unauthenticated` is how the SDK reports a session that is no longer
            // signed in, which after the initial login means the session was lost.
            IggyError::Disconnected
            | IggyError::NotConnected
            | IggyError::ClientShutdown
            | IggyError::CannotEstablishConnection
            | IggyError::TcpError
            | IggyError::StaleClient
            | IggyError::Unauthenticated => {
                (IggyDeliveryErrorKind::Disconnected, FailureKind::Transient)
            }

            // Retryable server refusals, ownership fences on an offset store, and every error
            // this crate does not recognize by name: never assumed permanent.
            _ => (IggyDeliveryErrorKind::Unavailable, FailureKind::Transient),
        };

        Self {
            kind,
            failure_kind,
            code: Some(error.as_code()),
        }
    }
}
