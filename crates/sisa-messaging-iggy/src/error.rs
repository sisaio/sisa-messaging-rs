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
