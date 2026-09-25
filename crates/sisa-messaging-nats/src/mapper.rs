//! Deterministic transport projection with redacted errors.

use crate::error::MappingError;
use async_nats::HeaderMap;
use sisa_messaging::{EnvelopeMapper, SerializedEnvelope};
use std::fmt;

mod constants;
mod decode;
mod encode;
mod headers;

/// A concrete, wildcard-free NATS subject.
#[derive(Clone, Eq, PartialEq)]
pub struct Subject(String);

impl Subject {
    /// Validates and owns a concrete publish subject.
    ///
    /// Subjects are at most 1024 bytes and consist of nonempty dot-separated
    /// ASCII tokens containing only letters, digits, `_`, or `-`. Wildcards,
    /// whitespace, and empty tokens are rejected.
    pub fn new(value: impl Into<String>) -> Result<Self, MappingError> {
        let value = value.into();

        if value.is_empty()
            || value.len() > 1024
            || value.split('.').any(|token| {
                token.is_empty()
                    || !token
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            })
        {
            return Err(MappingError::InvalidSubject);
        }

        Ok(Self(value))
    }

    /// Borrows the validated subject.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Debug for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Subject(<redacted>)")
    }
}

/// Resolves a concrete outbound subject.
pub trait SubjectResolver: Send + Sync {
    /// Computes a subject from validated logical fields.
    fn resolve(&self, envelope: &SerializedEnvelope) -> Result<Subject, MappingError>;
}

/// Resolves `{prefix}.{message_type}.v{version}`.
#[derive(Clone, Debug)]
pub struct TypeSubjectResolver {
    prefix: Subject,
}

impl TypeSubjectResolver {
    /// Constructs a resolver without network I/O.
    pub fn new(prefix: Subject) -> Self {
        Self { prefix }
    }
}

impl SubjectResolver for TypeSubjectResolver {
    fn resolve(&self, envelope: &SerializedEnvelope) -> Result<Subject, MappingError> {
        Subject::new(format!(
            "{}.{}.v{}",
            self.prefix.as_str(),
            envelope.message_type.as_str(),
            envelope.message_version
        ))
    }
}

/// Owned wire projection.
#[derive(Clone)]
pub struct NatsWire {
    /// Outbound destination or broker-delivered subject.
    pub subject: String,

    /// Deterministically projected metadata.
    pub headers: HeaderMap,

    /// Serialized body.
    pub payload: Vec<u8>,
}

/// Pure mapper using `Sisa-*` framework headers and `Sisa-Custom-*` custom headers.
pub struct NatsMapper<R> {
    resolver: R,
}

impl<R> NatsMapper<R> {
    /// Constructs a mapper without network I/O.
    pub fn new(resolver: R) -> Self {
        Self { resolver }
    }
}

impl<R: SubjectResolver> EnvelopeMapper<NatsWire> for NatsMapper<R> {
    type Error = MappingError;

    fn encode(&self, envelope: &SerializedEnvelope) -> Result<NatsWire, Self::Error> {
        encode::encode(&self.resolver, envelope)
    }

    fn decode(&self, wire: NatsWire) -> Result<SerializedEnvelope, Self::Error> {
        decode::decode(wire)
    }
}
