//! Bounded errors: never render Redis errors, URLs, keys, or wire values.

use sisa_messaging::{ErrorClassifier, FailureKind};
use std::fmt;

/// Redis provider failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RedisError {
    /// Invalid local settings or unopened source.
    Settings,

    /// Redis command failed or its outcome is unknown.
    Command,

    /// A command exceeded its finite deadline; its outcome is unknown.
    Timeout,

    /// A broker reply lacked the expected entry or confirmation.
    Protocol,

    /// Mapping the outbound envelope failed.
    Mapping,

    /// The source was closed by a missing stream or group.
    SourceClosed,

    /// The server does not support a required Streams command.
    Unsupported,
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Redis Streams operation failed")
    }
}

impl std::error::Error for RedisError {}

impl ErrorClassifier for RedisError {
    fn classify(&self) -> FailureKind {
        match self {
            Self::Settings
            | Self::Mapping
            | Self::Protocol
            | Self::SourceClosed
            | Self::Unsupported => FailureKind::Permanent,
            Self::Command | Self::Timeout => FailureKind::Transient,
        }
    }
}

pub(crate) fn map_stream_command(error: redis::RedisError) -> RedisError {
    if error.code() == Some("NOGROUP") {
        return RedisError::SourceClosed;
    }

    if error.kind() == redis::ErrorKind::ResponseError
        && error.detail().is_some_and(|detail| {
            ["unknown command", "unknown subcommand"]
                .iter()
                .any(|prefix| {
                    detail
                        .get(..prefix.len())
                        .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
                })
        })
    {
        return RedisError::Unsupported;
    }

    RedisError::Command
}

/// Invalid or missing envelope wire field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisMappingError;

impl fmt::Display for RedisMappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Redis Streams envelope mapping failed")
    }
}

impl std::error::Error for RedisMappingError {}

impl ErrorClassifier for RedisMappingError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}
