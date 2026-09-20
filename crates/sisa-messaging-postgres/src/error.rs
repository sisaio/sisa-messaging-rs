use sisa_messaging::{ErrorClassifier, FailureKind};
use std::fmt;

/// Safe PostgreSQL provider error.
///
/// Its outer [`Display`](fmt::Display) and [`Debug`](fmt::Debug) output is safe for ordinary
/// application logs: it never renders SQL, bind values, connection details, SQLSTATE, or a
/// database diagnostic. Callers that recursively format [`std::error::Error::source`] opt into
/// potentially sensitive, caller-controlled database diagnostics and must protect that output.
#[derive(thiserror::Error)]
#[non_exhaustive]
pub enum PostgresError {
    /// Database failure classified from a copied SQLSTATE while retaining its original source.
    #[error("database operation failed")]
    Database {
        /// Original driver failure; recursive diagnostics can include server-controlled detail.
        #[source]
        source: sqlx::Error,

        /// Optional SQLSTATE retained solely for retry classification; it excludes SQL and values.
        sqlstate: Option<String>,
    },

    /// A message identity already exists and the outbox cannot insert it again.
    #[error("duplicate outbox message identity")]
    DuplicateMessageId {
        /// Original unique-violation failure; recursive diagnostics can include server detail.
        #[source]
        source: sqlx::Error,
    },

    /// Caller input, serialized or persisted provider data, or numeric, time, and contract
    /// conversions are invalid or unrepresentable for the provider contract.
    #[error("persisted provider data is invalid")]
    InvalidData,
}

impl fmt::Debug for PostgresError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let category = match self {
            Self::Database { .. } => "Database",
            Self::DuplicateMessageId { .. } => "DuplicateMessageId",
            Self::InvalidData => "InvalidData",
        };
        formatter.write_str(category)
    }
}

impl From<sqlx::Error> for PostgresError {
    fn from(error: sqlx::Error) -> Self {
        let (sqlstate, constraint) = {
            let database = error.as_database_error();
            (
                database
                    .as_ref()
                    .and_then(|value| value.code())
                    .map(|value| value.to_string()),
                database
                    .as_ref()
                    .and_then(|value| value.constraint())
                    .map(str::to_owned),
            )
        };
        if sqlstate.as_deref() == Some("23505")
            && constraint.as_deref() == Some("ix_outbox_messages_message_id")
        {
            return Self::DuplicateMessageId { source: error };
        }
        Self::Database {
            source: error,
            sqlstate,
        }
    }
}

impl ErrorClassifier for PostgresError {
    fn classify(&self) -> FailureKind {
        let Self::Database { sqlstate, .. } = self else {
            return FailureKind::Permanent;
        };
        let Some(code) = sqlstate else {
            return FailureKind::Transient;
        };
        if code.starts_with("08")
            || code.starts_with("53")
            || matches!(
                code.as_ref(),
                "40001" | "40P01" | "55P03" | "57014" | "57P01" | "57P02" | "57P03"
            )
        {
            FailureKind::Transient
        } else {
            FailureKind::Permanent
        }
    }
}
