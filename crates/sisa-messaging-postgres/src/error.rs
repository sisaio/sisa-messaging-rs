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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlxDisposition {
    Transient,
    Permanent,
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
        let Self::Database { .. } = self else {
            return FailureKind::Permanent;
        };

        if classify_sqlx_error(
            match self {
                Self::Database { source, .. } => source,
                _ => unreachable!(),
            },
            match self {
                Self::Database { sqlstate, .. } => sqlstate.as_deref(),
                _ => unreachable!(),
            },
        ) == SqlxDisposition::Transient
        {
            FailureKind::Transient
        } else {
            FailureKind::Permanent
        }
    }
}

fn classify_sqlx_error(error: &sqlx::Error, sqlstate: Option<&str>) -> SqlxDisposition {
    match error {
        sqlx::Error::Database(_) => {
            if sqlstate.is_some_and(is_transient_sqlstate) {
                SqlxDisposition::Transient
            } else {
                SqlxDisposition::Permanent
            }
        }
        sqlx::Error::PoolTimedOut
        | sqlx::Error::Io(_)
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::BeginFailed => SqlxDisposition::Transient,
        sqlx::Error::Configuration(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::PoolClosed
        | sqlx::Error::InvalidArgument(_)
        | sqlx::Error::RowNotFound
        | sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::ColumnIndexOutOfBounds { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Encode(_)
        | sqlx::Error::Decode(_)
        | sqlx::Error::AnyDriverError(_)
        | sqlx::Error::InvalidSavePointStatement => SqlxDisposition::Permanent,
        _ => SqlxDisposition::Permanent,
    }
}

fn is_transient_sqlstate(code: &str) -> bool {
    code.starts_with("08")
        || code.starts_with("53")
        || matches!(
            code,
            "40001" | "40P01" | "55P03" | "57014" | "57P01" | "57P02" | "57P03"
        )
}
