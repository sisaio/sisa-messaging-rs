use sisa_messaging::{ErrorClassifier, FailureKind};

/// Safe PostgreSQL provider error. SQL text, bind values, and connection details are never rendered.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PostgresError {
    /// Database failure classified from an optional, safe SQLSTATE code only.
    #[error("database operation failed")]
    Database {
        /// Optional SQLSTATE retained solely for retry classification; it excludes SQL and values.
        sqlstate: Option<String>,
    },

    /// A message identity already exists and the outbox cannot insert it again.
    #[error("duplicate outbox message identity")]
    DuplicateMessageId,

    /// Caller input, serialized or persisted provider data, or numeric, time, and contract
    /// conversions are invalid or unrepresentable for the provider contract.
    #[error("persisted provider data is invalid")]
    InvalidData,
}

impl From<sqlx::Error> for PostgresError {
    fn from(error: sqlx::Error) -> Self {
        let database = error.as_database_error();
        let code = database.as_ref().and_then(|value| value.code());
        if code.as_deref() == Some("23505")
            && database.as_ref().and_then(|value| value.constraint())
                == Some("ix_outbox_messages_message_id")
        {
            return Self::DuplicateMessageId;
        }
        Self::Database {
            sqlstate: code.map(|value| value.to_string()),
        }
    }
}

impl ErrorClassifier for PostgresError {
    fn classify(&self) -> FailureKind {
        let Self::Database { sqlstate } = self else {
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
