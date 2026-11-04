//! What the PostgreSQL persistence is configured with.

use serde::{Deserialize, Serialize};

/// The settings of the PostgreSQL outbox and inbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct PostgresSettings {
    /// The schema that holds the tables, `sisa_messaging` by default. It keeps them apart from the
    /// application's own tables and migrations. Any name works: the SQL quotes it.
    pub schema: String,
}

// The schema the documented SQL uses.
impl Default for PostgresSettings {
    fn default() -> Self {
        Self {
            schema: "sisa_messaging".into(),
        }
    }
}
