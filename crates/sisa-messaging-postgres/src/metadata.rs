use sisa_messaging::Metadata;

use crate::PostgresError;

pub(crate) fn encode(value: &Metadata) -> Result<serde_json::Value, PostgresError> {
    serde_json::to_value(value).map_err(|_| PostgresError::InvalidData)
}

pub(crate) fn decode(value: serde_json::Value) -> Result<Metadata, PostgresError> {
    serde_json::from_value(value).map_err(|_| PostgresError::InvalidData)
}
