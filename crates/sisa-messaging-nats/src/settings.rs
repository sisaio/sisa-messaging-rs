//! Publisher operation bounds.

use crate::error::NatsError;
use std::time::Duration;

/// Publisher operation bounds. The duration must be nonzero.
#[derive(Clone, Copy, Debug)]
pub struct NatsPublisherSettings {
    /// Maximum time for one publish including its JetStream acknowledgement.
    pub publish_timeout: Duration,
}

impl NatsPublisherSettings {
    /// Validates operation time bounds.
    pub fn validate(self) -> Result<Self, NatsError> {
        if self.publish_timeout.is_zero() {
            return Err(NatsError::Settings);
        }
        Ok(self)
    }
}
