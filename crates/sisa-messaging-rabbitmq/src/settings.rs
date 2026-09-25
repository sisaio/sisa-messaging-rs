//! Publisher and source settings.

use crate::error::RabbitMqError;
use std::{
    fmt,
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};

/// Publisher operation bounds.
#[derive(Clone, Copy, Debug)]
pub struct RabbitMqPublisherSettings {
    /// Maximum time for one publish including its broker confirm. Must be nonzero.
    pub publish_timeout: Duration,

    /// Largest payload in bytes the publisher sends. Set it to the broker's `max_message_size`;
    /// a larger payload fails permanently with [`RabbitMqError::PayloadTooLarge`] before sending.
    pub max_message_size: NonZeroU32,
}

impl RabbitMqPublisherSettings {
    pub(crate) fn validate(self) -> Result<Self, RabbitMqError> {
        if self.publish_timeout.is_zero() {
            return Err(RabbitMqError::Settings);
        }

        Ok(self)
    }
}

/// Source settings for one consumer on one dedicated channel.
///
/// `Debug` never renders the queue name.
#[derive(Clone)]
pub struct RabbitMqSourceSettings {
    /// Name of the application-declared queue to consume. It must be nonempty, at most 255 bytes,
    /// and free of ASCII control bytes.
    pub queue: String,

    /// Maximum number of unsettled deliveries the broker sends to this consumer.
    pub prefetch: NonZeroU16,
}

impl fmt::Debug for RabbitMqSourceSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RabbitMqSourceSettings")
            .field("queue", &"<redacted>")
            .field("prefetch", &self.prefetch)
            .finish()
    }
}
