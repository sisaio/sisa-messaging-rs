//! Validated Iggy delivery-source settings.

use std::time::Duration;

use iggy::prelude::Identifier;

use crate::IggyDeliveryError;

/// Largest accepted per-poll batch length.
const MAX_BATCH_LENGTH: u32 = 1_024;

/// Largest accepted poll and assignment refresh interval; it keeps every deadline the source
/// computes from them representable.
const MAX_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Consumer-group, polling, and request-bound settings for an
/// [`IggyDeliverySource`](crate::IggyDeliverySource).
///
/// Every value is validated when it is set, so a constructed value is always usable. The stream,
/// topic, and consumer group are application-provisioned; the source looks them up and joins the
/// group, and never creates any of them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IggySourceSettings {
    pub(crate) stream: Identifier,

    pub(crate) topic: Identifier,

    pub(crate) group: Identifier,

    pub(crate) batch_length: u32,

    pub(crate) poll_interval: Duration,

    pub(crate) assignment_refresh_interval: Duration,

    pub(crate) request_timeout: Duration,
}

impl IggySourceSettings {
    /// Creates settings for a pre-provisioned consumer group on a stream's topic.
    ///
    /// Defaults: a batch length of 64 records, a 100 ms poll interval, a 1 s assignment refresh
    /// interval, and a 5 s request timeout.
    #[must_use]
    pub fn new(stream: Identifier, topic: Identifier, group: Identifier) -> Self {
        Self {
            stream,
            topic,
            group,
            batch_length: 64,
            poll_interval: Duration::from_millis(100),
            assignment_refresh_interval: Duration::from_secs(1),
            request_timeout: Duration::from_secs(5),
        }
    }

    /// Sets the most records one poll requests from a partition, from 1 through 1,024.
    ///
    /// It also bounds the records buffered per partition: a partition is polled again only after
    /// its buffered records are delivered and resolved.
    pub fn with_batch_length(mut self, batch_length: u32) -> Result<Self, IggyDeliveryError> {
        if batch_length == 0 || batch_length > MAX_BATCH_LENGTH {
            return Err(IggyDeliveryError::invalid_settings());
        }

        self.batch_length = batch_length;

        Ok(self)
    }

    /// Sets how long an owned partition waits before it is polled again after an empty poll, and
    /// before a withdrawn record is replayed. It must be greater than zero and at most one hour.
    pub fn with_poll_interval(
        mut self,
        poll_interval: Duration,
    ) -> Result<Self, IggyDeliveryError> {
        if poll_interval.is_zero() || poll_interval > MAX_INTERVAL {
            return Err(IggyDeliveryError::invalid_settings());
        }

        self.poll_interval = poll_interval;

        Ok(self)
    }

    /// Sets how often the source rechecks its group membership and the topic's partitions and
    /// probes the partitions it does not own. It must be greater than zero and at most one hour.
    pub fn with_assignment_refresh_interval(
        mut self,
        assignment_refresh_interval: Duration,
    ) -> Result<Self, IggyDeliveryError> {
        if assignment_refresh_interval.is_zero() || assignment_refresh_interval > MAX_INTERVAL {
            return Err(IggyDeliveryError::invalid_settings());
        }

        self.assignment_refresh_interval = assignment_refresh_interval;

        Ok(self)
    }

    /// Sets the bound for every server request the source and its settlements make; the SDK has
    /// no call timeout of its own. It must not be zero.
    pub fn with_request_timeout(
        mut self,
        request_timeout: Duration,
    ) -> Result<Self, IggyDeliveryError> {
        if request_timeout.is_zero() {
            return Err(IggyDeliveryError::invalid_settings());
        }

        self.request_timeout = request_timeout;

        Ok(self)
    }
}
