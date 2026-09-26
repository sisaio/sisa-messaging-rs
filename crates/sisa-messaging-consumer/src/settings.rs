//! Consumer policy and one-time validation.

use std::num::NonZeroUsize;
use std::time::Duration;

use sisa_messaging::IndividualSourceRequirements;

use crate::{ConsumerConfigError, SettingsField};

/// How an individual-delivery consumer settles deliveries that did not complete.
///
/// The mode is always selected by the application and is never inferred from a source
/// descriptor. Both modes acknowledge only after a committed success or a durable completion
/// observed through the inbox.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SettlementMode {
    /// Requests broker redelivery with a delayed negative acknowledgement and terminally discards
    /// poison or dead deliveries.
    ///
    /// Opening requires delayed retry and terminal discard from the source.
    #[default]
    Broker,

    /// Leaves unresolved deliveries unsettled for the source's bounded pending recovery.
    ///
    /// Opening requires neither delayed retry nor terminal discard, and the consumer never
    /// negatively acknowledges or terminates a delivery. Malformed input and durable dead results
    /// stop the consumer for operator action. Select this mode only for a source that redelivers
    /// unsettled deliveries through bounded recovery, such as Redis Streams idle reclaim; the
    /// runtime cannot detect a source that would instead lose or never redeliver them.
    PendingRecovery,
}

impl SettlementMode {
    /// Returns the source requirements every reachable settlement path of this mode needs.
    pub(crate) const fn requirements(self) -> IndividualSourceRequirements {
        match self {
            Self::Broker => IndividualSourceRequirements::new()
                .requiring_delayed_retry()
                .requiring_terminal_discard(),
            Self::PendingRecovery => IndividualSourceRequirements::new(),
        }
    }
}

/// Complete consumer policy supplied by the application.
///
/// Construct it from [`Default`] and assign the fields to change; every value is validated once
/// by [`Consumer::new`](crate::Consumer::new).
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConsumerSettings {
    /// Maximum received-but-unsettled deliveries, which also bounds open transactions.
    pub max_in_flight: NonZeroUsize,

    /// Bound for the whole source opening operation. Receive waits are cancel-safe and untimed.
    pub source_timeout: Duration,

    /// Bound for each framework-owned begin, inbox, commit, rollback, and failure-record
    /// operation. It never times out handler code.
    pub database_timeout: Duration,

    /// Bound for each acknowledgement, negative acknowledgement, and termination.
    pub settlement_timeout: Duration,

    /// Broker redelivery delay for retryable or in-progress work in [`SettlementMode::Broker`].
    pub nak_delay: Duration,

    /// Bound for the whole graceful drain after receiving stops for any cause.
    pub drain_timeout: Duration,

    /// Explicit settlement mode; never inferred from the source.
    pub mode: SettlementMode,
}

impl Default for ConsumerSettings {
    fn default() -> Self {
        Self {
            max_in_flight: NonZeroUsize::new(32).unwrap_or(NonZeroUsize::MIN),
            source_timeout: Duration::from_secs(10),
            database_timeout: Duration::from_secs(5),
            settlement_timeout: Duration::from_secs(10),
            nak_delay: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(20),
            mode: SettlementMode::Broker,
        }
    }
}

impl ConsumerSettings {
    pub(crate) fn validate(&self) -> Result<(), ConsumerConfigError> {
        non_zero(SettingsField::SourceTimeout, self.source_timeout)?;
        non_zero(SettingsField::DatabaseTimeout, self.database_timeout)?;
        non_zero(SettingsField::SettlementTimeout, self.settlement_timeout)?;
        non_zero(SettingsField::DrainTimeout, self.drain_timeout)?;

        match self.mode {
            // Zero-delay retry is a separate, explicitly selected policy (#11).
            SettlementMode::Broker => non_zero(SettingsField::NakDelay, self.nak_delay),
            SettlementMode::PendingRecovery => Ok(()),
        }
    }
}

fn non_zero(field: SettingsField, duration: Duration) -> Result<(), ConsumerConfigError> {
    if duration.is_zero() {
        return Err(ConsumerConfigError::ZeroDuration(field));
    }

    Ok(())
}
