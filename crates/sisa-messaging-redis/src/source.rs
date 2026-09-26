//! Bounded pending scan, claim, blocking read, and confirmed acknowledgement.

use crate::{RedisError, RedisWire, error::map_stream_command};
use redis::{
    aio::MultiplexedConnection,
    streams::{
        StreamClaimReply, StreamId, StreamInfoGroupsReply, StreamPendingCountReply, StreamReadReply,
    },
};
use sisa_messaging::{
    Delivery, IndividualCapability, IndividualDeliverySource, IndividualSettlement,
    IndividualSettlementError, IndividualSourceDescriptor, IndividualSourceOpenError,
    IndividualSourceRequirements,
};
use std::time::{Duration, Instant};

/// Finite read and reclaim budgets. Set these for the server's expected pending population.
#[derive(Clone, Copy, Debug)]
pub struct SourceSettings {
    /// Minimum idle duration before an entry may be claimed. At least one millisecond.
    pub min_idle: Duration,

    /// Time between pending scans. Must be non-zero.
    pub scan_cadence: Duration,

    /// Maximum entries inspected per XPENDING page, 1..=128.
    pub page_size: u16,

    /// Maximum XPENDING pages per receive, 1..=16.
    pub pages_per_receive: u8,

    /// Finite XREADGROUP block, 1..=1000 milliseconds.
    pub read_block: Duration,

    /// Finite command deadline, longer than the read block.
    pub command_timeout: Duration,
}

impl Default for SourceSettings {
    fn default() -> Self {
        Self {
            min_idle: Duration::from_secs(30),
            scan_cadence: Duration::from_secs(1),
            page_size: 32,
            pages_per_receive: 4,
            read_block: Duration::from_millis(100),
            command_timeout: Duration::from_secs(2),
        }
    }
}

impl SourceSettings {
    fn validate(self) -> Result<(), RedisError> {
        if self.min_idle.as_millis() == 0
            || self.min_idle.as_millis() > u64::MAX as u128
            || self.scan_cadence.is_zero()
            || self.page_size == 0
            || self.page_size > 128
            || self.pages_per_receive == 0
            || self.pages_per_receive > 16
            || self.read_block.as_millis() == 0
            || self.read_block.as_millis() > 1000
            || self.command_timeout <= self.read_block
        {
            return Err(RedisError::Settings);
        }

        Ok(())
    }
}

/// One claimed or newly read entry. Dropping it does not acknowledge it.
pub struct RedisDelivery {
    wire: RedisWire,

    settlement: RedisSettlement,
}

impl Delivery for RedisDelivery {
    type Wire = RedisWire;
    type Settlement = RedisSettlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.wire, self.settlement)
    }
}

/// Consuming XACK handle. One reply count confirms removal from the pending list.
pub struct RedisSettlement {
    connection: MultiplexedConnection,

    stream: String,

    group: String,

    id: String,

    timeout: Duration,
}

impl IndividualSettlement for RedisSettlement {
    type Error = RedisError;

    async fn heartbeat(&mut self) -> Result<(), IndividualSettlementError<Self::Error>> {
        Err(IndividualSettlementError::Unsupported(
            IndividualCapability::Heartbeat,
        ))
    }

    #[tracing::instrument(name = "ack", target = "messaging.redis", level = "debug", skip_all)]
    async fn ack(self) -> Result<(), IndividualSettlementError<Self::Error>> {
        let mut connection = self.connection;
        let mut command = redis::cmd("XACK");

        let query = command
            .arg(&self.stream)
            .arg(&self.group)
            .arg(&self.id)
            .query_async::<i64>(&mut connection);

        let count = tokio::time::timeout(self.timeout, query)
            .await
            .map_err(|_| IndividualSettlementError::Operation(RedisError::Timeout))?
            .map_err(|error| IndividualSettlementError::Operation(map_stream_command(error)))?;

        if count == 1 {
            Ok(())
        } else {
            Err(IndividualSettlementError::Operation(RedisError::Protocol))
        }
    }

    async fn nak(self, delay: Duration) -> Result<(), IndividualSettlementError<Self::Error>> {
        Err(IndividualSettlementError::Unsupported(nak_capability(
            delay,
        )))
    }

    async fn terminate(self) -> Result<(), IndividualSettlementError<Self::Error>> {
        Err(IndividualSettlementError::Unsupported(
            IndividualCapability::TerminalDiscard,
        ))
    }
}

fn nak_capability(delay: Duration) -> IndividualCapability {
    if delay.is_zero() {
        IndividualCapability::ImmediateRequeue
    } else {
        IndividualCapability::DelayedRetry
    }
}

/// Source using caller-supplied independent read and command connections.
///
/// The read connection can be occupied by finite `BLOCK`; the command connection performs
/// pending scans and settlement. Each concurrent source in one group needs its own consumer name.
pub struct RedisDeliverySource {
    read: MultiplexedConnection,

    commands: MultiplexedConnection,

    stream: String,

    group: String,

    consumer: String,

    settings: SourceSettings,

    scan_cursor: String,

    next_scan_at: Instant,

    opened: bool,

    closed: bool,
}

impl RedisDeliverySource {
    /// Constructs a source without network I/O or stream/group provisioning.
    pub fn new(
        read: MultiplexedConnection,
        commands: MultiplexedConnection,
        stream: String,
        group: String,
        consumer: String,
        settings: SourceSettings,
    ) -> Result<Self, RedisError> {
        settings.validate()?;

        if stream.is_empty() || group.is_empty() || consumer.is_empty() {
            return Err(RedisError::Settings);
        }

        Ok(Self {
            read,
            commands,
            stream,
            group,
            consumer,
            settings,
            scan_cursor: "-".to_owned(),
            next_scan_at: Instant::now(),
            opened: false,
            closed: false,
        })
    }

    /// Closes receiving locally. Pending entries remain for another live source to reclaim.
    pub fn close(&mut self) {
        self.closed = true;
    }

    fn delivery_from_entry(&self, entry: StreamId) -> Result<RedisDelivery, RedisError> {
        let version = entry.get::<Vec<u8>>("v");
        let envelope = entry.get::<Vec<u8>>("h");
        let payload = entry.get::<Vec<u8>>("p");
        let complete = version.is_some() && envelope.is_some() && payload.is_some();

        Ok(RedisDelivery {
            wire: RedisWire {
                version: version
                    .filter(|_| complete)
                    .unwrap_or_else(|| b"invalid".to_vec()),
                envelope: envelope.unwrap_or_default(),
                payload: payload.unwrap_or_default(),
            },
            settlement: RedisSettlement {
                connection: self.commands.clone(),
                stream: self.stream.clone(),
                group: self.group.clone(),
                id: entry.id,
                timeout: self.settings.command_timeout,
            },
        })
    }

    async fn reclaim(&mut self) -> Result<Option<RedisDelivery>, RedisError> {
        if Instant::now() < self.next_scan_at {
            return Ok(None);
        }

        let mut commands = self.commands.clone();

        for _ in 0..self.settings.pages_per_receive {
            let mut command = redis::cmd("XPENDING");

            let query = command
                .arg(&self.stream)
                .arg(&self.group)
                .arg(&self.scan_cursor)
                .arg("+")
                .arg(self.settings.page_size)
                .query_async::<StreamPendingCountReply>(&mut commands);

            let page = tokio::time::timeout(self.settings.command_timeout, query)
                .await
                .map_err(|_| RedisError::Timeout)?
                .map_err(map_stream_command)?;

            if page.ids.is_empty() {
                self.scan_cursor = "-".to_owned();
                self.next_scan_at = Instant::now() + self.settings.scan_cadence;

                return Ok(None);
            }

            let page_len = page.ids.len();

            for pending in page.ids {
                self.scan_cursor = format!("({}", pending.id);

                if (pending.last_delivered_ms as u128) < self.settings.min_idle.as_millis() {
                    continue;
                }

                let mut command = redis::cmd("XCLAIM");

                let query = command
                    .arg(&self.stream)
                    .arg(&self.group)
                    .arg(&self.consumer)
                    .arg(self.settings.min_idle.as_millis() as u64)
                    .arg(&pending.id)
                    .query_async::<StreamClaimReply>(&mut commands);

                let claimed = tokio::time::timeout(self.settings.command_timeout, query)
                    .await
                    .map_err(|_| RedisError::Timeout)?
                    .map_err(map_stream_command)?;

                if let Some(entry) = claimed.ids.into_iter().next() {
                    return self.delivery_from_entry(entry).map(Some);
                }
            }

            if page_len < usize::from(self.settings.page_size) {
                self.scan_cursor = "-".to_owned();
                self.next_scan_at = Instant::now() + self.settings.scan_cadence;

                return Ok(None);
            }
        }

        // Continue from this cursor on the next call, without scanning endlessly in one call.
        self.next_scan_at = Instant::now() + self.settings.scan_cadence;

        Ok(None)
    }

    async fn read_new(&mut self) -> Result<Option<RedisDelivery>, RedisError> {
        let mut command = redis::cmd("XREADGROUP");

        let query = command
            .arg("GROUP")
            .arg(&self.group)
            .arg(&self.consumer)
            .arg("COUNT")
            .arg(1)
            .arg("BLOCK")
            .arg(self.settings.read_block.as_millis() as u64)
            .arg("STREAMS")
            .arg(&self.stream)
            .arg(">")
            .query_async::<StreamReadReply>(&mut self.read);

        let reply = tokio::time::timeout(self.settings.command_timeout, query)
            .await
            .map_err(|_| RedisError::Timeout)?
            .map_err(map_stream_command)?;

        reply
            .keys
            .into_iter()
            .flat_map(|key| key.ids)
            .next()
            .map(|entry| self.delivery_from_entry(entry))
            .transpose()
    }
}

impl IndividualDeliverySource for RedisDeliverySource {
    type Delivery = RedisDelivery;
    type Error = RedisError;

    async fn open(
        &mut self,
        requirements: IndividualSourceRequirements,
    ) -> Result<IndividualSourceDescriptor, IndividualSourceOpenError<Self::Error>> {
        let descriptor = IndividualSourceDescriptor::new(None, None, false, false, false)
            .map_err(|_| IndividualSourceOpenError::Source(RedisError::Settings))?;

        descriptor
            .validate(requirements)
            .map_err(IndividualSourceOpenError::Unsupported)?;

        let mut commands = self.commands.clone();
        let mut command = redis::cmd("XINFO");

        let query = command
            .arg("GROUPS")
            .arg(&self.stream)
            .query_async::<StreamInfoGroupsReply>(&mut commands);

        let groups = tokio::time::timeout(self.settings.command_timeout, query)
            .await
            .map_err(|_| IndividualSourceOpenError::Source(RedisError::Timeout))?
            .map_err(|error| {
                let mapped = if error.kind() == redis::ErrorKind::ResponseError
                    && error
                        .detail()
                        .is_some_and(|detail| detail.eq_ignore_ascii_case("no such key"))
                {
                    RedisError::SourceClosed
                } else {
                    map_stream_command(error)
                };

                IndividualSourceOpenError::Source(mapped)
            })?;

        if !groups.groups.iter().any(|group| group.name == self.group) {
            return Err(IndividualSourceOpenError::Source(RedisError::SourceClosed));
        }

        self.opened = true;
        self.closed = false;

        Ok(descriptor)
    }

    #[tracing::instrument(
        name = "receive",
        target = "messaging.redis",
        level = "debug",
        skip_all
    )]
    async fn receive(&mut self) -> Result<Option<Self::Delivery>, Self::Error> {
        if self.closed {
            return Ok(None);
        }

        if !self.opened {
            return Err(RedisError::Settings);
        }

        loop {
            if let Some(delivery) = self.reclaim().await? {
                return Ok(Some(delivery));
            }

            if let Some(delivery) = self.read_new().await? {
                return Ok(Some(delivery));
            }
        }
    }
}

#[cfg(test)]
mod capability_tests {
    use super::{Duration, IndividualCapability, nak_capability};

    #[test]
    fn zero_delay_reports_immediate_requeue() {
        assert_eq!(
            nak_capability(Duration::ZERO),
            IndividualCapability::ImmediateRequeue
        );

        assert_eq!(
            nak_capability(Duration::from_nanos(1)),
            IndividualCapability::DelayedRetry
        );
    }
}
