//! Broker-confirmed individual settlement.

use crate::{
    error::RabbitMqError,
    telemetry::{self, Failure},
};
use lapin::{
    Acker, Channel,
    options::{BasicAckOptions, BasicQosOptions, BasicRejectOptions},
};
use sisa_messaging::{IndividualCapability, IndividualSettlement, IndividualSettlementError};
use std::time::{Duration, Instant};

type SettlementResult = Result<(), IndividualSettlementError<RabbitMqError>>;

/// Consuming settlement handle for one delivery.
///
/// AMQP 0-9-1 has no reply to `basic.ack` or `basic.reject`. After sending one, the handle sends
/// a synchronous `basic.qos` with the source's unchanged per-consumer prefetch on the same
/// channel. The broker processes a channel's frames in order, so its `basic.qos-ok` proves it
/// processed the settlement frame first; only then does the operation succeed. Each settlement
/// therefore costs one extra broker round trip, and a failed or unanswered `basic.qos` leaves the
/// settlement indeterminate.
///
/// The handle keeps its own clone of the source channel, so settlement remains possible after the
/// source is closed or dropped. A handle whose channel is no longer open fails locally with
/// [`RabbitMqError::Settlement`] without I/O, because delivery tags are scoped to the channel
/// that received them. The connection must not enable lapin automatic recovery: a recovered
/// channel reuses delivery tags that no longer identify this delivery. The provider cannot verify
/// this precondition.
///
/// Operations are not bounded internally; callers bound each one with their settlement timeout.
/// An error, timeout, or dropped operation leaves the broker outcome unknown.
pub struct RabbitMqSettlement {
    channel: Channel,

    acker: Acker,

    prefetch: u16,
}

impl RabbitMqSettlement {
    pub(crate) fn new(channel: Channel, acker: Acker, prefetch: u16) -> Self {
        Self {
            channel,
            acker,
            prefetch,
        }
    }

    fn fenced(&self) -> Result<(), RabbitMqError> {
        if !self.channel.status().connected() || !self.acker.usable() {
            return Err(RabbitMqError::Settlement);
        }

        Ok(())
    }

    async fn confirm_processed(&self) -> Result<(), RabbitMqError> {
        self.channel
            .basic_qos(self.prefetch, BasicQosOptions { global: false })
            .await
            .map_err(|_| RabbitMqError::Settlement)
    }

    async fn ack_inner(&self) -> Result<(), RabbitMqError> {
        self.fenced()?;

        let sent = self
            .acker
            .ack(BasicAckOptions { multiple: false })
            .await
            .map_err(|_| RabbitMqError::Settlement)?;

        if !sent {
            return Err(RabbitMqError::Settlement);
        }

        self.confirm_processed().await
    }

    async fn reject_inner(&self, requeue: bool) -> Result<(), RabbitMqError> {
        self.fenced()?;

        let sent = self
            .acker
            .reject(BasicRejectOptions { requeue })
            .await
            .map_err(|_| RabbitMqError::Settlement)?;

        if !sent {
            return Err(RabbitMqError::Settlement);
        }

        self.confirm_processed().await
    }
}

fn record(operation: &'static str, started: Instant, result: &SettlementResult) {
    let outcome = match result {
        Ok(()) => Ok(()),
        Err(IndividualSettlementError::Operation(error)) => Err(Failure::Provider(*error)),
        Err(IndividualSettlementError::Unsupported(_)) => Err(Failure::Unsupported),
        Err(_) => Err(Failure::Provider(RabbitMqError::Settlement)),
    };

    telemetry::finished_with(operation, started.elapsed(), outcome);
}

impl IndividualSettlement for RabbitMqSettlement {
    type Error = RabbitMqError;

    /// Always unsupported: AMQP 0-9-1 has no delivery deadline to extend. No broker action is
    /// taken.
    #[tracing::instrument(
        name = "heartbeat",
        target = "messaging.rabbitmq",
        level = "debug",
        skip_all
    )]
    async fn heartbeat(&mut self) -> SettlementResult {
        let started = Instant::now();

        let result = Err(IndividualSettlementError::Unsupported(
            IndividualCapability::Heartbeat,
        ));

        record("heartbeat", started, &result);

        result
    }

    /// Sends `basic.ack` and succeeds once the broker has processed it.
    #[tracing::instrument(name = "ack", target = "messaging.rabbitmq", level = "debug", skip_all)]
    async fn ack(self) -> SettlementResult {
        let started = Instant::now();

        let result = self
            .ack_inner()
            .await
            .map_err(IndividualSettlementError::Operation);

        record("ack", started, &result);

        result
    }

    /// A zero delay sends `basic.reject` with requeue and succeeds once the broker has processed
    /// it; the delivery returns to its queue for immediate redelivery. A nonzero delay is
    /// unsupported and takes no broker action.
    #[tracing::instrument(
        name = "nack",
        target = "messaging.rabbitmq",
        level = "debug",
        skip_all
    )]
    async fn nak(self, delay: Duration) -> SettlementResult {
        let started = Instant::now();

        let result = if delay.is_zero() {
            self.reject_inner(true)
                .await
                .map_err(IndividualSettlementError::Operation)
        } else {
            Err(IndividualSettlementError::Unsupported(
                IndividualCapability::DelayedRetry,
            ))
        };

        record("nack", started, &result);

        result
    }

    /// Sends `basic.reject` without requeue and succeeds once the broker has processed it. The
    /// broker dead-letters the delivery when the queue has a dead-letter exchange and otherwise
    /// discards it.
    #[tracing::instrument(
        name = "terminate",
        target = "messaging.rabbitmq",
        level = "debug",
        skip_all
    )]
    async fn terminate(self) -> SettlementResult {
        let started = Instant::now();

        let result = self
            .reject_inner(false)
            .await
            .map_err(IndividualSettlementError::Operation);

        record("terminate", started, &result);

        result
    }
}
