//! Broker-confirmed individual settlement.

use crate::{error::NatsError, telemetry};
use async_nats::jetstream::{self, AckKind};
use sisa_messaging::{IndividualSettlement, IndividualSettlementError};
use std::time::{Duration, Instant};

/// Consuming JetStream settlement handle.
pub struct NatsSettlement {
    pub(crate) message: jetstream::Message,
}

fn settlement_telemetry(
    result: &Result<(), IndividualSettlementError<NatsError>>,
) -> Result<(), NatsError> {
    match result {
        Ok(()) => Ok(()),
        Err(IndividualSettlementError::Operation(error)) => Err(*error),
        Err(IndividualSettlementError::Unsupported(_)) => Err(NatsError::Settings),
        Err(_) => Err(NatsError::Settlement),
    }
}

impl IndividualSettlement for NatsSettlement {
    type Error = NatsError;

    #[tracing::instrument(
        name = "heartbeat",
        target = "messaging.nats",
        level = "debug",
        skip_all
    )]
    async fn heartbeat(&mut self) -> Result<(), IndividualSettlementError<Self::Error>> {
        let started = Instant::now();

        let result = self
            .message
            .double_ack_with(AckKind::Progress)
            .await
            .map_err(|_| IndividualSettlementError::Operation(NatsError::Settlement));

        telemetry::finished(
            "heartbeat",
            started.elapsed(),
            settlement_telemetry(&result),
        );

        result
    }

    #[tracing::instrument(name = "ack", target = "messaging.nats", level = "debug", skip_all)]
    async fn ack(self) -> Result<(), IndividualSettlementError<Self::Error>> {
        let started = Instant::now();

        let result = self
            .message
            .double_ack()
            .await
            .map_err(|_| IndividualSettlementError::Operation(NatsError::Settlement));

        telemetry::finished("ack", started.elapsed(), settlement_telemetry(&result));

        result
    }

    #[tracing::instrument(name = "nack", target = "messaging.nats", level = "debug", skip_all)]
    async fn nak(self, delay: Duration) -> Result<(), IndividualSettlementError<Self::Error>> {
        let started = Instant::now();

        let result = self
            .message
            .double_ack_with(AckKind::Nak(Some(delay)))
            .await
            .map_err(|_| IndividualSettlementError::Operation(NatsError::Settlement));

        telemetry::finished("nack", started.elapsed(), settlement_telemetry(&result));

        result
    }

    #[tracing::instrument(
        name = "terminate",
        target = "messaging.nats",
        level = "debug",
        skip_all
    )]
    async fn terminate(self) -> Result<(), IndividualSettlementError<Self::Error>> {
        let started = Instant::now();

        let result = self
            .message
            .double_ack_with(AckKind::Term)
            .await
            .map_err(|_| IndividualSettlementError::Operation(NatsError::Settlement));

        telemetry::finished(
            "terminate",
            started.elapsed(),
            settlement_telemetry(&result),
        );

        result
    }
}
