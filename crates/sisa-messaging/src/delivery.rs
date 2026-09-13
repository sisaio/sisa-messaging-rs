//! Transport-neutral inbound delivery and settlement capabilities.

use std::error::Error;
use std::future::Future;
use std::num::NonZeroU64;
use std::time::Duration;

use crate::Classify;

/// A broker settlement handle.
///
/// Terminal actions consume the handle, preventing a second terminal settlement in safe Rust.
pub trait Settlement: Send + 'static {
    /// Settlement error with an explicit retry decision.
    type Error: Error + Send + Sync + 'static + Classify;

    /// Sends an active heartbeat for work-in-progress work without terminaling.
    fn heartbeat(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Confirms successful processing.
    fn ack(self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Requests redelivery after the supplied delay.
    fn nak(self, delay: Duration) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Terminates redelivery for a poison or permanently failed delivery.
    fn terminate(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// One owned transport delivery that can be split exactly once.
pub trait Delivery: Send + 'static {
    /// Owned transport wire representation consumed by an envelope mapper.
    type Wire: Send + 'static;
    /// Settlement handle retained independently from the wire value.
    type Settlement: Settlement;

    /// Splits wire data from its settlement handle.
    fn into_parts(self) -> (Self::Wire, Self::Settlement);
}

/// An inbound delivery stream with explicit startup and source-close semantics.
pub trait DeliverySource: Send {
    /// Delivery yielded by the source.
    type Delivery: Delivery;
    /// Fatal source error with an explicit retry decision.
    type Error: Error + Send + Sync + 'static + Classify;

    /// Performs one-time source initialization.
    fn open(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Waits cancel-safely for the next delivery; `None` means a clean source close.
    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<Option<Self::Delivery>, Self::Error>> + Send;

    /// Returns the configured acknowledgement deadline, when one exists.
    fn ack_wait(&self) -> Option<Duration>;

    /// Returns the finite configured delivery bound, or `None` when unknown/unlimited.
    fn max_deliver(&self) -> Option<NonZeroU64>;
}
