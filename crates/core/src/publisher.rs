//! The one trait that publishes, whatever the mode.

use std::error::Error;

use crate::Message;

/// Publishes messages: what domain code takes, so it doesn't care where a message goes.
///
/// The runtime implements it three times, and the code that owns the transaction picks one: the
/// bus publishes straight to the broker, the outbox publisher writes a row in the caller's
/// transaction, and a handler's context does what its endpoint is set up for.
pub trait Publisher {
    /// Why a publish failed.
    type Error: Error + Send + Sync + 'static;

    /// Publishes a message. When this returns `Ok`, the message is in the broker, or in the
    /// caller's transaction for the outbox.
    fn publish<Msg: Message>(
        &mut self,
        message: &Msg,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
