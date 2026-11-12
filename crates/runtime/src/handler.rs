//! Handlers, their context and their errors.

use std::any::type_name;
use std::error::Error;
use std::fmt;
use std::ops::DerefMut;
use std::sync::Arc;

use sisa_messaging_core::{Correlation, Envelope, JsonCodec, Message, Publisher, Source};

use crate::publish::{PublishError, encode};

/// Why a handler failed: an error's type and message, which go to the error destination as the
/// `errortype` and `errormessage` extensions once the endpoint gives up.
///
/// Any error converts into it, so a handler uses `?` on its own errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerError {
    error_type: String,
    message: String,
}

impl HandlerError {
    /// An error with a type and a message of your own.
    #[must_use]
    pub fn new(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
        }
    }

    /// What kind of error this is: the Rust type name of a converted error, such as
    /// `std::io::error::Error`.
    #[must_use]
    pub fn error_type(&self) -> &str {
        &self.error_type
    }

    /// What went wrong: the converted error's `Display`.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

// Takes any error, by its type name and message. `HandlerError` itself isn't an `Error`, so this
// doesn't overlap the standard `From<T> for T`.
impl<Err: Error> From<Err> for HandlerError {
    fn from(error: Err) -> Self {
        Self::new(type_name::<Err>(), error.to_string())
    }
}

// Writes the message, as the error destination gets it.
impl fmt::Display for HandlerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

/// What a handler gets with its message: the delivered envelope, a [`Publisher`] for the
/// messages it sends in turn, and on an endpoint with an inbox, the delivery's transaction.
///
/// Its publishes have the handled message as their cause, carry its correlation id and trace
/// context over, and are held until the handler returns `Ok`. The endpoint drops them when the
/// handler fails. Otherwise, on an endpoint with an inbox and a bus with an outbox, it writes them
/// to the outbox in the handler's transaction; in every other case it sends them after any commit
/// and before it acks the delivery.
///
/// `Transaction` is the inbox store's transaction, such as `sqlx::PgTransaction<'static>`, and
/// `()` on an endpoint without an inbox. `Codec` is the bus's [codec](crate::BusBuilder::codec),
/// which encodes the publishes.
pub struct HandlerContext<Transaction = (), Codec = JsonCodec> {
    source: Source,
    pub(crate) codec: Arc<Codec>,
    endpoint: String,
    envelope: Envelope,
    attempt: u32,
    pub(crate) held: Vec<Envelope>,
    pub(crate) transaction: Transaction,
}

impl<Codec> HandlerContext<(), Codec> {
    pub(crate) fn new(
        source: Source,
        codec: Arc<Codec>,
        endpoint: String,
        envelope: Envelope,
        attempt: u32,
    ) -> Self {
        Self {
            source,
            codec,
            endpoint,
            envelope,
            attempt,
            held: Vec::new(),
            transaction: (),
        }
    }
}

impl<Transaction, Codec> HandlerContext<Transaction, Codec> {
    // The same context with another transaction, and the transaction it had.
    pub(crate) fn with_transaction<Other>(
        self,
        transaction: Other,
    ) -> (HandlerContext<Other, Codec>, Transaction) {
        let context = HandlerContext {
            source: self.source,
            codec: self.codec,
            endpoint: self.endpoint,
            envelope: self.envelope,
            attempt: self.attempt,
            held: self.held,
            transaction,
        };
        (context, self.transaction)
    }

    /// The delivered envelope, the handled message's id, correlation and extensions included.
    #[must_use]
    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Which delivery of this message to this endpoint this is: 1 the first time.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The name of the endpoint handling the message.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl<Transaction: DerefMut, Codec> HandlerContext<Transaction, Codec> {
    /// The delivery's transaction, on an endpoint with an inbox, such as a `&mut PgConnection`
    /// for sqlx queries. The handler's database work goes here, so it commits with the inbox
    /// receipt, and rolls back with it when the handler fails.
    pub fn transaction(&mut self) -> &mut Transaction::Target {
        &mut self.transaction
    }
}

// Holds each publish, tied to the handled message, until the handler returns `Ok`.
impl<Transaction: Send, Codec: sisa_messaging_core::Codec> Publisher
    for HandlerContext<Transaction, Codec>
{
    type Error = PublishError;

    async fn publish<Msg: Message>(&mut self, message: &Msg) -> Result<(), PublishError> {
        let mut envelope = encode(&self.source, &*self.codec, message)?;
        envelope.correlation = Correlation::new(
            self.envelope.correlation.correlation_id,
            Some(self.envelope.id),
        );
        envelope.trace.clone_from(&self.envelope.trace);
        self.held.push(envelope);
        Ok(())
    }
}

// Shows which delivery this is and how many publishes it holds, not the data.
impl<Transaction, Codec> fmt::Debug for HandlerContext<Transaction, Codec> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandlerContext")
            .field("endpoint", &self.endpoint)
            .field("id", &self.envelope.id)
            .field("message_type", &self.envelope.message_type)
            .field("attempt", &self.attempt)
            .field("held", &self.held.len())
            .finish_non_exhaustive()
    }
}

/// Handles one message type on an endpoint.
///
/// An `async fn` taking the message and `&mut HandlerContext` is a handler, and so is a type
/// that implements this trait. On an endpoint with an inbox, a handler takes
/// `&mut HandlerContext<Transaction>`, with the inbox store's transaction, to work in it. On a bus
/// with another [codec](crate::BusBuilder::codec), it takes `&mut HandlerContext<Transaction,
/// Codec>`.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use sisa_messaging_core::Message;
/// use sisa_messaging_runtime::{HandlerContext, HandlerError};
///
/// #[derive(Serialize, Deserialize)]
/// struct OrderPlaced {
///     id: u64,
/// }
///
/// impl Message for OrderPlaced {
///     const TYPE: &'static str = "orders.order_placed.v1";
/// }
///
/// async fn charge_order(
///     order: OrderPlaced,
///     context: &mut HandlerContext,
/// ) -> Result<(), HandlerError> {
///     println!("charging order {} from {}", order.id, context.envelope().source);
///     Ok(())
/// }
/// ```
pub trait Handler<Msg, Transaction = (), Codec = JsonCodec>: Send + Sync + 'static {
    /// Handles a message. An error makes the endpoint retry the delivery, as its
    /// [`RetryPolicy`](crate::RetryPolicy) says.
    fn handle(
        &self,
        message: Msg,
        context: &mut HandlerContext<Transaction, Codec>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;
}

/// A function that is a [`Handler`] for every lifetime of its context, which Rust can't say in
/// one bound on stable. Implemented for every such function; not for use on its own.
#[doc(hidden)]
pub trait HandlerFn<'ctx, Msg, Transaction: 'ctx, Codec: 'ctx>: Send + Sync + 'static {
    type Future: Future<Output = Result<(), HandlerError>> + Send + 'ctx;

    fn call(
        &self,
        message: Msg,
        context: &'ctx mut HandlerContext<Transaction, Codec>,
    ) -> Self::Future;
}

// Names the future of a handler function, whose type depends on the context's lifetime.
impl<'ctx, Msg, Transaction: 'ctx, Codec: 'ctx, Function, Fut>
    HandlerFn<'ctx, Msg, Transaction, Codec> for Function
where
    Function: Fn(Msg, &'ctx mut HandlerContext<Transaction, Codec>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send + 'ctx,
{
    type Future = Fut;

    fn call(&self, message: Msg, context: &'ctx mut HandlerContext<Transaction, Codec>) -> Fut {
        self(message, context)
    }
}

// Lets an `async fn(Msg, &mut HandlerContext<Transaction, Codec>)` be a handler.
impl<Msg, Transaction: 'static, Codec: 'static, Function> Handler<Msg, Transaction, Codec>
    for Function
where
    Function: for<'ctx> HandlerFn<'ctx, Msg, Transaction, Codec>,
{
    fn handle(
        &self,
        message: Msg,
        context: &mut HandlerContext<Transaction, Codec>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        self.call(message, context)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};
    use sisa_messaging_core::{ContentType, MessageId, MessageType, TraceContext};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct InvoiceSent {
        order: u64,
    }

    impl Message for InvoiceSent {
        const TYPE: &'static str = "billing.invoice_sent.v1";
    }

    fn context(correlation_id: Option<MessageId>) -> HandlerContext {
        let mut envelope = Envelope::new(
            MessageType::new("orders.order_placed.v1").unwrap(),
            Source::new("/orders").unwrap(),
            ContentType::new("application/json").unwrap(),
            Bytes::from_static(b"{}"),
        );
        envelope.correlation = Correlation::new(correlation_id, None);
        envelope.trace = Some(TraceContext::new(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            None,
        ));
        HandlerContext::new(
            Source::new("/billing").unwrap(),
            Arc::new(JsonCodec),
            "billing".into(),
            envelope,
            1,
        )
    }

    #[tokio::test]
    async fn ties_its_publishes_to_the_handled_message() {
        let correlation_id = MessageId::new();
        let mut context = context(Some(correlation_id));

        context.publish(&InvoiceSent { order: 42 }).await.unwrap();

        let [held] = context.held.as_slice() else {
            panic!("expected one held publish, got {:?}", context.held);
        };
        assert_eq!(held.message_type.as_str(), InvoiceSent::TYPE);
        assert_eq!(held.source.as_str(), "/billing");
        assert_eq!(held.correlation.correlation_id, Some(correlation_id));
        assert_eq!(held.correlation.causation_id, Some(context.envelope.id));
        assert_eq!(held.trace, context.envelope.trace);
        assert_eq!(&held.data[..], br#"{"order":42}"#);
    }

    #[test]
    fn converts_any_error_with_its_type_name() {
        let error = HandlerError::from(std::fmt::Error);

        assert_eq!(error.error_type(), "core::fmt::Error");
        assert_eq!(
            error.message(),
            "an error occurred when formatting an argument"
        );
    }
}
