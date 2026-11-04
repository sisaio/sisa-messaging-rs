//! Endpoints: their handlers, and the loop that receives, handles and settles each delivery.

use std::any::Any;
use std::any::type_name;
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::{FutureExt, StreamExt};
use sisa_messaging_core::{
    Codec, CodecError, Deliveries, Delivery, Envelope, EnvelopeError, JsonCodec, Message,
    MessageType, Source, TransportPublisher, TransportSubscriber,
};
use tokio::sync::watch;
use tracing::Instrument;

use crate::{Handler, HandlerContext, HandlerError, RetryPolicy};

type BoxFuture<'life, Output> = Pin<Box<dyn Future<Output = Output> + Send + 'life>>;

// The message a panic was raised with, when it has one.
fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a panic without a message")
}

// How handling a delivery failed.
enum Failure {
    // No retry can fix it: no codec reads the data, the data isn't the message, or the endpoint
    // has no handler for its type.
    Undecodable(HandlerError),
    // The handler failed, or its publishes couldn't be sent: a retry may work.
    Handler(HandlerError),
}

// A handler with its message type erased, so an endpoint keeps handlers of several types.
trait ErasedHandler: Send + Sync {
    fn handle<'life>(
        &'life self,
        context: &'life mut HandlerContext,
    ) -> BoxFuture<'life, Result<(), Failure>>;
}

struct Typed<Msg, Inner> {
    handler: Inner,
    message: PhantomData<fn() -> Msg>,
}

// Decodes the delivered data as `Msg`, then runs the handler.
impl<Msg: Message, Inner: Handler<Msg>> ErasedHandler for Typed<Msg, Inner> {
    fn handle<'life>(
        &'life self,
        context: &'life mut HandlerContext,
    ) -> BoxFuture<'life, Result<(), Failure>> {
        Box::pin(async move {
            let envelope = context.envelope();
            if !JsonCodec.can_decode(&envelope.content_type) {
                return Err(Failure::Undecodable(HandlerError::new(
                    type_name::<CodecError>(),
                    format!("no codec reads {}", envelope.content_type),
                )));
            }
            let message = JsonCodec
                .decode::<Msg>(&envelope.data)
                .map_err(|error| Failure::Undecodable(error.into()))?;
            self.handler
                .handle(message, context)
                .await
                .map_err(Failure::Handler)
        })
    }
}

/// Sets up one endpoint: its handlers and its retry policy. [`BusBuilder::endpoint`]
/// hands one out.
///
/// [`BusBuilder::endpoint`]: crate::BusBuilder::endpoint
pub struct EndpointBuilder {
    pub(crate) name: String,
    retry: RetryPolicy,
    handlers: Vec<(Result<MessageType, EnvelopeError>, Arc<dyn ErasedHandler>)>,
}

impl EndpointBuilder {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            retry: RetryPolicy::default(),
            handlers: Vec::new(),
        }
    }

    /// Handles messages of type `Msg` with `handler`. The endpoint subscribes to every message
    /// type it has a handler for.
    #[must_use]
    pub fn handle<Msg: Message>(mut self, handler: impl Handler<Msg>) -> Self {
        let typed = Typed {
            handler,
            message: PhantomData::<fn() -> Msg>,
        };
        self.handlers
            .push((MessageType::of::<Msg>(), Arc::new(typed)));
        self
    }

    /// Retries failed deliveries as `policy` says, instead of [`RetryPolicy::default`].
    #[must_use]
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    // The endpoint with its handlers by message type.
    pub(crate) fn build(self) -> Result<Endpoint, crate::StartError> {
        let mut handlers = BTreeMap::new();
        for (message_type, handler) in self.handlers {
            let message_type = message_type?;
            if handlers.contains_key(&message_type) {
                return Err(crate::StartError::DuplicateHandler {
                    endpoint: self.name,
                    message_type: message_type.to_string(),
                });
            }
            handlers.insert(message_type, handler);
        }
        Ok(Endpoint {
            name: self.name,
            retry: self.retry,
            handlers,
        })
    }
}

// Shows the endpoint's name, retry policy and how many handlers it has.
impl fmt::Debug for EndpointBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EndpointBuilder")
            .field("name", &self.name)
            .field("retry", &self.retry)
            .field("handlers", &self.handlers.len())
            .finish()
    }
}

fn error_envelope(
    mut envelope: Envelope,
    endpoint: &str,
    error: &HandlerError,
    attempt: u32,
) -> Envelope {
    let extensions = [
        ("errorendpoint", endpoint.to_owned()),
        ("errortype", error.error_type().to_owned()),
        ("errormessage", error.message().to_owned()),
        ("errorattempts", attempt.to_string()),
    ];
    for (name, value) in extensions {
        envelope
            .extensions
            .insert(name, value)
            .expect("the error extension names follow the CloudEvents rules");
    }
    envelope
}

// Sends a handler's held publishes in order. One that fails fails the handling, so the delivery
// comes again and the handler runs again; publishes sent before the failure may then go twice.
async fn send_held(transport: &impl TransportPublisher, held: &[Envelope]) -> Result<(), Failure> {
    for envelope in held {
        transport
            .publish(envelope)
            .await
            .map_err(|error| Failure::Handler(error.into()))?;
    }
    Ok(())
}

pub(crate) struct Endpoint {
    pub(crate) name: String,
    retry: RetryPolicy,
    handlers: BTreeMap<MessageType, Arc<dyn ErasedHandler>>,
}

impl Endpoint {
    pub(crate) fn message_types(&self) -> Vec<MessageType> {
        self.handlers.keys().cloned().collect()
    }

    // Handles one delivery and settles it. A delivery is acked only once its handler returned
    // `Ok` and the handler's publishes are in the broker.
    async fn process<Transport>(
        &self,
        transport: &Transport,
        source: &Source,
        delivery: Transport::Delivery,
    ) where
        Transport: TransportPublisher + TransportSubscriber,
    {
        let attempt = delivery.attempt();
        let envelope = delivery.envelope().clone();
        let span = tracing::info_span!(
            "handle",
            endpoint = %self.name,
            message_type = %envelope.message_type,
            message_id = %envelope.id,
            attempt,
            traceparent = envelope.trace.as_ref().map(|trace| trace.traceparent.as_str()),
        );
        let mut context = HandlerContext::new(source.clone(), self.name.clone(), envelope, attempt);

        let handled = match self.handlers.get(&context.envelope().message_type) {
            // A handler that panics fails like one that returns an error, so one bad message
            // doesn't stop the endpoint.
            Some(handler) => AssertUnwindSafe(handler.handle(&mut context))
                .catch_unwind()
                .instrument(span)
                .await
                .unwrap_or_else(|payload| {
                    Err(Failure::Handler(HandlerError::new(
                        "panic",
                        panic_message(&*payload),
                    )))
                }),
            None => Err(Failure::Undecodable(HandlerError::new(
                "no_handler",
                format!(
                    "endpoint {} has no handler for this message type",
                    self.name
                ),
            ))),
        };
        let result = match handled {
            Ok(()) => send_held(transport, &context.held).await,
            Err(failure) => Err(failure),
        };

        let id = context.envelope().id;
        let settled = match result {
            Ok(()) => delivery.ack().await,
            Err(Failure::Handler(error)) if !self.retry.is_last(attempt) => {
                tracing::warn!(
                    endpoint = %self.name,
                    message_id = %id,
                    attempt,
                    error_type = error.error_type(),
                    error = error.message(),
                    "handler failed; retrying",
                );
                delivery.retry(self.retry.delay(attempt)).await
            }
            Err(Failure::Handler(error) | Failure::Undecodable(error)) => {
                let failed =
                    error_envelope(context.envelope().clone(), &self.name, &error, attempt);
                match transport.publish_error(&self.name, &failed).await {
                    Ok(()) => {
                        tracing::warn!(
                            endpoint = %self.name,
                            message_id = %id,
                            attempt,
                            error_type = error.error_type(),
                            error = error.message(),
                            "gave up; sent to the error destination",
                        );
                        delivery.reject().await
                    }
                    Err(publish_error) => {
                        tracing::error!(
                            endpoint = %self.name,
                            message_id = %id,
                            attempt,
                            error = %publish_error,
                            "could not send to the error destination; retrying",
                        );
                        delivery.retry(self.retry.delay(attempt)).await
                    }
                }
            }
        };
        // A delivery the broker didn't settle comes again, so this only needs a record.
        if let Err(error) = settled {
            tracing::warn!(
                endpoint = %self.name,
                message_id = %id,
                attempt,
                %error,
                "could not settle the delivery",
            );
        }
    }

    // Receives and handles deliveries one at a time until the bus stops: `stop` changes on
    // shutdown and closes when the last clone of the bus drops.
    pub(crate) async fn receive<Transport>(
        self,
        transport: Arc<Transport>,
        source: Source,
        mut deliveries: Deliveries<Transport::Delivery>,
        mut stop: watch::Receiver<()>,
    ) where
        Transport: TransportPublisher + TransportSubscriber,
    {
        loop {
            let next = tokio::select! {
                biased;
                _ = stop.changed() => break,
                next = deliveries.next() => next,
            };
            match next {
                Some(Ok(delivery)) => self.process(&*transport, &source, delivery).await,
                // The transport reconnects on its own; waiting the first retry delay keeps a
                // run of errors from spinning.
                Some(Err(error)) => {
                    tracing::warn!(endpoint = %self.name, %error, "could not receive");
                    tokio::select! {
                        biased;
                        _ = stop.changed() => break,
                        () = tokio::time::sleep(self.retry.first_delay) => {}
                    }
                }
                None => {
                    tracing::error!(
                        endpoint = %self.name,
                        "the transport ended the endpoint's deliveries; it stops receiving",
                    );
                    break;
                }
            }
        }
    }
}
