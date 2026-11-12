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
use sisa_eventing_core::{
    CodecError, Deliveries, Delivery, Envelope, EnvelopeError, InboxStore, JsonCodec, Message,
    MessageType, Source, StoreError, TransportPublisher, TransportSubscriber,
};
use tokio::sync::watch;
use tracing::Instrument;

use crate::inbox::{EndpointInbox, InboxPipeline};
use crate::{Handler, HandlerContext, HandlerError, NoInbox, RetryPolicy};

pub(crate) type BoxFuture<'life, Output> = Pin<Box<dyn Future<Output = Output> + Send + 'life>>;

// The message a panic was raised with, when it has one.
fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a panic without a message")
}

/// How handling a delivery failed.
#[derive(Debug)]
pub enum Failure {
    /// No retry can fix it: no codec reads the data, the data isn't the message, or the endpoint
    /// has no handler for its type.
    Undecodable(HandlerError),
    /// The handler failed, or a store or the broker didn't take its work: a retry may work.
    Handler(HandlerError),
}

// A store that fails may work on a retry.
impl From<StoreError> for Failure {
    fn from(error: StoreError) -> Self {
        Self::Handler(error.into())
    }
}

/// A handler with its message type erased, so an endpoint keeps handlers of several types. It
/// gives the context back when the handler returns `Ok`.
pub trait ErasedHandler<Transaction, Codec>: Send + Sync {
    /// Decodes the delivered data and runs the handler.
    fn handle(
        &self,
        context: HandlerContext<Transaction, Codec>,
    ) -> BoxFuture<'_, Result<HandlerContext<Transaction, Codec>, Failure>>;
}

/// An endpoint's handlers by the message type they handle.
pub type Handlers<Transaction, Codec> =
    BTreeMap<MessageType, Arc<dyn ErasedHandler<Transaction, Codec>>>;

struct Typed<Msg, Inner> {
    handler: Inner,
    message: PhantomData<fn() -> Msg>,
}

impl<Msg: Message, Inner> Typed<Msg, Inner> {
    async fn decode_and_handle<Transaction, Codec>(
        &self,
        context: &mut HandlerContext<Transaction, Codec>,
    ) -> Result<(), Failure>
    where
        Inner: Handler<Msg, Transaction, Codec>,
        Codec: sisa_eventing_core::Codec,
    {
        let envelope = context.envelope();
        if !context.codec.can_decode(&envelope.content_type) {
            return Err(Failure::Undecodable(HandlerError::new(
                type_name::<CodecError>(),
                format!("no codec reads {}", envelope.content_type),
            )));
        }
        let message = context
            .codec
            .decode::<Msg>(&envelope.data)
            .map_err(|error| Failure::Undecodable(error.into()))?;
        self.handler
            .handle(message, context)
            .await
            .map_err(Failure::Handler)
    }
}

// Decodes the delivered data as `Msg`, then runs the handler.
impl<Msg, Transaction, Codec, Inner> ErasedHandler<Transaction, Codec> for Typed<Msg, Inner>
where
    Msg: Message,
    Transaction: Send + 'static,
    Codec: sisa_eventing_core::Codec,
    Inner: Handler<Msg, Transaction, Codec>,
{
    fn handle(
        &self,
        mut context: HandlerContext<Transaction, Codec>,
    ) -> BoxFuture<'_, Result<HandlerContext<Transaction, Codec>, Failure>> {
        Box::pin(async move {
            self.decode_and_handle(&mut context).await?;
            Ok(context)
        })
    }
}

// A handler added before the endpoint's inbox: it runs in the delivery's transaction, without
// seeing it.
struct WithoutTransaction<Codec>(Arc<dyn ErasedHandler<(), Codec>>);

// Sets the transaction aside while the handler runs.
impl<Transaction: Send + 'static, Codec: Send + Sync + 'static> ErasedHandler<Transaction, Codec>
    for WithoutTransaction<Codec>
{
    fn handle(
        &self,
        context: HandlerContext<Transaction, Codec>,
    ) -> BoxFuture<'_, Result<HandlerContext<Transaction, Codec>, Failure>> {
        Box::pin(async move {
            let (context, transaction) = context.with_transaction(());
            let context = self.0.handle(context).await?;
            Ok(context.with_transaction(transaction).0)
        })
    }
}

/// The handler for the context's message type.
pub(crate) fn find<'handlers, Transaction, Codec>(
    handlers: &'handlers Handlers<Transaction, Codec>,
    endpoint: &str,
    envelope: &Envelope,
) -> Result<&'handlers dyn ErasedHandler<Transaction, Codec>, Failure> {
    handlers
        .get(&envelope.message_type)
        .map(|handler| &**handler)
        .ok_or_else(|| {
            Failure::Undecodable(HandlerError::new(
                "no_handler",
                format!("endpoint {endpoint} has no handler for this message type"),
            ))
        })
}

/// Runs `handler`. A handler that panics fails like one that returns an error, so one bad
/// message doesn't stop the endpoint.
pub(crate) async fn call<Transaction, Codec>(
    handler: &dyn ErasedHandler<Transaction, Codec>,
    context: HandlerContext<Transaction, Codec>,
) -> Result<HandlerContext<Transaction, Codec>, Failure> {
    AssertUnwindSafe(handler.handle(context))
        .catch_unwind()
        .await
        .unwrap_or_else(|payload| {
            Err(Failure::Handler(HandlerError::new(
                "panic",
                panic_message(&*payload),
            )))
        })
}

/// Runs a delivery's handler with the steps around it, such as the inbox's transaction, and
/// returns the publishes to send before the ack.
pub trait Pipeline<Codec>: Send + Sync {
    /// Handles the delivery in `context`.
    fn run(
        &self,
        context: HandlerContext<(), Codec>,
    ) -> BoxFuture<'_, Result<Vec<Envelope>, Failure>>;
}

// An endpoint without an inbox: the handler alone.
pub(crate) struct Plain<Codec> {
    pub(crate) endpoint: String,
    pub(crate) handlers: Handlers<(), Codec>,
}

// Runs the handler and hands its publishes back.
impl<Codec: Send + Sync + 'static> Pipeline<Codec> for Plain<Codec> {
    fn run(
        &self,
        context: HandlerContext<(), Codec>,
    ) -> BoxFuture<'_, Result<Vec<Envelope>, Failure>> {
        Box::pin(async move {
            let handler = find(&self.handlers, &self.endpoint, context.envelope())?;
            Ok(call(handler, context).await?.held)
        })
    }
}

// A handler as added, with the message type it handles, checked when the endpoint is built.
type Added<Transaction, Codec> = (
    Result<MessageType, EnvelopeError>,
    Arc<dyn ErasedHandler<Transaction, Codec>>,
);

/// Sets up one endpoint: its inbox, its handlers and its retry policy. [`BusBuilder::endpoint`]
/// hands one out.
///
/// [`BusBuilder::endpoint`]: crate::BusBuilder::endpoint
pub struct EndpointBuilder<Inbox: EndpointInbox = NoInbox, Codec = JsonCodec> {
    pub(crate) name: String,
    retry: RetryPolicy,
    inbox: Inbox,
    handlers: Vec<Added<Inbox::Transaction, Codec>>,
}

impl<Codec: Send + Sync + 'static> EndpointBuilder<NoInbox, Codec> {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            retry: RetryPolicy::default(),
            inbox: NoInbox,
            handlers: Vec::new(),
        }
    }

    /// Records each handled message in `store`, so a redelivered message is acked without
    /// running its handler again.
    ///
    /// Each delivery then runs in a transaction of the store's: the endpoint begins it, records
    /// the message, runs the handler, and commits before it acks. Handlers take
    /// `&mut HandlerContext<Store::Transaction>` and do their database work in
    /// [`HandlerContext::transaction`], so it commits with the record, and rolls back with it when
    /// the handler fails. On a bus with an outbox, the handlers' publishes go to the outbox in the
    /// same transaction; add the outbox to the bus before this endpoint.
    #[must_use]
    pub fn inbox<Store>(self, store: Store) -> EndpointBuilder<Store, Codec>
    where
        Store: InboxStore,
        Store::Transaction: 'static,
    {
        let handlers = self
            .handlers
            .into_iter()
            .map(|(message_type, handler)| {
                let handler: Arc<dyn ErasedHandler<Store::Transaction, Codec>> =
                    Arc::new(WithoutTransaction(handler));
                (message_type, handler)
            })
            .collect();
        EndpointBuilder {
            name: self.name,
            retry: self.retry,
            inbox: store,
            handlers,
        }
    }
}

impl<Inbox: EndpointInbox, Codec: sisa_eventing_core::Codec> EndpointBuilder<Inbox, Codec> {
    /// Handles messages of type `Msg` with `handler`. The endpoint subscribes to every message
    /// type it has a handler for.
    #[must_use]
    pub fn handle<Msg: Message>(
        mut self,
        handler: impl Handler<Msg, Inbox::Transaction, Codec>,
    ) -> Self {
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

    // The endpoint with its handlers by message type, for a bus with `outbox`.
    pub(crate) fn build<Outbox>(
        self,
        outbox: &Arc<Outbox>,
        codec: &Arc<Codec>,
    ) -> Result<Endpoint<Codec>, crate::StartError>
    where
        Inbox: InboxPipeline<Outbox, Codec>,
    {
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
            message_types: handlers.keys().cloned().collect(),
            transactional: Inbox::TRANSACTIONAL,
            pipeline: self.inbox.pipeline(self.name.clone(), handlers, outbox),
            codec: Arc::clone(codec),
            name: self.name,
            retry: self.retry,
        })
    }
}

// Shows the endpoint's name, retry policy and how many handlers it has.
impl<Inbox: EndpointInbox, Codec> fmt::Debug for EndpointBuilder<Inbox, Codec> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EndpointBuilder")
            .field("name", &self.name)
            .field("retry", &self.retry)
            .field("inbox", &Inbox::TRANSACTIONAL)
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

pub(crate) struct Endpoint<Codec> {
    pub(crate) name: String,
    retry: RetryPolicy,
    pub(crate) message_types: Vec<MessageType>,
    // Whether it has an inbox, whose transaction the bus's outbox must be able to write in.
    pub(crate) transactional: bool,
    pipeline: Box<dyn Pipeline<Codec>>,
    codec: Arc<Codec>,
}

impl<Codec> Endpoint<Codec> {
    // Handles one delivery and settles it. A delivery is acked only once its handler returned
    // `Ok`, its transaction committed, and the handler's publishes are in the outbox or the
    // broker.
    async fn process<Transport>(
        &self,
        transport: &Transport,
        source: &Source,
        delivery: Transport::Delivery,
    ) where
        Transport: TransportPublisher + TransportSubscriber,
    {
        let attempt = delivery.attempt();
        let envelope = delivery.envelope();
        let id = envelope.id;
        let span = tracing::info_span!(
            "handle",
            endpoint = %self.name,
            message_type = %envelope.message_type,
            message_id = %id,
            attempt,
            traceparent = envelope.trace.as_ref().map(|trace| trace.traceparent.as_str()),
        );
        let context = HandlerContext::new(
            source.clone(),
            Arc::clone(&self.codec),
            self.name.clone(),
            envelope.clone(),
            attempt,
        );

        let result = match self.pipeline.run(context).instrument(span).await {
            Ok(held) => send_held(transport, &held).await,
            Err(failure) => Err(failure),
        };

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
                    error_envelope(delivery.envelope().clone(), &self.name, &error, attempt);
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
