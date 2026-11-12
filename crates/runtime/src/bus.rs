//! The bus: what the application builds, publishes with, and stops.

use std::collections::HashSet;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use sisa_messaging_core::{
    EndpointSpec, EnvelopeError, JsonCodec, Message, OutboxStore, PartitionStore, Publisher,
    Source, TransportError, TransportPublisher, TransportSubscriber,
};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use crate::dispatch::{self, DispatchSettings};
use crate::endpoint::Endpoint;
use crate::inbox::{EndpointInbox, InboxPipeline};
use crate::maintenance::{self, PartitionMode, PartitionSettings};
use crate::publish::{PublishError, encode};
use crate::{EndpointBuilder, NoInbox, OutboxPublisher};

/// Why a bus couldn't start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StartError {
    /// The source, or a handled message's [`Message::TYPE`], isn't valid.
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
    /// Two endpoints have the same name.
    #[error("endpoint `{0}` is added twice")]
    DuplicateEndpoint(String),
    /// An endpoint has two handlers for one message type.
    #[error("endpoint `{endpoint}` has two handlers for {message_type}")]
    DuplicateHandler {
        /// The endpoint's name.
        endpoint: String,
        /// The message type handled twice.
        message_type: String,
    },
    /// An endpoint with an inbox was added before the outbox, so its handlers' publishes would
    /// bypass the outbox. Add the outbox first.
    #[error("endpoint `{0}` has an inbox and is added before the outbox")]
    InboxBeforeOutbox(String),
    /// An endpoint was added before the bus's [codec](BusBuilder::codec), so it would decode
    /// with another one. Set the codec first.
    #[error("endpoint `{0}` is added before the codec")]
    EndpointBeforeCodec(String),
    /// The outbox's [`DispatchSettings`] can't run, such as a zero batch size.
    #[error("invalid dispatch settings: {0}")]
    DispatchSettings(&'static str),
    /// The transport couldn't subscribe an endpoint.
    #[error("could not subscribe endpoint `{endpoint}`: {source}")]
    Subscribe {
        /// The endpoint's name.
        endpoint: String,
        /// The transport's error.
        source: TransportError,
    },
}

/// The type of a bus built without an outbox: `bus.outbox(..)` doesn't compile on it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct NoOutbox;

mod sealed {
    use super::{Arc, BoxFuture, DispatchSettings, NoOutbox, OutboxStore, TransportPublisher};
    use super::{FutureExt, dispatch, watch};

    /// What a bus can carry as its outbox: [`NoOutbox`], or an [`OutboxStore`], whose
    /// dispatcher the bus starts.
    pub trait Outbox<Transport>: Send + Sync + 'static {
        /// The dispatcher for this outbox, if it has one.
        fn dispatcher(
            store: Arc<Self>,
            transport: Arc<Transport>,
            settings: DispatchSettings,
            stop: watch::Receiver<()>,
        ) -> Option<BoxFuture<'static, ()>>;
    }

    // Nothing to dispatch.
    impl<Transport> Outbox<Transport> for NoOutbox {
        fn dispatcher(
            _store: Arc<Self>,
            _transport: Arc<Transport>,
            _settings: DispatchSettings,
            _stop: watch::Receiver<()>,
        ) -> Option<BoxFuture<'static, ()>> {
            None
        }
    }

    // Publishes the store's rows through the transport.
    impl<Transport: TransportPublisher, Store: OutboxStore> Outbox<Transport> for Store {
        fn dispatcher(
            store: Arc<Self>,
            transport: Arc<Transport>,
            settings: DispatchSettings,
            stop: watch::Receiver<()>,
        ) -> Option<BoxFuture<'static, ()>> {
            Some(dispatch::run(store, transport, settings, stop).boxed())
        }
    }
}

struct Inner<Transport, Outbox, Codec> {
    source: Source,
    codec: Arc<Codec>,
    transport: Arc<Transport>,
    endpoints: Vec<String>,
    outbox: Arc<Outbox>,
    // Sends on shutdown; dropped with the last clone, which closes it. Either stops the workers.
    stop: watch::Sender<()>,
    // Held while `shutdown` waits, so a second call waits for the same workers.
    workers: Mutex<Vec<JoinHandle<()>>>,
}

/// What the application holds: it publishes, and it runs the endpoints and their handlers.
///
/// Build one with [`Bus::builder`]. Clones share the same workers, which stop on
/// [`shutdown`](Bus::shutdown) or once the last clone drops. `publish`, from [`Publisher`], goes
/// straight to the broker and returns once the broker has the message.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use sisa_messaging_core::{Message, Publisher};
/// use sisa_messaging_runtime::{Bus, HandlerContext, HandlerError, InMemoryTransport};
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
///     Ok(())
/// }
///
/// # tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
/// let mut bus = Bus::builder("/orders", InMemoryTransport::new())
///     .endpoint("billing", |endpoint| endpoint.handle::<OrderPlaced>(charge_order))
///     .start()
///     .await?;
///
/// bus.publish(&OrderPlaced { id: 42 }).await?;
/// bus.shutdown().await;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// The outbox is part of the bus's type, so `outbox` exists only on a bus built with one, and a
/// bus without one doesn't compile there:
///
/// ```compile_fail
/// # use sisa_messaging_runtime::{Bus, InMemoryTransport};
/// fn place_order(bus: &Bus<InMemoryTransport>, transaction: &mut ()) {
///     bus.outbox(transaction);
/// }
/// ```
pub struct Bus<Transport, Outbox = NoOutbox, Codec = JsonCodec> {
    inner: Arc<Inner<Transport, Outbox, Codec>>,
}

impl<Transport> Bus<Transport> {
    /// Starts building a bus that publishes as `source`, a URI reference such as `/orders`, over
    /// `transport`.
    #[must_use]
    pub fn builder(source: impl Into<String>, transport: Transport) -> BusBuilder<Transport> {
        BusBuilder {
            source: source.into(),
            transport,
            endpoints: Vec::new(),
            maintenance: Vec::new(),
            outbox: Arc::new(NoOutbox),
            codec: Arc::new(JsonCodec),
            dispatch: DispatchSettings::default(),
        }
    }
}

impl<Transport, Store: OutboxStore, Codec> Bus<Transport, Store, Codec> {
    /// A publisher that writes to the outbox on `connection`, the application's own transaction,
    /// such as `&mut tx` from sqlx's `pool.begin()`. The messages go out after the transaction
    /// commits, and never if it rolls back.
    ///
    /// The publisher borrows the transaction only while it lives, so the application keeps using
    /// it, and commits it, as usual.
    pub fn outbox<'call>(
        &'call self,
        connection: &'call mut Store::Connection,
    ) -> OutboxPublisher<'call, Store, Codec> {
        OutboxPublisher::new(
            &self.inner.outbox,
            &self.inner.source,
            &self.inner.codec,
            connection,
        )
    }
}

impl<Transport, Outbox, Codec> Bus<Transport, Outbox, Codec> {
    /// Stops the workers and waits for them: each finishes the delivery it is handling, and
    /// takes no other. Clones stop too; publishing still works.
    ///
    /// Don't await it from a handler: the handler's worker waits for the handler, so the call
    /// would never return.
    pub async fn shutdown(&self) {
        self.inner.stop.send_replace(());
        let mut workers = self.inner.workers.lock().await;
        // Each handle stays until its worker ends, so a later call still waits for it if this
        // one is dropped midway.
        while let Some(worker) = workers.last_mut() {
            // A worker that panicked was logged when it did, and has stopped as well.
            drop(worker.await);
            workers.pop();
        }
    }
}

// Shares the workers and the transport.
impl<Transport, Outbox, Codec> Clone for Bus<Transport, Outbox, Codec> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// Publishes straight to the broker.
impl<Transport, Outbox, Codec> Publisher for Bus<Transport, Outbox, Codec>
where
    Transport: TransportPublisher,
    Outbox: Send + Sync + 'static,
    Codec: sisa_messaging_core::Codec,
{
    type Error = PublishError;

    async fn publish<Msg: Message>(&mut self, message: &Msg) -> Result<(), PublishError> {
        let envelope = encode(&self.inner.source, &*self.inner.codec, message)?;
        self.inner.transport.publish(&envelope).await?;
        Ok(())
    }
}

// Shows the source and the endpoints.
impl<Transport, Outbox, Codec: sisa_messaging_core::Codec> fmt::Debug
    for Bus<Transport, Outbox, Codec>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Bus")
            .field("source", &self.inner.source)
            .field("codec", &self.inner.codec.content_type())
            .field("endpoints", &self.inner.endpoints)
            .finish_non_exhaustive()
    }
}

// Starts a partition maintenance worker, given the bus's stop signal.
type Maintenance = Box<dyn FnOnce(watch::Receiver<()>) -> BoxFuture<'static, ()> + Send + Sync>;

/// Sets up a [`Bus`]: its endpoints, then [`start`](BusBuilder::start).
pub struct BusBuilder<Transport, Outbox = NoOutbox, Codec = JsonCodec> {
    source: String,
    transport: Transport,
    // Built as they are added, with their names; an error is reported by `start`.
    endpoints: Vec<(String, Result<Endpoint<Codec>, StartError>)>,
    maintenance: Vec<Maintenance>,
    outbox: Arc<Outbox>,
    codec: Arc<Codec>,
    dispatch: DispatchSettings,
}

impl<Transport, Outbox> BusBuilder<Transport, Outbox> {
    /// Encodes the bus's publishes and decodes its deliveries with `codec`, instead of
    /// [`JsonCodec`]. The codec is part of the bus's type, such as
    /// `Bus<NatsTransport, PostgresOutbox, MyCodec>`, and handlers take
    /// `&mut HandlerContext<Transaction, MyCodec>`.
    ///
    /// A delivery whose content type the codec can't read goes to the endpoint's error
    /// destination. A codec may wrap another, such as one that encrypts what JSON encodes; the
    /// CloudEvents attributes stay plaintext in the headers and in the outbox.
    ///
    /// Endpoints decode with the codec the bus has when they are added, so set it before them.
    ///
    /// ```
    /// use bytes::Bytes;
    /// use sisa_messaging_core::{Codec, CodecError, JsonCodec, Message};
    /// use sisa_messaging_runtime::{Bus, InMemoryTransport};
    /// # fn seal(data: &[u8]) -> Vec<u8> { data.to_vec() }
    /// # fn open(data: &[u8]) -> Vec<u8> { data.to_vec() }
    ///
    /// /// JSON, sealed with the application's own key.
    /// struct Sealed;
    ///
    /// impl Codec for Sealed {
    ///     fn content_type(&self) -> &'static str {
    ///         "application/vnd.example.sealed+json"
    ///     }
    ///
    ///     fn encode<Msg: Message>(&self, message: &Msg) -> Result<Bytes, CodecError> {
    ///         Ok(seal(&JsonCodec.encode(message)?).into())
    ///     }
    ///
    ///     fn decode<Msg: Message>(&self, data: &[u8]) -> Result<Msg, CodecError> {
    ///         JsonCodec.decode(&open(data))
    ///     }
    /// }
    ///
    /// let builder = Bus::builder("/orders", InMemoryTransport::new()).codec(Sealed);
    /// ```
    #[must_use]
    pub fn codec<Codec: sisa_messaging_core::Codec>(
        self,
        codec: Codec,
    ) -> BusBuilder<Transport, Outbox, Codec> {
        let endpoints = self
            .endpoints
            .into_iter()
            .map(|(name, endpoint)| {
                let error = match endpoint {
                    Ok(_) => StartError::EndpointBeforeCodec(name.clone()),
                    Err(error) => error,
                };
                (name, Err(error))
            })
            .collect();
        BusBuilder {
            source: self.source,
            transport: self.transport,
            endpoints,
            maintenance: self.maintenance,
            outbox: self.outbox,
            codec: Arc::new(codec),
            dispatch: self.dispatch,
        }
    }
}

impl<Transport, Codec> BusBuilder<Transport, NoOutbox, Codec> {
    /// Keeps an outbox in `store`, which makes [`Bus::outbox`] available on the bus, and starts
    /// its dispatcher with the bus. The store is part of the bus's type, such as
    /// `Bus<NatsTransport, PostgresOutbox>`.
    ///
    /// Endpoints with an inbox write their handlers' publishes to it, so add it before them.
    #[must_use]
    pub fn outbox<Store: OutboxStore>(self, store: Store) -> BusBuilder<Transport, Store, Codec> {
        let endpoints = self
            .endpoints
            .into_iter()
            .map(|(name, endpoint)| match endpoint {
                Ok(endpoint) if endpoint.transactional => {
                    (name.clone(), Err(StartError::InboxBeforeOutbox(name)))
                }
                other => (name, other),
            })
            .collect();
        BusBuilder {
            source: self.source,
            transport: self.transport,
            endpoints,
            maintenance: self.maintenance,
            outbox: Arc::new(store),
            codec: self.codec,
            dispatch: self.dispatch,
        }
    }
}

impl<Transport, Store: OutboxStore, Codec> BusBuilder<Transport, Store, Codec> {
    /// Sets how the dispatcher publishes the outbox; [`DispatchSettings::default`] otherwise.
    #[must_use]
    pub fn dispatch(mut self, settings: DispatchSettings) -> Self {
        self.dispatch = settings;
        self
    }
}

impl<Transport, Outbox, Codec: sisa_messaging_core::Codec> BusBuilder<Transport, Outbox, Codec> {
    /// Adds an endpoint: a durable subscription named `name`, such as `billing`, that every
    /// instance of the service shares. `configure` adds its handlers, and its
    /// [inbox](EndpointBuilder::inbox) if it has one.
    #[must_use]
    pub fn endpoint<Inbox>(
        mut self,
        name: impl Into<String>,
        configure: impl FnOnce(EndpointBuilder<NoInbox, Codec>) -> EndpointBuilder<Inbox, Codec>,
    ) -> Self
    where
        Inbox: EndpointInbox + InboxPipeline<Outbox, Codec>,
    {
        let builder = configure(EndpointBuilder::new(name.into()));
        self.endpoints.push((
            builder.name.clone(),
            builder.build(&self.outbox, &self.codec),
        ));
        self
    }

    /// Keeps the partitions of `store`'s table, such as the outbox's or the inbox's, as
    /// `settings` say: a worker creates them ahead and applies the retention once an hour, one
    /// instance at a time. With [`PartitionMode::External`] it adds no worker.
    #[must_use]
    pub fn partitions(mut self, store: impl PartitionStore, settings: PartitionSettings) -> Self {
        if settings.partitions == PartitionMode::Managed {
            self.maintenance.push(Box::new(move |stop| {
                maintenance::run(store, settings, stop).boxed()
            }));
        }
        self
    }
}

impl<Transport, Outbox, Codec> BusBuilder<Transport, Outbox, Codec>
where
    Transport: TransportPublisher + TransportSubscriber,
    Outbox: sealed::Outbox<Transport>,
    Codec: sisa_messaging_core::Codec,
{
    /// Subscribes every endpoint, then starts one worker for each, one for each table whose
    /// [`partitions`](BusBuilder::partitions) it keeps, and the outbox's dispatcher when the bus
    /// has an outbox, on the current Tokio runtime. No worker starts unless every endpoint
    /// subscribed.
    ///
    /// # Errors
    ///
    /// [`StartError`] when the source, a message type or the dispatch settings are invalid, an
    /// endpoint name or a handler is there twice, or the transport can't subscribe an endpoint.
    pub async fn start(self) -> Result<Bus<Transport, Outbox, Codec>, StartError> {
        let source = Source::new(self.source)?;
        if let Some(reason) = self.dispatch.invalid() {
            return Err(StartError::DispatchSettings(reason));
        }
        let mut names = HashSet::new();
        let mut endpoints = Vec::new();
        for (name, endpoint) in self.endpoints {
            if !names.insert(name.clone()) {
                return Err(StartError::DuplicateEndpoint(name));
            }
            endpoints.push(endpoint?);
        }

        let transport = Arc::new(self.transport);
        let mut subscribed = Vec::new();
        for endpoint in endpoints {
            let spec = EndpointSpec::new(endpoint.name.clone(), endpoint.message_types.clone());
            match transport.subscribe(&spec).await {
                Ok(deliveries) => subscribed.push((endpoint, deliveries)),
                Err(error) => {
                    return Err(StartError::Subscribe {
                        endpoint: endpoint.name,
                        source: error,
                    });
                }
            }
        }

        let (stop, _) = watch::channel(());
        let endpoint_names = subscribed
            .iter()
            .map(|(endpoint, _)| endpoint.name.clone())
            .collect();
        let mut workers: Vec<JoinHandle<()>> = subscribed
            .into_iter()
            .map(|(endpoint, deliveries)| {
                let name = endpoint.name.clone();
                let receive = endpoint.receive(
                    Arc::clone(&transport),
                    source.clone(),
                    deliveries,
                    stop.subscribe(),
                );
                // Handler panics are caught per delivery; this reports one in the transport.
                tokio::spawn(async move {
                    if AssertUnwindSafe(receive).catch_unwind().await.is_err() {
                        tracing::error!(endpoint = %name, "the endpoint's worker panicked; it stops receiving");
                    }
                })
            })
            .collect();
        workers.extend(
            self.maintenance
                .into_iter()
                .map(|maintain| tokio::spawn(maintain(stop.subscribe()))),
        );
        let outbox = self.outbox;
        let dispatcher = Outbox::dispatcher(
            Arc::clone(&outbox),
            Arc::clone(&transport),
            self.dispatch,
            stop.subscribe(),
        );
        workers.extend(dispatcher.map(|dispatch| {
            tokio::spawn(async move {
                if AssertUnwindSafe(dispatch).catch_unwind().await.is_err() {
                    tracing::error!("the outbox dispatcher panicked; it stops dispatching");
                }
            })
        }));
        Ok(Bus {
            inner: Arc::new(Inner {
                source,
                codec: self.codec,
                transport,
                endpoints: endpoint_names,
                outbox,
                stop,
                workers: Mutex::new(workers),
            }),
        })
    }
}

// Shows the source and the endpoints' names.
impl<Transport, Outbox, Codec> fmt::Debug for BusBuilder<Transport, Outbox, Codec> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let endpoints: Vec<&str> = self
            .endpoints
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        formatter
            .debug_struct("BusBuilder")
            .field("source", &self.source)
            .field("endpoints", &endpoints)
            .field("partition_workers", &self.maintenance.len())
            .finish_non_exhaustive()
    }
}
