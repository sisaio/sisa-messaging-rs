//! The bus: what the application builds, publishes with, and stops.

use std::collections::HashSet;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;
use sisa_messaging_core::{
    EndpointSpec, EnvelopeError, Message, OutboxStore, Publisher, Source, TransportError,
    TransportPublisher, TransportSubscriber,
};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use crate::publish::{PublishError, encode};
use crate::{EndpointBuilder, OutboxPublisher};

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

struct Inner<Transport, Outbox> {
    source: Source,
    transport: Arc<Transport>,
    endpoints: Vec<String>,
    outbox: Outbox,
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
pub struct Bus<Transport, Outbox = NoOutbox> {
    inner: Arc<Inner<Transport, Outbox>>,
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
            outbox: NoOutbox,
        }
    }
}

impl<Transport, Store: OutboxStore> Bus<Transport, Store> {
    /// A publisher that writes to the outbox on `connection`, the application's own transaction,
    /// such as `&mut tx` from sqlx's `pool.begin()`. The messages go out after the transaction
    /// commits, and never if it rolls back.
    ///
    /// The publisher borrows the transaction only while it lives, so the application keeps using
    /// it, and commits it, as usual.
    pub fn outbox<'call>(
        &'call self,
        connection: &'call mut Store::Connection,
    ) -> OutboxPublisher<'call, Store> {
        OutboxPublisher::new(&self.inner.outbox, &self.inner.source, connection)
    }
}

impl<Transport, Outbox> Bus<Transport, Outbox> {
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
impl<Transport, Outbox> Clone for Bus<Transport, Outbox> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// Publishes straight to the broker.
impl<Transport, Outbox> Publisher for Bus<Transport, Outbox>
where
    Transport: TransportPublisher,
    Outbox: Send + Sync + 'static,
{
    type Error = PublishError;

    async fn publish<Msg: Message>(&mut self, message: &Msg) -> Result<(), PublishError> {
        let envelope = encode(&self.inner.source, message)?;
        self.inner.transport.publish(&envelope).await?;
        Ok(())
    }
}

// Shows the source and the endpoints.
impl<Transport, Outbox> fmt::Debug for Bus<Transport, Outbox> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Bus")
            .field("source", &self.inner.source)
            .field("endpoints", &self.inner.endpoints)
            .finish_non_exhaustive()
    }
}

/// Sets up a [`Bus`]: its endpoints, then [`start`](BusBuilder::start).
pub struct BusBuilder<Transport, Outbox = NoOutbox> {
    source: String,
    transport: Transport,
    endpoints: Vec<EndpointBuilder>,
    outbox: Outbox,
}

impl<Transport> BusBuilder<Transport> {
    /// Keeps an outbox in `store`, which makes [`Bus::outbox`] available on the bus. The store is
    /// part of the bus's type, such as `Bus<NatsTransport, PostgresOutbox>`.
    #[must_use]
    pub fn outbox<Store: OutboxStore>(self, store: Store) -> BusBuilder<Transport, Store> {
        BusBuilder {
            source: self.source,
            transport: self.transport,
            endpoints: self.endpoints,
            outbox: store,
        }
    }
}

impl<Transport, Outbox> BusBuilder<Transport, Outbox> {
    /// Adds an endpoint: a durable subscription named `name`, such as `billing`, that every
    /// instance of the service shares. `configure` adds its handlers.
    #[must_use]
    pub fn endpoint(
        mut self,
        name: impl Into<String>,
        configure: impl FnOnce(EndpointBuilder) -> EndpointBuilder,
    ) -> Self {
        self.endpoints
            .push(configure(EndpointBuilder::new(name.into())));
        self
    }
}

impl<Transport, Outbox> BusBuilder<Transport, Outbox>
where
    Transport: TransportPublisher + TransportSubscriber,
    Outbox: Send + Sync + 'static,
{
    /// Subscribes every endpoint, then starts one worker for each, on the current Tokio runtime.
    /// No worker starts unless every endpoint subscribed.
    ///
    /// # Errors
    ///
    /// [`StartError`] when the source or a message type is invalid, an endpoint name or a
    /// handler is there twice, or the transport can't subscribe an endpoint.
    pub async fn start(self) -> Result<Bus<Transport, Outbox>, StartError> {
        let source = Source::new(self.source)?;
        let mut names = HashSet::new();
        let mut endpoints = Vec::new();
        for builder in self.endpoints {
            if !names.insert(builder.name.clone()) {
                return Err(StartError::DuplicateEndpoint(builder.name));
            }
            endpoints.push(builder.build()?);
        }

        let transport = Arc::new(self.transport);
        let mut subscribed = Vec::new();
        for endpoint in endpoints {
            let spec = EndpointSpec::new(endpoint.name.clone(), endpoint.message_types());
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
        let workers = subscribed
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
        Ok(Bus {
            inner: Arc::new(Inner {
                source,
                transport,
                endpoints: endpoint_names,
                outbox: self.outbox,
                stop,
                workers: Mutex::new(workers),
            }),
        })
    }
}

// Shows the source and the endpoints' names.
impl<Transport, Outbox> fmt::Debug for BusBuilder<Transport, Outbox> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let endpoints: Vec<&str> = self
            .endpoints
            .iter()
            .map(|endpoint| endpoint.name.as_str())
            .collect();
        formatter
            .debug_struct("BusBuilder")
            .field("source", &self.source)
            .field("endpoints", &endpoints)
            .finish_non_exhaustive()
    }
}
