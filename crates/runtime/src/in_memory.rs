//! A transport inside the process, for tests.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_util::stream;
use sisa_messaging_core::{
    Deliveries, Delivery, EndpointSpec, Envelope, MessageId, MessageType, TransportError,
    TransportPublisher, TransportSubscriber,
};
use tokio::sync::Notify;

fn lock<Value>(mutex: &Mutex<Value>) -> MutexGuard<'_, Value> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// A copy of a message waiting for one of the endpoint's subscribers.
struct Pending {
    envelope: Arc<Envelope>,
    attempt: u32,
}

// One endpoint's copies, shared by all its subscribers.
#[derive(Default)]
struct Queue {
    pending: Mutex<VecDeque<Pending>>,
    // `notify_one` keeps a permit when nobody waits, so a push between a subscriber finding the
    // queue empty and waiting isn't missed.
    ready: Notify,
    rejected: Mutex<Vec<MessageId>>,
}

impl Queue {
    fn push(&self, pending: Pending) {
        lock(&self.pending).push_back(pending);
        self.ready.notify_one();
    }

    async fn pop(&self) -> Pending {
        loop {
            if let Some(pending) = lock(&self.pending).pop_front() {
                return pending;
            }
            self.ready.notified().await;
        }
    }
}

#[derive(Default)]
struct Endpoint {
    message_types: HashSet<MessageType>,
    queue: Arc<Queue>,
}

#[derive(Default)]
struct State {
    endpoints: HashMap<String, Endpoint>,
    published: Vec<Envelope>,
    errors: HashMap<String, Vec<Envelope>>,
    failing: bool,
}

/// A message received from an [`InMemoryTransport`].
///
/// Dropping it without settling it puts the message back at once, with the next attempt number,
/// as a broker does once its acknowledgement timeout passes.
pub struct InMemoryDelivery {
    envelope: Arc<Envelope>,
    attempt: u32,
    queue: Arc<Queue>,
    settled: bool,
}

impl InMemoryDelivery {
    fn next_attempt(&self) -> Pending {
        Pending {
            envelope: Arc::clone(&self.envelope),
            attempt: self.attempt + 1,
        }
    }
}

// Settles a copy on its endpoint's queue: ack and reject drop it, retry puts it back later.
impl Delivery for InMemoryDelivery {
    fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    fn attempt(&self) -> u32 {
        self.attempt
    }

    async fn ack(mut self) -> Result<(), TransportError> {
        self.settled = true;
        Ok(())
    }

    /// Puts the message back after `delay`, on a Tokio task, so it needs a Tokio runtime with
    /// its timer.
    async fn retry(mut self, delay: Duration) -> Result<(), TransportError> {
        self.settled = true;
        let pending = self.next_attempt();
        let queue = Arc::clone(&self.queue);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            queue.push(pending);
        });
        Ok(())
    }

    async fn reject(mut self) -> Result<(), TransportError> {
        self.settled = true;
        lock(&self.queue.rejected).push(self.envelope.id);
        Ok(())
    }
}

// Puts an unsettled message back, as a broker redelivers one whose consumer went away.
impl Drop for InMemoryDelivery {
    fn drop(&mut self) {
        if !self.settled {
            self.queue.push(self.next_attempt());
        }
    }
}

// Shows which message and attempt this is, not the data.
impl fmt::Debug for InMemoryDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryDelivery")
            .field("id", &self.envelope.id)
            .field("message_type", &self.envelope.message_type)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

/// A transport inside the process, for the runtime's tests and yours: no broker, no Docker.
///
/// It keeps the promises of [`TransportPublisher`] and [`TransportSubscriber`]: each endpoint
/// gets its own copy of a message (fan-out), its subscribers share those copies (competing
/// consumers), a retried or dropped delivery comes again with the next attempt number, and an
/// acked or rejected one doesn't. Unlike a broker, it keeps nothing for an endpoint before the
/// endpoint's first subscription, and nothing at all once the last clone is dropped.
///
/// Clones share their messages, so a test can hold one clone while the bus uses another, and
/// read [`published`](InMemoryTransport::published) afterwards.
#[derive(Clone, Default)]
pub struct InMemoryTransport {
    state: Arc<Mutex<State>>,
}

impl InMemoryTransport {
    /// A transport with no endpoints and no messages.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every envelope published so far, in order.
    #[must_use]
    pub fn published(&self) -> Vec<Envelope> {
        lock(&self.state).published.clone()
    }

    /// The ids of the deliveries an endpoint rejected so far, in order.
    #[must_use]
    pub fn rejected(&self, endpoint: &str) -> Vec<MessageId> {
        lock(&self.state)
            .endpoints
            .get(endpoint)
            .map(|entry| lock(&entry.queue.rejected).clone())
            .unwrap_or_default()
    }

    /// Every envelope sent to an endpoint's error destination so far, in order.
    #[must_use]
    pub fn errors(&self, endpoint: &str) -> Vec<Envelope> {
        lock(&self.state)
            .errors
            .get(endpoint)
            .cloned()
            .unwrap_or_default()
    }

    /// Makes every publish, to a message type or an error destination, fail while `failing` is
    /// true, as a broker that can't be reached does.
    pub fn fail_publishes(&self, failing: bool) {
        lock(&self.state).failing = failing;
    }
}

fn unreachable_broker() -> TransportError {
    TransportError::Broker {
        context: "publishing in memory",
        source: "publishes are set to fail".into(),
    }
}

// Copies each envelope to every endpoint subscribed to its message type.
impl TransportPublisher for InMemoryTransport {
    async fn publish(&self, envelope: &Envelope) -> Result<(), TransportError> {
        let mut state = lock(&self.state);
        if state.failing {
            return Err(unreachable_broker());
        }
        state.published.push(envelope.clone());
        let envelope = Arc::new(envelope.clone());
        for endpoint in state.endpoints.values() {
            if endpoint.message_types.contains(&envelope.message_type) {
                endpoint.queue.push(Pending {
                    envelope: Arc::clone(&envelope),
                    attempt: 1,
                });
            }
        }
        Ok(())
    }
}

// Creates the endpoint on its first subscription; a later one joins it and adds its message
// types. An error destination is a list per endpoint, read with `errors`.
impl TransportSubscriber for InMemoryTransport {
    type Delivery = InMemoryDelivery;

    async fn subscribe(
        &self,
        endpoint: &EndpointSpec,
    ) -> Result<Deliveries<Self::Delivery>, TransportError> {
        let shared_queue = {
            let mut state = lock(&self.state);
            let entry = state.endpoints.entry(endpoint.name.clone()).or_default();
            entry
                .message_types
                .extend(endpoint.message_types.iter().cloned());
            Arc::clone(&entry.queue)
        };
        let deliveries = stream::unfold(shared_queue, async |queue| {
            let Pending { envelope, attempt } = queue.pop().await;
            let delivery = InMemoryDelivery {
                envelope,
                attempt,
                queue: Arc::clone(&queue),
                settled: false,
            };
            Some((Ok(delivery), queue))
        });
        Ok(Box::pin(deliveries))
    }

    async fn publish_error(
        &self,
        endpoint: &str,
        envelope: &Envelope,
    ) -> Result<(), TransportError> {
        let mut state = lock(&self.state);
        if state.failing {
            return Err(unreachable_broker());
        }
        state
            .errors
            .entry(endpoint.to_owned())
            .or_default()
            .push(envelope.clone());
        Ok(())
    }
}

// Shows the endpoints and how many messages were published, not the messages.
impl fmt::Debug for InMemoryTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock(&self.state);
        let mut endpoints: Vec<&str> = state.endpoints.keys().map(String::as_str).collect();
        endpoints.sort_unstable();
        formatter
            .debug_struct("InMemoryTransport")
            .field("endpoints", &endpoints)
            .field("published", &state.published.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::StreamExt;
    use sisa_messaging_core::{ContentType, Source};

    use super::*;

    const ORDER_PLACED: &str = "orders.order_placed.v1";

    fn envelope(message_type: &str) -> Envelope {
        Envelope::new(
            MessageType::new(message_type).unwrap(),
            Source::new("/orders").unwrap(),
            ContentType::new("application/json").unwrap(),
            Bytes::from_static(b"{}"),
        )
    }

    async fn subscribe(
        transport: &InMemoryTransport,
        name: &str,
        message_type: &str,
    ) -> Deliveries<InMemoryDelivery> {
        let spec = EndpointSpec::new(name, vec![MessageType::new(message_type).unwrap()]);
        transport.subscribe(&spec).await.unwrap()
    }

    async fn next(deliveries: &mut Deliveries<InMemoryDelivery>) -> InMemoryDelivery {
        deliveries.next().await.unwrap().unwrap()
    }

    // Whether a delivery arrives within a second; time is paused, so the wait is instant.
    async fn arrives(deliveries: &mut Deliveries<InMemoryDelivery>) -> bool {
        tokio::time::timeout(Duration::from_secs(1), deliveries.next())
            .await
            .is_ok()
    }

    #[tokio::test(start_paused = true)]
    async fn gives_each_endpoint_its_own_copy_of_its_message_types() {
        let transport = InMemoryTransport::new();
        let mut billing = subscribe(&transport, "billing", ORDER_PLACED).await;
        let mut shipping = subscribe(&transport, "shipping", ORDER_PLACED).await;
        let mut audit = subscribe(&transport, "audit", "orders.order_cancelled.v1").await;
        let sent = envelope(ORDER_PLACED);

        transport.publish(&sent).await.unwrap();

        assert_eq!(next(&mut billing).await.envelope(), &sent);
        assert_eq!(next(&mut shipping).await.envelope(), &sent);
        assert!(!arrives(&mut audit).await);
    }

    #[tokio::test(start_paused = true)]
    async fn shares_an_endpoint_between_its_subscribers() {
        let transport = InMemoryTransport::new();
        let mut first = subscribe(&transport, "billing", ORDER_PLACED).await;
        let mut second = subscribe(&transport, "billing", ORDER_PLACED).await;

        transport.publish(&envelope(ORDER_PLACED)).await.unwrap();
        transport.publish(&envelope(ORDER_PLACED)).await.unwrap();
        let one = next(&mut first).await;
        let other = next(&mut second).await;

        assert_ne!(one.envelope().id, other.envelope().id);
        assert!(!arrives(&mut first).await);
        assert!(!arrives(&mut second).await);
    }

    #[tokio::test(start_paused = true)]
    async fn counts_the_first_delivery_as_attempt_1() {
        let transport = InMemoryTransport::new();
        let mut billing = subscribe(&transport, "billing", ORDER_PLACED).await;

        transport.publish(&envelope(ORDER_PLACED)).await.unwrap();

        assert_eq!(next(&mut billing).await.attempt(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn doesnt_deliver_an_acked_message_again() {
        let transport = InMemoryTransport::new();
        let mut billing = subscribe(&transport, "billing", ORDER_PLACED).await;
        transport.publish(&envelope(ORDER_PLACED)).await.unwrap();

        next(&mut billing).await.ack().await.unwrap();

        assert!(!arrives(&mut billing).await);
    }

    #[tokio::test(start_paused = true)]
    async fn doesnt_deliver_a_rejected_message_again() {
        let transport = InMemoryTransport::new();
        let mut billing = subscribe(&transport, "billing", ORDER_PLACED).await;
        transport.publish(&envelope(ORDER_PLACED)).await.unwrap();

        next(&mut billing).await.reject().await.unwrap();

        assert!(!arrives(&mut billing).await);
    }

    #[tokio::test(start_paused = true)]
    async fn delivers_a_retried_message_again_after_the_delay() {
        let transport = InMemoryTransport::new();
        let mut billing = subscribe(&transport, "billing", ORDER_PLACED).await;
        transport.publish(&envelope(ORDER_PLACED)).await.unwrap();
        let start = tokio::time::Instant::now();

        next(&mut billing)
            .await
            .retry(Duration::from_secs(5))
            .await
            .unwrap();
        let again = next(&mut billing).await;

        assert_eq!(start.elapsed(), Duration::from_secs(5));
        assert_eq!(again.attempt(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn delivers_a_dropped_message_again_at_once() {
        let transport = InMemoryTransport::new();
        let mut billing = subscribe(&transport, "billing", ORDER_PLACED).await;
        transport.publish(&envelope(ORDER_PLACED)).await.unwrap();

        drop(next(&mut billing).await);

        assert_eq!(next(&mut billing).await.attempt(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn records_every_published_envelope_in_order() {
        let transport = InMemoryTransport::new();
        let sent = [
            envelope(ORDER_PLACED),
            envelope("orders.order_cancelled.v1"),
        ];

        for envelope in &sent {
            transport.publish(envelope).await.unwrap();
        }

        assert_eq!(transport.published(), sent);
    }

    #[tokio::test(start_paused = true)]
    async fn keeps_error_destinations_apart_from_published_messages() {
        let transport = InMemoryTransport::new();
        let failed = envelope(ORDER_PLACED);

        transport.publish_error("billing", &failed).await.unwrap();

        assert_eq!(transport.errors("billing"), [failed]);
        assert!(transport.errors("shipping").is_empty());
        assert!(transport.published().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn fails_publishes_while_told_to() {
        let transport = InMemoryTransport::new();
        let sent = envelope(ORDER_PLACED);

        transport.fail_publishes(true);
        assert!(transport.publish(&sent).await.is_err());
        assert!(transport.publish_error("billing", &sent).await.is_err());
        transport.fail_publishes(false);
        transport.publish(&sent).await.unwrap();

        assert_eq!(transport.published(), [sent]);
    }
}
