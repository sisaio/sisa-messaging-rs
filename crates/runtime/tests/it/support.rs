//! Messages and helpers the tests share.

use serde::{Deserialize, Serialize};
use sisa_messaging_core::{Message, Publisher};
use sisa_messaging_runtime::{
    Bus, EndpointBuilder, HandlerContext, HandlerError, InMemoryTransport,
};
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderPlaced {
    pub id: u64,
}

impl Message for OrderPlaced {
    const TYPE: &'static str = "orders.order_placed.v1";
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvoiceSent {
    pub order: u64,
}

impl Message for InvoiceSent {
    const TYPE: &'static str = "billing.invoice_sent.v1";
}

/// One handling: when, and which attempt.
#[derive(Debug, Clone, Copy)]
pub struct Handled {
    pub at: Instant,
    pub attempt: u32,
}

/// A handler that records each handling, publishes an `InvoiceSent`, and then fails its first
/// `failures` attempts.
#[derive(Clone, Default)]
pub struct Billing {
    pub handled: Arc<Mutex<Vec<Handled>>>,
    pub failures: u32,
}

impl Billing {
    pub fn failing(failures: u32) -> Self {
        Self {
            failures,
            ..Self::default()
        }
    }

    pub fn handled(&self) -> Vec<Handled> {
        self.handled.lock().unwrap().clone()
    }

    pub fn attempts(&self) -> Vec<u32> {
        self.handled()
            .iter()
            .map(|handled| handled.attempt)
            .collect()
    }

    pub fn endpoint(&self, endpoint: EndpointBuilder) -> EndpointBuilder {
        endpoint.handle::<OrderPlaced>(self.clone())
    }
}

impl sisa_messaging_runtime::Handler<OrderPlaced> for Billing {
    async fn handle(
        &self,
        order: OrderPlaced,
        context: &mut HandlerContext,
    ) -> Result<(), HandlerError> {
        let attempt = context.attempt();
        self.handled.lock().unwrap().push(Handled {
            at: Instant::now(),
            attempt,
        });
        context.publish(&InvoiceSent { order: order.id }).await?;
        if attempt <= self.failures {
            return Err(HandlerError::new(
                "payment_declined",
                "the card was declined",
            ));
        }
        Ok(())
    }
}

/// A bus on `transport` with a `billing` endpoint handled by `billing`.
pub async fn billing_bus(
    transport: &InMemoryTransport,
    billing: &Billing,
) -> Bus<InMemoryTransport> {
    Bus::builder("/billing", transport.clone())
        .endpoint("billing", |endpoint| billing.endpoint(endpoint))
        .start()
        .await
        .unwrap()
}

/// The envelopes of type `Msg` the transport has published.
pub fn published_of<Msg: Message>(
    transport: &InMemoryTransport,
) -> Vec<sisa_messaging_core::Envelope> {
    transport
        .published()
        .into_iter()
        .filter(|envelope| envelope.message_type.as_str() == Msg::TYPE)
        .collect()
}
