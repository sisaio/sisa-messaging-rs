//! The bus over Iggy: the runtime's retries and its error destination.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sisa_eventing_core::{Message, Publisher};
use sisa_eventing_iggy::IggyTransport;
use sisa_eventing_runtime::{Bus, Handler, HandlerContext, HandlerError, RetryPolicy};
use sisa_eventing_testing::{Iggy, eventually};

use crate::common::{header, settings, stored};

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    id: u64,
}

impl Message for OrderPlaced {
    const TYPE: &'static str = "orders.order_placed.v1";
}

/// Fails order 1 every time, and records the attempts at each order.
#[derive(Clone, Default)]
struct Billing {
    attempts: Arc<Mutex<Vec<(u64, u32)>>>,
}

impl Handler<OrderPlaced> for Billing {
    async fn handle(
        &self,
        order: OrderPlaced,
        context: &mut HandlerContext,
    ) -> Result<(), HandlerError> {
        self.attempts
            .lock()
            .unwrap()
            .push((order.id, context.attempt()));
        if order.id == 1 {
            return Err(HandlerError::new("card_declined", "the card was declined"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_failing_message_is_retried_then_sent_to_the_error_topic_and_the_next_is_handled() {
    let iggy = Iggy::start().await;
    let mut settings = settings(&iggy);
    settings.partitions = 1;
    let billing = Billing::default();
    let mut retry = RetryPolicy::default();
    retry.attempts = 3;
    retry.first_delay = Duration::from_millis(100);
    let mut bus = Bus::builder("/orders", IggyTransport::connect(settings).await.unwrap())
        .endpoint("billing", |endpoint| {
            endpoint.retry(retry).handle::<OrderPlaced>(billing.clone())
        })
        .start()
        .await
        .unwrap();

    bus.publish(&OrderPlaced { id: 1 }).await.unwrap();
    bus.publish(&OrderPlaced { id: 2 }).await.unwrap();

    eventually("billing handles the second order", async || {
        billing.attempts.lock().unwrap().contains(&(2, 1))
    })
    .await;
    // One partition: the second order waits for the first one's retries.
    assert_eq!(
        *billing.attempts.lock().unwrap(),
        [(1, 1), (1, 2), (1, 3), (2, 1)]
    );
    let copies = stored(&iggy, "_error.billing", 1).await;
    assert_eq!(copies.len(), 1);
    assert_eq!(
        header(&copies[0], "ce_errortype").as_deref(),
        Some("card_declined")
    );
    assert_eq!(header(&copies[0], "ce_errorattempts").as_deref(), Some("3"));
    bus.shutdown().await;
}
