//! The bus over Redis: the runtime's retries and its error destination.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sisa_messaging_core::{Message, Publisher};
use sisa_messaging_redis::RedisTransport;
use sisa_messaging_runtime::{Bus, Handler, HandlerContext, HandlerError, RetryPolicy};
use sisa_messaging_testing::{Redis, eventually};

use crate::common::{settings, stored};

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
async fn a_failing_message_is_retried_then_sent_to_the_error_stream_and_the_next_is_handled() {
    let redis = Redis::start().await;
    let mut settings = settings(&redis);
    settings.ack_wait = Duration::from_secs(1);
    let billing = Billing::default();
    let mut retry = RetryPolicy::default();
    retry.attempts = 3;
    retry.first_delay = Duration::from_millis(100);
    let mut bus = Bus::builder("/orders", RedisTransport::connect(settings).await.unwrap())
        .endpoint("billing", |endpoint| {
            endpoint.retry(retry).handle::<OrderPlaced>(billing.clone())
        })
        .start()
        .await
        .unwrap();

    bus.publish(&OrderPlaced { id: 1 }).await.unwrap();
    bus.publish(&OrderPlaced { id: 2 }).await.unwrap();

    eventually("billing gives up on the first order", async || {
        billing.attempts.lock().unwrap().contains(&(1, 3))
    })
    .await;
    // A retry leaves its entry pending, so the second order goes ahead of the first's retries.
    let attempts = billing.attempts.lock().unwrap().clone();
    assert!(attempts.contains(&(2, 1)), "{attempts:?}");
    assert_eq!(
        attempts
            .iter()
            .filter(|(id, _)| *id == 1)
            .collect::<Vec<_>>(),
        [&(1, 1), &(1, 2), &(1, 3)]
    );
    eventually("the copy is in the error stream", async || {
        stored(&redis, "sisa:_error:billing").await.len() == 1
    })
    .await;
    let copies = stored(&redis, "sisa:_error:billing").await;
    assert_eq!(copies[0]["ce_errortype"], "card_declined");
    assert_eq!(copies[0]["ce_errorattempts"], "3");
    bus.shutdown().await;
}
