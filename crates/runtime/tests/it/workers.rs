//! When the endpoint workers stop.

use std::sync::Arc;
use std::time::Duration;

use sisa_eventing_core::Publisher;
use sisa_eventing_runtime::{Bus, Handler, HandlerContext, HandlerError, InMemoryTransport};
use sisa_eventing_testing::eventually;
use tokio::sync::Notify;

use crate::support::{Billing, OrderPlaced, billing_bus};

const AN_HOUR: Duration = Duration::from_secs(3600);

#[tokio::test(start_paused = true)]
async fn stops_its_workers_on_shutdown_and_still_publishes() {
    let transport = InMemoryTransport::new();
    let billing = Billing::default();
    let mut bus = billing_bus(&transport, &billing).await;

    bus.shutdown().await;
    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();
    tokio::time::sleep(AN_HOUR).await;

    assert_eq!(transport.published().len(), 1);
    assert!(billing.handled().is_empty());
}

#[tokio::test(start_paused = true)]
async fn stops_its_workers_when_the_last_clone_drops() {
    let transport = InMemoryTransport::new();
    let billing = Billing::default();
    let mut bus = billing_bus(&transport, &billing).await;
    let clone = bus.clone();

    drop(clone);
    bus.publish(&OrderPlaced { id: 1 }).await.unwrap();
    eventually("the first handling", async || billing.handled().len() == 1).await;
    drop(bus);
    let mut publisher = Bus::builder("/orders", transport.clone())
        .start()
        .await
        .unwrap();
    publisher.publish(&OrderPlaced { id: 2 }).await.unwrap();
    tokio::time::sleep(AN_HOUR).await;

    assert_eq!(billing.handled().len(), 1);
}

struct Blocking {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Handler<OrderPlaced> for Blocking {
    async fn handle(&self, _: OrderPlaced, _: &mut HandlerContext) -> Result<(), HandlerError> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn makes_every_shutdown_wait_for_the_delivery_being_handled() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler = Blocking {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    };
    let mut bus = Bus::builder("/billing", InMemoryTransport::new())
        .endpoint("billing", |endpoint| {
            endpoint.handle::<OrderPlaced>(handler)
        })
        .start()
        .await
        .unwrap();
    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();
    started.notified().await;

    let shutdowns =
        [bus.clone(), bus.clone()].map(|clone| tokio::spawn(async move { clone.shutdown().await }));
    tokio::time::sleep(AN_HOUR).await;
    let waiting = shutdowns.iter().all(|shutdown| !shutdown.is_finished());
    release.notify_one();
    for shutdown in shutdowns {
        shutdown.await.unwrap();
    }

    assert!(waiting);
}
