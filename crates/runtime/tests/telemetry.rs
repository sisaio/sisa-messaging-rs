//! Trace context from a publish to the handling of the message, read back through the
//! OpenTelemetry SDK. `capture` installs process-wide providers, so this is a binary of its own.

#![expect(
    clippy::unwrap_used,
    reason = "tests and their helpers unwrap: a failure should panic"
)]

#[path = "it/support.rs"]
#[allow(dead_code, reason = "the other tests use the rest of the helpers")]
mod support;

use sisa_messaging_core::Publisher;
use sisa_messaging_runtime::InMemoryTransport;
use sisa_messaging_testing::eventually;
use sisa_telemetry::testing;
use tracing::Instrument;

use support::{Billing, InvoiceSent, OrderPlaced, billing_bus, published_of};

#[tokio::test]
async fn a_handling_starts_a_new_trace_linked_to_the_publish() {
    let captured = testing::capture();
    let transport = InMemoryTransport::new();
    let billing = Billing::default();
    let mut bus = billing_bus(&transport, &billing).await;

    let placing = tracing::info_span!("place_order");
    bus.publish(&OrderPlaced { id: 42 })
        .instrument(placing)
        .await
        .unwrap();
    eventually("the handling's span", async || {
        captured.span("handle").is_some()
    })
    .await;

    let placed = captured.span("place_order").unwrap().span_context;
    let handled = captured.span("handle").unwrap();
    assert_ne!(handled.span_context.trace_id(), placed.trace_id());
    assert_eq!(handled.links.len(), 1);
    assert_eq!(handled.links[0].span_context.trace_id(), placed.trace_id());
    assert_eq!(handled.links[0].span_context.span_id(), placed.span_id());

    // The envelope carries the publishing span, and the handler's own publish carries the
    // handling's span, not the first one.
    let [order] = published_of::<OrderPlaced>(&transport).try_into().unwrap();
    let [invoice] = published_of::<InvoiceSent>(&transport).try_into().unwrap();
    let traceparent = |span: &sisa_telemetry::opentelemetry::trace::SpanContext| {
        format!("00-{}-{}-01", span.trace_id(), span.span_id())
    };
    assert_eq!(order.trace.unwrap().traceparent, traceparent(&placed));
    assert_eq!(
        invoice.trace.unwrap().traceparent,
        traceparent(&handled.span_context)
    );
}
