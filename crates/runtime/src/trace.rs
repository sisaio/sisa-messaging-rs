//! Trace context between a publish and the handling of the message, through
//! `sisa-telemetry-core`. Without an application that installed OpenTelemetry, nothing here
//! writes or links anything.

use std::collections::HashMap;

use sisa_messaging_core::TraceContext;
use sisa_telemetry_core::propagation;

/// The trace context of the span a publish runs in, for the envelope to carry. `None` when no
/// propagator is installed or no span is recorded.
pub(crate) fn current() -> Option<TraceContext> {
    let mut headers = HashMap::new();
    propagation::inject(&tracing::Span::current(), &mut headers);

    let traceparent = headers.remove("traceparent")?;
    let tracestate = headers
        .remove("tracestate")
        .filter(|state| !state.is_empty());
    Some(TraceContext::new(traceparent, tracestate))
}

/// Makes `span`, which handles one message, the root of a new trace that links to the span the
/// message was published in. Call it before the span is entered. A message with no trace
/// context gets no link.
pub(crate) fn link(span: &tracing::Span, trace: Option<&TraceContext>) {
    let mut headers = HashMap::new();
    if let Some(trace) = trace {
        headers.insert("traceparent".to_owned(), trace.traceparent.clone());
        if let Some(tracestate) = &trace.tracestate {
            headers.insert("tracestate".to_owned(), tracestate.clone());
        }
    }
    propagation::link_from(span, &headers);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_telemetry_a_publish_carries_no_trace_context() {
        let span = tracing::info_span!("place_order");

        assert_eq!(span.in_scope(current), None);
    }

    #[test]
    fn without_telemetry_linking_does_nothing() {
        let trace = TraceContext::new(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            None,
        );

        link(&tracing::info_span!("handle"), Some(&trace));
    }
}
