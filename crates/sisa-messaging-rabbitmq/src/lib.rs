//! RabbitMQ AMQP 0-9-1 transport provider for outbound and inbound messaging.
//!
//! The application owns the connection, TLS, authentication, topology, and channels. It hands
//! the provider a lapin channel already in publisher-confirm mode for [`RabbitMqPublisher`], and
//! one dedicated channel per [`RabbitMqDeliverySource`]. The provider never declares exchanges or
//! queues, never enables confirms, and performs no I/O in constructors. The connection must not
//! enable lapin automatic recovery, because a recovered channel reuses delivery tags; the provider
//! cannot verify this precondition.
//!
//! [`RabbitMqMapper`] projects shared envelopes into an AMQP headers table and a [`Route`]
//! selected by a [`RouteResolver`]. Publication is mandatory and awaits the broker confirm.
//! Inbound delivery implements the individual-delivery profile: acknowledgement, immediate
//! requeue through a zero-delay negative acknowledgement, and terminal reject, each confirmed by
//! a following `basic.qos` round trip. Delayed retry and heartbeat are unsupported.
//!
//! Errors, `Debug` output, spans, and metrics never contain payloads, header values, routing keys,
//! exchange or queue names, URLs, or credentials.

#![forbid(unsafe_code)]

mod error;
mod mapper;
mod publisher;
mod settings;
mod settlement;
mod source;
mod telemetry;

pub use error::{MappingError, RabbitMqError};
pub use mapper::{
    ExchangeName, RabbitMqMapper, RabbitMqWire, Route, RouteResolver, RoutingKey, TypeRouteResolver,
};
pub use publisher::RabbitMqPublisher;
pub use settings::{RabbitMqPublisherSettings, RabbitMqSourceSettings};
pub use settlement::RabbitMqSettlement;
pub use source::{RabbitMqDelivery, RabbitMqDeliverySource};
