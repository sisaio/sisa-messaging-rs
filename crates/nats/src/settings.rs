//! What the NATS transport is configured with.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The settings of the NATS transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct NatsSettings {
    /// The server to connect to, `nats://localhost:4222` by default. A comma-separated list
    /// names several servers of one cluster.
    pub url: String,
    /// The first subject token of every message, `sisa` by default: a message of type
    /// `orders.order_placed.v1` goes to `sisa.orders.order_placed.v1`. It keeps deployments
    /// that share a server apart, such as `prod` and `staging`.
    pub prefix: String,
    /// The JetStream stream that captures `{prefix}.>`, `SISA` by default. It is created when
    /// missing and updated to these settings when it exists.
    pub stream: String,
    /// How long the stream remembers a message id, so a second publish of the same id is
    /// dropped: two minutes by default, as in JetStream.
    pub duplicate_window: Duration,
    /// How long a consumer waits for a delivery to be settled before it delivers the message
    /// again: thirty seconds by default, as in JetStream.
    pub ack_wait: Duration,
}

impl Default for NatsSettings {
    fn default() -> Self {
        Self {
            url: "nats://localhost:4222".into(),
            prefix: "sisa".into(),
            stream: "SISA".into(),
            duplicate_window: Duration::from_secs(120),
            ack_wait: Duration::from_secs(30),
        }
    }
}
