//! Fixed NATS wire header names shared by encoding and decoding.

pub(super) const NATS_MSG_ID: &str = "Nats-Msg-Id";
pub(super) const MESSAGE_ID: &str = "Sisa-Message-Id";
pub(super) const MESSAGE_TYPE: &str = "Sisa-Message-Type";
pub(super) const MESSAGE_VERSION: &str = "Sisa-Message-Version";
pub(super) const CONTENT_TYPE: &str = "Sisa-Content-Type";
pub(super) const ORDERING_KEY: &str = "Sisa-Ordering-Key";
pub(super) const CORRELATION_ID: &str = "Sisa-Correlation-Id";
pub(super) const CONVERSATION_ID: &str = "Sisa-Conversation-Id";
pub(super) const CAUSATION_ID: &str = "Sisa-Causation-Id";
pub(super) const REQUEST_ID: &str = "Sisa-Request-Id";
pub(super) const SOURCE: &str = "Sisa-Source";
pub(super) const DESTINATION: &str = "Sisa-Destination";
pub(super) const REPLY_TO: &str = "Sisa-Reply-To";
pub(super) const SENT_AT_MS: &str = "Sisa-Sent-At-Ms";
pub(super) const DEDUPLICATION_ID: &str = "Sisa-Deduplication-Id";
pub(super) const TENANT_ID: &str = "Sisa-Tenant-Id";
pub(super) const TRACEPARENT: &str = "Sisa-Traceparent";
pub(super) const TRACESTATE: &str = "Sisa-Tracestate";
pub(super) const CUSTOM_PREFIX: &str = "Sisa-Custom-";
pub(super) const CUSTOM_PREFIX_LOWERCASE: &str = "sisa-custom-";
