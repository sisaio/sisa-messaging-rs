//! What the Iggy transport is configured with.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The settings of the Iggy transport.
///
/// Start from the defaults and change what differs, or read them from a configuration file with
/// serde, where a missing field keeps its default:
///
/// ```no_run
/// use sisa_messaging_iggy::{IggySettings, IggyTransport};
///
/// # async fn connect() -> Result<(), sisa_messaging_core::TransportError> {
/// let mut settings = IggySettings::default();
/// settings.url = "iggy://billing:secret@iggy.internal:8090".into();
/// settings.stream = "staging".into();
/// let transport = IggyTransport::connect(settings).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct IggySettings {
    /// The server and the user to connect as, as an Iggy connection string over TCP:
    /// `iggy://iggy:iggy@localhost:8090` by default. The user needs the permission to manage
    /// streams and topics, which the transport creates when they are missing.
    pub url: String,
    /// The Iggy stream that holds every topic, `sisa` by default. It keeps deployments that share
    /// a server apart, such as `prod` and `staging`, and is created when missing.
    pub stream: String,
    /// How many partitions a topic the transport creates gets, 4 by default. An endpoint has at
    /// most this many instances busy on one topic at once. Topics that exist keep theirs.
    pub partitions: u32,
    /// How long connecting may take before it fails, 10 seconds by default.
    pub connect_timeout: Duration,
}

impl Default for IggySettings {
    fn default() -> Self {
        Self {
            url: "iggy://iggy:iggy@localhost:8090".into(),
            stream: "sisa".into(),
            partitions: 4,
            connect_timeout: Duration::from_secs(10),
        }
    }
}
