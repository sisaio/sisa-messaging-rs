//! What the Iggy transport is configured with.

use std::fmt;
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
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
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

// Shows the settings with the password of the URL hidden, so they can go in logs.
impl fmt::Debug for IggySettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IggySettings")
            .field("url", &without_password(&self.url))
            .field("stream", &self.stream)
            .field("partitions", &self.partitions)
            .field("connect_timeout", &self.connect_timeout)
            .finish()
    }
}

/// The connection string with `***` for the password of its user, if it has one.
fn without_password(url: &str) -> String {
    let start = url.find("://").map_or(0, |scheme| scheme + 3);
    let authority_end = url[start..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |end| start + end);
    let Some(at) = url[start..authority_end].rfind('@').map(|at| start + at) else {
        return url.to_owned();
    };
    match url[start..at].find(':').map(|colon| start + colon) {
        Some(colon) => format!("{}:***{}", &url[..colon], &url[at..]),
        None => url.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{IggySettings, without_password};

    #[test]
    fn debug_hides_the_password_of_the_url() {
        let settings = IggySettings {
            url: "iggy://billing:s3cret@iggy.internal:8090?tls=true".into(),
            ..IggySettings::default()
        };
        let shown = format!("{settings:?}");
        assert!(!shown.contains("s3cret"), "{shown}");
        assert!(
            shown.contains("iggy://billing:***@iggy.internal:8090?tls=true"),
            "{shown}"
        );
    }

    #[test]
    fn a_url_without_a_password_is_shown_as_it_is() {
        assert_eq!(
            without_password("iggy://localhost:8090"),
            "iggy://localhost:8090"
        );
        assert_eq!(
            without_password("iggy://pat@localhost:8090"),
            "iggy://pat@localhost:8090"
        );
    }
}
