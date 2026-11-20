//! What the Redis transport is configured with.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The settings of the Redis transport.
///
/// Start from the defaults and change what differs, or read them from a configuration file with
/// serde, where a missing field keeps its default:
///
/// ```no_run
/// use sisa_messaging_redis::{RedisSettings, RedisTransport};
///
/// # async fn connect() -> Result<(), sisa_messaging_core::TransportError> {
/// let mut settings = RedisSettings::default();
/// settings.url = "redis://billing:secret@redis.internal:6379/0".into();
/// settings.prefix = "staging".into();
/// let transport = RedisTransport::connect(settings).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct RedisSettings {
    /// The server, and the user and database to connect with, as a Redis URL:
    /// `redis://localhost:6379` by default. Valkey and Dragonfly take the same URL.
    pub url: String,
    /// The start of every key the transport uses, `sisa` by default: a message of type
    /// `orders.order_placed.v1` goes to the stream `sisa:orders.order_placed.v1`. It keeps
    /// deployments that share a server apart, such as `prod` and `staging`.
    pub prefix: String,
    /// How long a delivery may stay unsettled before another instance of its endpoint takes it
    /// over: thirty seconds by default, as JetStream's ack wait. It is also how long a crashed
    /// instance's messages wait before they come again.
    pub ack_wait: Duration,
    /// How long connecting, and each command, may take before it fails, 10 seconds by default.
    pub connect_timeout: Duration,
}

// The defaults each field names.
impl Default for RedisSettings {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".into(),
            prefix: "sisa".into(),
            ack_wait: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

// Shows the settings with the password of the URL hidden, so they can go in logs.
impl fmt::Debug for RedisSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisSettings")
            .field("url", &without_password(&self.url))
            .field("prefix", &self.prefix)
            .field("ack_wait", &self.ack_wait)
            .field("connect_timeout", &self.connect_timeout)
            .finish()
    }
}

/// The URL with `***` for its password: the one of its user, and the `pass` parameter a Unix
/// socket URL takes, as `unix:///run/redis.sock?pass=secret`. It is read as the client reads it,
/// so no spelling the client takes for either slips by. A URL the client can't read is not shown.
fn without_password(url: &str) -> String {
    let Some(mut url) = redis::parse_redis_url(url) else {
        return "(a URL the Redis client can't read)".to_owned();
    };
    if url.password().is_some() {
        // A URL with a host takes a password; one without has none to hide here.
        let _ = url.set_password(Some("***"));
    }
    if url.query_pairs().any(|(name, _)| name == "pass") {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(name, value)| {
                let value = if name == "pass" { "***".into() } else { value };
                (name.into_owned(), value.into_owned())
            })
            .collect();
        url.query_pairs_mut().clear().extend_pairs(&pairs);
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::{RedisSettings, without_password};

    #[test]
    fn debug_hides_the_password_of_the_url() {
        let settings = RedisSettings {
            url: "redis://billing:s3cret@redis.internal:6379/2".into(),
            ..RedisSettings::default()
        };
        let shown = format!("{settings:?}");
        assert!(!shown.contains("s3cret"), "{shown}");
        assert!(
            shown.contains("redis://billing:***@redis.internal:6379/2"),
            "{shown}"
        );
    }

    #[test]
    fn a_password_without_a_user_is_hidden_too() {
        assert_eq!(
            without_password("redis://:s3cret@localhost:6379"),
            "redis://:***@localhost:6379"
        );
    }

    #[test]
    fn the_pass_parameter_of_a_unix_socket_url_is_hidden() {
        assert_eq!(
            without_password("unix:///run/redis.sock?db=2&pass=s3cret"),
            "unix:///run/redis.sock?db=2&pass=***"
        );
        // Spelled as the client still reads it: percent-encoded, or with a tab the URL drops.
        for url in [
            "redis+unix:///run/redis.sock?user=billing&%70ass=s3cret",
            "redis+unix:///run/redis.sock?user=billing&pa\tss=s3cret",
        ] {
            let shown = without_password(url);
            assert!(!shown.contains("s3cret"), "{shown}");
            assert!(shown.contains("billing"), "{shown}");
        }
    }

    #[test]
    fn a_url_the_client_cant_read_is_not_shown() {
        let shown = without_password("http://billing:s3cret@redis.internal");
        assert!(!shown.contains("s3cret"), "{shown}");
    }

    #[test]
    fn a_url_without_a_password_is_shown_as_it_is() {
        assert_eq!(
            without_password("redis://localhost:6379"),
            "redis://localhost:6379"
        );
    }
}
