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
/// socket URL takes, as `unix:///run/redis.sock?pass=secret`. Redis URLs may carry a password
/// without a user, as `redis://:secret@host`.
fn without_password(url: &str) -> String {
    let (url, fragment) = url
        .split_once('#')
        .map_or((url, None), |(url, fragment)| (url, Some(fragment)));
    let mut shown = match url.split_once('?') {
        Some((base, query)) => format!(
            "{}?{}",
            without_user_password(base),
            without_pass_parameter(query)
        ),
        None => without_user_password(url),
    };
    if let Some(fragment) = fragment {
        shown.push('#');
        shown.push_str(fragment);
    }
    shown
}

/// The query with `***` for the value of each `pass` parameter, its name percent-decoded as the
/// client reads it.
fn without_pass_parameter(query: &str) -> String {
    query
        .split('&')
        .map(|pair| match pair.split_once('=') {
            Some((name, _)) if percent_decoded(name) == b"pass" => format!("{name}=***"),
            _ => pair.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn percent_decoded(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let escaped = (byte == b'%')
            .then(|| bytes.get(index + 1..index + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match escaped {
            Some(escaped) => {
                decoded.push(escaped);
                index += 3;
            }
            None => {
                decoded.push(if byte == b'+' { b' ' } else { byte });
                index += 1;
            }
        }
    }
    decoded
}

/// The URL, without its query, with `***` for the password of its user.
fn without_user_password(url: &str) -> String {
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
        assert_eq!(
            without_password("redis+unix:///run/redis.sock?user=billing&%70ass=s3cret#x"),
            "redis+unix:///run/redis.sock?user=billing&%70ass=***#x"
        );
    }

    #[test]
    fn a_url_without_a_password_is_shown_as_it_is() {
        assert_eq!(
            without_password("redis://localhost:6379"),
            "redis://localhost:6379"
        );
    }
}
