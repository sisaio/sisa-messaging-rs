//! Application-owned Iggy client and publisher configuration.

use std::fmt;
use std::time::Duration;

use crate::InvalidSendTimeout;

/// Iggy authentication credentials.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum IggyCredentials {
    /// Username and password login.
    UsernamePassword {
        /// Iggy user name.
        username: String,

        /// Iggy user password.
        password: String,
    },

    /// Personal access token login.
    PersonalAccessToken(String),
}

impl fmt::Debug for IggyCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UsernamePassword { username, .. } => formatter
                .debug_struct("UsernamePassword")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::PersonalAccessToken(_) => formatter
                .debug_tuple("PersonalAccessToken")
                .field(&"<redacted>")
                .finish(),
        }
    }
}

/// Optional TLS configuration for the Iggy TCP transport.
#[derive(Clone, Eq, PartialEq)]
pub struct IggyTlsSettings {
    pub(crate) domain: String,

    pub(crate) ca_file: Option<String>,
}

impl IggyTlsSettings {
    /// Creates TLS settings for the given server domain.
    #[must_use]
    pub fn new(domain: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
            ca_file: None,
        }
    }

    /// Sets a custom CA file path used to validate the server certificate.
    #[must_use]
    pub fn with_ca_file(mut self, ca_file: impl Into<String>) -> Self {
        self.ca_file = Some(ca_file.into());
        self
    }
}

impl fmt::Debug for IggyTlsSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IggyTlsSettings")
            .field("domain", &"<redacted>")
            .field("ca_file", &self.ca_file.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Typed Iggy client configuration.
#[derive(Clone)]
pub struct IggyClientSettings {
    pub(crate) server_address: String,

    pub(crate) credentials: IggyCredentials,

    pub(crate) tls: Option<IggyTlsSettings>,

    pub(crate) connect_timeout: Duration,

    pub(crate) heartbeat_interval: Duration,
}

impl IggyClientSettings {
    /// Creates configuration for the supplied server address and credentials.
    ///
    /// The default connect timeout is 10 seconds and the default heartbeat interval is 5 seconds,
    /// matching the SDK default. Server address, password, and personal access token values are
    /// redacted from `Debug` output.
    pub fn new(server_address: impl Into<String>, credentials: IggyCredentials) -> Self {
        Self {
            server_address: server_address.into(),
            credentials,
            tls: None,
            connect_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(5),
        }
    }

    /// Sets TLS configuration for the TCP transport.
    #[must_use]
    pub fn with_tls(mut self, tls: IggyTlsSettings) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Sets how long `start` waits for the connect-and-login sequence to complete.
    #[must_use]
    pub const fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    /// Sets the interval between client heartbeats sent to keep the session alive.
    #[must_use]
    pub const fn with_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.heartbeat_interval = heartbeat_interval;
        self
    }
}

impl fmt::Debug for IggyClientSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IggyClientSettings")
            .field("server_address", &"<redacted>")
            .field("credentials", &self.credentials)
            .field("tls", &self.tls)
            .field("connect_timeout", &self.connect_timeout)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .finish()
    }
}

/// Publication timing policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IggyPublisherSettings {
    send_timeout: Duration,
}

impl IggyPublisherSettings {
    /// Creates publication timing policy for the given per-request send timeout.
    ///
    /// A timeout does not establish that the request was rejected: it may still be committed by
    /// the server. See the crate documentation for the resulting duplicate-delivery behavior. A
    /// zero timeout is rejected: it would never let a request reach the server.
    pub fn new(send_timeout: Duration) -> Result<Self, InvalidSendTimeout> {
        if send_timeout.is_zero() {
            return Err(InvalidSendTimeout);
        }

        Ok(Self { send_timeout })
    }

    /// Returns how long to wait for the server's reply to a single `send_messages` request.
    #[must_use]
    pub const fn send_timeout(&self) -> Duration {
        self.send_timeout
    }
}

impl Default for IggyPublisherSettings {
    fn default() -> Self {
        Self {
            send_timeout: Duration::from_secs(30),
        }
    }
}
