//! Cloneable handle to an application-configured Apache Iggy client.

use std::fmt;
use std::sync::Arc;

use iggy::prelude::{
    AutoLogin, Client, ClientWrapper, Credentials as SdkCredentials, IggyClient as SdkIggyClient,
    NonZeroIggyDuration, TcpClient, TcpClientConfig, TcpClientConfigBuilder,
    TcpClientReconnectionConfig,
};

use crate::{IggyClientError, IggyClientErrorKind, IggyClientSettings, IggyCredentials};

/// Cloneable handle to an application-configured, connected Apache Iggy client.
#[derive(Clone)]
pub struct IggyClient {
    inner: Arc<SdkIggyClient>,
}

impl IggyClient {
    /// Connects over TCP and logs in within the configured connect timeout.
    ///
    /// The underlying SDK client is built with reconnection disabled, so a lost connection is not
    /// redialed automatically: construct a new [`IggyClient`] to reconnect. Auto-login happens
    /// only during this call. This does not check topic existence or broker-side authorization
    /// beyond the login exchange; those conditions are observed by subsequent publish operations.
    pub async fn start(settings: IggyClientSettings) -> Result<Self, IggyClientError> {
        validate(&settings)?;

        let heartbeat_interval = NonZeroIggyDuration::new(settings.heartbeat_interval)
            .map_err(|_| IggyClientError::new(IggyClientErrorKind::InvalidSettings))?;

        let mut config = TcpClientConfig {
            server_address: settings.server_address.clone(),
            auto_login: AutoLogin::Enabled(to_sdk_credentials(&settings.credentials)),
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            heartbeat_interval,
            ..TcpClientConfig::default()
        };

        if let Some(tls) = &settings.tls {
            config.tls_enabled = true;
            config.tls_domain = tls.domain.clone();
            config.tls_ca_file = tls.ca_file.clone();
        }

        let tcp_client = TcpClient::create(Arc::new(config))
            .map_err(|_| IggyClientError::new(IggyClientErrorKind::Connect))?;

        let sdk_client = SdkIggyClient::create(ClientWrapper::Tcp(tcp_client), None, None);

        match tokio::time::timeout(settings.connect_timeout, sdk_client.connect()).await {
            Ok(Ok(())) => Ok(Self {
                inner: Arc::new(sdk_client),
            }),
            Ok(Err(error)) => Err(IggyClientError::from(error)),
            Err(_elapsed) => Err(IggyClientError::new(IggyClientErrorKind::Timeout)),
        }
    }

    pub(crate) fn sdk_client(&self) -> &SdkIggyClient {
        &self.inner
    }
}

impl fmt::Debug for IggyClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IggyClient")
            .field("client", &"<redacted>")
            .finish()
    }
}

fn to_sdk_credentials(credentials: &IggyCredentials) -> SdkCredentials {
    match credentials {
        IggyCredentials::UsernamePassword { username, password } => {
            SdkCredentials::UsernamePassword(username.clone(), password.clone().into())
        }
        IggyCredentials::PersonalAccessToken(token) => {
            SdkCredentials::PersonalAccessToken(token.clone().into())
        }
    }
}

fn validate(settings: &IggyClientSettings) -> Result<(), IggyClientError> {
    if settings.server_address.trim().is_empty() {
        return Err(IggyClientError::new(IggyClientErrorKind::InvalidSettings));
    }

    match &settings.credentials {
        IggyCredentials::UsernamePassword { username, password } => {
            if username.trim().is_empty() || password.is_empty() {
                return Err(IggyClientError::new(IggyClientErrorKind::InvalidSettings));
            }
        }
        IggyCredentials::PersonalAccessToken(token) => {
            if token.is_empty() {
                return Err(IggyClientError::new(IggyClientErrorKind::InvalidSettings));
            }
        }
    }

    if let Some(tls) = &settings.tls
        && tls.domain.trim().is_empty()
    {
        return Err(IggyClientError::new(IggyClientErrorKind::InvalidSettings));
    }

    if settings.connect_timeout.is_zero() {
        return Err(IggyClientError::new(IggyClientErrorKind::InvalidSettings));
    }

    // Reuse the SDK's own address grammar rather than duplicating it: a builder constructed only
    // to validate the address, discarding the config it would otherwise produce.
    if TcpClientConfigBuilder::new()
        .with_server_address(settings.server_address.clone())
        .build()
        .is_err()
    {
        return Err(IggyClientError::new(IggyClientErrorKind::InvalidSettings));
    }

    Ok(())
}
