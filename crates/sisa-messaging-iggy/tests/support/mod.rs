#![allow(dead_code)]

use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use iggy::prelude::{
    AutoLogin, Client, ClientWrapper, Consumer, Credentials, Identifier,
    IggyClient as RawIggyClient, MessageClient, PollingStrategy, StreamClient, TcpClient,
    TcpClientConfig, TcpClientReconnectionConfig, TopicClient, TopicCreateOptions,
};
use sisa_messaging::MessageId;
use sisa_messaging_iggy::{IggyClient, IggyClientSettings, IggyCredentials};

pub const SERVER_ADDRESS_ENV: &str = "SISA_IGGY_SERVER_ADDRESS";
pub const TEST_STREAM_ENV: &str = "SISA_IGGY_TEST_STREAM";
pub const TEST_TOPIC_ENV: &str = "SISA_IGGY_TEST_TOPIC";
pub const USERNAME_ENV: &str = "SISA_IGGY_USERNAME";
pub const PASSWORD_ENV: &str = "SISA_IGGY_PASSWORD";

const DEFAULT_SERVER_ADDRESS: &str = "127.0.0.1:8090";
// Default Iggy root credentials. Acceptable only inside these opt-in tests, run against a
// throwaway local broker.
const DEFAULT_USERNAME: &str = "iggy";
const DEFAULT_PASSWORD: &str = "iggy";
const DEFAULT_TEST_STREAM: &str = "sisa-iggy-test-stream";
const DEFAULT_TEST_TOPIC: &str = "sisa-iggy-test-topic";

pub const TEST_TIMEOUT: Duration = Duration::from_secs(30);

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

pub fn server_address() -> String {
    env_or(SERVER_ADDRESS_ENV, DEFAULT_SERVER_ADDRESS)
}

pub fn username() -> String {
    env_or(USERNAME_ENV, DEFAULT_USERNAME)
}

pub fn password() -> String {
    env_or(PASSWORD_ENV, DEFAULT_PASSWORD)
}

pub fn test_stream() -> String {
    env_or(TEST_STREAM_ENV, DEFAULT_TEST_STREAM)
}

pub fn test_topic() -> String {
    env_or(TEST_TOPIC_ENV, DEFAULT_TEST_TOPIC)
}

pub fn unique_name(prefix: &str) -> String {
    format!("{prefix}-{}", MessageId::new())
}

/// Starts the crate's publisher-facing client against the configured test broker.
pub async fn new_iggy_client() -> IggyClient {
    let settings = IggyClientSettings::new(
        server_address(),
        IggyCredentials::UsernamePassword {
            username: username(),
            password: password(),
        },
    )
    .with_connect_timeout(Duration::from_secs(10));

    IggyClient::start(settings)
        .await
        .unwrap_or_else(|error| panic!("Iggy test publisher client failed to connect: {error}"))
}

/// Starts a raw SDK client for test-side provisioning, polling, and offset/consumer-group
/// operations that the library intentionally does not expose (creation and inbound consumption
/// stay application-owned).
pub async fn new_raw_client() -> RawIggyClient {
    let config = TcpClientConfig {
        server_address: server_address(),
        auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
            username(),
            password().into(),
        )),
        reconnection: TcpClientReconnectionConfig {
            enabled: false,
            ..TcpClientReconnectionConfig::default()
        },
        ..TcpClientConfig::default()
    };

    let tcp_client = TcpClient::create(Arc::new(config))
        .unwrap_or_else(|error| panic!("Iggy raw test client configuration is invalid: {error}"));
    let client = RawIggyClient::create(ClientWrapper::Tcp(tcp_client), None, None);

    Client::connect(&client)
        .await
        .unwrap_or_else(|error| panic!("Iggy raw test client failed to connect: {error}"));

    client
}

/// Creates the stream and topic if they do not already exist. Provisioning is application-owned
/// and stays inside test support, never inside the library.
pub async fn provision_stream_and_topic(client: &RawIggyClient, stream: &str, topic: &str) {
    let stream_id =
        Identifier::from_str_value(stream).unwrap_or_else(|_| panic!("invalid test stream name"));

    if client
        .get_stream(&stream_id)
        .await
        .unwrap_or_else(|error| panic!("Iggy test stream lookup failed: {error}"))
        .is_none()
    {
        let _ = client.create_stream(stream).await;
    }

    let topic_id =
        Identifier::from_str_value(topic).unwrap_or_else(|_| panic!("invalid test topic name"));

    if client
        .get_topic(&stream_id, &topic_id)
        .await
        .unwrap_or_else(|error| panic!("Iggy test topic lookup failed: {error}"))
        .is_none()
    {
        let _ = client
            .create_topic(
                &stream_id,
                topic,
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    ..TopicCreateOptions::default()
                },
            )
            .await;
    }
}

/// Polls the topic from its start until `marker` is observed as a message payload, or the
/// deadline elapses.
pub async fn poll_for_marker(
    client: &RawIggyClient,
    stream: &str,
    topic: &str,
    marker: &[u8],
    timeout: Duration,
) -> bool {
    let stream_id = Identifier::from_str_value(stream).expect("valid test stream identifier");
    let topic_id = Identifier::from_str_value(topic).expect("valid test topic identifier");
    let consumer = Consumer::new(Identifier::numeric(1).expect("valid nominal consumer id"));
    let deadline = Instant::now() + timeout;
    let mut offset = 0_u64;

    while Instant::now() < deadline {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::offset(offset),
                100,
                false,
            )
            .await;

        match polled {
            Ok(polled) if !polled.messages.is_empty() => {
                for message in &polled.messages {
                    offset = offset.max(message.header.offset + 1);
                    if message.payload.as_ref() == marker {
                        return true;
                    }
                }
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    false
}
