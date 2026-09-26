#![allow(dead_code)]

use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use std::collections::BTreeMap;

use iggy::prelude::{
    AutoLogin, Client, ClientWrapper, Consumer, ConsumerGroupClient, ConsumerOffsetClient,
    Credentials, HeaderKey, HeaderValue as SdkHeaderValue, Identifier, IggyClient as RawIggyClient,
    IggyMessage, MessageClient, Partitioning, PollingStrategy, StreamClient, TcpClient,
    TcpClientConfig, TcpClientReconnectionConfig, TopicClient, TopicCreateOptions, UserClient,
    UserStatus,
};
use sisa_messaging::{
    ContentType, Delivery, EnvelopeMapper, MessageId, MessageType, Metadata,
    PartitionedLogDeliverySource, PartitionedLogReceive, SerializedEnvelope,
};
use sisa_messaging_iggy::{
    IggyClient, IggyClientSettings, IggyCredentials, IggyDelivery, IggyDeliveryError,
    IggyDeliverySource, IggyEnvelopeMapper, IggySettlement, IggySourceSettings,
};

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

/// Starts a crate client authenticated as the given user.
pub async fn new_iggy_client_as(
    username: &str,
    password: &str,
) -> Result<IggyClient, sisa_messaging_iggy::IggyClientError> {
    let settings = IggyClientSettings::new(
        server_address(),
        IggyCredentials::UsernamePassword {
            username: username.to_owned(),
            password: password.to_owned(),
        },
    )
    .with_connect_timeout(Duration::from_secs(10));

    IggyClient::start(settings).await
}

/// Parses a test resource name as an Iggy identifier.
pub fn identifier(name: &str) -> Identifier {
    Identifier::from_str_value(name).unwrap_or_else(|_| panic!("invalid test identifier"))
}

/// A throwaway topic and pre-provisioned consumer group in the test stream, with a raw client
/// for provisioning, publishing, and cursor inspection.
pub struct GroupTopic {
    pub raw: RawIggyClient,

    pub stream: String,

    pub topic: String,

    pub group: String,

    pub partitions: u32,

    /// Users created through [`GroupTopic::create_user`], removed by [`GroupTopic::delete`].
    users: std::sync::Mutex<Vec<String>>,
}

/// Runs `body` against a fresh group topic and removes the topic, group, and any users it
/// created even when the body panics; the body's own panic then resumes.
pub async fn with_group_topic<F, Fut>(partitions: u32, body: F)
where
    F: FnOnce(std::sync::Arc<GroupTopic>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let options = TopicCreateOptions {
        partitions_count: Some(partitions),
        ..TopicCreateOptions::default()
    };

    with_group_topic_options(options, body).await;
}

/// [`with_group_topic`] for a topic created with the given options.
pub async fn with_group_topic_options<F, Fut>(options: TopicCreateOptions, body: F)
where
    F: FnOnce(std::sync::Arc<GroupTopic>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let fixture = std::sync::Arc::new(GroupTopic::create_with(options).await);
    let outcome = tokio::spawn(body(std::sync::Arc::clone(&fixture))).await;

    fixture.delete().await;

    if let Err(error) = outcome {
        match error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_) => panic!("Iggy test body was cancelled"),
        }
    }
}

impl GroupTopic {
    /// Creates a unique topic with `partitions` partitions and a unique consumer group on it.
    pub async fn create(partitions: u32) -> Self {
        Self::create_with(TopicCreateOptions {
            partitions_count: Some(partitions),
            ..TopicCreateOptions::default()
        })
        .await
    }

    /// Creates a unique topic with the given options and a unique consumer group on it.
    pub async fn create_with(options: TopicCreateOptions) -> Self {
        let partitions = options.partitions_count.unwrap_or(1);
        let raw = new_raw_client().await;
        let stream = test_stream();
        let topic = unique_name("sisa-iggy-source-topic");
        let group = unique_name("sisa-iggy-source-group");
        let stream_id = identifier(&stream);

        if raw
            .get_stream(&stream_id)
            .await
            .unwrap_or_else(|error| panic!("Iggy test stream lookup failed: {error}"))
            .is_none()
        {
            let _ = raw.create_stream(&stream).await;
        }

        raw.create_topic(&stream_id, &topic, &options)
            .await
            .unwrap_or_else(|error| panic!("Iggy test topic creation failed: {error}"));

        raw.create_consumer_group(&stream_id, &identifier(&topic), &group)
            .await
            .unwrap_or_else(|error| panic!("Iggy test consumer group creation failed: {error}"));

        Self {
            raw,
            stream,
            topic,
            group,
            partitions,
            users: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Creates an active user without permissions; [`GroupTopic::delete`] removes it.
    pub async fn create_user(&self, username: &str, password: &str) {
        self.users
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(username.to_owned());

        self.raw
            .create_user(username, password, UserStatus::Active, None)
            .await
            .unwrap_or_else(|error| panic!("Iggy test user creation failed: {error}"));
    }

    pub fn settings(&self) -> IggySourceSettings {
        IggySourceSettings::new(
            identifier(&self.stream),
            identifier(&self.topic),
            identifier(&self.group),
        )
        .with_poll_interval(Duration::from_millis(50))
        .and_then(|settings| settings.with_assignment_refresh_interval(Duration::from_millis(200)))
        .and_then(|settings| settings.with_request_timeout(Duration::from_secs(5)))
        .unwrap_or_else(|_| panic!("test source settings are valid"))
    }

    /// Starts a dedicated client and an opened source for this group.
    pub async fn source(&self) -> (IggyClient, IggyDeliverySource) {
        self.source_with(self.settings()).await
    }

    pub async fn source_with(
        &self,
        settings: IggySourceSettings,
    ) -> (IggyClient, IggyDeliverySource) {
        let client = new_iggy_client().await;
        let mut source = IggyDeliverySource::new(client.clone(), settings);

        tokio::time::timeout(TEST_TIMEOUT, source.open())
            .await
            .unwrap_or_else(|_| panic!("Iggy test source open timed out"))
            .unwrap_or_else(|error| panic!("Iggy test source open failed: {error}"));

        (client, source)
    }

    /// Publishes `count` fresh envelopes to one partition and returns their ids in order.
    pub async fn publish(&self, partition: u32, count: usize) -> Vec<MessageId> {
        let ids: Vec<MessageId> = (0..count).map(|_| MessageId::new()).collect();
        let messages: Vec<IggyMessage> = ids.iter().map(|id| sdk_message(*id)).collect();

        self.publish_messages(partition, messages).await;

        ids
    }

    /// Publishes already-built SDK messages to one partition.
    pub async fn publish_messages(&self, partition: u32, mut messages: Vec<IggyMessage>) {
        self.raw
            .send_messages(
                &identifier(&self.stream),
                &identifier(&self.topic),
                &Partitioning::partition_id(partition),
                &mut messages,
            )
            .await
            .unwrap_or_else(|error| panic!("Iggy test publish failed: {error}"));
    }

    /// Reads the group's stored offset for a partition.
    pub async fn stored_offset(&self, partition: u32) -> Option<u64> {
        self.raw
            .get_consumer_offset(
                &Consumer::group(identifier(&self.group)),
                &identifier(&self.stream),
                &identifier(&self.topic),
                Some(partition),
            )
            .await
            .unwrap_or_else(|error| panic!("Iggy test offset read failed: {error}"))
            .map(|offset| offset.stored_offset)
    }

    /// Best-effort removal of the group, topic, and created users.
    pub async fn delete(&self) {
        let users = std::mem::take(
            &mut *self
                .users
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );

        for user in users {
            let _ = self.raw.delete_user(&identifier(&user)).await;
        }

        let stream = identifier(&self.stream);
        let topic = identifier(&self.topic);

        let _ = self
            .raw
            .delete_consumer_group(&stream, &topic, &identifier(&self.group))
            .await;

        let _ = self.raw.delete_topic(&stream, &topic).await;
    }
}

/// A fixture envelope with a fresh identity and a small payload.
pub fn envelope(message_id: MessageId) -> SerializedEnvelope {
    SerializedEnvelope {
        message_id,
        message_type: MessageType::new("iggy.source.probe").expect("fixture type is valid"),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream")
            .expect("fixture content type is valid"),
        payload: message_id.to_string().into_bytes(),
        metadata: Metadata::default(),
        ordering_key: None,
    }
}

/// Projects a fixture envelope into an SDK message the way the publisher does.
pub fn sdk_message(message_id: MessageId) -> IggyMessage {
    sdk_message_with(message_id, |_| {})
}

/// Projects a fixture envelope into an SDK message and lets the caller alter its headers.
pub fn sdk_message_with(
    message_id: MessageId,
    alter: impl FnOnce(&mut BTreeMap<HeaderKey, SdkHeaderValue>),
) -> IggyMessage {
    let record = IggyEnvelopeMapper
        .encode(&envelope(message_id))
        .unwrap_or_else(|_| panic!("fixture envelope encodes"));

    let mut headers: BTreeMap<HeaderKey, SdkHeaderValue> = record
        .headers
        .iter()
        .map(|header| {
            let name = HeaderKey::try_from(header.name.as_str())
                .unwrap_or_else(|_| panic!("fixture header name is valid"));

            let value = std::str::from_utf8(&header.value)
                .ok()
                .and_then(|value| SdkHeaderValue::try_from(value).ok())
                .unwrap_or_else(|| panic!("fixture header value is valid"));

            (name, value)
        })
        .collect();

    alter(&mut headers);

    IggyMessage::builder()
        .id(record.id)
        .payload(record.payload.into())
        .user_headers(headers)
        .build()
        .unwrap_or_else(|_| panic!("fixture message builds"))
}

/// One received record: its partition, offset, and decoded identity, plus its settlement.
pub struct Received {
    pub partition: u32,

    pub offset: u64,

    pub message_id: MessageId,

    pub settlement: IggySettlement,
}

/// Splits a delivery and decodes its identity through the crate's mapper.
pub fn received(delivery: IggyDelivery) -> Received {
    use sisa_messaging::PartitionedLogSettlement;

    let (record, settlement) = delivery.into_parts();

    let envelope = IggyEnvelopeMapper
        .decode(record)
        .unwrap_or_else(|error| panic!("received record decodes: {error}"));

    Received {
        partition: *settlement.partition(),
        offset: settlement.offset(),
        message_id: envelope.message_id,
        settlement,
    }
}

/// Waits for the next source event.
pub async fn next_event(
    source: &mut IggyDeliverySource,
) -> Result<PartitionedLogReceive<IggyDelivery, u32>, IggyDeliveryError> {
    tokio::time::timeout(TEST_TIMEOUT, source.receive())
        .await
        .unwrap_or_else(|_| panic!("Iggy test source receive timed out"))
}

/// Waits for the next delivery, failing on any other event.
pub async fn next_delivery(source: &mut IggyDeliverySource) -> Received {
    match next_event(source).await {
        Ok(PartitionedLogReceive::Delivery(delivery)) => received(delivery),
        Ok(PartitionedLogReceive::OwnershipLost(partition)) => {
            panic!("unexpected ownership loss for partition {partition}")
        }
        Ok(PartitionedLogReceive::Closed) => panic!("unexpected source close"),
        Ok(_) => panic!("unexpected source event"),
        Err(error) => panic!("unexpected source error: {error}"),
    }
}
