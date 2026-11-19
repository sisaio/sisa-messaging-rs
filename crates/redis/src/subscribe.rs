//! The receiving half: a consumer group per endpoint on the stream of each message type it
//! handles, read by a task of its own as a consumer named for this instance.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use redis::aio::ConnectionManager;
use redis::streams::{
    StreamClaimReply, StreamId, StreamInfoConsumersReply, StreamPendingCountReply, StreamReadReply,
};
use redis::{RedisError, Value};
use sisa_messaging_core::{
    Deliveries, Delivery, EndpointSpec, Envelope, TransportError, TransportSubscriber,
};
use tokio::sync::mpsc;
use tokio::time::Instant;
use uuid::Uuid;

use crate::RedisTransport;
use crate::transport::{CE, DATA, append, append_envelope, broker};

/// The longest a read blocks before the task looks for messages to take over.
const POLL_WAIT: Duration = Duration::from_secs(1);

/// How many pending entries of a stream one look reads.
const CLAIM_BATCH: usize = 100;

/// How long a consumer with nothing pending stays in its group without reading, before another
/// instance removes it. Each instance reads as a consumer of its own, so every restart leaves
/// one behind.
const IDLE_CONSUMER: Duration = Duration::from_secs(60 * 60);

/// How many looks for entries to take over come between two looks for idle consumers.
const LOOKS_PER_CLEAN_UP: u32 = 60;

// The Kafka binding's header for the content type.
const CONTENT_TYPE: &str = "content-type";

// Reads an endpoint's streams through its consumer group, and appends its error copies.
impl TransportSubscriber for RedisTransport {
    type Delivery = RedisDelivery;

    /// Creates the endpoint's consumer group, named after the endpoint, on the stream of each of
    /// its message types, creating the streams when they are missing, and reads them as a
    /// consumer of its own.
    ///
    /// A new group starts with the messages published after it is created. A delivery left
    /// unsettled for longer than [`ack_wait`](crate::RedisSettings::ack_wait), such as one of an
    /// instance that stopped, is taken over by any instance of the endpoint. A message whose
    /// fields don't make an envelope goes to the error stream as it came, with the error in
    /// `ce_errorendpoint`, `ce_errortype` and `ce_errormessage`, and is then acknowledged.
    async fn subscribe(
        &self,
        endpoint: &EndpointSpec,
    ) -> Result<Deliveries<RedisDelivery>, TransportError> {
        if endpoint.message_types.is_empty() {
            return Err(broker(
                "creating the Redis consumer group",
                std::io::Error::other("an endpoint needs at least one message type"),
            ));
        }
        let group = Arc::new(Group {
            transport: self.clone(),
            name: endpoint.name.clone(),
            consumer: Uuid::now_v7().to_string(),
            streams: endpoint
                .message_types
                .iter()
                .map(|message_type| self.stream(message_type))
                .collect(),
        });
        group.create().await?;
        let connection = self.blocking_connection(POLL_WAIT).await?;

        let (sender, mut receiver) = mpsc::channel(1);
        tokio::spawn(read(Arc::clone(&group), connection, sender));
        Ok(Box::pin(futures_util::stream::poll_fn(move |context| {
            receiver.poll_recv(context)
        })))
    }

    /// Appends the envelope to the endpoint's error stream, `{prefix}:_error:{endpoint}`.
    async fn publish_error(
        &self,
        endpoint: &str,
        envelope: &Envelope,
    ) -> Result<(), TransportError> {
        let command = append_envelope(&self.error_stream(endpoint), envelope);
        self.query::<String>("publishing to the Redis error stream", &command)
            .await
            .map(drop)
    }
}

/// An endpoint's consumer group and this instance's consumer in it, which its reading task and
/// its deliveries share.
struct Group {
    transport: RedisTransport,
    name: String,
    consumer: String,
    streams: Vec<String>,
}

/// An entry of another consumer, or of a retry, that is due to be taken over.
struct Due {
    stream: String,
    id: String,
    delivered: usize,
}

/// What the reading task keeps between reads.
struct Reading {
    due: VecDeque<Due>,
    next_look: Instant,
    looks: u32,
}

impl Group {
    fn ack_wait(&self) -> Duration {
        self.transport.settings().ack_wait
    }

    /// How often the task looks for entries to take over, which is also how close to its delay
    /// a retry comes back.
    fn look_interval(&self) -> Duration {
        (self.ack_wait() / 4).clamp(Duration::from_millis(10), POLL_WAIT)
    }

    /// Creates the group on each stream, and the streams that are missing. An existing group
    /// keeps its position.
    async fn create(&self) -> Result<(), TransportError> {
        for stream in &self.streams {
            let mut command = redis::cmd("XGROUP");
            command
                .arg("CREATE")
                .arg(stream)
                .arg(&self.name)
                .arg("$")
                .arg("MKSTREAM");
            match self
                .transport
                .query::<()>("creating the Redis consumer group", &command)
                .await
            {
                Ok(()) => {}
                Err(error) if code(&error) == Some("BUSYGROUP") => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// The pending entries of the group's streams that no consumer has touched for the ack wait.
    async fn look_for_due(&self) -> Result<VecDeque<Due>, TransportError> {
        let idle = millis(self.ack_wait());
        let mut due = VecDeque::new();
        for stream in &self.streams {
            let mut command = redis::cmd("XPENDING");
            command
                .arg(stream)
                .arg(&self.name)
                .arg("IDLE")
                .arg(idle)
                .arg("-")
                .arg("+")
                .arg(CLAIM_BATCH);
            let pending: StreamPendingCountReply = self
                .transport
                .query("reading the pending Redis entries", &command)
                .await?;
            due.extend(pending.ids.into_iter().map(|entry| Due {
                stream: stream.clone(),
                id: entry.id,
                delivered: entry.times_delivered,
            }));
        }
        Ok(due)
    }

    /// Takes an entry over for this consumer if it is still idle for the ack wait, so only one
    /// instance gets it. The server counts the delivery.
    async fn take_over(&self, due: &Due) -> Result<Option<StreamId>, TransportError> {
        let mut command = redis::cmd("XCLAIM");
        command
            .arg(&due.stream)
            .arg(&self.name)
            .arg(&self.consumer)
            .arg(millis(self.ack_wait()))
            .arg(&due.id);
        let claimed: StreamClaimReply = self
            .transport
            .query("taking over a Redis entry", &command)
            .await?;
        Ok(claimed.ids.into_iter().next())
    }

    /// Sets how long the server takes the entry to have been idle, keeping it for this consumer.
    /// Returns false when the entry is no longer pending, as after an ack.
    async fn set_idle(
        &self,
        stream: &str,
        id: &str,
        idle: Duration,
    ) -> Result<bool, TransportError> {
        let mut command = redis::cmd("XCLAIM");
        command
            .arg(stream)
            .arg(&self.name)
            .arg(&self.consumer)
            .arg(0)
            .arg(id)
            .arg("IDLE")
            .arg(millis(idle))
            .arg("JUSTID");
        let claimed: Vec<String> = self
            .transport
            .query("holding a Redis entry for a retry", &command)
            .await?;
        Ok(!claimed.is_empty())
    }

    async fn ack(&self, stream: &str, id: &str) -> Result<(), TransportError> {
        let mut command = redis::cmd("XACK");
        command.arg(stream).arg(&self.name).arg(id);
        self.transport
            .query::<()>("acknowledging a Redis entry", &command)
            .await
    }

    /// Removes the consumers of other instances that have nothing pending and haven't read for
    /// an hour. A failure only leaves them for the next time.
    async fn remove_idle_consumers(&self) {
        let idle = IDLE_CONSUMER.as_millis();
        for stream in &self.streams {
            let mut info = redis::cmd("XINFO");
            info.arg("CONSUMERS").arg(stream).arg(&self.name);
            let consumers: StreamInfoConsumersReply = match self
                .transport
                .query("reading the Redis consumers", &info)
                .await
            {
                Ok(consumers) => consumers,
                Err(error) => {
                    tracing::warn!(endpoint = self.name, %error, "could not read the Redis consumers");
                    continue;
                }
            };
            for consumer in consumers.consumers {
                if consumer.name == self.consumer
                    || consumer.pending > 0
                    || (consumer.idle as u128) < idle
                {
                    continue;
                }
                let mut remove = redis::cmd("XGROUP");
                remove
                    .arg("DELCONSUMER")
                    .arg(stream)
                    .arg(&self.name)
                    .arg(&consumer.name);
                if let Err(error) = self
                    .transport
                    .query::<()>("removing a Redis consumer", &remove)
                    .await
                {
                    tracing::warn!(endpoint = self.name, %error, "could not remove an idle Redis consumer");
                }
            }
        }
    }

    /// The next delivery: an entry due to be taken over first, then a new one. `None` when none
    /// came within the wait, or the entry went to the error stream.
    async fn next(
        self: &Arc<Self>,
        connection: &mut ConnectionManager,
        reading: &mut Reading,
    ) -> Result<Option<RedisDelivery>, TransportError> {
        if Instant::now() >= reading.next_look {
            reading.next_look = Instant::now() + self.look_interval();
            reading.looks = reading.looks.wrapping_add(1);
            if reading.looks.is_multiple_of(LOOKS_PER_CLEAN_UP) {
                self.remove_idle_consumers().await;
            }
            reading.due = self.look_for_due().await?;
        }
        while let Some(due) = reading.due.pop_front() {
            if let Some(entry) = self.take_over(&due).await? {
                let attempt = u32::try_from(due.delivered.saturating_add(1)).unwrap_or(u32::MAX);
                return self.deliver(due.stream, entry, attempt).await;
            }
        }

        // `BLOCK 0` would wait forever.
        let block = reading
            .next_look
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(1));
        let mut command = redis::cmd("XREADGROUP");
        command
            .arg("GROUP")
            .arg(&self.name)
            .arg(&self.consumer)
            .arg("COUNT")
            .arg(1)
            .arg("BLOCK")
            .arg(millis(block))
            .arg("STREAMS")
            .arg(&self.streams)
            .arg(vec![">"; self.streams.len()]);
        let read: Option<StreamReadReply> = match command.query_async(connection).await {
            Ok(read) => read,
            Err(error) => {
                // A stream deleted since gets its group back, starting with new messages.
                if error.code() == Some("NOGROUP") {
                    self.create().await?;
                }
                return Err(broker("reading from Redis", error));
            }
        };
        let Some(key) = read.and_then(|read| read.keys.into_iter().next()) else {
            return Ok(None);
        };
        match key.ids.into_iter().next() {
            Some(entry) => self.deliver(key.key, entry, 1).await,
            None => Ok(None),
        }
    }

    /// The delivery of an entry, or `None` once an entry that isn't an envelope is in the error
    /// stream and acknowledged. If the copy fails, the entry stays pending and comes again after
    /// the ack wait, so it is never dropped without a copy.
    async fn deliver(
        self: &Arc<Self>,
        stream: String,
        entry: StreamId,
        attempt: u32,
    ) -> Result<Option<RedisDelivery>, TransportError> {
        match read_envelope(&entry) {
            Ok(envelope) => Ok(Some(RedisDelivery {
                envelope,
                attempt,
                stream,
                id: entry.id,
                group: Arc::clone(self),
            })),
            Err(error) => {
                self.set_aside(&stream, &entry, &error).await?;
                Ok(None)
            }
        }
    }

    // Copies an entry that isn't an envelope to the error stream with its fields as they came,
    // then acknowledges it. A failed ack means it comes again and is copied again.
    async fn set_aside(
        &self,
        stream: &str,
        entry: &StreamId,
        error: &str,
    ) -> Result<(), TransportError> {
        let mut fields: Vec<(String, Vec<u8>)> = entry
            .map
            .iter()
            .filter(|(name, _)| name.as_str() != DATA)
            .filter_map(|(name, value)| Some((name.clone(), bytes_of(value)?)))
            .collect();
        for (attribute, value) in [
            ("errorendpoint", self.name.as_str()),
            ("errortype", "invalid_envelope"),
            ("errormessage", error),
        ] {
            fields.push((format!("{CE}{attribute}"), value.as_bytes().to_vec()));
        }
        let data = entry.map.get(DATA).and_then(bytes_of).unwrap_or_default();
        let command = append(
            &self.transport.error_stream(&self.name),
            fields
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_slice())),
            &data,
        );
        self.transport
            .query::<String>("publishing to the Redis error stream", &command)
            .await?;
        self.ack(stream, &entry.id).await
    }

    // Keeps a retried entry fresh for this consumer until `until`, so no instance takes it over
    // before its delay, then lets it age for the ack wait. A failed refresh only lets it come
    // back sooner.
    async fn hold(self: Arc<Self>, stream: String, id: String, until: Instant) {
        let refresh = (self.ack_wait() / 2).max(Duration::from_millis(10));
        while Instant::now() < until {
            tokio::time::sleep_until(until.min(Instant::now() + refresh)).await;
            match self.set_idle(&stream, &id, Duration::ZERO).await {
                Ok(true) => {}
                Ok(false) => return,
                Err(error) => {
                    tracing::warn!(endpoint = self.name, %error, "could not hold a Redis entry for its retry; it may come back early");
                }
            }
        }
    }
}

// Reads one delivery each time the endpoint has room for it, so none waits long in a buffer
// while its ack wait runs, until the endpoint's deliveries are dropped.
async fn read(
    group: Arc<Group>,
    mut connection: ConnectionManager,
    sender: mpsc::Sender<Result<RedisDelivery, TransportError>>,
) {
    let mut reading = Reading {
        due: VecDeque::new(),
        next_look: Instant::now(),
        looks: 0,
    };
    while let Ok(permit) = sender.reserve().await {
        match group.next(&mut connection, &mut reading).await {
            Ok(Some(delivery)) => permit.send(Ok(delivery)),
            Ok(None) => {}
            Err(error) => permit.send(Err(error)),
        }
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn code(error: &TransportError) -> Option<&str> {
    match error {
        TransportError::Broker { source, .. } => source.downcast_ref::<RedisError>()?.code(),
        _ => None,
    }
}

/// The bytes of a field's value, as the server sends them.
fn bytes_of(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::BulkString(bytes) => Some(bytes.clone()),
        Value::SimpleString(text) => Some(text.clone().into_bytes()),
        Value::Int(number) => Some(number.to_string().into_bytes()),
        _ => None,
    }
}

// Reads the envelope from the `ce_` fields and the `data` field. Field names are compared without
// case, so `Ce_id` from another client counts. Without `ce_datacontenttype`, the `content-type`
// field names the content type, as a Kafka-style producer writes it.
fn read_envelope(entry: &StreamId) -> Result<Envelope, String> {
    let mut attributes = BTreeMap::new();
    let mut content_type = None;
    let mut data = None;
    for (name, value) in &entry.map {
        if name == DATA {
            data = bytes_of(value);
            continue;
        }
        let name = name.to_ascii_lowercase();
        if name != CONTENT_TYPE && !name.starts_with(CE) {
            continue;
        }
        let text = bytes_of(value)
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .ok_or_else(|| format!("the field {name} isn't UTF-8 text"))?;
        match name.strip_prefix(CE) {
            Some(attribute) => attributes.insert(attribute.to_owned(), text),
            None => content_type.replace(text),
        };
    }
    if let Some(content_type) = content_type {
        attributes
            .entry("datacontenttype".to_owned())
            .or_insert(content_type);
    }
    let data = data.ok_or_else(|| format!("the entry has no {DATA} field"))?;
    Envelope::from_attributes(attributes, Bytes::from(data)).map_err(|error| error.to_string())
}

/// A message received on an endpoint through its consumer group.
///
/// [`ack`](Delivery::ack) and [`reject`](Delivery::reject) acknowledge the entry with `XACK`.
/// [`retry`](Delivery::retry) leaves it pending and sets its idle time, so any instance of the
/// endpoint takes it over once the delay has passed; a delay longer than the ack wait also keeps
/// the entry fresh until then, from a task of this instance. The server counts the deliveries,
/// so the attempt survives a restart. A delivery dropped unsettled comes again after the ack
/// wait.
pub struct RedisDelivery {
    envelope: Envelope,
    attempt: u32,
    stream: String,
    id: String,
    group: Arc<Group>,
}

// Shows the envelope and where the entry is, never the connection.
impl fmt::Debug for RedisDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisDelivery")
            .field("envelope", &self.envelope)
            .field("attempt", &self.attempt)
            .field("stream", &self.stream)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

// Settles the entry in its group: acknowledged, or left pending to come again.
impl Delivery for RedisDelivery {
    fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Acknowledges the entry, so the group never delivers it again.
    async fn ack(self) -> Result<(), TransportError> {
        self.group.ack(&self.stream, &self.id).await
    }

    /// Leaves the entry pending, so it comes again once the delay has passed.
    async fn retry(self, delay: Duration) -> Result<(), TransportError> {
        let ack_wait = self.group.ack_wait();
        match ack_wait.checked_sub(delay) {
            Some(idle) => self
                .group
                .set_idle(&self.stream, &self.id, idle)
                .await
                .map(drop),
            None => {
                if self
                    .group
                    .set_idle(&self.stream, &self.id, Duration::ZERO)
                    .await?
                {
                    let until = Instant::now() + (delay - ack_wait);
                    tokio::spawn(Arc::clone(&self.group).hold(self.stream, self.id, until));
                }
                Ok(())
            }
        }
    }

    /// Acknowledges the entry, so the group never delivers it again.
    async fn reject(self) -> Result<(), TransportError> {
        self.group.ack(&self.stream, &self.id).await
    }
}
