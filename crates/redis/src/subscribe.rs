//! The receiving half: a consumer group per endpoint on the stream of each message type it
//! handles, read by a task of its own as a consumer named for this instance.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use redis::aio::ConnectionManager;
use redis::streams::{StreamInfoConsumersReply, StreamPendingCountReply};
use redis::{RedisError, Value};
use sisa_messaging_core::{
    Deliveries, Delivery, EndpointSpec, Envelope, MessageId, TransportError, TransportSubscriber,
};
use tokio::sync::mpsc;
use tokio::time::Instant;

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

/// Claims an entry again, with no minimum idle time, only while this consumer still owns it, so
/// an instance that stalled can't take back an entry another one has taken over since. KEYS[1]
/// is the stream; ARGV is the group, the consumer, the id and `1` for `JUSTID`. Replies with
/// what `XCLAIM` does, or an empty list.
const CLAIM_OWN: &str = r"
if #redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1, ARGV[2]) == 0 then
  return {}
end
if ARGV[4] == '1' then
  return redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[3], 'JUSTID')
end
return redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[3])
";

/// Removes a consumer only while it has nothing pending, since its pending entries would become
/// unclaimable. KEYS[1] is the stream; ARGV is the group and the consumer.
const REMOVE_IDLE_CONSUMER: &str = r"
if #redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2]) > 0 then
  return 0
end
return redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])
";

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
            // A UUID version 7, from the one clock the crates read.
            consumer: MessageId::new().to_string(),
            streams: endpoint
                .message_types
                .iter()
                .map(|message_type| self.stream(message_type))
                .collect(),
            held: Mutex::new(Vec::new()),
        });
        group.create().await?;
        let connection = self.blocking_connection(POLL_WAIT).await?;

        // The endpoint asks for each delivery, and only then is an entry read, so none waits
        // read in a buffer while its ack wait runs.
        let (sender, mut receiver) = mpsc::channel(1);
        let (asks, asked) = mpsc::channel(1);
        tokio::spawn(read(Arc::clone(&group), connection, asked, sender));
        let mut asking = false;
        Ok(Box::pin(futures_util::stream::poll_fn(move |context| {
            if !asking {
                // Full only while an ask is waiting, and closed once the task stopped.
                let _ = asks.try_send(());
                asking = true;
            }
            let polled = receiver.poll_recv(context);
            if polled.is_ready() {
                asking = false;
            }
            polled
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
    /// The entries this instance holds for a retry, until their delay has passed.
    held: Mutex<Vec<Held>>,
}

/// An entry held for a retry: kept fresh, so no other instance takes it over, and delivered
/// again by this instance once it is due.
struct Held {
    stream: String,
    id: String,
    attempt: u32,
    due: Instant,
    refreshed: Instant,
}

/// An entry of another consumer, or of a retry, that is due to be taken over.
struct Due {
    stream: String,
    id: String,
    delivered: usize,
}

/// An entry as the server sends it. Field names are bytes: another client may write any.
struct Entry {
    id: String,
    fields: Vec<(Vec<u8>, Value)>,
}

impl Entry {
    fn field(&self, name: &[u8]) -> Option<&Value> {
        self.fields
            .iter()
            .find(|(field, _)| field.as_slice() == name)
            .map(|(_, value)| value)
    }
}

/// A new entry a read returned beyond the first, already pending for this consumer.
struct Ready {
    stream: String,
    entry: Entry,
    /// When its idle time was last reset, by the read or by a refresh.
    refreshed: Instant,
}

/// What the reading task keeps between reads.
struct Reading {
    due: VecDeque<Due>,
    /// A read over several streams returns up to one entry of each. They wait here, kept fresh
    /// like the held entries, so no other instance takes them over while they wait.
    ready: VecDeque<Ready>,
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
            let pending: StreamPendingCountReply = match self
                .transport
                .query("reading the pending Redis entries", &command)
                .await
            {
                Ok(pending) => pending,
                // A stream deleted since gets its group back, with nothing pending.
                Err(error) if code(&error) == Some("NOGROUP") => {
                    self.create().await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            due.extend(pending.ids.into_iter().map(|entry| Due {
                stream: stream.clone(),
                id: entry.id,
                delivered: entry.times_delivered,
            }));
        }
        Ok(due)
    }

    /// Takes an entry over for this consumer if it is still idle for the ack wait, so only one
    /// instance gets it, with its attempt. The server counts the delivery.
    async fn take_over(&self, due: &Due) -> Result<Option<(Entry, u32)>, TransportError> {
        let mut command = redis::cmd("XCLAIM");
        command
            .arg(&due.stream)
            .arg(&self.name)
            .arg(&self.consumer)
            .arg(millis(self.ack_wait()))
            .arg(&due.id);
        let claimed: Value = self
            .transport
            .query("taking over a Redis entry", &command)
            .await?;
        let Some(entry) = entries_of(claimed).into_iter().next() else {
            return Ok(None);
        };
        Ok(Some((entry, self.delivered(due).await)))
    }

    /// How many times the server has delivered an entry this consumer just took over, which is
    /// its attempt: the count of the look that found it is behind if another instance took it
    /// over in between. Freshly claimed, no other instance takes it over before this reads.
    async fn delivered(&self, due: &Due) -> u32 {
        let mut command = redis::cmd("XPENDING");
        command
            .arg(&due.stream)
            .arg(&self.name)
            .arg(&due.id)
            .arg(&due.id)
            .arg(1)
            .arg(&self.consumer);
        let delivered = match self
            .transport
            .query::<StreamPendingCountReply>("reading a Redis entry's deliveries", &command)
            .await
        {
            Ok(pending) => pending.ids.first().map(|entry| entry.times_delivered),
            Err(error) => {
                tracing::warn!(endpoint = self.name, %error, "could not read a Redis entry's deliveries; its attempt may be low");
                None
            }
        };
        let delivered = delivered.unwrap_or(due.delivered.saturating_add(1));
        u32::try_from(delivered).unwrap_or(u32::MAX)
    }

    /// Claims an entry of this consumer again, which resets its idle time. With `count`, the
    /// server counts a delivery and the entry comes back; without, only the id. `None` when the
    /// entry is no longer this consumer's: acked, or taken over by another instance.
    async fn claim_own(
        &self,
        stream: &str,
        id: &str,
        count: bool,
    ) -> Result<Option<Entry>, TransportError> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(CLAIM_OWN)
            .arg(1)
            .arg(stream)
            .arg(&self.name)
            .arg(&self.consumer)
            .arg(id)
            .arg(if count { "0" } else { "1" });
        if count {
            let claimed: Value = self
                .transport
                .query("delivering a Redis entry again", &command)
                .await?;
            return Ok(entries_of(claimed).into_iter().next());
        }
        let claimed: Vec<String> = self
            .transport
            .query("holding a Redis entry for a retry", &command)
            .await?;
        Ok(claimed.into_iter().next().map(|claimed_id| Entry {
            id: claimed_id,
            fields: Vec::new(),
        }))
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Vec<Held>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Refreshes the held entries that have waited half the ack wait since their last refresh,
    /// and forgets those no longer pending. A failed refresh only lets another instance take the
    /// entry over sooner.
    async fn refresh_held(&self) {
        let now = Instant::now();
        let refresh = self.ack_wait() / 2;
        let stale: Vec<(String, String)> = self
            .held()
            .iter_mut()
            .filter(|held| held.refreshed + refresh <= now)
            .map(|held| {
                held.refreshed = now;
                (held.stream.clone(), held.id.clone())
            })
            .collect();
        for (stream, id) in stale {
            match self.claim_own(&stream, &id, false).await {
                Ok(Some(_)) => {}
                Ok(None) => self
                    .held()
                    .retain(|held| held.id != id || held.stream != stream),
                Err(error) => {
                    tracing::warn!(endpoint = self.name, %error, "could not hold a Redis entry for its retry; another instance may take it over early");
                }
            }
        }
    }

    /// Refreshes the read entries that have waited half the ack wait, and forgets those another
    /// instance has taken over, as after this one stalled.
    async fn refresh_ready(&self, reading: &mut Reading) {
        let now = Instant::now();
        let refresh = self.ack_wait() / 2;
        let mut kept = VecDeque::with_capacity(reading.ready.len());
        while let Some(mut ready) = reading.ready.pop_front() {
            if ready.refreshed + refresh <= now {
                match self.claim_own(&ready.stream, &ready.entry.id, false).await {
                    Ok(Some(_)) => ready.refreshed = now,
                    Ok(None) => continue,
                    Err(error) => {
                        tracing::warn!(endpoint = self.name, %error, "could not keep a read Redis entry; another instance may take it over");
                    }
                }
            }
            kept.push_back(ready);
        }
        reading.ready = kept;
    }

    /// Looks for entries to take over once the look interval has passed, and for idle consumers
    /// every so many looks. A look also gets a deleted stream its group back.
    async fn look(&self, reading: &mut Reading) -> Result<(), TransportError> {
        if Instant::now() < reading.next_look {
            return Ok(());
        }
        reading.next_look = Instant::now() + self.look_interval();
        reading.looks = reading.looks.wrapping_add(1);
        if reading.looks.is_multiple_of(LOOKS_PER_CLEAN_UP) {
            self.remove_idle_consumers().await;
        }
        reading.due = self.look_for_due().await?;
        Ok(())
    }

    /// What the task does while the endpoint is busy: keeps the held and the read entries fresh,
    /// and looks, so a deleted stream gets its group back before new messages go by it.
    async fn tick(&self, reading: &mut Reading) {
        self.refresh(reading).await;
        if let Err(error) = self.look(reading).await {
            tracing::warn!(endpoint = self.name, %error, "could not look for Redis entries to take over");
        }
    }

    /// Keeps the held and the read entries fresh.
    async fn refresh(&self, reading: &mut Reading) {
        self.refresh_held().await;
        self.refresh_ready(reading).await;
    }

    /// The next read entry still this consumer's. One that waited is claimed again first, which
    /// checks that, and gives its handler the whole ack wait.
    async fn take_ready(
        &self,
        reading: &mut Reading,
    ) -> Result<Option<(String, Entry)>, TransportError> {
        while let Some(ready) = reading.ready.pop_front() {
            if ready.refreshed.elapsed() < self.look_interval()
                || self
                    .claim_own(&ready.stream, &ready.entry.id, false)
                    .await?
                    .is_some()
            {
                return Ok(Some((ready.stream, ready.entry)));
            }
        }
        Ok(None)
    }

    /// Takes out one held entry whose delay has passed, if any.
    fn take_due(&self) -> Option<Held> {
        let now = Instant::now();
        let mut held = self.held();
        let index = held.iter().position(|held| held.due <= now)?;
        Some(held.swap_remove(index))
    }

    /// When the reading task next has to act on a held entry.
    fn next_held_event(&self) -> Option<Instant> {
        let refresh = self.ack_wait() / 2;
        self.held()
            .iter()
            .map(|held| held.due.min(held.refreshed + refresh))
            .min()
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
                // It may have read since the look, so the pending check is made again.
                let mut remove = redis::cmd("EVAL");
                remove
                    .arg(REMOVE_IDLE_CONSUMER)
                    .arg(1)
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
        self.refresh(reading).await;
        if let Some((stream, entry)) = self.take_ready(reading).await? {
            return self.deliver(stream, entry, 1).await;
        }
        while let Some(held) = self.take_due() {
            if let Some(entry) = self.claim_own(&held.stream, &held.id, true).await? {
                return self.deliver(held.stream, entry, held.attempt).await;
            }
        }
        self.look(reading).await?;
        while let Some(due) = reading.due.pop_front() {
            if let Some((entry, attempt)) = self.take_over(&due).await? {
                return self.deliver(due.stream, entry, attempt).await;
            }
        }

        // `BLOCK 0` would wait forever.
        let wake = self
            .next_held_event()
            .map_or(reading.next_look, |held| held.min(reading.next_look));
        let block = wake
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
        // The response timeout covers the reply; this covers the reconnecting before it too.
        let read: Value = match tokio::time::timeout(
            self.transport.settings().connect_timeout + block,
            command.query_async(connection),
        )
        .await
        .map_err(|elapsed| broker("reading from Redis", elapsed))?
        {
            Ok(read) => read,
            Err(error) => {
                // A stream deleted since gets its group back, starting with new messages.
                if error.code() == Some("NOGROUP") {
                    self.create().await?;
                }
                return Err(broker("reading from Redis", error));
            }
        };
        let now = Instant::now();
        reading
            .ready
            .extend(read_of(read).into_iter().map(|(stream, entry)| Ready {
                stream,
                entry,
                refreshed: now,
            }));
        match reading.ready.pop_front() {
            Some(ready) => self.deliver(ready.stream, ready.entry, 1).await,
            None => Ok(None),
        }
    }

    /// The delivery of an entry, or `None` once an entry that isn't an envelope is in the error
    /// stream and acknowledged. If the copy fails, the entry stays pending and comes again after
    /// the ack wait, so it is never dropped without a copy.
    async fn deliver(
        self: &Arc<Self>,
        stream: String,
        entry: Entry,
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
        entry: &Entry,
        error: &str,
    ) -> Result<(), TransportError> {
        let mut fields: Vec<(Vec<u8>, Vec<u8>)> = entry
            .fields
            .iter()
            .filter(|(name, _)| name.as_slice() != DATA.as_bytes())
            .filter_map(|(name, value)| Some((name.clone(), bytes_of(value)?)))
            .collect();
        for (attribute, value) in [
            ("errorendpoint", self.name.as_str()),
            ("errortype", "invalid_envelope"),
            ("errormessage", error),
        ] {
            fields.push((
                format!("{CE}{attribute}").into_bytes(),
                value.as_bytes().to_vec(),
            ));
        }
        let data = entry
            .field(DATA.as_bytes())
            .and_then(bytes_of)
            .unwrap_or_default();
        let command = append(
            &self.transport.error_stream(&self.name),
            fields
                .iter()
                .map(|(name, value)| (name.as_slice(), value.as_slice())),
            &data,
        );
        self.transport
            .query::<String>("publishing to the Redis error stream", &command)
            .await?;
        self.ack(stream, &entry.id).await
    }
}

// Reads one delivery each time the endpoint asks for one, until the endpoint's deliveries are
// dropped. While the endpoint is busy with a delivery, the entries held for a retry and those a
// read returned beyond the first are still kept fresh.
async fn read(
    group: Arc<Group>,
    mut connection: ConnectionManager,
    mut asked: mpsc::Receiver<()>,
    sender: mpsc::Sender<Result<RedisDelivery, TransportError>>,
) {
    let mut reading = Reading {
        due: VecDeque::new(),
        ready: VecDeque::new(),
        next_look: Instant::now(),
        looks: 0,
    };
    // Dropping the deliveries stops the work in progress too: a read, a reconnect or a refresh.
    'asks: loop {
        tokio::select! {
            ask = asked.recv() => if ask.is_none() {
                break;
            },
            () = tokio::time::sleep(group.look_interval()) => {
                tokio::select! {
                    () = group.tick(&mut reading) => {}
                    () = sender.closed() => break,
                }
                continue;
            }
        }
        loop {
            let next = tokio::select! {
                next = group.next(&mut connection, &mut reading) => next,
                () = sender.closed() => break 'asks,
            };
            let delivery = match next {
                Ok(Some(delivery)) => Ok(delivery),
                Ok(None) => continue,
                Err(error) => Err(error),
            };
            if sender.send(delivery).await.is_err() {
                break 'asks;
            }
            break;
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

/// The entries of a read, with the stream of each: an array, or a map in RESP3, of each stream's
/// name and entries. Nothing for a read that timed out.
fn read_of(reply: Value) -> Vec<(String, Entry)> {
    let streams = match reply {
        Value::Array(streams) => streams
            .into_iter()
            .filter_map(|stream| match stream {
                Value::Array(pair) => {
                    let mut pair = pair.into_iter();
                    Some((pair.next()?, pair.next()?))
                }
                _ => None,
            })
            .collect(),
        Value::Map(streams) => streams,
        _ => Vec::new(),
    };
    streams
        .into_iter()
        .filter_map(|(name, entries)| Some((String::from_utf8(bytes_of(&name)?).ok()?, entries)))
        .flat_map(|(name, entries)| {
            entries_of(entries)
                .into_iter()
                .map(move |entry| (name.clone(), entry))
        })
        .collect()
}

/// The entries of an array, as `XREADGROUP` and `XCLAIM` reply with: each the id and the fields,
/// or nil for one deleted since.
fn entries_of(reply: Value) -> Vec<Entry> {
    let Value::Array(entries) = reply else {
        return Vec::new();
    };
    entries.into_iter().filter_map(entry_of).collect()
}

fn entry_of(entry: Value) -> Option<Entry> {
    let Value::Array(entry) = entry else {
        return None;
    };
    let mut entry = entry.into_iter();
    let id = String::from_utf8(bytes_of(&entry.next()?)?).ok()?;
    let fields = match entry.next()? {
        Value::Array(flat) => {
            let mut flat = flat.into_iter();
            let mut fields = Vec::new();
            while let (Some(name), Some(value)) = (flat.next(), flat.next()) {
                fields.push((bytes_of(&name)?, value));
            }
            fields
        }
        Value::Map(pairs) => pairs
            .into_iter()
            .map(|(name, value)| Some((bytes_of(&name)?, value)))
            .collect::<Option<_>>()?,
        _ => return None,
    };
    Some(Entry { id, fields })
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
fn read_envelope(entry: &Entry) -> Result<Envelope, String> {
    let mut attributes = BTreeMap::new();
    let mut content_type = None;
    let mut data = None;
    for (name, value) in &entry.fields {
        if name.as_slice() == DATA.as_bytes() {
            data = bytes_of(value);
            continue;
        }
        let name = name.to_ascii_lowercase();
        if name.as_slice() != CONTENT_TYPE.as_bytes() && !name.starts_with(CE.as_bytes()) {
            continue;
        }
        let name = String::from_utf8(name).map_err(|name| {
            format!(
                "the field name {} isn't UTF-8 text",
                String::from_utf8_lossy(name.as_bytes())
            )
        })?;
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
/// [`retry`](Delivery::retry) leaves it pending, and this instance holds it: it claims the entry
/// again every half ack wait so no other instance takes it over, and delivers it again once the
/// delay has passed. If the instance stops first, another takes the entry over after the ack wait.
/// The server counts the deliveries it takes over, so the attempt survives a restart. A delivery
/// dropped unsettled comes again after the ack wait.
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

    /// Leaves the entry pending and holds it, so this instance delivers it again once the delay
    /// has passed.
    async fn retry(self, delay: Duration) -> Result<(), TransportError> {
        // Resets its idle time, and finds out whether it is still this consumer's.
        if self
            .group
            .claim_own(&self.stream, &self.id, false)
            .await?
            .is_some()
        {
            let now = Instant::now();
            self.group.held().push(Held {
                stream: self.stream,
                id: self.id,
                attempt: self.attempt.saturating_add(1),
                due: now + delay,
                refreshed: now,
            });
        }
        Ok(())
    }

    /// Acknowledges the entry, so the group never delivers it again.
    async fn reject(self) -> Result<(), TransportError> {
        self.group.ack(&self.stream, &self.id).await
    }
}
