//! The receiving half: a consumer group per endpoint on each topic it handles, read by a task of
//! its own that holds a partition while one of its messages waits for a retry.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use iggy::prelude::{
    Checkpoint, CommitPolicy, HeaderKind, IggyConsumer, IggyConsumerState, IggyError, IggyMessage,
    PollOptions, PollingStrategy, ReadError, ReceivedMessage,
};
use sisa_messaging_core::{
    Deliveries, Delivery, EndpointSpec, Envelope, TransportError, TransportSubscriber,
};
use tokio::sync::{Notify, mpsc};
use tokio::time::Instant;

use crate::IggyTransport;
use crate::transport::{CE, broker, fitted, header_key};

/// How long a long poll waits for messages before it asks again.
const POLL_WAIT: Duration = Duration::from_secs(1);

/// How long a partition waits before a message whose error copy failed is read again.
const SET_ASIDE_RETRY: Duration = Duration::from_secs(1);

impl TransportSubscriber for IggyTransport {
    type Delivery = IggyDelivery;

    /// Joins the endpoint's consumer group on the topic of each of its message types, creating
    /// the topics and the groups when they are missing, and streams their messages.
    ///
    /// A new group reads each partition from its first message, so a new endpoint also gets the
    /// messages already in its topics; after that, it resumes after the last committed offset. A
    /// message whose headers don't make an envelope goes to the error topic as it came, with the
    /// error in `ce_errorendpoint`, `ce_errortype` and `ce_errormessage`, and is then committed.
    async fn subscribe(
        &self,
        endpoint: &EndpointSpec,
    ) -> Result<Deliveries<IggyDelivery>, TransportError> {
        if endpoint.message_types.is_empty() {
            return Err(broker(
                "joining the Iggy consumer group",
                std::io::Error::other("an endpoint needs at least one message type"),
            ));
        }
        let (sender, mut receiver) = mpsc::channel(1);
        for message_type in &endpoint.message_types {
            let topic = message_type.as_str();
            self.ensure_topic(topic, self.settings().partitions).await?;
            let mut consumer = self
                .client()
                .consumer_group(endpoint.name.as_str(), self.stream_name(), topic)
                .map_err(|error| broker("joining the Iggy consumer group", error))?
                .create_consumer_group_if_not_exists()
                .polling_strategy(PollingStrategy::next())
                .poll_options(PollOptions {
                    max_wait: POLL_WAIT.into(),
                    commit: CommitPolicy::Manual,
                    ..PollOptions::default()
                })
                .build();
            consumer
                .init()
                .await
                .map_err(|error| broker("joining the Iggy consumer group", error))?;
            let shared = Arc::new(Shared {
                state: consumer.state(),
                partitions: Mutex::new(Partitions::default()),
                wake: Notify::new(),
            });
            tokio::spawn(read(
                consumer,
                shared,
                sender.clone(),
                self.clone(),
                endpoint.name.clone(),
            ));
        }
        drop(sender);

        let deliveries = futures_util::stream::poll_fn(move |context| receiver.poll_recv(context))
            .filter_map(|item| async move {
                match item {
                    Ok(delivery) if !delivery.is_current() => {
                        delivery.discard();
                        None
                    }
                    item => Some(item),
                }
            });
        Ok(Box::pin(deliveries))
    }

    /// Publishes the envelope to the endpoint's error topic, `_error.{endpoint}`, with one
    /// partition. Iggy keeps every message, so it stays next to the original of the same id. An
    /// attribute longer than the 255 bytes a header value takes is cut there.
    async fn publish_error(
        &self,
        endpoint: &str,
        envelope: &Envelope,
    ) -> Result<(), TransportError> {
        let mut headers = BTreeMap::new();
        for (name, value) in envelope.to_attributes() {
            headers.insert(header_key(&format!("{CE}{name}"))?, fitted(&value));
        }
        self.send_error(
            endpoint,
            envelope.id.as_uuid().as_u128(),
            headers,
            envelope.data.clone(),
        )
        .await
    }
}

/// What a consumer's task and its deliveries share.
struct Shared {
    /// Commits the offsets, without the consumer itself.
    state: IggyConsumerState,
    partitions: Mutex<Partitions>,
    /// Tells the task that a partition is held.
    wake: Notify,
}

impl Shared {
    fn partitions(&self) -> MutexGuard<'_, Partitions> {
        self.partitions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Holds the partition from `offset` on until `delay` has passed. Messages of the partition
    /// read before then are dropped unsettled, since they are read again after the hold.
    fn hold(&self, partition: u32, offset: u64, attempt: u32, delay: Duration) {
        let mut partitions = self.partitions();
        *partitions.generation.entry(partition).or_default() += 1;
        partitions.attempts.insert((partition, offset), attempt);
        // The messages from `offset` on come again, so they no longer wait for a commit.
        if let Some(open) = partitions.open.get_mut(&partition) {
            open.split_off(&offset);
        }
        partitions.merge_hold(partition, offset, Instant::now() + delay);
        drop(partitions);
        self.wake.notify_one();
    }

    /// Marks a message of the given generation settled, and returns the checkpoint to commit:
    /// the last of the settled messages that no unsettled one comes before, if any. A message
    /// read again since, or of a partition given up, returns nothing, since a later read of it
    /// commits instead.
    fn settle(&self, generation: u64, checkpoint: Checkpoint) -> Option<Checkpoint> {
        let mut partitions = self.partitions();
        let open = partitions.open.get_mut(&checkpoint.partition_id())?;
        match open.get_mut(&checkpoint.offset()) {
            Some((read_in, settled)) if *read_in == generation => *settled = Some(checkpoint),
            _ => return None,
        }
        let mut last = None;
        while let Some(entry) = open.first_entry() {
            match entry.get().1 {
                Some(settled) => {
                    last = Some(settled);
                    entry.remove();
                }
                None => break,
            }
        }
        last
    }

    /// Forgets a partition the group gave to another member, which reads it from the last
    /// committed offset.
    fn give_up(&self, partition: u32) {
        let mut partitions = self.partitions();
        *partitions.generation.entry(partition).or_default() += 1;
        partitions.held.remove(&partition);
        partitions.open.remove(&partition);
        partitions.attempts.retain(|(of, _), _| *of != partition);
    }

    /// Commits the checkpoint `settle` returned, if any.
    async fn commit(&self, checkpoint: Option<Checkpoint>) -> Result<(), TransportError> {
        match checkpoint {
            Some(checkpoint) => self
                .state
                .commit(checkpoint)
                .await
                .map_err(|error| broker("committing an Iggy offset", error)),
            None => Ok(()),
        }
    }
}

#[derive(Default)]
struct Partitions {
    /// Bumped by each hold, so deliveries read before it are dropped.
    generation: HashMap<u32, u64>,
    /// The offset each held partition reads again from, and when.
    held: HashMap<u32, (u64, Instant)>,
    /// The deliveries so far of each message that came back, by partition and offset.
    attempts: HashMap<(u32, u64), u32>,
    /// The messages of each partition read and not yet committed, by offset: the generation they
    /// were read in, and their checkpoint once settled. A checkpoint commits only once every
    /// earlier message is settled, so a commit never passes a message still to be retried.
    open: HashMap<u32, BTreeMap<u64, (u64, Option<Checkpoint>)>>,
}

impl Partitions {
    /// Holds the partition from `offset` on until `due`. A hold already there keeps the lower
    /// offset and the later deadline, so no retry comes before the delay it asked for.
    fn merge_hold(&mut self, partition: u32, offset: u64, due: Instant) {
        self.held
            .entry(partition)
            .and_modify(|held| *held = (held.0.min(offset), held.1.max(due)))
            .or_insert((offset, due));
    }
}

// Reads one consumer until the endpoint's deliveries are dropped or the consumer ends, then
// leaves the group.
async fn read(
    mut consumer: IggyConsumer,
    shared: Arc<Shared>,
    sender: mpsc::Sender<Result<IggyDelivery, TransportError>>,
    transport: IggyTransport,
    endpoint: String,
) {
    loop {
        let due = shared.partitions().held.values().map(|(_, due)| *due).min();
        let received = tokio::select! {
            () = sender.closed() => break,
            () = shared.wake.notified() => continue,
            () = tokio::time::sleep_until(due.unwrap_or_else(Instant::now)), if due.is_some() => {
                release(&mut consumer, &shared, &sender).await;
                continue;
            }
            received = consumer.next() => received,
        };
        let item = match received {
            None => break,
            Some(Err(error)) => match (error.partition_id, error.skip) {
                // A message that doesn't decode comes again until a seek moves past it.
                (Some(partition), Some(skip)) => {
                    match skip_unreadable(
                        &mut consumer,
                        &transport,
                        &endpoint,
                        partition,
                        skip,
                        &error,
                    )
                    .await
                    {
                        Ok(()) => continue,
                        Err(failure) => {
                            if sender.send(Err(failure)).await.is_err() {
                                break;
                            }
                            tokio::time::sleep(SET_ASIDE_RETRY).await;
                            continue;
                        }
                    }
                }
                _ => Err(broker("reading from Iggy", error.error)),
            },
            Some(Ok(received)) => match take(&shared, &received) {
                None => continue,
                Some((generation, attempt)) => match read_envelope(&received.message) {
                    Ok(envelope) => Ok(IggyDelivery {
                        envelope,
                        attempt,
                        partition: received.partition_id,
                        offset: received.message.header.offset,
                        checkpoint: received.checkpoint(),
                        generation,
                        shared: Arc::clone(&shared),
                        settled: false,
                    }),
                    Err(error) => {
                        match set_aside(
                            &transport, &endpoint, &shared, &received, generation, &error,
                        )
                        .await
                        {
                            Ok(()) => continue,
                            Err(error) => Err(error),
                        }
                    }
                },
            },
        };
        if sender.send(item).await.is_err() {
            break;
        }
    }
    // Manual commits store nothing on shutdown; leaving the group hands its partitions on.
    if let Err(error) = consumer.shutdown().await {
        tracing::warn!(endpoint, %error, "could not leave the Iggy consumer group; the server hands its partitions on once the session ends");
    }
}

// Copies a message that doesn't decode to the error topic, as the reply carried it, then reads
// on past it. It is never committed on its own; the next commit of its partition passes it.
async fn skip_unreadable(
    consumer: &mut IggyConsumer,
    transport: &IggyTransport,
    endpoint: &str,
    partition: u32,
    skip: PollingStrategy,
    error: &ReadError,
) -> Result<(), TransportError> {
    let message = error.error.to_string();
    let mut headers = BTreeMap::new();
    headers.insert(header_key(&format!("{CE}errorendpoint"))?, fitted(endpoint));
    headers.insert(
        header_key(&format!("{CE}errortype"))?,
        fitted("invalid_envelope"),
    );
    headers.insert(header_key(&format!("{CE}errormessage"))?, fitted(&message));
    // Iggy takes no empty data, so a message without its record carries the error.
    let data = error
        .record
        .clone()
        .filter(|record| !record.is_empty())
        .unwrap_or_else(|| Bytes::from(message.clone()));
    transport.send_error(endpoint, 0, headers, data).await?;
    consumer
        .seek(partition, skip)
        .await
        .map_err(|failure| broker("reading an Iggy partition past a message", failure))
}

// The generation and attempt of a message, or `None` when its partition is held.
fn take(shared: &Shared, received: &ReceivedMessage) -> Option<(u64, u32)> {
    let mut partitions = shared.partitions();
    let partition = received.partition_id;
    if partitions.held.contains_key(&partition) {
        return None;
    }
    let generation = partitions
        .generation
        .get(&partition)
        .copied()
        .unwrap_or_default();
    let attempt = partitions
        .attempts
        .get(&(partition, received.message.header.offset))
        .map_or(1, |attempts| attempts.saturating_add(1));
    partitions
        .open
        .entry(partition)
        .or_default()
        .insert(received.message.header.offset, (generation, None));
    Some((generation, attempt))
}

// Ends the holds that are due, reading each partition again from its held offset. A seek that
// fails is tried again later, unless the partition went to another member of the group.
async fn release(
    consumer: &mut IggyConsumer,
    shared: &Shared,
    sender: &mpsc::Sender<Result<IggyDelivery, TransportError>>,
) {
    let now = Instant::now();
    let due: Vec<(u32, u64)> = {
        let mut partitions = shared.partitions();
        let due: Vec<(u32, u64)> = partitions
            .held
            .iter()
            .filter(|(_, (_, at))| *at <= now)
            .map(|(partition, (offset, _))| (*partition, *offset))
            .collect();
        for (partition, _) in &due {
            partitions.held.remove(partition);
        }
        due
    };
    for (partition, offset) in due {
        match consumer
            .seek(partition, PollingStrategy::offset(offset))
            .await
        {
            Ok(()) => {}
            Err(IggyError::InvalidCommand) => shared.give_up(partition),
            Err(error) => {
                // A hold made while the seek waited stays, with the lower offset.
                shared
                    .partitions()
                    .merge_hold(partition, offset, Instant::now() + SET_ASIDE_RETRY);
                let _ = sender
                    .send(Err(broker("reading an Iggy partition again", error)))
                    .await;
            }
        }
    }
}

// The Kafka binding's header for the content type.
const CONTENT_TYPE: &str = "content-type";

// Reads the envelope from the `ce_` string headers and the payload. Header names are compared
// without case, so `Ce_id` from another client counts. Without `ce_datacontenttype`, the
// `content-type` header names the content type, as a Kafka-style producer writes it.
fn read_envelope(message: &IggyMessage) -> Result<Envelope, String> {
    let headers = message
        .user_headers_map()
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    let mut attributes = BTreeMap::new();
    let mut content_type = None;
    for (name, value) in &headers {
        let Ok(name) = name.as_str().map(str::to_ascii_lowercase) else {
            continue;
        };
        if name != CONTENT_TYPE && !name.starts_with(CE) {
            continue;
        }
        // A CloudEvents attribute of another type would otherwise go missing unnoticed.
        let text = match value.kind() {
            HeaderKind::String => value.as_str().map_err(|error| error.to_string())?,
            kind => return Err(format!("the header {name} is a {kind:?}, not a string")),
        };
        match name.strip_prefix(CE) {
            Some(attribute) => attributes.insert(attribute.to_owned(), text.to_owned()),
            None => content_type.replace(text.to_owned()),
        };
    }
    if let Some(content_type) = content_type {
        attributes
            .entry("datacontenttype".to_owned())
            .or_insert(content_type);
    }
    Envelope::from_attributes(attributes, message.payload.clone())
        .map_err(|error| error.to_string())
}

// Copies a message that isn't an envelope to the error topic as it came, then commits it. If the
// copy fails, the partition is held and the message read again, so it is never dropped without a
// copy. A failed commit means it comes again and is copied again.
async fn set_aside(
    transport: &IggyTransport,
    endpoint: &str,
    shared: &Shared,
    received: &ReceivedMessage,
    generation: u64,
    error: &str,
) -> Result<(), TransportError> {
    let mut headers = received
        .message
        .user_headers_map()
        .ok()
        .flatten()
        .unwrap_or_default();
    headers.insert(header_key(&format!("{CE}errorendpoint"))?, fitted(endpoint));
    headers.insert(
        header_key(&format!("{CE}errortype"))?,
        fitted("invalid_envelope"),
    );
    headers.insert(header_key(&format!("{CE}errormessage"))?, fitted(error));
    let partition = received.partition_id;
    let offset = received.message.header.offset;
    if let Err(failure) = transport
        .send_error(
            endpoint,
            received.message.header.id,
            headers,
            received.message.payload.clone(),
        )
        .await
    {
        let attempt = shared
            .partitions()
            .attempts
            .get(&(partition, offset))
            .map_or(1, |attempts| attempts.saturating_add(1));
        shared.hold(partition, offset, attempt, SET_ASIDE_RETRY);
        return Err(failure);
    }
    shared
        .commit(shared.settle(generation, received.checkpoint()))
        .await
}

/// A message received on an endpoint through its consumer group.
///
/// [`ack`](Delivery::ack) and [`reject`](Delivery::reject) commit its offset, and with it every
/// earlier offset of its partition. [`retry`](Delivery::retry) holds its partition and reads it
/// again from this message once the delay has passed; a delivery dropped without being settled
/// does the same at once.
pub struct IggyDelivery {
    envelope: Envelope,
    attempt: u32,
    partition: u32,
    offset: u64,
    checkpoint: Checkpoint,
    generation: u64,
    shared: Arc<Shared>,
    settled: bool,
}

impl fmt::Debug for IggyDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IggyDelivery")
            .field("envelope", &self.envelope)
            .field("attempt", &self.attempt)
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl IggyDelivery {
    // Whether no hold of its partition came after it was read.
    fn is_current(&self) -> bool {
        self.shared
            .partitions()
            .generation
            .get(&self.partition)
            .copied()
            .unwrap_or_default()
            == self.generation
    }

    // Drops a delivery read before a hold of its partition, which reads it again.
    fn discard(mut self) {
        self.settled = true;
    }

    async fn commit(mut self) -> Result<(), TransportError> {
        self.settled = true;
        self.shared
            .partitions()
            .attempts
            .remove(&(self.partition, self.offset));
        let checkpoint = self.shared.settle(self.generation, self.checkpoint);
        self.shared.commit(checkpoint).await
    }
}

impl Drop for IggyDelivery {
    fn drop(&mut self) {
        if !self.settled {
            self.shared
                .hold(self.partition, self.offset, self.attempt, Duration::ZERO);
        }
    }
}

impl Delivery for IggyDelivery {
    fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Commits the offset, and waits until the server has stored it.
    async fn ack(self) -> Result<(), TransportError> {
        self.commit().await
    }

    /// Holds the partition, so the message comes again once the delay has passed, before any
    /// later message of its partition.
    async fn retry(mut self, delay: Duration) -> Result<(), TransportError> {
        self.settled = true;
        self.shared
            .hold(self.partition, self.offset, self.attempt, delay);
        Ok(())
    }

    /// Commits the offset, so the endpoint never gets this message again.
    async fn reject(self) -> Result<(), TransportError> {
        self.commit().await
    }
}
