//! Replay-only consumer-group delivery source over the SDK's low-level client.
//!
//! The source polls each owned partition explicitly with the SDK's `poll_messages`, never the
//! SDK's `IggyConsumer`, and always with the SDK's auto-commit disabled, so a poll never moves the
//! group's cursor. A settlement stores its own record's offset with `store_consumer_offset` after
//! the consumer resolved that record, and nothing else writes the cursor. See the crate
//! documentation for the replay-only guarantees and the rebalance window.

mod settings;
mod wire;

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use iggy::binary::BinaryTransport;
use iggy::prelude::locking::IggyRwLockFn;
use iggy::prelude::{
    ClientWrapper, Consumer as SdkConsumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier,
    IggyError, IggyMessage, MessageClient, Partition, PollingStrategy, StreamClient, SystemClient,
    TopicClient,
};
use iggy_common::ClientState;
use sisa_messaging::{
    Delivery, FailureKind, PartitionAdvance, PartitionedLogDeliverySource, PartitionedLogReceive,
    PartitionedLogSettlement,
};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::{IggyClient, IggyDeliveryError, IggyDeliveryErrorKind, IggyRecord};

pub use settings::IggySourceSettings;

/// A replay-only partitioned-log delivery source for one pre-provisioned Iggy consumer group.
///
/// Give each source its own [`IggyClient`]: group membership belongs to the client's session.
/// Opening looks up the stream, topic, and group and joins the group; it never creates them. The
/// source never leaves the group: the server removes the member when the application shuts the
/// client down or its session ends. After [`IggyClient::shutdown`], `receive` reports a clean
/// close; a lost session, a timed-out request, or a server failure is a classified error.
///
/// Partition ownership is observed from the server's own fence: a partition is owned while its
/// explicit polls are answered and lost when a poll is fenced. Every assignment refresh rechecks
/// membership (rejoining after a removal), reloads the topic's partitions, and probes each
/// partition the source does not own; a fence on an owned partition triggers a refresh at once.
pub struct IggyDeliverySource {
    context: Arc<Context>,

    group: Identifier,

    batch_length: u32,

    poll_interval: Duration,

    assignment_refresh_interval: Duration,

    opened: Option<Resolved>,

    /// Known partitions in ascending id order.
    slots: Vec<Slot>,

    /// Partitions whose unresolved record was withdrawn, in the order they are reported.
    withdrawn: VecDeque<u32>,

    /// Slot indexes that may hold a deliverable record; each is revalidated when taken.
    ready: VecDeque<usize>,

    /// Settlement outcomes moved out of the shared queue, reused to avoid reallocation.
    outcomes: Vec<Outcome>,

    /// Round-robin position for choosing the next partition to poll.
    cursor: usize,

    refresh_at: Instant,

    closed: bool,
}

impl IggyDeliverySource {
    /// Constructs a source without performing network I/O.
    ///
    /// The client must be connected and dedicated to this source. See [`IggySourceSettings`] for
    /// the group, batch, poll, refresh, and request bounds.
    #[must_use]
    pub fn new(client: IggyClient, settings: IggySourceSettings) -> Self {
        let IggySourceSettings {
            stream,
            topic,
            group,
            batch_length,
            poll_interval,
            assignment_refresh_interval,
            request_timeout,
        } = settings;

        Self {
            context: Arc::new(Context {
                client,
                consumer: SdkConsumer::group(group.clone()),
                stream,
                topic,
                request_timeout,
                outcomes: Mutex::new(Vec::new()),
                notify: Notify::new(),
            }),
            group,
            batch_length,
            poll_interval,
            assignment_refresh_interval,
            opened: None,
            slots: Vec::new(),
            withdrawn: VecDeque::new(),
            ready: VecDeque::new(),
            outcomes: Vec::new(),
            cursor: 0,
            refresh_at: Instant::now(),
            closed: false,
        }
    }

    async fn open_group(&mut self) -> Result<(), IggyDeliveryError> {
        if self.opened.is_some() {
            return Ok(());
        }

        if !self.context.client.is_connected().await {
            return Err(IggyDeliveryError::new(
                IggyDeliveryErrorKind::Disconnected,
                FailureKind::Transient,
            ));
        }

        let context = Arc::clone(&self.context);
        let client = context.client.sdk_client();

        let stream = request(&context, client.get_stream(&context.stream))
            .await
            .map_err(Stop::into_error)?
            .ok_or_else(not_found)?;

        let topic = request(&context, client.get_topic(&context.stream, &context.topic))
            .await
            .map_err(Stop::into_error)?
            .ok_or_else(not_found)?;

        let group = request(
            &context,
            client.get_consumer_group(&context.stream, &context.topic, &self.group),
        )
        .await
        .map_err(Stop::into_error)?
        .ok_or_else(not_found)?;

        request(
            &context,
            client.join_consumer_group(&context.stream, &context.topic, &self.group),
        )
        .await
        .map_err(Stop::into_error)?;

        self.opened = Some(Resolved {
            stream: stream.id,
            topic: topic.id,
            group: group.id,
        });

        self.sync_partitions(&topic.partitions);
        self.refresh_at = Instant::now() + self.assignment_refresh_interval;

        Ok(())
    }

    /// Moves recorded settlement outcomes into partition state. Outcomes stamped with an older
    /// epoch, or for a record that is no longer the partition's unresolved one, are ignored.
    fn apply_outcomes(&mut self) {
        {
            let mut queued = lock(&self.context.outcomes);

            if queued.is_empty() {
                return;
            }

            std::mem::swap(&mut *queued, &mut self.outcomes);
        }

        let now = Instant::now();
        let mut outcomes = std::mem::take(&mut self.outcomes);

        for outcome in outcomes.drain(..) {
            let Some(index) = self.index_of(outcome.partition) else {
                continue;
            };

            let slot = &mut self.slots[index];

            if slot.epoch != outcome.epoch || slot.unresolved != Some(outcome.offset) {
                continue;
            }

            slot.unresolved = None;

            if outcome.advanced {
                if !slot.buffer.is_empty() {
                    self.ready.push_back(index);
                }
            } else {
                // Replay-only withdrawal: the store may still apply later, which can only cause
                // replay. The record is replayed from its own offset after one poll interval.
                slot.epoch = slot.epoch.wrapping_add(1);
                slot.buffer.clear();
                slot.next = Some(outcome.offset);
                slot.not_before = Some(now + self.poll_interval);
                self.withdrawn.push_back(slot.id);
            }
        }

        self.outcomes = outcomes;
    }

    fn take_ready(&mut self) -> Option<IggyDelivery> {
        while let Some(index) = self.ready.pop_front() {
            let slot = &mut self.slots[index];

            if !slot.owned || slot.unresolved.is_some() {
                continue;
            }

            let Some(message) = slot.buffer.pop_front() else {
                continue;
            };

            let offset = message.header.offset;
            slot.unresolved = Some(offset);

            return Some(IggyDelivery {
                record: wire::record(message),
                settlement: IggySettlement {
                    partition: slot.id,
                    offset,
                    report: Report {
                        context: Arc::clone(&self.context),
                        partition: slot.id,
                        offset,
                        epoch: slot.epoch,
                        advanced: false,
                    },
                },
            });
        }

        None
    }

    /// Chooses the next idle owned partition or partition due for an ownership probe.
    fn next_poll(&mut self, now: Instant) -> Option<usize> {
        let len = self.slots.len();

        for step in 0..len {
            let index = (self.cursor + step) % len;
            let slot = &self.slots[index];

            let due = if slot.owned {
                slot.buffer.is_empty()
                    && slot.unresolved.is_none()
                    && slot.not_before.is_none_or(|at| at <= now)
            } else {
                slot.probe_due
            };

            if due {
                self.cursor = (index + 1) % len;

                return Some(index);
            }
        }

        None
    }

    /// Issues one bounded poll and applies its reply after it completes, so a dropped poll
    /// changes nothing. The poll never commits: auto-commit is always disabled.
    async fn poll(&mut self, index: usize) -> Result<(), Stop> {
        let context = Arc::clone(&self.context);

        let (partition, strategy) = {
            let slot = &self.slots[index];

            (
                slot.id,
                slot.next
                    .map_or_else(PollingStrategy::next, PollingStrategy::offset),
            )
        };

        let polled = tokio::time::timeout(
            context.request_timeout,
            context.client.sdk_client().poll_messages(
                &context.stream,
                &context.topic,
                Some(partition),
                &context.consumer,
                &strategy,
                self.batch_length,
                false,
            ),
        )
        .await;

        let polled = match polled {
            Ok(Ok(polled)) => polled,
            Ok(Err(IggyError::ConsumerGroupPartitionNotOwned(..))) => {
                self.fence(index);

                return Ok(());
            }
            Ok(Err(IggyError::ConsumerGroupMemberNotFound(..))) => {
                // Not a member: the refresh this schedules rejoins the group.
                self.fence(index);
                self.refresh_at = Instant::now();

                return Ok(());
            }
            Ok(Err(error)) => return Err(stop(&context, error).await),
            Err(_elapsed) => return Err(Stop::Failed(IggyDeliveryError::timeout())),
        };

        // An explicit poll echoes the requested partition, including on an empty reply. Any other
        // id is the server's generation fence (the resynchronization sentinel) or an unknown
        // partition; neither delivers records.
        if polled.partition_id != partition {
            self.fence(index);

            return Ok(());
        }

        let now = Instant::now();
        let slot = &mut self.slots[index];

        if !slot.owned {
            slot.owned = true;
            slot.epoch = slot.epoch.wrapping_add(1);
            slot.next = None;
            slot.buffer.clear();
            slot.unresolved = None;
            slot.not_before = None;
        }

        slot.probe_due = false;

        let mut received = false;

        for message in polled.messages.into_iter().take(self.batch_length as usize) {
            let offset = message.header.offset;

            match slot.next {
                Some(expected) if offset < expected => continue,
                Some(expected) if offset > expected => return Err(offset_gap()),
                _ => {}
            }

            slot.next = Some(offset.checked_add(1).ok_or_else(offset_gap)?);
            slot.buffer.push_back(message);
            received = true;
        }

        if received {
            slot.not_before = None;

            if slot.unresolved.is_none() {
                self.ready.push_back(index);
            }
        } else {
            slot.not_before = Some(now + self.poll_interval);
        }

        Ok(())
    }

    /// Rechecks membership, rejoining when the server removed this member, reloads the topic's
    /// partitions, and schedules a probe of every partition this source does not own.
    async fn refresh(&mut self) -> Result<(), Stop> {
        let Some(resolved) = self.opened else {
            return Ok(());
        };

        let context = Arc::clone(&self.context);
        let client = context.client.sdk_client();
        let me = request(&context, client.get_me()).await?;

        let member = me.consumer_groups.iter().any(|joined| {
            joined.stream_id == resolved.stream
                && joined.topic_id == resolved.topic
                && joined.group_id == resolved.group
        });

        if !member {
            for index in 0..self.slots.len() {
                self.release(index);
            }

            // A failed rejoin, including a group deleted while the source ran, fails the run.
            request(
                &context,
                client.join_consumer_group(&context.stream, &context.topic, &self.group),
            )
            .await?;
        }

        let topic = request(&context, client.get_topic(&context.stream, &context.topic))
            .await?
            .ok_or_else(|| Stop::Failed(not_found()))?;

        self.sync_partitions(&topic.partitions);
        self.refresh_at = Instant::now() + self.assignment_refresh_interval;

        Ok(())
    }

    /// Adds newly created partitions and schedules a probe of every partition not owned.
    fn sync_partitions(&mut self, partitions: &[Partition]) {
        for partition in partitions {
            if let Err(position) = self
                .slots
                .binary_search_by_key(&partition.id, |slot| slot.id)
            {
                self.slots.insert(position, Slot::new(partition.id));
                // Insertion shifts later indexes, so queued indexes are rebuilt below.
                self.rebuild_ready();
            }
        }

        for slot in &mut self.slots {
            if !slot.owned {
                slot.probe_due = true;
            }
        }
    }

    fn rebuild_ready(&mut self) {
        self.ready.clear();

        for (index, slot) in self.slots.iter().enumerate() {
            if slot.owned && slot.unresolved.is_none() && !slot.buffer.is_empty() {
                self.ready.push_back(index);
            }
        }
    }

    /// Handles a fenced poll: an owned partition is lost and triggers an immediate refresh.
    fn fence(&mut self, index: usize) {
        if self.slots[index].owned {
            self.release(index);
            self.refresh_at = Instant::now();
        }

        self.slots[index].probe_due = false;
    }

    /// Gives up an owned partition, withdrawing its unresolved record, if any.
    fn release(&mut self, index: usize) {
        let slot = &mut self.slots[index];

        if !slot.owned {
            return;
        }

        slot.owned = false;
        slot.epoch = slot.epoch.wrapping_add(1);
        slot.next = None;
        slot.buffer.clear();
        slot.not_before = None;

        if slot.unresolved.take().is_some() {
            self.withdrawn.push_back(slot.id);
        }
    }

    fn index_of(&self, partition: u32) -> Option<usize> {
        self.slots
            .binary_search_by_key(&partition, |slot| slot.id)
            .ok()
    }

    /// The next time this source has work without a settlement outcome, at most one poll
    /// interval away.
    fn next_wake(&self, now: Instant) -> Instant {
        let mut wake = (now + self.poll_interval).min(self.refresh_at);

        for slot in &self.slots {
            if slot.owned
                && slot.buffer.is_empty()
                && slot.unresolved.is_none()
                && let Some(at) = slot.not_before
            {
                wake = wake.min(at);
            }
        }

        wake
    }
}

impl fmt::Debug for IggyDeliverySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IggyDeliverySource")
            .field("opened", &self.opened.is_some())
            .field("partitions", &self.slots.len())
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl PartitionedLogDeliverySource for IggyDeliverySource {
    type Partition = u32;
    type Delivery = IggyDelivery;
    type Error = IggyDeliveryError;

    async fn open(&mut self) -> Result<(), Self::Error> {
        self.open_group().await
    }

    async fn receive(
        &mut self,
    ) -> Result<PartitionedLogReceive<Self::Delivery, Self::Partition>, Self::Error> {
        if self.closed {
            return Ok(PartitionedLogReceive::Closed);
        }

        self.open_group().await?;

        loop {
            // Everything already known is reported before any I/O, so dropping this future at an
            // await below loses no event, outcome, or buffered record.
            self.apply_outcomes();

            if let Some(partition) = self.withdrawn.pop_front() {
                return Ok(PartitionedLogReceive::OwnershipLost(partition));
            }

            if let Some(delivery) = self.take_ready() {
                return Ok(PartitionedLogReceive::Delivery(delivery));
            }

            let now = Instant::now();

            let step = if now >= self.refresh_at {
                self.refresh().await
            } else if let Some(index) = self.next_poll(now) {
                self.poll(index).await
            } else {
                let wake = self.next_wake(now);

                tokio::select! {
                    () = self.context.notify.notified() => {}
                    () = tokio::time::sleep_until(wake) => {}
                }

                Ok(())
            };

            match step {
                Ok(()) => {}
                Err(Stop::Closed) => {
                    self.closed = true;

                    return Ok(PartitionedLogReceive::Closed);
                }
                Err(Stop::Failed(error)) => return Err(error),
            }
        }
    }
}

/// One Iggy record with its partition-offset settlement handle.
pub struct IggyDelivery {
    record: IggyRecord,

    settlement: IggySettlement,
}

impl Delivery for IggyDelivery {
    type Wire = IggyRecord;
    type Settlement = IggySettlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.record, self.settlement)
    }
}

impl fmt::Debug for IggyDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IggyDelivery")
            .field("partition", &self.settlement.partition)
            .field("offset", &self.settlement.offset)
            .finish_non_exhaustive()
    }
}

/// Stores one resolved record's offset as the consumer group's cursor for its partition.
///
/// `advance` writes the record's own offset, which Iggy treats as the last consumed record, so
/// the group resumes after it. An error, a timeout, or a dropped `advance` is indeterminate: the
/// store may still apply later. The source then withdraws the record with an ownership-loss event
/// and replays it; `advance` never returns [`PartitionAdvance::OwnershipLost`]. A settlement
/// dropped without `advance` is withdrawn and replayed the same way.
pub struct IggySettlement {
    partition: u32,

    offset: u64,

    report: Report,
}

impl IggySettlement {
    /// Returns the record's offset within its partition.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }
}

impl fmt::Debug for IggySettlement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IggySettlement")
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl PartitionedLogSettlement for IggySettlement {
    type Partition = u32;
    type Error = IggyDeliveryError;

    async fn advance(self) -> Result<PartitionAdvance, Self::Error> {
        let Self {
            partition,
            offset,
            mut report,
        } = self;

        let context = Arc::clone(&report.context);

        let stored = tokio::time::timeout(
            context.request_timeout,
            context.client.sdk_client().store_consumer_offset(
                &context.consumer,
                &context.stream,
                &context.topic,
                Some(partition),
                offset,
            ),
        )
        .await;

        match stored {
            Ok(Ok(())) => {
                report.advanced = true;

                Ok(PartitionAdvance::Advanced)
            }
            Ok(Err(error)) => Err(IggyDeliveryError::from(error)),
            Err(_elapsed) => Err(IggyDeliveryError::timeout()),
        }
    }

    fn partition(&self) -> &Self::Partition {
        &self.partition
    }
}

/// State shared between the source and its settlements.
struct Context {
    client: IggyClient,

    consumer: SdkConsumer,

    stream: Identifier,

    topic: Identifier,

    request_timeout: Duration,

    /// At most one outcome per live settlement.
    outcomes: Mutex<Vec<Outcome>>,

    notify: Notify,
}

/// A settlement's result, recorded when its `advance` finishes or it is dropped.
struct Outcome {
    partition: u32,

    offset: u64,

    epoch: u64,

    advanced: bool,
}

/// Records exactly one outcome for a settlement when it is dropped, including when an `advance`
/// future is dropped before completing.
struct Report {
    context: Arc<Context>,

    partition: u32,

    offset: u64,

    epoch: u64,

    advanced: bool,
}

impl Drop for Report {
    fn drop(&mut self) {
        lock(&self.context.outcomes).push(Outcome {
            partition: self.partition,
            offset: self.offset,
            epoch: self.epoch,
            advanced: self.advanced,
        });

        self.context.notify.notify_one();
    }
}

/// Numeric server ids resolved when the source opens, used to recognize its own membership.
#[derive(Clone, Copy)]
struct Resolved {
    stream: u32,

    topic: u32,

    group: u32,
}

/// Local state of one topic partition.
struct Slot {
    id: u32,

    owned: bool,

    /// Changes whenever the partition is gained, lost, or has a record withdrawn; settlement
    /// outcomes from an earlier epoch are ignored.
    epoch: u64,

    /// The next offset to poll, or `None` to resume after the group's stored offset.
    next: Option<u64>,

    /// Polled records not yet delivered, in offset order; at most one batch.
    buffer: VecDeque<IggyMessage>,

    /// The delivered record whose settlement has not reported yet.
    unresolved: Option<u64>,

    /// The earliest time an idle owned partition is polled again.
    not_before: Option<Instant>,

    /// Whether an unowned partition is due for an ownership probe.
    probe_due: bool,
}

impl Slot {
    fn new(id: u32) -> Self {
        Self {
            id,
            owned: false,
            epoch: 0,
            next: None,
            buffer: VecDeque::new(),
            unresolved: None,
            not_before: None,
            probe_due: true,
        }
    }
}

/// Why a source step stopped the current receive.
enum Stop {
    /// The application shut the client down.
    Closed,

    Failed(IggyDeliveryError),
}

impl Stop {
    /// Opening has no close outcome: a client already shut down cannot open a source.
    fn into_error(self) -> IggyDeliveryError {
        match self {
            Self::Closed => {
                IggyDeliveryError::new(IggyDeliveryErrorKind::Disconnected, FailureKind::Transient)
            }
            Self::Failed(error) => error,
        }
    }
}

/// Bounds one server request by the request timeout.
async fn request<T>(
    context: &Context,
    operation: impl Future<Output = Result<T, IggyError>>,
) -> Result<T, Stop> {
    match tokio::time::timeout(context.request_timeout, operation).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(stop(context, error).await),
        Err(_elapsed) => Err(Stop::Failed(IggyDeliveryError::timeout())),
    }
}

/// Reports a deliberately shut-down client as a clean close and classifies every other error.
async fn stop(context: &Context, error: IggyError) -> Stop {
    if is_shut_down(&context.client).await {
        Stop::Closed
    } else {
        Stop::Failed(IggyDeliveryError::from(error))
    }
}

/// Reads whether the SDK session was shut down, as distinct from lost.
async fn is_shut_down(client: &IggyClient) -> bool {
    let wrapper = client.sdk_client().client();
    let guard = wrapper.read().await;

    // This crate only builds TCP clients.
    let ClientWrapper::Tcp(tcp_client) = &*guard else {
        return false;
    };

    matches!(tcp_client.get_state().await, ClientState::Shutdown)
}

fn not_found() -> IggyDeliveryError {
    IggyDeliveryError::new(IggyDeliveryErrorKind::NotFound, FailureKind::Permanent)
}

fn offset_gap() -> Stop {
    Stop::Failed(IggyDeliveryError::new(
        IggyDeliveryErrorKind::OffsetGap,
        FailureKind::Permanent,
    ))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
