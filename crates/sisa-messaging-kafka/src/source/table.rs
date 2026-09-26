//! Deterministic partition ownership, record lane, and advance-request state.
//!
//! The table holds no broker or runtime types, so every transition is testable in isolation.
//! The member thread applies its results to the broker client: it pauses the partitions a
//! transition returns, seeks each resumable partition to its next position before resuming
//! it, and commits the advance batches it takes.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

/// Records handed to the source but not yet received by the consumer.
pub(crate) const LANE_CAPACITY: usize = 64;

/// Backpressure-paused partitions resume once the lane drains to this length.
pub(crate) const LANE_RESUME_AT: usize = LANE_CAPACITY / 2;

/// The lifecycle of one partition owned by the current generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum State {
    /// Fetching; the next record at or after the next position may be emitted.
    Idle,

    /// One record was emitted and the partition is paused. `handed` is false while it waits in
    /// the lane and true once the consumer owns its settlement handle.
    InFlight { offset: i64, handed: bool },

    /// An advance request is queued for the member thread.
    Advancing { offset: i64 },

    /// An advance is part of a transaction the member thread is committing.
    Sending { offset: i64 },

    /// The advance outcome is indeterminate; the partition stays paused until the committed
    /// cursor is re-established behind a producer epoch fence.
    Reconciling { offset: i64 },

    /// The handle was dropped without advancing, or the advance was fenced or failed
    /// permanently; the partition stays paused until revocation or shutdown.
    Held { offset: i64 },

    /// Reassigned while an earlier generation's handle or reconciliation for it is unfinished.
    Withheld,

    /// Paused because the lane was full.
    BackpressurePaused,

    /// Must be sought to its next position and resumed by the member thread.
    ResumePending,
}

struct Partition {
    state: State,

    /// The offset the next emitted record must reach and the seek target on resume.
    next_position: Option<i64>,
}

/// A record waiting in the lane for the consumer.
pub(crate) struct Lane<K, R> {
    pub(crate) key: K,

    pub(crate) offset: i64,

    pub(crate) generation: u64,

    pub(crate) record: R,
}

/// One queued advance request with its reply channel.
pub(crate) struct Request<W> {
    offset: i64,

    waiter: W,
}

/// One advance included in a transaction batch.
pub(crate) struct BatchItem<K, W> {
    pub(crate) key: K,

    pub(crate) offset: i64,

    pub(crate) waiter: W,
}

/// The current generation's live advance requests.
pub(crate) struct Batch<K, W> {
    pub(crate) generation: u64,

    pub(crate) items: Vec<BatchItem<K, W>>,
}

/// The next item for the consumer; ownership-loss releases precede records.
pub(crate) enum Popped<K, R> {
    Loss(K),

    Record(Lane<K, R>),
}

/// Whether an advance request was queued or conclusively answered without broker I/O.
pub(crate) enum Admission<W> {
    Queued,

    /// The handle's generation no longer owns the partition; nothing was sent.
    OwnershipLost(W),
}

/// The effect of offering one fetched record.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RecordOutcome<K> {
    pub(crate) emitted: bool,

    /// Partitions the member thread must pause.
    pub(crate) pause: Vec<K>,
}

/// The effect of a revocation.
pub(crate) struct Revocation<W> {
    /// Unsent requests, each answered with a conclusive ownership loss.
    pub(crate) lost: Vec<W>,
}

/// Partition ownership, lane, and request state shared by the member thread and the source.
pub(crate) struct Table<K, R, W> {
    generation: u64,

    assigned: bool,

    partitions: HashMap<K, Partition>,

    /// Earlier generations' partitions whose handles are still live, by generation.
    revoked: HashMap<K, u64>,

    lane: VecDeque<Lane<K, R>>,

    losses: VecDeque<K>,

    requests: HashMap<K, Request<W>>,

    backpressure: bool,
}

impl<K, R, W> Default for Table<K, R, W> {
    fn default() -> Self {
        Self {
            generation: 0,
            assigned: false,
            partitions: HashMap::new(),
            revoked: HashMap::new(),
            lane: VecDeque::new(),
            losses: VecDeque::new(),
            requests: HashMap::new(),
            backpressure: false,
        }
    }
}

impl<K, R, W> Table<K, R, W>
where
    K: Clone + Eq + Hash,
{
    /// The current generation, which is stale while no assignment is held.
    #[allow(dead_code, reason = "observed by the deterministic state tests")]
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn state(&self, key: &K) -> Option<State> {
        self.partitions.get(key).map(|partition| partition.state)
    }

    #[allow(dead_code, reason = "observed by the deterministic state tests")]
    pub(crate) fn next_position(&self, key: &K) -> Option<i64> {
        self.partitions
            .get(key)
            .and_then(|partition| partition.next_position)
    }

    #[allow(dead_code, reason = "observed by the deterministic state tests")]
    pub(crate) fn is_revoked_pending(&self, key: &K) -> bool {
        self.revoked.contains_key(key)
    }

    #[allow(dead_code, reason = "observed by the deterministic state tests")]
    pub(crate) fn lane_len(&self) -> usize {
        self.lane.len()
    }

    #[allow(dead_code, reason = "observed by the deterministic state tests")]
    pub(crate) const fn backpressure(&self) -> bool {
        self.backpressure
    }

    /// Whether a loss or record is ready for the consumer.
    pub(crate) fn has_output(&self) -> bool {
        !self.losses.is_empty() || !self.lane.is_empty()
    }

    /// Starts a new generation. Returns the partitions to withhold (pause) because an earlier
    /// generation's handle for them is still live.
    pub(crate) fn assign(&mut self, keys: impl IntoIterator<Item = K>) -> (u64, Vec<K>) {
        if self.assigned {
            // Eager assignment always follows a revocation; treat a gap defensively.
            let _ = self.revoke();
        }

        self.generation = self.generation.saturating_add(1);
        self.assigned = true;
        let mut withheld = Vec::new();

        for key in keys {
            let state = if self.revoked.contains_key(&key) {
                withheld.push(key.clone());

                State::Withheld
            } else {
                State::Idle
            };

            self.partitions.insert(
                key,
                Partition {
                    state,
                    next_position: None,
                },
            );
        }

        (self.generation, withheld)
    }

    /// Ends the current generation. Every partition whose record the consumer owns gets an
    /// ownership-loss release; unreceived lane records are discarded without one.
    pub(crate) fn revoke(&mut self) -> Revocation<W> {
        let generation = self.generation;

        let partitions: Vec<(K, State)> = self
            .partitions
            .drain()
            .map(|(key, partition)| (key, partition.state))
            .collect();

        for (key, state) in partitions {
            match state {
                State::InFlight { handed: true, .. } => {
                    self.push_loss(key.clone());
                    self.revoked.insert(key, generation);
                }
                State::Sending { .. } | State::Reconciling { .. } => {
                    // Only an unestablished reconciliation survives to a revocation; it keeps
                    // later generations withheld because the cursor is still unknown.
                    self.push_loss(key.clone());
                    self.revoked.insert(key, generation);
                }
                State::Advancing { .. } => self.push_loss(key),
                State::Idle
                | State::InFlight { handed: false, .. }
                | State::Held { .. }
                | State::Withheld
                | State::BackpressurePaused
                | State::ResumePending => {}
            }
        }

        self.lane.clear();
        self.assigned = false;
        self.backpressure = false;

        Revocation {
            lost: self
                .requests
                .drain()
                .map(|(_, request)| request.waiter)
                .collect(),
        }
    }

    /// Offers one fetched record; `record` is built only when the record is emitted.
    pub(crate) fn on_record(
        &mut self,
        key: &K,
        offset: i64,
        record: impl FnOnce() -> R,
    ) -> RecordOutcome<K> {
        let mut outcome = RecordOutcome {
            emitted: false,
            pause: Vec::new(),
        };

        if !self.assigned {
            return outcome;
        }

        let lane_full = self.lane.len() >= LANE_CAPACITY;

        let Some(partition) = self.partitions.get_mut(key) else {
            return outcome;
        };

        match partition.state {
            State::Idle => {
                if partition.next_position.is_some_and(|next| offset < next) {
                    // A fetch that predates the last seek; the partition keeps fetching.
                    return outcome;
                }

                if lane_full {
                    partition.state = State::BackpressurePaused;
                    partition.next_position.get_or_insert(offset);
                    self.backpressure = true;
                    outcome.pause.push(key.clone());

                    return outcome;
                }

                // Offsets above the next position are accepted: read_committed skips aborted
                // records and transaction markers, which leaves holes in the offset sequence.
                partition.state = State::InFlight {
                    offset,
                    handed: false,
                };

                self.lane.push_back(Lane {
                    key: key.clone(),
                    offset,
                    generation: self.generation,
                    record: record(),
                });

                outcome.emitted = true;
                outcome.pause.push(key.clone());

                if self.lane.len() >= LANE_CAPACITY {
                    self.backpressure = true;

                    for (other, partition) in &mut self.partitions {
                        if partition.state == State::Idle {
                            partition.state = State::BackpressurePaused;
                            outcome.pause.push(other.clone());
                        }
                    }
                }
            }
            State::Withheld | State::BackpressurePaused | State::ResumePending => {
                // The consumer position moved past this record; remember the earliest one so
                // the resume seek re-fetches it.
                partition.next_position.get_or_insert(offset);
            }
            State::InFlight { .. }
            | State::Advancing { .. }
            | State::Sending { .. }
            | State::Reconciling { .. }
            | State::Held { .. } => {}
        }

        outcome
    }

    /// Takes the next loss release or record. Returns whether the member thread must wake.
    pub(crate) fn pop(&mut self) -> (Option<Popped<K, R>>, bool) {
        if let Some(key) = self.losses.pop_front() {
            return (Some(Popped::Loss(key)), false);
        }

        while let Some(lane) = self.lane.pop_front() {
            let current = self.assigned && lane.generation == self.generation;

            if let Some(partition) = self.partitions.get_mut(&lane.key)
                && current
                && partition.state
                    == (State::InFlight {
                        offset: lane.offset,
                        handed: false,
                    })
            {
                partition.state = State::InFlight {
                    offset: lane.offset,
                    handed: true,
                };

                let wake = self.relieve_lane();

                return (Some(Popped::Record(lane)), wake);
            }
        }

        (None, self.relieve_lane())
    }

    /// Queues an advance for a handed record of the current generation.
    pub(crate) fn admit(
        &mut self,
        key: &K,
        generation: u64,
        offset: i64,
        waiter: W,
    ) -> Admission<W> {
        let current = self.assigned && generation == self.generation;

        if let Some(partition) = self.partitions.get_mut(key)
            && current
            && partition.state
                == (State::InFlight {
                    offset,
                    handed: true,
                })
        {
            partition.state = State::Advancing { offset };

            self.requests
                .insert(key.clone(), Request { offset, waiter });

            return Admission::Queued;
        }

        let _ = self.release_revoked(key, generation);

        Admission::OwnershipLost(waiter)
    }

    /// Records a settlement handle dropped without advancing. Returns whether the member thread
    /// must wake to resume a withheld partition.
    pub(crate) fn handle_dropped(&mut self, key: &K, generation: u64, offset: i64) -> bool {
        let current = self.assigned && generation == self.generation;

        if let Some(partition) = self.partitions.get_mut(key)
            && current
            && partition.state
                == (State::InFlight {
                    offset,
                    handed: true,
                })
        {
            partition.state = State::Held { offset };

            return false;
        }

        self.release_revoked(key, generation)
    }

    /// Records an advance future dropped before it observed its reply.
    ///
    /// While the request is queued or committing, the member thread observes the closed reply
    /// channel itself. Once it has resolved, the consumer may have missed the reply, so a
    /// release lets it clear the unresolved record before the partition's next offset.
    pub(crate) fn advance_abandoned(&mut self, key: &K, generation: u64, offset: i64) -> bool {
        if !self.assigned || generation != self.generation {
            return false;
        }

        let Some(partition) = self.partitions.get(key) else {
            return false;
        };

        match partition.state {
            State::Advancing { offset: pending }
            | State::Sending { offset: pending }
            | State::Reconciling { offset: pending }
                if pending == offset =>
            {
                false
            }
            _ => {
                self.push_loss(key.clone());

                true
            }
        }
    }

    /// Takes the current generation's advance requests. A request whose waiter is gone was
    /// never sent, so its partition is released and replays from the unadvanced offset.
    pub(crate) fn take_batch(&mut self, is_live: impl Fn(&W) -> bool) -> Batch<K, W> {
        let mut items = Vec::new();
        let requests: Vec<(K, Request<W>)> = self.requests.drain().collect();

        for (key, request) in requests {
            let Some(partition) = self.partitions.get_mut(&key) else {
                continue;
            };

            if is_live(&request.waiter) {
                partition.state = State::Sending {
                    offset: request.offset,
                };

                items.push(BatchItem {
                    key,
                    offset: request.offset,
                    waiter: request.waiter,
                });
            } else {
                partition.state = State::ResumePending;
                partition.next_position = Some(request.offset);
                self.push_loss(key);
            }
        }

        Batch {
            generation: self.generation,
            items,
        }
    }

    /// The transaction committed the advance. An undelivered reply becomes a release.
    pub(crate) fn advanced(&mut self, key: &K, offset: i64, delivered: bool) {
        self.resolve(
            key,
            offset,
            State::ResumePending,
            Some(offset + 1),
            delivered,
        );
    }

    /// The group generation fenced the advance, which proves the cursor did not move. The
    /// partition stays paused until the pending revocation.
    pub(crate) fn fenced(&mut self, key: &K, offset: i64, delivered: bool) {
        self.resolve(key, offset, State::Held { offset }, None, delivered);
    }

    /// The advance failed permanently; the partition stays paused.
    pub(crate) fn failed_permanently(&mut self, key: &K, offset: i64) {
        self.resolve(key, offset, State::Held { offset }, None, true);
    }

    /// The advance outcome is indeterminate.
    pub(crate) fn begin_reconcile(&mut self, key: &K, offset: i64) {
        if let Some(partition) = self.partitions.get_mut(key)
            && partition.state == (State::Sending { offset })
        {
            partition.state = State::Reconciling { offset };
        }
    }

    /// The committed cursor was re-established: continue after the record when it advanced,
    /// otherwise replay it. An undelivered reply becomes a release.
    pub(crate) fn reconciled(&mut self, key: &K, offset: i64, advanced: bool, delivered: bool) {
        if self.state(key) != Some(State::Reconciling { offset }) {
            return;
        }

        let next = if advanced { offset + 1 } else { offset };

        if let Some(partition) = self.partitions.get_mut(key) {
            partition.state = State::ResumePending;
            partition.next_position = Some(next);
        }

        if !delivered {
            self.push_loss(key.clone());
        }
    }

    /// Takes the partitions to seek and resume, each with its seek target. While the lane is
    /// full, released partitions stay paused until it drains.
    pub(crate) fn resumable(&mut self) -> Vec<(K, Option<i64>)> {
        if self.backpressure {
            for partition in self.partitions.values_mut() {
                if partition.state == State::ResumePending {
                    partition.state = State::BackpressurePaused;
                }
            }

            return Vec::new();
        }

        self.partitions
            .iter()
            .filter(|(_, partition)| partition.state == State::ResumePending)
            .map(|(key, partition)| (key.clone(), partition.next_position))
            .collect()
    }

    /// Marks partitions sought and resumed.
    pub(crate) fn resumed(&mut self, keys: &[K]) {
        for key in keys {
            if let Some(partition) = self.partitions.get_mut(key)
                && partition.state == State::ResumePending
            {
                partition.state = State::Idle;
            }
        }
    }

    /// Drains every queued request, for failure or shutdown.
    pub(crate) fn drain_requests(&mut self) -> Vec<W> {
        self.requests
            .drain()
            .map(|(_, request)| request.waiter)
            .collect()
    }

    fn resolve(&mut self, key: &K, offset: i64, state: State, next: Option<i64>, delivered: bool) {
        let Some(partition) = self.partitions.get_mut(key) else {
            return;
        };

        if !matches!(
            partition.state,
            State::Sending { offset: pending } | State::Reconciling { offset: pending }
                if pending == offset
        ) {
            return;
        }

        partition.state = state;

        if next.is_some() {
            partition.next_position = next;
        }

        if !delivered {
            self.push_loss(key.clone());
        }
    }

    fn release_revoked(&mut self, key: &K, generation: u64) -> bool {
        if self.revoked.get(key) != Some(&generation) {
            return false;
        }

        self.revoked.remove(key);

        match self.partitions.get_mut(key) {
            Some(partition) if partition.state == State::Withheld => {
                partition.state = State::ResumePending;

                true
            }
            _ => false,
        }
    }

    fn relieve_lane(&mut self) -> bool {
        if !self.backpressure || self.lane.len() > LANE_RESUME_AT {
            return false;
        }

        self.backpressure = false;
        let mut wake = false;

        for partition in self.partitions.values_mut() {
            if partition.state == State::BackpressurePaused {
                partition.state = State::ResumePending;
                wake = true;
            }
        }

        wake
    }

    fn push_loss(&mut self, key: K) {
        if !self.losses.contains(&key) {
            self.losses.push_back(key);
        }
    }
}
