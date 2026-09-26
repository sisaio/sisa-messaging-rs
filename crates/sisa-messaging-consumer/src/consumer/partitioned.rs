//! Ordered partition coordination on the shared bounded runtime.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use sisa_messaging::{
    Delivery, EnvelopeMapper, ErrorClassifier, FailureKind, Message, PartitionAdvance,
    PartitionedLogDeliverySource, PartitionedLogReceive, PartitionedLogSettlement, Serializer,
};
use sisa_messaging_inbox::{InboxStore, InboxUnitOfWork};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::telemetry::{self, MessageLabels};
use crate::{ConsumerError, ConsumerErrorKind, ConsumerExit, ConsumerHandler};

use super::process;
use super::receive::Intake;
use super::settlement::Resolution;
use super::worker::{StopOnPanic, Workers};
use super::{Consumer, Shared, run_loop};

struct ActiveEntry {
    token: Arc<CancellationToken>,

    stale: bool,

    coordinator_done: bool,

    deferred: bool,

    released: Arc<Notify>,
}

type Active<P> = Arc<Mutex<HashMap<P, ActiveEntry>>>;

struct DeferredStart {
    released: Arc<Notify>,

    shutdown: CancellationToken,

    tracker: tokio_util::task::TaskTracker,

    stop: CancellationToken,
}

fn lock<P: Eq + Hash>(active: &Active<P>) -> std::sync::MutexGuard<'_, HashMap<P, ActiveEntry>> {
    active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct PartitionIntake<M, S, Map, Codec, Inbox, H>
where
    S: PartitionedLogDeliverySource,
{
    source: S,

    shared: Arc<Shared<Map, Codec, Inbox, H>>,

    active: Active<S::Partition>,

    shutdown: CancellationToken,

    message: PhantomData<fn() -> M>,
}

impl<M, S, Map, Codec, Inbox, H> Intake for PartitionIntake<M, S, Map, Codec, Inbox, H>
where
    M: Message,
    S: PartitionedLogDeliverySource,
    Map: EnvelopeMapper<<S::Delivery as Delivery>::Wire> + 'static,
    Codec: Serializer<M> + 'static,
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
    H: ConsumerHandler<M, Inbox::Transaction> + 'static,
{
    type Item = PartitionedLogReceive<S::Delivery, S::Partition>;

    async fn receive(&mut self) -> Result<Option<Self::Item>, ConsumerError> {
        self.source.receive().await.map(Some).map_err(|error| {
            let failure = error.classify();

            ConsumerError::new(ConsumerErrorKind::Source, failure, Some(Box::new(error)))
        })
    }

    fn dispatch(&self, item: Self::Item, workers: &mut Workers) -> bool {
        match item {
            PartitionedLogReceive::Closed => return true,
            PartitionedLogReceive::OwnershipLost(partition) => {
                telemetry::partition_event(self.shared.labels, "ownership_lost");
                let mut active = lock(&self.active);

                if let Some(entry) = active.get_mut(&partition) {
                    entry.stale = true;
                    entry.token.cancel();

                    if entry.coordinator_done && !entry.deferred {
                        active.remove(&partition);
                    }
                }
            }
            PartitionedLogReceive::Delivery(delivery) => {
                let (wire, settlement) = delivery.into_parts();
                let partition = settlement.partition().clone();
                let token = Arc::new(CancellationToken::new());

                let deferred = {
                    let mut active = lock(&self.active);

                    if let Some(entry) = active.get_mut(&partition) {
                        if entry.stale && !entry.deferred {
                            entry.deferred = true;

                            Some(Arc::clone(&entry.released))
                        } else {
                            telemetry::partition_event(self.shared.labels, "overlap");

                            workers.fail(ConsumerError::new(
                                ConsumerErrorKind::PartitionOrder,
                                FailureKind::Permanent,
                                None,
                            ));

                            return false;
                        }
                    } else {
                        active.insert(
                            partition.clone(),
                            ActiveEntry {
                                token: token.clone(),
                                stale: false,
                                coordinator_done: false,
                                deferred: false,
                                released: Arc::new(Notify::new()),
                            },
                        );

                        None
                    }
                };

                if let Some(released) = deferred {
                    workers.spawn(deferred_coordinate::<M, _, _, _, _, _, _>(
                        Arc::clone(&self.shared),
                        wire,
                        settlement,
                        Arc::clone(&self.active),
                        token,
                        DeferredStart {
                            released,
                            shutdown: self.shutdown.clone(),
                            tracker: workers.tracker(),
                            stop: workers.stop_token(),
                        },
                    ));
                } else {
                    workers.spawn(coordinate::<M, _, _, _, _, _, _>(
                        Arc::clone(&self.shared),
                        wire,
                        settlement,
                        Arc::clone(&self.active),
                        token,
                        workers.tracker(),
                        workers.stop_token(),
                    ));
                }
            }
            _ => workers.fail(ConsumerError::new(
                ConsumerErrorKind::Runtime,
                FailureKind::Permanent,
                None,
            )),
        }

        false
    }

    fn stop(&self) {
        self.shutdown.cancel();
    }
}

struct ActiveGuard<P: Eq + Hash> {
    partition: P,

    active: Active<P>,

    token: Arc<CancellationToken>,

    safe_to_release: bool,
}

impl<P: Eq + Hash> Drop for ActiveGuard<P> {
    fn drop(&mut self) {
        let mut active = lock(&self.active);

        let Some(current) = active.get_mut(&self.partition) else {
            return;
        };

        if !Arc::ptr_eq(&current.token, &self.token) {
            return;
        }

        current.coordinator_done = true;

        if current.stale && current.deferred {
            // Keep the old token as a placeholder until the one deferred coordinator atomically
            // installs its replacement. Later records cannot overtake it in that gap.
            current.released.notify_one();
        } else if self.safe_to_release || current.stale {
            let released = Arc::clone(&current.released);
            active.remove(&self.partition);
            released.notify_one();
        }
    }
}

async fn deferred_coordinate<M, W, St, Map, Codec, Inbox, H>(
    shared: Arc<Shared<Map, Codec, Inbox, H>>,
    wire: W,
    settlement: St,
    active: Active<St::Partition>,
    token: Arc<CancellationToken>,
    start: DeferredStart,
) -> Result<(), ConsumerError>
where
    M: Message,
    W: Send + 'static,
    St: PartitionedLogSettlement,
    Map: EnvelopeMapper<W> + 'static,
    Codec: Serializer<M> + 'static,
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
    H: ConsumerHandler<M, Inbox::Transaction> + 'static,
{
    let _panic_stop = StopOnPanic(start.stop.clone());

    tokio::select! {
        biased;
        () = start.shutdown.cancelled() => return Ok(()),
        () = start.stop.cancelled() => return Ok(()),
        () = start.released.notified() => {}
    }

    if start.shutdown.is_cancelled() || start.stop.is_cancelled() {
        return Ok(());
    }

    let partition = settlement.partition().clone();

    {
        let mut current = lock(&active);

        if !current.get(&partition).is_some_and(|entry| {
            entry.stale
                && entry.coordinator_done
                && entry.deferred
                && Arc::ptr_eq(&entry.released, &start.released)
        }) {
            drop(current);
            start.stop.cancel();

            return Err(ConsumerError::new(
                ConsumerErrorKind::PartitionOrder,
                FailureKind::Permanent,
                None,
            ));
        }

        // The old workflow and its transaction are gone. Replace its placeholder under this
        // lock, so no following record can overtake the deferred delivery.
        current.insert(
            partition,
            ActiveEntry {
                token: token.clone(),
                stale: false,
                coordinator_done: false,
                deferred: false,
                released: Arc::new(Notify::new()),
            },
        );
    }

    coordinate::<M, _, _, _, _, _, _>(
        shared,
        wire,
        settlement,
        active,
        token,
        start.tracker,
        start.stop,
    )
    .await
}

async fn coordinate<M, W, St, Map, Codec, Inbox, H>(
    shared: Arc<Shared<Map, Codec, Inbox, H>>,
    wire: W,
    settlement: St,
    active: Active<St::Partition>,
    token: Arc<CancellationToken>,
    tracker: tokio_util::task::TaskTracker,
    stop: CancellationToken,
) -> Result<(), ConsumerError>
where
    M: Message,
    W: Send + 'static,
    St: PartitionedLogSettlement,
    Map: EnvelopeMapper<W> + 'static,
    Codec: Serializer<M> + 'static,
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
    H: ConsumerHandler<M, Inbox::Transaction> + 'static,
{
    // A settlement panic must wake an idle intake before its pending receive can strand the run.
    let _panic_stop = StopOnPanic(stop.clone());

    let mut guard = ActiveGuard {
        partition: settlement.partition().clone(),
        active,
        token: token.clone(),
        safe_to_release: false,
    };

    let in_handler = Arc::new(AtomicBool::new(false));

    let mut workflow =
        tokio_util::task::AbortOnDropHandle::new(tracker.spawn(
            process::process::<M, _, _, _, _, _>(
                Arc::clone(&shared),
                wire,
                Arc::clone(&in_handler),
            ),
        ));

    let joined = tokio::select! {
        biased;
        () = token.cancelled() => {
            workflow.abort();
            let _ = workflow.await;
            drop(settlement);
            return Ok(());
        }
        result = &mut workflow => result,
    };

    let processed = match joined {
        Ok(processed) => processed,
        Err(error) => {
            drop(settlement);
            stop.cancel();

            let kind = if error.is_panic() && in_handler.load(std::sync::atomic::Ordering::Acquire)
            {
                ConsumerErrorKind::HandlerPanicked
            } else if error.is_panic() {
                ConsumerErrorKind::ProviderPanicked
            } else {
                ConsumerErrorKind::Runtime
            };

            telemetry::task_failed(shared.labels, kind);

            return Err(ConsumerError::new(kind, FailureKind::Permanent, None));
        }
    };

    if token.is_cancelled() {
        telemetry::partition_event(shared.labels, "stale_work");
        drop(settlement);

        return Ok(());
    }

    match processed.resolution {
        Resolution::Completed | Resolution::Dead(_) => {
            // Advance is not selected against cancellation. Once started it must complete or
            // timeout before the coordinator can release local ownership.
            let advanced =
                tokio::time::timeout(shared.settings.settlement_timeout, settlement.advance())
                    .await;

            match advanced {
                Ok(Ok(PartitionAdvance::Advanced)) if !token.is_cancelled() => {
                    guard.safe_to_release = true;
                    telemetry::partition_event(shared.labels, "advanced");

                    Ok(())
                }
                Ok(Ok(PartitionAdvance::OwnershipLost)) => {
                    guard.safe_to_release = true;
                    telemetry::partition_event(shared.labels, "advance_fenced");

                    Ok(())
                }
                Ok(Err(error)) if error.classify() == FailureKind::Permanent => {
                    telemetry::partition_event(shared.labels, "advance_failed");
                    stop.cancel();

                    Err(ConsumerError::new(
                        ConsumerErrorKind::Settlement,
                        FailureKind::Permanent,
                        Some(Box::new(error)),
                    ))
                }
                _ => {
                    telemetry::partition_event(shared.labels, "advance_uncertain");

                    // The active entry deliberately remains unresolved. The source must fence
                    // late effects and reconcile its cursor/generation before it can confirm an
                    // ownership loss; other partitions may keep making progress meanwhile.
                    Ok(())
                }
            }
        }
        Resolution::Malformed => {
            drop(settlement);
            stop.cancel();

            Err(ConsumerError::new(
                ConsumerErrorKind::OperatorActionRequired(crate::OperatorReason::Malformed),
                FailureKind::Permanent,
                processed.error,
            ))
        }
        resolution => {
            drop(settlement);
            stop.cancel();

            Err(ConsumerError::new(
                ConsumerErrorKind::PartitionUnresolved,
                resolution.failure_kind().unwrap_or(FailureKind::Transient),
                processed.error,
            ))
        }
    }
}

pub(super) async fn run<M, S, Map, Codec, Inbox, H>(
    consumer: Consumer<M, (S, Map, Codec, Inbox, H)>,
    cancel: CancellationToken,
) -> Result<ConsumerExit, ConsumerError>
where
    M: Message,
    S: PartitionedLogDeliverySource,
    Map: EnvelopeMapper<<S::Delivery as Delivery>::Wire> + 'static,
    Codec: Serializer<M> + 'static,
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
    H: ConsumerHandler<M, Inbox::Transaction> + 'static,
{
    let labels = MessageLabels {
        message_type: M::TYPE,
        version: M::VERSION,
    };

    telemetry::started(labels);
    let ambient = opentelemetry::Context::current();
    let (mut source, mapper, codec, inbox, handler) = consumer.components;
    let settings = consumer.settings;

    let opened = tokio::select! {
        biased;
        () = cancel.cancelled() => return Ok(ConsumerExit::Cancelled),
        result = tokio::time::timeout(settings.source_timeout, source.open()) => result,
    };

    match opened {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Err(ConsumerError::new(
                ConsumerErrorKind::SourceOpen,
                error.classify(),
                Some(Box::new(error)),
            ));
        }
        Err(_) => {
            return Err(ConsumerError::new(
                ConsumerErrorKind::SourceOpenTimeout,
                FailureKind::Transient,
                None,
            ));
        }
    }

    let capacity = settings.max_in_flight.get();
    let drain_timeout = settings.drain_timeout;

    let shared = Arc::new(Shared {
        mapper,
        codec,
        inbox,
        handler,
        scope: consumer.scope,
        settings,
        labels,
        ambient,
    });

    let intake = PartitionIntake {
        source,
        shared,
        active: Arc::new(Mutex::new(HashMap::new())),
        shutdown: CancellationToken::new(),
        message: PhantomData,
    };

    let result = run_loop(
        intake,
        Workers::new(capacity, labels),
        &cancel,
        drain_timeout,
    )
    .await;

    telemetry::stopped(
        labels,
        match &result {
            Ok(ConsumerExit::Cancelled) => "cancelled",
            Ok(ConsumerExit::SourceClosed) => "source_closed",
            Err(_) => "failed",
        },
    );

    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    use super::{ActiveEntry, ActiveGuard, lock};

    #[test]
    fn indeterminate_advance_retains_partition_and_stale_token_cannot_clear_new_owner() {
        let active = Arc::new(Mutex::new(HashMap::new()));
        let first = Arc::new(CancellationToken::new());

        lock(&active).insert(
            7_u8,
            ActiveEntry {
                token: first.clone(),
                stale: false,
                coordinator_done: false,
                deferred: false,
                released: Arc::new(Notify::new()),
            },
        );

        drop(ActiveGuard {
            partition: 7_u8,
            active: active.clone(),
            token: first.clone(),
            safe_to_release: false,
        });

        assert!(lock(&active).contains_key(&7));

        let next = Arc::new(CancellationToken::new());

        lock(&active).insert(
            7_u8,
            ActiveEntry {
                token: next.clone(),
                stale: false,
                coordinator_done: false,
                deferred: false,
                released: Arc::new(Notify::new()),
            },
        );

        drop(ActiveGuard {
            partition: 7_u8,
            active: active.clone(),
            token: first,
            safe_to_release: true,
        });

        assert!(
            lock(&active)
                .get(&7)
                .is_some_and(|entry| Arc::ptr_eq(&entry.token, &next))
        );

        drop(ActiveGuard {
            partition: 7_u8,
            active: active.clone(),
            token: next,
            safe_to_release: true,
        });

        assert!(!lock(&active).contains_key(&7));
    }
}
