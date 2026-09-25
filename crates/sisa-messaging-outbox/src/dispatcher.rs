//! Bounded at-least-once dispatcher façade and top-level coordination.

mod claim;
mod leases;
mod outcomes;
mod publish;
mod report;
mod shutdown;
mod state;

use std::sync::Arc;

use sisa_messaging::Publisher;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{DispatcherError, DispatcherSettings, OutboxStore, RetryPolicy, SettingsError};

use self::leases::{RenewalOutcome, RenewalTurn, StoreSafety};
use self::outcomes::accounting::{self, PersistenceTurn};
use self::publish::{JoinOutcome, PublishResult};
use self::state::State;

pub use self::report::OutboxRunReport;

/// Composes one portable store and publisher into a bounded at-least-once worker.
pub struct OutboxDispatcher<S, P, R = crate::ExponentialBackoff> {
    store: S,

    publisher: Arc<P>,

    settings: DispatcherSettings<R>,
}

impl<S, P, R> OutboxDispatcher<S, P, R>
where
    S: OutboxStore,
    P: Publisher + 'static,
    R: RetryPolicy,
{
    /// Validates settings and constructs an idle dispatcher without performing I/O.
    pub fn new(
        store: S,
        publisher: P,
        settings: DispatcherSettings<R>,
    ) -> Result<Self, SettingsError> {
        settings.validate()?;

        Ok(Self {
            store,
            publisher: Arc::new(publisher),
            settings,
        })
    }

    /// Runs until cancellation or a terminal permanent failure.
    ///
    /// Cancellation stops new claims, drains for the configured bound, releases remaining
    /// ownership, and returns a report. Permanent store errors and publisher task panics retain
    /// their original source after one finite cleanup pass.
    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<OutboxRunReport, DispatcherError<S::Error>> {
        self.run_loop(cancellation)
            .instrument(tracing::info_span!(
                target: "messaging.outbox",
                "outbox.dispatch"
            ))
            .await
    }

    async fn run_loop(
        self,
        cancellation: CancellationToken,
    ) -> Result<OutboxRunReport, DispatcherError<S::Error>> {
        tracing::info!(target: "messaging.outbox", "dispatcher started");
        let mut report = OutboxRunReport::default();
        let mut state = State::new(self.settings.max_in_flight.get());
        let mut tasks = JoinSet::<PublishResult>::new();
        let mut next_claim = Instant::now();

        loop {
            if cancellation.is_cancelled() {
                let result = shutdown::graceful(
                    &self.store,
                    &self.settings,
                    &mut state,
                    &mut tasks,
                    &mut report,
                )
                .await;

                tracing::info!(target: "messaging.outbox", "dispatcher stopped");

                return result.map(|()| report);
            }

            if let Err(error) = publish::advance_ready(
                &self.publisher,
                self.settings.publish_timeout,
                &self.settings.retry_policy,
                &mut state,
                &mut tasks,
                &mut report,
            ) {
                return Err(shutdown::publisher_failure(
                    &self.store,
                    &mut state,
                    &mut tasks,
                    &mut report,
                    self.settings.store_timeout,
                    error,
                )
                .await);
            }

            match leases::renew_due(&self.store, &self.settings, &mut state, &mut report).await {
                RenewalTurn::Finished(outcome) => {
                    if let Some(error) = renewal_error(outcome) {
                        return Err(shutdown::store_failure(
                            &self.store,
                            &mut state,
                            &mut tasks,
                            &mut report,
                            self.settings.store_timeout,
                            error,
                        )
                        .await);
                    }

                    continue;
                }
                RenewalTurn::NotDue => {}
            }

            if state.has_persistence() || state.has_rejected() {
                match leases::protect_store_call(
                    &self.store,
                    &self.settings,
                    &mut state,
                    &mut report,
                )
                .await
                {
                    StoreSafety::Safe => {}
                    StoreSafety::Renewed(outcome) => {
                        if let Some(error) = renewal_error(outcome) {
                            return Err(shutdown::store_failure(
                                &self.store,
                                &mut state,
                                &mut tasks,
                                &mut report,
                                self.settings.store_timeout,
                                error,
                            )
                            .await);
                        }

                        continue;
                    }
                }
            }

            match accounting::persist_ready(
                &self.store,
                self.settings.store_timeout,
                &mut state,
                &mut report,
            )
            .await
            {
                PersistenceTurn::Permanent(error) => {
                    return Err(shutdown::store_failure(
                        &self.store,
                        &mut state,
                        &mut tasks,
                        &mut report,
                        self.settings.store_timeout,
                        error,
                    )
                    .await);
                }
                PersistenceTurn::Progressed => continue,
                PersistenceTurn::Idle => {}
            }

            match accounting::persist_rejected_ready(
                &self.store,
                self.settings.store_timeout,
                &mut state,
                &mut report,
            )
            .await
            {
                PersistenceTurn::Permanent(error) => {
                    return Err(shutdown::store_failure(
                        &self.store,
                        &mut state,
                        &mut tasks,
                        &mut report,
                        self.settings.store_timeout,
                        error,
                    )
                    .await);
                }
                PersistenceTurn::Progressed => continue,
                PersistenceTurn::Idle => {}
            }

            let claim_eligible = !state.has_rejected() && state.available() > 0;

            let claim_safe = claim_eligible
                && state
                    .store_call_blockers(Instant::now(), self.settings.store_timeout)
                    .is_empty();

            let claim_due = claim_safe && Instant::now() >= next_claim;

            if claim_due {
                if let Some(error) = claim::available(
                    &self.store,
                    &self.settings,
                    &mut state,
                    &mut report,
                    &mut next_claim,
                )
                .await
                {
                    return Err(shutdown::store_failure(
                        &self.store,
                        &mut state,
                        &mut tasks,
                        &mut report,
                        self.settings.store_timeout,
                        error,
                    )
                    .await);
                }

                continue;
            }

            let wake_at = match (state.next_renewal(), claim_safe.then_some(next_claim)) {
                (Some(renewal), Some(claim)) => Some(renewal.min(claim)),
                (Some(renewal), None) => Some(renewal),
                (None, Some(claim)) => Some(claim),
                (None, None) => None,
            };

            let readiness = async move {
                match wake_at {
                    Some(wake_at) => tokio::time::sleep_until(wake_at).await,
                    None => std::future::pending().await,
                }
            };

            tokio::select! {
                biased;
                () = cancellation.cancelled() => {},
                joined = tasks.join_next_with_id(), if state.has_tasks() => {
                    if let Some(joined) = joined {
                        match publish::finish_join(
                            joined,
                            &self.settings.retry_policy,
                            &mut state,
                        ) {
                            Ok(JoinOutcome::Retired) => report.aborted += 1,
                            Ok(JoinOutcome::Finished) => {}
                            Err(error) => {
                                return Err(shutdown::publisher_failure(
                                    &self.store,
                                    &mut state,
                                    &mut tasks,
                                    &mut report,
                                    self.settings.store_timeout,
                                    error,
                                )
                                .await);
                            }
                        }
                    }
                }
                () = readiness => {},
            }
        }
    }
}

fn renewal_error<E>(outcome: RenewalOutcome<E>) -> Option<E> {
    match outcome {
        RenewalOutcome::Completed { lost } if lost > 0 => {
            tracing::warn!(
                target: "messaging.outbox",
                operation = "extend_lease",
                shortfall = lost,
                "claim fencing shortfall"
            );

            None
        }
        RenewalOutcome::Failed { permanent_error } => {
            tracing::warn!(
                target: "messaging.outbox",
                operation = "extend_lease",
                "lease renewal failed; claims will be released"
            );

            permanent_error
        }
        RenewalOutcome::Completed { .. } => None,
    }
}
