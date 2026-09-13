use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sisa_messaging::FailureKind;
use sisa_messaging_outbox::{
    Claim, ClaimBatch, ClaimRequest, ClaimedRecord, FailureRecord, FencedClaims, OutboxStore,
    PoisonReport,
};

use super::ProtocolError;

pub(crate) const COMPLETE_ALL: u8 = 0;
pub(crate) const COMPLETE_FIRST: u8 = 1;
pub(crate) const COMPLETE_TRANSIENT_ONCE: u8 = 2;
pub(crate) const COMPLETE_PERMANENT: u8 = 3;
pub(crate) const COMPLETE_NONE: u8 = 4;
pub(crate) const FAIL_ALL: u8 = 0;
pub(crate) const FAIL_NONE: u8 = 1;
pub(crate) const FAIL_TRANSIENT: u8 = 2;
pub(crate) const RENEW_ALL: u8 = 0;
pub(crate) const RENEW_NONE: u8 = 1;
pub(crate) const RELEASE_ALL: u8 = 0;
pub(crate) const RELEASE_TRANSIENT: u8 = 1;
pub(crate) const RELEASE_PERMANENT: u8 = 2;
pub(crate) const RELEASE_NONE: u8 = 3;

#[derive(Default)]
pub(crate) struct StoreState {
    pub(crate) records: VecDeque<ClaimedRecord>,
    pub(crate) poison: PoisonReport,
    pub(crate) completes: Vec<Vec<Claim>>,
    pub(crate) failures: Vec<Vec<FailureRecord>>,
    pub(crate) releases: Vec<Vec<Claim>>,
    pub(crate) renewals: Vec<Vec<Claim>>,
    pub(crate) operations: Vec<&'static str>,
}

#[derive(Clone)]
pub(crate) struct FakeStore {
    state: Arc<Mutex<StoreState>>,
    pub(crate) claim_calls: Arc<AtomicUsize>,
    pub(crate) claim_entered: Arc<AtomicBool>,
    pub(crate) claim_delay_ms: Arc<AtomicUsize>,
    pub(crate) claim_limit_extra: Arc<AtomicUsize>,
    pub(crate) complete_entered: Arc<AtomicBool>,
    pub(crate) complete_delay_ms: Arc<AtomicUsize>,
    pub(crate) fail_entered: Arc<AtomicBool>,
    pub(crate) fail_delay_ms: Arc<AtomicUsize>,
    pub(crate) release_entered: Arc<AtomicBool>,
    pub(crate) release_delay_ms: Arc<AtomicUsize>,
    pub(crate) renew_entered: Arc<AtomicBool>,
    pub(crate) renew_delay_ms: Arc<AtomicUsize>,
    pub(crate) complete_mode: Arc<AtomicU8>,
    pub(crate) fail_mode: Arc<AtomicU8>,
    pub(crate) renew_mode: Arc<AtomicU8>,
    pub(crate) release_mode: Arc<AtomicU8>,
}

impl FakeStore {
    pub(crate) fn new(records: Vec<ClaimedRecord>) -> Self {
        Self {
            state: Arc::new(Mutex::new(StoreState {
                records: records.into(),
                ..StoreState::default()
            })),
            claim_calls: Arc::new(AtomicUsize::new(0)),
            claim_entered: Arc::new(AtomicBool::new(false)),
            claim_delay_ms: Arc::new(AtomicUsize::new(0)),
            claim_limit_extra: Arc::new(AtomicUsize::new(0)),
            complete_entered: Arc::new(AtomicBool::new(false)),
            complete_delay_ms: Arc::new(AtomicUsize::new(0)),
            fail_entered: Arc::new(AtomicBool::new(false)),
            fail_delay_ms: Arc::new(AtomicUsize::new(0)),
            release_entered: Arc::new(AtomicBool::new(false)),
            release_delay_ms: Arc::new(AtomicUsize::new(0)),
            renew_entered: Arc::new(AtomicBool::new(false)),
            renew_delay_ms: Arc::new(AtomicUsize::new(0)),
            complete_mode: Arc::new(AtomicU8::new(COMPLETE_ALL)),
            fail_mode: Arc::new(AtomicU8::new(FAIL_ALL)),
            renew_mode: Arc::new(AtomicU8::new(RENEW_ALL)),
            release_mode: Arc::new(AtomicU8::new(RELEASE_ALL)),
        }
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, StoreState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl OutboxStore for FakeStore {
    type Error = ProtocolError;

    async fn claim(&self, request: ClaimRequest) -> Result<ClaimBatch, Self::Error> {
        self.claim_calls.fetch_add(1, Ordering::SeqCst);
        self.claim_entered.store(true, Ordering::SeqCst);
        self.lock().operations.push("claim");
        let delay = self.claim_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        }
        let mut state = self.lock();
        let count = usize::try_from(request.limit.get())
            .unwrap_or(usize::MAX)
            .saturating_add(self.claim_limit_extra.load(Ordering::SeqCst))
            .min(state.records.len());
        let records = state.records.drain(..count).collect();
        let poison = std::mem::take(&mut state.poison);
        Ok(ClaimBatch { records, poison })
    }

    async fn complete(&self, claims: &[Claim]) -> Result<FencedClaims, Self::Error> {
        self.complete_entered.store(true, Ordering::SeqCst);
        self.lock().operations.push("complete");
        let delay = self.complete_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        }
        self.lock().completes.push(claims.to_vec());
        let mode = self.complete_mode.load(Ordering::SeqCst);
        if mode == COMPLETE_TRANSIENT_ONCE {
            self.complete_mode.store(COMPLETE_ALL, Ordering::SeqCst);
            return Err(ProtocolError {
                kind: FailureKind::Transient,
            });
        }
        if mode == COMPLETE_PERMANENT {
            return Err(ProtocolError {
                kind: FailureKind::Permanent,
            });
        }
        let confirmed = match mode {
            COMPLETE_FIRST => claims.first().copied().into_iter().collect(),
            COMPLETE_NONE => Vec::new(),
            _ => claims.to_vec(),
        };
        Ok(FencedClaims { confirmed })
    }

    async fn fail(&self, failures: &[FailureRecord]) -> Result<FencedClaims, Self::Error> {
        self.fail_entered.store(true, Ordering::SeqCst);
        self.lock().operations.push("fail");
        let delay = self.fail_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        }
        self.lock().failures.push(failures.to_vec());
        match self.fail_mode.load(Ordering::SeqCst) {
            FAIL_TRANSIENT => Err(ProtocolError {
                kind: FailureKind::Transient,
            }),
            FAIL_NONE => Ok(FencedClaims {
                confirmed: Vec::new(),
            }),
            _ => Ok(FencedClaims {
                confirmed: failures.iter().map(|failure| failure.claim).collect(),
            }),
        }
    }

    async fn release(&self, claims: &[Claim]) -> Result<FencedClaims, Self::Error> {
        self.release_entered.store(true, Ordering::SeqCst);
        self.lock().operations.push("release");
        let delay = self.release_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        }
        self.lock().releases.push(claims.to_vec());
        match self.release_mode.load(Ordering::SeqCst) {
            RELEASE_TRANSIENT => {
                return Err(ProtocolError {
                    kind: FailureKind::Transient,
                });
            }
            RELEASE_PERMANENT => {
                return Err(ProtocolError {
                    kind: FailureKind::Permanent,
                });
            }
            RELEASE_NONE => {
                return Ok(FencedClaims {
                    confirmed: Vec::new(),
                });
            }
            _ => {}
        }
        Ok(FencedClaims {
            confirmed: claims.to_vec(),
        })
    }

    async fn extend_lease(
        &self,
        claims: &[Claim],
        _lease: Duration,
    ) -> Result<FencedClaims, Self::Error> {
        self.renew_entered.store(true, Ordering::SeqCst);
        self.lock().operations.push("renew");
        let delay = self.renew_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        }
        self.lock().renewals.push(claims.to_vec());
        let confirmed = if self.renew_mode.load(Ordering::SeqCst) == RENEW_NONE {
            Vec::new()
        } else {
            claims.to_vec()
        };
        Ok(FencedClaims { confirmed })
    }
}
