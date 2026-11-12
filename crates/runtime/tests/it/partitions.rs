//! The partition maintenance worker, on a store that records what it was asked.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use sisa_eventing_core::{
    Date, Expiry, PartitionInfo, PartitionInterval, PartitionLock, PartitionStore, StoreError,
    Timestamp,
};
use sisa_eventing_runtime::{Bus, InMemoryTransport, PartitionMode, PartitionSettings};
use sisa_eventing_testing::eventually;

const AN_HOUR: Duration = Duration::from_secs(3600);

#[derive(Debug, Default)]
struct State {
    held_elsewhere: bool,
    locks: usize,
    ensured: Vec<(PartitionInterval, Date, u32)>,
    asked_older_than: Vec<Date>,
    old: Vec<PartitionInfo>,
    expired: Vec<(String, Expiry)>,
}

#[derive(Debug, Clone, Default)]
struct Store(Arc<Mutex<State>>);

impl Store {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.0.lock().unwrap()
    }
}

// Records each call instead of running DDL.
impl PartitionStore for Store {
    async fn try_lock(&self) -> Result<Option<PartitionLock>, StoreError> {
        let mut state = self.state();
        state.locks += 1;
        Ok((!state.held_elsewhere).then(|| PartitionLock::new(())))
    }

    async fn ensure_partitions(
        &self,
        interval: PartitionInterval,
        from: Date,
        count: u32,
    ) -> Result<(), StoreError> {
        self.state().ensured.push((interval, from, count));
        Ok(())
    }

    async fn default_rows(&self) -> Result<u64, StoreError> {
        Ok(0)
    }

    async fn expirable(&self, older_than: Date) -> Result<Vec<PartitionInfo>, StoreError> {
        let mut state = self.state();
        state.asked_older_than.push(older_than);
        Ok(state.old.clone())
    }

    async fn expire(&self, partition: &PartitionInfo, mode: Expiry) -> Result<(), StoreError> {
        self.state().expired.push((partition.name.clone(), mode));
        Ok(())
    }
}

async fn maintained(store: &Store, settings: PartitionSettings) -> Bus<InMemoryTransport> {
    Bus::builder("/orders", InMemoryTransport::new())
        .partitions(store.clone(), settings)
        .start()
        .await
        .unwrap()
}

fn old(name: &str, pending: u64, failed: u64) -> PartitionInfo {
    let day = Date::new(2026, 1, 1).unwrap();
    let mut partition = PartitionInfo::new(name, day, day.add_days(1));
    partition.pending = pending;
    partition.failed = failed;
    partition
}

#[tokio::test(start_paused = true)]
async fn creates_partitions_now_and_every_hour_until_shutdown() {
    let store = Store::default();
    let bus = maintained(&store, PartitionSettings::default()).await;

    eventually("the first run", async || store.state().ensured.len() == 1).await;
    tokio::time::sleep(AN_HOUR).await;
    eventually("the second run", async || store.state().ensured.len() == 2).await;
    bus.shutdown().await;
    tokio::time::sleep(3 * AN_HOUR).await;

    let today = Timestamp::now().date();
    assert_eq!(
        store.state().ensured,
        [(PartitionInterval::Day, today, 8); 2]
    );
}

#[tokio::test(start_paused = true)]
async fn skips_while_another_instance_holds_the_lock() {
    let store = Store::default();
    store.state().held_elsewhere = true;
    let bus = maintained(&store, PartitionSettings::default()).await;

    eventually("the lock to be tried", async || store.state().locks == 1).await;
    bus.shutdown().await;

    assert!(store.state().ensured.is_empty());
}

#[tokio::test(start_paused = true)]
async fn keeps_every_partition_without_a_retention() {
    let store = Store::default();
    store.state().old.push(old("old", 0, 0));
    let bus = maintained(&store, PartitionSettings::default()).await;

    eventually("the first run", async || store.state().ensured.len() == 1).await;
    bus.shutdown().await;

    assert!(store.state().asked_older_than.is_empty());
    assert!(store.state().expired.is_empty());
}

#[tokio::test(start_paused = true)]
async fn expires_partitions_past_the_retention_except_those_with_unsent_rows() {
    let store = Store::default();
    store
        .state()
        .old
        .extend([old("sent", 0, 0), old("pending", 1, 0), old("failed", 0, 1)]);
    let mut settings = PartitionSettings::default();
    settings.retention_days = Some(30);
    settings.on_expiry = Expiry::Drop;
    let bus = maintained(&store, settings).await;

    eventually("the expiry", async || !store.state().expired.is_empty()).await;
    bus.shutdown().await;

    let today = Timestamp::now().date();
    assert_eq!(store.state().asked_older_than, [today.add_days(-30)]);
    assert_eq!(store.state().expired, [("sent".to_owned(), Expiry::Drop)]);
}

#[tokio::test(start_paused = true)]
async fn starts_no_worker_for_external_partitions() {
    let store = Store::default();
    let mut settings = PartitionSettings::default();
    settings.partitions = PartitionMode::External;
    let bus = maintained(&store, settings).await;

    tokio::time::sleep(AN_HOUR).await;
    bus.shutdown().await;

    assert_eq!(store.state().locks, 0);
}
