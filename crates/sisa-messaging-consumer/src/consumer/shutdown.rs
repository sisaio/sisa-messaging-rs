//! Bounded drain after receiving stops for any cause.

use std::time::Duration;

use super::worker::Workers;

/// Lets in-flight deliveries finish and settle for at most `drain_timeout`.
///
/// One global deadline starts when receiving stops. At the deadline every coordinator is
/// aborted, which aborts its workflow and leaves its delivery unsettled; failures observed while
/// draining are still recorded. The drain ends only after every tracked workflow task, and so
/// every transaction, has been dropped.
pub(super) async fn drain(workers: &mut Workers, drain_timeout: Duration) {
    let deadline = tokio::time::sleep(drain_timeout);

    tokio::pin!(deadline);

    loop {
        tokio::select! {
            biased;
            joined = workers.coordinators.join_next() => match joined {
                Some(joined) => workers.record(joined),
                None => break,
            },
            () = &mut deadline => {
                workers.coordinators.abort_all();

                while let Some(joined) = workers.coordinators.join_next().await {
                    workers.record(joined);
                }

                break;
            }
        }
    }

    workers.tracker.close();
    workers.tracker.wait().await;
}
