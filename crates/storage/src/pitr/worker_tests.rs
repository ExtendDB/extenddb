// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The retention worker: its first sweep an hour after start, and the
//! `PitrRetention` worker metric of each sweep.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use extenddb_core::metrics::{Dimension, MetricName, MetricsCollector, MetricsQuery, QuerySource};
use extenddb_core::types::TableKeyInfo;
use futures::future::BoxFuture;

use super::{PitrCaptureStore, PitrState, ReconstructPage, retention_worker};
use crate::error::StorageError;
use crate::hooks::CancellationToken;

/// A store with one enabled table whose prune removes 3 rows, or fails when
/// `fail` is set, counting the sweeps (`pitr_tables` calls).
struct Store {
    sweeps: AtomicU32,
    fail: bool,
}

impl PitrCaptureStore for Store {
    fn pitr_state<'a>(
        &'a self,
        _: &'a str,
    ) -> BoxFuture<'a, Result<Option<PitrState>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn enable_pitr<'a>(
        &'a self,
        _: &'a TableKeyInfo,
        _: Option<u8>,
        _: SystemTime,
    ) -> BoxFuture<'a, Result<PitrState, StorageError>> {
        unreachable!("the worker never enables")
    }

    fn pitr_tables(&self) -> BoxFuture<'_, Result<Vec<(String, PitrState)>, StorageError>> {
        self.sweeps.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(vec![(
                "t".to_owned(),
                PitrState::new(1, SystemTime::UNIX_EPOCH),
            )])
        })
    }

    fn remove_orphaned_history(&self, _: u32) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async { Ok(0) })
    }

    fn disable_pitr<'a>(&'a self, _: &'a TableKeyInfo) -> BoxFuture<'a, Result<(), StorageError>> {
        unreachable!("the worker never disables")
    }

    fn reconstruct_page<'a>(
        &'a self,
        _: &'a str,
        _: SystemTime,
        _: Option<i64>,
        _: u32,
    ) -> BoxFuture<'a, Result<ReconstructPage, StorageError>> {
        unreachable!("the worker never reconstructs")
    }

    fn prune_before<'a>(
        &'a self,
        _: &'a str,
        _: SystemTime,
        _: u32,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        let fail = self.fail;
        Box::pin(async move {
            if fail {
                Err(StorageError::Internal("prune failed".to_owned()))
            } else {
                Ok(3)
            }
        })
    }
}

/// The worker metric `metric` recorded under `PitrRetention`, as its count.
fn recorded(metrics: &MetricsCollector, metric: MetricName) -> u64 {
    metrics
        .query(&MetricsQuery {
            metric: Some(metric),
            ..Default::default()
        })
        .iter()
        .filter(|m| {
            m.dimensions.contains(&Dimension::Operation(
                QuerySource::PitrRetention.to_string(),
            ))
        })
        .map(|m| m.count)
        .sum()
}

async fn run(fail: bool) -> (Arc<Store>, Arc<MetricsCollector>, Vec<u32>) {
    let store = Arc::new(Store {
        sweeps: AtomicU32::new(0),
        fail,
    });
    let metrics = Arc::new(MetricsCollector::new());
    let token = CancellationToken::new();
    let worker = tokio::spawn(retention_worker(
        store.clone(),
        metrics.clone(),
        token.clone(),
    ));
    let mut seen = Vec::new();
    for wait in [3_599, 2, 3_600] {
        tokio::time::sleep(Duration::from_secs(wait)).await;
        seen.push(store.sweeps.load(Ordering::SeqCst));
    }
    token.cancel();
    worker.await.expect("the worker stops at shutdown");
    (store, metrics, seen)
}

/// The first sweep runs an hour after start and one more each hour; each
/// success records `WorkerLastSuccess` for `PitrRetention`.
#[tokio::test(start_paused = true)]
async fn the_worker_sweeps_hourly_and_records_each_success() {
    let (_, metrics, seen) = run(false).await;
    assert_eq!(seen, vec![0, 1, 2]);
    assert_eq!(recorded(&metrics, MetricName::WorkerLastSuccess), 2);
    assert_eq!(recorded(&metrics, MetricName::WorkerErrorCount), 0);
}

/// A sweep that fails records `WorkerErrorCount` for `PitrRetention`, and
/// the worker keeps running.
#[tokio::test(start_paused = true)]
async fn a_failed_sweep_records_a_worker_error() {
    let (_, metrics, seen) = run(true).await;
    assert_eq!(seen, vec![0, 1, 2]);
    assert_eq!(recorded(&metrics, MetricName::WorkerErrorCount), 2);
    assert_eq!(recorded(&metrics, MetricName::WorkerLastSuccess), 0);
}
