// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What one GSI worker pass reads (#407).
//!
//! A pass reads item images only for rows that are due, and reports the
//! earliest not-yet-due row across every account keyspace as the time it has
//! work again.
//!
//! The workers do not run here: the test calls `run_gsi_pass` for each worker
//! partition, so nothing applies a row behind its back.

use crate::helpers::{IsolatedAccount, IsolatedEngine, isolated_engine, isolated_gsi_account};
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::DataEngine as _;
use extenddb_storage::management_store::SettingsStore;
use extenddb_storage_cassandra::workers::{GsiBatch, run_gsi_pass};
use extenddb_storage_cassandra::{CassandraCatalogStore, CassandraEngine};

/// Worker partitions, one per GSI worker (`gsi_queue::NUM_WORKERS`).
const WORKER_PARTITIONS: u64 = 4;
/// Rows queued per batch: enough that every partition gets some.
const ROWS_PER_BATCH: usize = 50;

/// Two accounts on one keyspace prefix of their own.
struct Fixture {
    engine: std::sync::Arc<CassandraEngine>,
    settings: CassandraCatalogStore,
    first: IsolatedAccount,
    second: IsolatedAccount,
    _isolated: IsolatedEngine,
}

async fn fixture() -> Fixture {
    let isolated = isolated_engine("gwp").await;
    let first = isolated_gsi_account(&isolated, "GsiWorkerPass", "GsiIdx", "gpk").await;
    let second = isolated_gsi_account(&isolated, "GsiWorkerPass", "GsiIdx", "gpk").await;
    Fixture {
        engine: isolated.engine.clone(),
        settings: isolated.settings(),
        first,
        second,
        _isolated: isolated,
    }
}

impl Fixture {
    /// Queue `count` writes to `account` under a delay of `delay_ms`.
    async fn queue(&self, account: &IsolatedAccount, tag: &str, count: usize, delay_ms: u64) {
        self.settings
            .set_setting("index_propagation_delay_ms", &delay_ms.to_string())
            .await
            .unwrap();
        for i in 0..count {
            let mut item = Item::new();
            item.insert("id".to_string(), AttributeValue::S(format!("{tag}-{i}")));
            item.insert("gpk".to_string(), AttributeValue::S(tag.to_string()));
            self.engine
                .put_item(
                    &account.key_info,
                    item,
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await
                .expect("put_item");
        }
    }

    /// One pass of each worker.
    async fn pass_every_worker(&self) -> Vec<GsiBatch> {
        let mut batches = Vec::new();
        for worker_id in 0..WORKER_PARTITIONS {
            batches.push(run_gsi_pass(&self.engine, worker_id).await.unwrap());
        }
        batches
    }

    /// Per worker partition, the earliest `ready_at` queued in either
    /// account: what each pass must report as its next due time.
    async fn earliest_queued_per_partition(&self) -> Vec<Option<i64>> {
        use cdrs_tokio::types::IntoRustByName as _;
        let mut earliest = vec![None; WORKER_PARTITIONS as usize];
        for account in [&self.first, &self.second] {
            let rows = self
                .engine
                .session()
                .query(&format!(
                    "SELECT worker_partition, ready_at FROM {}.gsi_pending",
                    account.keyspace
                ))
                .await
                .unwrap()
                .response_body()
                .unwrap()
                .into_rows()
                .unwrap_or_default();
            for row in rows {
                let partition: i32 = row.get_r_by_name("worker_partition").unwrap();
                let ready_at: Option<i64> = row.get_by_name("ready_at").ok().flatten();
                if let Some(ready_at) = ready_at {
                    let slot: &mut Option<i64> = &mut earliest[usize::try_from(partition).unwrap()];
                    *slot = Some(slot.map_or(ready_at, |due: i64| due.min(ready_at)));
                }
            }
        }
        earliest
    }
}

/// A pass reads item images only for due rows, and its next due time is
/// the earliest not-yet-due row of its partition across both accounts.
///
/// Phase 1: the first account holds one due row and rows due in 120 s, the
/// second account rows due in 60 s. Phase 2 adds rows due in 30 s to the
/// first account. In each partition the earliest row is the second
/// account's in phase 1 and the first account's in phase 2, so whichever
/// keyspace a pass reads last, one phase has the earliest row elsewhere.
#[tokio::test]
async fn pass_reads_images_of_due_rows_only_and_reports_earliest_due() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let f = fixture().await;

    // Phase 1.
    f.queue(&f.first, "due", 1, 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    f.queue(&f.first, "late", ROWS_PER_BATCH, 120_000).await;
    f.queue(&f.second, "mid", ROWS_PER_BATCH, 60_000).await;
    let not_due_phase1 = 2 * ROWS_PER_BATCH;

    let earliest_after_apply = {
        // The due row is applied in this pass; the rest stay queued.
        let batches = f.pass_every_worker().await;
        let applied: usize = batches.iter().map(|b| b.applied).sum();
        let payload_rows_read: usize = batches.iter().map(|b| b.payload_rows_read).sum();
        assert_eq!(applied, 1, "phase 1: one row was due");
        assert_eq!(
            payload_rows_read, 1,
            "phase 1: a pass read the item images of {payload_rows_read} queued rows; \
             only the 1 due row needs them ({not_due_phase1} were not due)"
        );
        let next_due: Vec<Option<i64>> = batches.iter().map(|b| b.next_due_ms).collect();
        (next_due, f.earliest_queued_per_partition().await)
    };
    assert_eq!(
        earliest_after_apply.0, earliest_after_apply.1,
        "phase 1: per-partition next due time (left) is not the earliest queued row (right)"
    );

    // Phase 2.
    f.queue(&f.first, "soon", ROWS_PER_BATCH, 30_000).await;
    let batches = f.pass_every_worker().await;
    let payload_rows_read: usize = batches.iter().map(|b| b.payload_rows_read).sum();
    assert_eq!(
        payload_rows_read, 0,
        "phase 2: a pass read the item images of {payload_rows_read} queued rows, none due"
    );
    let next_due: Vec<Option<i64>> = batches.iter().map(|b| b.next_due_ms).collect();
    assert_eq!(
        next_due,
        f.earliest_queued_per_partition().await,
        "phase 2: per-partition next due time (left) is not the earliest queued row (right)"
    );
}
