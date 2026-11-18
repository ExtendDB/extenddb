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

use crate::helpers::{
    IsolatedAccount, IsolatedEngine, isolated_account_keyspaces, isolated_engine,
    isolated_gsi_account,
};
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

/// Three accounts on one keyspace prefix of their own, in the order a
/// worker pass reads their keyspaces.
struct Fixture {
    engine: std::sync::Arc<CassandraEngine>,
    settings: CassandraCatalogStore,
    /// `accounts[0]` is read first in a pass, `accounts[2]` last.
    accounts: Vec<IsolatedAccount>,
    _isolated: IsolatedEngine,
}

async fn fixture() -> Fixture {
    let isolated = isolated_engine("gwp").await;
    let mut accounts = Vec::new();
    for _ in 0..3 {
        accounts.push(isolated_gsi_account(&isolated, "GsiWorkerPass", "GsiIdx", "gpk").await);
    }
    // A pass reads keyspaces in `system_schema` order, which follows the
    // token of the keyspace name, not the order the test made them.
    let listed = isolated_account_keyspaces(&isolated).await;
    accounts.sort_by_key(|account| {
        listed
            .iter()
            .position(|name| *name == account.keyspace)
            .expect("each account keyspace is listed")
    });
    Fixture {
        engine: isolated.engine.clone(),
        settings: isolated.settings(),
        accounts,
        _isolated: isolated,
    }
}

impl Fixture {
    /// Queue `count` writes to `account` under a delay of `delay_ms`.
    async fn queue(&self, account: usize, tag: &str, count: usize, delay_ms: u64) {
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
                    &self.accounts[account].key_info,
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

    /// Per worker partition, the earliest `ready_at` queued in any account,
    /// and the index in `accounts` of the account that holds it.
    async fn earliest_queued_per_partition(&self) -> Vec<Option<(i64, usize)>> {
        use cdrs_tokio::types::IntoRustByName as _;
        let mut earliest: Vec<Option<(i64, usize)>> = vec![None; WORKER_PARTITIONS as usize];
        for (index, account) in self.accounts.iter().enumerate() {
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
                    let slot = &mut earliest[usize::try_from(partition).unwrap()];
                    if slot.is_none_or(|(due, _)| ready_at < due) {
                        *slot = Some((ready_at, index));
                    }
                }
            }
        }
        earliest
    }

    /// Check one phase: every partition's earliest queued row is in account
    /// `expected_holder`, and each pass reports that row's `ready_at` as its
    /// next due time.
    async fn check_next_due(&self, phase: &str, batches: &[GsiBatch], expected_holder: usize) {
        let earliest = self.earliest_queued_per_partition().await;
        for (partition, slot) in earliest.iter().enumerate() {
            let (_, holder) = slot.unwrap_or_else(|| {
                panic!("{phase}: partition {partition} holds no queued row; queue more rows")
            });
            // A check of the test's own setup: the account that holds the
            // earliest row is the one this phase needs.
            assert_eq!(
                holder, expected_holder,
                "{phase}: the earliest row of partition {partition} is in account {holder}, \
                 not in account {expected_holder}, which this phase needs"
            );
        }
        let next_due: Vec<Option<i64>> = batches.iter().map(|b| b.next_due_ms).collect();
        let expected: Vec<Option<i64>> = earliest.iter().map(|s| s.map(|(due, _)| due)).collect();
        assert_eq!(
            next_due, expected,
            "{phase}: per-partition next due time (left) is not the earliest queued row (right)"
        );
    }
}

/// A pass reads item images only for due rows, and its next due time is the
/// earliest not-yet-due row of its partition across all account keyspaces.
///
/// The three accounts are numbered in the order a pass reads them.
/// - Phase 1: account 0 holds one due row and rows due in 30 to 60 s.
///   Account 1 holds rows due in 60 to 120 s. The earliest rows are in
///   account 0, which is not the last account a pass reads.
/// - Phase 2: account 2 gets rows due in 15 to 30 s. Its queue was empty, so
///   no earlier row holds them back. The earliest rows are now in account 2,
///   which is not the first account a pass reads.
///
/// So a pass that keeps the next due time of the last keyspace it reads fails
/// phase 1, and a pass that keeps the first keyspace's fails phase 2.
#[tokio::test]
async fn pass_reads_images_of_due_rows_only_and_reports_earliest_due() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let f = fixture().await;

    // Phase 1.
    f.queue(0, "due", 1, 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    f.queue(0, "mid", ROWS_PER_BATCH, 60_000).await;
    f.queue(1, "late", ROWS_PER_BATCH, 120_000).await;
    let not_due_phase1 = 2 * ROWS_PER_BATCH;

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
    // A pass that applied a row does not read the next due time: the worker
    // starts another pass at once. That next pass applies nothing.
    let batches = f.pass_every_worker().await;
    let applied: usize = batches.iter().map(|b| b.applied).sum();
    assert_eq!(applied, 0, "phase 1: no row is due on the second pass");
    f.check_next_due("phase 1", &batches, 0).await;

    // Phase 2.
    f.queue(2, "soon", ROWS_PER_BATCH, 30_000).await;
    let batches = f.pass_every_worker().await;
    let payload_rows_read: usize = batches.iter().map(|b| b.payload_rows_read).sum();
    assert_eq!(
        payload_rows_read, 0,
        "phase 2: a pass read the item images of {payload_rows_read} queued rows, none due"
    );
    f.check_next_due("phase 2", &batches, 2).await;
}
