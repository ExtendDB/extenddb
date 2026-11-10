// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend-independent checks of a [`PitrCaptureStore`], run by each backend's
//! own tests against a real database through a [`HistoryFixture`].
//!
//! The reconstruct rule is the one measured on the live service: a restore to
//! T holds the latest image of each key committed at or before T, and a key
//! deleted by T is absent (a restore between the third and fourth steps of
//! the four-step timeline below returned the one surviving item, two runs).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use extenddb_core::types::{AttributeValue, Item, ScalarAttributeType, TableKeyInfo};
use futures::future::BoxFuture;

use super::PitrCaptureStore;
use crate::DataEngine;

mod enable;
mod pins;
mod retention;
mod writes;
pub use enable::*;
pub use pins::*;
pub use retention::*;
pub use writes::*;

/// What a backend's test supplies: tables, the in-transaction record, and a
/// row count of the history.
pub trait HistoryFixture {
    /// The store under test.
    fn store(&self) -> &dyn PitrCaptureStore;

    /// The backend's data path, whose writes the hook records.
    fn data(&self) -> &dyn DataEngine;

    /// A table keyed on `pk` (S) and, when `sk` is given, a sort key `sk` of
    /// that type, with recovery enabled when `enabled`. Names are unique
    /// within one fixture.
    fn table<'a>(
        &'a self,
        name: &'a str,
        sk: Option<ScalarAttributeType>,
        enabled: bool,
    ) -> BoxFuture<'a, TableKeyInfo>;

    /// Record one mutation through the backend's write-transaction hook,
    /// then commit the transaction, or roll it back when `commit` is false.
    /// Returns what the hook returned.
    fn record<'a>(
        &'a self,
        info: &'a TableKeyInfo,
        key: &'a Item,
        image: Option<&'a Item>,
        at: SystemTime,
        commit: bool,
    ) -> BoxFuture<'a, bool>;

    /// The number of history rows of `table_id`.
    fn history_rows<'a>(&'a self, table_id: &'a str) -> BoxFuture<'a, i64>;

    /// The commit times of `table_id`'s history rows, in `seq` order.
    fn commit_times<'a>(&'a self, table_id: &'a str) -> BoxFuture<'a, Vec<i64>>;

    /// Every item the table's data table holds now.
    fn live_items<'a>(&'a self, info: &'a TableKeyInfo) -> BoxFuture<'a, Vec<Item>>;

    /// Run one SQL statement on the database that holds `pitr_state`, to put
    /// a row in a state the store's own calls do not reach.
    fn execute<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, ()>;
}

/// A fixed instant `secs` seconds after an arbitrary epoch.
#[must_use]
pub fn at(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_800_000_000 + secs)
}

pub(super) fn s(pk: &str, v: &str) -> Item {
    Item::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("v".to_owned(), AttributeValue::S(v.to_owned())),
    ])
}

fn keyed(pk: &str, sk: AttributeValue, v: &str) -> Item {
    Item::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("sk".to_owned(), sk),
        ("v".to_owned(), AttributeValue::S(v.to_owned())),
    ])
}

/// An item as a backend reads it back: parsed again from its JSON, which
/// normalizes a number's spelling.
fn as_read(item: &Item) -> Item {
    serde_json::from_value(serde_json::to_value(item).expect("json")).expect("item")
}

pub(super) fn sorted(items: &[Item]) -> Vec<String> {
    let mut v: Vec<String> = items
        .iter()
        .map(|i| serde_json::to_string(&as_read(i)).expect("json"))
        .collect();
    v.sort();
    v
}

async fn commit<F: HistoryFixture>(
    f: &F,
    info: &TableKeyInfo,
    key: &Item,
    image: Option<&Item>,
    t: SystemTime,
) -> bool {
    f.record(info, key, image, t, true).await
}

/// Every item of the state at `t`, read in pages of `limit`, sorted.
pub async fn state<F: HistoryFixture>(
    f: &F,
    info: &TableKeyInfo,
    t: SystemTime,
    limit: u32,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let page = f
            .store()
            .reconstruct_page(&info.table_id, t, after, limit)
            .await
            .expect("reconstruct");
        assert!(page.items.len() <= limit as usize);
        out.extend(
            page.items
                .iter()
                .map(|i| serde_json::to_string(i).expect("json")),
        );
        match page.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    out.sort();
    out
}

/// The four-step timeline of the live measurement, 10 s apart: t1 put k1=v1;
/// t2 update k1=v2 and put k2=x1; t3 delete k1; t4 put k1=v3 and update k2=x2.
async fn timeline<F: HistoryFixture>(f: &F, info: &TableKeyInfo) {
    let k1 = s("k1", "");
    let k2 = s("k2", "");
    commit(f, info, &k1, Some(&s("k1", "v1")), at(10)).await;
    commit(f, info, &k1, Some(&s("k1", "v2")), at(20)).await;
    commit(f, info, &k2, Some(&s("k2", "x1")), at(20)).await;
    commit(f, info, &k1, None, at(30)).await;
    commit(f, info, &k1, Some(&s("k1", "v3")), at(40)).await;
    commit(f, info, &k2, Some(&s("k2", "x2")), at(40)).await;
}

/// A row is added only for a table with recovery enabled.
pub async fn records_only_while_recovery_is_enabled<F: HistoryFixture>(f: &F) {
    let on = f.table("on", None, true).await;
    let off = f.table("off", None, false).await;
    let k = s("k", "a");
    assert!(commit(f, &on, &k, Some(&k), at(1)).await);
    assert!(!commit(f, &off, &k, Some(&k), at(1)).await);
    assert_eq!(f.history_rows(&on.table_id).await, 1);
    assert_eq!(f.history_rows(&off.table_id).await, 0);
}

/// A row rolls back with the write's transaction.
pub async fn a_record_rolls_back_with_its_transaction<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, true).await;
    let k = s("k", "a");
    assert!(f.record(&info, &k, Some(&k), at(1), false).await);
    assert_eq!(f.history_rows(&info.table_id).await, 0);
}

/// The timeline reconstructs at each step and between steps, in pages of 1,
/// 2 and 100.
pub async fn reconstructs_the_timeline_at_each_point<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, true).await;
    timeline(f, &info).await;
    let cases: [(u64, Vec<Item>); 7] = [
        (5, vec![]),
        (10, vec![s("k1", "v1")]),
        (20, vec![s("k1", "v2"), s("k2", "x1")]),
        (25, vec![s("k1", "v2"), s("k2", "x1")]),
        (30, vec![s("k2", "x1")]),
        (35, vec![s("k2", "x1")]),
        (40, vec![s("k1", "v3"), s("k2", "x2")]),
    ];
    for (secs, expected) in cases {
        for limit in [1, 2, 100] {
            assert_eq!(
                state(f, &info, at(secs), limit).await,
                sorted(&expected),
                "at +{secs}s, page {limit}"
            );
        }
    }
}

/// A commit one microsecond after T is not in the state at T.
pub async fn sub_second_commits_count_at_their_instant<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, true).await;
    let k = s("k", "");
    commit(f, &info, &k, Some(&s("k", "a")), at(10)).await;
    let later = at(10) + Duration::from_micros(1);
    commit(f, &info, &k, Some(&s("k", "b")), later).await;
    assert_eq!(state(f, &info, at(10), 10).await, sorted(&[s("k", "a")]));
    assert_eq!(state(f, &info, later, 10).await, sorted(&[s("k", "b")]));
}

/// Commits of one key at one instant resolve in commit order.
pub async fn commits_at_one_instant_resolve_in_commit_order<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, true).await;
    let k = s("k", "");
    commit(f, &info, &k, Some(&s("k", "a")), at(10)).await;
    commit(f, &info, &k, None, at(10)).await;
    commit(f, &info, &k, Some(&s("k", "c")), at(10)).await;
    assert_eq!(state(f, &info, at(10), 10).await, sorted(&[s("k", "c")]));
}

/// A sort key of each type identifies an item as the data table does: a
/// number by its value, whatever its spelling, and two sort keys under one
/// partition key as two items.
pub async fn sort_keys_of_every_type_identify_items<F: HistoryFixture>(f: &F) {
    let cases = [
        (
            ScalarAttributeType::S,
            AttributeValue::S("a".to_owned()),
            AttributeValue::S("a".to_owned()),
            AttributeValue::S("b".to_owned()),
        ),
        (
            ScalarAttributeType::N,
            AttributeValue::N("1".to_owned()),
            AttributeValue::N("1.0".to_owned()),
            AttributeValue::N("2".to_owned()),
        ),
        (
            ScalarAttributeType::B,
            AttributeValue::B(vec![0, 1]),
            AttributeValue::B(vec![0, 1]),
            AttributeValue::B(vec![0, 2]),
        ),
    ];
    for (i, (ty, first, same, sibling_sk)) in cases.into_iter().enumerate() {
        let info = f.table(&format!("t{i}"), Some(ty), true).await;
        let a = keyed("p", first.clone(), "1");
        let b = keyed("p", same, "2");
        let sibling = keyed("p", sibling_sk, "4");
        let other = keyed("q", first, "3");
        commit(f, &info, &a, Some(&a), at(1)).await;
        commit(f, &info, &sibling, Some(&sibling), at(1)).await;
        commit(f, &info, &b, Some(&b), at(2)).await;
        commit(f, &info, &other, Some(&other), at(3)).await;
        assert_eq!(
            state(f, &info, at(3), 10).await,
            sorted(&[b.clone(), sibling.clone(), other.clone()]),
            "{ty:?}"
        );
        commit(f, &info, &b, None, at(4)).await;
        assert_eq!(
            state(f, &info, at(4), 10).await,
            sorted(&[sibling, other]),
            "{ty:?}"
        );
    }
}

/// A table reads and prunes only its own history.
pub async fn tables_do_not_see_each_others_history<F: HistoryFixture>(f: &F) {
    let a = f.table("a", None, true).await;
    let b = f.table("b", None, true).await;
    commit(f, &a, &s("k", ""), Some(&s("k", "a")), at(1)).await;
    commit(f, &b, &s("k", ""), None, at(2)).await;
    assert_eq!(state(f, &a, at(5), 10).await, sorted(&[s("k", "a")]));
    assert!(state(f, &b, at(5), 10).await.is_empty());
    assert_eq!(
        f.store()
            .prune_before(&b.table_id, at(5), 10)
            .await
            .expect("prune"),
        1
    );
    assert_eq!(f.history_rows(&a.table_id).await, 1);
}

/// Pruning keeps the state at and after the cutoff, including an item written
/// once before it and never changed.
pub async fn pruning_keeps_every_state_from_the_cutoff_on<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, true).await;
    timeline(f, &info).await;
    let old = s("old", "kept");
    commit(f, &info, &old, Some(&old), at(1)).await;
    let mut before = Vec::new();
    for t in [30, 35, 40, 50] {
        before.push(state(f, &info, at(t), 10).await);
    }
    // At +30 s: k1's v1 and v2 are superseded by its delete, and the delete is
    // then k1's only row; k2's x1 and the old item are the state.
    assert_eq!(
        f.store()
            .prune_before(&info.table_id, at(30), 100)
            .await
            .expect("prune"),
        3
    );
    assert_eq!(
        f.store()
            .prune_before(&info.table_id, at(30), 100)
            .await
            .expect("prune"),
        0
    );
    for (t, expected) in [30, 35, 40, 50].into_iter().zip(before) {
        assert_eq!(state(f, &info, at(t), 10).await, expected, "at +{t}s");
    }
    assert_eq!(f.history_rows(&info.table_id).await, 4);
}

/// A prune one row at a time never brings a deleted item back.
pub async fn a_bounded_prune_never_brings_a_deleted_item_back<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, true).await;
    let k = s("k", "");
    commit(f, &info, &k, Some(&s("k", "a")), at(1)).await;
    commit(f, &info, &k, Some(&s("k", "b")), at(2)).await;
    commit(f, &info, &k, None, at(3)).await;
    let mut total = 0;
    loop {
        let removed = f
            .store()
            .prune_before(&info.table_id, at(10), 1)
            .await
            .expect("prune");
        assert!(removed <= 1);
        assert!(
            state(f, &info, at(10), 10).await.is_empty(),
            "after {total} rows"
        );
        if removed == 0 {
            break;
        }
        total += removed;
    }
    assert_eq!(total, 3);
    assert_eq!(f.history_rows(&info.table_id).await, 0);
}

/// Rows committed after the cutoff are not pruned.
pub async fn rows_after_the_cutoff_are_not_pruned<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, true).await;
    let k = s("k", "");
    commit(f, &info, &k, Some(&s("k", "a")), at(1)).await;
    commit(f, &info, &k, None, at(20)).await;
    assert_eq!(
        f.store()
            .prune_before(&info.table_id, at(10), 100)
            .await
            .expect("prune"),
        0
    );
    assert_eq!(state(f, &info, at(10), 10).await, sorted(&[s("k", "a")]));
    assert!(state(f, &info, at(20), 10).await.is_empty());
}

/// One `#[tokio::test]` per check. `$open` is an async fn returning
/// `Option<F>` (`None` skips, for a backend without a database to test on);
/// `$close` is an async fn taking the fixture once the check is done.
#[macro_export]
macro_rules! pitr_conformance_tests {
    ($open:path, $close:path) => {
        $crate::pitr_conformance_tests!(@each $open, $close,
            records_only_while_recovery_is_enabled,
            a_record_rolls_back_with_its_transaction,
            reconstructs_the_timeline_at_each_point,
            sub_second_commits_count_at_their_instant,
            commits_at_one_instant_resolve_in_commit_order,
            sort_keys_of_every_type_identify_items,
            tables_do_not_see_each_others_history,
            pruning_keeps_every_state_from_the_cutoff_on,
            a_bounded_prune_never_brings_a_deleted_item_back,
            rows_after_the_cutoff_are_not_pruned,
            every_data_path_write_is_recorded,
            writes_that_change_nothing_record_nothing,
            a_transaction_records_its_members_at_one_instant,
            a_cancelled_transaction_records_nothing,
            writes_to_a_table_without_recovery_record_nothing,
            the_state_now_is_the_live_table,
            the_enable_takes_a_baseline_the_hook_supersedes,
            an_enable_on_an_enabled_table_changes_only_a_given_period,
            a_disable_drops_the_history_and_a_re_enable_starts_again,
            the_sweep_prunes_to_the_retention_floor,
            a_sweep_stops_at_its_deadline,
            the_floor_is_the_enable_until_the_retention_passes,
            a_raised_period_keeps_the_floor_at_the_pruned_cutoff,
            a_prune_between_pages_keeps_the_state_of_the_reconstruct,
            a_page_after_a_prune_passed_its_time_refuses,
            a_page_after_a_disable_refuses,
            two_enables_at_once_both_succeed,
            a_stored_period_outside_1_to_35_is_an_error
        );
    };
    (@each $open:path, $close:path, $($check:ident),+) => {
        $(
            #[tokio::test]
            async fn $check() {
                let Some(fixture) = $open().await else { return };
                $crate::pitr::conformance::$check(&fixture).await;
                $close(fixture).await;
            }
        )+
    };
}
