// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checks of a reconstruct read in pages while a sweep runs: the pages
//! of one reconstruct return one state, and a page whose state the history
//! no longer holds refuses instead of returning part of it.

use std::time::SystemTime;

use extenddb_core::types::{Item, TableKeyInfo};

use super::super::ReconstructPage;
use super::{HistoryFixture, at, s, sorted, state};
use crate::error::StorageError;

async fn page<F: HistoryFixture>(
    f: &F,
    info: &TableKeyInfo,
    t: SystemTime,
    after: Option<i64>,
) -> Result<ReconstructPage, StorageError> {
    f.store()
        .reconstruct_page(&info.table_id, t, after, 1)
        .await
}

/// `a` at +1 s, `b`=1 at +2 s, `b`=2 at +4 s: the state at +3 s is `a` and
/// `b`=1, and a prune to +4 s alone would remove `b`=1.
async fn history<F: HistoryFixture>(f: &F) -> TableKeyInfo {
    let info = f.table("t", None, true).await;
    for (item, t) in [(s("a", "1"), 1), (s("b", "1"), 2), (s("b", "2"), 4)] {
        assert!(f.record(&info, &item, Some(&item), at(t), true).await);
    }
    info
}

fn items(pages: &[ReconstructPage]) -> Vec<Item> {
    pages.iter().flat_map(|p| p.items.clone()).collect()
}

/// A sweep between two pages of a reconstruct at T, to a cutoff past T,
/// leaves the rest of the state at T for the next page.
pub async fn a_prune_between_pages_keeps_the_state_of_the_reconstruct<F: HistoryFixture>(f: &F) {
    let info = history(f).await;
    let first = page(f, &info, at(3), None).await.expect("page 1");
    assert_eq!(first.items.len(), 1);
    f.store()
        .prune_before(&info.table_id, at(4), 100)
        .await
        .expect("prune");
    let second = page(f, &info, at(3), first.next).await.expect("page 2");
    let mut pages = vec![first, second];
    while let Some(next) = pages.last().and_then(|p| p.next) {
        pages.push(page(f, &info, at(3), Some(next)).await.expect("page"));
    }
    assert_eq!(sorted(&items(&pages)), sorted(&[s("a", "1"), s("b", "1")]));
    let state_now = f
        .store()
        .pitr_state(&info.table_id)
        .await
        .expect("state")
        .expect("enabled");
    assert!(state_now.pruned_to <= at(3), "{state_now:?}");
}

/// Once the pin has lapsed, a prune past T goes through, and the next page
/// of the reconstruct at T refuses: the history no longer holds its state.
pub async fn a_page_after_a_prune_passed_its_time_refuses<F: HistoryFixture>(f: &F) {
    let info = history(f).await;
    let first = page(f, &info, at(3), None).await.expect("page 1");
    f.execute(&format!(
        "UPDATE pitr_state SET pin_expires_us = 0 WHERE table_id = '{}'",
        info.table_id
    ))
    .await;
    assert_eq!(
        f.store()
            .prune_before(&info.table_id, at(4), 100)
            .await
            .expect("prune"),
        1
    );
    let state_now = f
        .store()
        .pitr_state(&info.table_id)
        .await
        .expect("state")
        .expect("enabled");
    assert_eq!(state_now.pruned_to, at(4));
    match page(f, &info, at(3), first.next).await {
        Err(StorageError::SnapshotExpired(_)) => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(
        state(f, &info, at(4), 10).await,
        sorted(&[s("a", "1"), s("b", "2")])
    );
}

/// A disable between two pages: the next page refuses.
pub async fn a_page_after_a_disable_refuses<F: HistoryFixture>(f: &F) {
    let info = history(f).await;
    let first = page(f, &info, at(3), None).await.expect("page 1");
    f.store().disable_pitr(&info).await.expect("disable");
    match page(f, &info, at(3), first.next).await {
        Err(StorageError::SnapshotExpired(_)) => {}
        other => panic!("{other:?}"),
    }
}
