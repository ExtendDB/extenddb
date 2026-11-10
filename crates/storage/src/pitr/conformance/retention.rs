// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checks of the retention sweep: every enabled table is pruned to the
//! floor of its window, `now` less the retention once that is past the
//! enable, and every state a restore inside the window reads stays.

use std::time::{Duration, Instant};

use extenddb_core::types::{AttributeValue, Item};

use super::super::{retention_floor, sweep};
use super::{HistoryFixture, at, sorted, state};

fn s(pk: &str, v: &str) -> Item {
    Item::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("v".to_owned(), AttributeValue::S(v.to_owned())),
    ])
}

const DAY: u64 = 86_400;

/// A sweep two days after an enable with a one-day retention prunes what no
/// restore after the floor reads, and keeps every state from the floor on.
pub async fn the_sweep_prunes_to_the_retention_floor<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, false).await;
    let other = f.table("u", None, false).await;
    f.store()
        .enable_pitr(&info, Some(1), at(0))
        .await
        .expect("enable");
    let k = s("k", "");
    for (v, t) in [("a", 10), ("b", 20)] {
        f.record(&info, &k, Some(&s("k", v)), at(t), true).await;
    }
    f.record(&info, &k, None, at(30), true).await;
    f.record(&info, &s("j", ""), Some(&s("j", "x")), at(40), true)
        .await;
    f.record(&info, &k, Some(&s("k", "c")), at(DAY + 100), true)
        .await;
    let tables = f.store().pitr_tables().await.expect("tables");
    assert_eq!(
        tables.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
        vec![info.table_id.as_str()]
    );
    let floor = at(DAY);
    let before = (
        state(f, &info, floor, 10).await,
        state(f, &info, at(DAY + 100), 10).await,
    );
    let report = sweep(
        f.store(),
        at(2 * DAY),
        2,
        Instant::now() + Duration::from_secs(60),
    )
    .await
    .expect("sweep");
    assert_eq!((report.pruned, report.stopped_at_deadline), (3, false));
    assert_eq!(state(f, &info, floor, 10).await, before.0);
    assert_eq!(state(f, &info, at(DAY + 100), 10).await, before.1);
    assert_eq!(before.0, sorted(&[s("j", "x")]));
    assert_eq!(f.history_rows(&info.table_id).await, 2);
    assert_eq!(f.history_rows(&other.table_id).await, 0);
}

/// A sweep past its deadline stops before it removes anything and says so.
pub async fn a_sweep_stops_at_its_deadline<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, false).await;
    f.store()
        .enable_pitr(&info, Some(1), at(0))
        .await
        .expect("enable");
    let k = s("k", "");
    f.record(&info, &k, Some(&s("k", "a")), at(10), true).await;
    f.record(&info, &k, Some(&s("k", "b")), at(20), true).await;
    let report = sweep(f.store(), at(2 * DAY), 10, Instant::now())
        .await
        .expect("sweep");
    assert!(report.stopped_at_deadline);
    assert_eq!(report.pruned, 0);
    assert_eq!(f.history_rows(&info.table_id).await, 2);
}

/// The floor is the enable instant until the retention has passed.
pub async fn the_floor_is_the_enable_until_the_retention_passes<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, false).await;
    f.store()
        .enable_pitr(&info, Some(1), at(DAY))
        .await
        .expect("enable");
    let k = s("k", "");
    f.record(&info, &k, Some(&s("k", "a")), at(DAY + 10), true)
        .await;
    f.record(&info, &k, Some(&s("k", "b")), at(DAY + 20), true)
        .await;
    let state_now = f
        .store()
        .pitr_state(&info.table_id)
        .await
        .expect("state")
        .expect("enabled");
    assert_eq!(retention_floor(&state_now, at(DAY + 3_600)), at(DAY));
    assert_eq!(retention_floor(&state_now, at(3 * DAY)), at(2 * DAY));
    let report = sweep(
        f.store(),
        at(DAY + 3_600),
        10,
        Instant::now() + Duration::from_secs(60),
    )
    .await
    .expect("sweep");
    assert_eq!(report.pruned, 0);
    assert_eq!(f.history_rows(&info.table_id).await, 2);
}

/// A period raised after a sweep keeps the floor at the sweep's cutoff: the
/// history no longer holds the states before it, so the window never
/// reaches them.
pub async fn a_raised_period_keeps_the_floor_at_the_pruned_cutoff<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, false).await;
    f.store()
        .enable_pitr(&info, Some(1), at(0))
        .await
        .expect("enable");
    let k = s("k", "");
    for (v, t) in [("v1", DAY), ("v2", 5 * DAY), ("v3", 9 * DAY + DAY / 2)] {
        f.record(&info, &k, Some(&s("k", v)), at(t), true).await;
    }
    let before = state(f, &info, at(9 * DAY), 10).await;
    let report = sweep(
        f.store(),
        at(10 * DAY),
        10,
        Instant::now() + Duration::from_secs(60),
    )
    .await
    .expect("sweep");
    assert_eq!(report.pruned, 1);
    let raised = f
        .store()
        .enable_pitr(&info, Some(35), at(10 * DAY))
        .await
        .expect("raise the period");
    assert_eq!(raised.recovery_period_days, 35);
    let state_now = f
        .store()
        .pitr_state(&info.table_id)
        .await
        .expect("state")
        .expect("enabled");
    assert_eq!(state_now.pruned_to, at(9 * DAY));
    let floor = retention_floor(&state_now, at(10 * DAY));
    assert_eq!(floor, at(9 * DAY));
    assert_eq!(state(f, &info, floor, 10).await, before);
    assert_eq!(before, sorted(&[s("k", "v2")]));
}
