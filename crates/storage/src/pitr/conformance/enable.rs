// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checks of enabling and disabling recovery. Measured on the live
//! service: items that exist at the enable are in every restore after it, at
//! their image then; a disable drops the history, and a re-enable takes a new
//! baseline; an enable on an enabled table changes only a given period.

use std::time::{Duration, SystemTime};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item, ScalarAttributeType, TableKeyInfo};

use super::super::{DEFAULT_RECOVERY_PERIOD_DAYS, PitrState};
use super::{HistoryFixture, at, sorted, state};

async fn put<F: HistoryFixture>(f: &F, info: &TableKeyInfo, item: &Item) {
    f.data()
        .put_item(
            info,
            item.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("put");
}

async fn enable<F: HistoryFixture>(
    f: &F,
    info: &TableKeyInfo,
    period: Option<u8>,
    t: SystemTime,
) -> PitrState {
    f.store()
        .enable_pitr(info, period, t)
        .await
        .expect("enable")
}

fn keyed(pk: &str, sk: Option<AttributeValue>, v: &str) -> Item {
    let mut item = Item::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("v".to_owned(), AttributeValue::S(v.to_owned())),
    ]);
    if let Some(sk) = sk {
        item.insert("sk".to_owned(), sk);
    }
    item
}

/// Sort key values that stress the key text: numbers that differ in
/// spelling, scale (a trailing zero) and magnitude, and binary values longer
/// than a line of base64.
fn sort_keys(ty: ScalarAttributeType) -> Vec<AttributeValue> {
    match ty {
        ScalarAttributeType::S => ["a", "a b", "\u{e9}", "z"]
            .iter()
            .map(|s| AttributeValue::S((*s).to_owned()))
            .collect(),
        ScalarAttributeType::N => [
            "0",
            "100",
            "1.5",
            "2.50",
            "-0.25",
            "1E-130",
            "9.9999E+125",
            "12345678901234567890.123",
        ]
        .iter()
        .map(|s| AttributeValue::N((*s).to_owned()))
        .collect(),
        ScalarAttributeType::B => vec![
            AttributeValue::B(vec![0]),
            AttributeValue::B(vec![0xff; 3]),
            AttributeValue::B((0..=255).collect()),
        ],
    }
}

/// Items that exist at the enable are the state at the enable, and a later
/// write of each one supersedes its baseline row: the baseline and the write
/// hook key an item the same way, for every key shape.
pub async fn the_enable_takes_a_baseline_the_hook_supersedes<F: HistoryFixture>(f: &F) {
    let shapes = [
        None,
        Some(ScalarAttributeType::S),
        Some(ScalarAttributeType::N),
        Some(ScalarAttributeType::B),
    ];
    for (n, ty) in shapes.into_iter().enumerate() {
        let info = f.table(&format!("b{n}"), ty, false).await;
        let sks: Vec<Option<AttributeValue>> =
            ty.map_or(vec![None], |t| sort_keys(t).into_iter().map(Some).collect());
        let mut before = Vec::new();
        for pk in ["p", "q"] {
            for sk in &sks {
                let item = keyed(pk, sk.clone(), "before");
                put(f, &info, &item).await;
                before.push(item);
            }
        }
        let enabled = SystemTime::now();
        enable(f, &info, None, enabled).await;
        assert_eq!(
            f.history_rows(&info.table_id).await,
            i64::try_from(before.len()).expect("count"),
            "{ty:?}: one baseline row per item"
        );
        assert_eq!(state(f, &info, enabled, 5).await, sorted(&before), "{ty:?}");
        let mut after = Vec::new();
        for item in &before {
            let mut changed = item.clone();
            changed.insert("v".to_owned(), AttributeValue::S("after".to_owned()));
            put(f, &info, &changed).await;
            after.push(changed);
        }
        let now = SystemTime::now();
        assert_eq!(state(f, &info, now, 5).await, sorted(&after), "{ty:?}");
        assert_eq!(
            state(f, &info, now, 5).await,
            sorted(&f.live_items(&info).await),
            "{ty:?}"
        );
        assert_eq!(state(f, &info, enabled, 5).await, sorted(&before), "{ty:?}");
    }
}

/// A fresh enable takes the default period, or the given one; an enable on an
/// enabled table changes only a given period and takes no second baseline.
pub async fn an_enable_on_an_enabled_table_changes_only_a_given_period<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, false).await;
    put(f, &info, &keyed("k", None, "a")).await;
    let first = enable(f, &info, None, at(10)).await;
    assert_eq!(first.recovery_period_days, DEFAULT_RECOVERY_PERIOD_DAYS);
    assert_eq!(first.enabled_at, at(10));
    let changed = enable(f, &info, Some(20), at(20)).await;
    assert_eq!(
        (changed.recovery_period_days, changed.enabled_at),
        (20, at(10))
    );
    let kept = enable(f, &info, None, at(30)).await;
    assert_eq!((kept.recovery_period_days, kept.enabled_at), (20, at(10)));
    assert_eq!(
        f.store().pitr_state(&info.table_id).await.expect("state"),
        Some(kept)
    );
    assert_eq!(f.history_rows(&info.table_id).await, 1);

    let other = f.table("u", None, false).await;
    let given = enable(f, &other, Some(7), at(10)).await;
    assert_eq!(given.recovery_period_days, 7);
}

/// A disable removes the state and the history; writes while disabled record
/// nothing; a re-enable is a fresh enable whose baseline holds them.
pub async fn a_disable_drops_the_history_and_a_re_enable_starts_again<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, false).await;
    put(f, &info, &keyed("b1", None, "first")).await;
    enable(f, &info, Some(7), SystemTime::now()).await;
    put(f, &info, &keyed("b1", None, "first-window")).await;
    f.store().disable_pitr(&info).await.expect("disable");
    assert_eq!(
        f.store().pitr_state(&info.table_id).await.expect("state"),
        None
    );
    assert_eq!(f.history_rows(&info.table_id).await, 0);
    put(f, &info, &keyed("b2", None, "while-disabled")).await;
    assert_eq!(f.history_rows(&info.table_id).await, 0);
    f.store()
        .disable_pitr(&info)
        .await
        .expect("a second disable");
    let again = SystemTime::now() + Duration::from_millis(1);
    let state_now = enable(f, &info, None, again).await;
    assert_eq!(state_now.recovery_period_days, DEFAULT_RECOVERY_PERIOD_DAYS);
    assert_eq!(state_now.enabled_at, again);
    assert_eq!(
        state(f, &info, again, 10).await,
        sorted(&[
            keyed("b1", None, "first-window"),
            keyed("b2", None, "while-disabled")
        ])
    );
}

/// Two enables of one table at once both succeed, and the table holds one
/// baseline: measured on the live service, both concurrent requests answer
/// 200 and the table then reports the period of the later one.
pub async fn two_enables_at_once_both_succeed<F: HistoryFixture>(f: &F) {
    for round in 0..10 {
        let info = f.table(&format!("t{round}"), None, false).await;
        for n in 0..300 {
            put(f, &info, &keyed(&format!("k{n}"), None, "v")).await;
        }
        let now = SystemTime::now();
        let (a, b) = futures::join!(
            f.store().enable_pitr(&info, None, now),
            f.store().enable_pitr(&info, Some(7), now)
        );
        let (a, b) = (a.expect("first enable"), b.expect("second enable"));
        assert!(
            [a.recovery_period_days, b.recovery_period_days].contains(&7),
            "round {round}: {a:?} {b:?}"
        );
        assert_eq!(f.history_rows(&info.table_id).await, 300, "round {round}");
        let stored = f.store().pitr_state(&info.table_id).await.expect("state");
        assert_eq!(
            stored.map(|s| s.recovery_period_days),
            Some(7),
            "round {round}"
        );
    }
}

/// A stored period outside 1 to 35 is a corrupt row: reading it is an
/// error, not some other period.
pub async fn a_stored_period_outside_1_to_35_is_an_error<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, true).await;
    for days in [0, 36, 300] {
        f.execute(&format!(
            "UPDATE pitr_state SET recovery_period_days = {days} WHERE table_id = '{}'",
            info.table_id
        ))
        .await;
        let read = f.store().pitr_state(&info.table_id).await;
        assert!(
            matches!(read, Err(crate::error::StorageError::Internal(ref m)) if m.contains(&days.to_string())),
            "{days}: {read:?}"
        );
        assert!(f.store().pitr_tables().await.is_err(), "{days}");
    }
}
