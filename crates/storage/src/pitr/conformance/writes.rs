// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checks that drive a backend's data paths and read what the write hook
//! recorded.

use std::time::SystemTime;

use extenddb_core::expression::{Expr, ExpressionMaps, PathElement, UpdateAction};
use extenddb_core::types::{
    AttributeValue, Item, ReturnValuesOnConditionCheckFailure, ScalarAttributeType, TableKeyInfo,
};

use super::{HistoryFixture, s, sorted, state};
use crate::error::StorageError;
use crate::transact::TransactWriteOp;

/// A key-only item.
fn k(pk: &str) -> Item {
    Item::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))])
}

fn path(name: &str) -> Vec<PathElement> {
    vec![PathElement::Attribute(name.to_owned())]
}

fn function(name: &str, attr: &str) -> Expr {
    Expr::Function {
        name: name.to_owned(),
        args: vec![Expr::Path(path(attr))],
    }
}

fn set_v(value: &str) -> (Vec<UpdateAction>, ExpressionMaps) {
    let mut maps = ExpressionMaps::default();
    maps.values
        .insert(":v".to_owned(), AttributeValue::S(value.to_owned()));
    (
        vec![UpdateAction::Set {
            path: path("v"),
            value: Expr::Placeholder(":v".to_owned()),
        }],
        maps,
    )
}

async fn put<F: HistoryFixture>(
    f: &F,
    info: &TableKeyInfo,
    item: &Item,
    condition: Option<&Expr>,
) -> Result<(), StorageError> {
    let maps = ExpressionMaps::default();
    f.data()
        .put_item(info, item.clone(), false, condition, &maps, None)
        .await
        .map(|_| ())
}

async fn delete<F: HistoryFixture>(f: &F, info: &TableKeyInfo, key: &Item) {
    let maps = ExpressionMaps::default();
    f.data()
        .delete_item(info, key, false, None, &maps, None)
        .await
        .expect("delete");
}

async fn update<F: HistoryFixture>(
    f: &F,
    info: &TableKeyInfo,
    key: &Item,
    actions: &[UpdateAction],
    maps: &ExpressionMaps,
) {
    f.data()
        .update_item(info, key, actions, false, false, None, maps, None)
        .await
        .expect("update");
}

fn now() -> SystemTime {
    SystemTime::now()
}

/// PutItem, UpdateItem and DeleteItem each add one row, which reconstructs
/// the item as the write left it.
pub async fn every_data_path_write_is_recorded<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, true).await;
    let key = k("k");
    put(f, &info, &s("k", "a"), None).await.expect("put");
    assert_eq!(state(f, &info, now(), 10).await, sorted(&[s("k", "a")]));
    let (actions, maps) = set_v("b");
    update(f, &info, &key, &actions, &maps).await;
    assert_eq!(state(f, &info, now(), 10).await, sorted(&[s("k", "b")]));
    delete(f, &info, &key).await;
    assert!(state(f, &info, now(), 10).await.is_empty());
    assert_eq!(f.history_rows(&info.table_id).await, 3);
}

/// A failed condition, a delete of a missing item and a REMOVE-only update
/// of a missing item change nothing and record nothing.
pub async fn writes_that_change_nothing_record_nothing<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, true).await;
    let exists = function("attribute_exists", "pk");
    let err = put(f, &info, &s("k", "a"), Some(&exists)).await;
    assert!(
        matches!(err, Err(StorageError::ConditionFailed(_))),
        "{err:?}"
    );
    delete(f, &info, &k("missing")).await;
    let remove = [UpdateAction::Remove { path: path("v") }];
    update(f, &info, &k("missing"), &remove, &ExpressionMaps::default()).await;
    assert_eq!(f.history_rows(&info.table_id).await, 0);
}

fn ops<'a>(
    a: &'a TableKeyInfo,
    b: &'a TableKeyInfo,
    items: &'a [Item; 4],
    actions: &'a [UpdateAction],
    maps: &'a ExpressionMaps,
    check: &'a Expr,
) -> Vec<TransactWriteOp<'a>> {
    let none = ReturnValuesOnConditionCheckFailure::None;
    vec![
        TransactWriteOp::Put {
            key_info: a,
            item: &items[0],
            condition: None,
            maps,
            return_values_on_ccf: none,
            stream: None,
        },
        TransactWriteOp::Delete {
            key_info: a,
            key: &items[1],
            condition: None,
            maps,
            return_values_on_ccf: none,
            stream: None,
        },
        TransactWriteOp::Update {
            key_info: b,
            key: &items[2],
            actions,
            condition: None,
            maps,
            return_values_on_ccf: none,
            stream: None,
        },
        TransactWriteOp::ConditionCheck {
            key_info: b,
            key: &items[3],
            condition: check,
            maps,
            return_values_on_ccf: none,
        },
    ]
}

/// TransactWriteItems over two tables records each changed member with one
/// commit time, and the condition check records nothing.
pub async fn a_transaction_records_its_members_at_one_instant<F: HistoryFixture>(f: &F) {
    let a = f.table("a", None, true).await;
    let b = f.table("b", None, true).await;
    put(f, &a, &s("gone", "x"), None).await.expect("put");
    put(f, &b, &s("checked", "x"), None).await.expect("put");
    let (actions, maps) = set_v("u");
    let exists = function("attribute_exists", "pk");
    let items = [s("new", "n"), k("gone"), k("upd"), k("checked")];
    f.data()
        .transact_write_items(&ops(&a, &b, &items, &actions, &maps, &exists), None)
        .await
        .expect("transact");
    let times_a = f.commit_times(&a.table_id).await;
    let times_b = f.commit_times(&b.table_id).await;
    assert_eq!(
        times_a.len(),
        3,
        "a: the earlier put, then the put and the delete"
    );
    assert_eq!(times_b.len(), 2, "b: the earlier put, then the update");
    assert_eq!(times_a[1], times_a[2]);
    assert_eq!(times_a[1], times_b[1]);
    assert_eq!(state(f, &a, now(), 10).await, sorted(&[s("new", "n")]));
    assert_eq!(
        state(f, &b, now(), 10).await,
        sorted(&[s("checked", "x"), s("upd", "u")])
    );
}

/// A transaction cancelled by a failed condition check records nothing.
pub async fn a_cancelled_transaction_records_nothing<F: HistoryFixture>(f: &F) {
    let a = f.table("a", None, true).await;
    let b = f.table("b", None, true).await;
    let (actions, maps) = set_v("u");
    let exists = function("attribute_exists", "pk");
    let items = [s("new", "n"), k("gone"), k("upd"), k("checked")];
    let err = f
        .data()
        .transact_write_items(&ops(&a, &b, &items, &actions, &maps, &exists), None)
        .await;
    assert!(
        matches!(err, Err(StorageError::TransactionCanceled(_))),
        "{err:?}"
    );
    assert_eq!(f.history_rows(&a.table_id).await, 0);
    assert_eq!(f.history_rows(&b.table_id).await, 0);
}

/// Writes to a table whose recovery is not enabled record nothing.
pub async fn writes_to_a_table_without_recovery_record_nothing<F: HistoryFixture>(f: &F) {
    let info = f.table("t", None, false).await;
    put(f, &info, &s("k", "a"), None).await.expect("put");
    delete(f, &info, &k("k")).await;
    assert_eq!(f.history_rows(&info.table_id).await, 0);
}

/// After a mix of writes over every key shape, the state now is exactly what
/// the data table holds.
pub async fn the_state_now_is_the_live_table<F: HistoryFixture>(f: &F) {
    type SortKey = fn(u32) -> AttributeValue;
    let shapes: [(Option<ScalarAttributeType>, Option<SortKey>); 4] = [
        (None, None),
        (
            Some(ScalarAttributeType::S),
            Some(|i| AttributeValue::S(format!("s{}", i % 3))),
        ),
        (
            Some(ScalarAttributeType::N),
            Some(|i| AttributeValue::N(format!("{}.0", i % 3))),
        ),
        (
            Some(ScalarAttributeType::B),
            Some(|i| AttributeValue::B(vec![0, (i % 3) as u8])),
        ),
    ];
    for (n, (ty, sk)) in shapes.into_iter().enumerate() {
        let info = f.table(&format!("t{n}"), ty, true).await;
        for i in 0..40_u32 {
            let mut item = s(&format!("p{}", i % 4), &format!("v{i}"));
            if let Some(sk) = sk {
                item.insert("sk".to_owned(), sk(i / 4));
            }
            let mut key = item.clone();
            key.remove("v");
            match i % 5 {
                0..=2 => put(f, &info, &item, None).await.expect("put"),
                3 => {
                    let (actions, maps) = set_v(&format!("u{i}"));
                    update(f, &info, &key, &actions, &maps).await;
                }
                _ => delete(f, &info, &key).await,
            }
        }
        assert_eq!(
            state(f, &info, now(), 7).await,
            sorted(&f.live_items(&info).await),
            "{ty:?}"
        );
    }
}
