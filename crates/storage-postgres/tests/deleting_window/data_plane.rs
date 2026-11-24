// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The data-plane rows of the DELETING window: the requests a `DELETING`
//! table serves, and the not-found answer once its data table is dropped.
//! The scratch database and the window come from `deleting_window.rs`.

use std::collections::{BTreeMap, HashMap};

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{
    AttributeValue, Item, ReturnValuesOnConditionCheckFailure, TableKeyInfo,
};
use extenddb_storage::{DataEngine, TransactGetOp, TransactWriteOp};

use super::*;

fn item(pk: &str) -> Item {
    BTreeMap::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))])
}

fn query_pk(pk: &str) -> (KeyCondition, ExpressionMaps) {
    let condition = KeyCondition {
        pk_path: vec![PathElement::Attribute("pk".to_owned())],
        pk_value: Expr::Placeholder(":pk".to_owned()),
        extra_pk_conditions: Vec::new(),
        sk_condition: None,
        extra_sk_conditions: Vec::new(),
    };
    let maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([(":pk".to_owned(), AttributeValue::S(pk.to_owned()))]),
    );
    (condition, maps)
}

/// Create `TABLE`, write the item `pre`, and open the window. Returns the key
/// info read while the row was still ACTIVE, the snapshot a cache would hold.
async fn open_window_with_item(s: &Scratch) -> TableKeyInfo {
    create_table_named(s, TABLE).await;
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("key info of the ACTIVE table");
    s.engine
        .put_item(
            &key_info,
            item("pre"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("seed the pre-existing item");
    start_deleting(s).await;
    key_info
}

/// During the window the key-info read finds the table, and PutItem, GetItem
/// (the pre-existing item and the one just written), Query and Scan are
/// served, as the service serves them.
#[tokio::test]
async fn data_plane_is_served_during_the_window() {
    if base_conn().is_none() {
        return skip("data_plane_is_served_during_the_window");
    }
    let s = scratch().await;
    open_window_with_item(&s).await;
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("a DELETING table is found by the data plane");

    s.engine
        .put_item(
            &key_info,
            item("w1"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("PutItem during the window");
    let got = s
        .engine
        .get_item(&key_info, &item("pre"))
        .await
        .expect("GetItem of the pre-existing item");
    assert_eq!(got, Some(item("pre")));
    let got = s
        .engine
        .get_item(&key_info, &item("w1"))
        .await
        .expect("GetItem of the window write");
    assert_eq!(got, Some(item("w1")));
    let (condition, maps) = query_pk("pre");
    let (rows, _) = s
        .engine
        .query(&key_info, &condition, &maps, true, None, None, None)
        .await
        .expect("Query during the window");
    assert_eq!(rows, vec![item("pre")]);
    let (rows, _) = s
        .engine
        .scan(&key_info, Some(1), None, None, None, None)
        .await
        .expect("Scan during the window");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        status_of(&s.catalog, TABLE).await.as_deref(),
        Some("DELETING")
    );

    s.cleanup().await;
}

/// Once the data table is dropped while the row still reads DELETING, a
/// request that reaches storage, here with the key info read before the
/// delete (the snapshot a cache would hold), answers the table as not found
/// rather than as an internal error.
#[tokio::test]
async fn a_request_after_the_data_drop_is_table_not_found() {
    if base_conn().is_none() {
        return skip("a_request_after_the_data_drop_is_table_not_found");
    }
    let s = scratch().await;
    let key_info = open_window_with_item(&s).await;
    sqlx::query(&format!("DROP TABLE \"_ddb_{}\"", key_info.table_id))
        .execute(&s.catalog)
        .await
        .expect("drop the data table under the DELETING row");

    let err = s
        .engine
        .put_item(
            &key_info,
            item("w1"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect_err("PutItem after the data drop");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == TABLE),
        "expected TableNotFound({TABLE:?}), got {err:?}"
    );
    let err = s
        .engine
        .get_item(&key_info, &item("pre"))
        .await
        .expect_err("GetItem after the data drop");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == TABLE),
        "expected TableNotFound({TABLE:?}), got {err:?}"
    );
    // The transaction paths run outside the single-request statements, so
    // they are funnelled at their own entry points.
    let pre = item("pre");
    let err = s
        .engine
        .transact_get_items(&[TransactGetOp {
            key_info: &key_info,
            key: &pre,
        }])
        .await
        .expect_err("TransactGetItems after the data drop");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == TABLE),
        "expected TableNotFound({TABLE:?}), got {err:?}"
    );
    let maps = ExpressionMaps::default();
    let w2 = item("w2");
    let err = s
        .engine
        .transact_write_items(
            &[TransactWriteOp::Put {
                key_info: &key_info,
                item: &w2,
                condition: None,
                maps: &maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: None,
            }],
            None,
        )
        .await
        .expect_err("TransactWriteItems after the data drop");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == TABLE),
        "expected TableNotFound({TABLE:?}), got {err:?}"
    );

    s.cleanup().await;
}
