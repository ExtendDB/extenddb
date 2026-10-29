// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage-level tests for the data plane while a table is `DELETING`, and
//! for two `DeleteTable` calls racing on one table.
//!
//! Amazon DynamoDB serves `PutItem`, `GetItem`, `Query` and `Scan` on a
//! `DELETING` table until the table is gone, and answers them
//! `ResourceNotFoundException` once it is (measured 2026-10-25). Two
//! `DeleteTable` calls on one `ACTIVE` table get one `200` and one
//! `ResourceInUseException` with the `Table is being deleted` sentence.
//!
//! The window is produced by setting the catalog row to `DELETING` directly,
//! as the control-plane tests in `delete_table.rs` do; the pair test lets
//! `delete_table` itself mark the row, with a delay long enough that no worker
//! finishes the deletion during the test.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{
    AttributeValue, DeleteTableInput, Item, ReturnValuesOnConditionCheckFailure, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine, TransactGetOp, TransactWriteOp};
use serde_json::json;

use crate::SqliteEngine;

const ACCOUNT: &str = "000000000000";

fn item(pk: &str) -> Item {
    BTreeMap::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))])
}

fn create_input(table: &str) -> extenddb_core::types::CreateTableInput {
    serde_json::from_value(json!({
        "TableName": table,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input")
}

async fn seed(engine: &SqliteEngine) {
    crate::schema::apply(&engine.pool).await.expect("schema");
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
        .execute(&engine.pool)
        .await
        .expect("delay");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
}

/// An in-memory engine with one table `t` holding the item `pre`, whose
/// catalog row is then set to `DELETING`: the window a slow data drop holds
/// open. Returns the engine and the key info read while the row was ACTIVE.
async fn engine_with_deleting_table() -> (SqliteEngine, TableKeyInfo) {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
    seed(&engine).await;
    engine
        .create_table_impl(ACCOUNT, create_input("t"), false)
        .await
        .expect("create table");
    let key_info = engine.table_key_info(ACCOUNT, "t").await.expect("key info");
    engine
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
    sqlx::query("UPDATE tables SET table_status = 'DELETING' WHERE table_name = 't'")
        .execute(&engine.pool)
        .await
        .expect("mark DELETING");
    (engine, key_info)
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

/// During the window the key-info read finds the table, and PutItem, GetItem
/// (the pre-existing item and the one just written), Query and Scan are
/// served, as the service serves them.
#[tokio::test]
async fn data_plane_is_served_during_the_window() {
    let (engine, _) = engine_with_deleting_table().await;
    let key_info = engine
        .table_key_info(ACCOUNT, "t")
        .await
        .expect("a DELETING table is found by the data plane");

    engine
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
    let got = engine
        .get_item(&key_info, &item("pre"))
        .await
        .expect("GetItem of the pre-existing item");
    assert_eq!(got, Some(item("pre")));
    let got = engine
        .get_item(&key_info, &item("w1"))
        .await
        .expect("GetItem of the window write");
    assert_eq!(got, Some(item("w1")));
    let (condition, maps) = query_pk("pre");
    let (rows, _) = engine
        .query(&key_info, &condition, &maps, true, None, None, None)
        .await
        .expect("Query during the window");
    assert_eq!(rows, vec![item("pre")]);
    let (rows, _) = engine
        .scan(&key_info, Some(1), None, None, None, None)
        .await
        .expect("Scan during the window");
    assert_eq!(rows.len(), 1);
}

/// Once the data table is dropped while the row still reads DELETING, a
/// request that reaches storage answers the table as not found, not as an
/// internal error.
#[tokio::test]
async fn a_request_after_the_data_drop_is_table_not_found() {
    let (engine, key_info) = engine_with_deleting_table().await;
    sqlx::query(&format!(
        "DROP TABLE {}",
        crate::data::data_table_name(&key_info.table_id)
    ))
    .execute(&engine.pool)
    .await
    .expect("drop the data table under the DELETING row");

    let err = engine
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
        matches!(&err, StorageError::TableNotFound(name) if name == "t"),
        "expected TableNotFound(\"t\"), got {err:?}"
    );
    let err = engine
        .get_item(&key_info, &item("pre"))
        .await
        .expect_err("GetItem after the data drop");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == "t"),
        "expected TableNotFound(\"t\"), got {err:?}"
    );
    // The transaction paths run outside the single-request statements, so
    // they are funnelled at their own entry points.
    let pre = item("pre");
    let err = engine
        .transact_get_items(&[TransactGetOp {
            key_info: &key_info,
            key: &pre,
        }])
        .await
        .expect_err("TransactGetItems after the data drop");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == "t"),
        "expected TableNotFound(\"t\"), got {err:?}"
    );
    let maps = ExpressionMaps::default();
    let w2 = item("w2");
    let err = engine
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
        matches!(&err, StorageError::TableNotFound(name) if name == "t"),
        "expected TableNotFound(\"t\"), got {err:?}"
    );
}

/// Two concurrent DeleteTable calls on one ACTIVE table: one marks the row
/// DELETING and answers the description, the other reads the marked row and
/// answers TableBeingDeleted. Ten rounds, a fresh table each; the row is left
/// DELETING exactly once and its data table is still present (no worker runs
/// here to finish the deletion).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_delete_tables_have_one_winner() {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target")
        });
    assert!(target.is_dir(), "cargo target directory must exist");
    let path = target.join(format!("deleting-pair-{}.sqlite", uuid::Uuid::new_v4()));
    let engine = Arc::new(
        SqliteEngine::new(path.to_str().expect("UTF-8 path"), 4, "us-east-1", 409_600)
            .await
            .expect("file engine"),
    );
    seed(&engine).await;

    for round in 0..10 {
        let table = format!("t_pair_{round}");
        sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
            .execute(&engine.pool)
            .await
            .expect("delay 0 so the create completes inline");
        engine
            .create_table_impl(ACCOUNT, create_input(&table), false)
            .await
            .expect("create table");
        sqlx::query("UPDATE settings SET value = '600' WHERE key = 'control_plane_delay_seconds'")
            .execute(&engine.pool)
            .await
            .expect("delay 600 so the delete marks the row");

        let tasks: Vec<_> = (0..2)
            .map(|_| {
                let engine = Arc::clone(&engine);
                let table = table.clone();
                tokio::spawn(async move {
                    engine
                        .delete_table(ACCOUNT, DeleteTableInput { table_name: table })
                        .await
                })
            })
            .collect();
        let mut ok = 0;
        let mut being_deleted = 0;
        for task in tasks {
            match task.await.expect("task") {
                Ok(_) => ok += 1,
                Err(StorageError::TableBeingDeleted(name)) if name == table => being_deleted += 1,
                Err(other) => panic!("round {round}: unexpected {other:?}"),
            }
        }
        assert_eq!((ok, being_deleted), (1, 1), "round {round}");

        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT table_status, table_id FROM tables WHERE table_name = ?")
                .bind(&table)
                .fetch_all(&engine.pool)
                .await
                .expect("rows");
        assert_eq!(rows.len(), 1, "round {round}: one catalog row");
        assert_eq!(rows[0].0, "DELETING", "round {round}");
        let data_table = format!("_ddb_{}", rows[0].1);
        let present: Option<String> =
            sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
                .bind(&data_table)
                .fetch_optional(&engine.pool)
                .await
                .expect("sqlite_master");
        assert_eq!(
            present.as_deref(),
            Some(data_table.as_str()),
            "round {round}"
        );
    }

    engine.pool.close().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}
