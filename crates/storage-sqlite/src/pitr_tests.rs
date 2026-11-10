// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The SQLite item-history store: the shared checks of
//! `extenddb_storage::pitr::conformance`, then what is SQLite's own (the
//! catalog cascade and the query plan).

use std::time::SystemTime;

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, Item, KeySchemaElement, KeyType,
    ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::DataEngine;
use extenddb_storage::pitr::PitrCaptureStore;
use extenddb_storage::pitr::conformance::{HistoryFixture, at};
use futures::future::BoxFuture;

use super::record_item_history_in_tx;
use crate::store::SqliteEngine;

const ACCOUNT: &str = "111111111111";

async fn open() -> Option<SqliteEngine> {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, ?)")
        .bind(ACCOUNT)
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
    Some(engine)
}

async fn close(_engine: SqliteEngine) {}

impl HistoryFixture for SqliteEngine {
    fn store(&self) -> &dyn PitrCaptureStore {
        self
    }

    fn data(&self) -> &dyn DataEngine {
        self
    }

    fn table<'a>(
        &'a self,
        name: &'a str,
        sk: Option<ScalarAttributeType>,
        enabled: bool,
    ) -> BoxFuture<'a, TableKeyInfo> {
        Box::pin(async move {
            let mut key_schema = vec![KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            }];
            let mut attribute_definitions = vec![AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            }];
            if let Some(t) = sk {
                key_schema.push(KeySchemaElement {
                    attribute_name: "sk".to_owned(),
                    key_type: KeyType::Range,
                });
                attribute_definitions.push(AttributeDefinition {
                    attribute_name: "sk".to_owned(),
                    attribute_type: t,
                });
            }
            let desc = self
                .create_table_impl(
                    ACCOUNT,
                    CreateTableInput {
                        table_name: name.to_owned(),
                        key_schema: key_schema.clone(),
                        attribute_definitions: attribute_definitions.clone(),
                        billing_mode: Some(BillingMode::PayPerRequest),
                        ..Default::default()
                    },
                    false,
                )
                .await
                .expect("create the table");
            let table_id = desc.table_id;
            if enabled {
                sqlx::query(
                    "INSERT INTO pitr_state (table_id, recovery_period_days, enabled_at_us) \
                     VALUES (?, 35, 0)",
                )
                .bind(&table_id)
                .execute(&self.pool)
                .await
                .expect("pitr_state");
            }
            TableKeyInfo {
                table_name: name.to_owned(),
                account_id: ACCOUNT.to_owned(),
                table_id,
                base_key_schema: key_schema.clone(),
                key_schema,
                attribute_definitions,
                ..Default::default()
            }
        })
    }

    fn record<'a>(
        &'a self,
        info: &'a TableKeyInfo,
        key: &'a Item,
        image: Option<&'a Item>,
        at: SystemTime,
        commit: bool,
    ) -> BoxFuture<'a, bool> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.expect("begin");
            let added = record_item_history_in_tx(&mut tx, info, key, image, at)
                .await
                .expect("record");
            if commit {
                tx.commit().await.expect("commit");
            } else {
                tx.rollback().await.expect("rollback");
            }
            added
        })
    }

    fn commit_times<'a>(&'a self, table_id: &'a str) -> BoxFuture<'a, Vec<i64>> {
        Box::pin(async move {
            sqlx::query_scalar(
                "SELECT commit_time_us FROM item_history WHERE table_id = ? ORDER BY seq",
            )
            .bind(table_id)
            .fetch_all(&self.pool)
            .await
            .expect("commit times")
        })
    }

    fn live_items<'a>(&'a self, info: &'a TableKeyInfo) -> BoxFuture<'a, Vec<Item>> {
        Box::pin(async move {
            let sql = format!(
                "SELECT item_data FROM {}",
                crate::data::data_table_name(&info.table_id)
            );
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(&sql)
                .fetch_all(&self.pool)
                .await
                .expect("live items");
            rows.into_iter()
                .map(|(v,)| serde_json::from_value(v).expect("item"))
                .collect()
        })
    }

    fn history_rows<'a>(&'a self, table_id: &'a str) -> BoxFuture<'a, i64> {
        Box::pin(async move {
            sqlx::query_scalar("SELECT COUNT(*) FROM item_history WHERE table_id = ?")
                .bind(table_id)
                .fetch_one(&self.pool)
                .await
                .expect("count")
        })
    }

    fn execute<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            sqlx::query(sql).execute(&self.pool).await.expect("execute");
        })
    }
}

extenddb_storage::pitr_conformance_tests!(open, close);

fn item(pk: &str) -> Item {
    Item::from([(
        "pk".to_owned(),
        extenddb_core::types::AttributeValue::S(pk.to_owned()),
    )])
}

/// The history and the recovery setting go with the table's catalog row.
#[tokio::test]
async fn dropping_the_catalog_row_drops_its_history_and_state() {
    let engine = open().await.expect("engine");
    let info = engine.table("t", None, true).await;
    let k = item("k");
    assert!(engine.record(&info, &k, Some(&k), at(1), true).await);
    sqlx::query("DELETE FROM tables WHERE table_id = ?")
        .bind(&info.table_id)
        .execute(&engine.pool)
        .await
        .expect("delete");
    assert_eq!(engine.history_rows(&info.table_id).await, 0);
    let state_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pitr_state")
        .fetch_one(&engine.pool)
        .await
        .expect("count");
    assert_eq!(state_rows, 0);
}

/// The reconstruct reads through the key index, with no table scan.
#[tokio::test]
async fn reconstruct_reads_through_the_key_index() {
    let engine = open().await.expect("engine");
    let info = engine.table("t", None, true).await;
    let k = item("k");
    engine.record(&info, &k, Some(&k), at(1), true).await;
    let sql = format!("EXPLAIN QUERY PLAN {}", super::reconstruct_sql());
    let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(&sql)
        .bind(&info.table_id)
        .bind(0_i64)
        .bind(0_i64)
        .bind(10_i64)
        .fetch_all(&engine.pool)
        .await
        .expect("plan");
    let text: Vec<&str> = plan.iter().map(|r| r.3.as_str()).collect();
    assert!(
        text.iter().any(|l| l.contains("idx_item_history_key")),
        "{text:?}"
    );
    assert!(text.iter().all(|l| !l.starts_with("SCAN")), "{text:?}");
}

/// A file database with a pool of four connections, where a write and an
/// enable can run on two connections at once.
async fn open_pooled() -> (std::sync::Arc<SqliteEngine>, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("eddb-pitr-{}.db", uuid::Uuid::new_v4()));
    let engine = SqliteEngine::new(&path.to_string_lossy(), 4, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, ?)")
        .bind(ACCOUNT)
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
    (std::sync::Arc::new(engine), path)
}

async fn close_pooled(engine: std::sync::Arc<SqliteEngine>, path: std::path::PathBuf) {
    let engine = std::sync::Arc::into_inner(engine).expect("sole owner");
    engine.pool.close().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.to_string_lossy()));
    }
}

fn valued(v: &str) -> Item {
    let mut i = item("k");
    i.insert(
        "v".to_owned(),
        extenddb_core::types::AttributeValue::S(v.to_owned()),
    );
    i
}

/// A write as the data path runs it, held open: the engine write lock, then
/// an immediate transaction with the data write and its history statement.
/// Returns what the history statement returned.
async fn write_in_flight<'a>(
    engine: &'a SqliteEngine,
    info: &TableKeyInfo,
    new: &Item,
) -> (
    tokio::sync::OwnedMutexGuard<()>,
    sqlx::Transaction<'a, sqlx::Sqlite>,
    bool,
) {
    let writer = engine.write_lock.clone().lock_owned().await;
    let mut tx = engine
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin");
    sqlx::query(&format!(
        "UPDATE {} SET item_data = ? WHERE pk = 'k'",
        crate::data::data_table_name(&info.table_id)
    ))
    .bind(serde_json::to_string(new).expect("json"))
    .execute(&mut *tx)
    .await
    .expect("write");
    let recorded = record_item_history_in_tx(&mut tx, info, new, Some(new), SystemTime::now())
        .await
        .expect("history statement");
    (writer, tx, recorded)
}

/// An enable that starts while a write is open waits for it, so its
/// baseline holds the write: the write's history statement ran before the
/// enable and recorded nothing.
#[tokio::test]
async fn an_enable_waits_for_a_write_in_flight() {
    use extenddb_storage::pitr::conformance::state;
    let (engine, path) = open_pooled().await;
    let info = engine.table("t", None, false).await;
    engine
        .put_item(&info, valued("old"), false, None, &Default::default(), None)
        .await
        .expect("put");
    let new = valued("new");
    let (writer, tx, recorded) = write_in_flight(&engine, &info, &new).await;
    assert!(!recorded);
    let enabling = {
        let (engine, info) = (engine.clone(), info.clone());
        tokio::spawn(async move {
            engine
                .enable_pitr(&info, None, SystemTime::now())
                .await
                .expect("enable")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !enabling.is_finished(),
        "the enable ran past the open write"
    );
    tx.commit().await.expect("commit the write");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(10), enabling)
        .await
        .expect("the enable finishes once the write commits")
        .expect("enable task");
    assert_eq!(
        state(&*engine, &info, SystemTime::now(), 10).await,
        vec![serde_json::to_string(&new).expect("json")]
    );
    close_pooled(engine, path).await;
}

/// A disable that starts while a write is open waits for it, so it removes
/// the write's history row too: no row of the write survives the disable.
#[tokio::test]
async fn a_disable_waits_for_a_write_in_flight() {
    let (engine, path) = open_pooled().await;
    let info = engine.table("t", None, false).await;
    engine
        .put_item(&info, valued("old"), false, None, &Default::default(), None)
        .await
        .expect("put");
    engine
        .enable_pitr(&info, None, SystemTime::now())
        .await
        .expect("enable");
    let new = valued("new");
    let (writer, tx, recorded) = write_in_flight(&engine, &info, &new).await;
    assert!(recorded);
    let disabling = {
        let (engine, info) = (engine.clone(), info.clone());
        tokio::spawn(async move { engine.disable_pitr(&info).await.expect("disable") })
    };
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !disabling.is_finished(),
        "the disable ran past the open write"
    );
    tx.commit().await.expect("commit the write");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(10), disabling)
        .await
        .expect("the disable finishes once the write commits")
        .expect("disable task");
    assert_eq!(engine.history_rows(&info.table_id).await, 0);
    assert_eq!(
        engine.pitr_state(&info.table_id).await.expect("state"),
        None
    );
    close_pooled(engine, path).await;
}

/// An enable or a disable of a table a DeleteTable removed names the table
/// as not found.
#[tokio::test]
async fn an_enable_or_disable_of_a_deleted_table_is_table_not_found() {
    let engine = open().await.expect("engine");
    let info = engine.table("gone", None, false).await;
    sqlx::query("DELETE FROM tables WHERE table_id = ?")
        .bind(&info.table_id)
        .execute(&engine.pool)
        .await
        .expect("delete the catalog row");
    let enabled = engine.enable_pitr(&info, None, SystemTime::now()).await;
    assert!(
        matches!(enabled, Err(extenddb_storage::error::StorageError::TableNotFound(ref n)) if n == "gone"),
        "{enabled:?}"
    );
    let disabled = engine.disable_pitr(&info).await;
    assert!(
        matches!(disabled, Err(extenddb_storage::error::StorageError::TableNotFound(ref n)) if n == "gone"),
        "{disabled:?}"
    );
}
