// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The PostgreSQL item-history store: the shared checks of
//! `extenddb_storage::pitr::conformance`, then the query plan.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`); skips otherwise. Each test builds
//! its own scratch databases and drops them afterwards, on a panic too.

#![cfg(feature = "pitr")]

mod pitr_support;

use std::time::SystemTime;

use extenddb_core::types::{AttributeValue, Item, ScalarAttributeType};
use extenddb_storage::DataEngine;
use extenddb_storage::pitr::PitrCaptureStore;
use extenddb_storage::pitr::conformance::{HistoryFixture, at};
use extenddb_storage_postgres::PostgresEngine;
use pitr_support::{close, open, open_two_databases};

extenddb_storage::pitr_conformance_tests!(open, close);

/// The reconstruct reads through the key index, with no sequential scan, on
/// a history large enough that the planner would pick one if it could.
#[tokio::test]
async fn reconstruct_reads_through_the_key_index() {
    let Some(s) = open().await else { return };
    let info = s.table("t", None, true).await;
    for i in 0..2_000 {
        let k = Item::from([("pk".to_owned(), AttributeValue::S(format!("k{}", i % 50)))]);
        s.record(&info, &k, Some(&k), at(i), true).await;
    }
    sqlx::query("ANALYZE item_history")
        .execute(&s.db)
        .await
        .expect("analyze");
    let sql = format!(
        "EXPLAIN {}",
        PostgresEngine::item_history_reconstruct_sql_for_tests()
    );
    let plan: Vec<String> = sqlx::query_scalar(&sql)
        .bind(&info.table_id)
        .bind(i64::MAX)
        .bind(i64::MIN)
        .bind(10_i64)
        .fetch_all(&s.db)
        .await
        .expect("plan");
    assert!(
        plan.iter().any(|l| l.contains("idx_item_history_key")),
        "{plan:#?}"
    );
    assert!(plan.iter().all(|l| !l.contains("Seq Scan")), "{plan:#?}");
    close(s).await;
}

/// Concurrent writers on a few keys: once they finish, the state now equals
/// the live table. Two writes to one item take its row lock in turn and each
/// draws its `seq` after its data write, so a key's latest history row is its
/// last committed write. Deletes of missing keys race first puts of the same
/// keys here, the case where a recorded delete could order wrongly.
#[tokio::test]
async fn concurrent_writers_leave_the_state_equal_to_the_live_table() {
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_storage::pitr::conformance::state;
    let Some(s) = open().await else { return };
    let info = s.table("t", None, true).await;
    let mut tasks = Vec::new();
    let engine = std::sync::Arc::new(s);
    for task in 0..8_u64 {
        let engine = engine.clone();
        let info = info.clone();
        tasks.push(tokio::spawn(async move {
            let maps = ExpressionMaps::default();
            let mut x = task.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            for i in 0..60_u64 {
                x = x
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let key = Item::from([(
                    "pk".to_owned(),
                    AttributeValue::S(format!("k{}", (x >> 33) % 3)),
                )]);
                if (x >> 40) % 3 == 0 {
                    engine
                        .engine
                        .delete_item(&info, &key, false, None, &maps, None)
                        .await
                        .expect("delete");
                } else {
                    let mut item = key.clone();
                    item.insert("v".to_owned(), AttributeValue::S(format!("{task}-{i}")));
                    engine
                        .engine
                        .put_item(&info, item, false, None, &maps, None)
                        .await
                        .expect("put");
                }
            }
        }));
    }
    for t in tasks {
        t.await.expect("writer");
    }
    let s = std::sync::Arc::into_inner(engine).expect("sole owner");
    let live: Vec<String> = {
        let mut v: Vec<String> = s
            .live_items(&info)
            .await
            .iter()
            .map(|i| serde_json::to_string(i).expect("json"))
            .collect();
        v.sort();
        v
    };
    assert_eq!(state(&s, &info, SystemTime::now(), 2).await, live);
    close(s).await;
}

/// A delete of a key whose first put is still uncommitted deletes nothing and
/// records nothing. The delete takes no row lock, so a recorded delete would
/// draw a higher `seq` than the put that commits after it, and the reconstruct
/// would drop the item the table holds.
#[tokio::test]
async fn a_delete_racing_an_uncommitted_first_put_records_nothing() {
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_storage::pitr::conformance::state;
    let Some(s) = open().await else { return };
    let info = s.table("t", None, true).await;
    let item = Item::from([
        ("pk".to_owned(), AttributeValue::S("k".to_owned())),
        ("v".to_owned(), AttributeValue::S("a".to_owned())),
    ]);
    let mut put = s.db.begin().await.expect("begin");
    sqlx::query(&format!(
        "INSERT INTO \"_ddb_{}\" (pk, item_data) VALUES ('k', $1)",
        info.table_id
    ))
    .bind(serde_json::to_value(&item).expect("json"))
    .execute(&mut *put)
    .await
    .expect("insert the item");
    sqlx::query(
        "INSERT INTO item_history (table_id, pk, sk, commit_time_us, item_data) \
         VALUES ($1, 'k', '', $2, $3)",
    )
    .bind(&info.table_id)
    .bind(extenddb_storage::pitr::unix_micros(SystemTime::now()).expect("time"))
    .bind(serde_json::to_value(&item).expect("json"))
    .execute(&mut *put)
    .await
    .expect("record the put");
    let key = Item::from([("pk".to_owned(), AttributeValue::S("k".to_owned()))]);
    let maps = ExpressionMaps::default();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        s.engine.delete_item(&info, &key, false, None, &maps, None),
    )
    .await
    .expect("the delete does not wait on the uncommitted put")
    .expect("delete");
    put.commit().await.expect("commit the put");
    assert_eq!(s.live_items(&info).await.len(), 1);
    assert_eq!(s.history_rows(&info.table_id).await, 1);
    assert_eq!(
        state(&s, &info, SystemTime::now(), 10).await,
        vec![serde_json::to_string(&item).expect("json")]
    );
    close(s).await;
}

/// Writers keep writing while recovery is enabled: once they finish, the
/// state now equals the live table. The enable excludes the table's writers
/// while it takes the baseline, so a write lands in the baseline or in the
/// history, never in neither.
#[tokio::test]
async fn an_enable_during_writes_loses_no_write() {
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_storage::pitr::conformance::state;
    let Some(s) = open().await else { return };
    let info = s.table("t", None, false).await;
    let engine = std::sync::Arc::new(s);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tasks = Vec::new();
    for task in 0..6_u32 {
        let (engine, info, stop) = (engine.clone(), info.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            let maps = ExpressionMaps::default();
            let mut i = 0_u32;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let item = Item::from([
                    (
                        "pk".to_owned(),
                        AttributeValue::S(format!("k{}", (task + i) % 5)),
                    ),
                    ("v".to_owned(), AttributeValue::S(format!("{task}-{i}"))),
                ]);
                engine
                    .engine
                    .put_item(&info, item, false, None, &maps, None)
                    .await
                    .expect("put");
                i += 1;
            }
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    engine
        .engine
        .enable_pitr(&info, None, SystemTime::now())
        .await
        .expect("enable");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for t in tasks {
        t.await.expect("writer");
    }
    let s = std::sync::Arc::into_inner(engine).expect("sole owner");
    let mut live: Vec<String> = s
        .live_items(&info)
        .await
        .iter()
        .map(|i| serde_json::to_string(i).expect("json"))
        .collect();
    live.sort();
    assert_eq!(state(&s, &info, SystemTime::now(), 3).await, live);
    close(s).await;
}

/// A write whose transaction is open when the enable starts: its history
/// statement ran before the enable, so it recorded nothing, and it commits
/// after. The enable waits for it, so the baseline holds the write; an enable
/// that did not wait would take its baseline without the write, and the
/// write would be in neither.
#[tokio::test]
async fn an_enable_waits_for_a_write_in_flight() {
    use extenddb_storage::pitr::conformance::state;
    let Some(s) = open().await else { return };
    let info = s.table("t", None, false).await;
    let old = Item::from([
        ("pk".to_owned(), AttributeValue::S("k".to_owned())),
        ("v".to_owned(), AttributeValue::S("old".to_owned())),
    ]);
    let mut new = old.clone();
    new.insert("v".to_owned(), AttributeValue::S("new".to_owned()));
    s.engine
        .put_item(&info, old, false, None, &Default::default(), None)
        .await
        .expect("put");
    let mut write = s.db.begin().await.expect("begin");
    sqlx::query(&format!(
        "UPDATE \"_ddb_{}\" SET item_data = $1 WHERE pk = 'k'",
        info.table_id
    ))
    .bind(serde_json::to_value(&new).expect("json"))
    .execute(&mut *write)
    .await
    .expect("write");
    let recorded = sqlx::query(
        "INSERT INTO item_history (table_id, pk, sk, commit_time_us, item_data) \
         SELECT $1, 'k', '', 0, $2 WHERE EXISTS (SELECT 1 FROM pitr_state WHERE table_id = $1)",
    )
    .bind(&info.table_id)
    .bind(serde_json::to_value(&new).expect("json"))
    .execute(&mut *write)
    .await
    .expect("history statement");
    assert_eq!(recorded.rows_affected(), 0);
    let s = std::sync::Arc::new(s);
    let enabling = {
        let (s, info) = (s.clone(), info.clone());
        tokio::spawn(async move {
            s.engine
                .enable_pitr(&info, None, SystemTime::now())
                .await
                .expect("enable")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    write.commit().await.expect("commit the write");
    tokio::time::timeout(std::time::Duration::from_secs(10), enabling)
        .await
        .expect("the enable finishes once the write commits")
        .expect("enable task");
    let s = std::sync::Arc::into_inner(s).expect("sole owner");
    assert_eq!(
        state(&s, &info, SystemTime::now(), 10).await,
        vec![serde_json::to_string(&new).expect("json")]
    );
    close(s).await;
}

/// The catalog is another database, so a deleted table's history and setting
/// stay in the data database until the retention sweep removes them, a batch
/// at a time, the setting last; a live table's history is left.
#[tokio::test]
async fn the_sweep_removes_the_history_of_deleted_tables() {
    let Some(s) = open().await else { return };
    let gone = s.table("gone", None, true).await;
    let kept = s.table("kept", None, true).await;
    for i in 0..5_u64 {
        let k = Item::from([("pk".to_owned(), AttributeValue::S(format!("k{i}")))]);
        s.record(&gone, &k, Some(&k), at(i), true).await;
        s.record(&kept, &k, Some(&k), at(i), true).await;
    }
    sqlx::query("DELETE FROM tables WHERE table_id = $1")
        .bind(&gone.table_id)
        .execute(&s.catalog)
        .await
        .expect("delete the catalog row");
    let report = extenddb_storage::pitr::sweep(
        &s.engine,
        at(10),
        2,
        std::time::Instant::now() + std::time::Duration::from_secs(60),
    )
    .await
    .expect("sweep");
    assert_eq!(report.orphaned, 5);
    assert_eq!(s.history_rows(&gone.table_id).await, 0);
    assert_eq!(s.history_rows(&kept.table_id).await, 5);
    let states: Vec<String> = sqlx::query_scalar("SELECT table_id FROM pitr_state")
        .fetch_all(&s.db)
        .await
        .expect("states");
    assert_eq!(states, vec![kept.table_id.clone()]);
    close(s).await;
}

/// The orphan sweep with the catalog and the data in two databases, as a
/// deployment runs them: the live table ids come from the catalog, which the
/// data database does not hold.
#[tokio::test]
async fn the_sweep_reads_live_tables_from_the_catalog_database() {
    let Some(s) = open_two_databases().await else {
        return;
    };
    let gone = s.table("gone", None, true).await;
    let kept = s.table("kept", None, true).await;
    for i in 0..3_u64 {
        let k = Item::from([("pk".to_owned(), AttributeValue::S(format!("k{i}")))]);
        s.record(&gone, &k, Some(&k), at(i), true).await;
        s.record(&kept, &k, Some(&k), at(i), true).await;
    }
    let catalog_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_name = 'tables'",
    )
    .fetch_one(&s.db)
    .await
    .expect("information schema");
    assert_eq!(catalog_tables, 0, "the data database holds no catalog");
    sqlx::query("DELETE FROM tables WHERE table_id = $1")
        .bind(&gone.table_id)
        .execute(&s.catalog)
        .await
        .expect("delete the catalog row");
    let report = extenddb_storage::pitr::sweep(
        &s.engine,
        at(10),
        100,
        std::time::Instant::now() + std::time::Duration::from_secs(60),
    )
    .await
    .expect("sweep");
    assert_eq!(report.orphaned, 3);
    assert_eq!(s.history_rows(&gone.table_id).await, 0);
    assert_eq!(s.history_rows(&kept.table_id).await, 3);
    close(s).await;
}

/// A disable that starts while a write of the table is open waits for it,
/// so it removes the write's history row too. A disable that did not wait
/// would delete the rows it sees, and the write's row would commit after it
/// and survive the disable.
#[tokio::test]
async fn a_disable_waits_for_a_write_in_flight() {
    let Some(s) = open().await else { return };
    let info = s.table("t", None, false).await;
    let old = Item::from([
        ("pk".to_owned(), AttributeValue::S("k".to_owned())),
        ("v".to_owned(), AttributeValue::S("old".to_owned())),
    ]);
    let mut new = old.clone();
    new.insert("v".to_owned(), AttributeValue::S("new".to_owned()));
    s.engine
        .put_item(&info, old, false, None, &Default::default(), None)
        .await
        .expect("put");
    s.engine
        .enable_pitr(&info, None, SystemTime::now())
        .await
        .expect("enable");
    let mut write = s.db.begin().await.expect("begin");
    sqlx::query(&format!(
        "UPDATE \"_ddb_{}\" SET item_data = $1 WHERE pk = 'k'",
        info.table_id
    ))
    .bind(serde_json::to_value(&new).expect("json"))
    .execute(&mut *write)
    .await
    .expect("write");
    let recorded = sqlx::query(
        "INSERT INTO item_history (table_id, pk, sk, commit_time_us, item_data) \
         SELECT $1, 'k', '', 0, $2 WHERE EXISTS (SELECT 1 FROM pitr_state WHERE table_id = $1)",
    )
    .bind(&info.table_id)
    .bind(serde_json::to_value(&new).expect("json"))
    .execute(&mut *write)
    .await
    .expect("history statement");
    assert_eq!(recorded.rows_affected(), 1);
    let s = std::sync::Arc::new(s);
    let disabling = {
        let (s, info) = (s.clone(), info.clone());
        tokio::spawn(async move { s.engine.disable_pitr(&info).await.expect("disable") })
    };
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !disabling.is_finished(),
        "the disable ran past the open write"
    );
    write.commit().await.expect("commit the write");
    tokio::time::timeout(std::time::Duration::from_secs(10), disabling)
        .await
        .expect("the disable finishes once the write commits")
        .expect("disable task");
    let s = std::sync::Arc::into_inner(s).expect("sole owner");
    assert_eq!(s.history_rows(&info.table_id).await, 0);
    assert_eq!(
        s.engine.pitr_state(&info.table_id).await.expect("state"),
        None
    );
    close(s).await;
}

/// A put and a delete that need no transaction, held by an enable's lock:
/// each runs once the enable commits and records its history row, because
/// the statement that writes the item also reads the state, after the lock.
#[tokio::test]
async fn a_write_held_by_an_enable_records_its_history() {
    use extenddb_core::expression::ExpressionMaps;
    let Some(s) = open().await else { return };
    let plain = s.table("plain", None, false).await;
    let sorted_table = s.table("sorted", Some(ScalarAttributeType::N), false).await;
    let keyed = Item::from([
        ("pk".to_owned(), AttributeValue::S("k".to_owned())),
        ("sk".to_owned(), AttributeValue::N("1.50".to_owned())),
    ]);
    s.engine
        .put_item(
            &sorted_table,
            keyed.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("put");
    let s = std::sync::Arc::new(s);
    for (info, delete) in [(plain.clone(), false), (sorted_table.clone(), true)] {
        let mut enable = s.db.begin().await.expect("begin");
        sqlx::query(&format!(
            "LOCK TABLE \"_ddb_{}\" IN SHARE ROW EXCLUSIVE MODE",
            info.table_id
        ))
        .execute(&mut *enable)
        .await
        .expect("lock");
        let writing = {
            let (s, info) = (s.clone(), info.clone());
            tokio::spawn(async move {
                let maps = ExpressionMaps::default();
                if delete {
                    let key = Item::from([
                        ("pk".to_owned(), AttributeValue::S("k".to_owned())),
                        ("sk".to_owned(), AttributeValue::N("1.5".to_owned())),
                    ]);
                    s.engine
                        .delete_item(&info, &key, false, None, &maps, None)
                        .await
                        .map(|_| ())
                } else {
                    let item = Item::from([("pk".to_owned(), AttributeValue::S("p".to_owned()))]);
                    s.engine
                        .put_item(&info, item, false, None, &maps, None)
                        .await
                        .map(|_| ())
                }
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !writing.is_finished(),
            "the write ran past the enable's lock"
        );
        sqlx::query(
            "INSERT INTO pitr_state (table_id, recovery_period_days, enabled_at_us) \
             VALUES ($1, 35, 0)",
        )
        .bind(&info.table_id)
        .execute(&mut *enable)
        .await
        .expect("state");
        enable.commit().await.expect("commit the enable");
        tokio::time::timeout(std::time::Duration::from_secs(10), writing)
            .await
            .expect("the write finishes once the enable commits")
            .expect("write task")
            .expect("write");
        assert_eq!(
            s.history_rows(&info.table_id).await,
            1,
            "{}",
            info.table_name
        );
    }
    let rows: Vec<(String, Option<serde_json::Value>)> =
        sqlx::query_as("SELECT sk, item_data FROM item_history WHERE table_id = $1")
            .bind(&sorted_table.table_id)
            .fetch_all(&s.db)
            .await
            .expect("rows");
    assert_eq!(rows, vec![("1.5".to_owned(), None)]);
    let s = std::sync::Arc::into_inner(s).expect("sole owner");
    close(s).await;
}

/// An enable or a disable that runs after a DeleteTable dropped the data
/// table names the table as not found.
#[tokio::test]
async fn an_enable_or_disable_after_the_drop_is_table_not_found() {
    use extenddb_storage::error::StorageError;
    let Some(s) = open().await else { return };
    let info = s.table("gone", None, false).await;
    sqlx::query(&format!("DROP TABLE \"_ddb_{}\"", info.table_id))
        .execute(&s.db)
        .await
        .expect("drop the data table");
    let enabled = s.engine.enable_pitr(&info, None, SystemTime::now()).await;
    assert!(
        matches!(enabled, Err(StorageError::TableNotFound(ref n)) if n == "gone"),
        "{enabled:?}"
    );
    let disabled = s.engine.disable_pitr(&info).await;
    assert!(
        matches!(disabled, Err(StorageError::TableNotFound(ref n)) if n == "gone"),
        "{disabled:?}"
    );
    close(s).await;
}
