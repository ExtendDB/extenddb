// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Controls on the restore copy's write path: what a restored row's write
//! timestamp is, that a backup whose payload does not fit its target or its
//! recorded count fails the restore, and that the rows the restore writes
//! have the shape the ordinary put path writes.

use std::collections::BTreeMap;
use std::time::Duration;

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeValue, BillingMode, CreateTableInput, DescribeTableInput, GsiInput, KeyType,
    Projection, ProjectionType, ScalarAttributeType, TableKeyInfo, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, DataEngine, RestoreTableOverrides, TableEngine};
use extenddb_storage_cassandra::CassandraEngine;

use crate::backup_engine::{
    attr, create_active_table, item, ks, make_gsis_synchronous, put_all, restore,
};
use crate::helpers::{TestAccount, setup_engine};
use crate::restore_write::{n, restored_indexed_table, s};

type Item = BTreeMap<String, AttributeValue>;

/// An item carrying every key of `restored_indexed_table`'s schema.
fn indexed_item(id: &str, sort: &str, group: &str, gsk: i64, lsk: i64, value: &str) -> Item {
    let mut it = item(id, Some(sort), value);
    it.insert("gpk".to_owned(), s(group));
    it.insert("gsk".to_owned(), n(gsk));
    it.insert("lsk".to_owned(), n(lsk));
    it
}

fn account_keyspace(account_id: &str) -> String {
    format!("extenddb_ttl_test_account_{account_id}")
}

fn items_table(table_id: &str) -> String {
    format!("items_{}", table_id.replace('-', "_"))
}

/// `(index_name, index table name)` for every index of `table_id`.
async fn index_tables(engine: &CassandraEngine, table_id: &str) -> Vec<(String, String)> {
    let cql = format!(
        "SELECT index_name, index_id FROM {}.indexes WHERE table_id = ?",
        engine.catalog_keyspace()
    );
    let mut out: Vec<(String, String)> = engine
        .session()
        .query_with_values(&cql, cdrs_tokio::query_values!(table_id))
        .await
        .expect("indexes")
        .response_body()
        .expect("indexes body")
        .into_rows()
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            let name: String = row.get_r_by_name("index_name").expect("index_name");
            let id: String = row.get_r_by_name("index_id").expect("index_id");
            (name, format!("index_{}", id.replace('-', "_")))
        })
        .collect();
    out.sort();
    out
}

async fn rows(engine: &CassandraEngine, cql: &str) -> Vec<cdrs_tokio::types::rows::Row> {
    engine
        .session()
        .query(cql)
        .await
        .expect("query")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default()
}

/// Poll `target` until it is gone (the copy failed and the target was
/// removed) or `timeout` passes; panic if it turns ACTIVE.
async fn wait_removed(engine: &CassandraEngine, account_id: &str, target: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let described = engine
            .describe_table(
                account_id,
                DescribeTableInput {
                    table_name: target.to_owned(),
                },
            )
            .await;
        match described {
            Err(StorageError::TableNotFound(_)) => return,
            Ok(d) if d.table_status == TableStatus::Active => panic!(
                "{target} turned ACTIVE (item_count {}) where the copy had to fail",
                d.item_count
            ),
            Ok(_) => {}
            Err(e) => panic!("describe {target}: {e:?}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{target} still present after {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Append `item` to `backup_arn`'s first payload bucket as its fourth row and
/// record `item_count` on the backup.
async fn add_backup_item(
    engine: &CassandraEngine,
    account_id: &str,
    backup_arn: &str,
    item: &Item,
    item_count: i64,
) {
    let payload = serde_json::to_string(item).expect("json");
    let insert = format!(
        "INSERT INTO {}.backup_items (backup_arn, bucket, item_index, item_data) VALUES (?, 0, 3, ?)",
        account_keyspace(account_id)
    );
    engine
        .session()
        .query_with_values(
            &insert,
            cdrs_tokio::query_values!(backup_arn, payload.as_str()),
        )
        .await
        .expect("insert payload row");
    set_item_count(engine, account_id, backup_arn, item_count).await;
}

async fn set_item_count(engine: &CassandraEngine, account_id: &str, backup_arn: &str, count: i64) {
    let bump = format!(
        "UPDATE {}.backups_by_arn SET item_count = ? WHERE account_id = ? AND backup_arn = ? IF EXISTS",
        engine.catalog_keyspace()
    );
    engine
        .session()
        .query_with_values(
            &bump,
            cdrs_tokio::query_values!(count, account_id, backup_arn),
        )
        .await
        .expect("set item_count");
}

/// A pk-only source table with one GSI on `gpk` (S), three items with `gpk`
/// set, backed up. Returns the account, the source key info and the ARN.
async fn backed_up_gsi_table(
    engine: &CassandraEngine,
    name: &str,
) -> (TestAccount, TableKeyInfo, String) {
    let account = TestAccount::new(engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let source = create_active_table(
        engine,
        &account_id,
        CreateTableInput {
            table_name: name.to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![
                attr("id", ScalarAttributeType::S),
                attr("gpk", ScalarAttributeType::S),
            ],
            billing_mode: Some(BillingMode::PayPerRequest),
            global_secondary_indexes: Some(vec![GsiInput {
                index_name: "ByGroup".to_owned(),
                key_schema: vec![ks("gpk", KeyType::Hash)],
                projection: Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            }]),
            ..Default::default()
        },
    )
    .await;
    make_gsis_synchronous(engine, &source.table_id, &["ByGroup"]).await;
    let items = (0..3)
        .map(|i| {
            let mut it = item(&format!("k{i}"), None, "v");
            it.insert("gpk".to_owned(), s("g"));
            it
        })
        .collect();
    put_all(engine, &source, items).await;
    let backup = engine
        .create_backup(&account_id, name, "control")
        .await
        .expect("create backup");
    (account, source, backup.backup_arn)
}

/// One restored base row's `version`, its write time, and the write times of
/// its index rows by index name.
struct RowStamps {
    version: i64,
    base_wt: i64,
    index_wts: Vec<(String, i64)>,
}

/// Every restored index row's write time is the coordinator's, as the base
/// row's is. The coordinator hands each statement its own timestamp, so an
/// item's GSI row and LSI row never share one; a restore that stamped them
/// from the host clock would give both the base row's `version`. Every index
/// row's write time is also within 10 ms of its base row's.
#[tokio::test]
async fn test_restored_index_rows_take_the_coordinator_timestamp() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let items = vec![
        indexed_item("a", "1", "g1", 1, 10, "v-a1"),
        indexed_item("b", "1", "g2", 3, 30, "v-b1"),
        {
            let mut it = item("c", Some("2"), "v-c2");
            it.insert("lsk".to_owned(), n(60));
            it
        },
    ];
    let (_account, restored) = restored_indexed_table(&engine, items, "RestoreStamp").await;
    let keyspace = account_keyspace(&restored.account_id);
    let base_table = items_table(&restored.table_id);
    let indexes = index_tables(&engine, &restored.table_id).await;
    assert_eq!(indexes.len(), 2, "{indexes:?}");
    let mut per_item: BTreeMap<String, RowStamps> = BTreeMap::new();
    for (index_name, index_table) in indexes {
        let index_rows = rows(
            &engine,
            &format!(
                "SELECT base_pk, base_sk_s, WRITETIME(item_data) AS wt FROM {keyspace}.{index_table}"
            ),
        )
        .await;
        assert!(!index_rows.is_empty(), "{index_name} has rows");
        for row in index_rows {
            let base_pk: String = row.get_r_by_name("base_pk").expect("base_pk");
            let base_sk: String = row.get_r_by_name("base_sk_s").expect("base_sk_s");
            let index_wt: i64 = row.get_r_by_name("wt").expect("wt");
            let base = engine
                .session()
                .query_with_values(
                    &format!(
                        "SELECT version, WRITETIME(item_data) AS wt FROM {keyspace}.{base_table} \
                         WHERE pk = ? AND sk_s = ?"
                    ),
                    cdrs_tokio::query_values!(base_pk.as_str(), base_sk.as_str()),
                )
                .await
                .expect("base row")
                .response_body()
                .expect("base body")
                .into_rows()
                .unwrap_or_default();
            let version: i64 = base[0].get_r_by_name("version").expect("version");
            let base_wt: i64 = base[0].get_r_by_name("wt").expect("base wt");
            per_item
                .entry(format!("{base_pk}/{base_sk}"))
                .or_insert(RowStamps {
                    version,
                    base_wt,
                    index_wts: Vec::new(),
                })
                .index_wts
                .push((index_name.clone(), index_wt));
        }
    }
    assert_eq!(per_item.len(), 3);
    let mut stamped_with_version = 0;
    let mut checked = 0;
    for (
        key,
        RowStamps {
            version,
            base_wt,
            index_wts,
        },
    ) in &per_item
    {
        for (index_name, index_wt) in index_wts {
            assert!(
                (index_wt - base_wt).abs() <= 10_000,
                "{index_name} row {key}: index write time {index_wt} is not within 10 ms of the \
                 base row's {base_wt}"
            );
            if index_wt == version {
                stamped_with_version += 1;
            }
            checked += 1;
        }
        if let [(first_name, first_wt), (second_name, second_wt)] = index_wts.as_slice() {
            assert_ne!(
                first_wt, second_wt,
                "{key}: the {first_name} and {second_name} rows share a write time; the \
                 coordinator gives each statement its own"
            );
        }
    }
    assert_eq!(checked, 5, "2 GSI rows and 3 LSI rows");
    assert_eq!(
        per_item.values().filter(|r| r.index_wts.len() == 2).count(),
        2,
        "a/1 and b/1 carry a GSI row and an LSI row each"
    );
    assert!(
        stamped_with_version < checked,
        "every index row's write time equals its base row's version: the rows were stamped from \
         the host clock"
    );
}

/// A backup item whose GSI key attribute has the wrong type (N where the
/// index declares S) does not fit the target's index key schema: the copy
/// refuses it and the target is removed, as the ordinary put path refuses
/// the item.
#[tokio::test]
async fn test_a_mistyped_index_key_in_a_backup_item_fails_the_restore() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (account, source, backup_arn) = backed_up_gsi_table(&engine, "TypedKeySource").await;
    let account_id = account.account_id.clone();
    let mut bad = item("k3", None, "v");
    bad.insert("gpk".to_owned(), n(5));
    let refused = engine
        .put_item(
            &source,
            bad.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await;
    assert!(
        matches!(refused, Err(StorageError::Validation(_))),
        "the ordinary put refuses a mistyped index key: {refused:?}"
    );
    add_backup_item(&engine, &account_id, &backup_arn, &bad, 4).await;
    let initial = restore(
        &engine,
        &account_id,
        &backup_arn,
        "TypedKeyTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore accepted");
    assert_eq!(initial.table_status, TableStatus::Creating);
    wait_removed(
        &engine,
        &account_id,
        "TypedKeyTarget",
        Duration::from_secs(90),
    )
    .await;
}

/// A backup whose recorded item count is one more than its payload does not
/// restore: the copy's count check fails it and the target is removed.
#[tokio::test]
async fn test_a_backup_item_count_above_its_payload_fails_the_restore() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, "extenddb_ttl_test").await;
    let account_id = account.account_id.clone();
    let source = create_active_table(
        &engine,
        &account_id,
        CreateTableInput {
            table_name: "CountSource".to_owned(),
            key_schema: vec![ks("id", KeyType::Hash)],
            attribute_definitions: vec![attr("id", ScalarAttributeType::S)],
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        },
    )
    .await;
    put_all(
        &engine,
        &source,
        (0..70)
            .map(|i| item(&format!("k{i:02}"), None, "v"))
            .collect(),
    )
    .await;
    let backup = engine
        .create_backup(&account_id, "CountSource", "count")
        .await
        .expect("create backup");
    set_item_count(&engine, &account_id, &backup.backup_arn, 71).await;
    let initial = restore(
        &engine,
        &account_id,
        &backup.backup_arn,
        "CountTarget",
        &RestoreTableOverrides::default(),
    )
    .await
    .expect("restore accepted");
    assert_eq!(initial.table_status, TableStatus::Creating);
    wait_removed(&engine, &account_id, "CountTarget", Duration::from_secs(90)).await;
}

/// `SELECT JSON *` of every row of `table`, without `version`, sorted.
async fn row_shapes(engine: &CassandraEngine, keyspace: &str, table: &str) -> Vec<String> {
    let mut shapes: Vec<String> = rows(engine, &format!("SELECT JSON * FROM {keyspace}.{table}"))
        .await
        .into_iter()
        .map(|row| {
            let json: String = row.get_r_by_name("[json]").expect("[json]");
            let mut value: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(&json).expect("row json");
            value.remove("version");
            serde_json::Value::Object(value).to_string()
        })
        .collect();
    shapes.sort();
    shapes
}

/// The rows a restore writes have the shape the ordinary put path writes:
/// the base table and each index table of a restored table hold, column by
/// column except `version`, the rows the source table holds for the same
/// items.
#[tokio::test]
async fn test_restored_rows_have_the_put_path_shape() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let items = vec![
        indexed_item("a", "1", "g1", 1, 10, "v-a1"),
        indexed_item("a", "2", "g1", 2, 20, "v-a2"),
        indexed_item("b", "1", "g2", 3, 30, "v-b1"),
        {
            let mut it = item("c", Some("2"), "v-c2");
            it.insert("lsk".to_owned(), n(60));
            it.insert("extra".to_owned(), s("not projected"));
            it
        },
    ];
    let (account, restored) = restored_indexed_table(&engine, items, "RestoreShape").await;
    let account_id = account.account_id.clone();
    let source = engine
        .table_key_info(&account_id, "RestoreShapeSource")
        .await
        .expect("source key info");
    let keyspace = account_keyspace(&account_id);

    let on_source = row_shapes(&engine, &keyspace, &items_table(&source.table_id)).await;
    let on_target = row_shapes(&engine, &keyspace, &items_table(&restored.table_id)).await;
    assert_eq!(on_source.len(), 4);
    assert_eq!(on_target, on_source, "base rows differ from the put path's");

    let source_indexes = index_tables(&engine, &source.table_id).await;
    let target_indexes = index_tables(&engine, &restored.table_id).await;
    assert_eq!(source_indexes.len(), 2);
    assert_eq!(target_indexes.len(), 2);
    for ((name, source_table), (target_name, target_table)) in
        source_indexes.iter().zip(&target_indexes)
    {
        assert_eq!(name, target_name);
        let on_source = row_shapes(&engine, &keyspace, source_table).await;
        let on_target = row_shapes(&engine, &keyspace, target_table).await;
        assert!(!on_source.is_empty(), "{name} has rows on the source");
        assert_eq!(
            on_target, on_source,
            "{name} rows differ from the put path's"
        );
    }
}
