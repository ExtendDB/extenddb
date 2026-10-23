// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog migration V006 (the stored `WarmThroughput` columns) on a catalog
//! that has not run it: the shape a deployment migrated by the previous
//! release has.
//!
//! Ordinary table and item operations work there, as they do without V005.
//! A request that names the member (`CreateTable` or `UpdateTable` with
//! `WarmThroughput`) needs the column and fails until `extenddb migrate
//! --yes` applies V006; `DescribeTable` reports the defaults. The catalog
//! version constant moves to 0.0.5 so `extenddb migrate` sees the catalog as
//! behind and applies V006 (without the bump the command reported nothing to
//! do while the backup paths listed V006 as pending and refused).

use extenddb_core::types::{
    CreateTableInput, DeleteTableInput, TableStatus, UpdateTableInput, WarmThroughputStatus,
};
use extenddb_storage::TableEngine;
use extenddb_storage_cassandra::CATALOG_VERSION;

use crate::backup_engine::{activate_tables, catalog_columns, describe, isolated_engine_through};

/// The constant and the last migration file agree, the same tripwire the
/// PostgreSQL runner has: adding a migration without moving the version
/// leaves `extenddb migrate` with nothing to do.
#[test]
fn the_catalog_version_is_the_one_v006_writes() {
    assert_eq!(CATALOG_VERSION, "0.0.5");
    let v006 = include_str!("../../migrations/catalog/V006__warm_throughput.cql");
    assert!(
        v006.contains("('catalog_version', '0.0.5')"),
        "V006 must stamp the catalog version the binary expects"
    );
    assert_eq!(
        extenddb_storage_cassandra::migrations::CATALOG_MIGRATIONS.len(),
        6,
        "a catalog migration was added or removed; move CATALOG_VERSION and this count"
    );
}

#[tokio::test]
async fn test_table_operations_work_on_a_catalog_without_v006() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let (engine, account_id, _drop_keyspaces) = isolated_engine_through("v5", Some(5)).await;
    let columns = catalog_columns(&engine, &engine.catalog_keyspace()).await;
    assert!(
        columns.iter().any(|c| c == "restore_heartbeat"),
        "V005 present"
    );
    assert!(
        !columns.iter().any(|c| c == "warm_throughput"),
        "V006 must not have run: {columns:?}"
    );

    let input = |name: &str, warm: Option<serde_json::Value>| -> CreateTableInput {
        let mut v = serde_json::json!({
            "TableName": name,
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "ByG",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }]
        });
        if let Some(w) = warm {
            v["WarmThroughput"] = w;
        }
        serde_json::from_value(v).expect("input")
    };
    // A CreateTable without the member works and describes the defaults.
    engine
        .create_table(&account_id, input("Unmigrated", None))
        .await
        .expect("CreateTable without WarmThroughput on a catalog without V006");
    activate_tables(&engine).await;
    let mut described = describe(&engine, &account_id, "Unmigrated").await;
    assert_eq!(described.table_status, TableStatus::Active);
    described.prepare_for_response();
    let wt = described.warm_throughput.expect("defaults");
    assert_eq!(
        (
            wt.read_units_per_second,
            wt.write_units_per_second,
            wt.status
        ),
        (Some(12_000), Some(4_000), WarmThroughputStatus::Active)
    );

    // Naming the member needs the column.
    let refused = engine
        .create_table(
            &account_id,
            input(
                "NeedsV006",
                Some(serde_json::json!({"ReadUnitsPerSecond": 20000})),
            ),
        )
        .await;
    assert!(
        refused.is_err(),
        "CreateTable with WarmThroughput before V006"
    );
    let update: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": "Unmigrated",
        "WarmThroughput": {"ReadUnitsPerSecond": 20000}
    }))
    .expect("update");
    assert!(
        engine.update_table(&account_id, update).await.is_err(),
        "UpdateTable with WarmThroughput before V006"
    );

    // `extenddb migrate --yes` applies V006 (the runner skips the recorded
    // files); the same engine then stores the member without a restart.
    extenddb_storage_cassandra::migrations::run_catalog_migrations(
        &engine.session_arc(),
        &engine.catalog_keyspace(),
    )
    .await
    .expect("apply V006");
    let columns = catalog_columns(&engine, &engine.catalog_keyspace()).await;
    assert_eq!(
        columns.iter().filter(|c| *c == "warm_throughput").count(),
        2,
        "tables and indexes both gain the column"
    );
    let update: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": "Unmigrated",
        "WarmThroughput": {"ReadUnitsPerSecond": 20000}
    }))
    .expect("update");
    let mut updated = engine
        .update_table(&account_id, update)
        .await
        .expect("UpdateTable with WarmThroughput after V006");
    updated.prepare_for_response();
    assert_eq!(
        updated.warm_throughput.expect("member").status,
        WarmThroughputStatus::Updating
    );
    engine
        .delete_table(
            &account_id,
            DeleteTableInput {
                table_name: "Unmigrated".to_owned(),
            },
        )
        .await
        .expect("DeleteTable");
}
