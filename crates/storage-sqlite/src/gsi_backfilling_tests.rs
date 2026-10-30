// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A GSI added by `UpdateTable` walks the service's two `CREATING` phases.
//!
//! Measured 2026-10-26 (see `extenddb_storage::gsi_backfill`): `Backfilling`
//! is `false` and a read is told the table has no such index until the
//! backfill starts, then `true` with the backfilling sentence until the index
//! is `ACTIVE`, when the member is gone and reads are served. The table's
//! `gsi_creating_phase_delay_ms` hold lengthens both phases so the walk is
//! observable on a three-item table.

use std::time::{Duration, Instant};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, DescribeTableInput};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine};
use serde_json::json;

use crate::SqliteEngine;

const ACCOUNT: &str = "000000000000";
const BACKFILLING: &str = "Cannot read from backfilling global secondary index: gidx";

/// What DescribeTable says about `gidx`: its status and its serialised
/// `Backfilling` member, or `None` before the catalog row is visible.
async fn gsi_state(engine: &SqliteEngine) -> Option<(String, Option<serde_json::Value>)> {
    let desc = engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "t".to_owned(),
            },
        )
        .await
        .expect("describe the table");
    let gsi = desc
        .global_secondary_indexes?
        .into_iter()
        .find(|g| g.index_name == "gidx")?;
    let wire = serde_json::to_value(&gsi).expect("serialise the GSI description");
    Some((gsi.index_status, wire.get("Backfilling").cloned()))
}

#[tokio::test]
async fn a_gsi_added_by_update_table_walks_both_creating_phases() {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
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
    let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
        "TableName": "t",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input");
    engine
        .create_table_impl(ACCOUNT, input, false)
        .await
        .expect("create table");
    sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_name = 't'")
        .execute(&engine.pool)
        .await
        .expect("activate the table");
    let key_info = engine.table_key_info(ACCOUNT, "t").await.expect("key info");
    for n in 0..3 {
        let item = std::collections::BTreeMap::from([
            ("pk".to_owned(), AttributeValue::S(format!("p{n}"))),
            ("g".to_owned(), AttributeValue::S("v".to_owned())),
        ]);
        engine
            .put_item(
                &key_info,
                item,
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put");
    }
    sqlx::query("INSERT INTO settings (key, value) VALUES (?, '1000')")
        .bind(extenddb_core::settings_keys::gsi_creating_phase_delay_key(
            "t",
        ))
        .execute(&engine.pool)
        .await
        .expect("set the phase hold");

    // UpdateTable returns once the index is ACTIVE on this backend, so it runs
    // in its own task while this one watches DescribeTable.
    let update: extenddb_core::types::UpdateTableInput = serde_json::from_value(json!({
        "TableName": "t",
        "AttributeDefinitions": [{"AttributeName": "g", "AttributeType": "S"}],
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "gidx",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }}]
    }))
    .expect("update input");
    let updater = engine.clone();
    let task = tokio::spawn(async move { updater.update_table_impl(ACCOUNT, update).await });

    // Phases seen, in order: 1 is Backfilling false, 2 is true, 3 is ACTIVE.
    // A reading counts only when DescribeTable agrees before and after the
    // read, so a phase change between the two calls cannot pair one phase's
    // description with the other's refusal.
    let mut seen: Vec<u8> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while seen.last() != Some(&3) {
        assert!(Instant::now() < deadline, "no ACTIVE state; seen {seen:?}");
        let before = gsi_state(&engine).await;
        let read = engine
            .index_info_by_table_id(&key_info.table_id, "gidx")
            .await;
        if before.is_none() || gsi_state(&engine).await != before {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let phase = match before.expect("checked above") {
            (status, Some(serde_json::Value::Bool(false))) if status == "CREATING" => {
                assert!(
                    matches!(read, Err(StorageError::IndexNotFound(ref n)) if n == "gidx"),
                    "before the backfill a read is told there is no such index: {read:?}"
                );
                1
            }
            (status, Some(serde_json::Value::Bool(true))) if status == "CREATING" => {
                assert!(
                    matches!(read, Err(StorageError::Validation(ref m)) if m == BACKFILLING),
                    "during the backfill a read gets the backfilling sentence: {read:?}"
                );
                2
            }
            (status, None) if status == "ACTIVE" => {
                read.expect("an ACTIVE index resolves");
                3
            }
            other => panic!("a state the service does not report: {other:?}"),
        };
        if seen.last() != Some(&phase) {
            assert!(
                seen.last().is_none_or(|&last| last < phase),
                "phase {phase} after {seen:?}"
            );
            seen.push(phase);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(seen, vec![1, 2, 3], "each phase is observed, in order");
    task.await
        .expect("the UpdateTable task")
        .expect("UpdateTable succeeds");
}
