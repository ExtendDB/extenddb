// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB test: a GSI added by `UpdateTable` walks the service's two
//! `CREATING` phases (measured 2026-10-26, see `extenddb_storage::gsi_backfill`).
//!
//! Needs a replica set, read from `EXTENDDB_TEST_MONGODB_URI`, and is
//! `#[ignore]`d like the backup live tests; run it with
//! `cargo test -p extenddb-storage-mongodb --lib -- --ignored live_gsi_`. It
//! uses its own catalog and data databases and drops them when it passes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, DescribeTableInput};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine};
use mongodb::bson::{Document, doc};

use crate::MongoEngine;

const ACCOUNT: &str = "123456789012";
const TABLE: &str = "t_gsi_backfilling";
const BACKFILLING: &str = "Cannot read from backfilling global secondary index: gidx";

async fn engine() -> MongoEngine {
    let uri = std::env::var("EXTENDDB_TEST_MONGODB_URI")
        .expect("set EXTENDDB_TEST_MONGODB_URI to a MongoDB replica set");
    let client = mongodb::Client::with_uri_str(uri).await.expect("client");
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let slots = crate::backup_engine::restore_server_slot_count(10);
    MongoEngine {
        client: client.clone(),
        catalog_db: client.database(&format!("g{}_cat", &tag[..12])),
        data_db: client.database(&format!("g{}_data", &tag[..12])),
        region: "us-east-1".to_owned(),
        gsi_cache: Arc::default(),
        tx_read_concern: mongodb::options::ReadConcern::snapshot(),
        backup_liveness: crate::backup_engine::BackupLiveness::default(),
        restore_server_slot_count: slots,
        restore_server_slots: Arc::new(tokio::sync::Semaphore::new(slots)),
        restore_account_slots: crate::backup_engine::RestoreAccountSlots::default(),
    }
}

/// What DescribeTable says about `gidx`: its status and its serialised
/// `Backfilling` member, or `None` before the catalog row is visible.
async fn gsi_state(engine: &MongoEngine) -> Option<(String, Option<serde_json::Value>)> {
    let desc = engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: TABLE.to_owned(),
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
#[ignore = "needs EXTENDDB_TEST_MONGODB_URI"]
async fn live_gsi_added_by_update_table_walks_both_creating_phases() {
    let engine = engine().await;
    let settings = engine.catalog_db.collection::<Document>("settings");
    settings
        .insert_one(doc! { "_id": "control_plane_delay_seconds", "value": "0" })
        .await
        .expect("settings");
    let worker = tokio::spawn(crate::ttl_worker::gsi_backfill_worker(Arc::new(
        engine.clone(),
    )));

    let input: extenddb_core::types::CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": TABLE,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input");
    engine
        .create_table_impl(ACCOUNT, input, false)
        .await
        .expect("create table");
    let key_info = engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("key info");
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
    settings
        .insert_one(doc! {
            "_id": extenddb_core::settings_keys::gsi_creating_phase_delay_key(TABLE),
            "value": "2000",
        })
        .await
        .expect("set the phase hold");

    // UpdateTable returns at once on this backend; the worker builds the index.
    let update: extenddb_core::types::UpdateTableInput =
        serde_json::from_value(serde_json::json!({
            "TableName": TABLE,
            "AttributeDefinitions": [{"AttributeName": "g", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "gidx",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))
        .expect("update input");
    engine
        .update_table(ACCOUNT, update)
        .await
        .expect("UpdateTable succeeds");

    // Phases seen, in order: 1 is Backfilling false, 2 is true, 3 is ACTIVE.
    // A reading counts only when DescribeTable agrees before and after the
    // read, so a phase change between the two calls cannot pair one phase's
    // description with the other's refusal.
    let mut seen: Vec<u8> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while seen.last() != Some(&3) {
        assert!(Instant::now() < deadline, "no ACTIVE state; seen {seen:?}");
        let before = gsi_state(&engine).await;
        let read = engine
            .index_info_by_table_id(&key_info.table_id, "gidx")
            .await;
        if before.is_none() || gsi_state(&engine).await != before {
            tokio::time::sleep(Duration::from_millis(50)).await;
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(seen, vec![1, 2, 3], "each phase is observed, in order");

    worker.abort();
    engine.catalog_db.drop().await.expect("drop catalog");
    engine.data_db.drop().await.expect("drop data");
}
