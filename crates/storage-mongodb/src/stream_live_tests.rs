// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for the stream lifecycle: a disable closes the stream,
//! an enable creates a new one, and of two overlapping stream changes exactly
//! one is applied.
//!
//! They need a replica set and read its URI from `EXTENDDB_TEST_MONGODB_URI`
//! (for example `mongodb://127.0.0.1:27482/?directConnection=true`). They are
//! `#[ignore]`d so `cargo test --lib` passes without one; run them with
//! `cargo test -p extenddb-storage-mongodb --lib -- --ignored live_stream`.
//! Each test uses its own catalog and data databases and drops them.

use std::sync::Arc;

use mongodb::bson::{Document, doc};

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DescribeStreamInput, DescribeTableInput,
    KeySchemaElement, KeyType, ScalarAttributeType, StreamSpecification, StreamStatus,
    StreamViewType, TableDescription,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{StreamEngine, TableEngine};

use crate::MongoEngine;

const ACCOUNT: &str = "123456789012";

fn uri() -> String {
    std::env::var("EXTENDDB_TEST_MONGODB_URI").expect(
        "set EXTENDDB_TEST_MONGODB_URI to a MongoDB replica set, e.g. \
         mongodb://127.0.0.1:27482/?directConnection=true",
    )
}

async fn engine() -> MongoEngine {
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let client = mongodb::Client::with_uri_str(uri()).await.expect("client");
    let slots = crate::backup_engine::restore_server_slot_count(10);
    let engine = MongoEngine {
        client: client.clone(),
        catalog_db: client.database(&format!("s{}_cat", &tag[..12])),
        data_db: client.database(&format!("s{}_data", &tag[..12])),
        region: "us-east-1".to_owned(),
        gsi_cache: Arc::default(),
        tx_read_concern: mongodb::options::ReadConcern::snapshot(),
        backup_liveness: crate::backup_engine::BackupLiveness::default(),
        restore_server_slot_count: slots,
        restore_server_slots: Arc::new(tokio::sync::Semaphore::new(slots)),
        restore_account_slots: crate::backup_engine::RestoreAccountSlots::default(),
    };
    engine
        .catalog_db
        .collection::<Document>("settings")
        .insert_one(doc! { "_id": "control_plane_delay_seconds", "value": "0" })
        .await
        .expect("settings");
    engine
}

async fn finish(engine: MongoEngine) {
    engine.catalog_db.drop().await.expect("drop catalog");
    engine.data_db.drop().await.expect("drop data");
}

async fn stream_table(engine: &MongoEngine, name: &str) -> TableDescription {
    engine
        .create_table_impl(
            ACCOUNT,
            CreateTableInput {
                table_name: name.to_owned(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_owned(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::KeysOnly),
                }),
                ..Default::default()
            },
            false,
        )
        .await
        .expect("create")
}

fn set_stream(name: &str, spec: serde_json::Value) -> extenddb_core::types::UpdateTableInput {
    serde_json::from_value(serde_json::json!({"TableName": name, "StreamSpecification": spec}))
        .expect("input")
}

async fn describe_stream(
    engine: &MongoEngine,
    arn: &str,
) -> extenddb_core::types::StreamDescription {
    engine
        .describe_stream(
            ACCOUNT,
            &DescribeStreamInput {
                stream_arn: arn.to_owned(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .expect("describe stream")
}

/// Of two stream changes fired at the same instant, one is applied and the
/// other refused; the table ends with one stream enabled and four open
/// shards, and every other shard closed.
async fn overlapping_pair(
    engine: &MongoEngine,
    name: &str,
    a: serde_json::Value,
    b: serde_json::Value,
) -> (usize, usize) {
    let (ra, rb) = tokio::join!(
        engine.update_table(ACCOUNT, set_stream(name, a)),
        engine.update_table(ACCOUNT, set_stream(name, b)),
    );
    let ok = [&ra, &rb].iter().filter(|r| r.is_ok()).count();
    let refused = [&ra, &rb]
        .iter()
        .filter(|r| {
            matches!(
                r,
                Err(StorageError::Validation(_)) | Err(StorageError::TableNotActive(_))
            )
        })
        .count();
    for r in [&ra, &rb] {
        if let Err(e) = r {
            assert!(
                matches!(
                    e,
                    StorageError::Validation(_) | StorageError::TableNotActive(_)
                ),
                "{e:?}"
            );
        }
    }
    (ok, refused)
}

async fn audit(engine: &MongoEngine, name: &str) {
    let table = engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: name.to_owned(),
            },
        )
        .await
        .expect("describe table");
    let (listed, _) = engine
        .list_streams(ACCOUNT, Some(name), 100, None)
        .await
        .expect("list");
    let mut enabled = 0;
    for s in &listed {
        let d = describe_stream(engine, &s.stream_arn).await;
        let open = d
            .shards
            .iter()
            .filter(|x| x.sequence_number_range.ending_sequence_number.is_none())
            .count();
        match d.stream_status {
            StreamStatus::Enabled => {
                enabled += 1;
                assert_eq!(d.shards.len(), 4, "{}", s.stream_arn);
                assert_eq!(open, 4, "an enabled stream has every shard open");
                assert_eq!(
                    table.latest_stream_arn.as_deref(),
                    Some(s.stream_arn.as_str())
                );
            }
            _ => assert_eq!(open, 0, "a disabled stream has every shard closed"),
        }
    }
    let spec_enabled = table
        .stream_specification
        .as_ref()
        .is_some_and(|s| s.stream_enabled);
    assert_eq!(enabled, usize::from(spec_enabled));
}

#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_stream_overlapping_changes_apply_exactly_one() {
    let engine = engine().await;
    let name = "overlap";
    stream_table(&engine, name).await;
    let disable = serde_json::json!({"StreamEnabled": false});
    let enable = serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"});
    let mut applied = 0;
    let mut refused = 0;
    for round in 0..10 {
        // Alternate the pairs so both orders and both mixes are exercised.
        let (a, b) = match round % 3 {
            0 => (disable.clone(), disable.clone()),
            1 => (enable.clone(), disable.clone()),
            _ => (disable.clone(), enable.clone()),
        };
        let (ok, no) = overlapping_pair(&engine, name, a, b).await;
        assert_eq!(
            (ok, no),
            (1, 1),
            "round {round}: exactly one change applies"
        );
        applied += ok;
        refused += no;
        audit(&engine, name).await;
        // Bring the table back to enabled for the next round.
        let _ = engine
            .update_table(ACCOUNT, set_stream(name, enable.clone()))
            .await;
        audit(&engine, name).await;
    }
    assert_eq!((applied, refused), (10, 10));
    finish(engine).await;
}

#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_stream_disable_closes_and_enable_replaces() {
    let engine = engine().await;
    let desc = stream_table(&engine, "lifecycle").await;
    let old = desc.latest_stream_arn.expect("arn");
    engine
        .update_table(
            ACCOUNT,
            set_stream("lifecycle", serde_json::json!({"StreamEnabled": false})),
        )
        .await
        .expect("disable");
    let closed = describe_stream(&engine, &old).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    assert_eq!(closed.stream_view_type, StreamViewType::KeysOnly);
    assert!(
        closed
            .shards
            .iter()
            .all(|x| x.sequence_number_range.ending_sequence_number.is_some())
    );
    let re = engine
        .update_table(
            ACCOUNT,
            set_stream(
                "lifecycle",
                serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}),
            ),
        )
        .await
        .expect("enable");
    let new = re.latest_stream_arn.expect("new arn");
    assert_ne!(new, old);
    let d = describe_stream(&engine, &new).await;
    assert!(matches!(d.stream_status, StreamStatus::Enabled));
    assert_eq!(d.stream_view_type, StreamViewType::NewImage);
    audit(&engine, "lifecycle").await;
    finish(engine).await;
}
