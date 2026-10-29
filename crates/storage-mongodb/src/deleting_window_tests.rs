// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage-level tests for what the control plane answers while a table is
//! `DELETING`, against a live MongoDB replica set from
//! `EXTENDDB_TEST_MONGODB_URI`, for example
//! `mongodb://127.0.0.1:27105/?directConnection=true`. Each test uses its own
//! catalog and data databases and drops them when it passes.
//!
//! Amazon DynamoDB answers a second `DeleteTable` and an `UpdateTable` in that
//! window with `ResourceInUseException` and the sentence `Attempt to change a
//! resource which is still in use: Table is being deleted: <name>` (measured
//! 2026-10-24). On this backend `DeleteTable` marks the document `DELETING`
//! and drops the collections in the same call, so the window is produced by
//! setting the document's status directly.

use std::sync::Arc;

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DeleteTableInput, KeySchemaElement,
    KeyType, ScalarAttributeType, UpdateTableInput,
};
use extenddb_storage::TableEngine;
use extenddb_storage::error::StorageError;
use mongodb::bson::{Document, doc};

use crate::MongoEngine;

const ACCOUNT: &str = "123456789012";
const TABLE: &str = "t_deleting_window";

fn uri() -> String {
    std::env::var("EXTENDDB_TEST_MONGODB_URI").expect(
        "set EXTENDDB_TEST_MONGODB_URI to a MongoDB replica set, e.g. \
         mongodb://127.0.0.1:27105/?directConnection=true",
    )
}

struct Fixture {
    engine: MongoEngine,
}

impl Fixture {
    async fn new() -> Self {
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let client = mongodb::Client::with_uri_str(uri()).await.expect("client");
        let slots = crate::backup_engine::restore_server_slot_count(10);
        let engine = MongoEngine {
            client: client.clone(),
            catalog_db: client.database(&format!("t{}_cat", &tag[..12])),
            data_db: client.database(&format!("t{}_data", &tag[..12])),
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
        Self { engine }
    }

    /// Create `TABLE` and set its document to `DELETING`: the window.
    async fn open_window(&self) {
        self.engine
            .create_table(
                ACCOUNT,
                CreateTableInput {
                    table_name: TABLE.to_owned(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "pk".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    attribute_definitions: vec![AttributeDefinition {
                        attribute_name: "pk".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    }],
                    billing_mode: Some(BillingMode::PayPerRequest),
                    ..Default::default()
                },
            )
            .await
            .expect("create the table");
        self.set_status("DELETING").await;
    }

    async fn set_status(&self, status: &str) {
        self.engine
            .catalog_db
            .collection::<Document>("tables")
            .update_one(
                doc! { "_id": { "account_id": ACCOUNT, "table_name": TABLE } },
                doc! { "$set": { "table_status": status } },
            )
            .await
            .expect("set the table status");
    }

    async fn status(&self) -> Option<String> {
        self.engine
            .catalog_db
            .collection::<Document>("tables")
            .find_one(doc! { "_id": { "account_id": ACCOUNT, "table_name": TABLE } })
            .await
            .expect("read the table document")
            .map(|d| d.get_str("table_status").expect("table_status").to_owned())
    }

    async fn finish(self) {
        self.engine.catalog_db.drop().await.expect("drop catalog");
        self.engine.data_db.drop().await.expect("drop data");
    }
}

fn update_input() -> UpdateTableInput {
    UpdateTableInput {
        table_name: TABLE.to_owned(),
        billing_mode: None,
        provisioned_throughput: None,
        deletion_protection_enabled: Some(true),
        global_secondary_index_updates: None,
        attribute_definitions: None,
        stream_specification: None,
        table_class: None,
        on_demand_throughput: None,
        table_throughput_mode: None,
        vector_index_updates: None,
    }
}

/// A second DeleteTable during the window reports the table as being deleted,
/// not as absent, and does not run the drop again: the document stays.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn a_second_delete_table_during_the_window_is_table_being_deleted() {
    let f = Fixture::new().await;
    f.open_window().await;

    let err = f
        .engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect_err("DeleteTable on a DELETING table");
    assert!(
        matches!(&err, StorageError::TableBeingDeleted(name) if name == TABLE),
        "expected TableBeingDeleted({TABLE:?}), got {err:?}"
    );
    assert_eq!(f.status().await.as_deref(), Some("DELETING"));

    f.finish().await;
}

/// UpdateTable during the window reports the table as being deleted, not with
/// the sentence for the other non-ACTIVE statuses, and changes nothing.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn update_table_during_the_window_is_table_being_deleted() {
    let f = Fixture::new().await;
    f.open_window().await;

    let err = f
        .engine
        .update_table(ACCOUNT, update_input())
        .await
        .expect_err("UpdateTable on a DELETING table");
    assert!(
        matches!(&err, StorageError::TableBeingDeleted(name) if name == TABLE),
        "expected TableBeingDeleted({TABLE:?}), got {err:?}"
    );
    let doc = f
        .engine
        .catalog_db
        .collection::<Document>("tables")
        .find_one(doc! { "_id": { "account_id": ACCOUNT, "table_name": TABLE } })
        .await
        .expect("read the table document")
        .expect("the document is still there");
    assert_ne!(
        doc.get_bool("deletion_protection_enabled").ok(),
        Some(true),
        "a refused UpdateTable must not have written"
    );

    f.finish().await;
}

/// A CREATING table keeps the existing answer: the new sentence is for
/// DELETING only.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn update_table_on_a_creating_table_keeps_table_not_active() {
    let f = Fixture::new().await;
    f.open_window().await;
    f.set_status("CREATING").await;

    let err = f
        .engine
        .update_table(ACCOUNT, update_input())
        .await
        .expect_err("UpdateTable on a CREATING table");
    assert!(
        matches!(&err, StorageError::TableNotActive(name) if name == TABLE),
        "expected TableNotActive({TABLE:?}), got {err:?}"
    );

    f.finish().await;
}

/// Two concurrent DeleteTable calls on one ACTIVE table: the conditioned mark
/// matches for one of them, the other answers TableBeingDeleted (the service's
/// answer for the second of two concurrent calls, measured 2026-10-25). Ten
/// rounds, a fresh table each; afterwards no catalog document and no data
/// collection for the table remain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn two_concurrent_delete_tables_have_one_winner() {
    let f = Fixture::new().await;
    let engine = Arc::new(f.engine.clone());

    for round in 0..10 {
        let table = format!("t_pair_{round}");
        let desc = engine
            .create_table(
                ACCOUNT,
                CreateTableInput {
                    table_name: table.clone(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "pk".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    attribute_definitions: vec![AttributeDefinition {
                        attribute_name: "pk".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    }],
                    billing_mode: Some(BillingMode::PayPerRequest),
                    ..Default::default()
                },
            )
            .await
            .expect("create the table");

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

        let doc = engine
            .catalog_db
            .collection::<Document>("tables")
            .find_one(doc! { "_id": { "account_id": ACCOUNT, "table_name": &table } })
            .await
            .expect("read the table document");
        assert!(doc.is_none(), "round {round}: the catalog document is gone");
        let collections = engine
            .data_db
            .list_collection_names()
            .await
            .expect("list collections");
        let data_coll = crate::data::data_collection_name(&desc.table_id);
        assert!(
            !collections.contains(&data_coll),
            "round {round}: the data collection is gone"
        );
    }

    drop(engine);
    f.finish().await;
}
