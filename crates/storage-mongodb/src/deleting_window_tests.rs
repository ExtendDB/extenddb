// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage-level tests for what the control plane answers while a table is
//! `DELETING`, against a live MongoDB replica set from
//! `EXTENDDB_TEST_MONGODB_URI`, for example
//! `mongodb://127.0.0.1:27105/?directConnection=true`. Without the variable
//! every test here reports a skip and passes, unless
//! `EXTENDDB_TEST_MONGODB_REQUIRED` is set, as in CI. Each test uses its own
//! catalog and data databases and drops them when it passes.
//!
//! Amazon DynamoDB answers a second `DeleteTable` and an `UpdateTable` in that
//! window with `ResourceInUseException` and the sentence `Attempt to change a
//! resource which is still in use: Table is being deleted: <name>` (measured
//! 2026-10-24). On this backend `DeleteTable` marks the document `DELETING`
//! and drops the collections in the same call, so the window is produced by
//! setting the document's status directly. Once the service accepts a
//! `DeleteTable` the table is removed, so a delete that stops after the mark
//! is finished by the call's own task or by the control plane, never left
//! `DELETING`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DeleteTableInput, KeySchemaElement,
    KeyType, ScalarAttributeType, TableDescription, Tag, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{MetadataEngine, TableEngine, WorkerStore};
use mongodb::bson::{Bson, Document, doc};

use crate::MongoEngine;

const ACCOUNT: &str = "123456789012";
const TABLE: &str = "t_deleting_window";

/// The replica set URI, or `None` to skip. With
/// `EXTENDDB_TEST_MONGODB_REQUIRED` set a missing URI is a failure, so CI
/// cannot pass by skipping.
fn uri() -> Option<String> {
    let uri = std::env::var("EXTENDDB_TEST_MONGODB_URI")
        .ok()
        .filter(|u| !u.trim().is_empty());
    assert!(
        uri.is_some() || std::env::var_os("EXTENDDB_TEST_MONGODB_REQUIRED").is_none(),
        "EXTENDDB_TEST_MONGODB_REQUIRED is set but EXTENDDB_TEST_MONGODB_URI is not; the \
         MongoDB DELETING window tests must run, not skip"
    );
    uri
}

struct Fixture {
    engine: MongoEngine,
}

impl Fixture {
    /// `None`, after printing why, when no replica set is named.
    async fn new() -> Option<Self> {
        let Some(uri) = uri() else {
            eprintln!(
                "SKIP: EXTENDDB_TEST_MONGODB_URI is not set, so there is no MongoDB replica set \
                 to test against."
            );
            return None;
        };
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let client = mongodb::Client::with_uri_str(uri).await.expect("client");
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
        Some(Self { engine })
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

    /// Create `TABLE` with a GSI and a stream, as the table that stalled a
    /// delete was, and a tag on it. Returns the description and the GSI's
    /// index ids.
    async fn create_full_table(&self) -> (TableDescription, Vec<String>) {
        let input: CreateTableInput = serde_json::from_value(serde_json::json!({
            "TableName": TABLE,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gk", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("create input");
        let desc = self
            .engine
            .create_table(ACCOUNT, input)
            .await
            .expect("create the table");
        self.engine
            .tag_resource(
                &desc.table_arn,
                &[Tag {
                    key: "k".to_owned(),
                    value: "v".to_owned(),
                }],
            )
            .await
            .expect("tag the table");
        let index_ids = self
            .engine
            .catalog_db
            .collection::<Document>("indexes")
            .distinct("index_id", doc! { "_id.table_id": &desc.table_id })
            .await
            .expect("read the index ids")
            .into_iter()
            .filter_map(|id| id.as_str().map(str::to_owned))
            .collect::<Vec<_>>();
        assert_eq!(index_ids.len(), 1, "one GSI");
        assert!(
            self.leftovers(&desc, &index_ids).await.len() == 6,
            "precondition: the document, both collections, the index document, the tag and \
             the stream shards exist"
        );
        (desc, index_ids)
    }

    /// What of the table `desc` describes is still in the databases: its
    /// document, its data and index collections, its index documents, its
    /// tags and its stream shards.
    async fn leftovers(&self, desc: &TableDescription, index_ids: &[String]) -> Vec<String> {
        let mut left = Vec::new();
        if self.status().await.is_some() {
            left.push("the table document".to_owned());
        }
        let collections = self
            .engine
            .data_db
            .list_collection_names()
            .await
            .expect("list the data collections");
        for id in std::iter::once(&desc.table_id).chain(index_ids) {
            let name = crate::data::data_collection_name(id);
            if collections.contains(&name) {
                left.push(name);
            }
        }
        let counts = [
            (
                "index documents",
                &self.engine.catalog_db,
                "indexes",
                doc! { "_id.table_id": &desc.table_id },
            ),
            (
                "tags",
                &self.engine.catalog_db,
                "tags",
                doc! { "resource_arn": &desc.table_arn },
            ),
            (
                "stream shards",
                &self.engine.data_db,
                "stream_shards",
                doc! { "table_id": &desc.table_id },
            ),
        ];
        for (what, db, coll, filter) in counts {
            let n = db
                .collection::<Document>(coll)
                .count_documents(filter)
                .await
                .expect("count documents");
            if n > 0 {
                left.push(format!("{n} {what}"));
            }
        }
        left
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

    /// Mark the document `DELETING` with `due` as its transition time, as a
    /// `DeleteTable` that stopped after its mark leaves it.
    async fn leave_deleting(&self, due: Bson) {
        self.engine
            .catalog_db
            .collection::<Document>("tables")
            .update_one(
                doc! { "_id": { "account_id": ACCOUNT, "table_name": TABLE } },
                doc! { "$set": { "table_status": "DELETING", "status_transition_at": due } },
            )
            .await
            .expect("mark the table DELETING");
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

fn delete_input() -> DeleteTableInput {
    DeleteTableInput {
        table_name: TABLE.to_owned(),
    }
}

/// A `status_transition_at` `seconds` from now (negative for the past).
fn transition_in(seconds: i64) -> Bson {
    Bson::DateTime(mongodb::bson::DateTime::from_millis(
        mongodb::bson::DateTime::now().timestamp_millis() + seconds * 1000,
    ))
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
async fn a_second_delete_table_during_the_window_is_table_being_deleted() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.open_window().await;

    let err = f
        .engine
        .delete_table(ACCOUNT, delete_input())
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
async fn update_table_during_the_window_is_table_being_deleted() {
    let Some(f) = Fixture::new().await else {
        return;
    };
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
async fn update_table_on_a_creating_table_keeps_table_not_active() {
    let Some(f) = Fixture::new().await else {
        return;
    };
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

/// A DeleteTable whose caller gives up while a drop waits (a client read
/// timeout) still finishes the delete once the drop can run, with no further
/// call: the steps after the mark run in a task the request only awaits. A
/// transaction holds the data collection, so the drop waits for it, and the
/// call is dropped after 2 s, as an HTTP client's timeout drops the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_table_whose_caller_gives_up_still_finishes() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (desc, index_ids) = f.create_full_table().await;

    let mut session = f.engine.client.start_session().await.expect("session");
    session
        .start_transaction()
        .await
        .expect("start a transaction");
    f.engine
        .data_db
        .collection::<Document>(&crate::data::data_collection_name(&desc.table_id))
        .insert_one(doc! { "_id": "held", "pk": "held" })
        .session(&mut session)
        .await
        .expect("write in the transaction");

    let gave_up = tokio::time::timeout(
        Duration::from_secs(2),
        f.engine.delete_table(ACCOUNT, delete_input()),
    )
    .await;
    assert!(
        gave_up.is_err(),
        "the drop waits for the transaction, so the call is still running at 2 s: {gave_up:?}"
    );
    assert_eq!(f.status().await.as_deref(), Some("DELETING"));

    session
        .abort_transaction()
        .await
        .expect("end the transaction");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let left = f.leftovers(&desc, &index_ids).await;
        if left.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "30 s after the transaction ended, with no further call, these remain: {left:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    f.finish().await;
}

/// The control plane finishes a delete whose DeleteTable stopped after the
/// mark (a step that failed, a process that died): the document, the
/// collections, the index documents, the tags and the stream shards all go,
/// and the name can be created again. A transition time in the past is one a
/// stopped DeleteTable leaves once `DELETE_FINISH_AFTER` has passed; none at
/// all is what a build before this one left.
#[tokio::test]
async fn the_control_plane_finishes_a_delete_left_deleting() {
    for due in [transition_in(-1), Bson::Null] {
        let Some(f) = Fixture::new().await else {
            return;
        };
        let (desc, index_ids) = f.create_full_table().await;
        f.leave_deleting(due.clone()).await;

        let transitions = f
            .engine
            .process_control_plane_transitions()
            .await
            .expect("control plane pass");
        assert_eq!(
            transitions,
            [(TABLE.to_owned(), "DELETING → deleted")],
            "due {due:?}"
        );
        let left = f.leftovers(&desc, &index_ids).await;
        assert!(left.is_empty(), "due {due:?}: these remain: {left:?}");
        f.create_full_table().await;

        f.finish().await;
    }
}

/// A delete whose transition time is still ahead belongs to a DeleteTable
/// that may still be running, and the control plane leaves it alone.
#[tokio::test]
async fn the_control_plane_leaves_a_delete_that_is_not_due() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (desc, index_ids) = f.create_full_table().await;
    f.leave_deleting(transition_in(60)).await;

    let transitions = f
        .engine
        .process_control_plane_transitions()
        .await
        .expect("control plane pass");
    assert!(transitions.is_empty(), "{transitions:?}");
    assert_eq!(f.status().await.as_deref(), Some("DELETING"));
    assert_eq!(f.leftovers(&desc, &index_ids).await.len(), 6);

    f.finish().await;
}

/// Two concurrent DeleteTable calls on one ACTIVE table: the conditioned mark
/// matches for one of them, the other answers TableBeingDeleted (the service's
/// answer for the second of two concurrent calls, measured 2026-10-25). Ten
/// rounds, a fresh table each; afterwards no catalog document and no data
/// collection for the table remain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_delete_tables_have_one_winner() {
    let Some(f) = Fixture::new().await else {
        return;
    };
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
