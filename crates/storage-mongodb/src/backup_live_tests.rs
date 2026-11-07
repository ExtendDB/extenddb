// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for backup and restore: the catalog index and its
//! claim, the detached copy and its job record, the reconciler's duties over
//! an abandoned restore, and the DeleteBackup/DeleteTable refusals during a
//! copy.
//!
//! They need a replica set (the data path uses transactions) and read its URI
//! from `EXTENDDB_TEST_MONGODB_URI`, for example
//! `mongodb://127.0.0.1:27741/?directConnection=true`. They are `#[ignore]`d so
//! `cargo test --lib` passes without one; run them with
//! `cargo test -p extenddb-storage-mongodb --lib -- --ignored live_`.
//! A run that sets `--ignored` without the variable fails rather than skips.
//!
//! Each test uses its own catalog and data databases (random names), runs the
//! catalog migration step on them, and drops them at the end, so tests run in
//! parallel and leave nothing on success. Each runs its own GSI backfill
//! worker, as `serve` does, and every engine uses a shortened heartbeat
//! interval (200 ms), which the tests read from the engine rather than
//! assume; the stale window is the one the reconciler would pass.

use std::sync::Arc;
use std::time::{Duration, Instant};

use extenddb_core::backup_format::BackupArn;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DeleteTableInput,
    DescribeTableInput, GsiInput, Item, KeySchemaElement, KeyType, LsiInput, Projection,
    ProjectionType, ScalarAttributeType, TableStatus,
};
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, DeleteStart, Heartbeat, JobStatus, RestoreKind,
    RestoreRegistrationRow, RestoreTargetKey, StaleClock, StatusFields, StoreIdentity, StoreKind,
    TimeBounds, UNOWNED_GENERATION, WriteOutcome,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::restore_engine::{BulkLoadFile, Finished, RestoreEngine};
use extenddb_storage::{
    BackupCatalog, JobPermit, LegacyBackupReader, RestoreRequest, RestoreTableOverrides,
    TableEngine,
};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use mongodb::bson::{self, Document, doc};

use super::{AfterBackupDrop, BackupLiveness, HeartbeatFault};
use crate::MongoEngine;
use crate::backup_catalog::{
    BACKUP_INDEX, RESTORE_JOB, RESTORE_JOB_GENERATION, RESTORE_JOB_HEARTBEAT, RESTORE_JOB_STATUS,
};
use crate::data::{data_collection_name, document_to_item, item_to_document};
use crate::migrations::{MongoTarget, run_backup_catalog_index};
use crate::restore_engine::INSERT_MANY_DOCUMENTS;

const ACCOUNT: &str = "123456789012";
const OTHER_ACCOUNT: &str = "210987654321";
const REGION: &str = "us-east-1";
const ITEMS: usize = 3_000;
/// The stale window the reconciler would pass (`stale_job_after_secs`).
const STALE_AFTER: Duration = Duration::from_secs(300);

fn uri() -> String {
    std::env::var("EXTENDDB_TEST_MONGODB_URI").expect(
        "set EXTENDDB_TEST_MONGODB_URI to a MongoDB replica set, e.g. \
         mongodb://127.0.0.1:27741/?directConnection=true",
    )
}

/// The deployment of the scratch databases `catalog` and `data`.
fn target(catalog: &str, data: &str) -> MongoTarget {
    MongoTarget::with_databases(uri(), catalog, data)
}

fn test_liveness() -> BackupLiveness {
    BackupLiveness {
        heartbeat_interval: Duration::from_millis(200),
        registration_gate: None,
        copy_gate: None,
        after_backup_drop: None,
        target_drop_fails: false,
        local_clock_behind: Duration::ZERO,
        heartbeat_fault: None,
        replacement_commit_unknown: false,
    }
}

struct Fixture {
    engine: MongoEngine,
    worker: tokio::task::JoinHandle<()>,
    catalog: String,
    data: String,
}

impl Fixture {
    async fn new() -> Self {
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let catalog = format!("t{}_cat", &tag[..12]);
        let data = format!("t{}_data", &tag[..12]);
        let engine = engine_on(&catalog, &data, test_liveness()).await;
        engine
            .catalog_db
            .collection::<Document>("settings")
            .insert_one(doc! { "_id": "control_plane_delay_seconds", "value": "0" })
            .await
            .expect("settings");
        // Migration 6's catalog body alone, as `init` and `migrate --yes`
        // run it: migration 1's collections and indexes are not created
        // here, so this catalog is not `init`'s; the tests of this file need
        // only what migration 6 creates.
        run_backup_catalog_index(&target(&catalog, &data))
            .await
            .expect("migration 6's body");
        let worker = spawn_worker(&engine);
        Self {
            engine,
            worker,
            catalog,
            data,
        }
    }

    /// A second engine on the same catalog and data databases with its own
    /// client: another ExtendDB process sharing this replica set.
    async fn second_instance(&self, liveness: BackupLiveness) -> MongoEngine {
        engine_on(&self.catalog, &self.data, liveness).await
    }

    async fn finish(self) {
        self.worker.abort();
        self.engine.catalog_db.drop().await.expect("drop catalog");
        self.engine.data_db.drop().await.expect("drop data");
    }

    /// [`Self::finish`] after a test body run under `catch_unwind`, whether
    /// or not it panicked, and then the panic, if any, goes on.
    async fn finish_after(self, outcome: std::thread::Result<()>) {
        self.finish().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}

async fn engine_on(catalog: &str, data: &str, liveness: BackupLiveness) -> MongoEngine {
    let client = mongodb::Client::with_uri_str(uri()).await.expect("client");
    MongoEngine {
        client: client.clone(),
        catalog_db: client.database(catalog),
        data_db: client.database(data),
        region: REGION.to_owned(),
        gsi_cache: Arc::default(),
        tx_read_concern: mongodb::options::ReadConcern::snapshot(),
        backup_liveness: liveness,
    }
}

fn spawn_worker(engine: &MongoEngine) -> tokio::task::JoinHandle<()> {
    let engine = Arc::new(engine.clone());
    tokio::spawn(crate::ttl_worker::gsi_backfill_worker(
        engine,
        extenddb_storage::CancellationToken::new(),
    ))
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn attr(name: &str, t: ScalarAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: t,
    }
}

/// A source table with a GSI (ALL) and an LSI (KEYS_ONLY), filled with
/// `ITEMS` items that all carry both index keys.
async fn source_table(engine: &MongoEngine, name: &str) -> String {
    source_table_with(engine, name, ITEMS).await
}

async fn source_table_with(engine: &MongoEngine, name: &str, items: usize) -> String {
    source_table_for(engine, ACCOUNT, name, items).await
}

async fn source_table_for(engine: &MongoEngine, account: &str, name: &str, items: usize) -> String {
    let input = CreateTableInput {
        table_name: name.to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: vec![
            attr("pk", ScalarAttributeType::S),
            attr("sk", ScalarAttributeType::N),
            attr("gpk", ScalarAttributeType::S),
            attr("lsk", ScalarAttributeType::N),
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "g1".to_owned(),
            key_schema: vec![key("gpk", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        local_secondary_indexes: Some(vec![LsiInput {
            index_name: "l1".to_owned(),
            key_schema: vec![key("pk", KeyType::Hash), key("lsk", KeyType::Range)],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
        }]),
        ..Default::default()
    };
    let desc = engine
        .create_table_impl(account, input.clone(), false)
        .await
        .expect("create source");
    assert_eq!(desc.table_status, TableStatus::Active);
    let docs: Vec<Document> = (0..items)
        .map(|i| {
            let item: Item = [
                ("pk".to_owned(), AttributeValue::S(format!("p{}", i % 50))),
                ("sk".to_owned(), AttributeValue::N(i.to_string())),
                ("gpk".to_owned(), AttributeValue::S(format!("g{}", i % 7))),
                ("lsk".to_owned(), AttributeValue::N((i * 3).to_string())),
                ("payload".to_owned(), AttributeValue::S("x".repeat(200))),
            ]
            .into_iter()
            .collect();
            item_to_document(&item, &input.key_schema, &input.attribute_definitions)
                .expect("item doc")
        })
        .collect();
    engine
        .data_db
        .collection::<Document>(&data_collection_name(&desc.table_id))
        .insert_many(docs)
        .await
        .expect("seed items");
    desc.table_arn
}

/// The ARN of a backup that does not exist, with a well-formed id: `arn`
/// with its last hex digit changed.
fn flip_last_hex(arn: &str) -> String {
    let (head, last) = arn.split_at(arn.len() - 1);
    format!("{head}{}", if last == "0" { "1" } else { "0" })
}

/// What the engine does before `restore_table_from_backup`.
async fn restore_request(engine: &MongoEngine, backup_arn: &str, target: &str) -> RestoreRequest {
    restore_request_for(engine, ACCOUNT, backup_arn, target).await
}

async fn restore_request_for(
    engine: &MongoEngine,
    account: &str,
    backup_arn: &str,
    target: &str,
) -> RestoreRequest {
    let source = engine
        .restore_source(account, backup_arn)
        .await
        .expect("restore source");
    source.ensure_restorable().expect("restorable");
    let input = source.restore_input(target, &RestoreTableOverrides::default());
    RestoreRequest {
        backup_arn: backup_arn.to_owned(),
        source_table_arn: source.source_table_arn.clone(),
        restore_date_time: source.backup_creation_date_time,
        input,
    }
}

/// The `tables` collection read at `majority`: the state a registration's or
/// a DeleteBackup's transaction will see. The engine's transactions read at
/// its `tx_read_concern` (`snapshot` here), which is majority-committed data;
/// a read at the collection's default (`local`) sees the backfill worker's
/// ACTIVE flip as soon as it is applied on the primary, while the majority
/// commit point still waits for the journal and for any concurrent batch
/// write holding an earlier oplog slot (another test's 3,000-item seed), so
/// a test that observed ACTIVE at `local` and then issued a request could be
/// answered `BackupInUse` by a transaction that still saw CREATING.
fn tables_majority(engine: &MongoEngine) -> mongodb::Collection<Document> {
    engine.catalog_db.collection_with_options(
        "tables",
        mongodb::options::CollectionOptions::builder()
            .read_concern(mongodb::options::ReadConcern::majority())
            .build(),
    )
}

async fn table_doc(engine: &MongoEngine, name: &str) -> Option<Document> {
    table_doc_for(engine, ACCOUNT, name).await
}

async fn wait_for_status(engine: &MongoEngine, name: &str, status: &str) -> Document {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(doc) = table_doc(engine, name).await
            && doc.get_str("table_status") == Ok(status)
        {
            return doc;
        }
        assert!(
            Instant::now() < deadline,
            "{name} did not reach {status}: {:?}",
            table_doc(engine, name).await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn count(engine: &MongoEngine, collection: &str) -> u64 {
    engine
        .data_db
        .collection::<Document>(collection)
        .count_documents(doc! {})
        .await
        .expect("count")
}

async fn index_ids(engine: &MongoEngine, table_id: &str) -> Vec<(String, String, String)> {
    use futures::TryStreamExt;
    let docs: Vec<Document> = engine
        .catalog_db
        .collection::<Document>("indexes")
        .find(doc! { "_id.table_id": table_id })
        .await
        .expect("indexes")
        .try_collect()
        .await
        .expect("indexes");
    let mut out: Vec<_> = docs
        .iter()
        .map(|d| {
            (
                d.get_document("_id")
                    .and_then(|id| id.get_str("index_name"))
                    .expect("name")
                    .to_owned(),
                d.get_str("index_id").expect("id").to_owned(),
                d.get_str("index_status").expect("status").to_owned(),
            )
        })
        .collect();
    out.sort();
    out
}

async fn collection_exists(engine: &MongoEngine, name: &str) -> bool {
    engine
        .data_db
        .list_collection_names()
        .await
        .expect("list")
        .iter()
        .any(|c| c == name)
}

/// ACTIVE with every item in the base collection and in both index
/// collections, and both indexes ACTIVE.
async fn assert_fully_restored(engine: &MongoEngine, name: &str) {
    assert_fully_restored_with(engine, name, ITEMS).await;
}

async fn assert_fully_restored_with(engine: &MongoEngine, name: &str, items: usize) {
    let doc = wait_for_status(engine, name, "ACTIVE").await;
    let table_id = doc.get_str("table_id").expect("table_id");
    assert_eq!(doc.get_i64("item_count"), Ok(items as i64));
    assert_eq!(
        count(engine, &data_collection_name(table_id)).await,
        items as u64
    );
    let indexes = index_ids(engine, table_id).await;
    assert_eq!(
        indexes.iter().map(|i| i.0.as_str()).collect::<Vec<_>>(),
        ["g1", "l1"]
    );
    for (index_name, index_id, status) in &indexes {
        assert_eq!(status, "ACTIVE", "{index_name}");
        assert_eq!(
            count(engine, &data_collection_name(index_id)).await,
            items as u64,
            "index {index_name}"
        );
    }
    // The job record went DONE with the hand-over and stayed with the table.
    let job = doc.get_document(RESTORE_JOB).expect("job record");
    assert_eq!(job.get_str("status"), Ok("DONE"));
    assert_eq!(job.get_str("kind"), Ok("legacy"));
    assert!(job.get_datetime("completed_at").is_ok());
    assert!(!doc.contains_key("restore_backfill_pending"));
}

/// An engine whose restores wait for one permit each before their copy.
fn gated(engine: &MongoEngine) -> (MongoEngine, Arc<tokio::sync::Semaphore>) {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut gated = engine.clone();
    gated.backup_liveness.copy_gate = Some(gate.clone());
    (gated, gate)
}

/// An engine whose restores wait for one permit each before they register
/// their target, and one more before their copy.
fn gated_at_registration(
    engine: &MongoEngine,
) -> (
    MongoEngine,
    Arc<tokio::sync::Semaphore>,
    Arc<tokio::sync::Semaphore>,
) {
    let (mut gated, copy_gate) = gated(engine);
    let registration_gate = Arc::new(tokio::sync::Semaphore::new(0));
    gated.backup_liveness.registration_gate = Some(registration_gate.clone());
    (gated, registration_gate, copy_gate)
}

/// The names among `targets` that exist in the catalog: the catalog's account
/// of which restores registered, whatever their requests answered.
async fn registered_targets(engine: &MongoEngine, targets: &[String]) -> Vec<String> {
    let mut registered = Vec::new();
    for target in targets {
        if table_doc(engine, target).await.is_some() {
            registered.push(target.clone());
        }
    }
    registered
}

/// The restore job record of `name`'s table document.
/// The server's clock (`$$NOW`), read through an aggregation.
async fn server_now(engine: &MongoEngine) -> bson::DateTime {
    let mut cursor = engine
        .catalog_db
        .collection::<Document>("tables")
        .aggregate(vec![
            doc! { "$limit": 1 },
            doc! { "$project": { "_id": 0, "now": "$$NOW" } },
        ])
        .await
        .expect("aggregate");
    let doc = cursor
        .next()
        .await
        .expect("one document")
        .expect("the server's clock");
    *doc.get_datetime("now").expect("now")
}

async fn job_record(engine: &MongoEngine, name: &str) -> Document {
    table_doc(engine, name)
        .await
        .expect("target")
        .get_document(RESTORE_JOB)
        .expect("job record")
        .clone()
}

fn target_key(doc: &Document) -> RestoreTargetKey {
    let id = doc.get_document("_id").expect("_id");
    RestoreTargetKey {
        account_id: id.get_str("account_id").expect("account").to_owned(),
        table_name: id.get_str("table_name").expect("name").to_owned(),
        table_id: doc.get_str("table_id").expect("table_id").to_owned(),
    }
}

/// Leave the state a process killed mid-copy leaves: the target CREATING, its
/// job record RUNNING with a heartbeat `age` old, and no task.
async fn age_running_record(engine: &MongoEngine, name: &str, age: Duration) {
    let heartbeat = bson::DateTime::from_millis(
        bson::DateTime::now().timestamp_millis() - i64::try_from(age.as_millis()).expect("i64"),
    );
    engine
        .catalog_db
        .collection::<Document>("tables")
        .update_one(
            doc! { "_id": { "account_id": ACCOUNT, "table_name": name } },
            doc! { "$set": {
                "table_status": "CREATING",
                "status_transition_at": bson::Bson::Null,
                RESTORE_JOB_STATUS: "RUNNING",
                RESTORE_JOB_HEARTBEAT: heartbeat,
            }, "$unset": { "restore_job.completed_at": "", "restore_backfill_pending": "" } },
        )
        .await
        .expect("age the record");
}

async fn backup_meta(engine: &MongoEngine, backup_arn: &str) -> Document {
    engine
        .catalog_db
        .collection::<Document>("backups")
        .find_one(doc! { "_id": backup_arn })
        .await
        .expect("read backup")
        .expect("backup document")
}

fn with_drop_fault(engine: &MongoEngine, fault: AfterBackupDrop) -> MongoEngine {
    let mut faulty = engine.clone();
    faulty.backup_liveness.after_backup_drop = Some(fault);
    faulty
}

fn store() -> StoreIdentity {
    StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: "/var/lib/extenddb/backups".to_owned(),
        store_id: "store-1".to_owned(),
    }
}

/// A `backup_index` row for a backup of `table` in `account` minted now,
/// with the manifest fixture's source table.
fn row(account: &str, table: &str, name: &str) -> BackupRow {
    let backup_id = extenddb_storage::util::new_backup_id();
    let backup_arn = extenddb_storage::util::backup_arn(REGION, account, table, &backup_id);
    let manifest: serde_json::Value = serde_json::from_str(include_str!(
        "../../core/testdata/extenddb-manifest/v1.json"
    ))
    .expect("fixture");
    BackupRow {
        backup_id: backup_id.clone(),
        account_id: account.to_owned(),
        table_name: table.to_owned(),
        backup_arn,
        backup_name: name.to_owned(),
        status: BackupStatus::Creating,
        store: store(),
        size_bytes: 0,
        item_count: 0,
        created_at: extenddb_storage::util::backup_creation_date_time(&backup_id).unwrap(),
        completed_at: None,
        failure_reason: None,
        source_table_details_json: manifest["source_table"].clone(),
        owner_generation: 0,
        heartbeat_at: 0.0,
    }
}

fn arn_of(row: &BackupRow) -> BackupArn {
    BackupArn::parse(&row.backup_arn).unwrap()
}

/// The claim: a second claim of the same backup is refused while the row
/// exists, the heartbeat confirms the claim and refreshes the row with the
/// server's clock, a status write is conditioned on the generation and the
/// `from` status, and the row is read back by account, backup id, table and
/// ARN. The catalog has an id and no maintenance claim until one is written.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_claim_backup_excludes_a_second_owner_and_fences_its_writes() {
    let fx = Fixture::new().await;
    assert!(
        uuid::Uuid::parse_str(&fx.engine.catalog_id().await.expect("id")).is_ok(),
        "the catalog has an id from the migration step"
    );
    assert!(
        !fx.engine
            .check_maintenance_claim(std::time::Duration::from_secs(300))
            .await
            .expect("check")
    );
    let row = row(ACCOUNT, "Music", "nightly");
    let claim = fx.engine.claim_backup(&row).await.expect("claim");
    assert_ne!(claim.generation(), UNOWNED_GENERATION);
    let second = fx.engine.claim_backup(&row).await.err();
    assert!(
        matches!(second, Some(StorageError::AlreadyExists(_))),
        "{second:?}"
    );
    let stored = fx
        .engine
        .read_backup_row(&arn_of(&row))
        .await
        .expect("read")
        .expect("the row exists");
    assert_eq!(stored.status, BackupStatus::Creating);
    assert_eq!(stored.owner_generation, claim.generation());
    assert_eq!(stored.store, store());
    assert_eq!(stored.created_at, row.created_at);
    assert_eq!(
        stored.source_table_details_json,
        row.source_table_details_json
    );
    assert!(
        stored.heartbeat_at > 0.0,
        "the heartbeat is the server's now"
    );
    let before = stored.heartbeat_at;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        claim.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    let after = fx
        .engine
        .read_backup_row(&arn_of(&row))
        .await
        .unwrap()
        .unwrap()
        .heartbeat_at;
    assert!(after > before, "{after} > {before}");

    // The same id under another account, table or region finds nothing.
    let other_account =
        BackupArn::new("aws", REGION, OTHER_ACCOUNT, "Music", &row.backup_id).unwrap();
    assert!(
        fx.engine
            .read_backup_row(&other_account)
            .await
            .unwrap()
            .is_none()
    );
    let other_table = BackupArn::new("aws", REGION, ACCOUNT, "Other", &row.backup_id).unwrap();
    assert!(
        fx.engine
            .read_backup_row(&other_table)
            .await
            .unwrap()
            .is_none()
    );
    let other_region =
        BackupArn::new("aws", "eu-west-1", ACCOUNT, "Music", &row.backup_id).unwrap();
    assert!(
        fx.engine
            .read_backup_row(&other_region)
            .await
            .unwrap()
            .is_none()
    );

    // A write from the wrong state matches nothing; the right one matches
    // once, and the row then carries what it set.
    let fields = StatusFields {
        size_bytes: Some(1234),
        item_count: Some(5),
        failure_reason: None,
        source_table_details_json: None,
        completed: true,
    };
    assert_eq!(
        claim
            .set_status(JobStatus::Running, JobStatus::Available, &fields)
            .await
            .unwrap(),
        WriteOutcome::NotMatched
    );
    assert_eq!(
        claim
            .set_status(JobStatus::Creating, JobStatus::Available, &fields)
            .await
            .unwrap(),
        WriteOutcome::Matched
    );
    assert_eq!(
        claim
            .set_status(JobStatus::Creating, JobStatus::Available, &fields)
            .await
            .unwrap(),
        WriteOutcome::NotMatched
    );
    let read = claim.read().await.expect("read");
    assert_eq!(read.status, JobStatus::Available);
    assert_eq!(read.owner_generation, claim.generation());
    let stored = fx
        .engine
        .read_backup_row(&arn_of(&row))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, BackupStatus::Available);
    assert_eq!(stored.size_bytes, 1234);
    assert_eq!(stored.item_count, 5);
    assert!(stored.completed_at.is_some());

    // A row another generation took over confirms nothing for this claim.
    fx.engine
        .catalog_db
        .collection::<Document>(BACKUP_INDEX)
        .update_one(
            doc! { "backup_id": &row.backup_id },
            doc! { "$inc": { "owner_generation": 1_i64 } },
        )
        .await
        .expect("take over");
    assert_eq!(claim.heartbeat().await.expect("heartbeat"), Heartbeat::Lost);
    assert_eq!(
        claim
            .set_status(
                JobStatus::Available,
                JobStatus::Failed,
                &StatusFields::default()
            )
            .await
            .unwrap(),
        WriteOutcome::NotMatched
    );
    drop(claim);
    // The row is the claim: a successor can take it only through its
    // insert, which the existing row refuses.
    let third = fx.engine.claim_backup(&row).await.err();
    assert!(
        matches!(third, Some(StorageError::AlreadyExists(_))),
        "{third:?}"
    );

    fx.finish().await;
}

/// `read_store_ids`: the distinct ids of the rows for a root, of every
/// status, scoped to the kind and the root.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_read_store_ids_lists_the_distinct_ids_of_a_root() {
    let fx = Fixture::new().await;
    let root = store().store_root.clone();
    let ids = |kind: StoreKind, root: String| {
        let engine = &fx.engine;
        async move { engine.read_store_ids(kind, &root).await.expect("ids") }
    };
    assert!(ids(StoreKind::Filesystem, root.clone()).await.is_empty());
    let first = row(ACCOUNT, "Music", "a");
    let _c1 = fx.engine.claim_backup(&first).await.expect("claim");
    let second = row(ACCOUNT, "Music", "b");
    let c2 = fx.engine.claim_backup(&second).await.expect("claim");
    c2.set_status(
        JobStatus::Creating,
        JobStatus::Failed,
        &StatusFields::default(),
    )
    .await
    .expect("fail");
    assert_eq!(
        ids(StoreKind::Filesystem, root.clone()).await,
        ["store-1"],
        "two rows of one store, of two statuses, are one id"
    );
    let mut other = row(ACCOUNT, "Songs", "c");
    other.store.store_id = "store-2".to_owned();
    let _c3 = fx.engine.claim_backup(&other).await.expect("claim");
    assert_eq!(
        ids(StoreKind::Filesystem, root.clone()).await,
        ["store-1", "store-2"]
    );
    assert!(
        ids(StoreKind::S3, root.clone()).await.is_empty(),
        "another kind at the same root is another store"
    );
    assert!(
        ids(StoreKind::Filesystem, "/elsewhere".to_owned())
            .await
            .is_empty()
    );
    fx.finish().await;
}

/// A row of `row(ACCOUNT, table, name)` with `status`, under the store of
/// `kind`, `root` and `id`.
fn row_at(
    table: &str,
    name: &str,
    status: BackupStatus,
    (kind, root, id): (StoreKind, &str, &str),
) -> BackupRow {
    let mut row = row(ACCOUNT, table, name);
    row.status = status;
    row.store = StoreIdentity {
        store_kind: kind,
        store_root: root.to_owned(),
        store_id: id.to_owned(),
    };
    row.size_bytes = 10;
    row.item_count = 2;
    row
}

/// The ARNs of `rows` in the listings' (`created_at`, `backup_arn`) order.
fn in_listing_order(rows: &[&BackupRow]) -> Vec<String> {
    let mut rows = rows.to_vec();
    rows.sort_by(|a, b| {
        a.created_at
            .total_cmp(&b.created_at)
            .then_with(|| a.backup_arn.cmp(&b.backup_arn))
    });
    rows.into_iter().map(|r| r.backup_arn.clone()).collect()
}

/// `list_backup_rows_by_root`: every row of the kind and root, whatever its
/// id, of every status or of one, whole and in the listings' order; nothing
/// of a root that differs by one byte (a trailing `/`, one character fewer),
/// of an S3 root that differs by its `?endpoint=` suffix, or of the other
/// kind at the same label.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_list_backup_rows_by_root_returns_every_id_and_status_of_the_root_and_nothing_else() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let root = "/srv/backups-old";
        let at = |id| (StoreKind::Filesystem, root, id);
        let mut of_root = Vec::new();
        for (i, status) in [
            BackupStatus::Available,
            BackupStatus::Deleting,
            BackupStatus::Deleted,
            BackupStatus::Failed,
        ]
        .into_iter()
        .enumerate()
        {
            let id = if i % 2 == 0 { "store-1" } else { "store-2" };
            of_root.push(row_at("Music", &format!("r{i}"), status, at(id)));
        }
        // Inserted newest first: the order asserted is the listing's.
        for row in of_root.iter().rev() {
            assert!(fx.engine.insert_backup_row(row).await.expect("insert"));
        }
        let creating = row_at("Music", "job", BackupStatus::Creating, at("store-1"));
        let claim = fx.engine.claim_backup(&creating).await.expect("claim");
        let slash = format!("{root}/");
        let s3 = "s3://bucket/prefix";
        let s3_endpoint = "s3://bucket/prefix?endpoint=http://127.0.0.1:9200";
        let others = [
            row_at(
                "Music",
                "slash",
                BackupStatus::Available,
                (StoreKind::Filesystem, &slash, "store-1"),
            ),
            row_at(
                "Music",
                "shorter",
                BackupStatus::Available,
                (StoreKind::Filesystem, "/srv/backups-ol", "store-1"),
            ),
            row_at(
                "Music",
                "kind",
                BackupStatus::Available,
                (StoreKind::S3, root, "store-1"),
            ),
            row_at(
                "Music",
                "s3",
                BackupStatus::Available,
                (StoreKind::S3, s3, "store-3"),
            ),
            row_at(
                "Music",
                "endpoint",
                BackupStatus::Available,
                (StoreKind::S3, s3_endpoint, "store-3"),
            ),
        ];
        for row in &others {
            assert!(fx.engine.insert_backup_row(row).await.expect("insert"));
        }
        let listed = |kind: StoreKind, root: String, status: Option<BackupStatus>| {
            let engine = &fx.engine;
            async move {
                let rows: Vec<BackupRow> = engine
                    .list_backup_rows_by_root(kind, &root, status)
                    .try_collect()
                    .await
                    .expect("listing");
                rows
            }
        };
        let arns = |rows: Vec<BackupRow>| -> Vec<String> {
            rows.into_iter().map(|r| r.backup_arn).collect()
        };
        let mut all: Vec<&BackupRow> = of_root.iter().collect();
        all.push(&creating);
        let whole = listed(StoreKind::Filesystem, root.to_owned(), None).await;
        assert_eq!(arns(whole.clone()), in_listing_order(&all));
        for row in &whole {
            assert_eq!(
                Some(row),
                fx.engine
                    .read_backup_row(&arn_of(row))
                    .await
                    .expect("read")
                    .as_ref(),
                "the listing returns rows whole"
            );
        }
        for status in BackupStatus::ALL {
            let of_status: Vec<&BackupRow> =
                all.iter().copied().filter(|r| r.status == status).collect();
            assert_eq!(
                arns(listed(StoreKind::Filesystem, root.to_owned(), Some(status)).await),
                in_listing_order(&of_status),
                "{status}"
            );
        }
        for (kind, label, row) in [
            (StoreKind::Filesystem, slash.as_str(), &others[0]),
            (StoreKind::Filesystem, "/srv/backups-ol", &others[1]),
            (StoreKind::S3, root, &others[2]),
            (StoreKind::S3, s3, &others[3]),
            (StoreKind::S3, s3_endpoint, &others[4]),
        ] {
            assert_eq!(
                arns(listed(kind, label.to_owned(), None).await),
                std::slice::from_ref(&row.backup_arn),
                "{kind} {label}"
            );
        }
        assert!(
            listed(StoreKind::Filesystem, "/srv/backups".to_owned(), None)
                .await
                .is_empty()
        );
        drop(claim);
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

/// `rewrite_store`: of the keys given, the rows still of the old kind and
/// root and not CREATING get the new store, with every other column left,
/// and are counted; a CREATING row, a row of another root, a row of the
/// other kind at the same label and a key with no row are left and not
/// counted; a second call counts 0; a row another writer moved to a third
/// root before the call is left there. The `$inc` of `lock_seq` is the one
/// other field the rewrite changes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_rewrite_store_moves_only_the_old_roots_rows_that_are_not_creating() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let old = (StoreKind::Filesystem, "/srv/backups-old", "old-1");
        let to = StoreIdentity {
            store_kind: StoreKind::Filesystem,
            store_root: "/srv/backups-new".to_owned(),
            store_id: "new-1".to_owned(),
        };
        let mut movable = Vec::new();
        for (i, status) in [
            BackupStatus::Available,
            BackupStatus::Deleting,
            BackupStatus::Deleted,
            BackupStatus::Failed,
        ]
        .into_iter()
        .enumerate()
        {
            movable.push(row_at("Music", &format!("m{i}"), status, old));
        }
        let moved_away = row_at("Music", "away", BackupStatus::Available, old);
        let other_root = row_at(
            "Music",
            "other-root",
            BackupStatus::Available,
            (StoreKind::Filesystem, "/srv/backups-other", "old-1"),
        );
        let other_kind = row_at(
            "Music",
            "other-kind",
            BackupStatus::Available,
            (StoreKind::S3, "/srv/backups-old", "old-1"),
        );
        for row in movable
            .iter()
            .chain([&moved_away, &other_root, &other_kind])
        {
            assert!(fx.engine.insert_backup_row(row).await.expect("insert"));
        }
        let creating = row_at("Music", "job", BackupStatus::Creating, old);
        let claim = fx.engine.claim_backup(&creating).await.expect("claim");
        let stored = |row: &BackupRow| {
            let engine = &fx.engine;
            let arn = arn_of(row);
            async move {
                engine
                    .read_backup_row(&arn)
                    .await
                    .expect("read")
                    .expect("the row exists")
            }
        };
        let lock_seq = |row: &BackupRow| {
            let engine = &fx.engine;
            let id = row.backup_id.clone();
            async move {
                engine
                    .catalog_db
                    .collection::<Document>(BACKUP_INDEX)
                    .find_one(doc! { "backup_id": &id })
                    .await
                    .expect("read")
                    .expect("the document exists")
                    .get_i64("lock_seq")
                    .expect("lock_seq")
            }
        };
        let mut before = Vec::new();
        for row in &movable {
            before.push((stored(row).await, lock_seq(row).await));
        }
        // Another writer moves one row to a third root first.
        let third = StoreIdentity {
            store_kind: StoreKind::Filesystem,
            store_root: "/srv/backups-third".to_owned(),
            store_id: "third-1".to_owned(),
        };
        assert_eq!(
            fx.engine
                .rewrite_store(old.0, old.1, &[moved_away.key()], &third)
                .await
                .expect("rewrite"),
            1
        );
        let mut ghost = row_at("Music", "ghost", BackupStatus::Available, old).key();
        ghost.backup_id = extenddb_storage::util::new_backup_id();
        let keys: Vec<extenddb_storage::backup_catalog::BackupKey> = movable
            .iter()
            .chain([&creating, &moved_away, &other_root, &other_kind])
            .map(BackupRow::key)
            .chain([ghost])
            .collect();
        assert_eq!(
            fx.engine
                .rewrite_store(old.0, old.1, &keys, &to)
                .await
                .expect("rewrite"),
            4,
            "the four rows of the old root that are not CREATING"
        );
        for (row, (before, seq)) in movable.iter().zip(&before) {
            assert_eq!(
                stored(row).await,
                BackupRow {
                    store: to.clone(),
                    ..before.clone()
                },
                "only the store changed"
            );
            assert_eq!(
                lock_seq(row).await,
                seq + 1,
                "the move advances the row's lock counter"
            );
        }
        let creating_now = stored(&creating).await;
        assert_eq!(
            (creating_now.status, creating_now.store.store_root.as_str()),
            (BackupStatus::Creating, old.1)
        );
        assert_eq!(creating_now.owner_generation, claim.generation());
        assert_eq!(stored(&moved_away).await.store, third);
        assert_eq!(stored(&other_root).await.store, other_root.store);
        assert_eq!(stored(&other_kind).await.store, other_kind.store);
        assert_eq!(
            fx.engine
                .rewrite_store(old.0, old.1, &keys, &to)
                .await
                .expect("rewrite"),
            0,
            "a second call finds nothing left to move"
        );
        let listed: Vec<BackupRow> = fx
            .engine
            .list_backup_rows(None, &to)
            .try_collect()
            .await
            .expect("listing");
        let movable_refs: Vec<&BackupRow> = movable.iter().collect();
        assert_eq!(
            listed.into_iter().map(|r| r.backup_arn).collect::<Vec<_>>(),
            in_listing_order(&movable_refs)
        );
        assert_eq!(
            fx.engine.read_store_ids(old.0, old.1).await.expect("ids"),
            ["old-1"],
            "the CREATING row keeps the old root"
        );
        assert_eq!(
            fx.engine
                .rewrite_store(old.0, old.1, &[], &to)
                .await
                .expect("rewrite"),
            0
        );
        drop(claim);
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

/// `rewrite_store` over more keys than one statement takes moves them all,
/// every chunk counted.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_rewrite_store_moves_every_chunk_of_keys() {
    use crate::backup_catalog::REWRITE_CHUNK_KEYS;
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let old = (StoreKind::Filesystem, "/srv/backups-old", "old-1");
        let to = StoreIdentity {
            store_kind: StoreKind::Filesystem,
            store_root: "/srv/backups-new".to_owned(),
            store_id: "new-1".to_owned(),
        };
        let rows = REWRITE_CHUNK_KEYS + 1;
        let mut keys = Vec::with_capacity(rows);
        for i in 0..rows {
            let row = row_at("Music", &format!("r{i}"), BackupStatus::Available, old);
            assert!(fx.engine.insert_backup_row(&row).await.expect("insert"));
            keys.push(row.key());
        }
        assert_eq!(
            fx.engine
                .rewrite_store(old.0, old.1, &keys, &to)
                .await
                .expect("rewrite"),
            rows as u64
        );
        let left: Vec<BackupRow> = fx
            .engine
            .list_backup_rows_by_root(old.0, old.1, None)
            .try_collect()
            .await
            .expect("listing");
        assert!(left.is_empty(), "{} rows left", left.len());
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

/// `summarize_store_roots`: one count per kind, root, id and status with a
/// row, in the one order; two ids of one root, two statuses of one id, two
/// roots and the two kinds are each counted apart.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_summarize_store_roots_counts_the_rows_of_each_store_and_status() {
    use extenddb_storage::backup_catalog::StoreRootCount;
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        assert!(
            fx.engine
                .summarize_store_roots()
                .await
                .expect("summary")
                .is_empty()
        );
        let a1 = (StoreKind::Filesystem, "/srv/a", "id-1");
        let a2 = (StoreKind::Filesystem, "/srv/a", "id-2");
        let b1 = (StoreKind::Filesystem, "/srv/b", "id-1");
        let s3 = (StoreKind::S3, "s3://bucket/a", "id-1");
        let rows = [
            row_at("Music", "1", BackupStatus::Available, a1),
            row_at("Music", "2", BackupStatus::Available, a1),
            row_at("Music", "3", BackupStatus::Failed, a1),
            row_at("Music", "4", BackupStatus::Available, a2),
            row_at("Music", "5", BackupStatus::Deleting, b1),
            row_at("Music", "7", BackupStatus::Available, b1),
            row_at("Music", "6", BackupStatus::Available, s3),
        ];
        for row in &rows {
            assert!(fx.engine.insert_backup_row(row).await.expect("insert"));
        }
        let creating = row_at("Music", "job", BackupStatus::Creating, b1);
        let claim = fx.engine.claim_backup(&creating).await.expect("claim");
        let line = |(kind, root, id): (StoreKind, &str, &str), status, rows| StoreRootCount {
            store: StoreIdentity {
                store_kind: kind,
                store_root: root.to_owned(),
                store_id: id.to_owned(),
            },
            status,
            rows,
        };
        assert_eq!(
            fx.engine.summarize_store_roots().await.expect("summary"),
            [
                line(a1, BackupStatus::Available, 2),
                line(a1, BackupStatus::Failed, 1),
                line(a2, BackupStatus::Available, 1),
                line(b1, BackupStatus::Creating, 1),
                line(b1, BackupStatus::Available, 1),
                line(b1, BackupStatus::Deleting, 1),
                line(s3, BackupStatus::Available, 1),
            ]
        );
        drop(claim);
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

/// `insert_backup_row`: the row lands as given, unowned (the document
/// carries `owner_generation: 0`, which the claim's upsert filter relies on),
/// with `completed_at` and `heartbeat_at` on the server's clock; a second
/// insert, an insert over a CREATING row a claim holds, and an insert of the
/// id under another account (the unique `backup_id`) answer `false` and
/// change nothing; a claim over the inserted row is refused; the row is read
/// and listed like a created one.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_insert_backup_row_inserts_an_unowned_row_only_when_absent() {
    let fx = Fixture::new().await;
    let mut adopted = row(ACCOUNT, "Music", "adopted");
    adopted.status = BackupStatus::Available;
    adopted.size_bytes = 1234;
    adopted.item_count = 5;
    // Ignored: the insert sets these three itself.
    adopted.owner_generation = 99;
    adopted.completed_at = None;
    adopted.heartbeat_at = 0.0;
    assert!(fx.engine.insert_backup_row(&adopted).await.expect("insert"));
    let stored = fx
        .engine
        .read_backup_row(&arn_of(&adopted))
        .await
        .expect("read")
        .expect("the row exists");
    assert_eq!(stored.owner_generation, UNOWNED_GENERATION);
    let completed_at = stored.completed_at.expect("completed_at is set");
    for at in [completed_at, stored.heartbeat_at] {
        assert!((at - adopted.created_at).abs() < 300.0, "{at}");
    }
    assert!(
        (stored.created_at - adopted.created_at).abs() < 1e-3,
        "{} is the id's instant {}",
        stored.created_at,
        adopted.created_at
    );
    assert_eq!(
        BackupRow {
            owner_generation: 99,
            completed_at: None,
            heartbeat_at: 0.0,
            created_at: adopted.created_at,
            ..stored.clone()
        },
        adopted,
        "every other column is the row as given"
    );

    // A second insert of the key, with other values, writes nothing.
    let mut again = adopted.clone();
    again.backup_name = "again".to_owned();
    again.size_bytes = 1;
    again.status = BackupStatus::Failed;
    assert!(!fx.engine.insert_backup_row(&again).await.expect("insert"));
    // The id under another account: the unique `backup_id`.
    let mut elsewhere = adopted.clone();
    elsewhere.account_id = OTHER_ACCOUNT.to_owned();
    elsewhere.backup_arn =
        extenddb_storage::util::backup_arn(REGION, OTHER_ACCOUNT, "Music", &adopted.backup_id);
    assert!(
        !fx.engine
            .insert_backup_row(&elsewhere)
            .await
            .expect("insert")
    );
    assert!(
        fx.engine
            .read_backup_row(&arn_of(&elsewhere))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fx.engine.read_backup_row(&arn_of(&adopted)).await.unwrap(),
        Some(stored.clone()),
        "the refused inserts changed nothing"
    );
    let claimed = fx.engine.claim_backup(&adopted).await.err();
    assert!(
        matches!(claimed, Some(StorageError::AlreadyExists(_))),
        "{claimed:?}"
    );

    // Over a CREATING row a live claim holds.
    let creating = row(ACCOUNT, "Music", "job");
    let claim = fx.engine.claim_backup(&creating).await.expect("claim");
    let mut over = creating.clone();
    over.status = BackupStatus::Available;
    assert!(!fx.engine.insert_backup_row(&over).await.expect("insert"));
    let job_row = fx
        .engine
        .read_backup_row(&arn_of(&creating))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job_row.status, BackupStatus::Creating);
    assert_eq!(job_row.owner_generation, claim.generation());

    // Read and listed like a created row.
    let arns =
        |rows: Vec<BackupRow>| -> Vec<String> { rows.into_iter().map(|r| r.backup_arn).collect() };
    let available: Vec<BackupRow> = fx
        .engine
        .list_backup_rows(Some(BackupStatus::Available), &store())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(available, std::slice::from_ref(&stored));
    let creating_rows: Vec<BackupRow> = fx
        .engine
        .list_backup_rows(Some(BackupStatus::Creating), &store())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        arns(creating_rows),
        std::slice::from_ref(&creating.backup_arn)
    );
    for table in [None, Some("Music")] {
        let listed: Vec<BackupRow> = fx
            .engine
            .list_account_backup_rows(ACCOUNT, table, &TimeBounds::default())
            .try_collect()
            .await
            .unwrap();
        assert!(
            listed.contains(&stored),
            "{table:?}: {:?}",
            arns(listed.clone())
        );
    }
    drop(claim);

    let document = fx
        .engine
        .backup_index()
        .find_one(doc! { "backup_id": &adopted.backup_id })
        .await
        .expect("read")
        .expect("the document");
    assert_eq!(document.get_i64("owner_generation"), Ok(0), "{document}");
    assert_eq!(
        fx.engine
            .backup_index()
            .count_documents(doc! { "backup_id": &adopted.backup_id })
            .await
            .expect("count"),
        1
    );
    fx.finish().await;
}

/// `remove_dead_creating_backup_row`: a CREATING row whose heartbeat is older
/// than the window is removed outright; one with a live owner, one that is
/// not CREATING, and one that is gone are left, answering `false`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_dead_creating_row_is_removed_and_a_live_or_moved_one_is_kept() {
    let fx = Fixture::new().await;
    let stale = std::time::Duration::from_secs(300);
    let index = fx.engine.backup_index();
    let age = |backup_id: String| {
        let index = index.clone();
        async move {
            index
                .update_one(
                    doc! { "backup_id": backup_id },
                    vec![doc! { "$set": { "heartbeat_at": { "$subtract": [ "$$NOW", 600_000_i64 ] } } }],
                )
                .await
                .expect("age");
        }
    };
    let creating = row(ACCOUNT, "Music", "a");
    let key = creating.key();
    let claim = fx.engine.claim_backup(&creating).await.expect("claim");
    assert!(
        !fx.engine
            .remove_dead_creating_backup_row(&key, stale)
            .await
            .unwrap(),
        "a live owner keeps its row"
    );
    age(creating.backup_id.clone()).await;
    assert!(
        fx.engine
            .remove_dead_creating_backup_row(&key, stale)
            .await
            .unwrap()
    );
    assert!(
        fx.engine
            .read_backup_row(&arn_of(&creating))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !fx.engine
            .remove_dead_creating_backup_row(&key, stale)
            .await
            .unwrap(),
        "gone"
    );
    drop(claim);

    let available = row(ACCOUNT, "Music", "b");
    let claim = fx.engine.claim_backup(&available).await.expect("claim");
    claim
        .set_status(
            JobStatus::Creating,
            JobStatus::Available,
            &StatusFields::default(),
        )
        .await
        .expect("available");
    age(available.backup_id.clone()).await;
    assert!(
        !fx.engine
            .remove_dead_creating_backup_row(&available.key(), stale)
            .await
            .unwrap(),
        "only a CREATING row"
    );
    assert_eq!(
        fx.engine
            .read_backup_row(&arn_of(&available))
            .await
            .unwrap()
            .unwrap()
            .status,
        BackupStatus::Available
    );
    drop(claim);
    fx.finish().await;
}

/// The `backup_index` collection read at `majority`, for the observations
/// a test makes between the writes under test.
fn index_majority(engine: &MongoEngine) -> mongodb::Collection<Document> {
    engine.catalog_db.collection_with_options(
        BACKUP_INDEX,
        mongodb::options::CollectionOptions::builder()
            .read_concern(mongodb::options::ReadConcern::majority())
            .build(),
    )
}

async fn index_doc(engine: &MongoEngine, row: &BackupRow) -> Document {
    index_majority(engine)
        .find_one(doc! { "account_id": &row.account_id, "backup_id": &row.backup_id })
        .await
        .expect("read the row")
        .expect("the row exists")
}

/// `claim_dead_backup`: a CREATING row is refused while its owner's heartbeat
/// is within the window, and taken over once the window has passed on the
/// server's clock (no hand-dating: the test waits), under a fresh generation
/// and with `heartbeat_at` as the dead owner left it; the old claim then
/// finds its heartbeat lost and its status write unmatched; the successor's
/// heartbeat makes it a live owner a further taker is refused, and its
/// AVAILABLE write carries the source table object and stamps
/// `completed_at`. An AVAILABLE row and a missing row are refused.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_claim_dead_backup_takes_over_a_stale_row_and_refuses_a_live_one() {
    let fx = Fixture::new().await;
    let window = Duration::from_secs(3);
    let dead = row(ACCOUNT, "Music", "a");
    let key = dead.key();
    let owner = fx.engine.claim_backup(&dead).await.expect("claim");
    assert_eq!(owner.heartbeat().await.unwrap(), Heartbeat::Confirmed);
    assert!(
        fx.engine
            .claim_dead_backup(&key, STALE_AFTER)
            .await
            .unwrap()
            .is_none(),
        "a live owner"
    );
    assert!(
        fx.engine
            .claim_dead_backup(&key, window)
            .await
            .unwrap()
            .is_none(),
        "a heartbeat within the window"
    );
    let before = index_doc(&fx.engine, &dead).await;
    let heartbeat_before = *before.get_datetime("heartbeat_at").expect("heartbeat_at");
    tokio::time::sleep(window + Duration::from_millis(300)).await;
    let successor = fx
        .engine
        .claim_dead_backup(&key, window)
        .await
        .unwrap()
        .expect("the window has passed on the server's clock");
    assert_ne!(successor.generation(), owner.generation());
    assert_ne!(successor.generation(), UNOWNED_GENERATION);
    let after = index_doc(&fx.engine, &dead).await;
    assert_eq!(
        *after.get_datetime("heartbeat_at").expect("heartbeat_at"),
        heartbeat_before,
        "the takeover left the heartbeat as the dead owner wrote it"
    );
    assert_eq!(after.get_str("status"), Ok("CREATING"));
    assert_eq!(
        after.get_i64("owner_generation").unwrap(),
        i64::try_from(successor.generation()).unwrap()
    );
    assert_eq!(owner.heartbeat().await.unwrap(), Heartbeat::Lost);
    assert_eq!(
        owner
            .set_status(
                JobStatus::Creating,
                JobStatus::Available,
                &StatusFields::default()
            )
            .await
            .unwrap(),
        WriteOutcome::NotMatched,
        "the old generation is fenced"
    );
    assert_eq!(successor.heartbeat().await.unwrap(), Heartbeat::Confirmed);

    // A second taker while the row is held: the heartbeat the successor
    // just refreshed is within the window, so it is refused; another
    // process's reconciler racing the takeover itself, before any
    // heartbeat, would take the row again and fence the successor.
    assert!(
        fx.engine
            .claim_dead_backup(&key, window)
            .await
            .unwrap()
            .is_none(),
        "a heartbeating successor is a live owner"
    );

    let mut source = dead.source_table_details_json.clone();
    source["item_count"] = serde_json::json!(4242);
    let fields = StatusFields {
        size_bytes: Some(777),
        item_count: Some(4242),
        failure_reason: None,
        source_table_details_json: Some(source.clone()),
        completed: true,
    };
    assert_eq!(
        successor
            .set_status(JobStatus::Creating, JobStatus::Available, &fields)
            .await
            .unwrap(),
        WriteOutcome::Matched
    );
    let stored = fx
        .engine
        .read_backup_row(&arn_of(&dead))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, BackupStatus::Available);
    assert_eq!(stored.size_bytes, 777);
    assert_eq!(stored.item_count, 4242);
    assert_eq!(
        stored.source_table_details_json, source,
        "the AVAILABLE write carries the source table object"
    );
    assert_eq!(stored.source_table().unwrap().item_count, 4242);
    assert!(stored.completed_at.is_some());
    assert_eq!(stored.owner_generation, successor.generation());

    tokio::time::sleep(window + Duration::from_millis(300)).await;
    assert!(
        fx.engine
            .claim_dead_backup(&key, window)
            .await
            .unwrap()
            .is_none(),
        "an AVAILABLE row with a stale heartbeat is not CREATING"
    );
    let missing = row(ACCOUNT, "Music", "none").key();
    assert!(
        fx.engine
            .claim_dead_backup(&missing, window)
            .await
            .unwrap()
            .is_none(),
        "a missing row"
    );
    drop(owner);
    drop(successor);
    fx.finish().await;
}

/// `list_backup_rows_older_than`: the rows of the store with the status
/// whose named clock column is older than the age on the server's clock. A
/// row FAILED just now by a dead owner is listed on the heartbeat clock (its
/// heartbeat is the owner's last) and not yet on the completed clock, which
/// tells the two columns apart; the completed window is then crossed by
/// waiting. A FAILED row with no `completed_at` is never listed on the
/// completed clock. A CREATING row, and a FAILED row of another store, are
/// not listed under FAILED; a DELETED row is listed under DELETED on its
/// `completed_at`. The listing keeps the catalog's order.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_rows_older_than_are_listed_on_the_named_clock() {
    let fx = Fixture::new().await;
    let window = Duration::from_secs(3);
    let names = |listed: Vec<BackupRow>| -> Vec<String> {
        listed.into_iter().map(|r| r.backup_name).collect()
    };
    let older = |status: BackupStatus, store: StoreIdentity, clock: StaleClock| {
        let engine = &fx.engine;
        async move {
            let rows: Vec<BackupRow> = engine
                .list_backup_rows_older_than(status, &store, clock, window)
                .try_collect()
                .await
                .expect("listing");
            names(rows)
        }
    };
    let mut other_store = store();
    other_store.store_id = "store-2".to_owned();

    let completed = row(ACCOUNT, "Music", "completed");
    let completed_claim = fx.engine.claim_backup(&completed).await.expect("claim");
    tokio::time::sleep(Duration::from_millis(5)).await;
    let unstamped = row(ACCOUNT, "Music", "unstamped");
    let unstamped_claim = fx.engine.claim_backup(&unstamped).await.expect("claim");
    tokio::time::sleep(Duration::from_millis(5)).await;
    let creating = row(ACCOUNT, "Music", "creating");
    let _creating_claim = fx.engine.claim_backup(&creating).await.expect("claim");
    let mut elsewhere = row(ACCOUNT, "Music", "elsewhere");
    elsewhere.store = other_store.clone();
    let elsewhere_claim = fx.engine.claim_backup(&elsewhere).await.expect("claim");
    let deleted = row(ACCOUNT, "Music", "deleted");
    let deleted_claim = fx.engine.claim_backup(&deleted).await.expect("claim");
    let done = StatusFields {
        completed: true,
        ..StatusFields::default()
    };
    assert_eq!(
        deleted_claim
            .set_status(JobStatus::Creating, JobStatus::Available, &done)
            .await
            .unwrap(),
        WriteOutcome::Matched
    );
    assert_eq!(
        fx.engine.start_backup_delete(&deleted.key()).await.unwrap(),
        DeleteStart::Deleting
    );
    assert!(fx.engine.set_backup_deleted(&deleted.key()).await.unwrap());
    assert!(
        older(BackupStatus::Failed, store(), StaleClock::Heartbeat)
            .await
            .is_empty(),
        "no row is FAILED yet"
    );

    tokio::time::sleep(window + Duration::from_millis(300)).await;
    // The owners die here: FAILED writes with the heartbeat left as it was.
    for (claim, fields) in [
        (&completed_claim, done.clone()),
        (&unstamped_claim, StatusFields::default()),
        (&elsewhere_claim, done.clone()),
    ] {
        assert_eq!(
            claim
                .set_status(JobStatus::Creating, JobStatus::Failed, &fields)
                .await
                .unwrap(),
            WriteOutcome::Matched
        );
    }
    let unstamped_doc = index_doc(&fx.engine, &unstamped).await;
    assert!(
        unstamped_doc.get("completed_at").is_none(),
        "a write with `completed: false` stamps nothing: {unstamped_doc}"
    );
    assert_eq!(
        older(BackupStatus::Failed, store(), StaleClock::Heartbeat).await,
        ["completed", "unstamped"],
        "rows FAILED just now are listed on their dead owners' heartbeats, in the catalog's order"
    );
    assert!(
        older(BackupStatus::Failed, store(), StaleClock::Completed)
            .await
            .is_empty(),
        "a `completed_at` stamped just now is within the window; an unset one is never older"
    );
    assert_eq!(
        older(BackupStatus::Deleted, store(), StaleClock::Completed).await,
        ["deleted"]
    );
    assert_eq!(
        older(BackupStatus::Creating, store(), StaleClock::Heartbeat).await,
        ["creating"]
    );
    assert_eq!(
        older(
            BackupStatus::Failed,
            other_store.clone(),
            StaleClock::Heartbeat
        )
        .await,
        ["elsewhere"]
    );

    tokio::time::sleep(window + Duration::from_millis(300)).await;
    assert_eq!(
        older(BackupStatus::Failed, store(), StaleClock::Completed).await,
        ["completed"],
        "the completed window crossed on the server's clock; no `completed_at` is never older"
    );
    assert_eq!(
        older(BackupStatus::Failed, store(), StaleClock::Heartbeat).await,
        ["completed", "unstamped"]
    );
    fx.finish().await;
}

/// The maintenance claim: taken once, refused to a second holder naming the
/// first, heartbeated on the server's clock, released; a holder whose
/// heartbeat is older than the stale window is dead, so its claim is not
/// reported held and is taken over, and its handle finds the claim lost. A
/// record without a heartbeat (no release of this server wrote one) is
/// stale.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_the_maintenance_claim_excludes_a_second_holder_and_expires_when_stale() {
    let fx = Fixture::new().await;
    let stale = std::time::Duration::from_secs(300);
    let check = || fx.engine.check_maintenance_claim(stale);
    let maintenance = fx
        .engine
        .catalog_db
        .collection::<Document>(crate::backup_catalog::BACKUP_MAINTENANCE);
    assert!(!check().await.expect("check"));
    let first = fx
        .engine
        .maintenance_claim("purge on host-a", stale)
        .await
        .expect("claim");
    assert_ne!(first.generation(), UNOWNED_GENERATION);
    assert!(check().await.expect("check"), "a live claim is held");
    match fx.engine.maintenance_claim("purge on host-b", stale).await {
        Err(StorageError::AlreadyExists(m)) => assert!(m.contains("purge on host-a"), "{m}"),
        Err(other) => panic!("a second holder is refused with AlreadyExists: {other:?}"),
        Ok(_) => panic!("a second holder is refused"),
    }
    assert_eq!(
        first.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    first.release().await.expect("release");
    assert!(!check().await.expect("check"), "released");
    assert_eq!(
        maintenance.count_documents(doc! {}).await.expect("count"),
        0
    );
    first.release().await.expect("a second release is a no-op");

    // A dead holder: its heartbeat aged past the window.
    let dead = fx
        .engine
        .maintenance_claim("purge on host-c", stale)
        .await
        .expect("claim");
    maintenance
        .update_one(
            doc! {},
            vec![doc! { "$set": { "heartbeat_at": { "$subtract": [ "$$NOW", 600_000_i64 ] } } }],
        )
        .await
        .expect("age");
    assert!(!check().await.expect("check"), "a stale claim is not held");
    let taker = fx
        .engine
        .maintenance_claim("purge on host-d", stale)
        .await
        .expect("a dead holder's claim is taken over");
    assert_ne!(taker.generation(), dead.generation());
    assert!(check().await.expect("check"));
    assert_eq!(
        dead.heartbeat().await.expect("heartbeat"),
        Heartbeat::Lost,
        "the dead holder's handle finds the claim lost"
    );
    dead.release()
        .await
        .expect("a release by a lost holder changes nothing");
    assert!(check().await.expect("check"), "the taker still holds it");
    let holder = maintenance
        .find_one(doc! {})
        .await
        .expect("find")
        .expect("record");
    assert_eq!(holder.get_str("holder").unwrap(), "purge on host-d");
    taker.release().await.expect("release");
    assert!(!check().await.expect("check"));

    // A record with no heartbeat at all is stale: taken over, never held.
    maintenance
        .insert_one(
            doc! { "_id": "claim", "holder": "an older release", "owner_generation": 1_i64 },
        )
        .await
        .expect("record");
    assert!(!check().await.expect("check"));
    let over = fx
        .engine
        .maintenance_claim("purge on host-e", stale)
        .await
        .expect("taken over");
    assert!(check().await.expect("check"));
    over.release().await.expect("release");
    fx.finish().await;
}

/// DeleteBackup's first step and the tombstone writes: only an AVAILABLE
/// row moves to DELETING; a CREATING row answers its status; a backup a
/// CREATING restore target reads answers the target's name; DELETING moves
/// to DELETED once; a DELETED row is removed once; a missing row is "Backup
/// not found".
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_start_backup_delete_checks_the_status_and_the_in_use_rule_under_the_lock() {
    let fx = Fixture::new().await;
    let row = row(ACCOUNT, "Music", "nightly");
    let key = row.key();
    assert!(matches!(
        fx.engine.start_backup_delete(&key).await,
        Err(StorageError::Validation(m)) if m.contains("Backup not found")
    ));
    let claim = fx.engine.claim_backup(&row).await.expect("claim");
    assert_eq!(
        fx.engine.start_backup_delete(&key).await.unwrap(),
        DeleteStart::Status(BackupStatus::Creating)
    );
    assert_eq!(
        claim
            .set_status(
                JobStatus::Creating,
                JobStatus::Available,
                &StatusFields::default()
            )
            .await
            .unwrap(),
        WriteOutcome::Matched
    );
    assert_eq!(fx.engine.backup_in_use(&key).await.unwrap(), None);

    // A CREATING restore target registered from the backup: in use, naming
    // the target; an ACTIVE one is not.
    source_table_with(&fx.engine, "del_target", 1).await;
    let tables = fx.engine.catalog_db.collection::<Document>("tables");
    let target_filter = doc! { "_id": { "account_id": ACCOUNT, "table_name": "del_target" } };
    tables
        .update_one(
            target_filter.clone(),
            doc! { "$set": {
                "table_status": "CREATING",
                "status_transition_at": bson::Bson::Null,
                "restore_source_backup_arn": &row.backup_arn,
                "restore_source_table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/Music",
                "restore_date_time": bson::DateTime::now(),
                RESTORE_JOB: {
                    "status": "RUNNING",
                    "kind": "manifest",
                    "owner_generation": 7_i64,
                    "heartbeat_at": bson::DateTime::now(),
                },
            } },
        )
        .await
        .expect("register a target");
    assert_eq!(
        fx.engine.backup_in_use(&key).await.unwrap().as_deref(),
        Some("del_target")
    );
    assert_eq!(
        fx.engine.start_backup_delete(&key).await.unwrap(),
        DeleteStart::InUse("del_target".to_owned())
    );
    assert_eq!(
        fx.engine.forget_available_backup_row(&key).await.unwrap(),
        extenddb_storage::backup_catalog::ForgetRow::InUse("del_target".to_owned()),
        "the outright removal keeps the in-use rule"
    );
    assert_eq!(
        fx.engine
            .read_backup_row(&arn_of(&row))
            .await
            .unwrap()
            .unwrap()
            .status,
        BackupStatus::Available,
        "a refused delete leaves the row AVAILABLE"
    );
    tables
        .update_one(target_filter, doc! { "$set": { "table_status": "ACTIVE" } })
        .await
        .expect("make it ACTIVE");
    assert_eq!(fx.engine.backup_in_use(&key).await.unwrap(), None);

    assert_eq!(
        fx.engine.start_backup_delete(&key).await.unwrap(),
        DeleteStart::Deleting
    );
    assert_eq!(
        fx.engine.start_backup_delete(&key).await.unwrap(),
        DeleteStart::Status(BackupStatus::Deleting),
        "a retry finds the row DELETING"
    );
    assert_eq!(
        fx.engine.forget_available_backup_row(&key).await.unwrap(),
        extenddb_storage::backup_catalog::ForgetRow::Status(BackupStatus::Deleting),
        "only an AVAILABLE row is removed outright"
    );
    // An AVAILABLE row goes outright, read and listed nowhere, with no
    // tombstone; a second removal finds no row.
    let mut adopted = self::row(ACCOUNT, "Music", "adopted");
    adopted.status = BackupStatus::Available;
    assert!(fx.engine.insert_backup_row(&adopted).await.expect("insert"));
    assert_eq!(
        fx.engine
            .forget_available_backup_row(&adopted.key())
            .await
            .unwrap(),
        extenddb_storage::backup_catalog::ForgetRow::Removed
    );
    assert!(
        fx.engine
            .read_backup_row(&arn_of(&adopted))
            .await
            .unwrap()
            .is_none()
    );
    for status in [None, Some(BackupStatus::Available)] {
        let listed: Vec<BackupRow> = fx
            .engine
            .list_backup_rows(status, &adopted.store)
            .try_collect()
            .await
            .unwrap();
        assert!(
            !listed.iter().any(|r| r.backup_arn == adopted.backup_arn),
            "{status:?}"
        );
    }
    let by_account: Vec<BackupRow> = fx
        .engine
        .list_account_backup_rows(&adopted.account_id, None, &TimeBounds::default())
        .try_collect()
        .await
        .unwrap();
    assert!(
        !by_account
            .iter()
            .any(|r| r.backup_arn == adopted.backup_arn)
    );
    assert!(matches!(
        fx.engine.forget_available_backup_row(&adopted.key()).await,
        Err(StorageError::Validation(m)) if m.contains("Backup not found")
    ));
    assert!(
        !fx.engine
            .remove_backup_row(&key, row.store.store_kind, &row.store.store_root)
            .await
            .unwrap(),
        "DELETING is not a tombstone"
    );
    assert!(fx.engine.set_backup_deleted(&key).await.unwrap());
    assert!(!fx.engine.set_backup_deleted(&key).await.unwrap());
    let stored = fx
        .engine
        .read_backup_row(&arn_of(&row))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, BackupStatus::Deleted);
    assert!(stored.completed_at.is_some());
    // A row another writer moved to another store after the caller read it
    // is left by a removal that names the store read.
    let moved_to = StoreIdentity {
        store_kind: row.store.store_kind,
        store_root: format!("{}-moved", row.store.store_root),
        store_id: "moved-1".to_owned(),
    };
    assert_eq!(
        fx.engine
            .rewrite_store(
                row.store.store_kind,
                &row.store.store_root,
                std::slice::from_ref(&key),
                &moved_to
            )
            .await
            .unwrap(),
        1
    );
    assert!(
        !fx.engine
            .remove_backup_row(&key, row.store.store_kind, &row.store.store_root)
            .await
            .unwrap(),
        "a row moved to another store is left"
    );
    assert_eq!(
        fx.engine
            .read_backup_row(&arn_of(&row))
            .await
            .unwrap()
            .map(|r| (r.status, r.store)),
        Some((BackupStatus::Deleted, moved_to.clone()))
    );
    // The same root under the other kind is another store: a removal that
    // names the kind it read leaves the row.
    let other_kind = StoreIdentity {
        store_kind: if moved_to.store_kind == StoreKind::S3 {
            StoreKind::Filesystem
        } else {
            StoreKind::S3
        },
        ..moved_to.clone()
    };
    assert_eq!(
        fx.engine
            .rewrite_store(
                moved_to.store_kind,
                &moved_to.store_root,
                std::slice::from_ref(&key),
                &other_kind
            )
            .await
            .unwrap(),
        1
    );
    assert!(
        !fx.engine
            .remove_backup_row(&key, moved_to.store_kind, &moved_to.store_root)
            .await
            .unwrap(),
        "a row moved to the other kind is left"
    );
    let moved_to = other_kind;
    assert!(
        fx.engine
            .remove_backup_row(&key, moved_to.store_kind, &moved_to.store_root)
            .await
            .unwrap()
    );
    assert!(
        !fx.engine
            .remove_backup_row(&key, moved_to.store_kind, &moved_to.store_root)
            .await
            .unwrap()
    );
    assert!(
        fx.engine
            .read_backup_row(&arn_of(&row))
            .await
            .unwrap()
            .is_none()
    );
    drop(claim);
    fx.finish().await;
}

/// The listings: by account with an optional table and the time bounds
/// (lower inclusive, upper exclusive) in (created_at, ARN) order, and by
/// store and status.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_the_listings_are_scoped_and_ordered() {
    let fx = Fixture::new().await;
    let mut rows = Vec::new();
    for (account, table, name) in [
        (ACCOUNT, "Music", "a"),
        (ACCOUNT, "Music", "b"),
        (ACCOUNT, "Other", "c"),
        (OTHER_ACCOUNT, "Music", "d"),
    ] {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let row = row(account, table, name);
        let claim = fx.engine.claim_backup(&row).await.expect("claim");
        if name == "b" {
            assert_eq!(
                claim
                    .set_status(
                        JobStatus::Creating,
                        JobStatus::Failed,
                        &StatusFields::default()
                    )
                    .await
                    .unwrap(),
                WriteOutcome::Matched
            );
        }
        rows.push((row, claim));
    }
    let names = |listed: Vec<BackupRow>| -> Vec<String> {
        listed.into_iter().map(|r| r.backup_name).collect()
    };
    let all: Vec<BackupRow> = fx
        .engine
        .list_account_backup_rows(ACCOUNT, None, &TimeBounds::default())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        names(all.clone()),
        ["a", "b", "c"],
        "every status, in creation order"
    );
    assert_eq!(all[1].status, BackupStatus::Failed);
    let music: Vec<BackupRow> = fx
        .engine
        .list_account_backup_rows(ACCOUNT, Some("Music"), &TimeBounds::default())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(music), ["a", "b"]);
    let bounds = TimeBounds {
        lower: Some(rows[1].0.created_at),
        upper: Some(rows[2].0.created_at),
    };
    let bounded: Vec<BackupRow> = fx
        .engine
        .list_account_backup_rows(ACCOUNT, None, &bounds)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(bounded), ["b"], "lower inclusive, upper exclusive");
    let other: Vec<BackupRow> = fx
        .engine
        .list_account_backup_rows(OTHER_ACCOUNT, None, &TimeBounds::default())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(other), ["d"]);

    let by_store: Vec<BackupRow> = fx
        .engine
        .list_backup_rows(None, &store())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(by_store), ["a", "b", "c", "d"]);
    let failed: Vec<BackupRow> = fx
        .engine
        .list_backup_rows(Some(BackupStatus::Failed), &store())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(failed), ["b"]);
    let elsewhere = StoreIdentity {
        store_id: "store-2".to_owned(),
        ..store()
    };
    let none: Vec<BackupRow> = fx
        .engine
        .list_backup_rows(None, &elsewhere)
        .try_collect()
        .await
        .unwrap();
    assert!(none.is_empty(), "another store's rows are not listed");
    drop(rows);
    fx.finish().await;
}

/// A catalog at the shape before migration 6, holding a finished
/// restore, a restore handed to the GSI backfill worker at the stop (its
/// copy complete, the target CREATING with `restore_backfill_pending`) and a
/// restore the stop interrupted (its target CREATING, no flag), migrates with
/// the registrations backfilled: the finished and the handed-over ones DONE,
/// the interrupted one RUNNING with `heartbeat_at` at the epoch and the
/// unowned generation, so the reconciler's first pass fails it and removes
/// its target and leaves the handed-over one to the worker. The body keeps
/// the catalog id; a second run changes nothing.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_migration_6_backfills_the_registrations_and_the_reconciler_removes_the_interrupted_one()
 {
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "bf_done").await;
    fx.engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");
    assert_fully_restored_with(&fx.engine, "bf_done", FEW).await;
    // A restore the earlier release had handed to the GSI backfill worker at
    // the stop: the copy complete, the table CREATING with
    // `restore_backfill_pending`, one index still CREATING (so the worker
    // waits rather than flipping it while the step runs). Its data is whole,
    // so the step backfills it DONE, not RUNNING: marked RUNNING with the
    // epoch heartbeat, the first reconciler pass would fail it and remove a
    // target the worker is about to finish. From a backup of its own: a
    // CREATING target keeps its backup in use, which would refuse the
    // interrupted restore below.
    let handed_backup = fx
        .engine
        .create(ACCOUNT, "src", "b2", JobPermit::detached())
        .await
        .expect("second backup");
    let request = restore_request(&fx.engine, &handed_backup.backup_arn, "bf_handed").await;
    let handed = fx
        .engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");
    assert_fully_restored_with(&fx.engine, "bf_handed", FEW).await;
    let tables = fx.engine.catalog_db.collection::<Document>("tables");
    let index_docs = fx.engine.catalog_db.collection::<Document>("indexes");
    index_docs
        .update_one(
            doc! { "_id.table_id": &handed.table_id, "_id.index_name": "g1" },
            doc! { "$set": { "index_status": "CREATING" } },
        )
        .await
        .expect("an index still being backfilled");
    tables
        .update_one(
            doc! { "_id": { "account_id": ACCOUNT, "table_name": "bf_handed" } },
            doc! { "$set": {
                "table_status": "CREATING",
                "status_transition_at": bson::Bson::Null,
                "restore_backfill_pending": true,
            } },
        )
        .await
        .expect("handed over to the worker");
    let (gated, _gate) = gated(&fx.engine);
    let request = restore_request(&fx.engine, &backup.backup_arn, "bf_cut").await;
    let interrupted = gated
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");
    let catalog_id = fx.engine.catalog_id().await.expect("id");

    // Back to the shape before migration 6: the registrations as the
    // earlier release wrote them. The interrupted restore's copy task loses
    // its claim with the record and stops.
    tables
        .update_many(
            doc! { "restore_source_backup_arn": { "$exists": true } },
            doc! {
                "$unset": { RESTORE_JOB: "" },
                "$set": {
                    "restore_operation_id": "old-operation",
                    "restore_heartbeat_at": bson::DateTime::now(),
                },
            },
        )
        .await
        .expect("old shape");
    let report = run_backup_catalog_index(&target(&fx.catalog, &fx.data))
        .await
        .expect("migrate again");
    assert_eq!(
        report.notes,
        [
            "backfilled 3 restore registration(s) as legacy restores, 1 of them RUNNING with a \
             dead owner for the reconciler to remove"
        ],
        "the catalog id is kept"
    );
    assert_eq!(fx.engine.catalog_id().await.expect("id"), catalog_id);

    let done = table_doc(&fx.engine, "bf_done").await.expect("bf_done");
    let job = done.get_document(RESTORE_JOB).expect("job record");
    assert_eq!(job.get_str("status"), Ok("DONE"));
    assert_eq!(job.get_str("kind"), Ok("legacy"));
    assert_eq!(job.get_i64("owner_generation"), Ok(0));
    assert_eq!(
        job.get_datetime("heartbeat_at")
            .map(|d| d.timestamp_millis()),
        Ok(0),
        "the epoch, not absent"
    );
    assert_eq!(
        job.get_datetime("completed_at"),
        done.get_datetime("creation_date_time"),
        "completed when the table was created"
    );
    assert!(!done.contains_key("restore_operation_id"));
    assert!(!done.contains_key("restore_heartbeat_at"));
    let handed_doc = table_doc(&fx.engine, "bf_handed").await.expect("bf_handed");
    assert_eq!(handed_doc.get_str("table_status"), Ok("CREATING"));
    assert_eq!(handed_doc.get_bool("restore_backfill_pending"), Ok(true));
    let job = handed_doc.get_document(RESTORE_JOB).expect("job record");
    assert_eq!(
        job.get_str("status"),
        Ok("DONE"),
        "a target handed to the worker has its copy complete: DONE, not RUNNING"
    );
    assert_eq!(job.get_str("kind"), Ok("legacy"));
    assert_eq!(job.get_i64("owner_generation"), Ok(0));
    assert_eq!(
        job.get_datetime("completed_at"),
        handed_doc.get_datetime("creation_date_time")
    );
    let cut = table_doc(&fx.engine, "bf_cut").await.expect("bf_cut");
    assert_eq!(cut.get_str("table_status"), Ok("CREATING"));
    let job = cut.get_document(RESTORE_JOB).expect("job record");
    assert_eq!(job.get_str("status"), Ok("RUNNING"));
    assert_eq!(job.get_str("kind"), Ok("legacy"));
    assert_eq!(job.get_i64("owner_generation"), Ok(0));
    assert_eq!(
        job.get_datetime("heartbeat_at")
            .map(|d| d.timestamp_millis()),
        Ok(0)
    );
    assert!(!job.contains_key("completed_at"));

    // A second run changes nothing.
    let again = run_backup_catalog_index(&target(&fx.catalog, &fx.data))
        .await
        .expect("a second run");
    assert!(again.notes.is_empty(), "{:?}", again.notes);

    // The first reconciler pass: the interrupted restore is dead.
    let dead = fx
        .engine
        .list_dead_restore_jobs(STALE_AFTER)
        .await
        .expect("list");
    assert_eq!(
        dead.len(),
        1,
        "the interrupted restore alone; the handed-over one is DONE: {dead:?}"
    );
    assert_eq!(
        dead[0].target,
        RestoreTargetKey {
            account_id: ACCOUNT.to_owned(),
            table_name: "bf_cut".to_owned(),
            table_id: interrupted.table_id.clone(),
        }
    );
    assert_eq!(dead[0].owner_generation, UNOWNED_GENERATION);
    assert_eq!(dead[0].kind, RestoreKind::Legacy);
    let indexes = index_ids(&fx.engine, &interrupted.table_id).await;
    assert_eq!(indexes.len(), 2);
    assert!(
        fx.engine
            .fail_restore_job(
                &dead[0].target,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
            .await
            .unwrap()
    );
    assert!(
        fx.engine
            .remove_restore_target(&dead[0].target)
            .await
            .unwrap()
    );
    assert!(table_doc(&fx.engine, "bf_cut").await.is_none());
    assert!(!collection_exists(&fx.engine, &data_collection_name(&interrupted.table_id)).await);
    for (_, index_id, _) in &indexes {
        assert!(!collection_exists(&fx.engine, &data_collection_name(index_id)).await);
    }
    assert!(
        index_ids(&fx.engine, &interrupted.table_id)
            .await
            .is_empty()
    );
    assert_eq!(
        table_doc(&fx.engine, "bf_done")
            .await
            .expect("bf_done")
            .get_str("table_status"),
        Ok("ACTIVE")
    );
    // The worker finishes the handed-over target once its index is done.
    index_docs
        .update_one(
            doc! { "_id.table_id": &handed.table_id, "_id.index_name": "g1" },
            doc! { "$set": { "index_status": "ACTIVE" } },
        )
        .await
        .expect("the index backfill done");
    let finished = wait_for_status(&fx.engine, "bf_handed", "ACTIVE").await;
    assert!(!finished.contains_key("restore_backfill_pending"));
    assert_eq!(
        finished
            .get_document(RESTORE_JOB)
            .expect("job")
            .get_str("status"),
        Ok("DONE")
    );
    fx.finish().await;
}

/// The legacy restore registers through the job record: RUNNING under the
/// claim's generation while the copy runs, with a heartbeat on the server's
/// clock, and DONE with the hand-over.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_legacy_restore_registers_a_running_job_record_and_finishes_it_done() {
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let (gated, gate) = gated(&fx.engine);
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    gated
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("registers");
    let job = job_record(&fx.engine, "dst").await;
    assert_eq!(job.get_str("status"), Ok("RUNNING"));
    assert_eq!(job.get_str("kind"), Ok("legacy"));
    let generation = job.get_i64("owner_generation").expect("generation");
    assert_ne!(generation, i64::try_from(UNOWNED_GENERATION).unwrap());
    let first = *job.get_datetime("heartbeat_at").expect("heartbeat");
    let age_ms = bson::DateTime::now().timestamp_millis() - first.timestamp_millis();
    assert!(age_ms.abs() < 60_000, "a fresh heartbeat: {age_ms} ms old");
    assert!(
        fx.engine
            .list_dead_restore_jobs(STALE_AFTER)
            .await
            .unwrap()
            .is_empty(),
        "a running restore is not dead"
    );
    // The heartbeat moves while the copy is held.
    tokio::time::sleep(fx.engine.backup_liveness.heartbeat_interval * 3).await;
    let later = *job_record(&fx.engine, "dst")
        .await
        .get_datetime("heartbeat_at")
        .expect("heartbeat");
    assert!(later > first, "the heartbeat moved: {later:?} > {first:?}");

    gate.add_permits(1);
    assert_fully_restored_with(&fx.engine, "dst", FEW).await;
    let job = job_record(&fx.engine, "dst").await;
    assert_eq!(job.get_str("status"), Ok("DONE"));
    assert_eq!(job.get_i64("owner_generation"), Ok(generation));
    assert!(job.get_datetime("completed_at").is_ok());
    fx.finish().await;
}

/// A restore whose heartbeat fails while its `$out` runs on the server (a
/// fault on the heartbeat's connection, injected at the next tick once the
/// test has seen the `$out` in `currentOp`): the copy stops without touching
/// the target (the record FAILED and the target removed, each conditioned),
/// and the `$out` the server runs to its end leaves no orphan
/// `_data_<table_id>`: the removal dropped the target collection the
/// registration created, and `$out` refuses to replace a target whose
/// indexes changed while it ran. The name is free and the backup no longer
/// in use afterwards. The copy is large enough that `$out` outlasts the
/// probe and several heartbeat periods, and the fault cell records that the
/// heartbeat failed as requested.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_heartbeat_lost_while_out_runs_leaves_no_orphan_collection() {
    const MANY: usize = 150_000;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", MANY).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let backup_key = extenddb_storage::backup_catalog::BackupKey::from_arn(
        &BackupArn::parse(&backup.backup_arn).expect("arn"),
    );
    let fault = Arc::new(std::sync::atomic::AtomicU8::new(HeartbeatFault::IDLE));
    let mut faulty = fx.engine.clone();
    faulty.backup_liveness.heartbeat_interval = Duration::from_millis(50);
    faulty.backup_liveness.heartbeat_fault = Some(fault.clone());
    let request = restore_request(&faulty, &backup.backup_arn, "dst").await;
    let desc = faulty
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore registers");
    let data = data_collection_name(&desc.table_id);

    // The `$out` shows in `currentOp` as an insert into its temporary
    // collection whose command is the aggregate over the backup collection
    // (named by the legacy document's own `backup_id`, not the ARN's).
    let legacy_id = backup_meta(&fx.engine, &backup.backup_arn)
        .await
        .get_str("backup_id")
        .expect("backup_id")
        .to_owned();
    let out_source = super::backup_collection_name(&legacy_id);
    let out_running = || async {
        !fx.engine
            .client
            .database("admin")
            .run_command(doc! {
                "currentOp": 1,
                "$ownOps": false,
                "command.aggregate": &out_source,
                "command.pipeline.$out": { "$exists": true },
            })
            .await
            .expect("currentOp")
            .get_array("inprog")
            .map(|ops| ops.is_empty())
            .unwrap_or(true)
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while !out_running().await {
        assert!(Instant::now() < deadline, "the copy's $out did not start");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    fault.store(HeartbeatFault::ARMED, std::sync::atomic::Ordering::SeqCst);

    // The catalog side: the target is removed once the heartbeat fails, with
    // `$out` still running.
    let deadline = Instant::now() + Duration::from_secs(60);
    while table_doc(&fx.engine, "dst").await.is_some() {
        assert!(
            Instant::now() < deadline,
            "the target was not removed after the heartbeat failed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        fault.load(std::sync::atomic::Ordering::SeqCst),
        HeartbeatFault::INJECTED,
        "the heartbeat failed as requested"
    );
    assert!(
        out_running().await,
        "the window was real: $out was still running when the target was removed"
    );

    // The server side: `$out` ends on its own and leaves no collection.
    let deadline = Instant::now() + Duration::from_secs(60);
    while out_running().await {
        assert!(Instant::now() < deadline, "$out did not end");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !collection_exists(&fx.engine, &data).await,
        "no orphan data collection after $out ended"
    );
    assert!(index_ids(&fx.engine, &desc.table_id).await.is_empty());
    assert!(table_doc(&fx.engine, "dst").await.is_none());
    assert_eq!(fx.engine.backup_in_use(&backup_key).await.unwrap(), None);

    // Liveness: the name restores again (the backup is not in use and the
    // name is free), through to the hand-over; the index backfill of a table
    // this size is the worker's and is not waited for.
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    let again = fx
        .engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore again");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let doc = table_doc(&fx.engine, "dst")
            .await
            .expect("the second target");
        let handed = doc
            .get_document(RESTORE_JOB)
            .map(|job| job.get_str("status") == Ok("DONE"))
            .unwrap_or(false);
        if handed || doc.get_str("table_status") == Ok("ACTIVE") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the second restore did not hand over"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        count(&fx.engine, &data_collection_name(&again.table_id)).await,
        MANY as u64
    );
    fx.finish().await;
}

/// A RUNNING restore record is dead by the age of its heartbeat on the
/// server's clock, whatever the table's age; a stale record is marked FAILED
/// and its target removed through `remove_restore_target`, with its data and
/// index collections, which frees the backup and the name; a fresh record,
/// and a record that is no longer RUNNING, are left alone.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_an_abandoned_restore_is_removed_only_when_its_heartbeat_is_stale() {
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    fx.engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");
    assert_fully_restored_with(&fx.engine, "dst", FEW).await;
    let doc = table_doc(&fx.engine, "dst").await.expect("target");
    let key = target_key(&doc);
    let indexes = index_ids(&fx.engine, &key.table_id).await;
    assert_eq!(indexes.len(), 2);
    let backup_key = extenddb_storage::backup_catalog::BackupKey::from_arn(
        &BackupArn::parse(&backup.backup_arn).expect("arn"),
    );

    // An ACTIVE former target under the right key: no longer a CREATING
    // restore target, so the removal is refused and the table, its data and
    // its index collections survive (the `table_status: CREATING` condition
    // is what excludes the removal from the ACTIVE flip).
    assert!(
        !fx.engine.remove_restore_target(&key).await.expect("remove"),
        "an ACTIVE table under the key is not a CREATING restore target"
    );
    let active = table_doc(&fx.engine, "dst")
        .await
        .expect("the ACTIVE table survives");
    assert_eq!(active.get_str("table_status"), Ok("ACTIVE"));
    assert_eq!(
        count(&fx.engine, &data_collection_name(&key.table_id)).await,
        FEW as u64
    );
    for (index_name, index_id, _) in &indexes {
        assert!(
            collection_exists(&fx.engine, &data_collection_name(index_id)).await,
            "index {index_name} survives"
        );
    }

    // A fresh heartbeat: a live restore, here or on another instance.
    age_running_record(&fx.engine, "dst", Duration::from_secs(1)).await;
    assert!(
        fx.engine
            .list_dead_restore_jobs(STALE_AFTER)
            .await
            .expect("list")
            .is_empty()
    );
    assert_eq!(
        fx.engine
            .backup_in_use(&backup_key)
            .await
            .unwrap()
            .as_deref(),
        Some("dst"),
        "a CREATING target keeps its backup in use"
    );
    let in_use = fx.engine.delete(ACCOUNT, &backup.backup_arn).await;
    assert!(
        matches!(in_use, Err(StorageError::BackupInUse(_))),
        "{in_use:?}"
    );

    // A stale heartbeat: dead.
    age_running_record(&fx.engine, "dst", Duration::from_secs(600)).await;
    let dead = fx
        .engine
        .list_dead_restore_jobs(STALE_AFTER)
        .await
        .expect("list");
    assert_eq!(dead.len(), 1, "{dead:?}");
    assert_eq!(dead[0].target, key);
    assert_eq!(dead[0].kind, RestoreKind::Legacy);
    assert_eq!(dead[0].status, JobStatus::Running);
    assert_eq!(dead[0].source_backup_arn, backup.backup_arn);
    assert!(
        fx.engine
            .fail_restore_job(
                &key,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
            .await
            .expect("fail")
    );
    let job = job_record(&fx.engine, "dst").await;
    assert_eq!(job.get_str("status"), Ok("FAILED"));
    assert_eq!(job.get_str("failure_reason"), Ok("owner dead"));
    assert!(job.get_datetime("completed_at").is_ok());
    assert!(
        !fx.engine
            .fail_restore_job(
                &key,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
            .await
            .expect("fail again"),
        "a record that is no longer RUNNING is not written twice"
    );
    assert!(
        fx.engine
            .list_dead_restore_jobs(STALE_AFTER)
            .await
            .unwrap()
            .is_empty(),
        "a FAILED record is not dead"
    );
    // A key with another table id names a table re-created under the name,
    // not this target.
    let other_id = RestoreTargetKey {
        table_id: "another-table".to_owned(),
        ..key.clone()
    };
    assert!(!fx.engine.remove_restore_target(&other_id).await.unwrap());
    // The removal with the reply to its collection drops lost: the document
    // is gone, so the target is removed (`true`), the name and the backup
    // are free and no record is left for a later cycle; the collections,
    // dropped before the reply was lost, are gone too.
    let mut drops_fail = fx.engine.clone();
    drops_fail.backup_liveness.target_drop_fails = true;
    assert!(
        drops_fail
            .remove_restore_target(&key)
            .await
            .expect("the target is removed although the reply to its collection drops was lost")
    );
    assert!(table_doc(&fx.engine, "dst").await.is_none());
    assert!(!collection_exists(&fx.engine, &data_collection_name(&key.table_id)).await);
    for (_, index_id, _) in &indexes {
        assert!(!collection_exists(&fx.engine, &data_collection_name(index_id)).await);
    }
    assert!(index_ids(&fx.engine, &key.table_id).await.is_empty());
    assert!(
        !fx.engine.remove_restore_target(&key).await.unwrap(),
        "a target that is gone is not removed twice"
    );

    // The backup and the name are free again.
    assert_eq!(fx.engine.backup_in_use(&backup_key).await.unwrap(), None);
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    fx.engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore into the freed name");
    assert_fully_restored_with(&fx.engine, "dst", FEW).await;
    let deleted = fx
        .engine
        .delete(ACCOUNT, &backup.backup_arn)
        .await
        .expect("DeleteBackup once the target is ACTIVE");
    assert_eq!(deleted.backup_details.backup_status, "DELETED");
    fx.finish().await;
}

/// The FAILED-target duty's listing: a FAILED record whose table is still a
/// CREATING restore target (the FAILED write landed and the removal did not)
/// is not listed while the record is fresh (the task that failed it may
/// still be removing the target), is listed once `completed_at` is older
/// than the stale window, as FAILED, and its target is then removed with no
/// second FAILED write; a FAILED record without a `completed_at` is aged by
/// its heartbeat; a FAILED record whose table is DELETING or ACTIVE is never
/// listed.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_failed_record_whose_target_removal_failed_is_listed_once_stale_and_removed() {
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    fx.engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");
    assert_fully_restored_with(&fx.engine, "dst", FEW).await;
    let key = target_key(&table_doc(&fx.engine, "dst").await.expect("target"));
    let server_ago = |age: Duration| {
        bson::DateTime::from_millis(
            bson::DateTime::now().timestamp_millis() - i64::try_from(age.as_millis()).expect("i64"),
        )
    };
    let set = |update: Document| {
        let tables = fx.engine.catalog_db.collection::<Document>("tables");
        async move {
            tables
                .update_one(
                    doc! { "_id": { "account_id": ACCOUNT, "table_name": "dst" } },
                    update,
                )
                .await
                .expect("set the record");
        }
    };
    let listed = || async {
        fx.engine
            .list_dead_restore_jobs(STALE_AFTER)
            .await
            .expect("list")
    };

    // The state a dead restore leaves once the reconciler failed its record
    // and the target's removal did not land.
    age_running_record(&fx.engine, "dst", Duration::from_secs(600)).await;
    let dead = listed().await;
    assert_eq!(dead.len(), 1, "{dead:?}");
    assert_eq!(dead[0].status, JobStatus::Running);
    assert!(
        fx.engine
            .fail_restore_job(
                &key,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
            .await
            .expect("fail")
    );
    let job = job_record(&fx.engine, "dst").await;
    assert_eq!(job.get_str("status"), Ok("FAILED"));
    assert!(job.get_datetime("completed_at").is_ok());
    assert_eq!(
        table_doc(&fx.engine, "dst")
            .await
            .expect("target")
            .get_str("table_status"),
        Ok("CREATING")
    );

    // Fresh: `fail_restore_job` stamped `completed_at` with the server's
    // clock a moment ago, and whoever failed it may still be removing the
    // target.
    assert!(
        listed().await.is_empty(),
        "a FAILED record within the stale window is left to its writer"
    );
    // Stale: listed, as FAILED, for the FAILED-target duty.
    set(doc! { "$set": { "restore_job.completed_at": server_ago(Duration::from_secs(600)) } })
        .await;
    let dead = listed().await;
    assert_eq!(dead.len(), 1, "{dead:?}");
    assert_eq!(dead[0].target, key);
    assert_eq!(dead[0].status, JobStatus::Failed);
    assert_eq!(dead[0].kind, RestoreKind::Legacy);
    assert_eq!(dead[0].source_backup_arn, backup.backup_arn);
    // A FAILED record without `completed_at` is aged by its heartbeat: fresh,
    // not listed; stale, listed.
    set(doc! {
        "$set": { RESTORE_JOB_HEARTBEAT: bson::DateTime::now() },
        "$unset": { "restore_job.completed_at": "" },
    })
    .await;
    assert!(
        listed().await.is_empty(),
        "a FAILED record with a fresh heartbeat and no completed_at is fresh"
    );
    set(doc! { "$set": { RESTORE_JOB_HEARTBEAT: server_ago(Duration::from_secs(600)) } }).await;
    assert_eq!(listed().await.len(), 1);
    // A FAILED record whose table the removal already moved to DELETING is
    // the control plane's row.
    set(doc! { "$set": { "table_status": "DELETING" } }).await;
    assert!(
        listed().await.is_empty(),
        "a DELETING table is not a target"
    );
    set(doc! { "$set": { "table_status": "CREATING" } }).await;

    // The reconciler's FAILED-target duty: no second FAILED write, the target
    // removed with its collections.
    assert!(
        !fx.engine
            .fail_restore_job(
                &key,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
            .await
            .expect("fail again"),
        "a FAILED record is not RUNNING and is not written FAILED again"
    );
    assert!(fx.engine.remove_restore_target(&key).await.expect("remove"));
    assert!(table_doc(&fx.engine, "dst").await.is_none());
    assert!(!collection_exists(&fx.engine, &data_collection_name(&key.table_id)).await);
    assert!(listed().await.is_empty());
    assert_eq!(
        fx.engine
            .backup_in_use(&extenddb_storage::backup_catalog::BackupKey::from_arn(
                &BackupArn::parse(&backup.backup_arn).expect("arn"),
            ))
            .await
            .unwrap(),
        None,
        "the backup is free once the target is gone"
    );

    // A FAILED record whose table is ACTIVE (a flip that won against a late
    // FAILED write) is nobody's duty.
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    fx.engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore again");
    assert_fully_restored_with(&fx.engine, "dst", FEW).await;
    set(doc! { "$set": {
        RESTORE_JOB_STATUS: "FAILED",
        "restore_job.completed_at": server_ago(Duration::from_secs(600)),
    } })
    .await;
    assert!(
        listed().await.is_empty(),
        "a FAILED record of an ACTIVE table is not listed"
    );
    fx.finish().await;
}

/// A restore target of another, live, instance on the same catalog is not
/// dead for many stale windows, because its heartbeat keeps moving, while a
/// record whose owner stopped is; DeleteBackup of the live one's source is
/// refused; the live restore then finishes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_restore_of_another_instance_is_not_dead_while_it_heartbeats() {
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    let b1 = fx
        .engine
        .create(ACCOUNT, "src", "b1", JobPermit::detached())
        .await
        .expect("backup 1");
    let b2 = fx
        .engine
        .create(ACCOUNT, "src", "b2", JobPermit::detached())
        .await
        .expect("backup 2");
    // A restore whose owner stopped: finished, then aged into a dead record.
    let request = restore_request(&fx.engine, &b1.backup_arn, "stopped").await;
    fx.engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");
    assert_fully_restored_with(&fx.engine, "stopped", FEW).await;
    age_running_record(&fx.engine, "stopped", Duration::from_secs(60)).await;

    // A live restore of another instance, held before its copy.
    let other = fx.second_instance(test_liveness()).await;
    let stale_after = other.backup_liveness.heartbeat_interval * 6;
    let (gated_other, gate) = gated(&other);
    let request = restore_request(&other, &b2.backup_arn, "dst").await;
    gated_other
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");
    let first = *job_record(&fx.engine, "dst")
        .await
        .get_datetime("heartbeat_at")
        .expect("heartbeat");

    // Across three stale windows, only the stopped one is dead.
    let until = Instant::now() + stale_after * 3;
    let mut checks = 0;
    while Instant::now() < until {
        let dead = fx
            .engine
            .list_dead_restore_jobs(stale_after)
            .await
            .expect("list");
        assert_eq!(
            dead.iter()
                .map(|j| j.target.table_name.as_str())
                .collect::<Vec<_>>(),
            ["stopped"],
            "only the record whose owner stopped is dead"
        );
        checks += 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(checks >= 10);
    // Liveness: the live heartbeat moved.
    let later = *job_record(&fx.engine, "dst")
        .await
        .get_datetime("heartbeat_at")
        .expect("heartbeat");
    assert!(later > first, "{later:?} > {first:?}");
    let in_use = fx.engine.delete(ACCOUNT, &b2.backup_arn).await;
    assert!(
        matches!(in_use, Err(StorageError::BackupInUse(_))),
        "{in_use:?}"
    );

    gate.add_permits(1);
    assert_fully_restored_with(&fx.engine, "dst", FEW).await;
    fx.finish().await;
}

/// After a DeleteBackup stopped after its drop: the backup is DELETING, so
/// it is neither described, listed nor restorable, and a retry of
/// DeleteBackup resumes the drop and answers DELETED.
async fn assert_deleting_then_resumed(engine: &MongoEngine, backup_arn: &str, backup_id: &str) {
    let meta = backup_meta(engine, backup_arn).await;
    assert_eq!(
        meta.get_str("backup_status"),
        Ok("DELETING"),
        "the backup must not read AVAILABLE once the drop may have run: {meta:?}"
    );
    assert!(!collection_exists(engine, &format!("_backup_{backup_id}")).await);
    let described = engine.describe(ACCOUNT, backup_arn).await;
    assert!(
        matches!(&described, Err(StorageError::Validation(m)) if m.starts_with("Backup not found")),
        "{described:?}"
    );
    assert!(engine.list(ACCOUNT, None).await.expect("list").is_empty());
    let source = engine.restore_source(ACCOUNT, backup_arn).await;
    assert!(
        matches!(&source, Err(StorageError::Validation(m)) if m.starts_with("Backup not found")),
        "a restore must not start from a backup whose data was dropped: {source:?}"
    );
    assert!(table_doc(engine, "dst").await.is_none());
    let resumed = engine
        .delete(ACCOUNT, backup_arn)
        .await
        .expect("a retry resumes the delete");
    assert_eq!(resumed.backup_details.backup_status, "DELETED");
    assert_eq!(
        backup_meta(engine, backup_arn)
            .await
            .get_str("backup_status"),
        Ok("DELETED")
    );
    let again = engine.delete(ACCOUNT, backup_arn).await;
    assert!(
        matches!(&again, Err(StorageError::Validation(m)) if m.starts_with("Backup not found")),
        "{again:?}"
    );
}

/// The reply to DeleteBackup's drop is an error after the server dropped
/// the collection. The request fails, but the backup stays DELETING until a
/// retry finishes the delete.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_delete_whose_drop_reply_fails_leaves_the_backup_deleting_until_a_retry() {
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", 300).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let backup_id = backup_meta(&fx.engine, &backup.backup_arn)
        .await
        .get_str("backup_id")
        .expect("backup_id")
        .to_owned();
    let faulty = with_drop_fault(&fx.engine, AfterBackupDrop::Fail);
    let failed = faulty.delete(ACCOUNT, &backup.backup_arn).await;
    assert!(
        matches!(&failed, Err(StorageError::Internal(_))),
        "{failed:?}"
    );
    assert_deleting_then_resumed(&fx.engine, &backup.backup_arn, &backup_id).await;
    fx.finish().await;
}

/// The DeleteBackup future is dropped (a client timeout) while it waits
/// for the reply to a drop the server already ran.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_delete_dropped_while_waiting_for_its_drop_leaves_the_backup_deleting() {
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", 300).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let backup_id = backup_meta(&fx.engine, &backup.backup_arn)
        .await
        .get_str("backup_id")
        .expect("backup_id")
        .to_owned();
    let faulty = with_drop_fault(&fx.engine, AfterBackupDrop::Hang);
    let timed_out = tokio::time::timeout(
        Duration::from_secs(3),
        faulty.delete(ACCOUNT, &backup.backup_arn),
    )
    .await;
    assert!(timed_out.is_err(), "the delete must hang after its drop");
    assert_deleting_then_resumed(&fx.engine, &backup.backup_arn, &backup_id).await;
    fx.finish().await;
}

/// A restore from a process whose clock is 10 minutes behind the server's
/// is not dead to another process (stale window 5 minutes): the record's
/// first heartbeat, written with the local clock at the insert, is replaced
/// by the server's clock at once, not one heartbeat interval later.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_restore_from_a_process_with_a_slow_clock_is_not_dead() {
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let liveness = BackupLiveness {
        heartbeat_interval: Duration::from_secs(3_600),
        registration_gate: None,
        copy_gate: None,
        after_backup_drop: None,
        target_drop_fails: false,
        local_clock_behind: Duration::from_secs(600),
        heartbeat_fault: None,
        replacement_commit_unknown: false,
    };
    let behind = fx.second_instance(liveness.clone()).await;
    let (behind, gate) = gated(&behind);
    let other = fx
        .second_instance(BackupLiveness {
            local_clock_behind: Duration::ZERO,
            ..liveness
        })
        .await;
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    behind
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let beat = *job_record(&fx.engine, "dst")
            .await
            .get_datetime("heartbeat_at")
            .expect("heartbeat");
        let age_ms = bson::DateTime::now().timestamp_millis() - beat.timestamp_millis();
        if age_ms.abs() < 60_000 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the record's heartbeat is {age_ms} ms old: the server clock must replace the \
             local one at once"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        other
            .list_dead_restore_jobs(STALE_AFTER)
            .await
            .expect("list")
            .is_empty()
    );
    assert!(table_doc(&fx.engine, "dst").await.is_some());

    gate.add_permits(1);
    assert_fully_restored_with(&fx.engine, "dst", FEW).await;
    fx.finish().await;
}

/// A DeleteBackup and restores of one backup order wholly one before the
/// other: when the delete goes first, every restore is "Backup not found"
/// and no target is left; when a registration goes first, exactly one target
/// exists, the delete is BackupInUse, and so is every other restore. Even
/// rounds hold the three registrations behind a gate until the delete has
/// answered, so the delete wins; odd rounds release the registrations and
/// start the delete at the same moment, a race over the backup's lock that
/// the registrations have won on every run so far. Both orders must occur.
/// The restores are accounted by the
/// catalog (the targets that exist for the request names), not by what their
/// requests answered: a request that outlives the registration bound answers
/// an error while its target appears, and the copy of every registered
/// target is released and driven to ACTIVE before the backup is deleted.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_delete_and_concurrent_restores_of_one_backup_serialize() {
    const FEW: usize = 20;
    const ROUNDS: usize = 6;
    const RESTORES: usize = 3;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    // A backup that stays, so a restore request of a deleted backup can be
    // built after the delete (the request is built from an AVAILABLE source).
    let keep = fx
        .engine
        .create(ACCOUNT, "src", "keep", JobPermit::detached())
        .await
        .expect("backup");
    let b_arn_for_request = keep.backup_arn.clone();
    let (mut delete_won, mut registration_won) = (0, 0);
    for round in 0..ROUNDS {
        let backup = fx
            .engine
            .create(ACCOUNT, "src", &format!("b{round}"), JobPermit::detached())
            .await
            .expect("backup");
        let targets: Vec<String> = (0..RESTORES).map(|i| format!("r{round}_{i}")).collect();
        let mut requests = Vec::new();
        for target in &targets {
            requests.push(restore_request(&fx.engine, &backup.backup_arn, target).await);
        }
        let (gated, registration_gate, copy_gate) = gated_at_registration(&fx.engine);
        let delete_first = round % 2 == 0;
        let restores = futures::future::join_all(
            requests
                .into_iter()
                .map(|request| gated.restore(ACCOUNT, request, JobPermit::detached())),
        );
        let delete = async {
            if delete_first {
                let outcome = fx.engine.delete(ACCOUNT, &backup.backup_arn).await;
                registration_gate.add_permits(RESTORES);
                outcome
            } else {
                registration_gate.add_permits(RESTORES);
                fx.engine.delete(ACCOUNT, &backup.backup_arn).await
            }
        };
        let (restores, delete) = tokio::join!(restores, delete);
        // The catalog's account of the race, which every answer agrees with
        // where the request saw one.
        let registered = registered_targets(&fx.engine, &targets).await;
        for (target, result) in targets.iter().zip(&restores) {
            match result {
                Ok(_) => assert!(
                    registered.contains(target),
                    "round {round}: {target} was accepted but has no target"
                ),
                Err(StorageError::BackupInUse(_)) => assert!(
                    !registered.contains(target),
                    "round {round}: {target} was refused but has a target"
                ),
                Err(StorageError::Validation(m)) if m.starts_with("Backup not found") => {
                    assert!(
                        !registered.contains(target),
                        "round {round}: {target} was refused but has a target"
                    );
                }
                Err(StorageError::Internal(m)) if m.contains("was not registered within") => {
                    eprintln!(
                        "round {round}: {target}: the request outlived the registration bound"
                    );
                }
                Err(other) => panic!("round {round}: {target}: {other}"),
            }
        }
        match delete {
            Ok(desc) => {
                delete_won += 1;
                assert_eq!(desc.backup_details.backup_status, "DELETED");
                assert!(
                    registered.is_empty(),
                    "round {round}: a target was registered after the delete won: \
                     {registered:?} {restores:?}"
                );
                for (target, result) in targets.iter().zip(&restores) {
                    assert!(
                        matches!(result, Err(StorageError::Validation(m)) if m.starts_with("Backup not found")),
                        "round {round}: {target}: {result:?}"
                    );
                }
            }
            Err(StorageError::BackupInUse(_)) => {
                registration_won += 1;
                assert_eq!(
                    registered.len(),
                    1,
                    "round {round}: exactly one restore registers: {registered:?} {restores:?}"
                );
                for (target, result) in targets.iter().zip(&restores) {
                    assert!(
                        !matches!(result, Err(StorageError::Validation(_))),
                        "round {round}: {target}: a registration won, so no restore saw the \
                         backup gone: {result:?}"
                    );
                }
                copy_gate.add_permits(registered.len());
                for target in &registered {
                    assert_fully_restored_with(&fx.engine, target, FEW).await;
                }
                fx.engine
                    .delete(ACCOUNT, &backup.backup_arn)
                    .await
                    .expect("DeleteBackup once every target is ACTIVE");
            }
            Err(other) => panic!("round {round}: the delete failed: {other}"),
        }
        assert_eq!(
            backup_meta(&fx.engine, &backup.backup_arn)
                .await
                .get_str("backup_status"),
            Ok("DELETED")
        );
        // The registration's own check, whichever order the race took: a
        // deleted backup is not there to restore from, and nothing is created.
        let late = format!("r{round}_late");
        let mut request = restore_request(&fx.engine, &b_arn_for_request, &late).await;
        request.backup_arn = backup.backup_arn.clone();
        let refused = fx
            .engine
            .restore(ACCOUNT, request, JobPermit::detached())
            .await;
        assert!(
            matches!(&refused, Err(StorageError::Validation(m)) if m.starts_with("Backup not found")),
            "round {round}: {refused:?}"
        );
        assert!(table_doc(&fx.engine, &late).await.is_none());
    }
    eprintln!(
        "the delete won {delete_won} and a registration won {registration_won} of {ROUNDS} rounds"
    );
    assert!(
        delete_won >= 1 && registration_won >= 1,
        "both orders occur: the delete won {delete_won}, a registration won {registration_won}"
    );
    fx.finish().await;
}

/// (a) The response is CREATING while the copy has not run; the target then
/// reaches ACTIVE with every item and index entry; the summary carries the
/// source table ARN and the backup's creation time.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_restore_returns_creating_and_the_detached_copy_finishes() {
    let fx = Fixture::new().await;
    let source_arn = source_table(&fx.engine, "src").await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    let (gated, gate) = gated(&fx.engine);

    let desc = tokio::time::timeout(
        Duration::from_secs(20),
        gated.restore(ACCOUNT, request, JobPermit::detached()),
    )
    .await
    .expect("the restore must return while its copy is held")
    .expect("restore");
    assert_eq!(desc.table_status, TableStatus::Creating);
    let doc = table_doc(&fx.engine, "dst").await.expect("target");
    assert_eq!(doc.get_str("table_status"), Ok("CREATING"));
    assert_eq!(
        count(&fx.engine, &data_collection_name(&desc.table_id)).await,
        0,
        "no item may be copied before the response"
    );
    let summary = fx
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "dst".to_owned(),
            },
        )
        .await
        .expect("describe")
        .restore_summary
        .expect("restore summary while CREATING");
    assert_eq!(
        summary.source_backup_arn.as_deref(),
        Some(backup.backup_arn.as_str())
    );
    assert_eq!(
        summary.source_table_arn.as_deref(),
        Some(source_arn.as_str())
    );
    assert_eq!(summary.restore_date_time, backup.backup_creation_date_time);
    assert!(summary.restore_in_progress);
    // The backup time is the shared id's instant, kept to the millisecond.
    assert_eq!(
        Some(backup.backup_creation_date_time),
        extenddb_storage::util::backup_creation_date_time(&backup.backup_arn)
    );

    gate.add_permits(1);
    assert_fully_restored(&fx.engine, "dst").await;
    fx.finish().await;
}

/// (b) A request future dropped after its first poll still produces a
/// complete restore.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_dropped_restore_request_still_reaches_active() {
    let fx = Fixture::new().await;
    source_table(&fx.engine, "src").await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    let mut future = fx.engine.restore(ACCOUNT, request, JobPermit::detached());
    assert!(
        (&mut future).now_or_never().is_none(),
        "the first poll must not complete the restore"
    );
    drop(future);
    assert_fully_restored(&fx.engine, "dst").await;
    fx.finish().await;
}

/// (e) While the copy runs, DeleteBackup of the source is BackupInUse and
/// DeleteTable of the target is refused; both succeed once it is ACTIVE.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_backup_and_target_are_protected_while_the_copy_runs() {
    let fx = Fixture::new().await;
    source_table(&fx.engine, "src").await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    let (gated, gate) = gated(&fx.engine);
    gated
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");

    let in_use = fx.engine.delete(ACCOUNT, &backup.backup_arn).await;
    assert!(
        matches!(in_use, Err(StorageError::BackupInUse(_))),
        "the source backup is in use while the copy runs: {in_use:?}"
    );
    let refused = fx
        .engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: "dst".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(&refused, Err(StorageError::IndexesInUse(m)) if m.contains("being restored")),
        "DeleteTable of a target being restored is refused: {refused:?}"
    );
    assert_eq!(
        table_doc(&fx.engine, "dst")
            .await
            .expect("target")
            .get_str("table_status"),
        Ok("CREATING"),
        "both calls ran inside the copy window"
    );

    gate.add_permits(1);
    assert_fully_restored(&fx.engine, "dst").await;
    fx.engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: "dst".to_owned(),
            },
        )
        .await
        .expect("DeleteTable after ACTIVE");
    let deleted = fx
        .engine
        .delete(ACCOUNT, &backup.backup_arn)
        .await
        .expect("DeleteBackup after ACTIVE");
    assert_eq!(deleted.backup_details.backup_status, "DELETED");
    fx.finish().await;
}

/// The definition captured at CreateBackup carries TTL and the stream
/// specification, and ListBackups reports the source table id.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_create_backup_captures_ttl_stream_and_table_id() {
    use extenddb_storage::MetadataEngine;
    let fx = Fixture::new().await;
    source_table(&fx.engine, "src").await;
    fx.engine
        .update_ttl(ACCOUNT, "src", "expires", true)
        .await
        .expect("ttl");
    let table = table_doc(&fx.engine, "src").await.expect("src");
    let table_id = table.get_str("table_id").expect("id").to_owned();
    let mut backups = Vec::new();
    for i in 0..3 {
        backups.push(
            fx.engine
                .create(ACCOUNT, "src", &format!("b{i}"), JobPermit::detached())
                .await
                .expect("backup"),
        );
    }
    for b in &backups {
        let source = fx
            .engine
            .restore_source(ACCOUNT, &b.backup_arn)
            .await
            .expect("source");
        assert_eq!(
            Some(source.backup_creation_date_time),
            extenddb_storage::util::backup_creation_date_time(&b.backup_arn)
        );
        assert_eq!(
            source.backup_creation_date_time,
            b.backup_creation_date_time
        );
        let def = source.definition.expect("definition");
        let ttl = def.time_to_live.expect("ttl captured");
        assert_eq!(ttl.attribute_name.as_deref(), Some("expires"));
        assert!(def.stream_specification.is_none());
        assert_eq!(def.global_secondary_indexes.len(), 1);
        assert_eq!(def.local_secondary_indexes.len(), 1);
        let described = fx
            .engine
            .describe(ACCOUNT, &b.backup_arn)
            .await
            .expect("describe");
        assert_eq!(
            described.backup_details.backup_creation_date_time,
            b.backup_creation_date_time
        );
        assert!(described.source_table_feature_details.is_none());
    }
    let listed = fx.engine.list(ACCOUNT, None).await.expect("list");
    assert_eq!(listed.len(), 3);
    for summary in &listed {
        assert_eq!(summary.table_id, table_id);
        assert_eq!(
            Some(summary.backup_creation_date_time),
            extenddb_storage::util::backup_creation_date_time(&summary.backup_arn)
        );
    }
    let foreign = fx
        .engine
        .restore_source("999999999999", &backups[0].backup_arn)
        .await;
    assert!(
        matches!(&foreign, Err(StorageError::Validation(m)) if m.starts_with("Backup not found")),
        "{foreign:?}"
    );
    fx.finish().await;
}

async fn table_doc_for(engine: &MongoEngine, account: &str, name: &str) -> Option<Document> {
    tables_majority(engine)
        .find_one(doc! { "_id": { "account_id": account, "table_name": name } })
        .await
        .expect("read table")
}

async fn wait_for_status_for(engine: &MongoEngine, account: &str, name: &str, status: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Some(doc) = table_doc_for(engine, account, name).await
            && doc.get_str("table_status") == Ok(status)
        {
            return;
        }
        // The account id is kept out of the message: the scanner reads an
        // interpolated account as a logged credential.
        let current = table_doc_for(engine, account, name)
            .await
            .and_then(|doc| doc.get_str("table_status").ok().map(str::to_owned));
        assert!(
            Instant::now() < deadline,
            "table {name} did not reach {status}: {current:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// One restore of a backup at a time. Four restores of one backup
/// started at the same moment: exactly one is accepted and the other three
/// are `BackupInUse`; a restore started while the accepted one is still
/// CREATING (copy held) is `BackupInUse` as well; once the target is ACTIVE a
/// new restore of the backup is accepted. Every refusal names the target the
/// backup is being restored into.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_second_restore_of_a_backup_while_one_is_creating_is_backup_in_use() {
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let (gated, gate) = gated(&fx.engine);
    let mut requests = Vec::new();
    for i in 0..4 {
        requests.push(restore_request(&fx.engine, &backup.backup_arn, &format!("dst{i}")).await);
    }
    let results = futures::future::join_all(
        requests
            .into_iter()
            .map(|request| gated.restore(ACCOUNT, request, JobPermit::detached())),
    )
    .await;
    let accepted: Vec<usize> = results
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.is_ok().then_some(i))
        .collect();
    assert_eq!(
        accepted.len(),
        1,
        "exactly one restore is accepted: {results:?}"
    );
    for (i, result) in results.iter().enumerate() {
        if i != accepted[0] {
            match result {
                Err(StorageError::BackupInUse(m)) => assert_eq!(
                    *m,
                    format!(
                        "Backup is being used to restore another table: {}",
                        backup.backup_arn
                    ),
                    "restore {i}: the service's text, naming the backup and not the target"
                ),
                other => panic!("restore {i} must be BackupInUse: {other:?}"),
            }
        }
    }
    let winner = format!("dst{}", accepted[0]);
    assert_eq!(
        table_doc(&fx.engine, &winner)
            .await
            .expect("target")
            .get_str("table_status"),
        Ok("CREATING")
    );
    // Sequential, while the winner's copy is held: still in use, and the
    // refusal names the target with the wording the other backends use.
    let request = restore_request(&fx.engine, &backup.backup_arn, "late").await;
    match fx
        .engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
    {
        Err(StorageError::BackupInUse(m)) => assert_eq!(
            m,
            format!(
                "Backup is being used to restore another table: {}",
                backup.backup_arn
            )
        ),
        other => panic!("a restore during the CREATING window is BackupInUse: {other:?}"),
    }
    assert!(table_doc(&fx.engine, "late").await.is_none());

    gate.add_permits(1);
    assert_fully_restored_with(&fx.engine, &winner, FEW).await;
    let request = restore_request(&fx.engine, &backup.backup_arn, "late").await;
    fx.engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("a restore after the first is ACTIVE is accepted");
    assert_fully_restored_with(&fx.engine, "late", FEW).await;
    fx.finish().await;
}

/// A restore into a name that is a CREATING restore target is
/// `TableBeingRestored`, naming the target and the backup it is being restored
/// from; the same name taken by an ACTIVE table is `TableAlreadyExists`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_restore_into_a_creating_restore_target_is_table_being_restored() {
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    let b1 = fx
        .engine
        .create(ACCOUNT, "src", "b1", JobPermit::detached())
        .await
        .expect("backup 1");
    let b2 = fx
        .engine
        .create(ACCOUNT, "src", "b2", JobPermit::detached())
        .await
        .expect("backup 2");
    let (gated, gate) = gated(&fx.engine);
    let request = restore_request(&fx.engine, &b1.backup_arn, "dst").await;
    gated
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("first restore");
    let request = restore_request(&fx.engine, &b2.backup_arn, "dst").await;
    match fx
        .engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
    {
        Err(StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        }) => {
            assert_eq!(table_name, "dst");
            assert_eq!(backup_arn, b1.backup_arn);
        }
        other => panic!("restore into a CREATING restore target: {other:?}"),
    }
    // What the engine's own name check reads before its job permit: the
    // target describes as CREATING with its source backup in the
    // RestoreSummary, which is what it answers TableInUse with.
    let described = fx
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "dst".to_owned(),
            },
        )
        .await
        .expect("describe the CREATING target");
    assert_eq!(described.table_status, TableStatus::Creating);
    assert_eq!(
        described
            .restore_summary
            .as_ref()
            .and_then(|r| r.source_backup_arn.as_deref()),
        Some(b1.backup_arn.as_str()),
        "the CREATING target names its source backup"
    );
    // The same backup into the same name: the name is what is refused, not
    // the backup (the service answers TableInUseException).
    let request = restore_request(&fx.engine, &b1.backup_arn, "dst").await;
    match fx
        .engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
    {
        Err(StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        }) => {
            assert_eq!(table_name, "dst");
            assert_eq!(backup_arn, b1.backup_arn);
        }
        other => panic!("the same backup into its CREATING target: {other:?}"),
    }
    // The name is checked before the backup: a backup that does not exist,
    // restored into that name, is refused as the name too.
    let mut missing = restore_request(&fx.engine, &b2.backup_arn, "dst").await;
    missing.backup_arn = flip_last_hex(&b2.backup_arn);
    match fx
        .engine
        .restore(ACCOUNT, missing, JobPermit::detached())
        .await
    {
        Err(StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        }) => {
            assert_eq!(table_name, "dst");
            assert_eq!(backup_arn, b1.backup_arn);
        }
        other => panic!("a missing backup into a CREATING restore target: {other:?}"),
    }
    gate.add_permits(1);
    assert_fully_restored_with(&fx.engine, "dst", FEW).await;
    let request = restore_request(&fx.engine, &b2.backup_arn, "dst").await;
    assert!(
        matches!(
            fx.engine
                .restore(ACCOUNT, request, JobPermit::detached())
                .await,
            Err(StorageError::TableAlreadyExists(_))
        ),
        "an ACTIVE table of that name is TableAlreadyExists"
    );
    // The former target describes as ACTIVE, so the engine's name check
    // answers TableAlreadyExists for it too.
    let former = fx
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "dst".to_owned(),
            },
        )
        .await
        .expect("describe the former target");
    assert_eq!(former.table_status, TableStatus::Active);
    fx.finish().await;
}

/// Twenty restores of twenty distinct backups (twenty accounts, so no shared
/// lock serializes their registration) against the driver's default pool of
/// 10 all register, the engine answers a catalog read right after, and every
/// target reaches ACTIVE. Before the fix, the registration's hand-over left
/// each creation's in-flight command holding a pooled connection while it
/// waited for another, and this wedged the process for good.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_twenty_concurrent_restores_of_distinct_backups_do_not_wedge_the_pool() {
    const N: usize = 20;
    const FEW: usize = 20;
    let fx = Fixture::new().await;
    let accounts: Vec<String> = (0..N).map(|i| format!("9100000000{i:02}")).collect();
    let mut requests = Vec::new();
    for account in &accounts {
        source_table_for(&fx.engine, account, "src", FEW).await;
        let backup = fx
            .engine
            .create(account, "src", "b", JobPermit::detached())
            .await
            .expect("backup");
        requests.push((
            account.clone(),
            restore_request_for(&fx.engine, account, &backup.backup_arn, "dst").await,
        ));
    }
    let started = Instant::now();
    let results = futures::future::join_all(requests.into_iter().map(|(account, request)| {
        let engine = fx.engine.clone();
        async move {
            tokio::time::timeout(
                Duration::from_secs(60),
                engine.restore(&account, request, JobPermit::detached()),
            )
            .await
        }
    }))
    .await;
    let elapsed = started.elapsed();
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("restore {i} failed: {e}"),
            Err(_) => panic!("restore {i} did not return within 60 s: the pool is wedged"),
        }
    }
    eprintln!("{N} concurrent restores of distinct backups registered in {elapsed:?}");
    // Liveness: the engine still answers a catalog read, promptly.
    let probe = tokio::time::timeout(
        Duration::from_secs(10),
        fx.engine.describe_table(
            &accounts[0],
            DescribeTableInput {
                table_name: "src".to_owned(),
            },
        ),
    )
    .await
    .expect("a catalog read after the restores returns within 10 s")
    .expect("describe");
    assert_eq!(probe.table_status, TableStatus::Active);
    for account in &accounts {
        wait_for_status_for(&fx.engine, account, "dst", "ACTIVE").await;
    }
    fx.finish().await;
}

/// UpdateTable on a CREATING restore target is `TableNotActive`, for
/// every kind of change (billing mode, deletion protection, a stream, a GSI),
/// and leaves the target as the backup describes; once ACTIVE the same
/// changes are accepted. A table that does not exist is `TableNotFound`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_update_table_on_a_creating_restore_target_is_refused() {
    use extenddb_core::types::{
        CreateGsiAction, GlobalSecondaryIndexUpdate, ProvisionedThroughput, StreamSpecification,
        StreamViewType, UpdateTableInput,
    };
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let (gated, gate) = gated(&fx.engine);
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    gated
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");
    let before = table_doc(&fx.engine, "dst").await.expect("target");
    assert_eq!(before.get_str("table_status"), Ok("CREATING"));

    let blank = UpdateTableInput {
        table_name: "dst".to_owned(),
        billing_mode: None,
        table_throughput_mode: None,
        provisioned_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: None,
        attribute_definitions: None,
        stream_specification: None,
        table_class: None,
        on_demand_throughput: None,
        vector_index_updates: None,
    };
    let changes = vec![
        UpdateTableInput {
            billing_mode: Some(BillingMode::Provisioned),
            provisioned_throughput: Some(ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 5,
            }),
            ..blank.clone()
        },
        UpdateTableInput {
            deletion_protection_enabled: Some(true),
            ..blank.clone()
        },
        UpdateTableInput {
            stream_specification: Some(StreamSpecification {
                stream_enabled: true,
                stream_view_type: Some(StreamViewType::NewAndOldImages),
            }),
            ..blank.clone()
        },
        UpdateTableInput {
            attribute_definitions: Some(vec![attr("gpk", ScalarAttributeType::S)]),
            global_secondary_index_updates: Some(vec![GlobalSecondaryIndexUpdate {
                create: Some(CreateGsiAction {
                    index_name: "g2".to_owned(),
                    key_schema: vec![key("gpk", KeyType::Hash)],
                    projection: Projection {
                        projection_type: ProjectionType::KeysOnly,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                }),
                update: None,
                delete: None,
            }]),
            ..blank.clone()
        },
    ];
    for change in &changes {
        let refused = fx.engine.update_table(ACCOUNT, change.clone()).await;
        assert!(
            matches!(&refused, Err(StorageError::TableNotActive(n)) if n == "dst"),
            "UpdateTable on a CREATING restore target: {refused:?}"
        );
    }
    let mut after = table_doc(&fx.engine, "dst").await.expect("target");
    // The restore's heartbeat refreshes the job record on its own schedule;
    // the record stays RUNNING.
    assert_eq!(
        after
            .get_document(RESTORE_JOB)
            .expect("job")
            .get_str("status"),
        Ok("RUNNING")
    );
    let mut before = before;
    before.remove(RESTORE_JOB);
    after.remove(RESTORE_JOB);
    assert_eq!(after, before, "a refused UpdateTable changes nothing");
    assert_eq!(
        index_ids(&fx.engine, before.get_str("table_id").expect("id"))
            .await
            .len(),
        2,
        "no GSI was added"
    );
    let missing = fx
        .engine
        .update_table(
            ACCOUNT,
            UpdateTableInput {
                table_name: "nope".to_owned(),
                deletion_protection_enabled: Some(true),
                ..blank.clone()
            },
        )
        .await;
    assert!(
        matches!(missing, Err(StorageError::TableNotFound(_))),
        "{missing:?}"
    );

    gate.add_permits(1);
    assert_fully_restored_with(&fx.engine, "dst", FEW).await;
    let desc = fx
        .engine
        .update_table(ACCOUNT, changes[1].clone())
        .await
        .expect("UpdateTable once ACTIVE");
    assert!(desc.deletion_protection_enabled);
    fx.finish().await;
}

/// A stream record's `created_at` field and its own `ApproximateCreationDateTime`
/// come from one clock read on the live write path, for a plain write and for
/// a service-identity deletion (the TTL worker's shape).
#[tokio::test]
#[ignore = "needs EXTENDDB_TEST_MONGODB_URI"]
async fn live_stream_record_created_at_matches_the_record_timestamp() {
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_core::types::{StreamSpecification, StreamViewType, UserIdentity};
    use extenddb_storage::{DataEngine, StreamCapture};

    let fx = Fixture::new().await;
    fx.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: "clock".to_owned(),
                key_schema: vec![key("pk", KeyType::Hash)],
                attribute_definitions: vec![attr("pk", ScalarAttributeType::S)],
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                ..Default::default()
            },
        )
        .await
        .expect("create the table");
    let key_info = fx
        .engine
        .table_key_info(ACCOUNT, "clock")
        .await
        .expect("key info");
    let maps = ExpressionMaps::default();
    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };
    for v in ["one", "two"] {
        let item: Item = [
            ("pk".to_owned(), AttributeValue::S("k".to_owned())),
            ("v".to_owned(), AttributeValue::S(v.to_owned())),
        ]
        .into_iter()
        .collect();
        fx.engine
            .put_item(&key_info, item, false, None, &maps, Some(&capture))
            .await
            .expect("put");
    }
    let key: Item = [("pk".to_owned(), AttributeValue::S("k".to_owned()))]
        .into_iter()
        .collect();
    fx.engine
        .delete_item(
            &key_info,
            &key,
            false,
            None,
            &maps,
            Some(&StreamCapture {
                view_type: StreamViewType::NewAndOldImages,
                user_identity: Some(UserIdentity {
                    identity_type: "Service".to_owned(),
                    principal_id: "dynamodb.amazonaws.com".to_owned(),
                }),
                region: Arc::from("us-east-1"),
            }),
        )
        .await
        .expect("delete");

    let mut cursor = fx
        .engine
        .data_db
        .collection::<Document>("stream_records")
        .find(doc! {})
        .await
        .expect("read back");
    let mut seen = 0;
    while cursor.advance().await.expect("cursor") {
        let d = cursor.deserialize_current().expect("document");
        let created_at = d.get_datetime("created_at").expect("created_at");
        let approx = d
            .get_document("record_data")
            .and_then(|r| r.get_document("dynamodb"))
            .expect("record_data.dynamodb")
            .get_i64("ApproximateCreationDateTime")
            .expect("ApproximateCreationDateTime");
        assert_eq!(
            created_at.timestamp_millis().div_euclid(1_000),
            approx,
            "created_at {created_at} and ApproximateCreationDateTime {approx} disagree"
        );
        seen += 1;
    }
    assert_eq!(seen, 3, "two puts and one delete");
    fx.finish().await;
}

// ─────────────────────────────────────────────────────────────────────────
// Store-backed restore: claim_restore, the claim, claim_restore_dead,
// read/list_restore_jobs, bulk_load, finish_restore, count_restore_target.
// The registration takes the `backup_index` row's lock (as the legacy path
// takes the `backups` row's), writes the record `kind = manifest` with the
// store and the claimant's spool, and the record's heartbeat is the server's
// clock from the registration on; bulk_load writes base documents only, the
// GSI backfill worker fills the indexes and flips the table ACTIVE.
// ─────────────────────────────────────────────────────────────────────────

/// A composite-key input with a GSI (ALL) and an LSI (KEYS_ONLY), the shape
/// `source_table_for` creates, for a restore target.
fn manifest_input(name: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: name.to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: vec![
            attr("pk", ScalarAttributeType::S),
            attr("sk", ScalarAttributeType::N),
            attr("gpk", ScalarAttributeType::S),
            attr("lsk", ScalarAttributeType::N),
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "g1".to_owned(),
            key_schema: vec![key("gpk", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        local_secondary_indexes: Some(vec![LsiInput {
            index_name: "l1".to_owned(),
            key_schema: vec![key("pk", KeyType::Hash), key("lsk", KeyType::Range)],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
        }]),
        ..Default::default()
    }
}

/// A `backup_index` row of a backup of `table`, left AVAILABLE: what a
/// store-backed restore registers against. Returns the row (its `key()` is
/// the registration's backup key).
async fn available_backup(engine: &MongoEngine, table: &str, name: &str) -> BackupRow {
    let row = row(ACCOUNT, table, name);
    let claim = engine.claim_backup(&row).await.expect("claim backup row");
    assert_eq!(
        claim
            .set_status(
                JobStatus::Creating,
                JobStatus::Available,
                &StatusFields::default()
            )
            .await
            .expect("to AVAILABLE"),
        WriteOutcome::Matched
    );
    drop(claim);
    row
}

/// The registration row for `row`, with the claimant's `spool_host`
/// (`None` for a filesystem store). `owner_generation` is unowned: the
/// backend mints its own.
fn registration(row: &BackupRow, spool: Option<&str>) -> RestoreRegistrationRow {
    RestoreRegistrationRow {
        backup: row.key(),
        source_table_arn: format!(
            "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{}",
            row.table_name
        ),
        restore_date_time: row.created_at,
        store: store(),
        spool_host: spool.map(str::to_owned),
        owner_generation: UNOWNED_GENERATION,
    }
}

/// Item `i` of a restore: every attribute the source shape carries, both
/// index keys, and a 38-digit non-key number that survives as a string.
fn manifest_item(i: usize) -> Item {
    [
        ("pk".to_owned(), AttributeValue::S(format!("p{}", i % 50))),
        ("sk".to_owned(), AttributeValue::N(i.to_string())),
        ("gpk".to_owned(), AttributeValue::S(format!("g{}", i % 7))),
        ("lsk".to_owned(), AttributeValue::N((i * 3).to_string())),
        (
            "big".to_owned(),
            AttributeValue::N("12345678901234567890123456789012345678".to_owned()),
        ),
    ]
    .into_iter()
    .collect()
}

/// The items `lo..hi` as the stream `bulk_load` reads.
fn item_stream(lo: usize, hi: usize) -> BoxStream<'static, Result<Item, StorageError>> {
    let items: Vec<_> = (lo..hi).map(manifest_item).collect();
    futures::stream::iter(items.into_iter().map(Ok)).boxed()
}

/// The registration writes the target CREATING with its `RestoreSummary`
/// and a RUNNING `kind = manifest` record carrying the store, the
/// claimant's `spool_host`, a fresh generation and the server's clock on
/// its heartbeat (the registering process's clock is an hour behind, and
/// the stored heartbeat is within seconds of the server's `$$NOW`); the
/// claim heartbeats and reads the record; `read_restore_job` and
/// `list_restore_jobs` fill the store and spool; `backup_in_use` names the
/// target.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_claim_restore_registers_the_target_and_its_record() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    let backup = available_backup(&fx.engine, "src", "b").await;
    let reg = registration(&backup, Some("host-a:/var/spool/x"));
    let behind = fx
        .second_instance(BackupLiveness {
            local_clock_behind: Duration::from_secs(3_600),
            ..test_liveness()
        })
        .await;
    let (desc, claim) = behind
        .claim_restore(ACCOUNT, manifest_input("dst"), &reg)
        .await
        .expect("register");
    assert_eq!(desc.table_status, TableStatus::Creating);
    let summary = desc.restore_summary.as_ref().expect("a RestoreSummary");
    assert_eq!(
        summary.source_backup_arn.as_deref(),
        Some(backup.backup_arn.as_str())
    );
    assert!(summary.restore_in_progress);
    let generation = claim.generation();
    assert_ne!(generation, UNOWNED_GENERATION);

    let doc = table_doc(&fx.engine, "dst").await.expect("target");
    let job = doc.get_document(RESTORE_JOB).expect("job record");
    assert_eq!(job.get_str("status"), Ok("RUNNING"));
    assert_eq!(job.get_str("kind"), Ok("manifest"));
    assert_eq!(
        u64::try_from(job.get_i64("owner_generation").unwrap()).unwrap(),
        generation
    );
    assert_eq!(
        job.get_str("store_kind"),
        Ok(StoreKind::Filesystem.as_str())
    );
    assert_eq!(job.get_str("store_root"), Ok(store().store_root.as_str()));
    assert_eq!(job.get_str("store_id"), Ok(store().store_id.as_str()));
    assert_eq!(job.get_str("spool_host"), Ok("host-a:/var/spool/x"));
    // The heartbeat is the server's clock stamped inside the registration,
    // not the inserting process's (an hour behind): within seconds of the
    // server's `$$NOW`, read back through an aggregation.
    let heartbeat = *job.get_datetime("heartbeat_at").expect("heartbeat");
    let now = server_now(&fx.engine).await;
    let age_ms = now.timestamp_millis() - heartbeat.timestamp_millis();
    assert!(
        (-2_000..=5_000).contains(&age_ms),
        "the record's heartbeat is the server's clock, not the slow process's: {heartbeat:?} \
         is {age_ms} ms behind {now:?}"
    );

    let key = target_key(&doc);
    let read = fx
        .engine
        .read_restore_job(&key)
        .await
        .expect("read")
        .expect("a record");
    assert_eq!(read.kind, RestoreKind::Manifest);
    assert_eq!(read.status, JobStatus::Running);
    assert_eq!(read.owner_generation, generation);
    assert_eq!(
        read.store.as_ref().map(|s| s.store_kind),
        Some(StoreKind::Filesystem)
    );
    assert_eq!(read.spool_host.as_deref(), Some("host-a:/var/spool/x"));
    assert_eq!(read.file_index, None);

    let listed = fx
        .engine
        .list_restore_jobs(Some(JobStatus::Running))
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].target, key);
    assert!(listed[0].store.is_some());
    assert_eq!(listed[0].spool_host.as_deref(), Some("host-a:/var/spool/x"));

    assert_eq!(
        fx.engine
            .backup_in_use(&backup.key())
            .await
            .unwrap()
            .as_deref(),
        Some("dst")
    );
    assert_eq!(claim.heartbeat().await.unwrap(), Heartbeat::Confirmed);
    drop(claim);
    fx.finish().await;
}

/// A DeleteBackup and a store-backed registration of one backup order wholly
/// one before the other, on the `backup_index` row's lock: a delete that
/// wins leaves the registration `Backup not found` and no target; a
/// registration that wins makes the delete `BackupInUse`. A loop of
/// concurrent races holds the invariant that no target is ever registered
/// against a row that is not AVAILABLE.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_the_two_orderings_against_delete_backup_on_the_backup_index_row() {
    let fx = Fixture::new().await;
    fx.worker.abort();

    // Order A: the delete wins.
    let a = available_backup(&fx.engine, "srcA", "ba").await;
    assert_eq!(
        fx.engine.start_backup_delete(&a.key()).await.unwrap(),
        DeleteStart::Deleting
    );
    let refused = fx
        .engine
        .claim_restore(ACCOUNT, manifest_input("dstA"), &registration(&a, None))
        .await
        .err();
    assert!(
        matches!(&refused, Some(StorageError::Validation(m)) if m.contains("Backup not found")),
        "the registration finds the backup gone: {refused:?}"
    );
    assert!(table_doc(&fx.engine, "dstA").await.is_none());

    // Order B: the registration wins.
    let b = available_backup(&fx.engine, "srcB", "bb").await;
    let (_, claim) = fx
        .engine
        .claim_restore(ACCOUNT, manifest_input("dstB"), &registration(&b, None))
        .await
        .expect("register");
    assert_eq!(
        fx.engine.start_backup_delete(&b.key()).await.unwrap(),
        DeleteStart::InUse("dstB".to_owned())
    );
    assert_eq!(
        fx.engine
            .read_backup_row(&arn_of(&b))
            .await
            .unwrap()
            .unwrap()
            .status,
        BackupStatus::Available,
        "a refused delete leaves the row AVAILABLE"
    );
    drop(claim);

    // Concurrent races: the lock keeps the two consistent every time, and
    // both outcomes occur across the loop (no frozen answer).
    let mut delete_won = 0;
    let mut restore_won = 0;
    for i in 0..16 {
        let backup = available_backup(&fx.engine, &format!("srcC{i}"), "bc").await;
        let name = format!("dstC{i}");
        let e1 = fx.engine.clone();
        let e2 = fx.engine.clone();
        let key = backup.key();
        let reg = registration(&backup, None);
        let del = tokio::spawn(async move { e1.start_backup_delete(&key).await });
        let res =
            tokio::spawn(
                async move { e2.claim_restore(ACCOUNT, manifest_input(&name), &reg).await },
            );
        let (del, res) = (del.await.unwrap().unwrap(), res.await.unwrap());
        let res_ok = res.is_ok();
        let res_err = res.err();
        let target = table_doc(&fx.engine, &format!("dstC{i}")).await;
        let status = fx
            .engine
            .read_backup_row(&arn_of(&backup))
            .await
            .unwrap()
            .expect("the backup_index row")
            .status;
        if target.is_some() {
            assert_eq!(
                status,
                BackupStatus::Available,
                "a target was registered, so the delete must have lost"
            );
            assert!(
                matches!(del, DeleteStart::InUse(_)),
                "iteration {i}: {del:?}"
            );
            assert!(res_ok, "iteration {i}");
            restore_won += 1;
        } else {
            assert!(
                matches!(&res_err, Some(StorageError::Validation(m)) if m.contains("Backup not found")),
                "iteration {i}: no target but the registration did not refuse: {res_err:?}"
            );
            assert_eq!(status, BackupStatus::Deleting, "iteration {i}");
            delete_won += 1;
        }
    }
    assert!(
        delete_won > 0 && restore_won > 0,
        "both orders occur: {delete_won} / {restore_won}"
    );
    fx.finish().await;
}

/// Two concurrent registrations of one backup leave one CREATING target and
/// answer `BackupInUse` to the other (the in-use rule under the lock).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_two_concurrent_registrations_of_one_backup_leave_one_target() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    let backup = available_backup(&fx.engine, "src", "b").await;
    let e1 = fx.engine.clone();
    let e2 = fx.engine.clone();
    let r1 = registration(&backup, None);
    let r2 = registration(&backup, None);
    let t1 =
        tokio::spawn(async move { e1.claim_restore(ACCOUNT, manifest_input("r1"), &r1).await });
    let t2 =
        tokio::spawn(async move { e2.claim_restore(ACCOUNT, manifest_input("r2"), &r2).await });
    let o1 = t1.await.unwrap();
    let o2 = t2.await.unwrap();
    let oks = [&o1, &o2].iter().filter(|o| o.is_ok()).count();
    let in_use = [&o1, &o2]
        .iter()
        .filter(|o| matches!(o, Err(StorageError::BackupInUse(_))))
        .count();
    let errs: Vec<_> = [&o1, &o2]
        .iter()
        .filter_map(|o| o.as_ref().err().map(ToString::to_string))
        .collect();
    assert_eq!(
        (oks, in_use),
        (1, 1),
        "one target, one BackupInUse: oks={oks}, errs={errs:?}"
    );
    let targets = registered_targets(&fx.engine, &["r1".to_owned(), "r2".to_owned()]).await;
    assert_eq!(
        targets.len(),
        1,
        "exactly one target registered: {targets:?}"
    );
    for (_, claim) in [o1, o2].into_iter().flatten() {
        drop(claim);
    }
    fx.finish().await;
}

/// A name a table or a CREATING restore target already holds is refused as
/// `TableAlreadyExists` or `TableBeingRestored`, with nothing registered.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_claim_restore_refuses_a_taken_name() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    // A plain ACTIVE table.
    fx.engine
        .create_table_impl(ACCOUNT, manifest_input("taken"), false)
        .await
        .expect("create a table");
    let backup = available_backup(&fx.engine, "src", "b").await;
    let exists = fx
        .engine
        .claim_restore(
            ACCOUNT,
            manifest_input("taken"),
            &registration(&backup, None),
        )
        .await
        .err();
    assert!(
        matches!(&exists, Some(StorageError::TableAlreadyExists(n)) if n == "taken"),
        "{exists:?}"
    );

    // A CREATING restore target.
    let (_, claim) = fx
        .engine
        .claim_restore(
            ACCOUNT,
            manifest_input("being"),
            &registration(&backup, None),
        )
        .await
        .expect("first registration");
    let b2 = available_backup(&fx.engine, "src2", "b2").await;
    let restoring = fx
        .engine
        .claim_restore(ACCOUNT, manifest_input("being"), &registration(&b2, None))
        .await
        .err();
    assert!(
        matches!(
            &restoring,
            Some(StorageError::TableBeingRestored { table_name, backup_arn })
                if table_name == "being" && backup_arn == &backup.backup_arn
        ),
        "{restoring:?}"
    );
    drop(claim);
    fx.finish().await;
}

/// `claim_restore_dead` takes over a RUNNING manifest record whose heartbeat
/// is stale: a fresh generation, the claimant's `spool_host` and
/// `heartbeat_at` refreshed to the server's clock in one write, so a second
/// taker issued right after the first (before the successor's first beat)
/// finds the record live and is refused, and the first taker's beat
/// confirms; the old generation's writes are fenced out. A live record, and
/// a legacy record whatever its age, are left alone.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_claim_restore_dead_takes_over_a_stale_manifest_record() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    let backup = available_backup(&fx.engine, "src", "b").await;
    let (doc, claim) = fx
        .engine
        .claim_restore(
            ACCOUNT,
            manifest_input("t"),
            &registration(&backup, Some("old:/s")),
        )
        .await
        .expect("register");
    let desc_doc = table_doc(&fx.engine, "t").await.expect("target");
    let key = target_key(&desc_doc);
    let g1 = claim.generation();
    let _ = doc;

    // Live: fresh heartbeat, so not dead.
    assert!(
        fx.engine
            .claim_restore_dead(&key, STALE_AFTER, Some("new:/s2"))
            .await
            .unwrap()
            .is_none(),
        "a live record is not taken over"
    );

    // Age it past the window and take it over.
    age_running_record(&fx.engine, "t", Duration::from_secs(600)).await;
    let aged = *job_record(&fx.engine, "t")
        .await
        .get_datetime("heartbeat_at")
        .expect("heartbeat");
    let taken = fx
        .engine
        .claim_restore_dead(&key, STALE_AFTER, Some("new:/s2"))
        .await
        .expect("takeover")
        .expect("a stale record is taken over");
    let g2 = taken.generation();
    assert_ne!(g2, g1, "a fresh generation");
    let after = job_record(&fx.engine, "t").await;
    assert_eq!(
        u64::try_from(after.get_i64("owner_generation").unwrap()).unwrap(),
        g2
    );
    assert_eq!(
        after.get_str("spool_host"),
        Ok("new:/s2"),
        "the claimant's spool is written"
    );
    // A second taker right after the first, before the successor's first
    // beat: the record is live to it, and the first taker's beat confirms.
    let second = fx
        .engine
        .claim_restore_dead(&key, STALE_AFTER, Some("third:/s3"))
        .await
        .expect("a second takeover attempt");
    assert!(
        second.is_none(),
        "a second taker right after the first is refused: {:?}",
        second.as_ref().map(|c| c.generation())
    );
    assert_eq!(
        taken.heartbeat().await.unwrap(),
        Heartbeat::Confirmed,
        "the first taker keeps the record"
    );
    let refreshed = *after.get_datetime("heartbeat_at").expect("heartbeat");
    assert!(
        refreshed > aged,
        "the takeover refreshed heartbeat_at: {refreshed:?} was {aged:?}"
    );
    let now = server_now(&fx.engine).await;
    let age_ms = now.timestamp_millis() - refreshed.timestamp_millis();
    assert!(
        (-2_000..=5_000).contains(&age_ms),
        "the refreshed heartbeat is the server's now: {refreshed:?} is {age_ms} ms behind {now:?}"
    );
    assert!(
        fx.engine
            .list_dead_restore_jobs(STALE_AFTER)
            .await
            .unwrap()
            .is_empty(),
        "the taken-over record is live to the listing at once"
    );

    // The old generation is fenced out; the new one owns the record.
    assert_eq!(
        claim
            .set_status(
                JobStatus::Running,
                JobStatus::Failed,
                &StatusFields::default()
            )
            .await
            .unwrap(),
        WriteOutcome::NotMatched
    );
    assert!(
        matches!(
            fx.engine.finish_restore(&key, &*claim, 0, 0).await.unwrap(),
            Finished::NotMatched(_)
        ),
        "the dead generation cannot finish the restore"
    );
    drop(claim);
    drop(taken);
    fx.finish().await;
}

/// A legacy record, and a record that is not RUNNING, are never taken over.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_claim_restore_dead_leaves_a_legacy_or_finished_record() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    let backup = available_backup(&fx.engine, "src", "b").await;
    let (doc, claim) = fx
        .engine
        .claim_restore(ACCOUNT, manifest_input("leg"), &registration(&backup, None))
        .await
        .expect("register");
    let key = target_key(&table_doc(&fx.engine, "leg").await.expect("target"));
    let _ = doc;
    // Make the record legacy and stale.
    fx.engine
        .catalog_db
        .collection::<Document>("tables")
        .update_one(
            doc! { "_id": { "account_id": ACCOUNT, "table_name": "leg" } },
            doc! { "$set": { "restore_job.kind": "legacy" } },
        )
        .await
        .expect("to legacy");
    age_running_record(&fx.engine, "leg", Duration::from_secs(600)).await;
    // age_running_record rewrites the record RUNNING; set kind legacy again.
    fx.engine
        .catalog_db
        .collection::<Document>("tables")
        .update_one(
            doc! { "_id": { "account_id": ACCOUNT, "table_name": "leg" } },
            doc! { "$set": { "restore_job.kind": "legacy" } },
        )
        .await
        .expect("to legacy");
    assert!(
        fx.engine
            .claim_restore_dead(&key, STALE_AFTER, Some("new:/s"))
            .await
            .unwrap()
            .is_none(),
        "a legacy record is not taken over"
    );
    drop(claim);
    fx.finish().await;
}

/// `bulk_load` writes a file's base documents and records `(file_index,
/// items_applied)` in the same transaction as each batch, so a resume
/// continues from the exact item `items_applied` names and re-applies
/// nothing; a duplicate key on a file being loaded for the first time (a
/// backup that holds a key twice) fails the file.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_bulk_load_records_each_batch_and_a_resume_continues_from_items_applied() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    let backup = available_backup(&fx.engine, "src", "b").await;
    let (doc, claim) = fx
        .engine
        .claim_restore(ACCOUNT, manifest_input("dst"), &registration(&backup, None))
        .await
        .expect("register");
    let key = target_key(&table_doc(&fx.engine, "dst").await.expect("target"));
    let _ = doc;

    // File 0: 2500 items across three insert_many batches, each committed
    // with its progress in one transaction.
    let applied = fx
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            item_stream(0, 2500),
        )
        .await
        .expect("file 0");
    assert_eq!(applied, 2500);
    assert_eq!(fx.engine.count_restore_target(&key).await.unwrap(), 2500);
    let record = fx.engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(record.file_index, Some(0));
    assert_eq!(record.items_applied, Some(2500));

    // File 1's first 1000 items commit (one batch), then the process dies:
    // the record names file 1 with 1000 applied.
    let applied = fx
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 1,
                start_at: 0,
                reapplying: false,
            },
            item_stream(2500, 3500),
        )
        .await
        .expect("file 1, first batch");
    assert_eq!(applied, 1000);
    let record = fx.engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(record.file_index, Some(1));
    assert_eq!(record.items_applied, Some(1000));

    // The resume continues file 1 from item 1000: the engine skipped the
    // 1000 already applied and streams only the rest, so nothing is
    // re-applied and no duplicate arises (the `reapplying` flag the engine
    // sets for the first resumed file is vestigial on this backend, since
    // the progress is exact).
    let resumed = fx
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 1,
                start_at: 1000,
                reapplying: true,
            },
            item_stream(3500, 4000),
        )
        .await
        .expect("resume file 1");
    assert_eq!(resumed, 500);
    assert_eq!(fx.engine.count_restore_target(&key).await.unwrap(), 4000);
    let record = fx.engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(record.file_index, Some(1));
    assert_eq!(record.items_applied, Some(1500));

    // A file whose items the target already holds (a backup that holds a
    // key twice) fails the file; the unique `_id` raises E11000.
    let dup = fx
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 2,
                start_at: 0,
                reapplying: false,
            },
            item_stream(2500, 3500),
        )
        .await;
    assert!(
        matches!(&dup, Err(StorageError::Validation(m)) if m.contains("holds a key twice")),
        "{dup:?}"
    );
    assert_eq!(fx.engine.count_restore_target(&key).await.unwrap(), 4000);
    drop(claim);
    fx.finish().await;
}

/// A claim lost between two `insert_many` calls of one file (the record's
/// generation moved by a takeover while the load ran): the read before the
/// next call finds the record no longer this claim's, the file ends as a
/// lost claim, and the second call's documents never land.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_bulk_load_stops_at_a_lost_claim_between_two_insert_many_calls() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    let backup = available_backup(&fx.engine, "src", "b").await;
    let (_doc, claim) = fx
        .engine
        .claim_restore(
            ACCOUNT,
            manifest_input("lost"),
            &registration(&backup, None),
        )
        .await
        .expect("register");
    let key = target_key(&table_doc(&fx.engine, "lost").await.expect("target"));
    // The stream hands the first call's documents, then moves the record's
    // generation when asked for the first item of the second call.
    let tables = fx.engine.catalog_db.collection::<Document>("tables");
    let moved_key = key.clone();
    let stream: BoxStream<'static, Result<Item, StorageError>> = Box::pin(futures::stream::unfold(
        0usize,
        move |i| {
            let tables = tables.clone();
            let key = moved_key.clone();
            async move {
                if i >= 2 * INSERT_MANY_DOCUMENTS + 500 {
                    return None;
                }
                if i == INSERT_MANY_DOCUMENTS {
                    tables
                        .update_one(
                            doc! { "_id": { "account_id": &key.account_id, "table_name": &key.table_name } },
                            doc! { "$inc": { RESTORE_JOB_GENERATION: 1_i64 } },
                        )
                        .await
                        .expect("another owner takes the record");
                }
                Some((Ok(manifest_item(i)), i + 1))
            }
        },
    ));
    let out = fx
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            stream,
        )
        .await;
    assert!(
        matches!(&out, Err(StorageError::Internal(m)) if m.contains("claim on the target was lost")),
        "the second batch's fence finds another owner: {out:?}"
    );
    assert_eq!(
        count(&fx.engine, &data_collection_name(&key.table_id)).await,
        INSERT_MANY_DOCUMENTS as u64,
        "the first batch's documents alone landed; the second batch is fenced out"
    );
    // The first batch committed its documents and its progress in one
    // transaction, so the record carries that batch's progress; the second
    // batch, fenced out, recorded nothing.
    let record = fx.engine.read_restore_job(&key).await.unwrap().unwrap();
    assert_eq!(
        record.file_index,
        Some(0),
        "the first batch's progress is recorded"
    );
    assert_eq!(record.items_applied, Some(INSERT_MANY_DOCUMENTS as u64));
    drop(claim);
    fx.finish().await;
}

/// `bulk_load` refuses an `N` sort key beyond Decimal128's precision with
/// the write path's `ValidationException`, and a 38-digit non-key number
/// survives a restore as a string.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_bulk_load_refuses_a_long_sort_key_and_keeps_a_long_number() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    let backup = available_backup(&fx.engine, "src", "b").await;
    let (_, claim) = fx
        .engine
        .claim_restore(ACCOUNT, manifest_input("dec"), &registration(&backup, None))
        .await
        .expect("register");
    let key = target_key(&table_doc(&fx.engine, "dec").await.expect("target"));

    let long_sk: Item = [
        ("pk".to_owned(), AttributeValue::S("p".to_owned())),
        // 35 significant digits: beyond Decimal128.
        (
            "sk".to_owned(),
            AttributeValue::N("12345678901234567890123456789012345".to_owned()),
        ),
        ("gpk".to_owned(), AttributeValue::S("g".to_owned())),
        ("lsk".to_owned(), AttributeValue::N("1".to_owned())),
    ]
    .into_iter()
    .collect();
    let refused = fx
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            futures::stream::iter(vec![Ok(long_sk)]).boxed(),
        )
        .await;
    assert!(
        matches!(&refused, Err(StorageError::Validation(m)) if m.contains("Decimal128")),
        "{refused:?}"
    );

    // A good item with a 38-digit non-key number restores intact.
    fx.engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            item_stream(0, 1),
        )
        .await
        .expect("a good item");
    let stored = fx
        .engine
        .data_db
        .collection::<Document>(&data_collection_name(&key.table_id))
        .find_one(doc! {})
        .await
        .unwrap()
        .expect("the item");
    let item = document_to_item(&stored).expect("item");
    assert_eq!(
        item.get("big"),
        Some(&AttributeValue::N(
            "12345678901234567890123456789012345678".to_owned()
        )),
        "the 38-digit non-key number survives"
    );
    drop(claim);
    fx.finish().await;
}

/// `finish_restore` hands the target to the backfill worker (`Pending`,
/// DONE + `restore_backfill_pending`), the backup stays in use until the
/// worker flips the table ACTIVE, and `list_dead_restore_jobs` leaves the
/// DONE record alone. A stale generation's `finish_restore` is `NotMatched`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_finish_restore_hands_over_and_the_worker_flips_active() {
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    fx.worker.abort();
    let backup = available_backup(&fx.engine, "src", "b").await;
    let (_, claim) = fx
        .engine
        .claim_restore(ACCOUNT, manifest_input("fin"), &registration(&backup, None))
        .await
        .expect("register");
    let key = target_key(&table_doc(&fx.engine, "fin").await.expect("target"));
    fx.engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            item_stream(0, FEW),
        )
        .await
        .expect("load");

    // A stale generation cannot finish it.
    age_running_record(&fx.engine, "fin", Duration::from_secs(600)).await;
    let taken = fx
        .engine
        .claim_restore_dead(&key, STALE_AFTER, None)
        .await
        .unwrap()
        .expect("takeover");
    assert_eq!(
        fx.engine
            .finish_restore(&key, &*claim, FEW as u64, 1)
            .await
            .unwrap(),
        Finished::NotMatched(format!(
            "the job record of fin is not RUNNING under generation {}, or the table is not \
             CREATING with table id {}",
            claim.generation(),
            key.table_id
        ))
    );

    assert_eq!(
        fx.engine
            .finish_restore(&key, &*taken, FEW as u64, 1)
            .await
            .unwrap(),
        Finished::Pending
    );
    let doc = table_doc(&fx.engine, "fin").await.expect("target");
    assert_eq!(doc.get_str("table_status"), Ok("CREATING"));
    assert!(doc.get_bool("restore_backfill_pending").unwrap_or(false));
    assert_eq!(
        doc.get_document(RESTORE_JOB).unwrap().get_str("status"),
        Ok("DONE")
    );
    assert_eq!(doc.get_i64("item_count"), Ok(FEW as i64));

    // Between the DONE write and the worker's flip the backup is in use and
    // the DONE record is not dead.
    assert_eq!(
        fx.engine
            .backup_in_use(&backup.key())
            .await
            .unwrap()
            .as_deref(),
        Some("fin")
    );
    assert!(
        fx.engine
            .list_dead_restore_jobs(STALE_AFTER)
            .await
            .unwrap()
            .iter()
            .all(|j| j.target.table_name != "fin"),
        "the DONE+pending record is not dead"
    );

    // The worker flips it ACTIVE with every index populated.
    let worker = spawn_worker(&fx.engine);
    let active = wait_for_status(&fx.engine, "fin", "ACTIVE").await;
    let table_id = active.get_str("table_id").expect("table_id");
    assert_eq!(active.get_i64("item_count"), Ok(FEW as i64));
    assert_eq!(
        count(&fx.engine, &data_collection_name(table_id)).await,
        FEW as u64
    );
    for (index_name, index_id, status) in index_ids(&fx.engine, table_id).await {
        assert_eq!(status, "ACTIVE", "{index_name}");
        assert_eq!(
            count(&fx.engine, &data_collection_name(&index_id)).await,
            FEW as u64,
            "index {index_name}"
        );
    }
    let job = active.get_document(RESTORE_JOB).expect("job record");
    assert_eq!(job.get_str("status"), Ok("DONE"));
    assert_eq!(job.get_str("kind"), Ok("manifest"));
    assert!(!active.contains_key("restore_backfill_pending"));
    assert_eq!(fx.engine.backup_in_use(&backup.key()).await.unwrap(), None);
    worker.abort();
    drop(claim);
    drop(taken);
    fx.finish().await;
}

/// The `restore_mongodb_after_insert_many` kill point and the file-index
/// write's fence: a claim taken over while `bulk_load` is paused between its
/// `insert_many` and the record write finds the write matched nothing (the
/// generation moved), so the lost claim's progress never lands. Runs only
/// under `test-hooks` (the pause point is compiled then) and must run alone
/// (`--test-threads=1`): the pause is driven by a process-wide environment
/// variable.
#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set and --features test-hooks, run with --test-threads=1"]
async fn a_takeover_during_bulk_load_fences_the_file_index_write() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    let backup = available_backup(&fx.engine, "src", "b").await;
    let (_, claim) = fx
        .engine
        .claim_restore(
            ACCOUNT,
            manifest_input("fence"),
            &registration(&backup, None),
        )
        .await
        .expect("register");
    let key = target_key(&table_doc(&fx.engine, "fence").await.expect("target"));

    // Take the record over before the owner's batch runs: its heartbeat is
    // aged and a second owner claims it under a fresh generation.
    age_running_record(&fx.engine, "fence", Duration::from_secs(600)).await;
    let taken = fx
        .engine
        .claim_restore_dead(&key, STALE_AFTER, None)
        .await
        .expect("takeover")
        .expect("a stale record is taken over");

    // The old claim's load is fenced inside the batch transaction: the
    // `find_one_and_update` on the record (RUNNING under the old generation)
    // matches nothing, so neither the documents nor the progress of the
    // batch are written.
    let out = fx
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            item_stream(0, 10),
        )
        .await;
    assert!(
        matches!(&out, Err(StorageError::Internal(m)) if m.contains("claim on the target was lost")),
        "the fenced-out load must not write the batch: {out:?}"
    );
    assert_eq!(
        count(&fx.engine, &data_collection_name(&key.table_id)).await,
        0,
        "no document lands after the takeover"
    );
    // The successor's record never advanced its file_index.
    assert_eq!(
        fx.engine
            .read_restore_job(&key)
            .await
            .unwrap()
            .unwrap()
            .file_index,
        None,
        "the lost claim's batch wrote no progress"
    );

    drop(taken);
    fx.finish().await;
}

/// The registration itself writes the record's first heartbeat on the
/// server's clock (`$$NOW`, in the registration's transaction): a target
/// registered with a first heartbeat ten minutes behind, as a host whose
/// clock is behind the database's would write it, carries the server's time
/// as soon as the registration commits, before any heartbeat loop runs, so
/// another server cannot list the new restore dead in the window before the
/// copy task's first refresh. No restore task runs here: the registration is
/// called directly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_registration_writes_its_first_heartbeat_on_the_server_clock() {
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", 10).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    let behind = bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 600_000);
    let mut registration = crate::backup_engine::RestoreRegistration {
        backup_arn: &request.backup_arn,
        source_table_arn: &request.source_table_arn,
        restore_date_time: bson::DateTime::now(),
        owner_generation: 7,
        first_heartbeat: behind,
        record: crate::backup_engine::RestoreRecord::Legacy,
        session: None,
    };
    fx.engine
        .create_table_for_restore(ACCOUNT, request.input, &mut registration)
        .await
        .expect("register");
    let job = job_record(&fx.engine, "dst").await;
    let beat = job.get_datetime("heartbeat_at").expect("heartbeat");
    let age_ms = bson::DateTime::now().timestamp_millis() - beat.timestamp_millis();
    assert!(
        age_ms.abs() < 60_000,
        "the registration's heartbeat is {age_ms} ms old: the server clock replaces the \
         process's inside the registration"
    );
    assert_eq!(job.get_str("status"), Ok("RUNNING"));
    assert_eq!(job.get_i64("owner_generation"), Ok(7));
    assert!(
        fx.engine
            .list_dead_restore_jobs(STALE_AFTER)
            .await
            .expect("list")
            .is_empty(),
        "a record just registered by a slow-clock host is not dead"
    );
    // The target this test registered has no task: remove it so the fixture
    // can drop its databases cleanly.
    let key = RestoreTargetKey {
        account_id: ACCOUNT.to_owned(),
        table_name: "dst".to_owned(),
        table_id: table_doc(&fx.engine, "dst")
            .await
            .expect("target")
            .get_str("table_id")
            .expect("table_id")
            .to_owned(),
    };
    assert!(fx.engine.remove_restore_target(&key).await.expect("remove"));
    fx.finish().await;
}

/// `fail_restore_job` is conditioned on the `owner_generation` and
/// `heartbeat_at` the listing read: a heartbeat that lands between the
/// listing and the write keeps the record its owner's (the write matches
/// nothing), as does a generation that is not the record's; with the listed
/// values current the write lands under a fresh generation.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_the_failed_write_matches_only_the_listed_generation_and_heartbeat() {
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", 10).await;
    let backup = fx
        .engine
        .create(ACCOUNT, "src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    fx.engine
        .restore(ACCOUNT, request, JobPermit::detached())
        .await
        .expect("restore");
    let done = wait_for_status(&fx.engine, "dst", "ACTIVE").await;
    let key = RestoreTargetKey {
        account_id: ACCOUNT.to_owned(),
        table_name: "dst".to_owned(),
        table_id: done.get_str("table_id").expect("table_id").to_owned(),
    };
    let tables = fx.engine.catalog_db.collection::<Document>("tables");
    let set = |update: Document| {
        let tables = tables.clone();
        async move {
            tables
                .update_one(
                    doc! { "_id": { "account_id": ACCOUNT, "table_name": "dst" } },
                    update,
                )
                .await
                .expect("set the record");
        }
    };
    let generation_of = || async {
        job_record(&fx.engine, "dst")
            .await
            .get_i64("owner_generation")
            .expect("generation")
    };

    set(doc! { "$set": { "table_status": "CREATING" } }).await;
    age_running_record(&fx.engine, "dst", Duration::from_secs(600)).await;
    let dead = fx
        .engine
        .list_dead_restore_jobs(STALE_AFTER)
        .await
        .expect("list");
    assert_eq!(dead.len(), 1, "{dead:?}");
    let listed = dead[0].clone();
    let generation = generation_of().await;
    assert_eq!(i64::try_from(listed.owner_generation).unwrap(), generation);

    // The owner's heartbeat lands after the listing.
    set(doc! { "$set": { RESTORE_JOB_HEARTBEAT: bson::DateTime::now() } }).await;
    assert!(
        !fx.engine
            .fail_restore_job(
                &key,
                listed.owner_generation,
                listed.heartbeat_at,
                "owner dead"
            )
            .await
            .expect("fail"),
        "a heartbeat after the listing keeps the record its owner's"
    );
    let job = job_record(&fx.engine, "dst").await;
    assert_eq!(job.get_str("status"), Ok("RUNNING"));
    assert_eq!(job.get_i64("owner_generation"), Ok(generation));
    assert!(
        fx.engine
            .list_dead_restore_jobs(STALE_AFTER)
            .await
            .expect("list")
            .is_empty()
    );
    // Another generation with the current heartbeat: no match either.
    let current = *job.get_datetime("heartbeat_at").expect("heartbeat");
    assert!(
        !fx.engine
            .fail_restore_job(
                &key,
                listed.owner_generation + 1,
                current.timestamp_millis() as f64 / 1000.0,
                "owner dead"
            )
            .await
            .expect("fail")
    );
    assert_eq!(
        job_record(&fx.engine, "dst").await.get_str("status"),
        Ok("RUNNING")
    );

    // Listed values that are still current: the write lands, under a fresh
    // generation.
    age_running_record(&fx.engine, "dst", Duration::from_secs(600)).await;
    let dead = fx
        .engine
        .list_dead_restore_jobs(STALE_AFTER)
        .await
        .expect("list");
    assert_eq!(dead.len(), 1);
    assert!(
        fx.engine
            .fail_restore_job(
                &dead[0].target,
                dead[0].owner_generation,
                dead[0].heartbeat_at,
                "owner dead"
            )
            .await
            .expect("fail")
    );
    let job = job_record(&fx.engine, "dst").await;
    assert_eq!(job.get_str("status"), Ok("FAILED"));
    assert_ne!(
        job.get_i64("owner_generation"),
        Ok(generation),
        "the FAILED write carries a fresh generation"
    );
    assert!(fx.engine.remove_restore_target(&key).await.expect("remove"));
    fx.finish().await;
}
/// A claim document with a fresh `heartbeat_at` and no `owner_generation`
/// (an operator's insert, not a release's): the upsert's filter matches it
/// and overwrites it with this call's generation, so the claim is this
/// call's and is heartbeated and released by it, where refusing it would
/// leave a document nobody heartbeats until it went stale. A document with
/// a generation and a fresh heartbeat (a live holder's) is still refused.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_claim_document_without_a_generation_is_taken_not_refused() {
    let fx = Fixture::new().await;
    let stale = std::time::Duration::from_secs(300);
    let maintenance = fx
        .engine
        .catalog_db
        .collection::<Document>(crate::backup_catalog::BACKUP_MAINTENANCE);
    maintenance
        .insert_one(doc! { "_id": "claim", "holder": "an operator's insert" })
        .await
        .expect("insert");
    maintenance
        .update_one(doc! {}, vec![doc! { "$set": { "heartbeat_at": "$$NOW" } }])
        .await
        .expect("stamp");
    let claim = fx
        .engine
        .maintenance_claim("purge on host-a", stale)
        .await
        .expect("a document with no owner_generation is taken");
    let document = maintenance
        .find_one(doc! { "_id": "claim" })
        .await
        .expect("read")
        .expect("one document");
    assert_eq!(document.get_str("holder").unwrap(), "purge on host-a");
    assert!(
        document.get("owner_generation").is_some(),
        "the document carries the taker's generation: {document:?}"
    );
    assert!(
        fx.engine
            .check_maintenance_claim(stale)
            .await
            .expect("check")
    );
    assert_eq!(
        claim.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    match fx.engine.maintenance_claim("purge on host-b", stale).await {
        Err(StorageError::AlreadyExists(m)) => assert!(m.contains("purge on host-a"), "{m}"),
        Err(other) => panic!("a live holder's document is refused with AlreadyExists: {other:?}"),
        Ok(_) => panic!("a live holder's document is refused"),
    }
    claim.release().await.expect("release");
    assert!(
        !fx.engine
            .check_maintenance_claim(stale)
            .await
            .expect("check")
    );
    fx.finish().await;
}

/// `newest_available_backup_row` reads the last AVAILABLE row in the
/// listing's order, and none when the store has no AVAILABLE row;
/// `list_running_restore_jobs` lists a RUNNING record by status alone, one
/// whose heartbeat is ahead of the server's clock included, which the
/// dead-job listing under a zero window does not list.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_the_newest_available_row_and_the_running_restores_are_read_by_status() {
    let fx = Fixture::new().await;
    let store = store();
    assert!(
        fx.engine
            .newest_available_backup_row(&store)
            .await
            .expect("read")
            .is_none(),
        "no AVAILABLE row yet"
    );
    let older = row(ACCOUNT, "Music", "older");
    let newer = row(ACCOUNT, "Music", "newer");
    for row in [&older, &newer] {
        let claim = fx.engine.claim_backup(row).await.expect("claim");
        assert_eq!(
            claim
                .set_status(
                    JobStatus::Creating,
                    JobStatus::Available,
                    &StatusFields::default()
                )
                .await
                .expect("publish"),
            WriteOutcome::Matched
        );
    }
    let newest = fx
        .engine
        .newest_available_backup_row(&store)
        .await
        .expect("read")
        .expect("one AVAILABLE row");
    let listed: Vec<BackupRow> = fx
        .engine
        .list_backup_rows(Some(BackupStatus::Available), &store)
        .try_collect()
        .await
        .expect("list");
    assert_eq!(
        newest.backup_arn,
        listed.last().unwrap().backup_arn,
        "the newest row is the last in the listing's order"
    );

    // A RUNNING restore record stamped 60 s ahead of the server's clock.
    source_table_with(&fx.engine, "rr_target", 1).await;
    let table_id = table_doc(&fx.engine, "rr_target")
        .await
        .expect("the table")
        .get_str("table_id")
        .expect("table_id")
        .to_owned();
    fx.engine
        .catalog_db
        .collection::<Document>("tables")
        .update_one(
            doc! { "_id": { "account_id": ACCOUNT, "table_name": "rr_target" } },
            vec![doc! { "$set": {
                "table_status": { "$literal": "CREATING" },
                "status_transition_at": { "$literal": bson::Bson::Null },
                "restore_source_backup_arn": { "$literal": &older.backup_arn },
                "restore_source_table_arn": {
                    "$literal": "arn:aws:dynamodb:us-east-1:123456789012:table/Music"
                },
                "restore_date_time": "$$NOW",
                RESTORE_JOB: {
                    "kind": { "$literal": "legacy" },
                    "status": { "$literal": "RUNNING" },
                    "owner_generation": { "$literal": 7_i64 },
                    "heartbeat_at": { "$add": [ "$$NOW", 60_000_i64 ] },
                },
            } }],
        )
        .await
        .expect("stamp a RUNNING record ahead of the clock");
    let running = fx
        .engine
        .list_running_restore_jobs()
        .await
        .expect("list running");
    assert_eq!(running.len(), 1, "{running:?}");
    assert_eq!(running[0].target.table_name, "rr_target");
    assert_eq!(running[0].target.table_id, table_id);
    assert!(
        fx.engine
            .list_dead_restore_jobs(std::time::Duration::ZERO)
            .await
            .expect("list dead")
            .is_empty(),
        "the age-based listing misses a heartbeat ahead of the clock"
    );
    fx.finish().await;
}

// --- `extenddb backup migrate-legacy`: the item stream, `list_all`, the
// replacement by an index row.

/// The index row of the store copy of the legacy backup `backup_arn`, as
/// the migration builds it.
async fn migrated_row(engine: &MongoEngine, backup_arn: &str) -> BackupRow {
    let source = engine
        .restore_source(ACCOUNT, backup_arn)
        .await
        .expect("source");
    let table = extenddb_storage::backup_definition::source_table_from_legacy(
        &source,
        extenddb_storage::backup_definition::LegacyVectorState::Unsupported,
        REGION,
        ACCOUNT,
    )
    .expect("source table");
    BackupRow {
        backup_id: BackupArn::parse(backup_arn)
            .expect("arn")
            .backup_id()
            .to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_name: source.source_table_name.clone(),
        backup_arn: backup_arn.to_owned(),
        backup_name: "b".to_owned(),
        status: BackupStatus::Available,
        store: store(),
        size_bytes: 4321,
        item_count: source.item_count,
        created_at: source.backup_creation_date_time,
        completed_at: None,
        failure_reason: None,
        source_table_details_json: serde_json::to_value(&table).expect("json"),
        owner_generation: 77,
        heartbeat_at: 0.0,
    }
}

/// A legacy backup `b` of a new table `name` with `items` items, and the
/// name of its item collection.
async fn legacy_backup(engine: &MongoEngine, name: &str, items: usize) -> (String, String) {
    source_table_with(engine, name, items).await;
    let arn = engine
        .create(ACCOUNT, name, "b", JobPermit::detached())
        .await
        .expect("backup")
        .backup_arn;
    let collection = super::backup_collection_name(
        backup_meta(engine, &arn)
            .await
            .get_str("backup_id")
            .expect("backup_id"),
    );
    (arn, collection)
}

/// The items of `collection`, by their JSON text, sorted.
async fn collection_items(engine: &MongoEngine, collection: &str) -> Vec<String> {
    let docs: Vec<Document> = engine
        .data_db
        .collection::<Document>(collection)
        .find(doc! {})
        .await
        .expect("find")
        .try_collect()
        .await
        .expect("documents");
    let mut items: Vec<String> = docs
        .iter()
        .map(|d| serde_json::to_string(&document_to_item(d).expect("item")).expect("text"))
        .collect();
    items.sort();
    items
}

/// The stream reads every document of the backup's collection, past one
/// fetch batch (2,500 items, batches of 1,000), decoded as the data path
/// decodes them; it carries the backup's name and source, and the vector
/// state of a backend without vector support.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_legacy_items_stream_every_document_of_the_backup_collection() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let (arn, collection) = legacy_backup(&fx.engine, "src", 2_500).await;
        let opened = fx.engine.items(ACCOUNT, &arn).await.expect("items");
        assert_eq!(opened.backup_name, "b");
        assert_eq!(
            opened.source,
            fx.engine
                .restore_source(ACCOUNT, &arn)
                .await
                .expect("source")
        );
        assert_eq!(
            opened.vector_state,
            extenddb_storage::backup_definition::LegacyVectorState::Unsupported
        );
        let mut streamed: Vec<String> = opened
            .items
            .map(|item| serde_json::to_string(&item.expect("an item")).expect("text"))
            .collect()
            .await;
        streamed.sort();
        let expected = collection_items(&fx.engine, &collection).await;
        assert_eq!(expected.len(), 2_500);
        assert_eq!(streamed, expected);
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

/// `items` refuses another account's backup, a missing one and a backup
/// with no `backup_id` (no item collection), and a DeleteBackup's drop
/// ends a running stream with an error.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_legacy_items_refuse_what_cannot_be_read_and_end_at_a_drop() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let (arn, _) = legacy_backup(&fx.engine, "src", 2_500).await;
        for (account, backup_arn) in [(OTHER_ACCOUNT, arn.clone()), (ACCOUNT, flip_last_hex(&arn))]
        {
            let err = fx.engine.items(account, &backup_arn).await.err();
            assert!(
                matches!(&err, Some(StorageError::Validation(m)) if m.contains("Backup not found")),
                "{err:?}"
            );
        }
        let mut items = fx.engine.items(ACCOUNT, &arn).await.expect("items").items;
        assert!(items.next().await.expect("one").is_ok());
        fx.engine.delete(ACCOUNT, &arn).await.expect("delete");
        let mut read = 1;
        let mut ended_with = None;
        while let Some(item) = items.next().await {
            match item {
                Ok(_) => read += 1,
                Err(e) => {
                    ended_with = Some(e);
                    break;
                }
            }
        }
        assert!(ended_with.is_some(), "the drop ends the stream");
        assert!(read < 2_500, "{read} items read");
        assert!(items.next().await.is_none(), "nothing follows the error");

        let (old, _) = legacy_backup(&fx.engine, "old", 3).await;
        fx.engine
            .catalog_db
            .collection::<Document>("backups")
            .update_one(doc! { "_id": &old }, doc! { "$unset": { "backup_id": "" } })
            .await
            .expect("a backup from before $out");
        let err = fx.engine.items(ACCOUNT, &old).await.err();
        assert!(
            matches!(&err, Some(StorageError::Unsupported(m)) if m.contains("no item collection")),
            "{err:?}"
        );
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_list_all_lists_every_account_and_leaves_deleted_backups_out() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let mut arns = Vec::new();
        for name in ["a", "b", "c", "d"] {
            arns.push(legacy_backup(&fx.engine, name, 2).await.0);
        }
        let backups = fx.engine.catalog_db.collection::<Document>("backups");
        backups
            .update_one(
                doc! { "_id": &arns[1] },
                doc! { "$set": { "account_id": OTHER_ACCOUNT } },
            )
            .await
            .expect("move");
        backups
            .update_one(
                doc! { "_id": &arns[2] },
                doc! { "$set": { "backup_status": "DELETING" } },
            )
            .await
            .expect("deleting");
        fx.engine.delete(ACCOUNT, &arns[3]).await.expect("delete");
        let key =
            |account: &str, arn: &str, status| extenddb_storage::backup_catalog::LegacyBackupKey {
                account_id: account.to_owned(),
                backup_arn: arn.to_owned(),
                status,
            };
        assert_eq!(
            fx.engine.list_all().await.expect("list"),
            vec![
                key(ACCOUNT, &arns[0], BackupStatus::Available),
                key(OTHER_ACCOUNT, &arns[1], BackupStatus::Available),
                key(ACCOUNT, &arns[2], BackupStatus::Deleting),
            ]
        );
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_replacement_inserts_the_unowned_row_and_removes_the_backup() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let (arn, collection) = legacy_backup(&fx.engine, "src", 30).await;
        let row = migrated_row(&fx.engine, &arn).await;
        assert!(collection_exists(&fx.engine, &collection).await);
        assert_eq!(
            fx.engine
                .replace_with_index_row(&row)
                .await
                .expect("replace"),
            extenddb_storage::backup_catalog::Replaced::Replaced
        );
        let read = fx
            .engine
            .read_backup_row(&arn_of(&row))
            .await
            .expect("read")
            .expect("the index row");
        assert_eq!(read.owner_generation, UNOWNED_GENERATION);
        assert!(read.completed_at.is_some());
        assert_eq!(read.status, BackupStatus::Available);
        assert_eq!(read.store, row.store);
        assert_eq!(
            read.source_table().expect("source"),
            row.source_table().expect("source")
        );
        let doc = backup_meta(&fx.engine, &arn).await;
        assert_eq!(doc.get_str("backup_status"), Ok("DELETED"));
        assert!(!collection_exists(&fx.engine, &collection).await);
        assert!(fx.engine.describe(ACCOUNT, &arn).await.is_err());
        assert!(
            fx.engine
                .list(ACCOUNT, None)
                .await
                .expect("list")
                .is_empty()
        );
        assert!(fx.engine.list_all().await.expect("list").is_empty());
        assert_eq!(
            fx.engine
                .replace_with_index_row(&row)
                .await
                .expect("replace"),
            extenddb_storage::backup_catalog::Replaced::AlreadyReplaced
        );
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

/// A replacement whose drop fails after the commit leaves the document
/// DELETING with the index row in: DeleteBackup of the legacy ARN reads it
/// as not found and leaves it, `list_all` lists it, and the next
/// replacement finishes it as `AlreadyReplaced`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_replacement_that_stopped_before_its_drop_is_finished_by_the_next() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let (arn, collection) = legacy_backup(&fx.engine, "src", 30).await;
        let row = migrated_row(&fx.engine, &arn).await;
        let faulty = with_drop_fault(&fx.engine, AfterBackupDrop::Fail);
        assert!(faulty.replace_with_index_row(&row).await.is_err());
        let doc = backup_meta(&fx.engine, &arn).await;
        assert_eq!(doc.get_str("backup_status"), Ok("DELETING"));
        assert_eq!(doc.get_bool(super::MIGRATED_TO_STORE), Ok(true));
        assert!(
            fx.engine
                .read_backup_row(&arn_of(&row))
                .await
                .expect("read")
                .is_some()
        );
        let err = fx.engine.delete(ACCOUNT, &arn).await.err();
        assert!(
            matches!(&err, Some(StorageError::Validation(m)) if m.contains("Backup not found")),
            "{err:?}"
        );
        assert_eq!(
            backup_meta(&fx.engine, &arn).await.get_str("backup_status"),
            Ok("DELETING"),
            "DeleteBackup left it"
        );
        assert_eq!(
            fx.engine.list_all().await.expect("list"),
            vec![extenddb_storage::backup_catalog::LegacyBackupKey {
                account_id: ACCOUNT.to_owned(),
                backup_arn: arn.clone(),
                status: BackupStatus::Deleting,
            }]
        );
        // A collection the failed drop left (the fault fires after the drop
        // ran) is dropped again; then DELETED.
        assert_eq!(
            fx.engine
                .replace_with_index_row(&row)
                .await
                .expect("replace"),
            extenddb_storage::backup_catalog::Replaced::AlreadyReplaced
        );
        assert_eq!(
            backup_meta(&fx.engine, &arn).await.get_str("backup_status"),
            Ok("DELETED")
        );
        assert!(!collection_exists(&fx.engine, &collection).await);
        assert!(fx.engine.list_all().await.expect("list").is_empty());
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

/// A replacement that committed and stopped before its drop, then a
/// DeleteBackup of the migrated backup whose index row is removed since
/// (the tombstone duty, or `--forget-backup`): `finish_replacement` finishes
/// the marked DELETING document whatever the index row holds, dropping the
/// collection and writing DELETED; a second call has nothing to finish, and
/// the document counts neither as an in-catalog backup nor as a deleted
/// one.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_stopped_replacement_is_finished_after_its_row_is_gone() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let (arn, collection) = legacy_backup(&fx.engine, "src", 30).await;
        let row = migrated_row(&fx.engine, &arn).await;
        // The fault fires once the drop's command was sent; the copy of the
        // collection a drop that never ran leaves is made here.
        let faulty = with_drop_fault(&fx.engine, AfterBackupDrop::Fail);
        assert!(faulty.replace_with_index_row(&row).await.is_err());
        fx.engine
            .data_db
            .collection::<Document>(&collection)
            .insert_one(doc! { "item_data": { "pk": { "S": "left" } } })
            .await
            .expect("a collection the drop left");
        fx.engine
            .backup_index()
            .delete_one(doc! { "backup_id": &row.backup_id })
            .await
            .expect("the index row removed");
        assert!(
            fx.engine
                .read_backup_row(&arn_of(&row))
                .await
                .expect("read")
                .is_none()
        );
        assert_eq!(
            fx.engine
                .holder_status(ACCOUNT, &arn)
                .await
                .expect("holder"),
            None,
            "the migration replaced it"
        );
        assert!(
            fx.engine
                .finish_replacement(ACCOUNT, &arn)
                .await
                .expect("finish")
        );
        assert_eq!(
            backup_meta(&fx.engine, &arn).await.get_str("backup_status"),
            Ok("DELETED")
        );
        assert!(!collection_exists(&fx.engine, &collection).await);
        assert!(
            !fx.engine
                .finish_replacement(ACCOUNT, &arn)
                .await
                .expect("finish again")
        );
        assert!(fx.engine.list_all().await.expect("list").is_empty());
        assert!(fx.engine.list_deleted().await.expect("deleted").is_empty());
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

/// `holder_status` answers for a backup document in any status, DELETED
/// included, and for none the replacement marked; `list_deleted` lists the
/// documents a DeleteBackup left DELETED and not a replaced one.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_holder_status_and_list_deleted_tell_a_delete_from_a_replacement() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let (deleted, _) = legacy_backup(&fx.engine, "gone", 3).await;
        let (replaced, _) = legacy_backup(&fx.engine, "moved", 3).await;
        assert_eq!(
            fx.engine
                .holder_status(ACCOUNT, &deleted)
                .await
                .expect("holder"),
            Some(BackupStatus::Available)
        );
        fx.engine.delete(ACCOUNT, &deleted).await.expect("delete");
        let row = migrated_row(&fx.engine, &replaced).await;
        assert_eq!(
            fx.engine
                .replace_with_index_row(&row)
                .await
                .expect("replace"),
            extenddb_storage::backup_catalog::Replaced::Replaced
        );
        assert_eq!(
            backup_meta(&fx.engine, &replaced)
                .await
                .get_str("backup_status"),
            Ok("DELETED")
        );
        assert_eq!(
            fx.engine
                .holder_status(ACCOUNT, &deleted)
                .await
                .expect("holder"),
            Some(BackupStatus::Deleted)
        );
        assert_eq!(
            fx.engine
                .holder_status(ACCOUNT, &replaced)
                .await
                .expect("holder"),
            None
        );
        assert_eq!(
            fx.engine
                .holder_status("999999999999", &deleted)
                .await
                .expect("holder"),
            None
        );
        assert_eq!(
            fx.engine.list_deleted().await.expect("deleted"),
            vec![extenddb_storage::backup_catalog::LegacyBackupKey {
                account_id: ACCOUNT.to_owned(),
                backup_arn: deleted.clone(),
                status: BackupStatus::Deleted,
            }]
        );
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

/// A replacement whose transaction committed and whose commit answer was
/// lost settles it by reading back what it wrote: it answers `Replaced`
/// (this call's), and finishes the drop.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_replacement_whose_commit_answer_is_lost_settles_it() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let (arn, collection) = legacy_backup(&fx.engine, "src", 10).await;
        let row = migrated_row(&fx.engine, &arn).await;
        let mut lost = fx.engine.clone();
        lost.backup_liveness.replacement_commit_unknown = true;
        assert_eq!(
            lost.replace_with_index_row(&row).await.expect("replace"),
            extenddb_storage::backup_catalog::Replaced::Replaced
        );
        assert_eq!(
            backup_meta(&fx.engine, &arn).await.get_str("backup_status"),
            Ok("DELETED")
        );
        assert!(!collection_exists(&fx.engine, &collection).await);
        assert!(
            fx.engine
                .read_backup_row(&arn_of(&row))
                .await
                .expect("read")
                .is_some()
        );
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}

/// The replacement refuses, writing nothing, while a restore target of the
/// backup is CREATING, when the backup is not AVAILABLE or missing, and
/// when another store's row holds its id; a row `backup sync` adopted is
/// finished.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_replacement_refuses_an_in_use_or_unavailable_backup_and_writes_nothing() {
    use extenddb_storage::backup_catalog::Replaced;
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        let (arn, collection) = legacy_backup(&fx.engine, "src", 30).await;
        let row = migrated_row(&fx.engine, &arn).await;
        let (gated, gate) = gated(&fx.engine);
        let request = restore_request(&fx.engine, &arn, "dst").await;
        gated
            .restore(ACCOUNT, request, JobPermit::detached())
            .await
            .expect("registers");
        assert_eq!(
            fx.engine
                .replace_with_index_row(&row)
                .await
                .expect("replace"),
            Replaced::InUse("dst".to_owned())
        );
        assert_eq!(
            backup_meta(&fx.engine, &arn).await.get_str("backup_status"),
            Ok("AVAILABLE")
        );
        assert!(collection_exists(&fx.engine, &collection).await);
        assert!(
            fx.engine
                .read_backup_row(&arn_of(&row))
                .await
                .expect("read")
                .is_none()
        );
        gate.add_permits(1);
        assert_fully_restored_with(&fx.engine, "dst", 30).await;

        // Another store's row holds the id.
        let mut elsewhere = row.clone();
        elsewhere.store.store_root = "/srv/elsewhere".to_owned();
        assert!(
            fx.engine
                .insert_backup_row(&elsewhere)
                .await
                .expect("insert")
        );
        let err = fx.engine.replace_with_index_row(&row).await.err();
        assert!(
            matches!(&err, Some(StorageError::AlreadyExists(m)) if m.contains("/srv/elsewhere")),
            "{err:?}"
        );
        assert_eq!(
            backup_meta(&fx.engine, &arn).await.get_str("backup_status"),
            Ok("AVAILABLE")
        );
        assert!(collection_exists(&fx.engine, &collection).await);

        // A copy `backup sync` adopted: finished.
        let (adopted, adopted_collection) = legacy_backup(&fx.engine, "adopted", 3).await;
        let adopted_row = migrated_row(&fx.engine, &adopted).await;
        assert!(
            fx.engine
                .insert_backup_row(&adopted_row)
                .await
                .expect("insert")
        );
        assert_eq!(
            fx.engine
                .replace_with_index_row(&adopted_row)
                .await
                .expect("replace"),
            Replaced::AlreadyReplaced
        );
        assert_eq!(
            backup_meta(&fx.engine, &adopted)
                .await
                .get_str("backup_status"),
            Ok("DELETED")
        );
        assert!(!collection_exists(&fx.engine, &adopted_collection).await);

        // Not AVAILABLE, then missing.
        let (gone, _) = legacy_backup(&fx.engine, "gone", 3).await;
        let gone_row = migrated_row(&fx.engine, &gone).await;
        fx.engine.delete(ACCOUNT, &gone).await.expect("delete");
        assert_eq!(
            fx.engine
                .replace_with_index_row(&gone_row)
                .await
                .expect("replace"),
            Replaced::NotAvailable("the in-catalog backup is DELETED".to_owned())
        );
        let mut missing = gone_row.clone();
        missing.backup_arn = flip_last_hex(&gone);
        missing.backup_id = BackupArn::parse(&missing.backup_arn)
            .unwrap()
            .backup_id()
            .to_owned();
        assert!(matches!(
            fx.engine.replace_with_index_row(&missing).await.expect("replace"),
            Replaced::NotAvailable(m) if m.contains("Backup not found")
        ));
        assert!(
            fx.engine
                .read_backup_row(&arn_of(&gone_row))
                .await
                .expect("read")
                .is_none()
        );
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}
