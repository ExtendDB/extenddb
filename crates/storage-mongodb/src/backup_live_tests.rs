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
    BackupRow, BackupStatus, DeleteStart, Heartbeat, JobStatus, RestoreKind, RestoreTargetKey,
    StatusFields, StoreIdentity, StoreKind, TimeBounds, UNOWNED_GENERATION, WriteOutcome,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupCatalog, JobPermit, LegacyBackupReader, RestoreRequest, RestoreTableOverrides,
    TableEngine,
};
use futures::{FutureExt, TryStreamExt};
use mongodb::bson::{self, Document, doc};

use super::{AfterBackupDrop, BackupLiveness, HeartbeatFault};
use crate::MongoEngine;
use crate::backup_catalog::{BACKUP_INDEX, RESTORE_JOB, RESTORE_JOB_HEARTBEAT, RESTORE_JOB_STATUS};
use crate::bootstrapper::{CATALOG_VERSION, apply_backup_catalog_index, require_served_catalog};
use crate::data::{data_collection_name, item_to_document};

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

fn test_liveness() -> BackupLiveness {
    BackupLiveness {
        heartbeat_interval: Duration::from_millis(200),
        registration_gate: None,
        copy_gate: None,
        after_backup_drop: None,
        target_drop_fails: false,
        local_clock_behind: Duration::ZERO,
        heartbeat_fault: None,
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
        // The catalog index step alone (`002_backup_catalog_index`, as
        // `init` and `migrate` run it): the collections and indexes of the
        // initial step are not created here, so this catalog is not `init`'s;
        // the tests of this file need only what the step creates.
        apply_backup_catalog_index(&engine.catalog_db)
            .await
            .expect("catalog index step");
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
    tokio::spawn(crate::ttl_worker::gsi_backfill_worker(engine))
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
    assert!(
        !fx.engine.remove_backup_row(&key).await.unwrap(),
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
    assert!(fx.engine.remove_backup_row(&key).await.unwrap());
    assert!(!fx.engine.remove_backup_row(&key).await.unwrap());
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

/// A catalog at the shape before the catalog index step, holding a finished
/// restore, a restore handed to the GSI backfill worker at the stop (its
/// copy complete, the target CREATING with `restore_backfill_pending`) and a
/// restore the stop interrupted (its target CREATING, no flag), migrates with
/// the registrations backfilled: the finished and the handed-over ones DONE,
/// the interrupted one RUNNING with `heartbeat_at` at the epoch and the
/// unowned generation, so the reconciler's first pass fails it and removes
/// its target and leaves the handed-over one to the worker. The step writes
/// the version and keeps the catalog id; a second run changes nothing.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_the_migration_step_backfills_the_registrations_and_the_reconciler_removes_the_interrupted_one()
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

    // Back to the shape before the step: the registrations as the earlier
    // release wrote them, the step forgotten, the version before it. The
    // interrupted restore's copy task loses its claim with the record and
    // stops.
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
    let settings = fx.engine.catalog_db.collection::<Document>("settings");
    settings
        .update_one(
            doc! { "_id": "catalog_version" },
            doc! { "$set": { "value": "0.0.2" } },
        )
        .await
        .expect("old version");
    fx.engine
        .catalog_db
        .collection::<Document>("schema_history")
        .delete_one(doc! { "_id": "002_backup_catalog_index" })
        .await
        .expect("forget the step");

    let report = apply_backup_catalog_index(&fx.engine.catalog_db)
        .await
        .expect("migrate again");
    assert_eq!(report.registrations_backfilled, 3);
    assert!(!report.catalog_id_written, "the catalog id is kept");
    assert_eq!(fx.engine.catalog_id().await.expect("id"), catalog_id);
    assert_eq!(
        settings
            .find_one(doc! { "_id": "catalog_version" })
            .await
            .expect("read")
            .and_then(|d| d.get_str("value").ok().map(str::to_owned))
            .as_deref(),
        Some(CATALOG_VERSION)
    );
    assert!(
        fx.engine
            .catalog_db
            .collection::<Document>("schema_history")
            .find_one(doc! { "_id": "002_backup_catalog_index" })
            .await
            .expect("read")
            .is_some()
    );

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
    let again = apply_backup_catalog_index(&fx.engine.catalog_db)
        .await
        .expect("a second run");
    assert_eq!(again.registrations_backfilled, 0);
    assert!(!again.catalog_id_written);

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
            .fail_restore_job(&dead[0].target, "owner dead")
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

/// `serve` refuses a catalog whose `catalog_version` is not this binary's,
/// naming `extenddb migrate --yes`; the step that brings it forward makes the
/// same catalog acceptable again.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_serve_refuses_a_catalog_at_an_older_version() {
    let fx = Fixture::new().await;
    require_served_catalog(&fx.engine.catalog_db)
        .await
        .expect("a migrated catalog is served");

    let settings = fx.engine.catalog_db.collection::<Document>("settings");
    settings
        .update_one(
            doc! { "_id": "catalog_version" },
            doc! { "$set": { "value": "0.0.2" } },
        )
        .await
        .expect("old version");
    let refusal = require_served_catalog(&fx.engine.catalog_db)
        .await
        .expect_err("a 0.0.2 catalog is refused")
        .to_string();
    assert!(refusal.contains("extenddb migrate --yes"), "{refusal}");
    assert!(refusal.contains("at version 0.0.2"), "{refusal}");
    assert!(refusal.contains(CATALOG_VERSION), "{refusal}");

    settings
        .delete_one(doc! { "_id": "catalog_version" })
        .await
        .expect("no version row");
    assert!(
        matches!(
            require_served_catalog(&fx.engine.catalog_db).await,
            Err(extenddb_storage::migrations::MigrationError::Status(
                extenddb_storage::migrations::CatalogStatus::Uninitialized
            ))
        ),
        "a catalog without a version row is not a catalog"
    );

    apply_backup_catalog_index(&fx.engine.catalog_db)
        .await
        .expect("migrate");
    require_served_catalog(&fx.engine.catalog_db)
        .await
        .expect("served again once migrated");
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
            .fail_restore_job(&key, "owner dead")
            .await
            .expect("fail")
    );
    let job = job_record(&fx.engine, "dst").await;
    assert_eq!(job.get_str("status"), Ok("FAILED"));
    assert_eq!(job.get_str("failure_reason"), Ok("owner dead"));
    assert!(job.get_datetime("completed_at").is_ok());
    assert!(
        !fx.engine
            .fail_restore_job(&key, "owner dead")
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
            .fail_restore_job(&key, "owner dead")
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
            .fail_restore_job(&key, "owner dead")
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
                Err(StorageError::BackupInUse(m)) => assert!(
                    m.starts_with("Backup is being used to restore table dst"),
                    "restore {i}: {m}"
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
                "Backup is being used to restore table {winner}: {}",
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
