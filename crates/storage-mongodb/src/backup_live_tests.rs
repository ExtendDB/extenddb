// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for backup and restore: the detached copy, the
//! heartbeat sweep, and the DeleteBackup/DeleteTable refusals during a copy.
//!
//! They need a replica set (the data path uses transactions) and read its URI
//! from `EXTENDDB_TEST_MONGODB_URI`, for example
//! `mongodb://127.0.0.1:27741/?directConnection=true`. They are `#[ignore]`d so
//! `cargo test --lib` passes without one; run them with
//! `cargo test -p extenddb-storage-mongodb --lib -- --ignored live_`.
//! A run that sets `--ignored` without the variable fails rather than skips.
//!
//! Each test uses its own catalog and data databases (random names) and drops
//! them at the end, so tests run in parallel and leave nothing on success. Each
//! runs its own GSI backfill worker, as `serve` does, and every engine uses
//! shortened heartbeat timing (200 ms refresh, 1.5 s stale), which the tests
//! read from the engine rather than assume.

use std::sync::Arc;
use std::time::{Duration, Instant};

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DeleteTableInput,
    DescribeTableInput, GsiInput, Item, KeySchemaElement, KeyType, LsiInput, Projection,
    ProjectionType, ScalarAttributeType, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, RestoreRequest, RestoreTableOverrides, TableEngine};
use futures::FutureExt;
use mongodb::bson::{self, Document, doc};

use super::{
    AfterBackupDrop, BackupLiveness, DELETE_CLAIM, DELETE_CLAIM_HEARTBEAT, RESTORE_CLAIM,
    RESTORE_CLAIM_HEARTBEAT, RestoreProvenance, TARGET_HEARTBEAT, claim_update,
    restore_claim_filter,
};
use crate::MongoEngine;
use crate::data::{data_collection_name, item_to_document};

const ACCOUNT: &str = "123456789012";
const ITEMS: usize = 3_000;

fn uri() -> String {
    std::env::var("EXTENDDB_TEST_MONGODB_URI").expect(
        "set EXTENDDB_TEST_MONGODB_URI to a MongoDB replica set, e.g. \
         mongodb://127.0.0.1:27741/?directConnection=true",
    )
}

fn test_liveness() -> BackupLiveness {
    BackupLiveness {
        heartbeat_interval: Duration::from_millis(200),
        stale_after: Duration::from_millis(1_500),
        slot_wait: Duration::from_secs(2),
        copy_gate: None,
        after_backup_drop: None,
        local_clock_behind: Duration::ZERO,
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
    // The driver default pool (10) and the slot count production derives
    // from it, so the tests see the same admission bound as `serve`.
    let slots = super::restore_server_slot_count(10);
    MongoEngine {
        client: client.clone(),
        catalog_db: client.database(catalog),
        data_db: client.database(data),
        region: "us-east-1".to_owned(),
        gsi_cache: Arc::default(),
        tx_read_concern: mongodb::options::ReadConcern::snapshot(),
        backup_liveness: liveness,
        restore_server_slot_count: slots,
        restore_server_slots: Arc::new(tokio::sync::Semaphore::new(slots)),
        restore_account_slots: super::RestoreAccountSlots::default(),
        contention_deadline: crate::data_engine::CONTENTION_DEADLINE,
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
        .backup_restore_source(account, backup_arn)
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

async fn table_doc(engine: &MongoEngine, name: &str) -> Option<Document> {
    engine
        .catalog_db
        .collection::<Document>("tables")
        .find_one(doc! { "_id": { "account_id": ACCOUNT, "table_name": name } })
        .await
        .expect("read table")
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
    assert!(!doc.contains_key(TARGET_HEARTBEAT));
    assert!(!doc.contains_key("restore_operation_id"));
}

fn gated(engine: &MongoEngine) -> (MongoEngine, Arc<tokio::sync::Semaphore>) {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut gated = engine.clone();
    gated.backup_liveness.copy_gate = Some(gate.clone());
    (gated, gate)
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
        .create_backup(ACCOUNT, "src", "b")
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    let (gated, gate) = gated(&fx.engine);

    let desc = tokio::time::timeout(
        Duration::from_secs(20),
        gated.restore_table_from_backup(ACCOUNT, request),
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
/// complete restore, whether the poll parked in the registration or in the
/// wait for an account slot.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_dropped_restore_request_still_reaches_active() {
    let fx = Fixture::new().await;
    source_table(&fx.engine, "src").await;
    let backup = fx
        .engine
        .create_backup(ACCOUNT, "src", "b")
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    let mut future = fx.engine.restore_table_from_backup(ACCOUNT, request);
    assert!(
        (&mut future).now_or_never().is_none(),
        "the first poll must not complete the restore"
    );
    drop(future);
    assert_fully_restored(&fx.engine, "dst").await;

    // Every account slot is held, so the request's first poll parks in the
    // admission wait; the slots are freed only after the future is gone.
    let account_slots = fx.engine.restore_account_slots.for_account(ACCOUNT);
    let held = Arc::clone(&account_slots)
        .acquire_many_owned(u32::try_from(account_slots.available_permits()).expect("u32"))
        .await
        .expect("hold every account slot");
    let backup = fx
        .engine
        .create_backup(ACCOUNT, "src", "b2")
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst2").await;
    let mut future = fx.engine.restore_table_from_backup(ACCOUNT, request);
    assert!(
        (&mut future).now_or_never().is_none(),
        "the first poll must not complete the restore"
    );
    drop(future);
    assert!(
        table_doc(&fx.engine, "dst2").await.is_none(),
        "nothing is created while the restore waits for a slot"
    );
    drop(held);
    assert_fully_restored(&fx.engine, "dst2").await;
    fx.finish().await;
}

/// Leave the state a process killed mid-restore leaves: the backup's restore
/// claim and a CREATING target with provenance, both with a heartbeat far in
/// the past, and no task.
async fn simulate_crashed_restore(engine: &MongoEngine, backup_arn: &str, target: &str) -> String {
    let dead = "dead-operation";
    let backups = engine.catalog_db.collection::<Document>("backups");
    let claimed = backups
        .find_one_and_update(
            restore_claim_filter(ACCOUNT, backup_arn),
            claim_update(RESTORE_CLAIM, RESTORE_CLAIM_HEARTBEAT, dead),
        )
        .await
        .expect("claim")
        .expect("claimable");
    let request = restore_request(engine, backup_arn, target).await;
    let desc = engine
        .create_table_for_restore(
            ACCOUNT,
            request.input,
            &RestoreProvenance {
                backup_arn,
                source_table_arn: &request.source_table_arn,
                restore_date_time: bson::DateTime::now(),
                operation_id: dead,
                first_heartbeat: bson::DateTime::now(),
                inserted: None,
            },
        )
        .await
        .expect("target");
    drop(claimed);
    let long_ago = bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 600_000);
    let mut set = Document::new();
    set.insert(RESTORE_CLAIM_HEARTBEAT, long_ago);
    backups
        .update_one(doc! { "_id": backup_arn }, doc! { "$set": set })
        .await
        .expect("age claim");
    let mut set = Document::new();
    set.insert(TARGET_HEARTBEAT, long_ago);
    engine
        .catalog_db
        .collection::<Document>("tables")
        .update_one(
            doc! { "_id": { "account_id": ACCOUNT, "table_name": target } },
            doc! { "$set": set },
        )
        .await
        .expect("age target");
    desc.table_id
}

/// (c) A crashed restore is swept completely; DeleteBackup of its source then
/// succeeds and the target name can be restored into.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_an_abandoned_restore_is_swept_and_frees_the_backup_and_the_name() {
    let fx = Fixture::new().await;
    source_table(&fx.engine, "src").await;
    let crashed = fx
        .engine
        .create_backup(ACCOUNT, "src", "b1")
        .await
        .expect("backup 1");
    let spare = fx
        .engine
        .create_backup(ACCOUNT, "src", "b2")
        .await
        .expect("backup 2");
    // The periodic sweep in the fixture's worker would race this test's own
    // sweep; stop it so the assertions see exactly one sweep's result.
    fx.worker.abort();
    let table_id = simulate_crashed_restore(&fx.engine, &crashed.backup_arn, "dst").await;
    let indexes = index_ids(&fx.engine, &table_id).await;
    assert_eq!(indexes.len(), 2);
    assert!(collection_exists(&fx.engine, &data_collection_name(&table_id)).await);

    let before = fx.engine.delete_backup(ACCOUNT, &crashed.backup_arn).await;
    assert!(
        matches!(before, Err(StorageError::BackupInUse(_))),
        "the crashed restore blocks DeleteBackup before the sweep: {before:?}"
    );

    let sweep = fx.engine.sweep_stale_backup_claims().await.expect("sweep");
    assert_eq!(
        sweep.restore_claims_cleared,
        std::slice::from_ref(&crashed.backup_arn)
    );
    assert_eq!(
        sweep.restore_targets_removed,
        [format!("{ACCOUNT}/dst")],
        "the abandoned target must be removed"
    );
    assert!(table_doc(&fx.engine, "dst").await.is_none());
    assert!(!collection_exists(&fx.engine, &data_collection_name(&table_id)).await);
    for (_, index_id, _) in &indexes {
        assert!(!collection_exists(&fx.engine, &data_collection_name(index_id)).await);
    }
    assert!(index_ids(&fx.engine, &table_id).await.is_empty());
    let meta = fx
        .engine
        .catalog_db
        .collection::<Document>("backups")
        .find_one(doc! { "_id": &crashed.backup_arn })
        .await
        .expect("read")
        .expect("backup");
    assert!(!meta.contains_key(RESTORE_CLAIM) && !meta.contains_key(RESTORE_CLAIM_HEARTBEAT));

    let deleted = fx
        .engine
        .delete_backup(ACCOUNT, &crashed.backup_arn)
        .await
        .expect("DeleteBackup after the sweep");
    assert_eq!(deleted.backup_details.backup_status, "DELETED");

    let worker = spawn_worker(&fx.engine);
    let request = restore_request(&fx.engine, &spare.backup_arn, "dst").await;
    fx.engine
        .restore_table_from_backup(ACCOUNT, request)
        .await
        .expect("restore into the freed name");
    assert_fully_restored(&fx.engine, "dst").await;
    worker.abort();
    fx.finish().await;
}

/// (d) Claims and restore targets of another, live, instance on the same
/// catalog survive this instance's sweep for many stale windows, because their
/// heartbeats keep moving; once the owner stops, the sweep removes them.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_live_claim_of_another_instance_survives_this_instances_sweep() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    source_table(&fx.engine, "src").await;
    let b1 = fx
        .engine
        .create_backup(ACCOUNT, "src", "b1")
        .await
        .expect("backup 1");
    let b2 = fx
        .engine
        .create_backup(ACCOUNT, "src", "b2")
        .await
        .expect("backup 2");
    let other = fx.second_instance(test_liveness()).await;
    let stale_after = fx.engine.backup_liveness.stale_after;
    assert!(other.backup_liveness.heartbeat_interval * 3 < stale_after);

    // A live delete claim on b1, held the way delete_backup holds it.
    let op = "live-delete";
    let heartbeat = other.start_backup_heartbeat(op, ACCOUNT, &b1.backup_arn, None);
    other
        .catalog_db
        .collection::<Document>("backups")
        .update_one(
            doc! { "_id": &b1.backup_arn },
            claim_update(DELETE_CLAIM, DELETE_CLAIM_HEARTBEAT, op),
        )
        .await
        .expect("claim");
    // A live restore target restoring b2, held before its copy.
    let (gated_other, gate) = gated(&other);
    let request = restore_request(&other, &b2.backup_arn, "dst").await;
    gated_other
        .restore_table_from_backup(ACCOUNT, request)
        .await
        .expect("restore");

    let backups = fx.engine.catalog_db.collection::<Document>("backups");
    let claim_heartbeat = || async {
        backups
            .find_one(doc! { "_id": &b1.backup_arn })
            .await
            .expect("read")
            .and_then(|d| d.get_datetime(DELETE_CLAIM_HEARTBEAT).ok().copied())
    };
    let target_heartbeat = || async {
        table_doc(&fx.engine, "dst")
            .await
            .and_then(|d| d.get_datetime(TARGET_HEARTBEAT).ok().copied())
    };
    let first_claim = claim_heartbeat().await.expect("claim heartbeat");
    let first_target = target_heartbeat().await.expect("target heartbeat");

    // Sweep repeatedly across three stale windows.
    let until = Instant::now() + stale_after * 3;
    let mut sweeps = 0;
    while Instant::now() < until {
        let sweep = fx.engine.sweep_stale_backup_claims().await.expect("sweep");
        assert!(sweep.is_empty(), "a live owner's state was swept: {sweep}");
        sweeps += 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(sweeps >= 10);
    let meta = backups
        .find_one(doc! { "_id": &b1.backup_arn })
        .await
        .expect("read")
        .expect("backup");
    assert_eq!(
        meta.get_str(DELETE_CLAIM),
        Ok(op),
        "the live claim must still be held"
    );
    assert!(
        table_doc(&fx.engine, "dst").await.is_some(),
        "the live target must remain"
    );
    // Liveness: both heartbeats moved.
    assert!(claim_heartbeat().await.expect("claim heartbeat") > first_claim);
    assert!(target_heartbeat().await.expect("target heartbeat") > first_target);
    let in_use = fx.engine.delete_backup(ACCOUNT, &b2.backup_arn).await;
    assert!(
        matches!(in_use, Err(StorageError::BackupInUse(_))),
        "{in_use:?}"
    );

    // The owner of the delete claim stops: the sweep takes the claim over and
    // finishes the delete once the heartbeat is stale.
    drop(heartbeat);
    tokio::time::sleep(stale_after + Duration::from_millis(500)).await;
    let sweep = fx.engine.sweep_stale_backup_claims().await.expect("sweep");
    assert_eq!(
        sweep.delete_claims_finished,
        std::slice::from_ref(&b1.backup_arn)
    );
    assert!(sweep.restore_targets_removed.is_empty(), "{sweep}");

    // The restore finishes normally.
    let worker = spawn_worker(&fx.engine);
    gate.add_permits(1);
    assert_fully_restored(&fx.engine, "dst").await;
    worker.abort();
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
        .create_backup(ACCOUNT, "src", "b")
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    let (gated, gate) = gated(&fx.engine);
    gated
        .restore_table_from_backup(ACCOUNT, request)
        .await
        .expect("restore");

    let in_use = fx.engine.delete_backup(ACCOUNT, &backup.backup_arn).await;
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
        .delete_backup(ACCOUNT, &backup.backup_arn)
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
                .create_backup(ACCOUNT, "src", &format!("b{i}"))
                .await
                .expect("backup"),
        );
    }
    for b in &backups {
        let source = fx
            .engine
            .backup_restore_source(ACCOUNT, &b.backup_arn)
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
            .describe_backup(ACCOUNT, &b.backup_arn)
            .await
            .expect("describe");
        assert_eq!(
            described.backup_details.backup_creation_date_time,
            b.backup_creation_date_time
        );
        assert!(described.source_table_feature_details.is_none());
    }
    let listed = fx.engine.list_backups(ACCOUNT, None).await.expect("list");
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
        .backup_restore_source("999999999999", &backups[0].backup_arn)
        .await;
    assert!(
        matches!(&foreign, Err(StorageError::Validation(m)) if m.starts_with("Backup not found")),
        "{foreign:?}"
    );
    fx.finish().await;
}

/// The GSI backfill worker's periodic sweep, with no explicit sweep call and
/// no restart, removes a crashed restore and finishes an interrupted
/// DeleteBackup.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_the_worker_sweeps_abandoned_state_without_a_restart() {
    let fx = Fixture::new().await;
    source_table(&fx.engine, "src").await;
    let crashed = fx
        .engine
        .create_backup(ACCOUNT, "src", "b1")
        .await
        .expect("backup 1");
    let interrupted = fx
        .engine
        .create_backup(ACCOUNT, "src", "b2")
        .await
        .expect("backup 2");
    simulate_crashed_restore(&fx.engine, &crashed.backup_arn, "dst").await;
    let mut set = doc! { DELETE_CLAIM: "dead-delete" };
    set.insert(
        DELETE_CLAIM_HEARTBEAT,
        bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 600_000),
    );
    fx.engine
        .catalog_db
        .collection::<Document>("backups")
        .update_one(
            doc! { "_id": &interrupted.backup_arn },
            doc! { "$set": set },
        )
        .await
        .expect("stale delete claim");

    // Worst case: one heartbeat interval until the sweep is due, plus the
    // worker's 5 s tick; the state is already older than stale_after.
    let deadline =
        Instant::now() + fx.engine.backup_liveness.heartbeat_interval + Duration::from_secs(15);
    loop {
        let target_gone = table_doc(&fx.engine, "dst").await.is_none();
        let status = fx
            .engine
            .catalog_db
            .collection::<Document>("backups")
            .find_one(doc! { "_id": &interrupted.backup_arn })
            .await
            .expect("read")
            .and_then(|d| d.get_str("backup_status").ok().map(str::to_owned));
        if target_gone && status.as_deref() == Some("DELETED") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the worker did not sweep: target gone {target_gone}, b2 status {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    fx.engine
        .delete_backup(ACCOUNT, &crashed.backup_arn)
        .await
        .expect("DeleteBackup after the worker's sweep");
    fx.finish().await;
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

/// After a DeleteBackup or sweep stopped after its drop: the claim is still
/// held, the backup's data is gone, the backup is not restorable, and once the
/// claim is stale the sweep finishes the delete.
async fn assert_claim_held_then_swept(engine: &MongoEngine, backup_arn: &str, backup_id: &str) {
    // Give a claim's Drop (a spawned release) time to run if it were armed.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let meta = backup_meta(engine, backup_arn).await;
    assert!(
        meta.contains_key(DELETE_CLAIM),
        "the delete claim must not be released once the drop may have run: {meta:?}"
    );
    assert_eq!(meta.get_str("backup_status"), Ok("AVAILABLE"));
    assert!(!collection_exists(engine, &format!("_backup_{backup_id}")).await);
    let request = restore_request(engine, backup_arn, "dst").await;
    let refused = engine.restore_table_from_backup(ACCOUNT, request).await;
    assert!(
        matches!(&refused, Err(StorageError::BackupInUse(_))),
        "a restore must not start from a backup whose data was dropped: {refused:?}"
    );
    assert!(table_doc(engine, "dst").await.is_none());
    let operation = meta.get_str(DELETE_CLAIM).expect("claim id").to_owned();
    let second = engine.delete_backup(ACCOUNT, backup_arn).await;
    assert!(
        matches!(&second, Err(StorageError::BackupInUse(m)) if m.starts_with("Backup is being deleted")),
        "a DeleteBackup during a held delete claim must answer BackupInUse: {second:?}"
    );
    let meta = backup_meta(engine, backup_arn).await;
    assert_eq!(meta.get_str(DELETE_CLAIM), Ok(operation.as_str()));
    assert_eq!(meta.get_str("backup_status"), Ok("AVAILABLE"));

    tokio::time::sleep(engine.backup_liveness.stale_after + Duration::from_millis(500)).await;
    let sweep = engine.sweep_stale_backup_claims().await.expect("sweep");
    assert_eq!(sweep.delete_claims_finished, [backup_arn.to_owned()]);
    let meta = backup_meta(engine, backup_arn).await;
    assert_eq!(meta.get_str("backup_status"), Ok("DELETED"));
    assert!(!meta.contains_key(DELETE_CLAIM));
}

/// The reply to DeleteBackup's drop is an error after the server dropped
/// the collection. The request fails, but the claim stays until the sweep
/// finishes the delete.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_delete_whose_drop_reply_fails_keeps_its_claim() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    source_table(&fx.engine, "src").await;
    let backup = fx
        .engine
        .create_backup(ACCOUNT, "src", "b")
        .await
        .expect("backup");
    let backup_id = backup_meta(&fx.engine, &backup.backup_arn)
        .await
        .get_str("backup_id")
        .expect("backup_id")
        .to_owned();
    let faulty = with_drop_fault(&fx.engine, AfterBackupDrop::Fail);
    let failed = faulty.delete_backup(ACCOUNT, &backup.backup_arn).await;
    assert!(
        matches!(&failed, Err(StorageError::Internal(_))),
        "{failed:?}"
    );
    assert_claim_held_then_swept(&fx.engine, &backup.backup_arn, &backup_id).await;
    fx.finish().await;
}

/// The DeleteBackup future is dropped (a client timeout) while it waits
/// for the reply to a drop the server already ran.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_delete_dropped_while_waiting_for_its_drop_keeps_its_claim() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    source_table(&fx.engine, "src").await;
    let backup = fx
        .engine
        .create_backup(ACCOUNT, "src", "b")
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
        faulty.delete_backup(ACCOUNT, &backup.backup_arn),
    )
    .await;
    assert!(timed_out.is_err(), "the delete must hang after its drop");
    assert_claim_held_then_swept(&fx.engine, &backup.backup_arn, &backup_id).await;
    fx.finish().await;
}

/// B1 addendum: the sweep's own delete takeover gets an error reply after its
/// drop ran. The claim it took over stays held, and the next sweep finishes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_sweep_whose_drop_reply_fails_keeps_the_claim() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    source_table(&fx.engine, "src").await;
    let backup = fx
        .engine
        .create_backup(ACCOUNT, "src", "b")
        .await
        .expect("backup");
    let backup_id = backup_meta(&fx.engine, &backup.backup_arn)
        .await
        .get_str("backup_id")
        .expect("backup_id")
        .to_owned();
    let mut set = doc! { DELETE_CLAIM: "dead-delete" };
    set.insert(
        DELETE_CLAIM_HEARTBEAT,
        bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 600_000),
    );
    fx.engine
        .catalog_db
        .collection::<Document>("backups")
        .update_one(doc! { "_id": &backup.backup_arn }, doc! { "$set": set })
        .await
        .expect("stale delete claim");

    let faulty = with_drop_fault(&fx.engine, AfterBackupDrop::Fail);
    let failed = faulty.sweep_stale_backup_claims().await;
    assert!(
        matches!(&failed, Err(StorageError::Internal(_))),
        "{failed:?}"
    );
    let meta = backup_meta(&fx.engine, &backup.backup_arn).await;
    assert_ne!(
        meta.get_str(DELETE_CLAIM),
        Ok("dead-delete"),
        "the sweep took the claim over before dropping"
    );
    assert_claim_held_then_swept(&fx.engine, &backup.backup_arn, &backup_id).await;
    fx.finish().await;
}

/// The sweep leaves a stale delete claim alone while a restore target of the
/// backup is CREATING (the DeleteBackup died between its claim and its
/// recheck), and finishes the delete once the restore is ACTIVE.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_stale_delete_claim_waits_for_a_running_restore() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    source_table(&fx.engine, "src").await;
    let backup = fx
        .engine
        .create_backup(ACCOUNT, "src", "b")
        .await
        .expect("backup");
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    let (gated, gate) = gated(&fx.engine);
    gated
        .restore_table_from_backup(ACCOUNT, request)
        .await
        .expect("restore");
    let mut set = doc! { DELETE_CLAIM: "dead-delete" };
    set.insert(
        DELETE_CLAIM_HEARTBEAT,
        bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 600_000),
    );
    fx.engine
        .catalog_db
        .collection::<Document>("backups")
        .update_one(doc! { "_id": &backup.backup_arn }, doc! { "$set": set })
        .await
        .expect("stale delete claim");

    for _ in 0..3 {
        let sweep = fx.engine.sweep_stale_backup_claims().await.expect("sweep");
        assert!(sweep.is_empty(), "{sweep}");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let meta = backup_meta(&fx.engine, &backup.backup_arn).await;
    assert_eq!(
        meta.get_str(DELETE_CLAIM),
        Ok("dead-delete"),
        "the claim must be left as it was: {meta:?}"
    );

    let worker = spawn_worker(&fx.engine);
    gate.add_permits(1);
    assert_fully_restored(&fx.engine, "dst").await;
    let sweep = fx.engine.sweep_stale_backup_claims().await.expect("sweep");
    assert_eq!(
        sweep.delete_claims_finished,
        std::slice::from_ref(&backup.backup_arn)
    );
    assert_eq!(
        backup_meta(&fx.engine, &backup.backup_arn)
            .await
            .get_str("backup_status"),
        Ok("DELETED")
    );
    worker.abort();
    fx.finish().await;
}

/// The first heartbeat refresh happens at once, not one interval later.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_the_heartbeat_refreshes_at_once() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    source_table(&fx.engine, "src").await;
    let backup = fx
        .engine
        .create_backup(ACCOUNT, "src", "b")
        .await
        .expect("backup");
    let long_ago = bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 600_000);
    let mut set = doc! { DELETE_CLAIM: "op" };
    set.insert(DELETE_CLAIM_HEARTBEAT, long_ago);
    fx.engine
        .catalog_db
        .collection::<Document>("backups")
        .update_one(doc! { "_id": &backup.backup_arn }, doc! { "$set": set })
        .await
        .expect("claim");
    let mut slow = fx.engine.clone();
    slow.backup_liveness.heartbeat_interval = Duration::from_secs(3_600);
    let _heartbeat = slow.start_backup_heartbeat("op", ACCOUNT, &backup.backup_arn, None);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let beat = *backup_meta(&fx.engine, &backup.backup_arn)
            .await
            .get_datetime(DELETE_CLAIM_HEARTBEAT)
            .expect("heartbeat");
        if beat > long_ago {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the first refresh did not happen at once"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    fx.finish().await;
}

/// A restore from a process whose clock is 10 minutes behind the server's
/// is not swept by another process (stale window 5 minutes), because the
/// target's first heartbeat is replaced with the server's clock at once.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_restore_from_a_process_with_a_slow_clock_is_not_swept() {
    let fx = Fixture::new().await;
    fx.worker.abort();
    source_table(&fx.engine, "src").await;
    let backup = fx
        .engine
        .create_backup(ACCOUNT, "src", "b")
        .await
        .expect("backup");
    let liveness = BackupLiveness {
        heartbeat_interval: Duration::from_secs(3_600),
        stale_after: Duration::from_secs(300),
        slot_wait: Duration::from_secs(2),
        copy_gate: None,
        after_backup_drop: None,
        local_clock_behind: Duration::from_secs(600),
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
        .restore_table_from_backup(ACCOUNT, request)
        .await
        .expect("restore");
    let beat = *table_doc(&fx.engine, "dst")
        .await
        .expect("target")
        .get_datetime(TARGET_HEARTBEAT)
        .expect("heartbeat");
    let age_ms = bson::DateTime::now().timestamp_millis() - beat.timestamp_millis();
    assert!(
        age_ms.abs() < 60_000,
        "the target heartbeat is {age_ms} ms old: the server clock must replace the local one"
    );
    let sweep = other.sweep_stale_backup_claims().await.expect("sweep");
    assert!(sweep.is_empty(), "{sweep}");
    assert!(table_doc(&fx.engine, "dst").await.is_some());

    let worker = spawn_worker(&other);
    gate.add_permits(1);
    assert_fully_restored(&fx.engine, "dst").await;
    worker.abort();
    fx.finish().await;
}

async fn table_doc_for(engine: &MongoEngine, account: &str, name: &str) -> Option<Document> {
    engine
        .catalog_db
        .collection::<Document>("tables")
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
/// new restore of the backup is accepted.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_second_restore_of_a_backup_while_one_is_creating_is_backup_in_use() {
    const FEW: usize = 300;
    let fx = Fixture::new().await;
    source_table_with(&fx.engine, "src", FEW).await;
    let backup = fx
        .engine
        .create_backup(ACCOUNT, "src", "b")
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
            .map(|request| gated.restore_table_from_backup(ACCOUNT, request)),
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
                    m.starts_with("Backup is being used to restore table dst")
                        || m.starts_with("Backup is being used to restore another table: "),
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
    match fx.engine.restore_table_from_backup(ACCOUNT, request).await {
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
    // The refused restores left no claim behind: DeleteBackup is refused only
    // because of the winner's CREATING target.
    let meta = backup_meta(&fx.engine, &backup.backup_arn).await;
    assert!(!meta.contains_key(RESTORE_CLAIM), "{meta:?}");

    gate.add_permits(1);
    assert_fully_restored_with(&fx.engine, &winner, FEW).await;
    let request = restore_request(&fx.engine, &backup.backup_arn, "late").await;
    fx.engine
        .restore_table_from_backup(ACCOUNT, request)
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
        .create_backup(ACCOUNT, "src", "b1")
        .await
        .expect("backup 1");
    let b2 = fx
        .engine
        .create_backup(ACCOUNT, "src", "b2")
        .await
        .expect("backup 2");
    let (gated, gate) = gated(&fx.engine);
    let request = restore_request(&fx.engine, &b1.backup_arn, "dst").await;
    gated
        .restore_table_from_backup(ACCOUNT, request)
        .await
        .expect("first restore");
    let request = restore_request(&fx.engine, &b2.backup_arn, "dst").await;
    match fx.engine.restore_table_from_backup(ACCOUNT, request).await {
        Err(StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        }) => {
            assert_eq!(table_name, "dst");
            assert_eq!(backup_arn, b1.backup_arn);
        }
        other => panic!("restore into a CREATING restore target: {other:?}"),
    }
    // The same backup into the same name: the name is what is refused, not
    // the backup (the service answers TableInUseException), and b1's claim
    // is released.
    let request = restore_request(&fx.engine, &b1.backup_arn, "dst").await;
    match fx.engine.restore_table_from_backup(ACCOUNT, request).await {
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
    match fx.engine.restore_table_from_backup(ACCOUNT, missing).await {
        Err(StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        }) => {
            assert_eq!(table_name, "dst");
            assert_eq!(backup_arn, b1.backup_arn);
        }
        other => panic!("a missing backup into a CREATING restore target: {other:?}"),
    }
    let meta = backup_meta(&fx.engine, &b1.backup_arn).await;
    assert!(!meta.contains_key(RESTORE_CLAIM), "{meta:?}");
    // b2 was not left claimed by the refused restore.
    let meta = backup_meta(&fx.engine, &b2.backup_arn).await;
    assert!(!meta.contains_key(RESTORE_CLAIM), "{meta:?}");
    // With every account slot held, a client retrying its own restore is
    // still answered at once, not after the slot wait.
    let account_slots = fx.engine.restore_account_slots.for_account(ACCOUNT);
    let held = Arc::clone(&account_slots)
        .acquire_many_owned(u32::try_from(account_slots.available_permits()).expect("u32"))
        .await
        .expect("hold every account slot");
    let request = restore_request(&fx.engine, &b1.backup_arn, "dst").await;
    let started = Instant::now();
    let retried = fx.engine.restore_table_from_backup(ACCOUNT, request).await;
    assert!(
        started.elapsed() < fx.engine.backup_liveness.slot_wait,
        "the retry waited for a slot: {:?}",
        started.elapsed()
    );
    assert!(
        matches!(&retried, Err(StorageError::TableBeingRestored { table_name, .. }) if table_name == "dst"),
        "{retried:?}"
    );
    drop(held);
    gate.add_permits(1);
    assert_fully_restored_with(&fx.engine, "dst", FEW).await;
    let request = restore_request(&fx.engine, &b2.backup_arn, "dst").await;
    assert!(
        matches!(
            fx.engine.restore_table_from_backup(ACCOUNT, request).await,
            Err(StorageError::TableAlreadyExists(_))
        ),
        "an ACTIVE table of that name is TableAlreadyExists"
    );
    fx.finish().await;
}

/// Twenty restores of twenty distinct backups (twenty accounts, so no
/// per-account bound and no shared claim serializes their registration)
/// against the driver's default pool of 10, with the server bound lifted,
/// all register, the engine answers a catalog read right after, and every
/// target reaches ACTIVE. Before the fix, the registration's hand-over left
/// each creation's in-flight command holding a pooled connection while it
/// waited for another, and this wedged the process for good.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_twenty_concurrent_restores_of_distinct_backups_do_not_wedge_the_pool() {
    const N: usize = 20;
    const FEW: usize = 20;
    let fx = Fixture::new().await;
    let mut unbounded = fx.engine.clone();
    unbounded.restore_server_slot_count = 64;
    unbounded.restore_server_slots = Arc::new(tokio::sync::Semaphore::new(64));
    let accounts: Vec<String> = (0..N).map(|i| format!("9100000000{i:02}")).collect();
    let mut requests = Vec::new();
    for account in &accounts {
        source_table_for(&fx.engine, account, "src", FEW).await;
        let backup = fx
            .engine
            .create_backup(account, "src", "b")
            .await
            .expect("backup");
        requests.push((
            account.clone(),
            restore_request_for(&fx.engine, account, &backup.backup_arn, "dst").await,
        ));
    }
    let started = Instant::now();
    let results = futures::future::join_all(requests.into_iter().map(|(account, request)| {
        let engine = unbounded.clone();
        async move {
            tokio::time::timeout(
                Duration::from_secs(60),
                engine.restore_table_from_backup(&account, request),
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

/// With the default bound (`min(16, pool/2)` = 5 for the default pool
/// of 10), seven restores of seven backups from two accounts (four and three,
/// under the per-account bound of four) started while copies are held: five are
/// admitted and two are `LimitExceeded` naming the server bound after the
/// slot wait, the engine still answers meanwhile, the five finish once the
/// copies are released, and their slots are then free again.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_restores_beyond_the_server_bound_are_limit_exceeded_and_slots_are_freed() {
    const FEW: usize = 20;
    let fx = Fixture::new().await;
    assert_eq!(fx.engine.restore_server_slot_count, 5);
    let (gated, gate) = gated(&fx.engine);
    let accounts = ["920000000001", "920000000002"];
    let mut requests = Vec::new();
    // One backup per restore: a second restore of one backup while the first
    // is CREATING is BackupInUse, which is not what this test is about.
    for (account, restores) in accounts.iter().zip([4, 3]) {
        source_table_for(&fx.engine, account, "src", FEW).await;
        for i in 0..restores {
            let backup = fx
                .engine
                .create_backup(account, "src", &format!("b{i}"))
                .await
                .expect("backup");
            let target = format!("dst{i}");
            requests.push((
                (*account).to_owned(),
                target.clone(),
                restore_request_for(&fx.engine, account, &backup.backup_arn, &target).await,
            ));
        }
    }
    let started = Instant::now();
    let results = futures::future::join_all(requests.iter().map(|(account, _, request)| {
        let engine = gated.clone();
        async move {
            engine
                .restore_table_from_backup(account, request.clone())
                .await
        }
    }))
    .await;
    let elapsed = started.elapsed();
    let admitted: Vec<&(String, String, RestoreRequest)> = requests
        .iter()
        .zip(&results)
        .filter_map(|(r, result)| result.is_ok().then_some(r))
        .collect();
    assert_eq!(admitted.len(), 5, "five restores are admitted: {results:?}");
    for result in &results {
        if let Err(e) = result {
            match e {
                StorageError::LimitExceeded(m) => {
                    assert!(m.contains("on this server (at most 5"), "{m}");
                }
                other => panic!("a refused restore is LimitExceeded: {other:?}"),
            }
        }
    }
    assert!(
        elapsed >= fx.engine.backup_liveness.slot_wait,
        "the refused restores waited the slot wait: {elapsed:?}"
    );
    // The two refused restores never claimed their backup or inserted a
    // target: the refusal came before any catalog write.
    for (i, (account, target, _)) in requests.iter().enumerate() {
        let doc = table_doc_for(&fx.engine, account, target).await;
        let is_admitted = admitted.iter().any(|(a, t, _)| a == account && t == target);
        assert_eq!(doc.is_some(), is_admitted, "request {i}: {target}");
    }
    // Liveness while five copies are held.
    tokio::time::timeout(
        Duration::from_secs(10),
        fx.engine.describe_table(
            accounts[0],
            DescribeTableInput {
                table_name: "src".to_owned(),
            },
        ),
    )
    .await
    .expect("a catalog read while copies are held returns within 10 s")
    .expect("describe");
    assert_eq!(fx.engine.restore_server_slots.available_permits(), 0);

    gate.add_permits(5);
    for (account, target, _) in &admitted {
        wait_for_status_for(&fx.engine, account, target, "ACTIVE").await;
    }
    // Slots are released when the copy task ends, shortly after hand-over.
    let deadline = Instant::now() + Duration::from_secs(10);
    while fx.engine.restore_server_slots.available_permits() < 5 {
        assert!(Instant::now() < deadline, "the five slots are freed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    fx.finish().await;
}

/// The refusal paths of the admission bound, without a server.
#[tokio::test]
async fn restore_permits_wait_then_limit_exceeded() {
    let wait = Duration::from_millis(50);
    let account = Arc::new(tokio::sync::Semaphore::new(4));
    let server = Arc::new(tokio::sync::Semaphore::new(1));
    let held = super::acquire_restore_permits(Arc::clone(&account), Arc::clone(&server), 1, wait)
        .await
        .expect("the first restore gets both slots");
    let other_account = Arc::new(tokio::sync::Semaphore::new(4));
    match super::acquire_restore_permits(Arc::clone(&other_account), Arc::clone(&server), 1, wait)
        .await
    {
        Err(StorageError::LimitExceeded(m)) => {
            assert!(m.contains("on this server (at most 1"), "{m}")
        }
        other => panic!("a full server bound must refuse: {:?}", other.err()),
    }
    assert_eq!(other_account.available_permits(), 4);
    drop(held);
    super::acquire_restore_permits(Arc::clone(&other_account), Arc::clone(&server), 1, wait)
        .await
        .expect("a freed server slot is granted");
    let full_account = Arc::new(tokio::sync::Semaphore::new(0));
    let roomy_server = Arc::new(tokio::sync::Semaphore::new(4));
    match super::acquire_restore_permits(full_account, Arc::clone(&roomy_server), 4, wait).await {
        Err(StorageError::LimitExceeded(m)) => assert!(m.contains("for this account"), "{m}"),
        other => panic!("a full account must refuse: {:?}", other.err()),
    }
    assert_eq!(roomy_server.available_permits(), 4);
    assert_eq!(super::restore_server_slot_count(1), 1);
    assert_eq!(super::restore_server_slot_count(10), 5);
    assert_eq!(super::restore_server_slot_count(40), 16);
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
        .create_backup(ACCOUNT, "src", "b")
        .await
        .expect("backup");
    let (gated, gate) = gated(&fx.engine);
    let request = restore_request(&fx.engine, &backup.backup_arn, "dst").await;
    gated
        .restore_table_from_backup(ACCOUNT, request)
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
    // The restore's heartbeat refreshes this field on its own schedule.
    let mut before = before;
    before.remove(TARGET_HEARTBEAT);
    after.remove(TARGET_HEARTBEAT);
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
