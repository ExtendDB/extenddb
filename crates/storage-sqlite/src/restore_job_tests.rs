// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage-level tests of the store-backed restore on SQLite: the
//! registration under the write lock (`claim_restore`), the takeover of a
//! record whose slot no task holds (`claim_restore_dead`), the per-batch load
//! with its progress write (`bulk_load`), the ACTIVE flip (`finish_restore`)
//! and the count. The engine's restore job drives these through
//! `BackupCatalog` and `RestoreEngine`; what each must guarantee on this
//! backend is what a test here pins. Every test builds its own in-memory
//! engine.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::types::{
    AttributeValue, CreateTableInput, DistanceFunction, Item, Projection, ProjectionType,
    TableDescription, TableStatus, VectorAttribute, VectorIndexSpecification,
};
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, DeleteStart, Heartbeat, JobClaim, JobStatus, RestoreJobRow,
    RestoreKind, RestoreRegistrationRow, RestoreTargetKey, StatusFields, StoreIdentity, StoreKind,
    WriteOutcome, backup_in_use_message, is_backup_not_found,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::restore_engine::{BulkLoadFile, Finished, RestoreEngine};
use extenddb_storage::{BackupCatalog, JobPermit, TableEngine};
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::SqliteEngine;
use crate::backup_catalog::{SqliteJobClaim, restore_slot_key};
use crate::restore_engine::{RESTORE_BATCH_BYTES, RESTORE_BATCH_ITEMS};

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
/// A stale window no test's heartbeat crosses on its own.
const STALE: Duration = Duration::from_secs(300);

// --- the engine, rows, inputs and items -------------------------------------------

async fn engine() -> SqliteEngine {
    let engine = SqliteEngine::new(":memory:", 2, REGION, 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    for (key, value) in [
        ("control_plane_delay_seconds", "0"),
        ("index_propagation_delay_ms", "0"),
    ] {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&engine.pool)
        .await
        .expect("setting");
    }
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
    engine
}

fn store() -> StoreIdentity {
    StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: "/var/lib/extenddb/backups".to_owned(),
        store_id: "store-1".to_owned(),
    }
}

/// An AVAILABLE `backup_index` row of a backup of `table`, written through
/// the claim as a backup job writes it.
async fn available_backup(engine: &SqliteEngine, table: &str) -> BackupRow {
    let backup_id = extenddb_storage::util::new_backup_id();
    let backup_arn = extenddb_storage::util::backup_arn(REGION, ACCOUNT, table, &backup_id);
    let manifest: serde_json::Value = serde_json::from_str(include_str!(
        "../../core/testdata/extenddb-manifest/v1.json"
    ))
    .expect("fixture");
    let row = BackupRow {
        backup_id: backup_id.clone(),
        account_id: ACCOUNT.to_owned(),
        table_name: table.to_owned(),
        backup_arn,
        backup_name: "nightly".to_owned(),
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
    };
    let claim = engine.claim_backup(&row).await.expect("claim the row");
    let written = claim
        .set_status(
            JobStatus::Creating,
            JobStatus::Available,
            &StatusFields {
                item_count: Some(3),
                size_bytes: Some(1234),
                completed: true,
                ..StatusFields::default()
            },
        )
        .await
        .expect("the AVAILABLE write");
    assert_eq!(written, WriteOutcome::Matched);
    drop(claim);
    BackupRow {
        status: BackupStatus::Available,
        ..row
    }
}

fn registration(row: &BackupRow, spool_host: Option<&str>) -> RestoreRegistrationRow {
    RestoreRegistrationRow {
        backup: row.key(),
        source_table_arn: format!(
            "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{}",
            row.table_name
        ),
        restore_date_time: row.created_at,
        store: row.store.clone(),
        spool_host: spool_host.map(str::to_owned),
        owner_generation: 0,
    }
}

/// The target: a composite-key table with a GSI on `g`, an LSI on `l` and,
/// when asked, a vector index on `emb` (three dimensions).
fn target_input(name: &str, vector: bool) -> CreateTableInput {
    let mut input: CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
            {"AttributeName": "g", "AttributeType": "S"},
            {"AttributeName": "l", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [{
            "IndexName": "gix",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "KEYS_ONLY"}
        }],
        "LocalSecondaryIndexes": [{
            "IndexName": "lix",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "l", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .expect("input");
    if vector {
        input.vector_indexes = Some(vec![VectorIndexSpecification {
            index_name: "vix".to_owned(),
            dimensions: 3,
            distance_function: DistanceFunction::Cosine,
            vector_attribute: VectorAttribute {
                attribute_name: "emb".to_owned(),
            },
            search_schema: None,
            projection: Some(Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            }),
        }]);
    }
    input
}

/// Item `i` of the fixture: every item has `pk` and `sk`, every second one
/// `g` (the GSI's key), every third one `l` (the LSI's), every fifth one
/// `emb` (the vector).
fn item(i: usize) -> Item {
    let mut item =
        serde_json::json!({"pk": {"S": format!("p{}", i % 7)}, "sk": {"N": i.to_string()}});
    if i.is_multiple_of(2) {
        item["g"] = serde_json::json!({"S": format!("g{}", i % 4)});
    }
    if i.is_multiple_of(3) {
        item["l"] = serde_json::json!({"S": format!("l{i}")});
    }
    if i.is_multiple_of(5) {
        item["emb"] = serde_json::json!({"L": [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}]});
    }
    serde_json::from_value(item).expect("item")
}

fn items(range: std::ops::Range<usize>) -> Vec<Item> {
    range.map(item).collect()
}

fn stream(items: Vec<Item>) -> BoxStream<'static, Result<Item, StorageError>> {
    Box::pin(futures::stream::iter(items.into_iter().map(Ok)))
}

fn file(index: u32, start_at: u64) -> BulkLoadFile {
    BulkLoadFile {
        file_index: index,
        start_at,
        reapplying: false,
    }
}

fn key_of(desc: &TableDescription) -> RestoreTargetKey {
    RestoreTargetKey {
        account_id: ACCOUNT.to_owned(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    }
}

/// Register a restore of `row` into `target` and return the description, its
/// key and the claim.
async fn register(
    engine: &SqliteEngine,
    row: &BackupRow,
    target: &str,
    vector: bool,
    spool_host: Option<&str>,
) -> (TableDescription, RestoreTargetKey, Box<dyn JobClaim>) {
    let (desc, claim) = engine
        .claim_restore(
            ACCOUNT,
            target_input(target, vector),
            &registration(row, spool_host),
        )
        .await
        .expect("register the restore");
    let key = key_of(&desc);
    (desc, key, claim)
}

/// A claim on the record under `generation`, as a job whose record passed to
/// another owner still holds one.
fn stale_claim(engine: &SqliteEngine, table_id: &str, generation: u64) -> SqliteJobClaim {
    SqliteJobClaim::for_restore(engine, table_id, generation, JobPermit::detached())
}

// --- reads --------------------------------------------------------------------

async fn table_status(engine: &SqliteEngine, name: &str) -> Option<String> {
    sqlx::query_scalar("SELECT table_status FROM tables WHERE account_id = ? AND table_name = ?")
        .bind(ACCOUNT)
        .bind(name)
        .fetch_optional(&engine.pool)
        .await
        .expect("table status")
}

async fn record(engine: &SqliteEngine, key: &RestoreTargetKey) -> RestoreJobRow {
    engine
        .read_restore_job(key)
        .await
        .expect("read the record")
        .expect("the record exists")
}

/// `(status, kind, owner_generation, heartbeat_at, spool_host, file_index,
/// items_applied, completed_at)` of the record, straight from the table.
type RecordColumns = (
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

async fn record_columns(engine: &SqliteEngine, table_id: &str) -> RecordColumns {
    sqlx::query_as(
        "SELECT status, kind, owner_generation, heartbeat_at, spool_host, file_index, \
         items_applied, completed_at FROM table_restores WHERE table_id = ?",
    )
    .bind(table_id)
    .fetch_one(&engine.pool)
    .await
    .expect("read table_restores")
}

async fn rows_in(engine: &SqliteEngine, physical: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {physical}"))
        .fetch_one(&engine.pool)
        .await
        .expect("count rows")
}

async fn base_rows(engine: &SqliteEngine, table_id: &str) -> i64 {
    rows_in(engine, &crate::data::data_table_name(table_id)).await
}

/// `(index_name, index_id, index_status)` of the table's secondary indexes.
async fn indexes(engine: &SqliteEngine, table_id: &str) -> Vec<(String, String, String)> {
    sqlx::query_as(
        "SELECT index_name, index_id, index_status FROM indexes WHERE table_id = ? \
         ORDER BY index_name",
    )
    .bind(table_id)
    .fetch_all(&engine.pool)
    .await
    .expect("read indexes")
}

async fn index_rows(engine: &SqliteEngine, table_id: &str, name: &str) -> i64 {
    let id = indexes(engine, table_id)
        .await
        .into_iter()
        .find(|(n, _, _)| n == name)
        .map(|(_, id, _)| id)
        .expect("the index exists");
    rows_in(engine, &crate::data::index_table_name(&id)).await
}

/// `(index_id, index_status, backfilling)` of the table's vector indexes.
async fn vector_indexes(
    engine: &SqliteEngine,
    table_id: &str,
) -> Vec<(String, String, Option<bool>)> {
    sqlx::query_as(
        "SELECT index_id, index_status, backfilling FROM vector_indexes WHERE table_id = ?",
    )
    .bind(table_id)
    .fetch_all(&engine.pool)
    .await
    .expect("read vector_indexes")
}

async fn vector_rows(engine: &SqliteEngine, table_id: &str) -> i64 {
    let (id, _, _) = vector_indexes(engine, table_id)
        .await
        .pop()
        .expect("the vector index exists");
    rows_in(engine, &crate::data::vector_table_name(table_id, &id)).await
}

async fn progress(engine: &SqliteEngine, table_id: &str) -> (Option<i64>, Option<i64>) {
    let (_, _, _, _, _, file_index, items_applied, _) = record_columns(engine, table_id).await;
    (file_index, items_applied)
}

async fn tables_named(engine: &SqliteEngine, name: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM tables WHERE account_id = ? AND table_name = ?")
        .bind(ACCOUNT)
        .bind(name)
        .fetch_one(&engine.pool)
        .await
        .expect("count tables")
}

// --- the registration -----------------------------------------------------------

/// `claim_restore` takes the name's slot and creates the CREATING target with
/// no scheduled transition and its RUNNING `manifest` record carrying the
/// store, the spool host and the claim's generation in one transaction; the
/// description carries the `RestoreSummary`; the record reads back through
/// `read_restore_job` and `list_restore_jobs`; the heartbeat confirms and
/// refreshes the row; the secondary indexes are ACTIVE from creation and the
/// vector index CREATING with its table; dropping the claim frees the slot.
#[tokio::test]
async fn claim_restore_registers_the_target_with_its_record() {
    let engine = engine().await;
    let row = available_backup(&engine, "Music").await;
    let slot = restore_slot_key(ACCOUNT, "Restored");
    assert!(!engine.job_slots.holds(&slot));
    let (desc, key, claim) = register(&engine, &row, "Restored", true, Some("host-a:/spool")).await;
    assert_eq!(desc.table_status, TableStatus::Creating);
    let summary = desc.restore_summary.as_ref().expect("a RestoreSummary");
    assert_eq!(
        summary.source_backup_arn.as_deref(),
        Some(row.backup_arn.as_str())
    );
    assert_eq!(
        summary.source_table_arn.as_deref(),
        Some(format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music").as_str())
    );
    assert_eq!(summary.restore_date_time, row.created_at);
    assert!(summary.restore_in_progress);
    assert!(
        engine.job_slots.holds(&slot),
        "the claim holds the name's slot"
    );

    let (status, kind, generation, heartbeat_at, spool_host, file_index, items_applied, done) =
        record_columns(&engine, &key.table_id).await;
    assert_eq!((status.as_str(), kind.as_str()), ("RUNNING", "manifest"));
    assert_eq!(generation, i64::try_from(claim.generation()).unwrap());
    assert!(heartbeat_at.starts_with("20"), "{heartbeat_at}");
    assert_eq!(spool_host.as_deref(), Some("host-a:/spool"));
    assert_eq!((file_index, items_applied, done), (None, None, None));
    let transition: Option<String> =
        sqlx::query_scalar("SELECT status_transition_at FROM tables WHERE table_id = ?")
            .bind(&key.table_id)
            .fetch_one(&engine.pool)
            .await
            .expect("read the transition");
    assert_eq!(
        transition, None,
        "no scheduled transition: the job flips the table"
    );

    let job = record(&engine, &key).await;
    assert_eq!(job.target, key);
    assert_eq!(job.source_backup_arn, row.backup_arn);
    assert_eq!(job.kind, RestoreKind::Manifest);
    assert_eq!(job.status, JobStatus::Running);
    assert_eq!(job.owner_generation, claim.generation());
    assert_eq!(job.store, Some(store()));
    assert_eq!(job.spool_host.as_deref(), Some("host-a:/spool"));
    assert_eq!((job.file_index, job.items_applied), (None, None));
    assert_eq!(
        (job.failure_reason.as_deref(), job.completed_at),
        (None, None)
    );
    let running = engine
        .list_restore_jobs(Some(JobStatus::Running))
        .await
        .expect("list");
    assert_eq!(running, vec![job.clone()]);
    assert_eq!(
        engine.list_restore_jobs(None).await.expect("list all"),
        vec![job.clone()]
    );
    assert!(
        engine
            .list_restore_jobs(Some(JobStatus::Done))
            .await
            .expect("list done")
            .is_empty()
    );
    assert_eq!(
        engine.backup_in_use(&row.key()).await.expect("in use"),
        Some("Restored".to_owned()),
        "the backup is in use while its target is CREATING"
    );
    assert!(
        engine
            .list_dead_restore_jobs(STALE)
            .await
            .expect("dead")
            .is_empty(),
        "a fresh record is not dead"
    );

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        claim.heartbeat().await.expect("heartbeat"),
        Heartbeat::Confirmed
    );
    let after = record(&engine, &key).await;
    assert!(
        after.heartbeat_at > job.heartbeat_at,
        "{} > {}",
        after.heartbeat_at,
        job.heartbeat_at
    );
    assert_eq!(
        after.owner_generation,
        claim.generation(),
        "the heartbeat left the rest of the record"
    );
    let read = claim.read().await.expect("read through the claim");
    assert_eq!(read.status, JobStatus::Running);
    assert_eq!(read.owner_generation, claim.generation());

    for (name, _, status) in indexes(&engine, &key.table_id).await {
        assert_eq!(status, "ACTIVE", "secondary index {name} is created ACTIVE");
    }
    let vectors = vector_indexes(&engine, &key.table_id).await;
    assert_eq!(vectors.len(), 1);
    assert_eq!(
        (vectors[0].1.as_str(), vectors[0].2),
        ("CREATING", None),
        "the vector index is CREATING with its table and not a backfill"
    );

    drop(claim);
    assert!(
        !engine.job_slots.holds(&slot),
        "dropping the claim frees the slot"
    );
    assert_eq!(
        table_status(&engine, "Restored").await.as_deref(),
        Some("CREATING")
    );
}

/// DeleteBackup winning: `start_backup_delete` moves the row to DELETING and a
/// registration after it finds the backup gone, creates no target, keeps no
/// slot and writes no record. The two also serialize when they contend on
/// the write lock: started together behind it, the outcomes are one of the
/// two consistent pairs and never a target beside a DELETING row.
#[tokio::test]
async fn delete_backup_winning_before_the_registration_leaves_no_target() {
    let engine = engine().await;
    let row = available_backup(&engine, "Music").await;
    assert_eq!(
        engine
            .start_backup_delete(&row.key())
            .await
            .expect("delete"),
        DeleteStart::Deleting
    );
    let refused = engine
        .claim_restore(
            ACCOUNT,
            target_input("Restored", false),
            &registration(&row, None),
        )
        .await
        .err()
        .expect("the registration finds the backup gone");
    assert!(
        is_backup_not_found(&refused),
        "the registration finds the backup gone: {refused:?}"
    );
    assert_eq!(tables_named(&engine, "Restored").await, 0, "no target");
    assert!(
        !engine
            .job_slots
            .holds(&restore_slot_key(ACCOUNT, "Restored"))
    );
    assert!(
        engine
            .list_restore_jobs(None)
            .await
            .expect("list")
            .is_empty(),
        "no record"
    );

    // Contended: both behind the write lock, released together.
    let engine = Arc::new(engine);
    for attempt in 0..6 {
        let row = available_backup(&engine, &format!("Music{attempt}")).await;
        let name = format!("Restored{attempt}");
        let held = engine.write_lock.lock().await;
        let registering = tokio::spawn({
            let engine = Arc::clone(&engine);
            let row = row.clone();
            let name = name.clone();
            async move {
                engine
                    .claim_restore(
                        ACCOUNT,
                        target_input(&name, false),
                        &registration(&row, None),
                    )
                    .await
            }
        });
        let deleting = tokio::spawn({
            let engine = Arc::clone(&engine);
            let key = row.key();
            async move { engine.start_backup_delete(&key).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!registering.is_finished() && !deleting.is_finished());
        drop(held);
        let registered = tokio::time::timeout(Duration::from_secs(10), registering)
            .await
            .expect("the registration ends")
            .expect("no panic");
        let deleted = tokio::time::timeout(Duration::from_secs(10), deleting)
            .await
            .expect("the delete ends")
            .expect("no panic")
            .expect("start_backup_delete");
        match (registered, deleted) {
            (Ok((desc, claim)), DeleteStart::InUse(table)) => {
                assert_eq!(table, name, "attempt {attempt}: the registration won");
                assert_eq!(
                    table_status(&engine, &name).await.as_deref(),
                    Some("CREATING")
                );
                drop(claim);
                assert!(
                    engine
                        .remove_restore_target(&key_of(&desc))
                        .await
                        .expect("remove")
                );
            }
            (Err(e), DeleteStart::Deleting) => {
                assert!(is_backup_not_found(&e), "attempt {attempt}: {e:?}");
                assert_eq!(tables_named(&engine, &name).await, 0, "attempt {attempt}");
            }
            (registered, deleted) => panic!(
                "attempt {attempt}: the two outcomes are not one ordering: {:?} / {deleted:?}",
                registered.map(|(d, _)| d.table_status)
            ),
        }
    }
}

/// The registration winning: the backup is in use (`start_backup_delete`
/// answers the target's name), a second registration of the backup into
/// another name is `BackupInUse` naming the target and leaves no slot, and
/// the row stays AVAILABLE.
#[tokio::test]
async fn a_registration_winning_before_delete_backup_keeps_the_backup_in_use() {
    let engine = engine().await;
    let row = available_backup(&engine, "Music").await;
    let (_desc, _key, _claim) = register(&engine, &row, "First", false, None).await;
    assert_eq!(
        engine
            .start_backup_delete(&row.key())
            .await
            .expect("delete"),
        DeleteStart::InUse("First".to_owned())
    );
    let second = engine
        .claim_restore(
            ACCOUNT,
            target_input("Second", false),
            &registration(&row, None),
        )
        .await
        .err()
        .expect("a second registration of the backup is refused");
    match second {
        // The service's text names the backup, not the target.
        StorageError::BackupInUse(m) => assert_eq!(m, backup_in_use_message(&row.backup_arn)),
        other => panic!("{other:?}"),
    }
    assert_eq!(tables_named(&engine, "Second").await, 0);
    assert!(!engine.job_slots.holds(&restore_slot_key(ACCOUNT, "Second")));
    let arn = extenddb_core::backup_format::BackupArn::parse(&row.backup_arn).unwrap();
    assert_eq!(
        engine
            .read_backup_row(&arn)
            .await
            .expect("read")
            .expect("the row")
            .status,
        BackupStatus::Available
    );
}

/// A taken name: an existing table is `TableAlreadyExists`; a CREATING
/// restore target is `TableBeingRestored` naming its backup, both while its
/// claim holds the slot and after the claim is gone (the committed row).
#[tokio::test]
async fn a_taken_name_is_refused_as_create_table_or_as_a_restore_in_progress() {
    let engine = engine().await;
    let row = available_backup(&engine, "Music").await;
    engine
        .create_table_impl(ACCOUNT, target_input("Plain", false), false)
        .await
        .expect("a table");
    let taken = engine
        .claim_restore(
            ACCOUNT,
            target_input("Plain", false),
            &registration(&row, None),
        )
        .await
        .err()
        .expect("the name is taken");
    assert!(
        matches!(&taken, StorageError::TableAlreadyExists(n) if n == "Plain"),
        "{taken:?}"
    );
    assert!(!engine.job_slots.holds(&restore_slot_key(ACCOUNT, "Plain")));

    let other = available_backup(&engine, "Other").await;
    let (_desc, _key, claim) = register(&engine, &row, "Target", false, None).await;
    async fn refuse(engine: &SqliteEngine, other: &BackupRow) -> StorageError {
        engine
            .claim_restore(
                ACCOUNT,
                target_input("Target", false),
                &registration(other, None),
            )
            .await
            .err()
            .expect("the name is a restore target")
    }
    // While the first claim holds the slot: refused on the slot, the
    // committed CREATING row naming the backup.
    match refuse(&engine, &other).await {
        StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        } => {
            assert_eq!(table_name, "Target");
            assert_eq!(backup_arn, row.backup_arn, "the slot held");
        }
        other => panic!("slot held: {other:?}"),
    }
    // The claim gone, the target still CREATING: refused in the
    // registration transaction on the committed row.
    drop(claim);
    assert!(!engine.job_slots.holds(&restore_slot_key(ACCOUNT, "Target")));
    match refuse(&engine, &other).await {
        StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        } => {
            assert_eq!(table_name, "Target");
            assert_eq!(
                backup_arn, row.backup_arn,
                "the slot free, the row committed"
            );
        }
        other => panic!("slot free: {other:?}"),
    }
    assert!(
        !engine.job_slots.holds(&restore_slot_key(ACCOUNT, "Target")),
        "a refused registration returns the slot"
    );
}

// --- the takeover ---------------------------------------------------------------

/// `claim_restore_dead`: refused while the owner's claim holds the slot,
/// whatever the heartbeat says; a record whose slot no task holds is taken at
/// once, fresh heartbeat or not, with a fresh generation, the claimant's
/// `spool_host` and `heartbeat_at` refreshed to the database's clock (an
/// aged record is live to the dead listing at once); a third taker is
/// refused; the old generation's heartbeat is lost and its writes match
/// nothing; the successor's FAILED write matches and leaves the progress; a
/// DONE record, a legacy record and a missing one are `None` and hold no
/// slot.
#[tokio::test]
async fn a_dead_record_is_taken_over_with_the_claimants_spool_and_a_fresh_heartbeat() {
    let engine = engine().await;
    let row = available_backup(&engine, "Music").await;
    let (_desc, key, claim) = register(&engine, &row, "Restored", false, Some("host-a:/a")).await;
    let old_generation = claim.generation();
    engine
        .bulk_load(&key, claim.as_ref(), file(0, 0), stream(items(0..10)))
        .await
        .expect("a first file");
    assert_eq!(progress(&engine, &key.table_id).await, (Some(0), Some(10)));
    assert!(
        engine
            .claim_restore_dead(&key, STALE, Some("host-b:/b"))
            .await
            .expect("claim")
            .is_none(),
        "a live claim's slot refuses the takeover"
    );
    // The dead owner's last beat, an hour ago: the record is in the dead
    // listing until the takeover refreshes it.
    sqlx::query(
        "UPDATE table_restores SET heartbeat_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', \
         '-1 hour') WHERE table_id = ?",
    )
    .bind(&key.table_id)
    .execute(&engine.pool)
    .await
    .expect("age the record");
    let before = record_columns(&engine, &key.table_id).await;
    assert_eq!(before.4.as_deref(), Some("host-a:/a"));

    drop(claim);
    assert_eq!(
        engine
            .list_dead_restore_jobs(STALE)
            .await
            .expect("dead")
            .len(),
        1,
        "the aged record is listed before the takeover"
    );
    let successor = engine
        .claim_restore_dead(&key, STALE, Some("host-b:/b"))
        .await
        .expect("claim")
        .expect("a record whose slot no task holds is dead at once");
    assert_ne!(successor.generation(), old_generation);
    let after = record_columns(&engine, &key.table_id).await;
    assert_eq!(after.0, "RUNNING");
    assert_eq!(
        after.2,
        i64::try_from(successor.generation()).unwrap(),
        "the successor's generation"
    );
    assert!(
        after.3 > before.3,
        "the takeover refreshed heartbeat_at: {} was {}",
        after.3,
        before.3
    );
    let age_secs: f64 = sqlx::query_scalar(
        "SELECT (julianday('now') - julianday(heartbeat_at)) * 86400.0 FROM table_restores \
         WHERE table_id = ?",
    )
    .bind(&key.table_id)
    .fetch_one(&engine.pool)
    .await
    .expect("the record's age");
    assert!(
        age_secs.abs() < 5.0,
        "the refreshed heartbeat is the database's now: {age_secs} s old"
    );
    assert!(
        engine
            .list_dead_restore_jobs(STALE)
            .await
            .expect("dead")
            .is_empty(),
        "the taken-over record is live to the listing at once"
    );
    assert_eq!(
        after.4.as_deref(),
        Some("host-b:/b"),
        "the claimant's spool"
    );
    assert_eq!(
        (after.5, after.6),
        (Some(0), Some(10)),
        "the progress stays"
    );
    assert!(
        engine
            .job_slots
            .holds(&restore_slot_key(ACCOUNT, "Restored"))
    );
    assert!(
        engine
            .claim_restore_dead(&key, STALE, None)
            .await
            .expect("claim")
            .is_none(),
        "a third taker is refused"
    );

    let old = stale_claim(&engine, &key.table_id, old_generation);
    assert_eq!(old.heartbeat().await.expect("beat"), Heartbeat::Lost);
    assert_eq!(
        old.set_status(
            JobStatus::Running,
            JobStatus::Failed,
            &StatusFields {
                failure_reason: Some("old owner".to_owned()),
                completed: true,
                ..StatusFields::default()
            }
        )
        .await
        .expect("write"),
        WriteOutcome::NotMatched
    );
    assert_eq!(
        engine
            .finish_restore(&key, &old, 10, 100)
            .await
            .expect("finish"),
        Finished::NotMatched(format!(
            "the job record of Restored is not RUNNING under generation {old_generation}, or \
             the table is not CREATING"
        ))
    );
    assert_eq!(
        table_status(&engine, "Restored").await.as_deref(),
        Some("CREATING")
    );
    assert_eq!(
        successor.heartbeat().await.expect("beat"),
        Heartbeat::Confirmed
    );
    assert_eq!(
        successor
            .set_status(
                JobStatus::Running,
                JobStatus::Failed,
                &StatusFields {
                    item_count: Some(99),
                    failure_reason: Some("target_not_empty".to_owned()),
                    completed: true,
                    ..StatusFields::default()
                }
            )
            .await
            .expect("write"),
        WriteOutcome::Matched
    );
    let failed = record_columns(&engine, &key.table_id).await;
    assert_eq!(failed.0, "FAILED");
    assert!(failed.7.is_some(), "the FAILED write stamps completed_at");
    assert_eq!(
        (failed.5, failed.6),
        (Some(0), Some(10)),
        "a status write leaves the progress; item_count is not items_applied"
    );
    let read = successor.read().await.expect("read");
    assert_eq!(read.status, JobStatus::Failed);
    assert_eq!(read.failure_reason.as_deref(), Some("target_not_empty"));
    assert_eq!(
        successor
            .set_status(
                JobStatus::Running,
                JobStatus::Failed,
                &StatusFields::default()
            )
            .await
            .expect("write"),
        WriteOutcome::NotMatched,
        "a FAILED record is not RUNNING"
    );
    assert!(
        engine
            .list_dead_restore_jobs(STALE)
            .await
            .expect("dead")
            .is_empty(),
        "a fresh FAILED record is not listed yet"
    );
    drop(successor);
    assert!(
        !engine
            .job_slots
            .holds(&restore_slot_key(ACCOUNT, "Restored")),
        "dropping the successor frees the slot"
    );
    assert!(
        engine
            .claim_restore_dead(&key, STALE, None)
            .await
            .expect("claim")
            .is_none(),
        "a FAILED record is not taken over"
    );
    assert!(
        !engine
            .job_slots
            .holds(&restore_slot_key(ACCOUNT, "Restored")),
        "a refused takeover holds no slot"
    );
    let listed = engine
        .list_restore_jobs(Some(JobStatus::Failed))
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed[0].failure_reason.as_deref(),
        Some("target_not_empty")
    );
    assert_eq!(listed[0].spool_host.as_deref(), Some("host-b:/b"));
    assert!(listed[0].completed_at.is_some());

    // A legacy record is never taken over: the reconciler fails and removes it.
    let legacy = engine
        .create_table_impl(ACCOUNT, target_input("Legacy", false), true)
        .await
        .expect("a CREATING table");
    sqlx::query(&format!(
        "INSERT INTO table_restores (table_id, source_backup_arn, source_table_arn, \
         restore_date_time, status, kind, owner_generation, heartbeat_at) \
         VALUES (?, 'arn:legacy', 'arn:aws:dynamodb:us-east-1:123456789012:table/Music', {NOW}, \
         'RUNNING', 'legacy', 7, '1970-01-01T00:00:00.000Z')",
        NOW = crate::backup_catalog::NOW
    ))
    .bind(&legacy.table_id)
    .execute(&engine.pool)
    .await
    .expect("a legacy record");
    let legacy_key = key_of(&legacy);
    assert!(
        engine
            .claim_restore_dead(&legacy_key, STALE, None)
            .await
            .expect("claim")
            .is_none(),
        "a legacy record is not taken over"
    );
    assert!(!engine.job_slots.holds(&restore_slot_key(ACCOUNT, "Legacy")));
    let dead = engine.list_dead_restore_jobs(STALE).await.expect("dead");
    assert_eq!(dead.len(), 1, "{dead:?}");
    assert_eq!(dead[0].kind, RestoreKind::Legacy);
    assert_eq!(dead[0].target, legacy_key);
    assert_eq!(
        (dead[0].store.clone(), dead[0].spool_host.clone()),
        (None, None)
    );
    let missing = RestoreTargetKey {
        account_id: ACCOUNT.to_owned(),
        table_name: "Nothing".to_owned(),
        table_id: "no-such-id".to_owned(),
    };
    assert!(
        engine
            .claim_restore_dead(&missing, STALE, None)
            .await
            .expect("claim")
            .is_none()
    );
    assert!(
        !engine
            .job_slots
            .holds(&restore_slot_key(ACCOUNT, "Nothing"))
    );
    assert!(
        engine
            .read_restore_job(&missing)
            .await
            .expect("read")
            .is_none()
    );
}

// --- the load -------------------------------------------------------------------

/// `bulk_load` writes the base rows, every secondary index's rows and the
/// vector rows across the batch boundary, recording `(file_index,
/// items_applied)` on the record; a file resumed at `start_at` records the
/// running total; a key the backup holds twice, within a file or against an
/// earlier file, fails the file with nothing of its batch committed and the
/// progress untouched; an `Err` item fails the file the same way; another
/// account's key is refused before any write.
#[tokio::test]
async fn bulk_load_writes_each_batch_with_its_indexes_and_progress() {
    let engine = engine().await;
    let row = available_backup(&engine, "Music").await;
    let (_desc, key, claim) = register(&engine, &row, "Restored", true, None).await;
    let n = RESTORE_BATCH_ITEMS + 500;
    let applied = engine
        .bulk_load(&key, claim.as_ref(), file(0, 0), stream(items(0..n)))
        .await
        .expect("load file 0");
    assert_eq!(applied as usize, n);
    assert_eq!(base_rows(&engine, &key.table_id).await as usize, n);
    assert_eq!(
        index_rows(&engine, &key.table_id, "gix").await as usize,
        n / 2
    );
    assert_eq!(
        index_rows(&engine, &key.table_id, "lix").await as usize,
        n.div_ceil(3)
    );
    assert_eq!(vector_rows(&engine, &key.table_id).await as usize, n / 5);
    assert_eq!(
        progress(&engine, &key.table_id).await,
        (Some(0), Some(n as i64))
    );
    assert_eq!(
        engine.count_restore_target(&key).await.expect("count") as usize,
        n
    );
    let listed = record(&engine, &key).await;
    assert_eq!(
        (listed.file_index, listed.items_applied),
        (Some(0), Some(n as u64))
    );

    // A file resumed after five of its items: the record carries the total.
    let applied = engine
        .bulk_load(&key, claim.as_ref(), file(1, 5), stream(items(n..n + 3)))
        .await
        .expect("load the rest of file 1");
    assert_eq!(applied, 3);
    assert_eq!(progress(&engine, &key.table_id).await, (Some(1), Some(8)));
    assert_eq!(base_rows(&engine, &key.table_id).await as usize, n + 3);

    // A key twice within a file.
    let mut twice = items(n + 3..n + 6);
    twice.push(item(n + 4));
    let e = engine
        .bulk_load(&key, claim.as_ref(), file(2, 0), stream(twice))
        .await
        .expect_err("a key twice fails the file");
    match &e {
        StorageError::Validation(m) => {
            assert!(
                m.starts_with("holds a primary key the target already has"),
                "the backend names no file (the engine prefixes the file's key): {m}"
            );
            assert!(m.contains("the backup holds a key twice"), "{m}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        base_rows(&engine, &key.table_id).await as usize,
        n + 3,
        "nothing of the failed batch committed"
    );
    assert_eq!(
        index_rows(&engine, &key.table_id, "gix").await as usize,
        (n + 3).div_ceil(2),
        "no index row of the failed batch"
    );
    assert_eq!(progress(&engine, &key.table_id).await, (Some(1), Some(8)));

    // A key of file 0 in file 3.
    let mut across = items(n + 6..n + 8);
    across.push(item(1));
    let e = engine
        .bulk_load(&key, claim.as_ref(), file(3, 0), stream(across))
        .await
        .expect_err("a key of an earlier file fails the file");
    assert!(
        matches!(&e, StorageError::Validation(m) if m.starts_with("holds a primary key the target already has")),
        "{e:?}"
    );
    assert_eq!(base_rows(&engine, &key.table_id).await as usize, n + 3);
    assert_eq!(progress(&engine, &key.table_id).await, (Some(1), Some(8)));

    // An `Err` item mid-stream: the error, nothing committed.
    let failing: BoxStream<'static, Result<Item, StorageError>> = Box::pin(
        futures::stream::iter(items(n + 10..n + 12).into_iter().map(Ok)).chain(
            futures::stream::once(async {
                Err(StorageError::Validation(
                    "item cannot be restored".to_owned(),
                ))
            }),
        ),
    );
    let e = engine
        .bulk_load(&key, claim.as_ref(), file(4, 0), failing)
        .await
        .expect_err("an Err item fails the file");
    assert!(
        matches!(&e, StorageError::Validation(m) if m == "item cannot be restored"),
        "{e:?}"
    );
    assert_eq!(base_rows(&engine, &key.table_id).await as usize, n + 3);
    assert_eq!(progress(&engine, &key.table_id).await, (Some(1), Some(8)));

    // Another account's key names no restore target of this account.
    let foreign = RestoreTargetKey {
        account_id: "111111111111".to_owned(),
        ..key.clone()
    };
    assert!(matches!(
        engine.count_restore_target(&foreign).await,
        Err(StorageError::TableNotFound(_))
    ));
    assert!(matches!(
        engine
            .bulk_load(&foreign, claim.as_ref(), file(5, 0), stream(items(0..1)))
            .await,
        Err(StorageError::TableNotFound(_))
    ));
    assert_eq!(base_rows(&engine, &key.table_id).await as usize, n + 3);
}

/// A resume from a mid-file `items_applied`: the engine hands the rest of the
/// file with `start_at` set, and exactly those items land, the record's total
/// becoming the file's count.
#[tokio::test]
async fn a_resume_from_a_mid_file_progress_applies_exactly_the_rest() {
    let engine = engine().await;
    let row = available_backup(&engine, "Music").await;
    let (_desc, key, claim) = register(&engine, &row, "Restored", false, None).await;
    let applied = engine
        .bulk_load(&key, claim.as_ref(), file(0, 0), stream(items(0..60)))
        .await
        .expect("the first run's batch");
    assert_eq!(applied, 60);
    assert_eq!(progress(&engine, &key.table_id).await, (Some(0), Some(60)));
    // The successor: `start_at` 60, the stream from item 60.
    let applied = engine
        .bulk_load(&key, claim.as_ref(), file(0, 60), stream(items(60..100)))
        .await
        .expect("the resumed rest of the file");
    assert_eq!(applied, 40, "exactly the rest");
    assert_eq!(progress(&engine, &key.table_id).await, (Some(0), Some(100)));
    assert_eq!(engine.count_restore_target(&key).await.expect("count"), 100);
    assert_eq!(index_rows(&engine, &key.table_id, "gix").await, 50);
    assert_eq!(index_rows(&engine, &key.table_id, "lix").await, 34);

    // An empty file (a resume whose `start_at` is the file's whole count,
    // or a data file with no items) records its progress as complete.
    let applied = engine
        .bulk_load(&key, claim.as_ref(), file(1, 0), stream(Vec::new()))
        .await
        .expect("an empty file");
    assert_eq!(applied, 0);
    assert_eq!(
        progress(&engine, &key.table_id).await,
        (Some(1), Some(0)),
        "an empty file's progress is recorded"
    );
    let applied = engine
        .bulk_load(&key, claim.as_ref(), file(2, 7), stream(Vec::new()))
        .await
        .expect("a file resumed at its end");
    assert_eq!(applied, 0);
    assert_eq!(
        progress(&engine, &key.table_id).await,
        (Some(2), Some(7)),
        "`start_at` is the recorded total of an empty rest"
    );
}

/// A batch closes at the first item whose text takes it past
/// `RESTORE_BATCH_BYTES`, long before the count cap: a file of 100 items of
/// about 350 KB each (35 MB of text against the 32 MiB bound) is written in
/// two transactions, the first committed with its progress before the
/// stream hands over its last item.
#[tokio::test]
async fn a_batch_closes_at_the_byte_bound_before_the_count_cap() {
    let engine = engine().await;
    let row = available_backup(&engine, "Music").await;
    let (_desc, key, claim) = register(&engine, &row, "Restored", false, None).await;
    const N: usize = 100;
    const TEXT: usize = 350_000;
    const {
        assert!(N < RESTORE_BATCH_ITEMS);
        // 100 items cross the byte bound once.
        assert!(N * TEXT > RESTORE_BATCH_BYTES && (N - 2) * TEXT < 2 * RESTORE_BATCH_BYTES);
    }
    let big = |i: usize| -> Item {
        let mut it = item(i);
        it.insert("v".to_owned(), AttributeValue::S("x".repeat(TEXT)));
        it
    };
    // The stream reads the record when asked for the last item: by then the
    // byte bound has closed and committed the first batch.
    let pool = engine.pool.clone();
    let table_id = key.table_id.clone();
    let seen = Arc::new(std::sync::Mutex::new(None));
    let seen_by_stream = Arc::clone(&seen);
    let bounded: BoxStream<'static, Result<Item, StorageError>> =
        Box::pin(futures::stream::unfold(0usize, move |i| {
            let pool = pool.clone();
            let table_id = table_id.clone();
            let seen = Arc::clone(&seen_by_stream);
            async move {
                if i >= N {
                    return None;
                }
                if i == N - 1 {
                    let progress: (Option<i64>, Option<i64>) = sqlx::query_as(
                        "SELECT file_index, items_applied FROM table_restores WHERE table_id = ?",
                    )
                    .bind(&table_id)
                    .fetch_one(&pool)
                    .await
                    .expect("progress");
                    *seen.lock().expect("seen") = Some(progress);
                }
                Some((Ok(big(i)), i + 1))
            }
        }));
    let applied = engine
        .bulk_load(&key, claim.as_ref(), file(0, 0), bounded)
        .await
        .expect("load");
    assert_eq!(applied as usize, N);
    let (file_index, items_applied) = seen
        .lock()
        .expect("seen")
        .expect("the stream read the record at its last item");
    assert_eq!(
        file_index,
        Some(0),
        "the first batch committed with its progress"
    );
    let first_batch = items_applied.expect("the first batch's total") as usize;
    assert!(
        0 < first_batch && first_batch < N,
        "the first batch closed at the byte bound with {first_batch} of {N} items"
    );
    assert!(
        first_batch * TEXT < RESTORE_BATCH_BYTES + TEXT + 1_000 * N,
        "the batch closed at the first item past the bound: {first_batch} items"
    );
    assert_eq!(
        progress(&engine, &key.table_id).await,
        (Some(0), Some(N as i64))
    );
    assert_eq!(base_rows(&engine, &key.table_id).await as usize, N);
}

/// A claim lost between two batches of one file: the second batch's progress
/// write matches no record, the batch rolls back whole (base, index and
/// vector rows), and `bulk_load` fails as a lost claim; the first batch and
/// its progress stand.
#[tokio::test]
async fn a_lost_claim_mid_file_rolls_its_batch_back() {
    let engine = engine().await;
    let row = available_backup(&engine, "Music").await;
    let (_desc, key, claim) = register(&engine, &row, "Restored", true, None).await;
    let n = RESTORE_BATCH_ITEMS + 200;
    // The stream hands the first batch, then takes the record away before
    // the first item of the second.
    let pool = engine.pool.clone();
    let table_id = key.table_id.clone();
    let taken: BoxStream<'static, Result<Item, StorageError>> =
        Box::pin(futures::stream::unfold(0usize, move |i| {
            let pool = pool.clone();
            let table_id = table_id.clone();
            async move {
                if i >= n {
                    return None;
                }
                if i == RESTORE_BATCH_ITEMS {
                    sqlx::query(
                        "UPDATE table_restores SET owner_generation = owner_generation + 1 \
                         WHERE table_id = ?",
                    )
                    .bind(&table_id)
                    .execute(&pool)
                    .await
                    .expect("another owner takes the record");
                }
                Some((Ok(item(i)), i + 1))
            }
        }));
    let e = engine
        .bulk_load(&key, claim.as_ref(), file(0, 0), taken)
        .await
        .expect_err("the lost claim ends the file");
    assert!(
        matches!(&e, StorageError::Internal(m) if m.starts_with("claim lost")),
        "{e:?}"
    );
    assert_eq!(
        base_rows(&engine, &key.table_id).await as usize,
        RESTORE_BATCH_ITEMS,
        "the second batch's base rows rolled back"
    );
    assert_eq!(
        index_rows(&engine, &key.table_id, "gix").await as usize,
        RESTORE_BATCH_ITEMS / 2,
        "the second batch's index rows rolled back"
    );
    assert_eq!(
        vector_rows(&engine, &key.table_id).await as usize,
        RESTORE_BATCH_ITEMS / 5,
        "the second batch's vector rows rolled back"
    );
    assert_eq!(
        progress(&engine, &key.table_id).await,
        (Some(0), Some(RESTORE_BATCH_ITEMS as i64)),
        "the first batch's progress stands"
    );
    assert_eq!(claim.heartbeat().await.expect("beat"), Heartbeat::Lost);
}

// --- the flip -------------------------------------------------------------------

/// `finish_restore`: a stale generation is `NotMatched` and changes nothing;
/// the claim's flips the record DONE with `completed_at` and the progress
/// left, the table ACTIVE with its counts and no transition, and every
/// secondary and vector index ACTIVE in one transaction; the heartbeat is
/// lost after DONE; a second flip is `NotMatched`; the backup is no longer
/// in use; `describe_table` reports ACTIVE with the count.
/// `remove_restore_target` of a CREATING target removes it with its vector
/// data table.
#[tokio::test]
async fn finish_restore_flips_the_table_and_every_index() {
    let engine = engine().await;
    let row = available_backup(&engine, "Music").await;
    let (_desc, key, claim) = register(&engine, &row, "Restored", true, None).await;
    engine
        .bulk_load(&key, claim.as_ref(), file(0, 0), stream(items(0..30)))
        .await
        .expect("load");
    // A secondary index left CREATING by hand, beside the vector index the
    // creation left CREATING; the vector row's `backfilling` set as a backfill
    // in progress leaves it, so the flip's `backfilling = NULL` is observed
    // (an ACTIVE row may not carry it: `chk_vector_active_has_no_backfilling`).
    sqlx::query(
        "UPDATE indexes SET index_status = 'CREATING' WHERE table_id = ? AND index_name = 'gix'",
    )
    .bind(&key.table_id)
    .execute(&engine.pool)
    .await
    .expect("a CREATING index");
    sqlx::query("UPDATE vector_indexes SET backfilling = 1 WHERE table_id = ?")
        .bind(&key.table_id)
        .execute(&engine.pool)
        .await
        .expect("a backfilling vector index");
    assert_eq!(
        vector_indexes(&engine, &key.table_id)
            .await
            .iter()
            .map(|(_, status, backfilling)| (status.as_str(), *backfilling))
            .collect::<Vec<_>>(),
        vec![("CREATING", Some(true))],
        "the fixture's one vector index is CREATING and backfilling before the flip"
    );

    let stale = stale_claim(&engine, &key.table_id, claim.generation() + 1);
    assert!(matches!(
        engine
            .finish_restore(&key, &stale, 30, 3000)
            .await
            .expect("finish"),
        Finished::NotMatched(_)
    ));
    assert_eq!(
        table_status(&engine, "Restored").await.as_deref(),
        Some("CREATING")
    );
    assert_eq!(record_columns(&engine, &key.table_id).await.0, "RUNNING");

    assert_eq!(
        engine
            .finish_restore(&key, claim.as_ref(), 30, 3000)
            .await
            .expect("finish"),
        Finished::Active
    );
    let (status, _, _, _, _, file_index, items_applied, completed_at) =
        record_columns(&engine, &key.table_id).await;
    assert_eq!(status, "DONE");
    assert!(completed_at.is_some(), "DONE stamps completed_at");
    assert_eq!(
        (file_index, items_applied),
        (Some(0), Some(30)),
        "the DONE write leaves the progress"
    );
    let table: (String, i64, i64, Option<String>) = sqlx::query_as(
        "SELECT table_status, item_count, table_size_bytes, status_transition_at \
         FROM tables WHERE table_id = ?",
    )
    .bind(&key.table_id)
    .fetch_one(&engine.pool)
    .await
    .expect("the table row");
    assert_eq!(table, ("ACTIVE".to_owned(), 30, 3000, None));
    for (name, _, status) in indexes(&engine, &key.table_id).await {
        assert_eq!(status, "ACTIVE", "every secondary index is ACTIVE ({name})");
    }
    for (id, status, backfilling) in vector_indexes(&engine, &key.table_id).await {
        assert_eq!(
            (status.as_str(), backfilling),
            ("ACTIVE", None),
            "every vector index is ACTIVE with `backfilling` cleared ({id})"
        );
    }
    assert_eq!(claim.heartbeat().await.expect("beat"), Heartbeat::Lost);
    assert!(matches!(
        engine
            .finish_restore(&key, claim.as_ref(), 30, 3000)
            .await
            .expect("finish"),
        Finished::NotMatched(_)
    ));
    assert_eq!(
        engine.backup_in_use(&row.key()).await.expect("in use"),
        None
    );
    let described = engine
        .describe_table(
            ACCOUNT,
            extenddb_core::types::DescribeTableInput {
                table_name: "Restored".to_owned(),
            },
        )
        .await
        .expect("describe");
    assert_eq!(described.table_status, TableStatus::Active);
    assert_eq!(described.item_count, 30);
    let done = engine
        .list_restore_jobs(Some(JobStatus::Done))
        .await
        .expect("list");
    assert_eq!(done.len(), 1);
    assert!(done[0].completed_at.is_some());
    assert!(
        matches!(
            engine.count_restore_target(&key).await,
            Err(StorageError::TableNotFound(_))
        ),
        "an ACTIVE table is no longer a CREATING restore target"
    );

    // Removal of another target: the table, its record, its index and
    // vector data tables.
    let other = available_backup(&engine, "Other").await;
    let (desc, other_key, other_claim) = register(&engine, &other, "Gone", true, None).await;
    engine
        .bulk_load(
            &other_key,
            other_claim.as_ref(),
            file(0, 0),
            stream(items(0..10)),
        )
        .await
        .expect("load");
    let (vector_id, _, _) = vector_indexes(&engine, &other_key.table_id)
        .await
        .pop()
        .unwrap();
    let data_tables = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN (?, ?)",
        )
        .bind(crate::data::data_table_name(&desc.table_id).trim_matches('"'))
        .bind(crate::data::vector_table_name(&desc.table_id, &vector_id).trim_matches('"'))
        .fetch_one(&engine.pool)
        .await
        .expect("sqlite_master")
    };
    assert_eq!(data_tables().await, 2);
    drop(other_claim);
    assert!(
        engine
            .remove_restore_target(&other_key)
            .await
            .expect("remove")
    );
    assert_eq!(tables_named(&engine, "Gone").await, 0);
    assert_eq!(
        data_tables().await,
        0,
        "the data and vector tables are dropped"
    );
    assert!(
        engine
            .read_restore_job(&other_key)
            .await
            .expect("read")
            .is_none()
    );
    assert!(
        !engine
            .remove_restore_target(&other_key)
            .await
            .expect("remove again")
    );
}
