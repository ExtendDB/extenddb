// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests of the store-backed restore on PostgreSQL: the
//! registration under the `backup_index` row's lock (`claim_restore`), the
//! takeover of a dead record (`claim_restore_dead`), the per-file load with
//! its progress row (`bulk_load`), the ACTIVE flip (`finish_restore`), the
//! counts and the progress sweep. The engine's restore job drives these
//! through `BackupCatalog` and `RestoreEngine`; what each must guarantee on
//! this backend is what a test here pins.
//!
//! Each test builds a throwaway catalog and data database and drops them
//! when it ends, passing or failing, as `backup_restore.rs` does; the tests
//! run one at a time. Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`;
//! without it every test reports a skip and passes, unless
//! `EXTENDDB_TEST_PG_REQUIRED` says the job must reach a server.

use std::time::Duration;

use extenddb_core::types::{
    CreateTableInput, DistanceFunction, Item, Projection, ProjectionType, TableDescription,
    TableStatus, VectorAttribute, VectorIndexSpecification,
};
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, DeleteStart, Heartbeat, JobClaim, JobStatus, RestoreJobRow,
    RestoreKind, RestoreRegistrationRow, RestoreTargetKey, StatusFields, StoreIdentity, StoreKind,
    WriteOutcome, backup_in_use_message,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::migrations::Runner;
use extenddb_storage::restore_engine::{BulkLoadFile, Finished, RestoreEngine};
use extenddb_storage::{BackupCatalog, TableEngine};
use extenddb_storage_postgres::RESTORE_LOCK_SEED;
use extenddb_storage_postgres::migrations::{PostgresMigrations, PostgresTarget};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
/// A stale window no test's heartbeat crosses on its own.
const STALE: Duration = Duration::from_secs(300);

// --- the scratch databases --------------------------------------------------------

/// A catalog database and a separate data database, the layout `extenddb
/// init` creates, with pgvector installed in the data database when the
/// server offers it.
struct Scratch {
    engine: PostgresEngine,
    /// The data database.
    db: PgPool,
    catalog: PgPool,
    admin: PgPool,
    db_names: [String; 2],
    vector: bool,
    _guard: DbGuard,
    _serial: tokio::sync::MutexGuard<'static, ()>,
}

/// Drops the scratch databases if the test panics, from the moment they are
/// created, including during setup.
struct DbGuard {
    names: Vec<String>,
}

/// One test at a time: each opens its own pools and claim sessions.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

impl Scratch {
    async fn cleanup(self) {
        self.db.close().await;
        self.catalog.close().await;
        drop_databases(&self.admin, &self.db_names).await;
        self.admin.close().await;
    }
}

async fn drop_databases(admin: &PgPool, names: &[String]) {
    for name in names {
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
            .execute(admin)
            .await;
    }
}

impl Drop for DbGuard {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        let (Some(base), names) = (base_conn(), self.names.clone()) else {
            return;
        };
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async {
                if let Ok(admin) = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&format!("{base}/postgres"))
                    .await
                {
                    drop_databases(&admin, &names).await;
                    admin.close().await;
                }
            });
        })
        .join();
    }
}

/// The server's base URL, or `None` to skip. With `EXTENDDB_TEST_PG_REQUIRED`
/// set to any value but `0`, a job that must reach a server fails here
/// instead.
fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.trim().is_empty() {
        let required =
            std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0");
        assert!(
            !required,
            "EXTENDDB_TEST_PG_CONNECTION_STRING is not set, and EXTENDDB_TEST_PG_REQUIRED says \
             this job must reach a PostgreSQL server"
        );
        return None;
    }
    Some(conn.trim_end_matches('/').to_owned())
}

/// `true` when the test can run; prints the skip line otherwise.
fn pg_available(test: &str) -> bool {
    if base_conn().is_none() {
        eprintln!("SKIP {test}: no PostgreSQL");
        return false;
    }
    true
}

async fn connect(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(3)
        .connect(url)
        .await
        .expect("connect to a scratch database")
}

async fn scratch() -> Scratch {
    let serial = SERIAL.lock().await;
    let base = base_conn().expect("caller checks base_conn() first");
    let stem = format!("eddb_rstr_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let (catalog_name, data_name) = (format!("{stem}_c"), format!("{stem}_d"));
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    let guard = DbGuard {
        names: vec![catalog_name.clone(), data_name.clone()],
    };
    for name in [&catalog_name, &data_name] {
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&admin)
            .await
            .expect("create a scratch database");
    }
    let catalog_url = format!("{base}/{catalog_name}");
    let data_url = format!("{base}/{data_name}");
    // A fresh install, migrated by the runner as `init` does: migration 8
    // creates `restore_progress` in the data database.
    let target = PostgresTarget::new(catalog_url.parse().expect("a postgres URL"), &data_name)
        .with_data_connection_row(&data_url);
    let runner = PostgresMigrations::open_unlocked(&target)
        .await
        .expect("open the migration runner");
    Runner::new(&runner, "test")
        .migrate()
        .await
        .expect("migrate the scratch databases");
    runner.close().await;
    let catalog = connect(&catalog_url).await;
    let db = connect(&data_url).await;
    let vector: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = 'vector')",
    )
    .fetch_one(&db)
    .await
    .expect("read the available extensions");
    if vector {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
            .execute(&db)
            .await
            .expect("create the pgvector extension");
    } else {
        eprintln!("NOTE: no pgvector on this server; the vector index cells are skipped");
    }
    for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, '0') \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .execute(&catalog)
        .await
        .expect("set a delay to zero");
    }
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(&data_url)
    .execute(&catalog)
    .await
    .expect("point the catalog at the data database");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{stem}"))
        .execute(&catalog)
        .await
        .expect("seed the account row");
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: catalog_url.clone(),
            pool_size: 6,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch databases");
    Scratch {
        engine,
        db,
        catalog,
        admin,
        db_names: [catalog_name, data_name],
        vector,
        _guard: guard,
        _serial: serial,
    }
}

// --- rows, inputs and items -----------------------------------------------------

fn store() -> StoreIdentity {
    StoreIdentity {
        store_kind: StoreKind::S3,
        store_root: "s3://bucket/extenddb".to_owned(),
        store_id: "store-1".to_owned(),
    }
}

/// An AVAILABLE `backup_index` row of a backup of `table`, written through
/// the claim as a backup job writes it.
async fn available_backup(s: &Scratch, table: &str) -> BackupRow {
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
        backup_arn: backup_arn.clone(),
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
    let claim = s.engine.claim_backup(&row).await.expect("claim the row");
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

/// The target: a composite-key table with a GSI on `g`, an LSI on `l`, and
/// a vector index on `emb` (three dimensions) when the server has pgvector.
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

fn stream(items: Vec<Item>) -> futures::stream::BoxStream<'static, Result<Item, StorageError>> {
    Box::pin(futures::stream::iter(items.into_iter().map(Ok)))
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
    s: &Scratch,
    row: &BackupRow,
    target: &str,
    spool_host: Option<&str>,
) -> (TableDescription, RestoreTargetKey, Box<dyn JobClaim>) {
    let (desc, claim) = s
        .engine
        .claim_restore(
            ACCOUNT,
            target_input(target, s.vector),
            &registration(row, spool_host),
        )
        .await
        .expect("register the restore");
    let key = key_of(&desc);
    (desc, key, claim)
}

// --- reads --------------------------------------------------------------------

async fn table_status(s: &Scratch, name: &str) -> Option<String> {
    sqlx::query_scalar("SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2")
        .bind(ACCOUNT)
        .bind(name)
        .fetch_optional(&s.catalog)
        .await
        .expect("table status")
}

async fn record(s: &Scratch, key: &RestoreTargetKey) -> RestoreJobRow {
    s.engine
        .read_restore_job(key)
        .await
        .expect("read the record")
        .expect("the record exists")
}

/// `(status, kind, owner_generation, heartbeat_at, spool_host, completed_at)`
/// of the record, straight from the table.
async fn record_columns(
    s: &Scratch,
    table_id: &str,
) -> (String, String, i64, f64, Option<String>, Option<f64>) {
    sqlx::query_as(
        "SELECT status, kind, owner_generation, EXTRACT(EPOCH FROM heartbeat_at)::FLOAT8, \
         spool_host, EXTRACT(EPOCH FROM completed_at)::FLOAT8 FROM table_restores \
         WHERE table_id = $1",
    )
    .bind(table_id)
    .fetch_one(&s.catalog)
    .await
    .expect("read table_restores")
}

async fn rows_in(db: &PgPool, physical: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {physical}"))
        .fetch_one(db)
        .await
        .expect("count rows")
}

async fn base_rows(s: &Scratch, table_id: &str) -> i64 {
    rows_in(&s.db, &format!("\"_ddb_{table_id}\"")).await
}

/// `(index_name, index_id, index_status)` of the table's secondary indexes.
async fn indexes(s: &Scratch, table_id: &str) -> Vec<(String, String, String)> {
    sqlx::query_as(
        "SELECT index_name, index_id, index_status FROM indexes WHERE table_id = $1 \
         ORDER BY index_name",
    )
    .bind(table_id)
    .fetch_all(&s.catalog)
    .await
    .expect("read indexes")
}

async fn index_rows(s: &Scratch, table_id: &str, name: &str) -> i64 {
    let id = indexes(s, table_id)
        .await
        .into_iter()
        .find(|(n, _, _)| n == name)
        .map(|(_, id, _)| id)
        .expect("the index exists");
    rows_in(&s.db, &format!("\"_ddb_{id}\"")).await
}

/// `(index_id, index_status)` of the table's vector indexes.
async fn vector_indexes(s: &Scratch, table_id: &str) -> Vec<(String, String)> {
    sqlx::query_as("SELECT index_id, index_status FROM vector_indexes WHERE table_id = $1")
        .bind(table_id)
        .fetch_all(&s.catalog)
        .await
        .expect("read vector_indexes")
}

async fn vector_rows(s: &Scratch, table_id: &str) -> i64 {
    let (id, _) = vector_indexes(s, table_id)
        .await
        .pop()
        .expect("the vector index exists");
    rows_in(&s.db, &format!("\"_ddb_vec_{id}\"")).await
}

/// Whether the data database has a relation named `relation` (the physical
/// name without quotes), by `to_regclass`.
async fn relation_exists(s: &Scratch, relation: &str) -> bool {
    let regclass: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::TEXT")
        .bind(format!("\"{relation}\""))
        .fetch_one(&s.db)
        .await
        .expect("probe the relation");
    regclass.is_some()
}

async fn progress_rows(s: &Scratch, table_id: &str) -> Vec<(i64, i64)> {
    sqlx::query_as(
        "SELECT file_index, items_applied FROM restore_progress WHERE table_id = $1 \
         ORDER BY file_index",
    )
    .bind(table_id)
    .fetch_all(&s.db)
    .await
    .expect("read restore_progress")
}

/// The backend pid holding the restore lock on `table`, if any.
async fn lock_holder(s: &Scratch, table: &str) -> Option<i32> {
    sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted \
         AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         AND ((classid::bigint << 32) | objid::bigint) = hashtextextended($1, $2)",
    )
    .bind(format!("{ACCOUNT}/{table}"))
    .bind(RESTORE_LOCK_SEED)
    .fetch_optional(&s.catalog)
    .await
    .expect("find the advisory lock holder")
}

/// Date the record's heartbeat `age` into the past by hand. This proves the
/// takeover's predicate on a stale row, not the window boundary on the
/// database's clock, which `abandoned_restore_is_removed_only_when_its_heartbeat_is_stale`
/// in `backup_restore.rs` crosses.
async fn age_heartbeat(s: &Scratch, table_id: &str, age: Duration) -> f64 {
    sqlx::query_scalar(
        "UPDATE table_restores SET heartbeat_at = now() - make_interval(secs => $2) \
         WHERE table_id = $1 RETURNING EXTRACT(EPOCH FROM heartbeat_at)::FLOAT8",
    )
    .bind(table_id)
    .bind(age.as_secs_f64())
    .fetch_one(&s.catalog)
    .await
    .expect("age the heartbeat")
}

// --- the registration -----------------------------------------------------------

/// `claim_restore` creates the CREATING target with no scheduled transition
/// and its RUNNING `manifest` record carrying the store, the spool host and
/// the claim's generation, in one transaction; the description carries the
/// `RestoreSummary`; the record reads back through `read_restore_job` and
/// `list_restore_jobs`; the claim holds the name's advisory lock and its
/// heartbeat confirms.
#[tokio::test]
async fn claim_restore_registers_the_target_with_its_record() {
    if !pg_available("claim_restore_registers_the_target_with_its_record") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    assert_eq!(lock_holder(&s, "Restored").await, None);
    let (desc, key, claim) = register(&s, &row, "Restored", Some("host-a:/spool")).await;
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
        lock_holder(&s, "Restored").await.is_some(),
        "the claim holds the name lock"
    );

    let (status, kind, generation, heartbeat_at, spool_host, completed_at) =
        record_columns(&s, &key.table_id).await;
    assert_eq!((status.as_str(), kind.as_str()), ("RUNNING", "manifest"));
    assert_eq!(generation, i64::try_from(claim.generation()).unwrap());
    assert!(heartbeat_at > 0.0);
    assert_eq!(spool_host.as_deref(), Some("host-a:/spool"));
    assert_eq!(completed_at, None);
    let transition: Option<f64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM status_transition_at)::FLOAT8 FROM tables WHERE table_id = $1",
    )
    .bind(&key.table_id)
    .fetch_one(&s.catalog)
    .await
    .expect("read the transition");
    assert_eq!(
        transition, None,
        "no scheduled transition: the job flips the table"
    );

    let job = record(&s, &key).await;
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
    let running = s
        .engine
        .list_restore_jobs(Some(JobStatus::Running))
        .await
        .expect("list");
    assert_eq!(running, vec![job.clone()]);
    assert!(
        s.engine
            .list_restore_jobs(Some(JobStatus::Done))
            .await
            .expect("list")
            .is_empty()
    );
    assert_eq!(
        s.engine.backup_in_use(&row.key()).await.expect("in use"),
        Some("Restored".to_owned())
    );
    assert_eq!(claim.heartbeat().await.expect("beat"), Heartbeat::Confirmed);
    let heartbeat_now = record_columns(&s, &key.table_id).await.3;
    assert!(
        heartbeat_now >= heartbeat_at,
        "the heartbeat refreshed the row"
    );

    // The indexes a restore target is created with are ACTIVE from the start
    // (the flip has nothing to move), and the GSI and LSI data tables exist.
    let index_rows = indexes(&s, &key.table_id).await;
    assert_eq!(index_rows.len(), 2);
    assert!(index_rows.iter().all(|(_, _, status)| status == "ACTIVE"));
    assert_eq!(base_rows(&s, &key.table_id).await, 0);
    drop(claim);
    s.cleanup().await;
}

/// The ordering DeleteBackup wins: with `start_backup_delete` holding the
/// `backup_index` row FOR UPDATE, a registration waits behind it and, once
/// the row is DELETING, answers `Backup not found` and creates no target.
#[tokio::test]
async fn delete_backup_winning_before_the_registration_leaves_no_target() {
    if !pg_available("delete_backup_winning_before_the_registration_leaves_no_target") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    // `start_backup_delete` locks the row FOR UPDATE and then reads
    // `table_restores`; a gate on that table holds it with the row lock
    // taken.
    let mut gate = s.catalog.begin().await.expect("begin the gate");
    sqlx::query("LOCK TABLE table_restores IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *gate)
        .await
        .expect("hold DeleteBackup after its row lock");
    let key = row.key();
    let mut delete = Box::pin(s.engine.start_backup_delete(&key));
    assert!(
        tokio::time::timeout(Duration::from_secs(2), &mut delete)
            .await
            .is_err(),
        "DeleteBackup did not reach the gate"
    );
    let delete_waits: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_locks \
         WHERE relation = 'table_restores'::regclass AND NOT granted)",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("observe the blocked DeleteBackup");
    assert!(
        delete_waits,
        "DeleteBackup was not paused after its row lock"
    );

    let input = target_input("Late", s.vector);
    let reg = registration(&row, None);
    let mut restore = Box::pin(s.engine.claim_restore(ACCOUNT, input, &reg));
    assert!(
        tokio::time::timeout(Duration::from_millis(700), &mut restore)
            .await
            .is_err(),
        "the registration did not wait for DeleteBackup's row lock"
    );
    assert_eq!(
        table_status(&s, "Late").await,
        None,
        "the registration created its target before the row lock"
    );

    gate.commit().await.expect("let DeleteBackup finish");
    assert_eq!(
        delete.await.expect("DeleteBackup wins"),
        DeleteStart::Deleting
    );
    let err = restore
        .await
        .err()
        .expect("the registration finds the backup gone");
    assert!(
        extenddb_storage::backup_catalog::is_backup_not_found(&err),
        "{err:?}"
    );
    assert_eq!(
        table_status(&s, "Late").await,
        None,
        "no target was created"
    );
    assert_eq!(
        lock_holder(&s, "Late").await,
        None,
        "the refused registration released the name lock"
    );
    assert!(
        s.engine
            .list_restore_jobs(None)
            .await
            .expect("list")
            .is_empty()
    );
    s.cleanup().await;
}

/// The other ordering: a registration that commits first leaves
/// `start_backup_delete` with `InUse` naming the target, and a second
/// registration of the same backup is `BackupInUse` while the target is
/// CREATING.
#[tokio::test]
async fn a_registration_winning_before_delete_backup_keeps_the_backup_in_use() {
    if !pg_available("a_registration_winning_before_delete_backup_keeps_the_backup_in_use") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    let (_desc, key, claim) = register(&s, &row, "First", None).await;
    assert_eq!(
        s.engine
            .start_backup_delete(&row.key())
            .await
            .expect("DeleteBackup answers"),
        DeleteStart::InUse("First".to_owned())
    );
    let second = s
        .engine
        .claim_restore(
            ACCOUNT,
            target_input("Second", s.vector),
            &registration(&row, None),
        )
        .await
        .err()
        .expect("a second restore of the backup is refused");
    // The service's text names the backup, not the target.
    assert!(
        matches!(&second, StorageError::BackupInUse(m) if *m == backup_in_use_message(&row.backup_arn)),
        "{second:?}"
    );
    assert_eq!(table_status(&s, "Second").await, None);
    assert_eq!(lock_holder(&s, "Second").await, None);
    // The row stayed AVAILABLE through both refusals.
    let status: String = sqlx::query_scalar("SELECT status FROM backup_index WHERE backup_id = $1")
        .bind(&row.backup_id)
        .fetch_one(&s.catalog)
        .await
        .expect("status");
    assert_eq!(status, "AVAILABLE");
    drop(claim);
    let _ = key;
    s.cleanup().await;
}

/// A taken name: an ACTIVE table of the name is `TableAlreadyExists`; a
/// CREATING restore target of the name is `TableBeingRestored` naming its
/// source backup, whether the first registration's lock is still held or
/// its owner is gone.
#[tokio::test]
async fn a_taken_name_is_refused_as_create_table_or_as_a_restore_in_progress() {
    if !pg_available("a_taken_name_is_refused_as_create_table_or_as_a_restore_in_progress") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    s.engine
        .create_table(ACCOUNT, target_input("Taken", false))
        .await
        .expect("create a table of the name");
    let err = s
        .engine
        .claim_restore(
            ACCOUNT,
            target_input("Taken", s.vector),
            &registration(&row, None),
        )
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(&err, StorageError::TableAlreadyExists(n) if n == "Taken"),
        "{err:?}"
    );

    let (_desc, _key, claim) = register(&s, &row, "Restoring", None).await;
    let other = available_backup(&s, "Other").await;
    // With the first claim's lock held, the name lock refuses first.
    let err = s
        .engine
        .claim_restore(
            ACCOUNT,
            target_input("Restoring", s.vector),
            &registration(&other, None),
        )
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(&err, StorageError::TableBeingRestored { table_name, backup_arn }
            if table_name == "Restoring" && *backup_arn == row.backup_arn),
        "{err:?}"
    );
    // With the lock gone (a dead owner), the unique index refuses the same way.
    drop(claim);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while lock_holder(&s, "Restoring").await.is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the lock session did not end"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let err = s
        .engine
        .claim_restore(
            ACCOUNT,
            target_input("Restoring", s.vector),
            &registration(&other, None),
        )
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(&err, StorageError::TableBeingRestored { table_name, .. } if table_name == "Restoring"),
        "{err:?}"
    );
    s.cleanup().await;
}

// --- the takeover -------------------------------------------------------------

/// `claim_restore_dead` is refused while the owner's lock session lives and
/// while the heartbeat is young, and taken once both are gone: a fresh
/// generation, the claimant's `spool_host` and a refreshed `heartbeat_at`
/// (the database's clock) in one write, so a second taker finds the record
/// live at once; `list_dead_restore_jobs` lists the record with its members
/// only once stale; the old generation's writes are `NotMatched`.
#[tokio::test]
async fn a_dead_record_is_taken_over_with_the_claimants_spool_and_a_fresh_heartbeat() {
    if !pg_available("a_dead_record_is_taken_over_with_the_claimants_spool_and_a_fresh_heartbeat") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    let (_desc, key, claim) = register(&s, &row, "Target", Some("host-a:/spool")).await;
    let old_generation = claim.generation();
    // A live owner: the lock is held.
    assert!(
        s.engine
            .claim_restore_dead(&key, STALE, Some("host-b:/spool"))
            .await
            .expect("takeover")
            .is_none(),
        "a held lock is a live owner"
    );
    // The lock gone (the session ended) but the heartbeat young.
    let holder = lock_holder(&s, "Target")
        .await
        .expect("the owner's session");
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(holder)
        .execute(&s.catalog)
        .await
        .expect("end the owner's session");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while lock_holder(&s, "Target").await.is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the lock did not go"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The owner's next heartbeat is lost: its session is gone.
    assert!(
        !matches!(claim.heartbeat().await, Ok(Heartbeat::Confirmed)),
        "a terminated claim session cannot confirm"
    );
    assert!(
        s.engine
            .claim_restore_dead(&key, STALE, Some("host-b:/spool"))
            .await
            .expect("takeover")
            .is_none(),
        "a young heartbeat is a live owner"
    );
    assert!(
        s.engine
            .list_dead_restore_jobs(STALE)
            .await
            .expect("list")
            .is_empty(),
        "a young record is not listed"
    );
    assert_eq!(
        record(&s, &key).await.spool_host.as_deref(),
        Some("host-a:/spool")
    );

    // Stale: listed with its members, and taken over.
    let aged = age_heartbeat(&s, &key.table_id, Duration::from_secs(3600)).await;
    let dead = s.engine.list_dead_restore_jobs(STALE).await.expect("list");
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].target, key);
    assert_eq!(dead[0].kind, RestoreKind::Manifest);
    assert_eq!(dead[0].status, JobStatus::Running);
    assert_eq!(dead[0].store, Some(store()));
    assert_eq!(dead[0].spool_host.as_deref(), Some("host-a:/spool"));
    assert_eq!(dead[0].owner_generation, old_generation);
    let successor = s
        .engine
        .claim_restore_dead(&key, STALE, Some("host-b:/spool"))
        .await
        .expect("takeover")
        .expect("a stale record with no lock is dead");
    assert_ne!(successor.generation(), old_generation);
    let (status, _, generation, heartbeat_at, spool_host, _) =
        record_columns(&s, &key.table_id).await;
    assert_eq!(status, "RUNNING");
    assert_eq!(generation, i64::try_from(successor.generation()).unwrap());
    assert_eq!(
        spool_host.as_deref(),
        Some("host-b:/spool"),
        "the claimant's spool"
    );
    assert!(
        heartbeat_at > aged + 3_000.0,
        "the takeover refreshed heartbeat_at: {heartbeat_at} vs the aged {aged}"
    );
    let db_now: f64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM now())::FLOAT8")
        .fetch_one(&s.catalog)
        .await
        .expect("the database's clock");
    assert!(
        (db_now - heartbeat_at).abs() < 5.0,
        "the refreshed heartbeat is the database's now: {heartbeat_at} vs {db_now}"
    );
    assert!(
        lock_holder(&s, "Target").await.is_some(),
        "the successor holds the lock"
    );
    // A second taker is refused while the successor lives: the lock, and
    // the fresh heartbeat (the window a second instance's reconciler would
    // otherwise find the record stale in).
    assert!(
        s.engine
            .claim_restore_dead(&key, STALE, Some("host-c:/spool"))
            .await
            .expect("takeover")
            .is_none()
    );
    assert!(
        s.engine
            .list_dead_restore_jobs(STALE)
            .await
            .expect("list")
            .is_empty(),
        "the taken-over record is live to the listing at once"
    );
    // The old generation's writes match nothing: the flip through the old
    // claim, and the old claim's status write (its session is gone, so the
    // write's outcome is unknown and the re-read shows the successor).
    assert!(matches!(
        s.engine
            .finish_restore(&key, &*claim, 0, 0)
            .await
            .expect("finish"),
        Finished::NotMatched(_)
    ));
    assert_eq!(record(&s, &key).await.status, JobStatus::Running);
    let old_write = claim
        .set_status(
            JobStatus::Running,
            JobStatus::Failed,
            &StatusFields {
                failure_reason: Some("stale".to_owned()),
                completed: true,
                ..StatusFields::default()
            },
        )
        .await;
    assert!(
        matches!(old_write, Ok(WriteOutcome::Unknown) | Err(_)),
        "{old_write:?}"
    );
    let reread = claim.read().await.expect("the pool re-read");
    assert_eq!(reread.owner_generation, successor.generation());
    assert_eq!(reread.status, JobStatus::Running);
    // The successor's heartbeat confirms and its status write matches; after
    // its FAILED write a second write from it is NotMatched (not RUNNING).
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
                    failure_reason: Some("count".to_owned()),
                    completed: true,
                    ..StatusFields::default()
                },
            )
            .await
            .expect("write"),
        WriteOutcome::Matched
    );
    assert_eq!(
        successor
            .set_status(
                JobStatus::Running,
                JobStatus::Done,
                &StatusFields::default()
            )
            .await
            .expect("write"),
        WriteOutcome::NotMatched
    );
    let job = record(&s, &key).await;
    assert_eq!(job.status, JobStatus::Failed);
    assert_eq!(job.failure_reason.as_deref(), Some("count"));
    assert!(job.completed_at.is_some());
    assert!(
        s.engine
            .list_dead_restore_jobs(STALE)
            .await
            .expect("list")
            .is_empty(),
        "a fresh FAILED record is left to its job's removal"
    );
    drop(successor);
    s.cleanup().await;
}

// --- the load -------------------------------------------------------------------

/// `bulk_load` writes the base rows, the GSI, LSI and vector rows and the
/// file's progress row in one data transaction, across the 500-row batch
/// boundary; `start_at` is added to the recorded total; a duplicate key
/// within a file or against an earlier file fails the file with nothing of
/// it committed, no progress row included; a file whose progress is already
/// recorded fails whole, its index rows included.
#[tokio::test]
async fn bulk_load_writes_each_file_with_its_indexes_and_progress_in_one_transaction() {
    if !pg_available("bulk_load_writes_each_file_with_its_indexes_and_progress_in_one_transaction")
    {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    let (_desc, key, claim) = register(&s, &row, "Loaded", None).await;

    // File 0: 1200 items, past the 500-row batch.
    let applied = s
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            stream(items(0..1200)),
        )
        .await
        .expect("load file 0");
    assert_eq!(applied, 1200);
    assert_eq!(base_rows(&s, &key.table_id).await, 1200);
    assert_eq!(
        index_rows(&s, &key.table_id, "gix").await,
        600,
        "every second item has g"
    );
    assert_eq!(
        index_rows(&s, &key.table_id, "lix").await,
        400,
        "every third item has l"
    );
    if s.vector {
        assert_eq!(
            vector_rows(&s, &key.table_id).await,
            240,
            "every fifth item has emb"
        );
    }
    assert_eq!(progress_rows(&s, &key.table_id).await, vec![(0, 1200)]);
    assert_eq!(
        s.engine.count_restore_target(&key).await.expect("count"),
        1200
    );

    // File 1 resumed at item 5 of its 8: the record carries the running total.
    let applied = s
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 1,
                start_at: 5,
                reapplying: false,
            },
            stream(items(1205..1208)),
        )
        .await
        .expect("load file 1");
    assert_eq!(applied, 3);
    assert_eq!(
        progress_rows(&s, &key.table_id).await,
        vec![(0, 1200), (1, 8)]
    );
    assert_eq!(base_rows(&s, &key.table_id).await, 1203);

    // File 2 holds a key twice: nothing of it lands, no progress row.
    let mut twice = items(2000..2010);
    twice.push(item(2003));
    let err = s
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 2,
                start_at: 0,
                reapplying: false,
            },
            stream(twice),
        )
        .await
        .expect_err("a duplicate key fails the file");
    assert!(
        matches!(&err, StorageError::Validation(m)
            if m.starts_with("holds a primary key the target already has")
                && m.contains("the backup holds a key twice")),
        "the backend names no file (the engine prefixes the file's key): {err:?}"
    );
    assert_eq!(base_rows(&s, &key.table_id).await, 1203);
    assert_eq!(
        progress_rows(&s, &key.table_id).await,
        vec![(0, 1200), (1, 8)]
    );

    // File 3 repeats a key file 0 wrote: the same.
    let mut across = items(3000..3004);
    across.push(item(7));
    let err = s
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 3,
                start_at: 0,
                reapplying: false,
            },
            stream(across),
        )
        .await
        .expect_err("a key of an earlier file fails the file");
    assert!(
        matches!(&err, StorageError::Validation(m)
            if m.starts_with("holds a primary key the target already has")),
        "{err:?}"
    );
    assert_eq!(base_rows(&s, &key.table_id).await, 1203);
    assert_eq!(
        index_rows(&s, &key.table_id, "gix").await,
        600 + 1,
        "1206 has g"
    );
    assert_eq!(
        progress_rows(&s, &key.table_id).await,
        vec![(0, 1200), (1, 8)]
    );

    // A decode or validation failure the engine found mid-stream: the items
    // before it are not committed either.
    let failing: futures::stream::BoxStream<'static, Result<Item, StorageError>> =
        Box::pin(futures::stream::iter(
            items(4000..4003)
                .into_iter()
                .map(Ok)
                .chain(std::iter::once(Err(StorageError::Validation(
                    "key {\"pk\": \"p1\"}, attribute n: 1E126 is not a number".to_owned(),
                )))),
        ));
    let err = s
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 4,
                start_at: 0,
                reapplying: false,
            },
            failing,
        )
        .await
        .expect_err("an Err item fails the file");
    assert!(
        matches!(&err, StorageError::Validation(m) if m.contains("1E126")),
        "{err:?}"
    );
    assert_eq!(base_rows(&s, &key.table_id).await, 1203);
    assert_eq!(
        progress_rows(&s, &key.table_id).await,
        vec![(0, 1200), (1, 8)]
    );

    // File 0 again with keys the target lacks: its rows and index rows land
    // in the file's transaction, and the progress row's primary key rolls
    // the whole file back, index rows included.
    let err = s
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            stream(items(5000..5004)),
        )
        .await
        .expect_err("a recorded file is not applied twice");
    assert!(
        matches!(&err, StorageError::Validation(m) if m.contains("data file 0")),
        "{err:?}"
    );
    assert_eq!(base_rows(&s, &key.table_id).await, 1203);
    assert_eq!(index_rows(&s, &key.table_id, "gix").await, 601);
    assert_eq!(
        index_rows(&s, &key.table_id, "lix").await,
        400 + 1,
        "1206 has l"
    );
    if s.vector {
        assert_eq!(
            vector_rows(&s, &key.table_id).await,
            240 + 1,
            "1205 has emb"
        );
    }
    assert_eq!(
        progress_rows(&s, &key.table_id).await,
        vec![(0, 1200), (1, 8)]
    );

    // File 1's row says 8 of its items were applied and the file holds 11
    // (the decoder died after the file's commit and the job's FAILED write
    // never landed): the resume starts the file at 8, and the rest completes
    // it, the row updated in place instead of refused on its key.
    let applied = s
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 1,
                start_at: 8,
                reapplying: false,
            },
            stream(items(1208..1211)),
        )
        .await
        .expect("a file resumed from a short progress row completes");
    assert_eq!(applied, 3);
    assert_eq!(
        progress_rows(&s, &key.table_id).await,
        vec![(0, 1200), (1, 11)],
        "the short row is updated to the file's total"
    );
    assert_eq!(base_rows(&s, &key.table_id).await, 1206);
    assert_eq!(index_rows(&s, &key.table_id, "gix").await, 603);
    assert_eq!(index_rows(&s, &key.table_id, "lix").await, 402);
    if s.vector {
        assert_eq!(vector_rows(&s, &key.table_id).await, 242);
    }

    // Another account's key, or a wrong table id, reaches no table.
    let foreign = RestoreTargetKey {
        account_id: "210987654321".to_owned(),
        ..key.clone()
    };
    assert!(matches!(
        s.engine.count_restore_target(&foreign).await,
        Err(StorageError::TableNotFound(_))
    ));
    assert!(matches!(
        s.engine
            .bulk_load(
                &foreign,
                &*claim,
                BulkLoadFile {
                    file_index: 9,
                    start_at: 0,
                    reapplying: false,
                },
                stream(items(9000..9001)),
            )
            .await,
        Err(StorageError::TableNotFound(_))
    ));
    drop(claim);
    s.cleanup().await;
}

/// `finish_restore` marks the record DONE and the table and every index
/// ACTIVE in one transaction, conditioned on the claim's generation and on
/// CREATING, then removes the progress rows; a second call, or one under a
/// stale generation, is `NotMatched` and writes nothing. The progress sweep
/// keeps a RUNNING target's rows and removes any other's;
/// `remove_restore_target` deletes a target's rows.
#[tokio::test]
async fn finish_restore_flips_the_table_and_every_index_and_clears_the_progress() {
    if !pg_available("finish_restore_flips_the_table_and_every_index_and_clears_the_progress") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    let (_desc, key, claim) = register(&s, &row, "Done", None).await;
    let other_row = available_backup(&s, "Other").await;
    let (_desc2, other, other_claim) = register(&s, &other_row, "Still", None).await;
    for (k, c) in [(&key, &claim), (&other, &other_claim)] {
        s.engine
            .bulk_load(
                k,
                &**c,
                BulkLoadFile {
                    file_index: 0,
                    start_at: 0,
                    reapplying: false,
                },
                stream(items(0..30)),
            )
            .await
            .expect("load");
    }
    // An index a backend left CREATING (none does on this backend; the flip
    // states the rule once) is moved with the table.
    sqlx::query(
        "UPDATE indexes SET index_status = 'CREATING' WHERE table_id = $1 AND index_name = 'gix'",
    )
    .bind(&key.table_id)
    .execute(&s.catalog)
    .await
    .expect("set an index CREATING");
    if s.vector {
        sqlx::query("UPDATE vector_indexes SET index_status = 'CREATING' WHERE table_id = $1")
            .bind(&key.table_id)
            .execute(&s.catalog)
            .await
            .expect("set the vector index CREATING");
    }

    // The sweep: `Still`'s rows stay while it is RUNNING; a row of a table
    // that is gone goes.
    sqlx::query(
        "INSERT INTO restore_progress (table_id, file_index, items_applied) VALUES ('gone', 0, 5)",
    )
    .execute(&s.db)
    .await
    .expect("an orphan row");
    // A caller whose RUNNING list predates both registrations (the list is
    // read before the sweep; a registration and its first file can land in
    // between): the sweep protects the RUNNING records it reads itself after
    // the candidates, so neither live row goes, only the orphan.
    assert_eq!(
        s.engine.sweep_restore_progress(&[]).await.expect("sweep"),
        1,
        "the orphan row alone, with an empty caller list"
    );
    assert_eq!(progress_rows(&s, &key.table_id).await, vec![(0, 30)]);
    assert_eq!(progress_rows(&s, &other.table_id).await, vec![(0, 30)]);
    sqlx::query(
        "INSERT INTO restore_progress (table_id, file_index, items_applied) VALUES ('gone', 0, 5)",
    )
    .execute(&s.db)
    .await
    .expect("the orphan row again");
    let running: Vec<String> = s
        .engine
        .list_restore_jobs(Some(JobStatus::Running))
        .await
        .expect("list")
        .into_iter()
        .map(|j| j.target.table_id)
        .collect();
    assert_eq!(running.len(), 2);
    assert_eq!(
        s.engine
            .sweep_restore_progress(&running)
            .await
            .expect("sweep"),
        1
    );
    assert_eq!(progress_rows(&s, &key.table_id).await, vec![(0, 30)]);
    assert_eq!(progress_rows(&s, &other.table_id).await, vec![(0, 30)]);
    assert_eq!(
        s.engine.read_restore_progress(&key).await.expect("read"),
        vec![(0, 30)]
    );

    // A stale generation flips nothing.
    let stale_row = available_backup(&s, "Stale").await;
    let (_d, stale_key, stale_claim) = register(&s, &stale_row, "StaleOwner", None).await;
    let _ = stale_key;
    assert!(matches!(
        s.engine
            .finish_restore(&key, &*stale_claim, 30, 4096)
            .await
            .expect("finish"),
        Finished::NotMatched(_)
    ));
    assert_eq!(table_status(&s, "Done").await.as_deref(), Some("CREATING"));
    assert_eq!(record(&s, &key).await.status, JobStatus::Running);
    assert_eq!(progress_rows(&s, &key.table_id).await, vec![(0, 30)]);
    drop(stale_claim);

    assert_eq!(
        s.engine
            .finish_restore(&key, &*claim, 30, 4096)
            .await
            .expect("finish"),
        Finished::Active
    );
    assert_eq!(table_status(&s, "Done").await.as_deref(), Some("ACTIVE"));
    let (status, _, _, _, _, completed_at) = record_columns(&s, &key.table_id).await;
    assert_eq!(status, "DONE");
    assert!(completed_at.is_some());
    let counts: (i64, i64) =
        sqlx::query_as("SELECT item_count, table_size_bytes FROM tables WHERE table_id = $1")
            .bind(&key.table_id)
            .fetch_one(&s.catalog)
            .await
            .expect("counts");
    assert_eq!(counts, (30, 4096));
    assert!(
        indexes(&s, &key.table_id)
            .await
            .iter()
            .all(|(_, _, status)| status == "ACTIVE"),
        "every secondary index is ACTIVE"
    );
    if s.vector {
        assert!(
            vector_indexes(&s, &key.table_id)
                .await
                .iter()
                .all(|(_, status)| status == "ACTIVE"),
            "the vector index is ACTIVE"
        );
    }
    assert!(
        progress_rows(&s, &key.table_id).await.is_empty(),
        "the progress rows are gone"
    );
    assert_eq!(progress_rows(&s, &other.table_id).await, vec![(0, 30)]);
    let job = record(&s, &key).await;
    assert_eq!(job.status, JobStatus::Done);
    assert!(job.completed_at.is_some());
    // The job record's heartbeat is no longer refreshed: not RUNNING.
    assert_eq!(claim.heartbeat().await.expect("beat"), Heartbeat::Lost);
    // A second flip matches nothing.
    assert!(matches!(
        s.engine
            .finish_restore(&key, &*claim, 30, 4096)
            .await
            .expect("finish"),
        Finished::NotMatched(_)
    ));
    // The restored table reads through the data plane as any ACTIVE table.
    let desc = s
        .engine
        .describe_table(
            ACCOUNT,
            extenddb_core::types::DescribeTableInput {
                table_name: "Done".to_owned(),
            },
        )
        .await
        .expect("describe");
    assert_eq!(desc.table_status, TableStatus::Active);
    assert_eq!(desc.item_count, 30);

    // `remove_restore_target` on the other target deletes its progress rows
    // with the target.
    assert!(
        s.engine
            .remove_restore_target(&other)
            .await
            .expect("remove")
    );
    assert!(progress_rows(&s, &other.table_id).await.is_empty());
    assert_eq!(table_status(&s, "Still").await, None);
    drop(other_claim);
    drop(claim);
    s.cleanup().await;
}

/// `remove_restore_target` of a target with a GSI, an LSI and (with pgvector)
/// a vector index drops every relation the target's rows named: the data
/// table, each secondary index's table and the vector index's `_ddb_vec_`
/// table, so a failed vector restore leaves no relation holding its items
/// with no catalog row naming it.
#[tokio::test]
async fn remove_restore_target_drops_the_vector_relation_with_the_data_table() {
    if !pg_available("remove_restore_target_drops_the_vector_relation_with_the_data_table") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    let (_desc, key, claim) = register(&s, &row, "Vectors", None).await;
    s.engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            stream(items(0..30)),
        )
        .await
        .expect("load");
    let mut relations: Vec<String> = vec![format!("_ddb_{}", key.table_id)];
    relations.extend(
        indexes(&s, &key.table_id)
            .await
            .into_iter()
            .map(|(_, id, _)| format!("_ddb_{id}")),
    );
    if s.vector {
        assert_eq!(
            vector_rows(&s, &key.table_id).await,
            6,
            "every fifth item has emb"
        );
        relations.extend(
            vector_indexes(&s, &key.table_id)
                .await
                .into_iter()
                .map(|(id, _)| format!("_ddb_vec_{id}")),
        );
        assert_eq!(
            relations.len(),
            4,
            "the data table, two index tables, one vector table"
        );
    }
    for relation in &relations {
        assert!(
            relation_exists(&s, relation).await,
            "{relation} exists before the removal"
        );
    }

    assert!(s.engine.remove_restore_target(&key).await.expect("remove"));
    assert_eq!(table_status(&s, "Vectors").await, None);
    if s.vector {
        assert!(vector_indexes(&s, &key.table_id).await.is_empty());
    }
    for relation in &relations {
        assert!(
            !relation_exists(&s, relation).await,
            "{relation} is dropped with the target"
        );
    }
    drop(claim);
    s.cleanup().await;
}

/// A `finish_restore` whose progress delete fails after the flip committed
/// (the data database cannot run the `DELETE`: the `restore_progress` table
/// is renamed under it) is `Active`: the table is ACTIVE and the record DONE,
/// the failure is logged and not returned, the rows are left, and the
/// reconciler's `sweep_restore_progress` removes them once the record is no
/// longer RUNNING.
#[tokio::test]
async fn a_progress_delete_failing_after_the_flip_leaves_the_restore_finished() {
    if !pg_available("a_progress_delete_failing_after_the_flip_leaves_the_restore_finished") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    let (_desc, key, claim) = register(&s, &row, "Flipped", None).await;
    s.engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            stream(items(0..30)),
        )
        .await
        .expect("load");
    assert_eq!(progress_rows(&s, &key.table_id).await, vec![(0, 30)]);

    sqlx::query("ALTER TABLE restore_progress RENAME TO restore_progress_away")
        .execute(&s.db)
        .await
        .expect("take the progress table away");
    let finished = s.engine.finish_restore(&key, &*claim, 30, 4096).await;
    sqlx::query("ALTER TABLE restore_progress_away RENAME TO restore_progress")
        .execute(&s.db)
        .await
        .expect("bring the progress table back");
    assert!(
        matches!(finished, Ok(Finished::Active)),
        "a progress delete failing after the flip is not the restore's failure: {finished:?}"
    );
    assert_eq!(table_status(&s, "Flipped").await.as_deref(), Some("ACTIVE"));
    assert_eq!(record(&s, &key).await.status, JobStatus::Done);
    assert_eq!(
        progress_rows(&s, &key.table_id).await,
        vec![(0, 30)],
        "the rows the delete could not remove are left"
    );
    assert_eq!(
        s.engine.sweep_restore_progress(&[]).await.expect("sweep"),
        1,
        "the reconciler's sweep removes the DONE target's rows"
    );
    assert!(progress_rows(&s, &key.table_id).await.is_empty());
    drop(claim);
    s.cleanup().await;
}

/// Terminating the claim session: the next heartbeat is lost, the status
/// write's outcome is unknown, and the record stays RUNNING for the
/// reconciler once stale.
#[tokio::test]
async fn a_terminated_claim_session_loses_its_next_heartbeat() {
    if !pg_available("a_terminated_claim_session_loses_its_next_heartbeat") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    let (_desc, key, claim) = register(&s, &row, "Cut", None).await;
    assert_eq!(claim.heartbeat().await.expect("beat"), Heartbeat::Confirmed);
    let holder = lock_holder(&s, "Cut").await.expect("the claim session");
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(holder)
        .execute(&s.catalog)
        .await
        .expect("terminate");
    let beat = claim.heartbeat().await;
    assert!(!matches!(beat, Ok(Heartbeat::Confirmed)), "{beat:?}");
    let write = claim
        .set_status(
            JobStatus::Running,
            JobStatus::Done,
            &StatusFields::default(),
        )
        .await;
    assert!(
        matches!(write, Ok(WriteOutcome::Unknown) | Err(_)),
        "{write:?}"
    );
    assert_eq!(record(&s, &key).await.status, JobStatus::Running);
    assert_eq!(table_status(&s, "Cut").await.as_deref(), Some("CREATING"));
    s.cleanup().await;
}

/// A legacy restore record is not a takeover candidate: `claim_restore_dead`
/// leaves a stale `kind = legacy` record to the reconciler's legacy duty.
#[tokio::test]
async fn a_legacy_record_is_not_taken_over() {
    if !pg_available("a_legacy_record_is_not_taken_over") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    let (_desc, key, claim) = register(&s, &row, "Legacyish", None).await;
    drop(claim);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while lock_holder(&s, "Legacyish").await.is_some() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    sqlx::query("UPDATE table_restores SET kind = 'legacy' WHERE table_id = $1")
        .bind(&key.table_id)
        .execute(&s.catalog)
        .await
        .expect("make the record legacy");
    age_heartbeat(&s, &key.table_id, Duration::from_secs(3600)).await;
    assert!(
        s.engine
            .claim_restore_dead(&key, STALE, None)
            .await
            .expect("takeover")
            .is_none()
    );
    assert_eq!(record(&s, &key).await.kind, RestoreKind::Legacy);
    assert_eq!(lock_holder(&s, "Legacyish").await, None);
    s.cleanup().await;
}

/// `sweep_restore_progress` reads the candidate progress-row ids before the
/// live RUNNING set, so a registration that commits and writes a progress
/// row between the two reads keeps that row: the row is not among the
/// candidates read first, and the record is RUNNING in the live set read
/// after. An orphan row with no record is swept in the same call, so the
/// sweep is seen to run. With the two reads swapped the registration's row
/// would be a candidate and not live, and would be deleted.
#[tokio::test]
async fn sweep_keeps_a_progress_row_registered_between_the_two_reads() {
    if !pg_available("sweep_keeps_a_progress_row_registered_between_the_two_reads") {
        return;
    }
    let s = scratch().await;
    // An orphan progress row (no record names it): a candidate the sweep
    // removes, so the sweep is doing work in this cycle.
    sqlx::query("INSERT INTO restore_progress (table_id, file_index, items_applied) VALUES ('orphan-tid', 0, 5)")
        .execute(&s.db)
        .await
        .expect("seed an orphan progress row");

    let (arrived, proceed) = extenddb_storage_postgres::sweep_pause::arm();
    let engine = s.engine.clone();
    let sweep = tokio::spawn(async move { engine.sweep_restore_progress(&[]).await });

    // The sweep has read the candidate ids and is paused before the live
    // set: register a RUNNING target and write its progress row now.
    arrived.notified().await;
    let row = available_backup(&s, "Music").await;
    let (_desc, key, claim) = register(&s, &row, "Late", None).await;
    sqlx::query(
        "INSERT INTO restore_progress (table_id, file_index, items_applied) VALUES ($1, 0, 7)",
    )
    .bind(&key.table_id)
    .execute(&s.db)
    .await
    .expect("the registration's progress row");
    proceed.notify_one();

    let removed = sweep.await.expect("join").expect("sweep");
    assert_eq!(removed, 1, "only the orphan progress row was swept");
    assert_eq!(
        progress_rows(&s, &key.table_id).await,
        vec![(0, 7)],
        "the live registration's progress row stayed"
    );
    assert!(
        progress_rows(&s, "orphan-tid").await.is_empty(),
        "the orphan row went"
    );
    drop(claim);
    s.cleanup().await;
}
