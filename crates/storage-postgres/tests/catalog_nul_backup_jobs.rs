// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The store-backed backup job and restore job with attribute names that
//! hold U+0000 or U+0001, on PostgreSQL.
//!
//! The backup job writes the source table, its names included, to
//! `backup_index.source_table_details_json`, and `DescribeBackup` reads it
//! back. The restore job reads the key schema of the target and of its
//! indexes from the catalog rows that the registration created. PostgreSQL
//! `jsonb` refuses U+0000, so these documents are stored through the escape
//! of `catalog_json`, as the other catalog documents that carry names are.
//! The tests take a backup through the engine's `dispatch` into a
//! filesystem store, as a server with `[backup.store]` does, restore it, and
//! compare the names that `DescribeBackup` and `DescribeTable` return with
//! the names of the source table, byte for byte.
//!
//! An item attribute name that holds U+0000 is item data, which the
//! PostgreSQL item columns cannot hold yet. So the table whose key schema
//! holds U+0000 has no items, and the items of the other table carry names
//! that hold U+0001 only. See `catalog_nul_support` for the database these
//! tests need.

mod catalog_nul_support;

use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use catalog_nul_support::{
    ACCOUNT, Options, REGION, Scratch, base_conn, documents, scratch, skip, stored_text,
};
use extenddb_backup_store::FilesystemStore;
use extenddb_core::backup_format::BackupArn;
use extenddb_core::limits::LimitsConfig;
use extenddb_engine::{
    BackupRuntime, ConfiguredStore, OperationContext, check_configured_store, dispatch,
};
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, JobStatus, StatusFields, StoreIdentity, StoreKind, WriteOutcome,
};
use extenddb_storage::{BackupCatalog, CancellationToken, StorageEngine};
use futures::StreamExt;
use serde_json::{Value, json};

/// U+0000 in the partition key.
const PK0: &str = "p\u{0}k";
/// U+0001 in the sort key.
const SK1: &str = "s\u{1}k";
/// U+0001 in a GSI partition key.
const GK1: &str = "g\u{1}k";
/// U+0000 in a projected name.
const N0: &str = "n\u{0}k";
/// U+0001 in the partition key of the table with items.
const PK1: &str = "p\u{1}k";
/// A U+0001 pair in its sort key: the stored form of U+0000, which the
/// decoder must not read as U+0000.
const SK11: &str = "s\u{1}\u{1}k";
/// U+0001 U+0002 in a projected name: the stored form of U+0001.
const NK: &str = "n\u{1}\u{2}";
/// U+0000 in the key of a GSI that no item carries.
const ZK: &str = "z\u{0}k";

/// A scratch directory under the cargo target directory, removed on drop.
struct StoreDir(PathBuf);

impl Drop for StoreDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The engine on the scratch databases with a healthy filesystem store, as
/// a server with `[backup.store]` runs it.
struct Jobs {
    ctx: OperationContext,
    runtime: Arc<BackupRuntime>,
    shutdown: CancellationToken,
    _dir: StoreDir,
}

async fn jobs(s: &Scratch) -> Jobs {
    let dir = StoreDir(
        PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("nul-backup-jobs-{}", uuid::Uuid::new_v4())),
    );
    std::fs::create_dir_all(&dir.0).expect("the scratch directory");
    let root = dir.0.join("backups");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .expect("the store root");
    let storage: Arc<dyn StorageEngine> = Arc::new(s.engine.clone());
    let store = Arc::new(
        FilesystemStore::open(&root)
            .await
            .expect("filesystem store"),
    );
    let configured = ConfiguredStore::checking(
        StoreKind::Filesystem,
        root.to_str().expect("UTF-8 path").to_owned(),
        store,
        None,
    );
    let outcome = check_configured_store(&configured, &*storage, "postgres").await;
    assert!(outcome.is_ok(), "the store's first check: {outcome:?}");
    configured.record_check(outcome);
    let shutdown = CancellationToken::new();
    let limits = Arc::new(LimitsConfig::default());
    let runtime = Arc::new(
        BackupRuntime::new(4)
            .with_store(configured)
            .with_source("postgres", "test")
            .with_limits(limits.clone())
            .with_shutdown(shutdown.child_token()),
    );
    let ctx = OperationContext {
        storage,
        backup: runtime.clone(),
        limits,
        region: Arc::from(REGION),
        account_id: Arc::from(ACCOUNT),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(Vec::new()),
        authorized_key_info: Default::default(),
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
        item_authorizer: None,
    };
    Jobs {
        ctx,
        runtime,
        shutdown,
        _dir: dir,
    }
}

async fn try_call(ctx: &OperationContext, operation: &str, body: Value) -> Result<Value, String> {
    dispatch(operation, body, ctx, "127.0.0.1:0")
        .await
        .map(|r| r.body)
        .map_err(|e| format!("{e:?}"))
}

async fn call(ctx: &OperationContext, operation: &str, body: Value) -> Value {
    try_call(ctx, operation, body)
        .await
        .unwrap_or_else(|e| panic!("{operation}: {e}"))
}

/// `DescribeTable` of `table` once it is ACTIVE and no backup or restore
/// job runs, within 60 s.
async fn wait_active(jobs: &Jobs, table: &str) -> Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let desc = try_call(&jobs.ctx, "DescribeTable", json!({"TableName": table}))
            .await
            .unwrap_or_else(|e| panic!("DescribeTable of {table}: {e}"));
        if desc["Table"]["TableStatus"] == "ACTIVE" {
            while jobs.runtime.running_jobs() > 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "{table} is ACTIVE, but a backup or restore job still runs after 60 s"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            return desc;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{table} did not reach ACTIVE: {desc}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Take a backup of `table` with the backup job and wait until it is
/// AVAILABLE and its job ended, within 60 s; its `DescribeBackup`.
async fn backup(jobs: &Jobs, table: &str) -> Value {
    let created = call(
        &jobs.ctx,
        "CreateBackup",
        json!({"TableName": table, "BackupName": "nightly"}),
    )
    .await;
    let arn = created["BackupDetails"]["BackupArn"]
        .as_str()
        .expect("an ARN")
        .to_owned();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let described = call(&jobs.ctx, "DescribeBackup", json!({"BackupArn": arn})).await;
        let status = &described["BackupDescription"]["BackupDetails"]["BackupStatus"];
        if status == "AVAILABLE" {
            while jobs.runtime.running_jobs() > 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the backup of {table} is AVAILABLE, but its job still runs after 60 s"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            return described;
        }
        assert!(
            status == "CREATING" && std::time::Instant::now() < deadline,
            "the backup of {table} did not become AVAILABLE: {described}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Restore the backup `described` names into `target` with the restore job
/// and wait until the target is ACTIVE; its `DescribeTable`.
async fn restore(jobs: &Jobs, described: &Value, target: &str) -> Value {
    let arn = &described["BackupDescription"]["BackupDetails"]["BackupArn"];
    call(
        &jobs.ctx,
        "RestoreTableFromBackup",
        json!({"TargetTableName": target, "BackupArn": arn}),
    )
    .await;
    wait_active(jobs, target).await
}

/// The `AttributeName` of each element of a JSON array.
fn json_names(v: &Value) -> Vec<&str> {
    v.as_array()
        .unwrap_or_else(|| panic!("not an array: {v}"))
        .iter()
        .map(|e| e["AttributeName"].as_str().expect("an AttributeName"))
        .collect()
}

/// The attribute definition names of a JSON array, sorted.
fn json_def_names(v: &Value) -> Vec<&str> {
    let mut names = json_names(v);
    names.sort_unstable();
    names
}

/// The JSON strings of an array.
fn json_strings(v: &Value) -> Vec<&str> {
    v.as_array()
        .unwrap_or_else(|| panic!("not an array: {v}"))
        .iter()
        .map(|e| e.as_str().expect("a string"))
        .collect()
}

/// The GSI `name` of a `DescribeTable` or `SourceTableFeatureDetails`.
fn gsi<'a>(indexes: &'a Value, name: &str) -> &'a Value {
    indexes
        .as_array()
        .unwrap_or_else(|| panic!("no GSIs: {indexes}"))
        .iter()
        .find(|i| i["IndexName"] == name)
        .unwrap_or_else(|| panic!("no GSI {name} in {indexes}"))
}

/// The `backup_index` documents of `table`'s backups, as stored.
async fn stored_backup_index(s: &Scratch, table: &str) -> String {
    documents(
        &s.catalog,
        "SELECT source_table_details_json::text FROM backup_index WHERE table_name = $1",
        table,
    )
    .await
    .join(" ")
}

/// Every name in its stored form, and no U+0000.
fn assert_stored_escaped(text: &str, names: &[&str], what: &str) {
    assert!(!text.is_empty(), "{what}: no document");
    assert!(!text.contains("\\u0000"), "{what} holds U+0000: {text}");
    for name in names {
        let stored = format!("\"{}\"", stored_text(name));
        assert!(text.contains(&stored), "{what}: no {stored} in {text}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_backed_backup_and_restore_keep_key_names_that_hold_u0000_and_u0001() {
    let test = "a_store_backed_backup_and_restore_keep_key_names_that_hold_u0000_and_u0001";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(test, Options::default()).await.expect("a scratch");
    let jobs = jobs(&s).await;
    let table = "nul_keys";
    call(
        &jobs.ctx,
        "CreateTable",
        json!({
            "TableName": table,
            "KeySchema": [
                {"AttributeName": PK0, "KeyType": "HASH"},
                {"AttributeName": SK1, "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": PK0, "AttributeType": "S"},
                {"AttributeName": SK1, "AttributeType": "S"},
                {"AttributeName": GK1, "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "by_u1",
                "KeySchema": [{"AttributeName": GK1, "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": [N0]}
            }]
        }),
    )
    .await;
    wait_active(&jobs, table).await;

    let described = backup(&jobs, table).await;
    assert_stored_escaped(
        &stored_backup_index(&s, table).await,
        &[PK0, SK1, GK1, N0],
        "the backup_index row",
    );
    let details = &described["BackupDescription"];
    assert_eq!(
        json_names(&details["SourceTableDetails"]["KeySchema"]),
        [PK0, SK1]
    );
    let source_gsi = gsi(
        &details["SourceTableFeatureDetails"]["GlobalSecondaryIndexes"],
        "by_u1",
    );
    assert_eq!(json_names(&source_gsi["KeySchema"]), [GK1]);
    assert_eq!(
        json_strings(&source_gsi["Projection"]["NonKeyAttributes"]),
        [N0]
    );

    let restored_name = "nul_keys_restored";
    let restored = restore(&jobs, &described, restored_name).await;
    let restored = &restored["Table"];
    assert_eq!(json_names(&restored["KeySchema"]), [PK0, SK1]);
    assert_eq!(
        json_def_names(&restored["AttributeDefinitions"]),
        json_def_names(&json!([
            {"AttributeName": PK0}, {"AttributeName": SK1}, {"AttributeName": GK1}
        ]))
    );
    let restored_gsi = gsi(&restored["GlobalSecondaryIndexes"], "by_u1");
    assert_eq!(json_names(&restored_gsi["KeySchema"]), [GK1]);
    assert_eq!(
        json_strings(&restored_gsi["Projection"]["NonKeyAttributes"]),
        [N0]
    );
    jobs.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn items_restored_by_the_restore_job_reach_a_gsi_keyed_on_a_u0001_name() {
    let test = "items_restored_by_the_restore_job_reach_a_gsi_keyed_on_a_u0001_name";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(test, Options::default()).await.expect("a scratch");
    let jobs = jobs(&s).await;
    let table = "nul_items";
    call(
        &jobs.ctx,
        "CreateTable",
        json!({
            "TableName": table,
            "KeySchema": [
                {"AttributeName": PK1, "KeyType": "HASH"},
                {"AttributeName": SK11, "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": PK1, "AttributeType": "S"},
                {"AttributeName": SK11, "AttributeType": "S"},
                {"AttributeName": GK1, "AttributeType": "S"},
                {"AttributeName": ZK, "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "by_u1",
                    "KeySchema": [{"AttributeName": GK1, "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": [NK]}
                },
                {
                    "IndexName": "by_u0",
                    "KeySchema": [{"AttributeName": ZK, "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "KEYS_ONLY"}
                }
            ]
        }),
    )
    .await;
    wait_active(&jobs, table).await;
    for i in 0..6 {
        call(
            &jobs.ctx,
            "PutItem",
            json!({"TableName": table, "Item": {
                PK1: {"S": format!("item-{i}")},
                SK11: {"S": i.to_string()},
                GK1: {"S": format!("g-{}", i % 2)},
                NK: {"S": format!("v{i}")},
                "other": {"S": "not projected"}
            }}),
        )
        .await;
    }
    let query = |target: &str| {
        json!({
            "TableName": target,
            "IndexName": "by_u1",
            "KeyConditionExpression": "#g = :g",
            "ExpressionAttributeNames": {"#g": GK1},
            "ExpressionAttributeValues": {":g": {"S": "g-0"}}
        })
    };
    let source_page = call(&jobs.ctx, "Query", query(table)).await;
    assert_eq!(source_page["Count"], 3, "{source_page}");

    let described = backup(&jobs, table).await;
    assert_stored_escaped(
        &stored_backup_index(&s, table).await,
        &[PK1, SK11, GK1, NK, ZK],
        "the backup_index row",
    );
    assert_eq!(
        json_names(&described["BackupDescription"]["SourceTableDetails"]["KeySchema"]),
        [PK1, SK11]
    );

    let restored_name = "nul_items_restored";
    let restored = restore(&jobs, &described, restored_name).await;
    let restored = &restored["Table"];
    assert_eq!(json_names(&restored["KeySchema"]), [PK1, SK11]);
    let scanned = call(
        &jobs.ctx,
        "Scan",
        json!({"TableName": restored_name, "Select": "COUNT"}),
    )
    .await;
    assert_eq!(scanned["Count"], 6, "{scanned}");
    assert_eq!(
        json_names(&gsi(&restored["GlobalSecondaryIndexes"], "by_u1")["KeySchema"]),
        [GK1]
    );
    assert_eq!(
        json_names(&gsi(&restored["GlobalSecondaryIndexes"], "by_u0")["KeySchema"]),
        [ZK]
    );
    let page = call(&jobs.ctx, "Query", query(restored_name)).await;
    assert_eq!(page["Count"], 3, "{page}");
    let mut keys: Vec<&str> = page["Items"]
        .as_array()
        .expect("Items")
        .iter()
        .map(|item| {
            assert_eq!(item[GK1]["S"], "g-0", "{item}");
            assert!(item[NK]["S"].is_string(), "{item}");
            assert!(item.get("other").is_none(), "{item}");
            item[PK1]["S"].as_str().expect("the partition key")
        })
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["item-0", "item-2", "item-4"]);
    jobs.shutdown.cancel();
}

/// The source table of the backup in the `backup_index` round trip: the
/// manifest fixture's, with names that hold U+0000 and U+0001 in every
/// member that carries one.
fn source_table(table: &str) -> Value {
    let manifest: Value = serde_json::from_str(include_str!(
        "../../core/testdata/extenddb-manifest/v1.json"
    ))
    .expect("fixture");
    let mut source = manifest["source_table"].clone();
    source["table_name"] = json!(table);
    source["table_arn"] = json!(format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{table}"));
    source["key_schema"] = json!([
        {"AttributeName": PK0, "KeyType": "HASH"},
        {"AttributeName": SK11, "KeyType": "RANGE"}
    ]);
    source["attribute_definitions"] = json!([
        {"AttributeName": PK0, "AttributeType": "S"},
        {"AttributeName": SK11, "AttributeType": "S"},
        {"AttributeName": GK1, "AttributeType": "S"}
    ]);
    source["global_secondary_indexes"][0]["KeySchema"] =
        json!([{"AttributeName": GK1, "KeyType": "HASH"}]);
    source["global_secondary_indexes"][0]["Projection"]["NonKeyAttributes"] = json!([N0, NK]);
    source["local_secondary_indexes"][0]["KeySchema"] = json!([
        {"AttributeName": PK0, "KeyType": "HASH"},
        {"AttributeName": SK11, "KeyType": "RANGE"}
    ]);
    source["vector_indexes"][0]["VectorAttribute"]["AttributeName"] = json!(ZK);
    source["time_to_live"]["AttributeName"] = json!(SK1);
    source
}

#[tokio::test]
async fn a_backup_index_row_keeps_names_that_hold_u0000_and_u0001() {
    let test = "a_backup_index_row_keeps_names_that_hold_u0000_and_u0001";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(test, Options::default()).await.expect("a scratch");
    let table = "nul_source";
    let backup_id = extenddb_storage::util::new_backup_id();
    let arn = extenddb_storage::util::backup_arn(REGION, ACCOUNT, table, &backup_id);
    let store = StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: "/srv/extenddb-backups".to_owned(),
        store_id: "store-1".to_owned(),
    };
    let row = BackupRow {
        backup_id: backup_id.clone(),
        account_id: ACCOUNT.to_owned(),
        table_name: table.to_owned(),
        backup_arn: arn.clone(),
        backup_name: "nightly".to_owned(),
        status: BackupStatus::Creating,
        store: store.clone(),
        size_bytes: 0,
        item_count: 0,
        created_at: extenddb_storage::util::backup_creation_date_time(&backup_id).unwrap(),
        completed_at: None,
        failure_reason: None,
        source_table_details_json: source_table(table),
        owner_generation: 0,
        heartbeat_at: 0.0,
    };
    let names = [PK0, SK1, SK11, GK1, N0, NK, ZK];
    let parsed = BackupArn::parse(&arn).expect("a backup ARN");

    // The claim inserts the row as the backup job does.
    let claim = s.engine.claim_backup(&row).await.expect("claim the row");
    assert_stored_escaped(
        &stored_backup_index(&s, table).await,
        &names,
        "the CREATING row",
    );
    let read = s
        .engine
        .read_backup_row(&parsed)
        .await
        .expect("read the row")
        .expect("the row exists");
    assert_eq!(
        read.source_table_details_json,
        row.source_table_details_json
    );

    // The AVAILABLE write replaces the document, as the job's last write
    // does with the manifest's source table.
    let mut available = source_table(table);
    available["item_count"] = json!(3);
    let written = claim
        .set_status(
            JobStatus::Creating,
            JobStatus::Available,
            &StatusFields {
                item_count: Some(3),
                size_bytes: Some(1234),
                source_table_details_json: Some(available.clone()),
                completed: true,
                ..StatusFields::default()
            },
        )
        .await
        .expect("the AVAILABLE write");
    assert_eq!(written, WriteOutcome::Matched);
    drop(claim);
    assert_stored_escaped(
        &stored_backup_index(&s, table).await,
        &names,
        "the AVAILABLE row",
    );
    let read = s
        .engine
        .read_backup_row(&parsed)
        .await
        .expect("read the row")
        .expect("the row exists");
    assert_eq!(read.source_table_details_json, available);
    let schema = read.source_table().expect("the source table");
    let key_names: Vec<&str> = schema
        .key_schema
        .iter()
        .map(|k| k.attribute_name.as_str())
        .collect();
    assert_eq!(key_names, [PK0, SK11]);
    let index = &schema.global_secondary_indexes[0];
    assert_eq!(index.key_schema[0].attribute_name, GK1);
    assert_eq!(
        index.projection.non_key_attributes.as_deref(),
        Some(&[N0.to_owned(), NK.to_owned()][..])
    );
    let listed: Vec<BackupRow> = s
        .engine
        .list_backup_rows(Some(BackupStatus::Available), &store)
        .map(|r| r.expect("a listed row"))
        .collect()
        .await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].source_table_details_json, available);
}
