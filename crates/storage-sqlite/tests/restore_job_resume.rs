// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A store-backed restore on SQLite whose owner died mid-file is resumed by
//! one reconciler cycle in the same process, from the record's
//! `(file_index, items_applied)`, and ends ACTIVE with the backup's item
//! count (design 4(e) and 5.3 step 5 on this backend: a RUNNING `manifest`
//! record whose in-process slot no task holds is dead at once, and the
//! progress on the record is where the resume starts).
//!
//! The engine is built the way the server builds it (the backend's server
//! components factory, its workers spawned through the runtime hooks), the
//! store is a real filesystem store, and the backup is taken through the
//! engine's `dispatch` with data files small enough to make several. The
//! dead owner is played at the storage level: `claim_restore`, `bulk_load`
//! of the first file and of half of the second, the claim dropped (its slot
//! freed) and the record's heartbeat aged as the server's restart ages it.
//! The cycle takes the record over, the resume loads the rest, and the table
//! reaches ACTIVE with every item once; a second cycle finds nothing.

use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use extenddb_backup_store::FilesystemStore;
use extenddb_core::backup_format::{
    BackupArn, EXTENDDB_MANIFEST_FILE, ExtenddbManifest, data_file_key,
};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::Item;
use extenddb_engine::reconciler::{self, ReconcilerState};
use extenddb_engine::{
    BackupRuntime, ConfiguredStore, JobSettings, OperationContext, check_configured_store, dispatch,
};
use extenddb_storage::backup_catalog::{
    JobStatus, RestoreKind, RestoreRegistrationRow, RestoreTargetKey, StoreKind, UNOWNED_GENERATION,
};
use extenddb_storage::restore_engine::BulkLoadFile;
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage::{
    CancellationToken, RestoreTableOverrides, WorkerContext,
    backup_definition::restore_input_from_manifest,
};
use extenddb_storage_sqlite::SqliteConfig;
use serde_json::json;

const REGION: &str = "us-east-1";
const ITEMS: usize = 300;

/// A scratch directory under the cargo target directory, removed on drop.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn call(
    ctx: &OperationContext,
    operation: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    dispatch(operation, body, ctx, "127.0.0.1:0")
        .await
        .unwrap_or_else(|e| panic!("{operation}: {e:?}"))
        .body
}

async fn wait_table_status(ctx: &OperationContext, table: &str, status: &str) -> serde_json::Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let desc = call(ctx, "DescribeTable", json!({"TableName": table})).await;
        if desc["Table"]["TableStatus"] == status {
            return desc;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{table} did not reach {status}: {desc}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn scan_count(ctx: &OperationContext, table: &str, index: Option<&str>) -> usize {
    let mut count = 0;
    let mut start: Option<serde_json::Value> = None;
    loop {
        let mut body = json!({"TableName": table, "Select": "COUNT"});
        if let Some(index) = index {
            body["IndexName"] = json!(index);
        }
        if let Some(key) = &start {
            body["ExclusiveStartKey"] = key.clone();
        }
        let page = call(ctx, "Scan", body).await;
        count += usize::try_from(page["Count"].as_u64().expect("Count")).unwrap();
        match page.get("LastEvaluatedKey") {
            Some(key) if !key.is_null() => start = Some(key.clone()),
            _ => return count,
        }
    }
}

/// The items of data file `index` of the backup under `root`, decoded as the
/// engine's load decodes them.
fn file_items(
    root: &std::path::Path,
    arn: &BackupArn,
    manifest: &ExtenddbManifest,
    index: usize,
) -> Vec<Item> {
    let entry = &manifest.data_files[index];
    // Data file keys count from one; `BulkLoadFile::file_index` from zero.
    assert_eq!(entry.key, data_file_key(u32::try_from(index + 1).unwrap()));
    let compressed = std::fs::File::open(root.join(arn.store_prefix()).join(&entry.key))
        .expect("the data file in the store");
    let mut reader = manifest
        .reader_prehashed(entry, compressed)
        .expect("a reader");
    let mut items = Vec::new();
    while let Some(item) = reader.next_item().expect("an item") {
        items.push(item);
    }
    assert_eq!(items.len() as u64, entry.item_count);
    items
}

fn stream(
    items: Vec<Item>,
) -> futures::stream::BoxStream<'static, Result<Item, extenddb_storage::error::StorageError>> {
    Box::pin(futures::stream::iter(items.into_iter().map(Ok)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_restore_is_resumed_by_one_cycle_from_its_recorded_progress() {
    let scratch = Scratch(
        PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("restore-job-resume-{}", uuid::Uuid::new_v4())),
    );
    std::fs::create_dir_all(&scratch.0).expect("scratch directory");
    let db_path = scratch.0.join("catalog.sqlite");
    let root = scratch.0.join("backups");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .expect("root");

    // The engine as the server builds it.
    let config = SqliteConfig {
        path: db_path.to_str().expect("UTF-8 path").to_owned(),
        pool_size: 4,
    };
    let components = (extenddb_storage_sqlite::backend().server_components)(&config, REGION, {
        let mut options = ServerComponentsOptions::default();
        options.bootstrap_if_uninitialized = true;
        options.release = "test";
        options.max_concurrent_jobs = 4;
        options
    })
    .await
    .unwrap_or_else(|e| panic!("server components: {e:?}"));
    let storage = components.engine;
    let shutdown = CancellationToken::new();
    let (_layer, reload_handle) =
        tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new("info"));
    let worker_ctx = WorkerContext {
        metrics: Arc::new(extenddb_core::metrics::MetricsCollector::new()),
        catalog_store: components.catalog_store.clone(),
        reload_handle,
        config_log_level: "info".to_owned(),
        shutdown: shutdown.clone(),
    };
    let hooks = components
        .runtime_hooks
        .expect("the SQLite backend has runtime hooks");
    let workers = hooks.spawn_workers(&worker_ctx).await;
    let catalog = sqlx::SqlitePool::connect(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("a second connection to the catalog");
    for (key, value) in [
        ("control_plane_delay_seconds", "0.05"),
        ("index_propagation_delay_ms", "0"),
    ] {
        sqlx::query("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)")
            .bind(key)
            .bind(value)
            .execute(&catalog)
            .await
            .expect("setting");
    }
    let account_id: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'default_account_id'")
            .fetch_one(&catalog)
            .await
            .expect("the bootstrapped account");

    // A healthy filesystem store, its identity file written by the check
    // the server runs at startup; data files of about 4 KB so the backup
    // has several.
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
    let outcome = check_configured_store(&configured, &*storage, "sqlite").await;
    assert!(outcome.is_ok(), "the store's first check: {outcome:?}");
    configured.record_check(outcome);
    let stale_job_after = Duration::from_secs(300);
    let runtime = Arc::new(
        BackupRuntime::new(4)
            .with_stale_job_after(stale_job_after)
            .with_store(configured)
            .with_source("sqlite", "test")
            .with_job_settings(JobSettings {
                data_file_target_bytes: 4096,
                ..JobSettings::default()
            })
            .with_limits(Arc::new(LimitsConfig::default()))
            .with_shutdown(shutdown.child_token()),
    );
    let ctx = OperationContext {
        storage: storage.clone(),
        backup: runtime.clone(),
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from(REGION),
        account_id: Arc::from(account_id.as_str()),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(Vec::new()),
        authorized_key_info: Default::default(),
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
    };

    call(
        &ctx,
        "CreateTable",
        json!({
            "TableName": "Music",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"},
                {"AttributeName": "g", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "gix",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }]
        }),
    )
    .await;
    wait_table_status(&ctx, "Music", "ACTIVE").await;
    for i in 0..ITEMS {
        let mut item = json!({
            "pk": {"S": format!("p{}", i % 7)},
            "sk": {"N": i.to_string()},
            "note": {"S": format!("item {i} of the fixture, padded to make the files small {}", "x".repeat(40))}
        });
        if i % 2 == 0 {
            item["g"] = json!({"S": format!("g{}", i % 4)});
        }
        call(&ctx, "PutItem", json!({"TableName": "Music", "Item": item})).await;
    }

    // The backup, to the store, AVAILABLE.
    let details = call(
        &ctx,
        "CreateBackup",
        json!({"TableName": "Music", "BackupName": "nightly"}),
    )
    .await;
    let arn_text = details["BackupDetails"]["BackupArn"]
        .as_str()
        .expect("an ARN")
        .to_owned();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let described = call(&ctx, "DescribeBackup", json!({"BackupArn": arn_text})).await;
        if described["BackupDescription"]["BackupDetails"]["BackupStatus"] == "AVAILABLE" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the backup did not become AVAILABLE: {described}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    while runtime.running_jobs() > 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let arn = BackupArn::parse(&arn_text).expect("a backup ARN");
    let row = storage
        .read_backup_row(&arn)
        .await
        .expect("read the row")
        .expect("the row exists");
    let manifest_bytes = std::fs::read(root.join(arn.store_prefix()).join(EXTENDDB_MANIFEST_FILE))
        .expect("manifest");
    let manifest = ExtenddbManifest::from_json_bytes(&manifest_bytes).expect("a manifest");
    assert!(
        manifest.data_files.len() >= 3,
        "the backup has several data files: {}",
        manifest.data_files.len()
    );
    let total: u64 = manifest.data_files.iter().map(|f| f.item_count).sum();
    assert_eq!(total as usize, ITEMS);

    // The dead owner: the registration, file 0 whole, half of file 1, then
    // gone (the claim dropped frees the slot; the heartbeat aged as the
    // server's restart ages every RUNNING record).
    let input = restore_input_from_manifest(
        &manifest.source_table,
        "Restored",
        &RestoreTableOverrides::default(),
    );
    let registration = RestoreRegistrationRow {
        backup: row.key(),
        source_table_arn: manifest.source_table.table_arn.clone(),
        restore_date_time: row.created_at,
        store: row.store.clone(),
        spool_host: None,
        owner_generation: UNOWNED_GENERATION,
    };
    let (desc, claim) = storage
        .claim_restore(&account_id, input, &registration)
        .await
        .expect("register the restore");
    let key = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    let file0 = file_items(&root, &arn, &manifest, 0);
    let file1 = file_items(&root, &arn, &manifest, 1);
    let half = file1.len() / 2;
    assert!(half > 0);
    storage
        .bulk_load(
            &key,
            claim.as_ref(),
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            stream(file0.clone()),
        )
        .await
        .expect("file 0");
    storage
        .bulk_load(
            &key,
            claim.as_ref(),
            BulkLoadFile {
                file_index: 1,
                start_at: 0,
                reapplying: false,
            },
            stream(file1[..half].to_vec()),
        )
        .await
        .expect("half of file 1");
    let dead_generation = claim.generation();
    drop(claim);
    let before = storage
        .read_restore_job(&key)
        .await
        .expect("read")
        .expect("the record");
    assert_eq!(before.status, JobStatus::Running);
    assert_eq!(before.kind, RestoreKind::Manifest);
    assert_eq!(
        (before.file_index, before.items_applied),
        (Some(1), Some(half as u64))
    );
    assert_eq!(
        storage.count_restore_target(&key).await.expect("count") as usize,
        file0.len() + half
    );
    sqlx::query(
        "UPDATE table_restores SET heartbeat_at = '1970-01-01T00:00:00.000Z' WHERE table_id = ?",
    )
    .bind(&key.table_id)
    .execute(&catalog)
    .await
    .expect("age the heartbeat as a restart does");
    let desc_before = call(&ctx, "DescribeTable", json!({"TableName": "Restored"})).await;
    assert_eq!(desc_before["Table"]["TableStatus"], "CREATING");
    assert!(
        desc_before["Table"]["RestoreSummary"]["RestoreInProgress"]
            .as_bool()
            .unwrap_or(false),
        "{desc_before}"
    );

    // One cycle: the record's slot no task holds makes it dead at once; the
    // resume is spawned through the runtime.
    let mut state = ReconcilerState::with_shutdown(shutdown.child_token());
    let report = reconciler::cycle(&storage, &runtime, stale_job_after, "sqlite", &mut state)
        .await
        .expect("a reconciler cycle");
    assert_eq!(report.store_healthy, Some(true), "{report:?}");
    assert_eq!(
        report.manifest_restores_resumed,
        vec!["Restored".to_owned()],
        "{report:?}"
    );
    assert!(report.manifest_restores_failed.is_empty(), "{report:?}");
    assert!(report.legacy_restores_removed.is_empty(), "{report:?}");
    let taken = storage
        .read_restore_job(&key)
        .await
        .expect("read")
        .expect("the record");
    assert_ne!(
        taken.owner_generation, dead_generation,
        "the takeover minted a generation"
    );
    assert_eq!(taken.spool_host, None, "a filesystem store has no spool");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while runtime.running_jobs() > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the resumed restore did not end"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The table ACTIVE with every item once; the record DONE at the last
    // file's count; the GSI answering.
    let desc_after = wait_table_status(&ctx, "Restored", "ACTIVE").await;
    assert_eq!(
        desc_after["Table"]["ItemCount"],
        json!(0),
        "the restored table's ItemCount is 0 at ACTIVE, as the service reports it"
    );
    assert!(
        desc_after["Table"].get("RestoreSummary").is_none(),
        "{desc_after}"
    );
    assert_eq!(scan_count(&ctx, "Restored", None).await, ITEMS);
    assert_eq!(scan_count(&ctx, "Restored", Some("gix")).await, ITEMS / 2);
    assert_eq!(scan_count(&ctx, "Music", None).await, ITEMS);
    let done = storage
        .read_restore_job(&key)
        .await
        .expect("read")
        .expect("the record");
    assert_eq!(done.status, JobStatus::Done);
    assert!(done.completed_at.is_some());
    let last = manifest.data_files.len() - 1;
    assert_eq!(
        (done.file_index, done.items_applied),
        (
            Some(u32::try_from(last).unwrap()),
            Some(manifest.data_files[last].item_count)
        ),
        "the progress names the last file complete"
    );
    assert_eq!(done.owner_generation, taken.owner_generation);
    assert_eq!(
        storage.backup_in_use(&row.key()).await.expect("in use"),
        None
    );

    // A second cycle has nothing left to resume.
    let report = reconciler::cycle(&storage, &runtime, stale_job_after, "sqlite", &mut state)
        .await
        .expect("a second cycle");
    assert!(report.manifest_restores_resumed.is_empty(), "{report:?}");
    assert!(report.manifest_restores_failed.is_empty(), "{report:?}");

    catalog.close().await;
    shutdown.cancel();
    for worker in workers {
        let _ = tokio::time::timeout(Duration::from_secs(5), worker).await;
    }
}
