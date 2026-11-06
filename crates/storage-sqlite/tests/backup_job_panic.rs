// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A CreateBackup job whose task panics on SQLite leaves a CREATING row that
//! the reconciler's next cycle resolves to FAILED, in the same process, with
//! no restart (design 4(e): at runtime a row whose in-process slot no task
//! holds is dead; PR 8's acceptance).
//!
//! The engine is built the way the server builds it (the backend's server
//! components factory, its workers spawned through the runtime hooks, so a
//! table reaches ACTIVE as it does under `serve`), the store is a filesystem
//! store whose `put` panics, and the request goes through the engine's
//! `dispatch`. The job's task panics at its first put; the task's drop
//! releases the `Arc<dyn JobClaim>` and with it the row's `JobSlot`, so the
//! SQLite catalog's `claim_dead_backup` takes the row over at once, whatever
//! its heartbeat says, and the reconciler writes FAILED (no manifest) and
//! then, since the dead owner's heartbeat is within the window, leaves the
//! prefix for a later cycle.

use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use extenddb_backup_store::{BackupStore, ByteStream, FilesystemStore, ObjectMeta, StoreError};
use extenddb_core::backup_format::BackupArn;
use extenddb_core::limits::LimitsConfig;
use extenddb_engine::reconciler::{self, OWNER_DEAD_NO_MANIFEST, ReconcilerState};
use extenddb_engine::{
    BackupRuntime, ConfiguredStore, OperationContext, check_configured_store, dispatch,
};
use extenddb_storage::backup_catalog::{BackupStatus, StoreKind};
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage::{CancellationToken, WorkerContext};
use extenddb_storage_sqlite::SqliteConfig;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use serde_json::json;

const REGION: &str = "us-east-1";

/// A filesystem store whose `put` panics: every other call reaches the real
/// store, so the identity check, the listing and the delete behave as on a
/// server; only the job's first object write dies.
struct PanicOnPut {
    inner: FilesystemStore,
    puts: AtomicUsize,
}

impl BackupStore for PanicOnPut {
    fn put<'a>(
        &'a self,
        key: &'a str,
        _body: ByteStream,
    ) -> BoxFuture<'a, Result<ObjectMeta, StoreError>> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { panic!("injected panic in put of {key}") })
    }

    fn put_if_absent<'a>(
        &'a self,
        key: &'a str,
        body: ByteStream,
    ) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
        self.inner.put_if_absent(key, body)
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<ByteStream, StoreError>> {
        self.inner.get(key)
    }

    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
        self.inner.head(key)
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxStream<'a, Result<ObjectMeta, StoreError>> {
        self.inner.list(prefix)
    }

    fn delete_prefix<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<u64, StoreError>> {
        self.inner.delete_prefix(prefix)
    }

    fn validate(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner.validate()
    }
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_task_that_panics_leaves_a_row_the_next_cycle_resolves_without_a_restart() {
    let scratch = Scratch(
        PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("backup-job-panic-{}", uuid::Uuid::new_v4())),
    );
    std::fs::create_dir_all(&scratch.0).expect("scratch directory");
    let db_path = scratch.0.join("catalog.sqlite");
    let root = scratch.0.join("backups");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .expect("root");

    // The engine as the server builds it: a dev bootstrap of a fresh file
    // (the migrations, the default account), the workers spawned.
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
    let account_id: String = {
        let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", db_path.display()))
            .await
            .expect("a second connection to the catalog");
        sqlx::query("INSERT OR REPLACE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0.05')")
            .execute(&pool)
            .await
            .expect("setting");
        let id = sqlx::query_scalar("SELECT value FROM settings WHERE key = 'default_account_id'")
            .fetch_one(&pool)
            .await
            .expect("the bootstrapped account");
        pool.close().await;
        id
    };

    // The store: a healthy filesystem store (its identity file created by
    // the check the server runs at startup) whose `put` panics.
    let store = Arc::new(PanicOnPut {
        inner: FilesystemStore::open(&root)
            .await
            .expect("filesystem store"),
        puts: AtomicUsize::new(0),
    });
    let configured = ConfiguredStore::checking(
        StoreKind::Filesystem,
        root.to_str().expect("UTF-8 path").to_owned(),
        store.clone(),
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
        item_authorizer: None,
    };

    call(
        &ctx,
        "CreateTable",
        json!({
            "TableName": "Music",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    )
    .await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let desc = call(&ctx, "DescribeTable", json!({"TableName": "Music"})).await;
        if desc["Table"]["TableStatus"] == "ACTIVE" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Music did not become ACTIVE: {desc}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    call(
        &ctx,
        "PutItem",
        json!({"TableName": "Music", "Item": {"pk": {"S": "a"}, "n": {"N": "1"}}}),
    )
    .await;

    // The request answers CREATING; the job's task then panics at its first
    // put and ends.
    let details = call(
        &ctx,
        "CreateBackup",
        json!({"TableName": "Music", "BackupName": "nightly"}),
    )
    .await;
    assert_eq!(
        details["BackupDetails"]["BackupStatus"], "CREATING",
        "{details}"
    );
    let arn_text = details["BackupDetails"]["BackupArn"]
        .as_str()
        .expect("an ARN")
        .to_owned();
    let arn = BackupArn::parse(&arn_text).expect("a backup ARN");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while runtime.running_jobs() > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the job task did not end"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        store.puts.load(Ordering::SeqCst),
        1,
        "the task died at its first put"
    );
    let row = storage
        .read_backup_row(&arn)
        .await
        .expect("read the row")
        .expect("the row exists");
    assert_eq!(
        row.status,
        BackupStatus::Creating,
        "the panicking task wrote nothing"
    );
    let heartbeat_before = row.heartbeat_at;
    assert_eq!(
        runtime.free_slots(),
        4,
        "the task's drop returned the job permit"
    );

    // One reconciler cycle in the same process: the slot no task holds makes
    // the row dead at once, and without a manifest it is FAILED.
    let mut state = ReconcilerState::with_shutdown(shutdown.child_token());
    let report = reconciler::cycle(&*storage, &runtime, stale_job_after, "sqlite", &mut state)
        .await
        .expect("a reconciler cycle");
    assert_eq!(report.store_healthy, Some(true), "{report:?}");
    assert_eq!(
        report.creating_resolved,
        vec![(arn_text.clone(), BackupStatus::Failed)],
        "{report:?}"
    );
    let row = storage
        .read_backup_row(&arn)
        .await
        .expect("read the row")
        .expect("the row exists");
    assert_eq!(row.status, BackupStatus::Failed);
    assert_eq!(row.failure_reason.as_deref(), Some(OWNER_DEAD_NO_MANIFEST));
    assert!(
        row.completed_at.is_some(),
        "the FAILED write stamps completed_at"
    );
    assert_eq!(
        row.heartbeat_at, heartbeat_before,
        "the takeover left the heartbeat"
    );
    // The dead owner's heartbeat is within the window, so the prefix (empty
    // here: the put never landed) is the FAILED-prefix duty's once it is
    // stale, not this cycle's.
    assert!(report.failed_prefixes_deleted.is_empty(), "{report:?}");
    let described = dispatch(
        "DescribeBackup",
        json!({"BackupArn": arn_text}),
        &ctx,
        "127.0.0.1:0",
    )
    .await;
    match described {
        Err(extenddb_core::error::DynamoDbError::BackupNotFoundException(_)) => {}
        Err(other) => panic!("DescribeBackup of the FAILED row: {other:?}"),
        Ok(answer) => panic!("DescribeBackup of the FAILED row answered {}", answer.body),
    }
    // A second cycle has nothing left to resolve.
    let report = reconciler::cycle(&*storage, &runtime, stale_job_after, "sqlite", &mut state)
        .await
        .expect("a second cycle");
    assert!(report.creating_resolved.is_empty(), "{report:?}");

    shutdown.cancel();
    for worker in workers {
        let _ = tokio::time::timeout(Duration::from_secs(5), worker).await;
    }
}
