// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Modes of the SQLite database file and its `-wal` and `-shm` files after
//! `init`, `serve`, a first write, a clean stop and a second `serve`. The
//! database holds the encryption key beside the secrets it protects, so the
//! files must be readable by their owner only, whatever the umask.
//!
//! SQLite creates a database file at 0644 less the umask, and each sidecar
//! with the mode of the database file. These tests run in their own process
//! and set a umask of 022, under which that default comes out 0644. The first
//! test shows that a connection opened outside the crate's openers does leave
//! 0644 files under this umask, so the other tests measure the openers.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use extenddb_storage::bootstrapper::Bootstrapper;
use extenddb_storage::server_components::{ServerComponents, ServerComponentsOptions};
use extenddb_storage::{MetadataEngine, TableEngine};
use extenddb_storage_sqlite::{SqliteBootstrapper, SqliteConfig};

const ACCOUNT: &str = "000000000000";

/// A fresh directory for one test, with the process umask set to 022.
fn deployment_dir() -> PathBuf {
    // SAFETY: umask reads and replaces the process file-mode mask and has no
    // other effect; every test in this binary sets the same value.
    unsafe { libc::umask(0o022) };
    let dir = std::env::temp_dir().join(format!(
        "extenddb-file-modes-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

fn database_path(dir: &Path) -> String {
    dir.join("x.sqlite").to_string_lossy().into_owned()
}

/// Name and mode of every file directly in `dir`. A file that a closing
/// connection removes between the listing and the stat is skipped.
fn modes(dir: &Path) -> BTreeMap<String, u32> {
    std::fs::read_dir(dir)
        .expect("read_dir")
        .filter_map(|entry| {
            let entry = entry.expect("entry");
            let meta = match std::fs::metadata(entry.path()) {
                Ok(meta) => meta,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
                Err(e) => panic!("{}: {e}", entry.path().display()),
            };
            Some((
                entry.file_name().to_string_lossy().into_owned(),
                meta.permissions().mode() & 0o777,
            ))
        })
        .collect()
}

/// Every file in `dir` is 0600; `expected` names files that must be there.
fn assert_owner_only(dir: &Path, step: &str, expected: &[&str]) {
    let modes = modes(dir);
    assert!(
        modes.contains_key("x.sqlite"),
        "after {step}: no database file in {modes:?}"
    );
    for name in expected {
        assert!(
            modes.contains_key(*name),
            "after {step}: {name} missing from {modes:?}"
        );
    }
    for (name, mode) in &modes {
        assert_eq!(
            *mode, 0o600,
            "after {step}: {name} is {mode:o} in {modes:?}"
        );
    }
}

fn set_every_mode(dir: &Path, mode: u32) {
    for name in modes(dir).keys() {
        std::fs::set_permissions(dir.join(name), std::fs::Permissions::from_mode(mode))
            .expect("chmod");
    }
}

/// What `extenddb init --backend sqlite` does through the bootstrapper.
async fn init(path: &str) {
    let bootstrapper = SqliteBootstrapper::new(path);
    bootstrapper
        .ensure_app_user()
        .await
        .expect("ensure_app_user");
    bootstrapper
        .grant_app_role_to_admin()
        .await
        .expect("grant_app_role_to_admin");
    bootstrapper
        .create_catalog_db()
        .await
        .expect("create_catalog_db");
    bootstrapper.create_data_db().await.expect("create_data_db");
    bootstrapper
        .acquire_migration_lock()
        .await
        .expect("acquire_migration_lock");
    assert!(
        !bootstrapper
            .is_catalog_initialized()
            .await
            .expect("is_catalog_initialized")
    );
    bootstrapper
        .run_catalog_migrations()
        .await
        .expect("run_catalog_migrations");
    bootstrapper
        .record_data_connection()
        .await
        .expect("record_data_connection");
    bootstrapper
        .run_data_migrations()
        .await
        .expect("run_data_migrations");
    bootstrapper
        .bootstrap_encryption_key()
        .await
        .expect("bootstrap_encryption_key");
    bootstrapper
        .bootstrap_default_account()
        .await
        .expect("bootstrap_default_account");
    bootstrapper
        .bootstrap_admin_user(None, None)
        .await
        .expect("bootstrap_admin_user");
    bootstrapper
        .release_migration_lock()
        .await
        .expect("release_migration_lock");
}

/// What `extenddb migrate` opens on a database with nothing pending: the lock,
/// then the bootstrapper's pool for the catalog state and the pending data
/// migrations.
async fn migrate_reads(path: &str) {
    let bootstrapper = SqliteBootstrapper::new(path);
    bootstrapper
        .acquire_migration_lock()
        .await
        .expect("acquire_migration_lock");
    assert!(
        bootstrapper
            .is_catalog_initialized()
            .await
            .expect("is_catalog_initialized")
    );
    bootstrapper
        .pending_data_migrations()
        .await
        .expect("pending_data_migrations");
    bootstrapper
        .release_migration_lock()
        .await
        .expect("release_migration_lock");
}

/// What `extenddb serve` does to open the database.
async fn serve(path: &str, options: ServerComponentsOptions) -> ServerComponents {
    let config = SqliteConfig {
        path: path.to_owned(),
        pool_size: 2,
    };
    (extenddb_storage_sqlite::backend().server_components)(&config, "us-east-1", options)
        .await
        .expect("server components")
}

/// A first write: an account through the management store (the `manage`
/// command's opener), then CreateTable and TagResource through the engine.
async fn first_write(path: &str, components: &ServerComponents) {
    let management = (extenddb_storage_sqlite::backend().management_store)(path)
        .await
        .expect("management store");
    management
        .create_account(ACCOUNT, "modes")
        .await
        .expect("create account");
    let input: extenddb_core::types::CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": "modes",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input");
    let engine = &*components.engine;
    TableEngine::create_table(engine, ACCOUNT, input)
        .await
        .expect("create table");
    MetadataEngine::tag_resource(
        engine,
        &format!("arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/modes"),
        &[extenddb_core::types::Tag {
            key: "k".to_owned(),
            value: "v".to_owned(),
        }],
    )
    .await
    .expect("tag resource");
}

/// The control for the other tests: a connection that none of the crate's
/// openers made leaves the database and its sidecars at 0644 under this
/// umask, so a 0600 result below comes from the openers, not the umask.
#[tokio::test]
async fn a_connection_outside_the_openers_leaves_0644_files() {
    use sqlx::Executor;
    let dir = deployment_dir();
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                conn.execute("PRAGMA journal_mode = WAL").await?;
                Ok(())
            })
        })
        .connect(&format!("sqlite://{}?mode=rwc", database_path(&dir)))
        .await
        .expect("connect");
    sqlx::query("CREATE TABLE t (k INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("write");
    let modes = modes(&dir);
    for name in ["x.sqlite", "x.sqlite-wal", "x.sqlite-shm"] {
        assert_eq!(modes.get(name), Some(&0o644), "{name} in {modes:?}");
    }
    pool.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// `init`, `serve`, a first write, a clean stop and a second `serve` leave
/// every file owner-only, with the WAL sidecars present while a server is up.
#[tokio::test]
async fn init_serve_write_stop_and_restart_leave_every_file_owner_only() {
    let dir = deployment_dir();
    let path = database_path(&dir);
    let sidecars = ["x.sqlite-wal", "x.sqlite-shm"];

    init(&path).await;
    assert_owner_only(&dir, "init", &["x.sqlite.lock"]);

    let components = serve(&path, ServerComponentsOptions::default()).await;
    assert_owner_only(&dir, "serve", &sidecars);

    first_write(&path, &components).await;
    assert_owner_only(&dir, "the first write", &sidecars);

    drop(components);
    assert_owner_only(&dir, "a clean stop", &[]);

    let again = serve(&path, ServerComponentsOptions::default()).await;
    assert_owner_only(&dir, "a second serve", &sidecars);
    drop(again);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A dev-mode `serve` that bootstraps a database no `init` created makes it
/// owner-only too.
#[tokio::test]
async fn a_bootstrapping_serve_creates_owner_only_files() {
    let dir = deployment_dir();
    let path = database_path(&dir);
    let mut options = ServerComponentsOptions::default();
    options.bootstrap_if_uninitialized = true;
    let components = serve(&path, options).await;
    assert_owner_only(
        &dir,
        "a bootstrapping serve",
        &["x.sqlite-wal", "x.sqlite-shm"],
    );
    drop(components);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Files left at 0644 by an earlier version are set to 0600 by the next
/// `serve`, and by the next `migrate`, before anything reads them.
#[tokio::test]
async fn serve_and_migrate_restrict_files_left_at_0644() {
    let dir = deployment_dir();
    let path = database_path(&dir);
    init(&path).await;

    set_every_mode(&dir, 0o644);
    let components = serve(&path, ServerComponentsOptions::default()).await;
    assert_owner_only(
        &dir,
        "serve over 0644 files",
        &["x.sqlite-wal", "x.sqlite-shm"],
    );
    drop(components);

    set_every_mode(&dir, 0o644);
    migrate_reads(&path).await;
    assert_owner_only(&dir, "migrate over 0644 files", &[]);
    let _ = std::fs::remove_dir_all(&dir);
}
