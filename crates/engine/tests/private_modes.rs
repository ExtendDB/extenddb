// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The modes of export files and of the directories made for them, under
//! umask 0.
//!
//! The umask is process-wide: a test that changes it changes the mode of
//! everything the other tests of its binary create meanwhile. This binary
//! therefore holds a single test, which sets umask 0 and runs every case in
//! order. Under umask 0 a mode the code does not set shows up as `0777` or
//! `0666`. The unit tests in `src/private_fs.rs` check the same modes under
//! the inherited umask. Scratch directories are `tempfile::TempDir`s, so a
//! failed assertion does not leave them behind.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use extenddb_core::limits::LimitsConfig;
use extenddb_engine::private_fs::create_private_dir_all;
use extenddb_engine::{
    AuthorizedKeyInfo, BackupRuntime, OperationContext, handle_create_table, handle_export_table,
};
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage_sqlite::SqliteConfig;
use serde_json::json;
use tempfile::TempDir;

const REGION: &str = "us-east-1";

#[test]
fn owner_only_modes_hold_under_a_umask_of_zero() {
    // SAFETY: umask(2) has no preconditions and cannot fail. This is the only
    // test in the binary, so nothing else creates files under this umask.
    unsafe {
        libc::umask(0);
    }

    missing_directories();
    existing_directories();
    export_through_the_handler();
    standard_calls_follow_the_umask();
}

/// `create_private_dir_all` creates the directory asked for 0700 and its
/// missing ancestors 0755, never writable by group or others, and leaves the
/// existing parent alone.
fn missing_directories() {
    let dir = scratch("missing");
    let base = dir.path();
    let leaf = base.join("a").join("b").join("c");

    create_private_dir_all(&leaf).unwrap();

    for dir in [base.join("a"), base.join("a").join("b")] {
        assert_eq!(
            mode_of(&dir),
            Mode(0o755),
            "a missing ancestor, {}",
            dir.display()
        );
    }
    assert_eq!(mode_of(&leaf), Mode(0o700));
    assert_eq!(
        mode_of(base),
        Mode(0o755),
        "the existing parent keeps its mode"
    );
}

/// An existing directory keeps its mode, whether it is the one asked for or
/// an ancestor of it, and a second call changes nothing.
fn existing_directories() {
    let dir = scratch("existing");
    let shared = dir.path().join("shared");
    std::fs::create_dir(&shared).unwrap();
    set_mode(&shared, 0o775);

    create_private_dir_all(&shared).unwrap();
    assert_eq!(
        mode_of(&shared),
        Mode(0o775),
        "an existing directory keeps its mode"
    );

    let child = shared.join("child");
    create_private_dir_all(&child).unwrap();
    assert_eq!(
        mode_of(&shared),
        Mode(0o775),
        "an existing parent keeps its mode"
    );
    assert_eq!(mode_of(&child), Mode(0o700));

    create_private_dir_all(&child).unwrap();
    assert_eq!(
        mode_of(&child),
        Mode(0o700),
        "a second call changes nothing"
    );
}

/// An `ExportTableToPointInTime` on a file-backed `SqliteEngine` creates
/// the account directory 0700 and the export file 0600, and leaves the root
/// alone.
fn export_through_the_handler() {
    let dir = scratch("export");
    let base = dir.path();
    let root = base.join("exports");
    std::fs::create_dir(&root).unwrap();
    set_mode(&root, 0o755);

    let (account_dir, file) = block_on(async {
        let ctx = context(base, &root).await;
        let account_dir = root.join(&*ctx.account_id);
        let file = account_dir.join("modes.json");
        handle_create_table(
            json!({
                "TableName": "modes",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST",
            }),
            &ctx,
        )
        .await
        .unwrap();
        handle_export_table(
            json!({
                "TableArn": format!("arn:aws:dynamodb:{REGION}:{}:table/modes", ctx.account_id),
                "FilePath": file.display().to_string(),
            }),
            &ctx,
        )
        .await
        .unwrap();
        (account_dir, file)
    });

    assert_eq!(mode_of(&account_dir), Mode(0o700), "the account directory");
    assert_eq!(mode_of(&file), Mode(0o600), "the export file");
    assert_eq!(
        mode_of(&root),
        Mode(0o755),
        "the configured root keeps its mode"
    );
}

/// Control: the standard calls follow the umask, so the cases above would
/// have seen `0777` and `0666` had the code not set the modes.
fn standard_calls_follow_the_umask() {
    let base = scratch("control");
    let dir = base.path().join("plain");
    let file = base.path().join("plain.json");

    std::fs::create_dir_all(&dir).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&file)
        .unwrap();

    assert_eq!(mode_of(&dir), Mode(0o777));
    assert_eq!(mode_of(&file), Mode(0o666));
}

/// A context on a file-backed `SqliteEngine` under `base`, in the account
/// the engine bootstrapped, exporting to `root`.
async fn context(base: &Path, root: &Path) -> OperationContext {
    let config = SqliteConfig {
        path: base.join("catalog.sqlite").display().to_string(),
        pool_size: 2,
    };
    let components = (extenddb_storage_sqlite::backend().server_components)(&config, REGION, {
        let mut options = ServerComponentsOptions::default();
        options.bootstrap_if_uninitialized = true;
        options.release = "test";
        options
    })
    .await
    .unwrap_or_else(|e| panic!("server components: {e:?}"));
    let account_id = components
        .catalog_store
        .default_account_id()
        .await
        .unwrap()
        .expect("the bootstrapped account");
    // A table is created ACTIVE, so the export finds it without a worker.
    components
        .catalog_store
        .set_setting("control_plane_delay_seconds", "0")
        .await
        .unwrap();
    OperationContext {
        storage: components.engine,
        backup: Arc::new(BackupRuntime::new(1)),
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from(REGION),
        account_id: Arc::from(account_id),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(vec![Arc::new(root.canonicalize().unwrap())]),
        authorized_key_info: AuthorizedKeyInfo::new(),
        auth_cache: extenddb_auth::AuthCacheRegistry::default(),
        table_key_info_lookup: None,
        item_authorizer: None,
    }
}

/// A fresh `0755` directory under the temp dir, removed when dropped.
///
/// The temp dir is canonicalized first. The export root is passed
/// canonicalized and the export path is checked against it as written, so a
/// temp dir reached through a symlink (`/var` on macOS) would put the export
/// path outside the root.
fn scratch(name: &str) -> TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("extenddb-private-modes-{name}-"))
        .tempdir_in(std::env::temp_dir().canonicalize().unwrap())
        .unwrap();
    set_mode(dir.path(), 0o755);
    dir
}

/// A permission mode that prints in octal when an assertion fails.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Mode(u32);

impl std::fmt::Debug for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04o}", self.0)
    }
}

fn set_mode(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn mode_of(path: &Path) -> Mode {
    Mode(
        std::fs::symlink_metadata(path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
    )
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
