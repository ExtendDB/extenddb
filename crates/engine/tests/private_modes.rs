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
//! the inherited umask.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use extenddb_core::limits::LimitsConfig;
use extenddb_engine::private_fs::create_private_dir_all;
use extenddb_engine::{
    AuthorizedKeyInfo, BackupRuntime, OperationContext, handle_create_table, handle_export_table,
};
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage_sqlite::SqliteConfig;
use serde_json::json;

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
/// missing ancestors with the default mode, and leaves the existing parent
/// alone.
fn missing_directories() {
    let base = scratch("missing");
    let leaf = base.join("a").join("b").join("c");

    create_private_dir_all(&leaf).unwrap();

    for dir in [base.join("a"), base.join("a").join("b")] {
        assert_eq!(
            mode_of(&dir),
            0o777,
            "a missing ancestor, {}",
            dir.display()
        );
    }
    assert_eq!(mode_of(&leaf), 0o700);
    assert_eq!(mode_of(&base), 0o755, "the existing parent keeps its mode");
    std::fs::remove_dir_all(&base).unwrap();
}

/// An existing directory keeps its mode, whether it is the one asked for or
/// an ancestor of it, and a second call changes nothing.
fn existing_directories() {
    let base = scratch("existing");
    let shared = base.join("shared");
    std::fs::create_dir(&shared).unwrap();
    set_mode(&shared, 0o775);

    create_private_dir_all(&shared).unwrap();
    assert_eq!(
        mode_of(&shared),
        0o775,
        "an existing directory keeps its mode"
    );

    let child = shared.join("child");
    create_private_dir_all(&child).unwrap();
    assert_eq!(mode_of(&shared), 0o775, "an existing parent keeps its mode");
    assert_eq!(mode_of(&child), 0o700);

    create_private_dir_all(&child).unwrap();
    assert_eq!(mode_of(&child), 0o700, "a second call changes nothing");
    std::fs::remove_dir_all(&base).unwrap();
}

/// An `ExportTableToPointInTime` on a file-backed `SqliteEngine` creates
/// the account directory 0700 and the export file 0600, and leaves the root
/// alone.
fn export_through_the_handler() {
    let base = scratch("export");
    let root = base.join("exports");
    std::fs::create_dir(&root).unwrap();
    set_mode(&root, 0o755);

    let (account_dir, file) = block_on(async {
        let ctx = context(&base, &root).await;
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

    assert_eq!(mode_of(&account_dir), 0o700, "the account directory");
    assert_eq!(mode_of(&file), 0o600, "the export file");
    assert_eq!(mode_of(&root), 0o755, "the configured root keeps its mode");
    std::fs::remove_dir_all(&base).unwrap();
}

/// Control: the standard calls follow the umask, so the cases above would
/// have seen `0777` and `0666` had the code not set the modes.
fn standard_calls_follow_the_umask() {
    let base = scratch("control");
    let dir = base.join("plain");
    let file = base.join("plain.json");

    std::fs::create_dir_all(&dir).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&file)
        .unwrap();

    assert_eq!(mode_of(&dir), 0o777);
    assert_eq!(mode_of(&file), 0o666);
    std::fs::remove_dir_all(&base).unwrap();
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

/// A fresh `0755` directory under the temp dir.
fn scratch(name: &str) -> PathBuf {
    let base = std::env::temp_dir().join(format!(
        "extenddb-private-modes-{name}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&base).unwrap();
    set_mode(&base, 0o755);
    base
}

fn set_mode(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn mode_of(path: &Path) -> u32 {
    std::fs::symlink_metadata(path)
        .unwrap()
        .permissions()
        .mode()
        & 0o777
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
