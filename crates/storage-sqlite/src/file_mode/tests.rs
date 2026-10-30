// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for `file_mode`.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::{connect, prepare, restrict};

fn temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "extenddb-file-mode-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .permissions()
        .mode()
        & 0o777
}

fn set_mode(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
}

fn sidecar(db: &Path, suffix: &str) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// A pool opened the way the crate's openers open theirs (WAL journal),
/// without `prepare` or `restrict`, holding its connection so the
/// sidecars stay on disk.
async fn raw_wal_pool(path: &Path) -> sqlx::SqlitePool {
    use sqlx::Executor;
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                conn.execute("PRAGMA journal_mode = WAL").await?;
                Ok(())
            })
        })
        .connect(&crate::sqlite_util::sqlite_url(&path.to_string_lossy()))
        .await
        .expect("connect");
    sqlx::query("CREATE TABLE IF NOT EXISTS t (k INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("write");
    pool
}

#[test]
fn prepare_creates_a_missing_database_with_mode_0600() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    prepare(&db.to_string_lossy()).expect("prepare");
    assert_eq!(mode(&db), 0o600);
    assert_eq!(std::fs::metadata(&db).expect("metadata").len(), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A configured `sqlite:` URL names the same files as the path in it.
#[test]
fn prepare_and_restrict_accept_a_sqlite_url() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    let location = format!("sqlite://{}?mode=rwc", db.display());
    prepare(&location).expect("prepare");
    assert_eq!(mode(&db), 0o600);
    let wal = sidecar(&db, "-wal");
    std::fs::write(&wal, b"").expect("create");
    set_mode(&db, 0o644);
    set_mode(&wal, 0o644);
    restrict(&location).expect("restrict");
    assert_eq!(mode(&db), 0o600);
    assert_eq!(mode(&wal), 0o600);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn prepare_and_restrict_set_an_existing_database_and_its_sidecars_to_0600() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    let files: Vec<PathBuf> = ["", "-wal", "-shm", "-journal"]
        .iter()
        .map(|suffix| sidecar(&db, suffix))
        .collect();
    for file in &files {
        std::fs::write(file, b"").expect("create");
        set_mode(file, 0o644);
    }
    prepare(&db.to_string_lossy()).expect("prepare");
    for file in &files {
        assert_eq!(mode(file), 0o600, "{}", file.display());
    }
    for file in &files {
        set_mode(file, 0o644);
    }
    restrict(&db.to_string_lossy()).expect("restrict");
    for file in &files {
        assert_eq!(mode(file), 0o600, "{}", file.display());
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn prepare_creates_nothing_when_the_connection_would_not() {
    let dir = temp_dir();
    let plain = dir.join("plain.sqlite");
    let read_only = dir.join("ro.sqlite");
    // A sqlite: URL is passed to sqlx as given: without mode=rwc sqlx does
    // not pass SQLITE_OPEN_CREATE, and mode=ro wins over mode=rwc.
    prepare(&format!("sqlite://{}", plain.display())).expect("prepare");
    prepare(&format!("sqlite://{}?mode=ro", read_only.display())).expect("prepare");
    prepare(&format!(
        "sqlite://{}?mode=rwc&mode=ro",
        read_only.display()
    ))
    .expect("prepare");
    // A `file:` URI with its own query (here `?mode=ro`, percent-encoded
    // for sqlx), and a VFS outside the unix family, are left to SQLite.
    prepare(&format!(
        "sqlite:file:{}%3Fmode=ro?mode=rwc",
        dir.join("uri.sqlite").display()
    ))
    .expect("prepare");
    prepare(&format!(
        "sqlite://{}?mode=rwc&vfs=memdb",
        dir.join("vfs.sqlite").display()
    ))
    .expect("prepare");
    prepare(":memory:").expect("prepare");
    prepare("sqlite::memory:").expect("prepare");
    restrict(&plain.to_string_lossy()).expect("restrict");
    assert_eq!(std::fs::read_dir(&dir).expect("read_dir").count(), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Open a pool through `connect` that switches to WAL and writes in its
/// connection setup, and return it with the modes of the database file
/// and its `-wal` and `-shm` files as that setup saw them, which is before
/// `connect` calls `restrict`. With `set_in_setup`, the setup then sets
/// those files to that mode.
async fn connect_and_record(
    db: &Path,
    set_in_setup: Option<u32>,
) -> (sqlx::SqlitePool, Vec<(&'static str, u32)>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let options = {
        let (seen, db) = (Arc::clone(&seen), db.to_path_buf());
        sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(move |conn, _| {
                let (seen, db) = (Arc::clone(&seen), db.clone());
                Box::pin(async move {
                    use sqlx::Executor;
                    conn.execute("PRAGMA journal_mode = WAL").await?;
                    conn.execute("CREATE TABLE IF NOT EXISTS t (k INTEGER PRIMARY KEY)")
                        .await?;
                    let files =
                        ["", "-wal", "-shm"].map(|suffix| (suffix, sidecar(&db, suffix)));
                    let modes = files.clone().map(|(suffix, file)| (suffix, mode(&file)));
                    seen.lock().expect("lock").extend(modes);
                    if let Some(setup_mode) = set_in_setup {
                        for (_, file) in &files {
                            set_mode(file, setup_mode);
                        }
                    }
                    Ok(())
                })
            })
    };
    let pool = connect(&db.to_string_lossy(), options)
        .await
        .expect("connect");
    let seen = seen.lock().expect("lock").clone();
    (pool, seen)
}

/// The database file `connect` creates, and the sidecars its connection
/// creates, are 0600 from the moment they exist.
#[tokio::test]
async fn connect_creates_a_missing_database_and_its_sidecars_at_0600() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    let (pool, seen) = connect_and_record(&db, None).await;
    assert_eq!(seen, [("", 0o600), ("-wal", 0o600), ("-shm", 0o600)]);
    pool.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The rule the fix relies on, whatever the umask: SQLite gives a new
/// `-wal` or `-shm` file the mode of the database file. A database left
/// at 0644 with no sidecars is set to 0600 by `connect` before the
/// connection opens it, so the sidecars the connection creates are 0600.
#[tokio::test]
async fn connect_restricts_an_existing_database_before_sqlite_creates_its_sidecars() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    raw_wal_pool(&db).await.close().await;
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(sidecar(&db, suffix));
    }
    set_mode(&db, 0o644);
    let (pool, seen) = connect_and_record(&db, None).await;
    assert_eq!(seen, [("", 0o600), ("-wal", 0o600), ("-shm", 0o600)]);
    pool.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Whatever the connection leaves behind is set to 0600 once the pool
/// has connected. Here the connection setup itself sets the database file
/// and its sidecars to 0644.
#[tokio::test]
async fn connect_restricts_the_files_after_the_pool_connects() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    let (pool, _) = connect_and_record(&db, Some(0o644)).await;
    for suffix in ["", "-wal", "-shm"] {
        assert_eq!(mode(&sidecar(&db, suffix)), 0o600, "{suffix}");
    }
    pool.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn prepare_creates_the_target_of_a_dangling_symlink() {
    let dir = temp_dir();
    let target = dir.join("real.sqlite");
    let link = dir.join("link.sqlite");
    std::os::unix::fs::symlink(&target, &link).expect("symlink");
    let location = link.to_string_lossy().into_owned();
    prepare(&location).expect("prepare");
    assert!(target.exists(), "target not created");
    assert_eq!(mode(&target), 0o600);
    // SQLite names the sidecars after the target, and they take its mode.
    let pool = raw_wal_pool(&link).await;
    for suffix in ["-wal", "-shm"] {
        let file = sidecar(&target, suffix);
        assert!(file.exists(), "{} missing", file.display());
        assert_eq!(mode(&file), 0o600, "{}", file.display());
    }
    set_mode(&sidecar(&target, "-wal"), 0o644);
    restrict(&location).expect("restrict");
    assert_eq!(mode(&sidecar(&target, "-wal")), 0o600);
    pool.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}
