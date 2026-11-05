// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Opening the runner's and the status's connections: the status of a WAL
//! catalog in a directory the process cannot write, a database the runner
//! cannot write, and sqlx's slow-statement warnings, which the operator
//! lines replace.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

use extenddb_storage::migrations::CatalogStatus;
use sqlx::{ConnectOptions, Connection};

use super::SqliteMigrations;
use super::tests::{DEV_0_1_14, LATEST, RELEASE, exec, load_dump, run, status, temp_db};

/// The bytes of the database and of its `-wal` and `-shm` files.
fn files(db: &Path) -> Vec<Option<Vec<u8>>> {
    ["", "-wal", "-shm"]
        .iter()
        .map(|suffix| std::fs::read(format!("{}{suffix}", db.display())).ok())
        .collect()
}

fn chmod(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
}

/// A process that can write a 0500 directory (root) cannot test this.
fn writes_anyway(dir: &Path) -> bool {
    std::fs::write(dir.join("probe"), b"").is_ok()
}

#[tokio::test]
async fn the_status_of_a_wal_catalog_in_a_read_only_directory_changes_nothing() {
    let db = temp_db("ro-dir");
    run(&db).await.expect("migrate");
    // A writer keeps the catalog in WAL with its -wal and -shm files, as a
    // running server does.
    let writer = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db.display()))
        .await
        .expect("writer");
    sqlx::query("UPDATE settings SET value = value WHERE key = 'catalog_version'")
        .execute(&writer)
        .await
        .expect("write");
    let dir = db.parent().expect("dir").to_owned();
    let before = files(&db);
    assert!(
        before.iter().all(Option::is_some),
        "the catalog is not in WAL"
    );
    chmod(&dir, 0o500);
    if writes_anyway(&dir) {
        chmod(&dir, 0o700);
        writer.close().await;
        eprintln!("SKIP: this process writes a read-only directory");
        return;
    }
    let started = Instant::now();
    let read = super::catalog_status(&db.to_string_lossy()).await;
    let elapsed = started.elapsed();
    let after = files(&db);
    chmod(&dir, 0o700);
    writer.close().await;
    assert_eq!(
        read.expect("status"),
        CatalogStatus::Current { version: LATEST }
    );
    assert!(elapsed < Duration::from_secs(5), "took {elapsed:?}");
    assert!(
        before == after,
        "reading the status changed the database's files"
    );
}

/// With no process holding the WAL catalog open there is no `-shm` file,
/// and a reader has to create one in the directory: the status fails at
/// once, and the message says why, not only that the database is read-only.
#[tokio::test]
async fn a_closed_wal_catalog_in_a_read_only_directory_names_the_shm_file() {
    let db = temp_db("ro-dir-closed");
    run(&db).await.expect("migrate");
    let dir = db.parent().expect("dir").to_owned();
    assert!(
        files(&db)[1..].iter().all(Option::is_none),
        "a -wal or -shm file is left open"
    );
    chmod(&dir, 0o500);
    if writes_anyway(&dir) {
        chmod(&dir, 0o700);
        eprintln!("SKIP: this process writes a read-only directory");
        return;
    }
    let started = Instant::now();
    let read = super::catalog_status(&db.to_string_lossy()).await;
    let elapsed = started.elapsed();
    chmod(&dir, 0o700);
    let message = read
        .expect_err("a WAL reader needs to create -shm")
        .to_string();
    assert!(elapsed < Duration::from_secs(5), "took {elapsed:?}");
    assert!(
        message.contains("WAL mode") && message.contains("-shm"),
        "{message}"
    );
}

#[tokio::test]
async fn a_database_the_runner_cannot_write_fails_at_once_with_its_cause() {
    // A rollback-journal database, as a 0.1.x release left it: the runner's
    // connection switches it to WAL, which writes the file.
    let db = temp_db("ro-file");
    exec(
        &db,
        "CREATE TABLE t (x INTEGER); PRAGMA journal_mode = DELETE",
    )
    .await;
    chmod(&db, 0o400);
    if std::fs::OpenOptions::new().write(true).open(&*db).is_ok() {
        chmod(&db, 0o600);
        eprintln!("SKIP: this process writes a 0400 file");
        return;
    }
    let started = Instant::now();
    let opened = SqliteMigrations::open(&db.to_string_lossy()).await;
    let elapsed = started.elapsed();
    chmod(&db, 0o600);
    let Err(err) = opened else {
        panic!("the runner opened a database it cannot write");
    };
    assert!(elapsed < Duration::from_secs(5), "took {elapsed:?}");
    let message = err.to_string();
    assert!(
        message.contains("readonly database") && !message.contains("pool timed out"),
        "{message}"
    );
}

/// Set by the parent of the slow-statement test for its child.
const SLOW_CHILD: &str = "EXTENDDB_TEST_SLOW_STATEMENT_CHILD";

/// The child half of the slow-statement test; does nothing in an ordinary
/// run. Every event goes to stderr. A connection that logs every statement
/// as slow runs one, so the parent sees that the capture works, then a run
/// migrates the catalog at the path given.
#[tokio::test]
async fn slow_statement_test_child_process() {
    let Ok(db) = std::env::var(SLOW_CHILD) else {
        return;
    };
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .init();
    let mut probe = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(format!("{db}.probe"))
        .create_if_missing(true)
        .log_slow_statements(log::LevelFilter::Warn, Duration::ZERO)
        .connect()
        .await
        .expect("probe");
    sqlx::query("SELECT 1")
        .execute(&mut probe)
        .await
        .expect("probe");
    let _ = probe.close().await;
    eprintln!("=== run");
    let report = super::migrate(&db, RELEASE, Duration::from_secs(10))
        .await
        .expect("migrate");
    eprintln!("=== applied {}", report.applied.len());
}

/// In a test build the runner's connections count every statement as slow
/// (`backend.rs`), so a run with statement logging left on would print a
/// warning for every statement, the copy and the rebuilds included.
#[tokio::test]
async fn a_run_prints_no_slow_statement_warning() {
    let db = temp_db("slow");
    load_dump(&db, DEV_0_1_14).await;
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "migrations::open_tests::slow_statement_test_child_process",
            "--nocapture",
        ])
        .env(SLOW_CHILD, db.to_string_lossy().as_ref())
        .output()
        .expect("child");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    let (probe, run) = stderr.split_once("=== run").expect("the child ran");
    assert!(
        probe.contains("slow statement"),
        "the capture saw nothing: {probe}"
    );
    assert!(run.contains("=== applied"), "{run}");
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
    assert!(!run.contains("slow statement"), "{run}");
}
