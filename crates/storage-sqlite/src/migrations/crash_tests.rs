// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A migrator process killed inside a body. The test binary runs itself as
//! the child, selected by an environment variable.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use extenddb_storage::migrations::{
    CatalogStatus, CodeBody, Done, MigrationBackend, MigrationError, MigrationId, Runner, Step,
};

use super::tests::{
    DEV_0_1_14, LATEST, RELEASE, catalog_store, count, load_dump, run, scalar, status, temp_db,
    test_row, version_row,
};
use super::{SqliteMigrations, SqliteStores};

/// Set to the database path in the child process of the kill test.
const CRASH_CHILD: &str = "EXTENDDB_MIGRATION_CRASH_CHILD";

/// Writes inside the runner's transaction, signals that it has, and waits
/// to be killed.
struct WriteAndWait;

#[async_trait]
impl CodeBody<SqliteStores> for WriteAndWait {
    fn fingerprint(&self) -> &'static str {
        "test/write-and-wait"
    }
    async fn detect(&self, _: &SqliteStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }
    async fn run(&self, stores: &SqliteStores) -> Result<Done, MigrationError> {
        sqlx::raw_sql(
            "UPDATE tables SET attribute_definitions = '[]'; \
             INSERT INTO settings (key, value) VALUES ('killed_body', 'wrote');",
        )
        .execute(&stores.pool)
        .await
        .map_err(|e| MigrationError::store("write", e))?;
        let db = std::env::var(CRASH_CHILD).unwrap_or_default();
        std::fs::write(format!("{db}.ready"), b"").expect("ready file");
        tokio::time::sleep(Duration::from_secs(120)).await;
        Ok(Done::default())
    }
}

/// The child half of the kill test; does nothing in an ordinary run.
#[tokio::test]
async fn kill_test_child_process() {
    let Ok(db) = std::env::var(CRASH_CHILD) else {
        return;
    };
    let migrations = SqliteMigrations::open(&db).await.expect("open");
    let store = catalog_store(std::path::Path::new(&db));
    let _ = migrations
        .apply(
            &store,
            MigrationId::SortKeyDefinitions,
            Step::Code(&WriteAndWait),
            test_row(3),
            Some(3),
        )
        .await;
}

/// The child process, killed when dropped, so a failed assertion does not
/// leave it sleeping.
struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A migrator killed with SIGKILL inside a code body leaves the database as
/// the last committed migration left it, and a rerun completes.
#[cfg(unix)]
#[tokio::test]
async fn a_migrator_killed_inside_a_body_leaves_nothing_and_a_rerun_completes() {
    let db = temp_db("killed");
    load_dump(&db, DEV_0_1_14).await;
    let location = db.to_string_lossy().into_owned();
    let migrations = SqliteMigrations::open(&location).await.expect("open");
    Runner::new(&migrations, RELEASE)
        .migrate_through(MigrationId::Baseline)
        .await
        .expect("migration 1");
    migrations.close().await;
    let definitions = scalar(&db, "SELECT attribute_definitions FROM tables").await;

    let mut child = Child(
        std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "migrations::crash_tests::kill_test_child_process",
                "--nocapture",
            ])
            .env(CRASH_CHILD, &location)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("child"),
    );
    let ready = PathBuf::from(format!("{location}.ready"));
    let started = std::time::Instant::now();
    while !ready.exists() {
        assert!(
            child.0.try_wait().expect("child state").is_none(),
            "the child exited early"
        );
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the child never wrote"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    child.0.kill().expect("SIGKILL");
    child.0.wait().expect("reaped");

    assert_eq!(
        scalar(&db, "SELECT attribute_definitions FROM tables").await,
        definitions
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM settings WHERE key = 'killed_body'"
        )
        .await,
        0
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM extenddb_migrations").await,
        1
    );
    assert_eq!(version_row(&db).await.as_deref(), Some("1"));
    run(&db).await.expect("rerun");
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
}

/// Set to the copy's path in the child process of the copy kill test.
const COPY_CHILD: &str = "EXTENDDB_MIGRATION_COPY_CHILD";

/// Signals that the partial copy is written and synced, and waits to be
/// killed before the rename.
fn wait_before_rename(partial: &std::path::Path) {
    std::fs::write(format!("{}.ready", partial.display()), b"").expect("ready file");
    std::thread::sleep(Duration::from_secs(120));
}

/// The child half of the copy kill test; does nothing in an ordinary run.
#[tokio::test]
async fn copy_kill_test_child_process() {
    let Ok(copy) = std::env::var(COPY_CHILD) else {
        return;
    };
    let db = std::env::var(CRASH_CHILD).expect("database");
    let migrations = SqliteMigrations::open(&db).await.expect("open");
    let _ = super::copy::pre_migration_copy(
        migrations.pool(),
        std::path::Path::new(&db),
        std::path::Path::new(&copy),
        super::copy::free_space,
        wait_before_rename,
    )
    .await;
}

/// A migrator killed after it wrote its copy and before the rename leaves
/// no file under the copy's name, so an operator restoring "the copy" can
/// only find complete ones; the database is untouched.
#[cfg(unix)]
#[tokio::test]
async fn a_copy_interrupted_by_a_kill_leaves_nothing_under_the_copy_name() {
    let db = temp_db("killed-copy");
    load_dump(&db, DEV_0_1_14).await;
    let location = db.to_string_lossy().into_owned();
    let copy = super::copy::copy_path(&db, "legacy", LATEST, std::time::SystemTime::now());
    let mut child = Child(
        std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "migrations::crash_tests::copy_kill_test_child_process",
                "--nocapture",
            ])
            .env(CRASH_CHILD, &location)
            .env(COPY_CHILD, &copy)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("child"),
    );
    let ready = PathBuf::from(format!("{}.partial.ready", copy.display()));
    let started = std::time::Instant::now();
    while !ready.exists() {
        assert!(
            child.0.try_wait().expect("child state").is_none(),
            "the child exited early"
        );
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the child never copied"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    child.0.kill().expect("SIGKILL");
    child.0.wait().expect("reaped");

    assert!(!copy.exists(), "a file has the copy's name");
    let partial = PathBuf::from(format!("{}.partial", copy.display()));
    assert!(partial.exists());
    assert_eq!(version_row(&db).await.as_deref(), Some("0.0.3"));
    assert!(matches!(
        status(&db).await,
        CatalogStatus::Behind { version: None, .. }
    ));

    // The next run, in a later second and so under another copy name,
    // removes the stopped run's partial copy and its journal, and names it.
    super::tests::next_second().await;
    let report = super::tests::run(&db).await.expect("the next run");
    let journal = PathBuf::from(format!("{}.partial-journal", copy.display()));
    assert!(!partial.exists(), "the stopped run's partial copy is left");
    assert!(!journal.exists(), "the stopped run's journal is left");
    let removed = format!("removed {}, left by an earlier run", partial.display());
    assert!(
        report.notes.iter().any(|n| n.starts_with(&removed)),
        "{:?}",
        report.notes
    );
}
