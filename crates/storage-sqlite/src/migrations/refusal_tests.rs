// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What the SQLite runner refuses, and that a refusal or a failed body
//! commits nothing: foreign key violations, a failing file body, the
//! post-check, a ledger it cannot read, the copy's name and the free space.

use extenddb_storage::migrations::{
    CatalogStatus, MigrationBackend, MigrationError, MigrationId, Runner, Step, exit,
};

use super::SqliteMigrations;
use super::tests::{
    DEV_0_1_14, LATEST, Orphan, RELEASE, catalog_store, count, exec, load_dump, plain_pool, run,
    scalar, status, temp_db, test_row, version_row,
};

async fn schema(db: &std::path::Path) -> Vec<(String, String, Option<String>)> {
    let pool = plain_pool(db).await;
    let rows = sqlx::query_as("SELECT type, name, sql FROM sqlite_master ORDER BY type, name")
        .fetch_all(&pool)
        .await
        .expect("schema");
    pool.close().await;
    rows
}

#[tokio::test]
async fn a_foreign_key_violation_a_body_adds_rolls_the_body_back() {
    let db = temp_db("fk");
    run(&db).await.expect("fresh");
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    let err = migrations
        .apply(
            &catalog_store(&db),
            MigrationId::IndexTableBaseKey,
            Step::Code(&Orphan),
            test_row(LATEST + 5),
            None,
        )
        .await
        .expect_err("orphan row");
    assert!(err.to_string().contains("violate a foreign key"), "{err}");
    migrations.close().await;
    assert_eq!(count(&db, "SELECT COUNT(*) FROM indexes").await, 0);
    assert_eq!(
        count(
            &db,
            &format!(
                "SELECT COUNT(*) FROM extenddb_migrations WHERE id = {}",
                LATEST + 5
            )
        )
        .await,
        0
    );
}

/// Rows that violated a foreign key before the run (a parent deleted with
/// foreign keys off, which the `sqlite3` shell does by default) are noted in
/// every body's row and do not stop it; a violation a body adds still does.
#[tokio::test]
async fn foreign_key_violations_that_predate_the_run_are_noted() {
    let db = temp_db("fk-before");
    load_dump(&db, DEV_0_1_14).await;
    exec(&db, "PRAGMA foreign_keys = OFF; DELETE FROM tables;").await;
    run(&db).await.expect("migrate past the earlier violations");
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
    let notes = scalar(&db, "SELECT notes FROM extenddb_migrations WHERE id = 1")
        .await
        .expect("row");
    assert!(
        notes.contains("violated a foreign key before migration 1 baseline ran"),
        "{notes}"
    );
    assert!(notes.contains("indexes (2)"), "{notes}");
    assert!(notes.contains("stream_shards (4)"), "{notes}");

    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    let err = migrations
        .apply(
            &catalog_store(&db),
            MigrationId::IndexTableBaseKey,
            Step::Code(&Orphan),
            test_row(LATEST + 5),
            None,
        )
        .await
        .expect_err("a new orphan");
    migrations.close().await;
    assert!(err.to_string().contains("left 1 row(s)"), "{err}");
}

/// A file body's statements run inside the runner's transaction: when its
/// last statement fails, the first one's write is gone too.
#[tokio::test]
async fn a_file_body_that_fails_part_way_commits_nothing() {
    let db = temp_db("file-body");
    run(&db).await.expect("fresh");
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    let err = migrations
        .apply(
            &catalog_store(&db),
            MigrationId::IndexTableBaseKey,
            Step::File {
                name: "0004_index_table_base_key.sql",
                text: "INSERT INTO settings (key, value) VALUES ('file_body', 'wrote');\n\
                       INSERT INTO no_such_table VALUES (1);",
            },
            test_row(LATEST + 5),
            Some(LATEST + 5),
        )
        .await
        .expect_err("the second statement fails");
    migrations.close().await;
    assert!(err.to_string().contains("no such table"), "{err}");
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM settings WHERE key = 'file_body'").await,
        0
    );
    assert_eq!(version_row(&db).await, Some(LATEST.to_string()));
}

#[tokio::test]
async fn a_body_that_fails_part_way_leaves_nothing_and_a_rerun_completes() {
    let db = temp_db("crash");
    load_dump(&db, DEV_0_1_14).await;
    let location = db.to_string_lossy().into_owned();
    let migrations = SqliteMigrations::open(&location).await.expect("open");
    Runner::new(&migrations, RELEASE)
        .migrate_through(MigrationId::Baseline)
        .await
        .expect("migration 1");
    // Migration 3's step with a body that writes and then fails.
    let err = migrations
        .apply(
            &catalog_store(&db),
            MigrationId::SortKeyDefinitions,
            Step::Code(&Orphan),
            test_row(3),
            Some(3),
        )
        .await
        .expect_err("fails at the foreign key check");
    assert_eq!(err.exit_code(), exit::ERROR);
    migrations.close().await;
    assert_eq!(
        version_row(&db).await.as_deref(),
        Some("1"),
        "the version row is the one committed with migration 1"
    );
    let CatalogStatus::Behind { version, .. } = status(&db).await else {
        panic!("expected Behind");
    };
    assert_eq!(version, Some(1));
    let report = run(&db).await.expect("rerun");
    assert_eq!(report.from, Some(1));
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
}

/// Shapes the post-check refuses after migration 1: an extra column, and an
/// index on other columns. Each refusal commits nothing: the schema, the
/// version row and the marker row that keeps released servers off the
/// catalog are as they were, and there is no ledger.
#[tokio::test]
async fn the_post_check_refuses_extra_parts_and_commits_nothing() {
    for (case, change, expected) in [
        (
            "extra-column",
            "CREATE TABLE table_restores (\
                 table_id TEXT PRIMARY KEY REFERENCES tables(table_id) ON DELETE CASCADE, \
                 source_backup_arn TEXT NOT NULL, source_table_arn TEXT, \
                 restore_date_time TEXT NOT NULL, restored_by TEXT NOT NULL)",
            "found [restored_by TEXT NOT NULL]",
        ),
        (
            "other-index",
            "DROP INDEX idx_backups_table; \
             CREATE INDEX idx_backups_table ON backups (table_name);",
            "index idx_backups_table differs",
        ),
    ] {
        let db = temp_db(case);
        load_dump(&db, DEV_0_1_14).await;
        exec(&db, change).await;
        let before = schema(&db).await;
        let err = run(&db).await.expect_err(case);
        let message = err.to_string();
        assert!(
            message.contains("nothing was committed"),
            "{case}: {message}"
        );
        assert!(message.contains(expected), "{case}: {message}");
        assert_eq!(schema(&db).await, before, "{case}");
        assert_eq!(version_row(&db).await.as_deref(), Some("0.0.3"), "{case}");
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM settings WHERE key = 'resource_tag_migration'"
            )
            .await,
            1,
            "{case}: the marker is still there"
        );
    }
}

/// A ledger this binary cannot read is an error, never "not applied".
#[tokio::test]
async fn a_ledger_that_cannot_be_read_is_an_error() {
    let db = temp_db("bad-ledger");
    run(&db).await.expect("fresh");
    exec(&db, "ALTER TABLE extenddb_migrations DROP COLUMN notes").await;
    let found = super::catalog_status(&db.to_string_lossy()).await;
    assert!(
        matches!(found, Err(MigrationError::Store(ref m)) if m.contains("read extenddb_migrations")),
        "{found:?}"
    );
    assert!(run(&db).await.is_err());
}

/// Reading the status writes nothing: a rollback-journal database keeps its
/// journal mode and its bytes.
#[tokio::test]
async fn the_status_writes_nothing() {
    let db = temp_db("read-only");
    run(&db).await.expect("fresh");
    exec(&db, "PRAGMA journal_mode = DELETE").await;
    let bytes = std::fs::read(&*db).expect("database");
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
    assert_eq!(std::fs::read(&*db).expect("database"), bytes);
    assert_eq!(
        scalar(&db, "PRAGMA journal_mode").await.as_deref(),
        Some("delete")
    );
}

/// The list of policies a pending migration 2 rewrites, which the preview
/// and `verify` print, is read on a read-only connection as the status is:
/// a rollback-journal database keeps its journal mode and its bytes. A
/// connection that opens the database for writing sets WAL mode, and when
/// it is the last one to close it checkpoints the WAL into the file.
#[tokio::test]
async fn listing_the_policies_migration_2_rewrites_writes_nothing() {
    use extenddb_storage::migrations::{CatalogMigrations, Target};

    use super::SqliteCatalogMigrations;

    let db = temp_db("pending-read-only");
    load_dump(&db, DEV_0_1_14).await;
    let pool = plain_pool(&db).await;
    let (account, user): (String, String) =
        sqlx::query_as("SELECT account_id, user_name FROM iam_users LIMIT 1")
            .fetch_one(&pool)
            .await
            .expect("an IAM user");
    sqlx::query(
        "INSERT INTO iam_policies (account_id, principal_type, principal_name, policy_name, \
         policy_document) VALUES (?, 'user', ?, 'tagged', ?)",
    )
    .bind(&account)
    .bind(&user)
    .bind(
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"dynamodb:ResourceTag/env":"prod"}}}]}"#,
    )
    .execute(&pool)
    .await
    .expect("policy");
    pool.close().await;
    exec(&db, "PRAGMA journal_mode = DELETE").await;
    let bytes = std::fs::read(&*db).expect("database");
    // As in the binary: the trait's default would open the installed
    // backend's management store.
    let _ = extenddb_storage::set_backend(crate::backend());

    let target = Target::at(db.to_string_lossy());
    let status = SqliteCatalogMigrations
        .status(&target)
        .await
        .expect("status");
    let lines = SqliteCatalogMigrations
        .pending_changes(&target, &status)
        .await
        .expect("the policies migration 2 rewrites");
    assert_eq!(
        lines[0],
        "Migration 2 rewrites 1 stored policy from dynamodb:ResourceTag to aws:ResourceTag:"
    );
    assert!(
        lines[1].contains(&format!("account {account} ")) && lines[1].contains("policy tagged"),
        "{lines:?}"
    );
    assert!(
        std::fs::read(&*db).expect("database") == bytes,
        "listing the policies changed the database file"
    );
    assert_eq!(
        scalar(&db, "PRAGMA journal_mode").await.as_deref(),
        Some("delete")
    );
}

/// A file at the copy's name, whatever it holds, is never kept as the copy
/// or replaced: the run stops, and the file and the database are untouched.
#[tokio::test]
async fn a_file_at_the_copy_name_stops_the_run() {
    let db = temp_db("copy-exists");
    load_dump(&db, DEV_0_1_14).await;
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    let at = std::time::SystemTime::now();
    let copy = super::copy::copy_path(&db, "legacy", LATEST, at);
    let full = super::copy::copy_path(&db, "earlier", LATEST, at);
    super::copy::pre_migration_copy(migrations.pool(), &db, &full, super::copy::Hooks::RUN)
        .await
        .expect("a full copy");
    let no_tables = db.with_file_name("no-tables.sqlite");
    exec(&no_tables, "PRAGMA user_version = 1").await;
    let first_page = std::fs::read(&full).expect("copy")[..4096].to_vec();
    for content in [
        first_page,
        Vec::new(),
        std::fs::read(&no_tables).expect("database"),
        std::fs::read(&full).expect("copy"),
    ] {
        std::fs::write(&copy, &content).expect("plant");
        let err =
            super::copy::pre_migration_copy(migrations.pool(), &db, &copy, super::copy::Hooks::RUN)
                .await
                .expect_err("refused");
        assert!(err.to_string().contains("already exists"), "{err}");
        assert_eq!(std::fs::read(&copy).expect("kept"), content);
    }
    migrations.close().await;
    assert_eq!(version_row(&db).await.as_deref(), Some("0.0.3"));
}

/// A copy that fails while it writes removes its partial file: the likeliest
/// cause is a full disk, where that file is as large as the database. A
/// directory in place of the journal SQLite writes beside the partial file
/// makes the copy fail after the partial file exists.
#[tokio::test]
async fn a_failed_copy_leaves_no_partial_file() {
    let db = temp_db("copy-fails");
    load_dump(&db, DEV_0_1_14).await;
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    let copy = super::copy::copy_path(&db, "legacy", LATEST, std::time::SystemTime::now());
    let partial = std::path::PathBuf::from(format!("{}.partial", copy.display()));
    let journal = std::path::PathBuf::from(format!("{}.partial-journal", copy.display()));
    std::fs::create_dir(&journal).expect("a directory at the journal's path");
    let err =
        super::copy::pre_migration_copy(migrations.pool(), &db, &copy, super::copy::Hooks::RUN)
            .await
            .expect_err("the copy fails");
    migrations.close().await;
    assert!(err.to_string().contains("copy the database"), "{err}");
    assert!(
        !partial.exists(),
        "the failed copy left {}",
        partial.display()
    );
    assert!(!copy.exists());
}

/// Another connection's reader whose snapshot holds frames of the WAL keeps
/// every checkpoint from moving them into the main file. The copy reads the
/// committed frames of the WAL with the main file, so it holds the write in
/// them, and it is owner-only.
#[cfg(unix)]
#[tokio::test]
async fn a_copy_holds_the_writes_a_reader_keeps_in_the_wal() {
    use std::os::unix::fs::PermissionsExt;
    let db = temp_db("copy-wal");
    load_dump(&db, DEV_0_1_14).await;
    let reader = super::tests::plain_pool(&db).await;
    sqlx::query("INSERT INTO settings (key, value) VALUES ('in_the_wal', '1')")
        .execute(&reader)
        .await
        .expect("write");
    let mut held = reader.acquire().await.expect("connection");
    sqlx::raw_sql("BEGIN; SELECT COUNT(*) FROM settings;")
        .execute(&mut *held)
        .await
        .expect("a read transaction on the WAL");
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");

    let copy = super::copy::copy_path(&db, "legacy", LATEST, std::time::SystemTime::now());
    let lines =
        super::copy::pre_migration_copy(migrations.pool(), &db, &copy, super::copy::Hooks::RUN)
            .await
            .expect("the copy");
    let line = lines.last().expect("the copy's line");
    assert!(line.contains(&copy.display().to_string()), "{line}");
    let wal = std::fs::metadata(format!("{}-wal", db.display()))
        .expect("the WAL")
        .len();
    assert!(wal > 0, "the write is no longer in the WAL");
    let mode = std::fs::metadata(&copy).expect("copy").permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    sqlx::raw_sql("ROLLBACK")
        .execute(&mut *held)
        .await
        .expect("end the read");
    drop(held);
    reader.close().await;
    migrations.close().await;
    assert_eq!(
        count(
            &copy,
            "SELECT COUNT(*) FROM settings WHERE key = 'in_the_wal'"
        )
        .await,
        1
    );
}

/// The copy opens no file descriptor of the database outside SQLite.
/// Closing one would release every POSIX lock this process holds on the
/// file, the shared lock of the runner's connection included, and another
/// process that then closed its connection would take itself for the last
/// one and delete the WAL, with the commits the run makes after it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_copy_leaves_the_runners_lock_on_the_database() {
    let db = temp_db("copy-lock");
    load_dump(&db, DEV_0_1_14).await;
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    // A connection in WAL mode keeps the shared lock its first read takes.
    sqlx::query("SELECT COUNT(*) FROM sqlite_master")
        .execute(migrations.pool())
        .await
        .expect("read");
    let held = posix_locks(&db);
    assert!(!held.is_empty(), "the runner's connection holds no lock");
    let copy = super::copy::copy_path(&db, "legacy", LATEST, std::time::SystemTime::now());
    super::copy::pre_migration_copy(migrations.pool(), &db, &copy, super::copy::Hooks::RUN)
        .await
        .expect("the copy");
    assert_eq!(
        posix_locks(&db),
        held,
        "the copy released the runner's lock"
    );
    migrations.close().await;
}

/// This process's POSIX locks on the file at `path`, from /proc/locks: one
/// `<type> <start>-<end>` entry each.
#[cfg(target_os = "linux")]
fn posix_locks(path: &std::path::Path) -> Vec<String> {
    use std::os::unix::fs::MetadataExt;
    let inode = std::fs::metadata(path).expect("the file").ino().to_string();
    let pid = std::process::id().to_string();
    std::fs::read_to_string("/proc/locks")
        .expect("/proc/locks")
        .lines()
        .filter_map(|line| {
            // `1: POSIX  ADVISORY  READ 4242 08:01:131074 1073741826 1073742335`
            let fields: Vec<&str> = line.split_whitespace().collect();
            let ours = fields.len() >= 8
                && fields[1] == "POSIX"
                && fields[4] == pid
                && fields[5].rsplit(':').next() == Some(inode.as_str());
            ours.then(|| format!("{} {}-{}", fields[3], fields[6], fields[7]))
        })
        .collect()
}

/// The free space the probe reports is compared with three times the
/// database's size before the copy is taken, after the partial copies of
/// stopped runs are removed, since they may be what fills the disk.
#[tokio::test]
async fn a_short_filesystem_stops_the_run_before_the_copy() {
    let db = temp_db("short");
    load_dump(&db, DEV_0_1_14).await;
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    let copy = super::copy::copy_path(&db, "legacy", LATEST, std::time::SystemTime::now());
    let stopped = std::path::PathBuf::from(format!(
        "{}.pre-migration-legacy-to-4-20260101T000000Z.partial",
        db.display()
    ));
    std::fs::write(&stopped, b"a stopped run's copy").expect("plant");
    let err = super::copy::pre_migration_copy(
        migrations.pool(),
        &db,
        &copy,
        super::copy::Hooks {
            free_space: |_| Some(4096),
            ..super::copy::Hooks::RUN
        },
    )
    .await
    .expect_err("short");
    migrations.close().await;
    assert!(
        err.to_string().contains("three times the database's size"),
        "{err}"
    );
    assert!(!copy.exists());
    assert!(!stopped.exists(), "the space check came before the removal");
}

/// A run with nothing to apply takes no copy, and still removes the partial
/// copies stopped runs left, with their journals, and names them.
#[tokio::test]
async fn a_run_with_nothing_to_apply_removes_stopped_partial_copies() {
    let db = temp_db("partials-current");
    run(&db).await.expect("fresh");
    let stem = format!("{}.pre-migration-3-to-4-20260101T000000Z", db.display());
    let partials = [format!("{stem}.partial"), format!("{stem}.partial-journal")];
    for p in &partials {
        std::fs::write(p, b"left by a stopped run").expect("plant");
    }
    let report = run(&db).await.expect("nothing to apply");
    assert!(report.applied.is_empty(), "{report:?}");
    for p in &partials {
        assert!(!std::path::Path::new(p).exists(), "{p} is left");
        assert!(
            report
                .notes
                .iter()
                .any(|n| n.starts_with(&format!("removed {p},"))),
            "{:?}",
            report.notes
        );
    }
}

/// A run killed after its copy was complete, with nothing committed, leaves
/// the copy. The next run's copy has the same bytes, so it replaces it
/// whatever its `<to>`, and the run names it. A copy with other bytes, one a
/// process opened (a `-wal` beside it) and one with another `<from>` stay,
/// and a reconverge after the run, whose copy holds the migrated database,
/// replaces none.
#[tokio::test]
async fn a_copy_of_the_same_contents_replaces_an_earlier_one() {
    let db = temp_db("superseded");
    load_dump(&db, DEV_0_1_14).await;
    let at = |second: u64| {
        std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_767_225_600 + second)
    };
    let stopped = super::copy::copy_path(&db, "legacy", LATEST, at(0));
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    super::copy::pre_migration_copy(migrations.pool(), &db, &stopped, super::copy::Hooks::RUN)
        .await
        .expect("a stopped run's copy");
    migrations.close().await;
    let bytes = std::fs::read(&stopped).expect("copy");
    let plant = |path: &std::path::Path, content: &[u8]| {
        std::fs::write(path, content).expect("plant");
    };
    let older_binary = super::copy::copy_path(&db, "legacy", 3, at(1));
    plant(&older_binary, &bytes);
    let mut changed = bytes.clone();
    *changed.last_mut().expect("a page") ^= 1;
    let other_bytes = super::copy::copy_path(&db, "legacy", LATEST, at(2));
    plant(&other_bytes, &changed);
    let opened = super::copy::copy_path(&db, "legacy", LATEST, at(3));
    plant(&opened, &bytes);
    plant(
        std::path::Path::new(&format!("{}-wal", opened.display())),
        b"",
    );
    let other_version = super::copy::copy_path(&db, "2", LATEST, at(4));
    plant(&other_version, &bytes);

    let report = run(&db).await.expect("migrate");
    for path in [&stopped, &older_binary] {
        assert!(!path.exists(), "{} is left", path.display());
        let removed = format!(
            "removed {}, a copy an earlier run took of the same database contents",
            path.display()
        );
        assert!(
            report.notes.iter().any(|n| n.starts_with(&removed)),
            "{:?}",
            report.notes
        );
    }
    for path in [&other_bytes, &opened, &other_version] {
        assert!(path.exists(), "{} was removed", path.display());
    }

    super::tests::next_second().await;
    let report = super::tests::reconverge(&db).await.expect("reconverge");
    assert!(
        report.notes.iter().all(|n| !n.starts_with("removed")),
        "{:?}",
        report.notes
    );
    for path in [&other_bytes, &opened, &other_version] {
        assert!(path.exists(), "{} was removed", path.display());
    }
}

/// A run refused by migration 1's post-check committed nothing, so it
/// removes the copy it took: two failed runs leave no copy, and each
/// reports the post-check alone.
#[tokio::test]
async fn a_run_that_commits_nothing_removes_its_copy() {
    let db = temp_db("failed-first-step");
    load_dump(&db, DEV_0_1_14).await;
    exec(&db, "ALTER TABLE tables ADD COLUMN not_in_any_release TEXT").await;
    for _ in 0..2 {
        let err = run(&db).await.expect_err("the post-check refuses");
        assert!(!matches!(err, MigrationError::Stopped { .. }), "{err:?}");
        assert!(err.to_string().contains("nothing was committed"), "{err}");
        assert_eq!(super::tests::copies(&db), Vec::<std::path::PathBuf>::new());
        super::tests::next_second().await;
    }
    assert_eq!(version_row(&db).await.as_deref(), Some("0.0.3"));
}

/// No SQLite migration is declared additive until CI runs the previous
/// release against a catalog this binary migrated; the PR that adds that
/// job deletes this test.
#[test]
fn no_migration_is_additive_before_the_old_binary_job() {
    extenddb_storage::migrations::refuse_additive("sqlite", super::migration)
        .unwrap_or_else(|e| panic!("{e}"));
}

/// `migrate --yes` on a path with no database refuses it as `Uninitialized`
/// (exit 1, naming `extenddb init`) and creates no file and no directory;
/// on a file that is not a catalog it refuses under the lock and writes
/// nothing.
#[tokio::test]
async fn migrate_yes_refuses_a_store_that_is_not_a_catalog() {
    use extenddb_storage::migrations::{CatalogMigrations, RunKind, Target, run_locked};

    use super::SqliteCatalogMigrations;

    let db = temp_db("not-a-catalog");
    let missing_dir = db.with_file_name("missing-dir");
    let missing = Target::at(missing_dir.join("extenddb.sqlite").to_string_lossy());
    let err = SqliteCatalogMigrations
        .lock(&missing, std::time::Duration::ZERO)
        .await
        .expect_err("no database");
    assert_eq!(err, MigrationError::Status(CatalogStatus::Uninitialized));
    assert_eq!(err.exit_code(), exit::ERROR);
    assert!(!missing_dir.exists(), "the lock created a directory");

    std::fs::write(&*db, b"").expect("empty file");
    let empty = Target::at(db.to_string_lossy());
    for kind in [RunKind::Migrate, RunKind::Reconverge] {
        let lock = SqliteCatalogMigrations
            .lock(&empty, std::time::Duration::ZERO)
            .await
            .expect("lock");
        let err = run_locked(&SqliteCatalogMigrations, &empty, &lock, RELEASE, kind)
            .await
            .expect_err("not a catalog");
        assert_eq!(err, MigrationError::Status(CatalogStatus::Uninitialized));
    }
    assert_eq!(std::fs::metadata(&*db).expect("file").len(), 0);
    assert!(!db.with_file_name("db.sqlite-wal").exists());
}

/// A run that committed a step before it stopped keeps its copy and names
/// it in the error; one that committed nothing removes it.
#[tokio::test]
async fn a_stopped_run_keeps_its_copy_only_after_a_commit() {
    use extenddb_storage::migrations::{AppliedStep, RunReport};

    let db = temp_db("stopped-copy");
    load_dump(&db, DEV_0_1_14).await;
    let location = db.to_string_lossy().into_owned();
    let stopped = |notes: Vec<String>, applied: Vec<AppliedStep>| {
        Err(MigrationError::Stopped {
            error: Box::new(MigrationError::Store("migration 3 failed".to_owned())),
            report: RunReport {
                from: None,
                to: Some(2),
                applied,
                notes,
            },
        })
    };
    let committed = vec![AppliedStep {
        id: 1,
        name: "baseline",
        store_name: location.clone(),
        notes: Vec::new(),
    }];

    let migrations = SqliteMigrations::open(&location).await.expect("open");
    let notes = migrations
        .before_first_write(None, LATEST)
        .await
        .expect("copy");
    let kept = migrations.discard_unused_copy(stopped(notes.clone(), committed));
    let Err(MigrationError::Stopped { report, .. }) = kept else {
        panic!("expected Stopped: {kept:?}");
    };
    assert_eq!(report.notes, notes);
    let [copy] = super::tests::copies(&db).try_into().expect("one copy");
    assert!(notes[0].contains(&copy.display().to_string()), "{notes:?}");
    migrations.close().await;
    std::fs::remove_file(&copy).expect("remove the kept copy");

    super::tests::next_second().await;
    let migrations = SqliteMigrations::open(&location).await.expect("open");
    let notes = migrations
        .before_first_write(None, LATEST)
        .await
        .expect("copy");
    assert_eq!(super::tests::copies(&db).len(), 1);
    let removed = migrations.discard_unused_copy(stopped(notes, Vec::new()));
    assert_eq!(
        removed,
        Err(MigrationError::Store("migration 3 failed".to_owned()))
    );
    assert_eq!(super::tests::copies(&db), Vec::<std::path::PathBuf>::new());
    migrations.close().await;
}

/// A step whose `COMMIT` succeeded is reported as applied even when the
/// `PRAGMA foreign_keys = ON` after it fails: the run stops with that error
/// at its next step and keeps its copy, since the database has changed.
#[tokio::test]
async fn a_failed_pragma_after_a_commit_keeps_the_copy() {
    use extenddb_storage::migrations::Runner;

    let db = temp_db("pragma-after-commit");
    load_dump(&db, DEV_0_1_14).await;
    let location = db.to_string_lossy().into_owned();
    let migrations = SqliteMigrations::open(&location).await.expect("open");
    migrations
        .fail_restore
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let result = Runner::new(&migrations, RELEASE).migrate().await;
    let result = migrations.discard_unused_copy(result);
    migrations.close().await;
    let Err(MigrationError::Stopped { error, report }) = result else {
        panic!("expected Stopped: {result:?}");
    };
    assert!(
        error
            .to_string()
            .contains("was committed, and then PRAGMA foreign_keys = ON"),
        "{error}"
    );
    let applied: Vec<u32> = report.applied.iter().map(|s| s.id).collect();
    assert_eq!(applied, [1]);
    assert_eq!(
        super::tests::copies(&db).len(),
        1,
        "the copy of a changed database was removed"
    );
}

/// A body and its ledger row commit together: when the row's insert fails
/// after the body ran, none of the body's changes are left, and the id is
/// still pending. Migration 2's body rewrites the policy this test stores.
#[tokio::test]
async fn a_failed_ledger_insert_leaves_nothing_of_the_body() {
    let db = temp_db("ledger-insert");
    load_dump(&db, DEV_0_1_14).await;
    let location = db.to_string_lossy().into_owned();
    let migrations = SqliteMigrations::open(&location).await.expect("open");
    Runner::new(&migrations, RELEASE)
        .migrate_through(MigrationId::Baseline)
        .await
        .expect("migration 1");
    migrations.close().await;
    let pool = plain_pool(&db).await;
    let (account, user): (String, String) =
        sqlx::query_as("SELECT account_id, user_name FROM iam_users LIMIT 1")
            .fetch_one(&pool)
            .await
            .expect("an IAM user");
    sqlx::query(
        "INSERT INTO iam_policies (account_id, principal_type, principal_name, policy_name, \
         policy_document) VALUES (?, 'user', ?, 'tagged', ?)",
    )
    .bind(&account)
    .bind(&user)
    .bind(
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"dynamodb:ResourceTag/env":"prod"}}}]}"#,
    )
    .execute(&pool)
    .await
    .expect("policy");
    pool.close().await;
    let document = "SELECT policy_document FROM iam_policies WHERE policy_name = 'tagged'";
    let before = scalar(&db, document).await;

    let migrations = SqliteMigrations::open(&location).await.expect("open");
    migrations
        .fail_insert_row
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let result = Runner::new(&migrations, RELEASE).migrate().await;
    let result = migrations.discard_unused_copy(result);
    migrations.close().await;
    let err = result.expect_err("the insert fails");
    assert!(err.to_string().contains("record migration 2"), "{err}");
    assert_eq!(
        scalar(&db, document).await,
        before,
        "the body's rewrite is left"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM extenddb_migrations").await,
        1
    );
    let CatalogStatus::Behind { pending, .. } = status(&db).await else {
        panic!("migration 2 is pending");
    };
    assert_eq!(
        pending[0].id,
        MigrationId::ResourceTagConditionKeys.number()
    );
}
