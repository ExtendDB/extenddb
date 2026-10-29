// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The SQLite runner: checksums, the expected definitions, the connection
//! protocol, the copy, the entry points and the in-memory case. The lock
//! is in `serve_tests`, beside the server's refusal while a migration
//! holds it.
//! Released catalogs are in `released_tests`, migrations 2 to 4 in
//! `golden_tests`, refusals and rollbacks in `refusal_tests`, and the killed
//! migrator in `crash_tests`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use extenddb_storage::migrations::{
    Body, CatalogStatus, CodeBody, Done, LedgerRow, MigrationBackend, MigrationError, MigrationId,
    Outcome, RELEASED_THROUGH, Runner, Step, StoreBody, StoreKind, StoreRef, check_released,
};
use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;

use super::{SqliteMigrations, SqliteStores, migrate, migration, postcheck};

pub(super) const RELEASE: &str = "1.0.0-test";

/// The last id this binary carries.
pub(super) const LATEST: u32 = MigrationId::LATEST.number();

/// A database path in a directory of its own, removed when dropped, so a
/// failed test leaves nothing behind.
pub(super) struct TempDb(PathBuf);

impl std::ops::Deref for TempDb {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for TempDb {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        if let Some(dir) = self.0.parent() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

pub(super) fn temp_db(name: &str) -> TempDb {
    let dir = std::env::temp_dir().join(format!(
        "extenddb-migrations-{name}-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    TempDb(dir.join("db.sqlite"))
}

/// A plain pool over the file, as a test reads it.
pub(super) async fn plain_pool(db: &Path) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}?mode=rwc", db.display()))
        .await
        .expect("open")
}

/// Execute a released dump into a new file with the mode a server gives it.
pub(super) async fn load_dump(db: &Path, dump: &str) {
    let pool = plain_pool(db).await;
    sqlx::raw_sql(dump).execute(&pool).await.expect("load dump");
    sqlx::raw_sql("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL")
        .execute(&pool)
        .await
        .expect("pragmas");
    pool.close().await;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(db, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
}

pub(super) async fn exec(db: &Path, sql: &str) {
    let pool = plain_pool(db).await;
    sqlx::raw_sql(sql).execute(&pool).await.expect(sql);
    pool.close().await;
}

pub(super) async fn scalar(db: &Path, sql: &str) -> Option<String> {
    let pool = plain_pool(db).await;
    let value: Option<String> = sqlx::query_scalar(sql)
        .fetch_optional(&pool)
        .await
        .expect(sql);
    pool.close().await;
    value
}

pub(super) async fn count(db: &Path, sql: &str) -> i64 {
    let pool = plain_pool(db).await;
    let n: i64 = sqlx::query_scalar(sql).fetch_one(&pool).await.expect(sql);
    pool.close().await;
    n
}

pub(super) async fn version_row(db: &Path) -> Option<String> {
    scalar(
        db,
        "SELECT value FROM settings WHERE key = 'catalog_version'",
    )
    .await
}

pub(super) async fn status(db: &Path) -> CatalogStatus {
    super::catalog_status(&db.to_string_lossy())
        .await
        .expect("status")
}

pub(super) async fn run(
    db: &Path,
) -> Result<extenddb_storage::migrations::RunReport, MigrationError> {
    migrate(&db.to_string_lossy(), RELEASE, Duration::from_secs(10)).await
}

/// Reconverge the database at `db` through the runner.
pub(super) async fn reconverge(
    db: &Path,
) -> Result<extenddb_storage::migrations::RunReport, MigrationError> {
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    let report = Runner::new(&migrations, RELEASE).reconverge().await;
    migrations.close().await;
    report
}

/// Wait for the next second of the wall clock, so a second run takes a copy
/// with a name of its own.
pub(super) async fn next_second() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock");
    tokio::time::sleep(Duration::from_nanos(
        1_000_000_000 - u64::from(now.subsec_nanos()) + 1_000_000,
    ))
    .await;
}

/// Whether a test rewrites its expected file instead of comparing with it:
/// `EXTENDDB_BLESS_EXPECTED=1`, the rule the PostgreSQL tests use too.
pub(super) fn bless() -> bool {
    std::env::var("EXTENDDB_BLESS_EXPECTED").as_deref() == Ok("1")
}

/// Every copy taken beside `db`.
pub(super) fn copies(db: &Path) -> Vec<PathBuf> {
    let name = db.file_name().expect("name").to_string_lossy().into_owned();
    let mut found: Vec<PathBuf> = std::fs::read_dir(db.parent().expect("dir"))
        .expect("dir")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.file_name().is_some_and(|n| {
                n.to_string_lossy()
                    .starts_with(&format!("{name}.pre-migration-"))
            })
        })
        .collect();
    found.sort();
    found
}

pub(super) const DEV_0_1_7: &str =
    include_str!("../../tests/fixtures/released/dev-image-0.1.7.sql");
pub(super) const DEV_0_1_14: &str =
    include_str!("../../tests/fixtures/released/dev-image-0.1.14.sql");
pub(super) const DEV_0_1_14_BACKUP: &str =
    include_str!("../../tests/fixtures/released/dev-image-0.1.14-backup.sql");

#[test]
fn the_released_bodies_match_their_checksums() {
    let lines = check_released("sqlite", migration, super::RELEASED, RELEASED_THROUGH)
        .unwrap_or_else(|e| panic!("{e}"));
    println!("{lines}");
}

/// `file_body!` embeds the file it names under the invoking crate's root and
/// records the file name for the checksum rules, so a file body names its
/// file once; its two-argument form is the same file's text.
#[test]
fn file_body_names_its_file_once() {
    let body: StoreBody<SqliteStores> =
        extenddb_storage::file_body!(StoreKind::Catalog, "migrations", "0001_baseline.sql");
    assert_eq!(body.store, StoreKind::Catalog);
    let Body::File { name, text } = body.body else {
        panic!("file_body! made a code body");
    };
    assert_eq!(name, "0001_baseline.sql");
    assert_eq!(text, include_str!("../../migrations/0001_baseline.sql"));
    assert_eq!(
        extenddb_storage::file_body!("migrations", "0001_baseline.sql"),
        text
    );
}

/// Migration 1's pinned source is its module and the file it executes.
#[test]
fn migration_1_pins_its_module_and_its_file() {
    let source = super::m0001_baseline::SOURCE;
    assert!(source.contains("pub(crate) struct Baseline;"));
    assert!(source.ends_with(include_str!("../../migrations/0001_baseline.sql")));
    assert!(source.ends_with(super::m0001_baseline::FILE));
}

/// The committed expected definitions are what migration 1 creates in an
/// empty database. `EXTENDDB_BLESS_EXPECTED=1` rewrites the file.
#[tokio::test]
async fn the_expected_definitions_are_those_of_a_fresh_baseline() {
    let db = temp_db("expected");
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    Runner::new(&migrations, RELEASE)
        .migrate_through(MigrationId::Baseline)
        .await
        .expect("migration 1");
    let generated = postcheck::expected_file(migrations.pool())
        .await
        .expect("dump");
    migrations.close().await;
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/0001_baseline.expected.json");
    if bless() {
        std::fs::write(&path, &generated).expect("write expected");
    }
    assert_eq!(
        super::BASELINE_EXPECTED,
        generated,
        "regenerate with EXTENDDB_BLESS_EXPECTED=1 cargo test -p extenddb-storage-sqlite \
         the_expected_definitions"
    );
    assert!(generated.contains("\"name\": \"backup_items\""));
}

#[tokio::test]
async fn a_fresh_database_is_current_after_one_run_and_seeds_nothing() {
    let db = temp_db("fresh");
    assert_eq!(status(&db).await, CatalogStatus::Uninitialized);
    assert!(!db.exists(), "the status does not create the file");
    let report = run(&db).await.expect("migrate");
    assert_eq!((report.from, report.to), (None, Some(LATEST)));
    assert!(
        report.notes.is_empty(),
        "an empty database is not copied: {report:?}"
    );
    assert!(copies(&db).is_empty());
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM extenddb_migrations WHERE outcome = 'applied'"
        )
        .await,
        i64::from(LATEST)
    );
    assert_eq!(version_row(&db).await, Some(LATEST.to_string()));
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM settings").await,
        1,
        "only the version row"
    );
    assert!(run(&db).await.expect("again").applied.is_empty());
}

/// A file the runner creates is 0600 before SQLite writes to it, and SQLite
/// gives its WAL and shared-memory files the same mode.
#[cfg(unix)]
#[tokio::test]
async fn a_database_the_runner_creates_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let db = temp_db("private");
    run(&db).await.expect("migrate");
    let mode = |path: &Path| std::fs::metadata(path).expect("file").permissions().mode() & 0o777;
    assert_eq!(mode(&db), 0o600);
    let pool = plain_pool(&db).await;
    sqlx::query("INSERT INTO settings (key, value) VALUES ('probe', '1')")
        .execute(&pool)
        .await
        .expect("write");
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", db.display()));
        assert_eq!(mode(&sidecar), 0o600, "{suffix}");
    }
    pool.close().await;
}

/// A body that rebuilds `tables`, the parent of `indexes`, `stream_shards`,
/// `vector_indexes` and `table_restores`, with the documented procedure.
struct RebuildTables;

/// The rebuild: a copy with the same definition, the rows, the old table
/// dropped, the copy renamed, and the table's index created again.
async fn rebuild_tables_sql(pool: &SqlitePool) -> String {
    let table: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'tables'",
    )
    .fetch_one(pool)
    .await
    .expect("tables");
    let index: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE name = 'idx_tables_pending_transition'",
    )
    .fetch_one(pool)
    .await
    .expect("index");
    let copy = table.replacen("CREATE TABLE tables", "CREATE TABLE tables_new", 1);
    assert_ne!(copy, table);
    format!(
        "{copy}; INSERT INTO tables_new SELECT * FROM tables; DROP TABLE tables; \
         ALTER TABLE tables_new RENAME TO tables; {index};"
    )
}

#[async_trait]
impl CodeBody<SqliteStores> for RebuildTables {
    fn fingerprint(&self) -> &'static str {
        "test/rebuild-tables"
    }
    async fn detect(&self, _: &SqliteStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }
    async fn run(&self, stores: &SqliteStores) -> Result<Done, MigrationError> {
        let sql = rebuild_tables_sql(&stores.pool).await;
        sqlx::raw_sql(&sql)
            .execute(&stores.pool)
            .await
            .map_err(|e| MigrationError::store("rebuild", e))?;
        Ok(Done::default())
    }
}

/// Inserts an index row for a table that does not exist.
pub(super) struct Orphan;

#[async_trait]
impl CodeBody<SqliteStores> for Orphan {
    fn fingerprint(&self) -> &'static str {
        "test/orphan"
    }
    async fn detect(&self, _: &SqliteStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }
    async fn run(&self, stores: &SqliteStores) -> Result<Done, MigrationError> {
        sqlx::query(
            "INSERT INTO indexes (table_id, index_id, index_name, index_type, key_schema, \
             projection) VALUES ('missing', 'i', 'i', 'GSI', '[]', '{}')",
        )
        .execute(&stores.pool)
        .await
        .map_err(|e| MigrationError::store("orphan", e))?;
        Ok(Done::default())
    }
}

pub(super) fn test_row(id: u32) -> Option<LedgerRow> {
    Some(LedgerRow {
        id,
        name: "test".into(),
        kind: extenddb_storage::migrations::Kind::Breaking,
        checksum: "test".into(),
        outcome: Outcome::Applied,
        notes: String::new(),
        applied_at: String::new(),
        applied_by: RELEASE.into(),
    })
}

pub(super) fn catalog_store(db: &Path) -> StoreRef {
    StoreRef {
        kind: StoreKind::Catalog,
        name: db.to_string_lossy().into_owned(),
    }
}

async fn children(db: &Path) -> (i64, i64, i64) {
    (
        count(db, "SELECT COUNT(*) FROM indexes").await,
        count(db, "SELECT COUNT(*) FROM stream_shards").await,
        count(db, "SELECT COUNT(*) FROM stream_records").await,
    )
}

#[tokio::test]
async fn a_rebuild_of_tables_keeps_its_index_shard_and_stream_rows() {
    let db = temp_db("rebuild");
    load_dump(&db, DEV_0_1_14).await;
    let before = children(&db).await;
    assert_eq!(before, (2, 4, 10));
    let migrations = SqliteMigrations::open(&db.to_string_lossy())
        .await
        .expect("open");
    migrations
        .apply(
            &catalog_store(&db),
            MigrationId::IndexTableBaseKey,
            Step::Code(&RebuildTables),
            test_row(LATEST + 5),
            None,
        )
        .await
        .expect("rebuild under the protocol");
    let on: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(migrations.pool())
        .await
        .expect("pragma");
    assert_eq!(on, 1, "foreign keys are on again after the body");
    migrations.close().await;
    assert_eq!(children(&db).await, before);

    // The control: the same rebuild with foreign keys on deletes them.
    let control = temp_db("rebuild-control");
    load_dump(&control, DEV_0_1_14).await;
    let pool = plain_pool(&control).await;
    let sql = rebuild_tables_sql(&pool).await;
    sqlx::raw_sql(&format!("PRAGMA foreign_keys = ON; BEGIN; {sql} COMMIT;"))
        .execute(&pool)
        .await
        .expect("rebuild with foreign keys on");
    pool.close().await;
    assert_eq!(children(&control).await, (0, 0, 0));
}

#[cfg(unix)]
#[tokio::test]
async fn the_copy_has_the_database_mode_and_a_name_no_run_reuses() {
    use std::os::unix::fs::PermissionsExt;
    let db = temp_db("copy");
    load_dump(&db, DEV_0_1_14).await;
    // A mode a plain VACUUM INTO, which creates the file from 0644 less the
    // umask, cannot produce.
    std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o660)).expect("chmod");
    let report = run(&db).await.expect("migrate");
    let found = copies(&db);
    assert_eq!(found.len(), 1, "{found:?}");
    let copy = &found[0];
    let name = copy
        .file_name()
        .expect("name")
        .to_string_lossy()
        .into_owned();
    let stamp = name
        .strip_prefix(&format!("db.sqlite.pre-migration-legacy-to-{LATEST}-"))
        .unwrap_or_else(|| panic!("{name}"));
    assert_eq!(stamp.len(), "20261029T061706Z".len(), "{stamp}");
    assert!(
        stamp.ends_with('Z') && stamp.as_bytes()[8] == b'T',
        "{stamp}"
    );
    assert_eq!(report.notes.len(), 1);
    assert!(
        report.notes[0].contains(&copy.display().to_string()),
        "{report:?}"
    );
    let mode = std::fs::metadata(copy).expect("copy").permissions().mode() & 0o777;
    assert_eq!(mode, 0o660, "the copy has the database's mode");
    assert_eq!(
        version_row(copy).await.as_deref(),
        Some("0.0.3"),
        "the copy is the database before the migration"
    );
}

#[tokio::test]
async fn an_in_memory_database_migrates_on_the_engine_connection() {
    let engine = crate::SqliteEngine::new(":memory:", 1, "us-east-1", 400_000)
        .await
        .expect("engine");
    let migrations =
        SqliteMigrations::on_pool(engine.pool.clone(), ":memory:").expect("one connection");
    let runner = Runner::new(&migrations, RELEASE);
    assert_eq!(
        runner.status().await.expect("status"),
        CatalogStatus::Uninitialized
    );
    let report = runner.migrate().await.expect("migrate");
    assert!(report.notes.is_empty(), "no copy of an in-memory database");
    assert_eq!(
        runner.status().await.expect("status"),
        CatalogStatus::Current { version: LATEST }
    );
    migrations.close().await;
    // The engine's connection is still open, and has foreign keys on.
    let on: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&engine.pool)
        .await
        .expect("engine pool");
    assert_eq!(on, 1);
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM extenddb_migrations")
        .fetch_one(&engine.pool)
        .await
        .expect("ledger on the engine connection");
    assert_eq!(rows, i64::from(LATEST));
    // The file entry points would open a second, empty database.
    let refused = migrate(":memory:", RELEASE, Duration::from_secs(1)).await;
    assert!(
        matches!(refused, Err(MigrationError::Refused(_))),
        "{refused:?}"
    );
    let refused = super::catalog_status(":memory:").await;
    assert!(
        matches!(refused, Err(MigrationError::Refused(_))),
        "{refused:?}"
    );
}

/// A file engine's pool holds several connections, and a body on one of
/// them would run outside the runner's transaction on another.
#[tokio::test]
async fn on_pool_refuses_a_pool_of_several_connections() {
    let db = temp_db("on-pool");
    let engine = crate::SqliteEngine::new(&db.to_string_lossy(), 4, "us-east-1", 400_000)
        .await
        .expect("engine");
    let refused = SqliteMigrations::on_pool(engine.pool.clone(), &db.to_string_lossy());
    assert!(
        matches!(refused, Err(MigrationError::Refused(ref m)) if m.contains("allows 4")),
        "{:?}",
        refused.err()
    );
    engine.pool.close().await;
}
