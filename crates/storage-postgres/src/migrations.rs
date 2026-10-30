// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL` schema migration helpers for catalog and data databases.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::PgPool;

/// Embedded catalog migration files, applied in order.
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_schema.sql",
        include_str!("../../storage-postgres/migrations/001_schema.sql"),
    ),
    (
        "002_vector_indexes.sql",
        include_str!("../../storage-postgres/migrations/002_vector_indexes.sql"),
    ),
    (
        "003_backup_definitions.sql",
        include_str!("../../storage-postgres/migrations/003_backup_definitions.sql"),
    ),
    (
        crate::catalog_escape::ESCAPE_CATALOG_MIGRATION,
        include_str!("../../storage-postgres/migrations/004_escape_catalog_control_chars.sql"),
    ),
];

/// The runner's convergence write, executed after the walk over
/// [`CATALOG_MIGRATIONS`]. Same statement shape as the files' own in-file
/// writes, parameterized on the compiled version.
pub(crate) const SET_CATALOG_VERSION_SQL: &str =
    "UPDATE settings SET value = $1 WHERE key = 'catalog_version'";

/// Run catalog migrations, skipping already-applied ones.
pub(crate) async fn run_catalog_migrations(pool: &PgPool) -> OpResult<()> {
    println!("--- Running catalog migrations...");
    for (filename, sql) in CATALOG_MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        // 004 moves the version for a change of stored form; the rows are
        // re-encoded first, each table in its own committed transaction with a
        // progress marker, so a crash before the version write resumes here.
        if *filename == crate::catalog_escape::ESCAPE_CATALOG_MIGRATION {
            crate::catalog_escape::escape_legacy_catalog_control_chars(pool).await?;
        }
        sqlx::raw_sql(sql)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Migration {filename} failed: {e}")))?;
        // TODO(#221): applying this SQL and recording it are separate commits.
        // A crash here can leave a migration applied but unrecorded. Catalog 001
        // is normally shielded from replay by its version write, data 001 has an
        // adoption guard, and data 002 is repeatable, but those are narrow
        // recovery properties: catalog 001 is not idempotent and replaying data
        // 003 drops the token table. The sqlx adoption must remove the files'
        // own BEGIN/COMMIT and commit each ledger row with its migration before
        // another migration lands.
        record_migration(pool, filename).await?;
    }
    // The runner owns the final version write. Each file still writes the
    // version it introduces, but a replay can re-apply an EARLIER file while a
    // later one stays recorded and skipped: the re-applied file's in-file write
    // then leaves the stored version behind the schema actually present, the
    // startup gate refuses the catalog, and a second migrate finds nothing
    // unrecorded and writes nothing, stranding the deployment. Converging on
    // the compiled version after every completed walk closes that gap. The
    // in-file writes stay until #221 moves version ownership into the runner
    // entirely.
    converge_catalog_version(pool).await?;
    for step in converge_catalog_shapes(pool).await? {
        println!("    Converged catalog shape: {step}.");
    }
    println!("    Migrations applied.");
    Ok(())
}

/// Filenames of [`CATALOG_MIGRATIONS`] not recorded in the catalog's
/// `schema_history`. The ledger keys on the filename, so this is the set
/// [`run_catalog_migrations`] would apply, whatever `catalog_version` says:
/// a catalog stamped with the current version by another migration of the
/// same number (a different branch's `003_*`) still lists this binary's file
/// here.
pub(crate) async fn pending_catalog_migrations(pool: &PgPool) -> OpResult<Vec<String>> {
    let mut pending = Vec::new();
    for (filename, _sql) in CATALOG_MIGRATIONS {
        if !is_migration_applied(pool, filename).await? {
            pending.push((*filename).to_owned());
        }
    }
    Ok(pending)
}

/// One catalog shape step: a description for reports, whether the catalog
/// needs it (a query returning one boolean), and the idempotent SQL that
/// applies it.
struct ShapeStep {
    what: &'static str,
    needed: &'static str,
    apply: &'static str,
    /// Whether `serve` applies this step at startup
    /// ([`converge_catalog_shapes_at_serve`]); every step is applied by
    /// `extenddb migrate --yes`.
    at_serve: bool,
}

/// Shape steps for a catalog whose recorded migrations ran an earlier
/// revision of a file (the ledger keys on the filename, so the walk skips
/// the edited file). Each `needed` query is false on a catalog the current
/// files created, and on one where `table_restores` does not exist yet (the
/// walk creates it in the current shape).
///
/// `003_backup_definitions.sql` was edited in place before release: its
/// earlier revision created `table_restores` without `source_table_arn` and
/// with `DEFAULT NOW()` on `restore_date_time`. DescribeTable selects
/// `source_table_arn` for every table, so on that shape every DescribeTable
/// fails. The added column is nullable (rows written by the earlier build
/// have no value, and are reported without a SourceTableArn); every row this
/// build writes sets it.
///
/// Dropping the default is applied by `migrate --yes` only, never by
/// `serve`: the earlier build inserts `table_restores` rows without
/// `restore_date_time` and relies on the default, so dropping it while an
/// earlier-build server is still running makes that server's restores fail.
/// `migrate` is run with every server stopped. Until then the default is
/// harmless to this build, which always writes the value.
const CATALOG_SHAPE_STEPS: &[ShapeStep] = &[
    ShapeStep {
        what: "add table_restores.source_table_arn",
        needed: "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = 'public' AND table_name = 'table_restores') \
                 AND NOT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = 'table_restores' \
                 AND column_name = 'source_table_arn')",
        apply: "ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS source_table_arn TEXT",
        at_serve: true,
    },
    ShapeStep {
        what: "drop the default of table_restores.restore_date_time",
        needed: "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = 'table_restores' \
                 AND column_name = 'restore_date_time' AND column_default IS NOT NULL)",
        apply: "ALTER TABLE table_restores ALTER COLUMN restore_date_time DROP DEFAULT",
        at_serve: false,
    },
];

/// How long a startup shape step waits for its table lock, in seconds. An
/// `ALTER TABLE` waits behind every open transaction that touched the table,
/// and every later statement on the table waits behind the `ALTER`; with
/// other servers still serving, an unbounded wait would stall them all. On
/// timeout the server refuses to start instead.
pub(crate) const SERVE_SHAPE_LOCK_TIMEOUT_SECS: u64 = 5;

async fn shape_step_needed(pool: &PgPool, step: &ShapeStep) -> OpResult<bool> {
    sqlx::query_scalar(step.needed)
        .fetch_one(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Check catalog shape ({}): {e}", step.what)))
}

/// The [`CATALOG_SHAPE_STEPS`] this catalog needs, by description.
pub(crate) async fn pending_catalog_shapes(pool: &PgPool) -> OpResult<Vec<String>> {
    let mut pending = Vec::new();
    for step in CATALOG_SHAPE_STEPS {
        if shape_step_needed(pool, step).await? {
            pending.push(step.what.to_owned());
        }
    }
    Ok(pending)
}

/// Apply the [`CATALOG_SHAPE_STEPS`] this catalog needs; returns their
/// descriptions. Runs at the end of [`run_catalog_migrations`], that is from
/// `extenddb migrate --yes`. A step that is not needed issues no DDL, so on a
/// current catalog this only reads `information_schema`. Every step is
/// idempotent, so two runs at once are safe.
pub(crate) async fn converge_catalog_shapes(pool: &PgPool) -> OpResult<Vec<String>> {
    let mut applied = Vec::new();
    for step in CATALOG_SHAPE_STEPS {
        if !shape_step_needed(pool, step).await? {
            continue;
        }
        sqlx::query(step.apply)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Catalog shape step ({}): {e}", step.what)))?;
        applied.push(step.what.to_owned());
    }
    Ok(applied)
}

/// `serve`'s startup converge: apply the needed [`CATALOG_SHAPE_STEPS`]
/// marked `at_serve`, each in its own transaction under `SET LOCAL
/// lock_timeout` of [`SERVE_SHAPE_LOCK_TIMEOUT_SECS`]. Returns the applied
/// steps' descriptions. A step that cannot take its lock in time fails with
/// an error saying so and changes nothing.
pub(crate) async fn converge_catalog_shapes_at_serve(pool: &PgPool) -> OpResult<Vec<String>> {
    let mut applied = Vec::new();
    for step in CATALOG_SHAPE_STEPS.iter().filter(|s| s.at_serve) {
        if !shape_step_needed(pool, step).await? {
            continue;
        }
        let fail = |e: sqlx::Error| {
            if crate::data::lock_not_available(&e) {
                OpError::Internal(format!(
                    "Catalog shape step ({}): could not lock the table within \
                     {SERVE_SHAPE_LOCK_TIMEOUT_SECS} s, another session holds it",
                    step.what
                ))
            } else {
                OpError::Internal(format!("Catalog shape step ({}): {e}", step.what))
            }
        };
        let mut tx = pool.begin().await.map_err(fail)?;
        sqlx::query(&format!(
            "SET LOCAL lock_timeout = '{SERVE_SHAPE_LOCK_TIMEOUT_SECS}s'"
        ))
        .execute(&mut *tx)
        .await
        .map_err(fail)?;
        sqlx::query(step.apply)
            .execute(&mut *tx)
            .await
            .map_err(fail)?;
        tx.commit().await.map_err(fail)?;
        applied.push(step.what.to_owned());
    }
    Ok(applied)
}

/// Write the compiled catalog version, but never move the stored version
/// backwards and never overwrite one this binary cannot parse: a catalog
/// stamped by a newer binary (or in a scheme this one does not know) stays
/// stamped, so that binary's startup gate keeps working and this one's keeps
/// refusing it. `extenddb migrate` refuses such a catalog before the walk;
/// this is the runner-level guard for every other caller. The rule is
/// `extenddb_storage::util::check_stored_catalog_version`, shared with SQLite
/// and `extenddb migrate`.
async fn converge_catalog_version(pool: &PgPool) -> OpResult<()> {
    let stored: Option<(String,)> =
        sqlx::query_as("SELECT value FROM settings WHERE key = 'catalog_version'")
            .fetch_optional(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Read catalog_version: {e}")))?;
    extenddb_storage::util::check_stored_catalog_version(
        stored.as_ref().map(|(v,)| v.as_str()),
        crate::CATALOG_VERSION,
    )
    .map_err(OpError::Internal)?;
    sqlx::query(SET_CATALOG_VERSION_SQL)
        .bind(crate::CATALOG_VERSION.to_string())
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Write catalog_version: {e}")))?;
    Ok(())
}

/// Embedded data-database migration files, applied in order. Tracked in the
/// data database's own `schema_history` table (a separate database from the
/// catalog), so `extenddb migrate` applies exactly the pending migrations.
pub(crate) const DATA_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_data_schema.sql",
        include_str!("../../storage-postgres/data_migrations/001_data_schema.sql"),
    ),
    (
        "002_gsi_pending.sql",
        include_str!("../../storage-postgres/data_migrations/002_gsi_pending.sql"),
    ),
    (
        "003_idempotency_account_scope.sql",
        include_str!("../../storage-postgres/data_migrations/003_idempotency_account_scope.sql"),
    ),
    (
        "004_vector_index_state.sql",
        include_str!("../../storage-postgres/data_migrations/004_vector_index_state.sql"),
    ),
];

/// Run data database migrations, skipping already-applied ones.
///
/// Mirrors [`run_catalog_migrations`]: each migration is recorded in
/// `schema_history` and skipped on later runs. The data database has its own
/// ledger because it is a separate database from the catalog.
pub(crate) async fn run_data_migrations(pool: &PgPool) -> OpResult<()> {
    println!("--- Running data migrations...");

    // Ensure the data database has a migration ledger before tracking. (The
    // catalog ledger lives in a different database and cannot be reused here.)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_history (\
             filename TEXT PRIMARY KEY, \
             applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\
         )",
    )
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Create data schema_history: {e}")))?;

    // Adopt a pre-tracking deployment: if 001 was applied by an earlier version
    // (its tables exist) but isn't recorded, record it WITHOUT re-running it.
    // Re-running 001 would execute `setval('stream_seq', ...)` again and could
    // regress the stream sequence on a live database, producing duplicate
    // sequence numbers.
    if !is_migration_applied(pool, "001_data_schema.sql").await?
        && table_exists(pool, "stream_shards").await?
    {
        println!("    Adopting existing 001_data_schema.sql (pre-tracking deployment).");
        record_migration(pool, "001_data_schema.sql").await?;
    }

    for (filename, sql) in DATA_MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        sqlx::raw_sql(sql)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Data migration {filename} failed: {e}")))?;
        // TODO(#221): applying this SQL and recording it are separate commits.
        // A crash here can leave a migration applied but unrecorded. Catalog 001
        // is normally shielded from replay by its version write, data 001 has an
        // adoption guard, and data 002 is repeatable, but those are narrow
        // recovery properties: catalog 001 is not idempotent and replaying data
        // 003 drops the token table. The sqlx adoption must remove the files'
        // own BEGIN/COMMIT and commit each ledger row with its migration before
        // another migration lands.
        record_migration(pool, filename).await?;
    }
    println!("    Data migrations applied.");
    Ok(())
}

/// Programmatic ("code") data migrations, tracked in `schema_history` alongside
/// the SQL migrations. Unlike a static `.sql` file, these enumerate the
/// dynamically-named index tables (`_ddb_<id>`) from the catalog and must run
/// outside a transaction (they use `CREATE INDEX CONCURRENTLY`), so they cannot
/// be expressed as SQL in [`DATA_MIGRATIONS`]. Applied by `extenddb migrate`
/// after the SQL migrations, so the operator controls when the change happens.
pub(crate) const DATA_CODE_MIGRATIONS: &[&str] = &["003_gsi_base_key_index"];

/// Run programmatic data migrations, skipping already-applied ones.
///
/// Needs the catalog pool (to enumerate index tables and their base key schema)
/// and the data pool (where the `_ddb_*` tables and the `schema_history` ledger
/// live). Each step is recorded in `schema_history` and skipped on later runs,
/// exactly like the SQL migrations.
pub(crate) async fn run_data_code_migrations(
    catalog_pool: &PgPool,
    data_pool: &PgPool,
) -> OpResult<()> {
    for name in DATA_CODE_MIGRATIONS {
        if is_migration_applied(data_pool, name).await? {
            println!("    {name} — already applied, skipping.");
            continue;
        }
        println!("    Applying {name}...");
        match *name {
            "003_gsi_base_key_index" => {
                ensure_gsi_base_key_indexes(catalog_pool, data_pool).await?;
            }
            other => {
                return Err(OpError::Internal(format!(
                    "Unknown data code migration: {other}"
                )));
            }
        }
        record_migration(data_pool, name).await?;
    }
    Ok(())
}

/// Bring a fresh catalog database and a fresh data database (the same pool
/// twice on the shared layout) to the current schema the way `extenddb
/// migrate --yes` does: every catalog migration, every data migration, then
/// the data code migrations, each recorded in its ledger. For test harnesses
/// in this crate's own `tests/` directory, which otherwise copy the file
/// lists and go stale when a migration is added here.
///
/// # Errors
///
/// Returns the first migration's error, formatted.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub async fn apply_all_migrations(catalog: &PgPool, data: &PgPool) -> Result<(), String> {
    run_catalog_migrations(catalog)
        .await
        .map_err(|e| format!("{e:?}"))?;
    run_data_migrations(data)
        .await
        .map_err(|e| format!("{e:?}"))?;
    run_data_code_migrations(catalog, data)
        .await
        .map_err(|e| format!("{e:?}"))
}

/// Create the base-table-key index on every existing GSI/LSI table.
///
/// During GSI propagation each index table (`_ddb_<id>`) is looked up back to
/// its base item via `WHERE base_pk = $1 AND base_sk_* = $2`; without a leading
/// `(base_pk, base_sk_*)` index that is a sequential scan. New tables get this
/// index at creation time (see `ddl.rs`); this migration adds it to tables
/// created before the index existed. `CREATE INDEX CONCURRENTLY IF NOT EXISTS`
/// is idempotent and does not block concurrent writes.
async fn ensure_gsi_base_key_indexes(catalog_pool: &PgPool, data_pool: &PgPool) -> OpResult<()> {
    use crate::catalog_json::from_catalog_json;
    use extenddb_core::types::{AttributeDefinition, KeySchemaElement};
    use extenddb_storage::util::{sk_column, sk_column_n};

    // Enumerate every index and its base table key schema from the catalog.
    let rows: Vec<(String, serde_json::Value, serde_json::Value)> = sqlx::query_as(
        "SELECT i.index_id, t.key_schema, t.attribute_definitions \
         FROM indexes i \
         JOIN tables t ON i.table_id = t.table_id",
    )
    .fetch_all(catalog_pool)
    .await
    .map_err(|e| OpError::Internal(format!("Enumerate indexes: {e}")))?;

    for (index_id, ks_json, ad_json) in rows {
        let base_ks: Vec<KeySchemaElement> =
            from_catalog_json(ks_json).map_err(|e| OpError::Internal(e.to_string()))?;
        let attr_defs: Vec<AttributeDefinition> =
            from_catalog_json(ad_json).map_err(|e| OpError::Internal(e.to_string()))?;

        let base_sks = crate::data::all_sort_key_info(&base_ks, &attr_defs);
        let idx_table = crate::data::index_table_name(&index_id);
        let idx_name = format!("_ddb_{index_id}_base_key_idx");

        let mut base_key_cols = vec!["base_pk".to_owned()];
        for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
            let col = if i == 0 {
                format!("base_{}", sk_column(sk_type))
            } else {
                format!("base_{}", sk_column_n(i, sk_type))
            };
            base_key_cols.push(col);
        }

        // CONCURRENTLY cannot run inside a transaction, so execute on the pool.
        let sql = format!(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"{}\" ON {} ({})",
            idx_name,
            idx_table,
            base_key_cols.join(", ")
        );
        sqlx::query(&sql)
            .execute(data_pool)
            .await
            .map_err(|e| OpError::Internal(format!("Base key index on {idx_table}: {e}")))?;
    }
    Ok(())
}

/// Check if a table exists in the public schema.
pub(crate) async fn table_exists(pool: &PgPool, name: &str) -> OpResult<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_name = $1 AND table_schema = 'public')",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check table exists: {e}")))?;
    Ok(exists)
}

/// Filenames of [`DATA_MIGRATIONS`] not yet applied to this data database.
///
/// Mirrors the apply logic in [`run_data_migrations`] without executing
/// anything, so callers (e.g. `extenddb migrate`) can report and gate on
/// pending work. A pre-tracking baseline (`001_data_schema.sql` whose tables
/// already exist but isn't recorded) is treated as already applied: it will be
/// adopted — recorded without re-running — not applied, so it is not reported
/// as pending.
pub(crate) async fn pending_data_migrations(pool: &PgPool) -> OpResult<Vec<String>> {
    let has_history = table_exists(pool, "schema_history").await?;
    // Pre-tracking deployment: 001 ran under an earlier version (its tables
    // exist) but was never recorded. It is adopted, not re-run.
    let adopts_baseline = !has_history && table_exists(pool, "stream_shards").await?;

    let mut pending = Vec::new();
    for (filename, _sql) in DATA_MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            continue;
        }
        if *filename == "001_data_schema.sql" && adopts_baseline {
            continue;
        }
        pending.push((*filename).to_owned());
    }
    // Code migrations are tracked in the same data-database ledger.
    for name in DATA_CODE_MIGRATIONS {
        if !is_migration_applied(pool, name).await? {
            pending.push((*name).to_owned());
        }
    }
    Ok(pending)
}

/// Check if a migration has already been applied.
pub(crate) async fn is_migration_applied(pool: &PgPool, filename: &str) -> OpResult<bool> {
    if table_exists(pool, "schema_history").await? {
        let applied: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM schema_history WHERE filename = $1)")
                .bind(filename)
                .fetch_one(pool)
                .await
                .map_err(|e| OpError::Internal(format!("Check migration: {e}")))?;
        return Ok(applied.0);
    }
    Ok(false)
}

/// Record a migration in the `schema_history` table.
async fn record_migration(pool: &PgPool, filename: &str) -> OpResult<()> {
    if !table_exists(pool, "schema_history").await? {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO schema_history (filename) VALUES ($1) ON CONFLICT (filename) DO NOTHING",
    )
    .bind(filename)
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Record migration: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::CATALOG_MIGRATIONS;
    use crate::CATALOG_VERSION;

    /// The catalog version and the migration list must move together.
    ///
    /// A migration that creates its schema without moving the version leaves a
    /// deployment the binary refuses to serve; moving the version without a
    /// migration leaves one that cannot reach it. Both are caught today only by a
    /// test that needs a live PostgreSQL and a built binary, so this is the
    /// tripwire that fires in an ordinary `cargo test`: adding a migration file
    /// breaks the count, which forces a decision about the version.
    #[test]
    fn the_migration_count_and_the_catalog_version_agree() {
        assert_eq!(
            CATALOG_MIGRATIONS.len(),
            4,
            "a catalog migration was added or removed; update CATALOG_VERSION and this count"
        );
        assert_eq!(CATALOG_VERSION.to_string(), "0.0.5");
    }

    /// The version the binary expects must be the version the schema writes.
    ///
    /// These two live in different languages and different files, so nothing but
    /// a check like this ties them together. Without it a version bump that
    /// forgets the SQL side produces a deployment that migrates "successfully"
    /// and then refuses to start.
    #[test]
    fn the_last_migration_writes_the_expected_catalog_version() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .last()
            .expect("there is at least one catalog migration");
        let expected = format!("'{}'", CATALOG_VERSION);
        assert!(
            sql.contains("catalog_version") && sql.contains(&expected),
            "{filename} must set catalog_version to {expected}"
        );
    }

    /// Each migration is registered under the filename it is stored as.
    ///
    /// The ledger keys on this string, so a mismatch between the registered name
    /// and the file would record one name and look for another, and the migration
    /// would be applied again on every run.
    #[test]
    fn every_migration_is_registered_under_a_sql_filename_with_its_own_contents() {
        for (filename, sql) in CATALOG_MIGRATIONS {
            assert!(filename.ends_with(".sql"), "{filename}");
            assert!(!sql.trim().is_empty(), "{filename} is empty");
        }
        // The failure this guards is a copy-pasted `include_str!` that points one
        // entry at another file's bytes: the ledger would then record one name
        // while the SQL of another ran, and the missed migration would be applied
        // again on every upgrade. Two entries sharing contents is what that looks
        // like, so distinctness is the assertion that delivers the rationale.
        for (i, (left_name, left_sql)) in CATALOG_MIGRATIONS.iter().enumerate() {
            for (right_name, right_sql) in &CATALOG_MIGRATIONS[i + 1..] {
                assert_ne!(
                    left_sql, right_sql,
                    "{left_name} and {right_name} embed identical SQL; check their include_str! paths"
                );
            }
        }
    }

    /// The fail-closed catalog guard (`converge_catalog_version`, via
    /// `check_stored_catalog_version`) against a live catalog: a stored
    /// version this binary cannot parse, or a newer one, is refused and left
    /// exactly as stored; an older one is moved up and an equal one kept.
    /// Needs `EXTENDDB_TEST_PG_CONNECTION_STRING`; reports a skip without it.
    #[tokio::test]
    async fn converge_refuses_an_unreadable_or_newer_catalog_version_and_leaves_it() {
        let Some(base) = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING")
            .ok()
            .filter(|s| !s.trim().is_empty())
        else {
            eprintln!(
                "SKIP converge_refuses_an_unreadable_or_newer_catalog_version: no PostgreSQL"
            );
            return;
        };
        let base = base.trim_end_matches('/').to_owned();
        let name = format!("eddb_conv_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{base}/postgres"))
            .await
            .expect("admin");
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&admin)
            .await
            .expect("create scratch");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{base}/{name}"))
            .await
            .expect("scratch");
        sqlx::query("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            .execute(&pool)
            .await
            .expect("settings");

        let compiled = CATALOG_VERSION.to_string();
        // (stored, expect Ok, stored value afterwards)
        let cases: [(Option<&str>, bool, Option<&str>); 5] = [
            (Some("garbage"), false, Some("garbage")),
            (Some(""), false, Some("")),
            (Some("9.9.9"), false, Some("9.9.9")),
            (Some("0.0.3"), true, Some(compiled.as_str())),
            (Some(compiled.as_str()), true, Some(compiled.as_str())),
        ];
        let mut outcomes = Vec::new();
        for (stored, _, _) in &cases {
            sqlx::query("DELETE FROM settings")
                .execute(&pool)
                .await
                .expect("clear");
            if let Some(v) = stored {
                sqlx::query("INSERT INTO settings (key, value) VALUES ('catalog_version', $1)")
                    .bind(v)
                    .execute(&pool)
                    .await
                    .expect("seed");
            }
            let result = super::converge_catalog_version(&pool).await;
            let after: Option<String> =
                sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_version'")
                    .fetch_optional(&pool)
                    .await
                    .expect("read back");
            outcomes.push((result.map_err(|e| format!("{e:?}")), after));
        }
        // Absent: proceeds, and (the write is an UPDATE) stays absent.
        sqlx::query("DELETE FROM settings")
            .execute(&pool)
            .await
            .expect("clear");
        let absent = super::converge_catalog_version(&pool).await.is_ok();

        pool.close().await;
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
            .execute(&admin)
            .await;
        admin.close().await;

        for ((stored, ok, want_after), (result, after)) in cases.iter().zip(&outcomes) {
            assert_eq!(result.is_ok(), *ok, "stored {stored:?}: {result:?}");
            assert_eq!(
                after.as_deref(),
                *want_after,
                "stored {stored:?} afterwards"
            );
        }
        let garbage = outcomes[0].0.as_ref().expect_err("garbage refused");
        assert!(garbage.contains("refusing to overwrite"), "{garbage}");
        let newer = outcomes[2].0.as_ref().expect_err("newer refused");
        assert!(newer.contains("newer than this binary"), "{newer}");
        assert!(absent, "an absent version proceeds");
    }
}
