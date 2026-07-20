// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL` schema migration helpers for catalog and data databases.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::PgPool;

/// Embedded catalog migration files, applied in order.
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[(
    "001_schema.sql",
    include_str!("../../storage-postgres/migrations/001_schema.sql"),
)];

/// Run catalog migrations, skipping already-applied ones.
pub(crate) async fn run_catalog_migrations(pool: &PgPool) -> OpResult<()> {
    println!("--- Running catalog migrations...");
    for (filename, sql) in CATALOG_MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        sqlx::raw_sql(sql)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Migration {filename} failed: {e}")))?;
        record_migration(pool, filename).await?;
    }
    println!("    Migrations applied.");
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
        record_migration(pool, filename).await?;
    }
    println!("    Data migrations applied.");
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
    Ok(pending)
}

/// Check if a migration has already been applied.
async fn is_migration_applied(pool: &PgPool, filename: &str) -> OpResult<bool> {
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
