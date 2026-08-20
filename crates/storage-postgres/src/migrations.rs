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

/// Create the base-table-key index on every existing GSI/LSI table.
///
/// During GSI propagation each index table (`_ddb_<id>`) is looked up back to
/// its base item via `WHERE base_pk = $1 AND base_sk_* = $2`; without a leading
/// `(base_pk, base_sk_*)` index that is a sequential scan. New tables get this
/// index at creation time (see `ddl.rs`); this migration adds it to tables
/// created before the index existed. `CREATE INDEX CONCURRENTLY IF NOT EXISTS`
/// is idempotent and does not block concurrent writes.
async fn ensure_gsi_base_key_indexes(catalog_pool: &PgPool, data_pool: &PgPool) -> OpResult<()> {
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
            serde_json::from_value(ks_json).map_err(|e| OpError::Internal(e.to_string()))?;
        let attr_defs: Vec<AttributeDefinition> =
            serde_json::from_value(ad_json).map_err(|e| OpError::Internal(e.to_string()))?;

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
