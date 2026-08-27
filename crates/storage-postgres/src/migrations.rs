// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL schema migrations for the catalog and data databases.
//!
//! SQL-file migrations use sqlx's built-in migrator (`sqlx::migrate!`), which
//! embeds the files at compile time, applies each in its own transaction
//! together with its ledger row, and records a checksum of the file bytes in a
//! `_sqlx_migrations` table. Editing a migration after it has been applied is
//! a hard error rather than a silent no-op (ADR-0003).
//!
//! Programmatic ("code") migrations cannot be static SQL: they enumerate
//! dynamically-named tables from the catalog and use DDL that cannot run in a
//! transaction (`CREATE INDEX CONCURRENTLY`). They are Rust code, reviewed and
//! version-controlled, tracked in a small `code_migrations` ledger in the data
//! database, and applied after the SQL migrations.
//!
//! Migrations run only during `init` and `migrate`, never while serving.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::PgPool;
use sqlx::migrate::Migrator;

use crate::CATALOG_VERSION;

/// Catalog database migrator (files under `crates/storage-postgres/migrations`).
pub(crate) static CATALOG_MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Data database migrator (files under `crates/storage-postgres/data_migrations`).
pub(crate) static DATA_MIGRATOR: Migrator = sqlx::migrate!("./data_migrations");

/// Apply catalog migrations, then record the catalog version.
///
/// sqlx runs each pending migration in its own transaction, commits it
/// atomically with its ledger row, and skips those already recorded, so this is
/// idempotent. The version write is a separate step after the migrations
/// commit: sqlx knows nothing about our semver. It is intentionally not atomic
/// with the migration (ADR-0003); re-running `migrate` repairs a version left
/// stale by a crash between the two.
pub(crate) async fn run_catalog_migrations(pool: &PgPool) -> OpResult<()> {
    println!("--- Running catalog migrations...");
    CATALOG_MIGRATOR
        .run(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Catalog migration failed: {e}")))?;
    write_catalog_version(pool).await?;
    println!("    Catalog schema at version {CATALOG_VERSION}.");
    Ok(())
}

/// Apply data-database SQL migrations.
///
/// The data database is tracked by its own `_sqlx_migrations` table and has no
/// separate version. `migrate`, not just `init`, runs this so existing
/// deployments pick up data-schema changes.
pub(crate) async fn run_data_migrations(pool: &PgPool) -> OpResult<()> {
    println!("--- Running data migrations...");
    DATA_MIGRATOR
        .run(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Data migration failed: {e}")))?;
    println!("    Data migrations complete.");
    Ok(())
}

/// Write the compiled-in catalog version into the `settings` table.
async fn write_catalog_version(pool: &PgPool) -> OpResult<()> {
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('catalog_version', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(CATALOG_VERSION.to_string())
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Record catalog version: {e}")))?;
    Ok(())
}

/// Programmatic ("code") data migrations, tracked in the data database's
/// `code_migrations` ledger. Unlike a static `.sql` file, these enumerate the
/// dynamically-named index tables (`_ddb_<id>`) from the catalog and must run
/// outside a transaction (they use `CREATE INDEX CONCURRENTLY`), so they cannot
/// be expressed as sqlx migration files. Applied by `extenddb migrate` after
/// the SQL migrations, so the operator controls when the change happens.
pub(crate) const DATA_CODE_MIGRATIONS: &[&str] = &["003_gsi_base_key_index"];

/// Run programmatic data migrations, skipping already-applied ones.
///
/// Needs the catalog pool (to enumerate index tables and their base key schema)
/// and the data pool (where the `_ddb_*` tables and the `code_migrations`
/// ledger live). Each step is recorded in `code_migrations` and skipped on
/// later runs. sqlx cannot track these (they are not files with checksums);
/// the code itself is reviewed and version-controlled instead.
pub(crate) async fn run_data_code_migrations(
    catalog_pool: &PgPool,
    data_pool: &PgPool,
) -> OpResult<()> {
    ensure_code_migrations_ledger(data_pool).await?;
    for name in DATA_CODE_MIGRATIONS {
        if is_code_migration_applied(data_pool, name).await? {
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
        record_code_migration(data_pool, name).await?;
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

/// Names of data migrations (SQL and code) not yet applied to this data
/// database.
///
/// Compares the embedded SQL migrations against the versions recorded in the
/// data database's `_sqlx_migrations` table (successful rows only, so a dirty
/// migration is reported pending), and the code migrations against the
/// `code_migrations` ledger, without applying anything, so `migrate` can report
/// pending work and gate on it. Independent of the catalog version.
pub(crate) async fn pending_data_migrations(pool: &PgPool) -> OpResult<Vec<String>> {
    let applied: std::collections::HashSet<i64> = if table_exists(pool, "_sqlx_migrations").await? {
        sqlx::query_scalar::<_, i64>("SELECT version FROM _sqlx_migrations WHERE success")
            .fetch_all(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Read _sqlx_migrations: {e}")))?
            .into_iter()
            .collect()
    } else {
        std::collections::HashSet::new()
    };

    let mut pending = Vec::new();
    for migration in DATA_MIGRATOR.iter() {
        if !applied.contains(&migration.version) {
            pending.push(format!(
                "{:03}_{}.sql",
                migration.version,
                migration.description.replace(' ', "_")
            ));
        }
    }
    for name in DATA_CODE_MIGRATIONS {
        if !is_code_migration_applied(pool, name).await? {
            pending.push((*name).to_owned());
        }
    }
    Ok(pending)
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

/// Create the `code_migrations` ledger if it does not exist.
async fn ensure_code_migrations_ledger(pool: &PgPool) -> OpResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS code_migrations (\
             name TEXT PRIMARY KEY, \
             applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\
         )",
    )
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Create code_migrations ledger: {e}")))?;
    Ok(())
}

/// Check if a code migration has already been applied.
async fn is_code_migration_applied(pool: &PgPool, name: &str) -> OpResult<bool> {
    if !table_exists(pool, "code_migrations").await? {
        return Ok(false);
    }
    let applied: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM code_migrations WHERE name = $1)")
            .bind(name)
            .fetch_one(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Check code migration: {e}")))?;
    Ok(applied)
}

/// Record a code migration in the `code_migrations` ledger.
async fn record_code_migration(pool: &PgPool, name: &str) -> OpResult<()> {
    sqlx::query("INSERT INTO code_migrations (name) VALUES ($1) ON CONFLICT (name) DO NOTHING")
        .bind(name)
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Record code migration: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tripwire (ADR-0003): pin the embedded migration counts and the catalog
    // version. Adding a catalog migration without bumping CATALOG_VERSION fails
    // here, forcing a deliberate version decision. The data database has no
    // version, so its counts are pinned only to make a new data or code
    // migration a conscious change.
    const EXPECTED_CATALOG_MIGRATIONS: usize = 1;
    const EXPECTED_DATA_MIGRATIONS: usize = 3;
    const EXPECTED_DATA_CODE_MIGRATIONS: usize = 1;
    const EXPECTED_CATALOG_VERSION: &str = "0.1.0";

    // Checksum lockfile (ADR-0003 CI net): pin the sqlx checksum (SHA-384 of
    // the file bytes) of every shipped migration. Editing an already-applied
    // migration changes its checksum, so this fails `cargo test` in the PR
    // runner: it catches in CI what sqlx otherwise only catches at runtime
    // against a live database. `.gitattributes` pins *.sql to LF, so the bytes
    // (and these checksums) are stable across platforms. To add a migration,
    // append its (version, checksum) below using the value the assertion prints.
    // The values are sqlx's SHA-384 migration checksums; a future sqlx bump that
    // changed the hash algorithm would require regenerating them.
    const CATALOG_CHECKSUMS: &[(i64, &str)] = &[(
        1,
        "e54990978c4a080c0744cd3f90fde86d88b7c9a871dfefe2455cb843deac1a752076125d7811e2559e6faa18e7983460",
    )];
    const DATA_CHECKSUMS: &[(i64, &str)] = &[
        (
            1,
            "36fb3dc917923ca6f34fda2157999ad132996a937ee9893f91c260f8c09276b237c5279f750ad94af97bd6b1fd966a8f",
        ),
        (
            2,
            "8da1bcb8c9864258b0c12711b5df5090d0c1caa52a0102466a8ca94084ac56c9db385a650b34fe29a45dc942318a0100",
        ),
        (
            3,
            "f5a73cbb1bac5e979acb0952973f9d2491a44e80b4eaee175634b35e462bbd3c3090b664fc336ccd473404616bdb81aa",
        ),
    ];

    fn assert_checksums(migrator: &Migrator, expected: &[(i64, &str)]) {
        let actual: Vec<(i64, String)> = migrator
            .iter()
            .map(|m| {
                let hex = m
                    .checksum
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                (m.version, hex)
            })
            .collect();
        let expected: Vec<(i64, String)> = expected
            .iter()
            .map(|(v, h)| (*v, (*h).to_owned()))
            .collect();
        assert_eq!(
            actual, expected,
            "migration checksums changed: an already-shipped migration was edited \
             (forbidden), or one was added/removed. If the change is intentional and \
             the file has not shipped, update the pinned checksum(s) with the actual \
             values shown here."
        );
    }

    #[test]
    fn migration_checksums_are_pinned() {
        assert_checksums(&CATALOG_MIGRATOR, CATALOG_CHECKSUMS);
        assert_checksums(&DATA_MIGRATOR, DATA_CHECKSUMS);
    }

    #[test]
    fn migration_counts_and_catalog_version_are_pinned() {
        assert_eq!(
            CATALOG_MIGRATOR.iter().count(),
            EXPECTED_CATALOG_MIGRATIONS,
            "catalog migration count changed: bump CATALOG_VERSION and update EXPECTED_CATALOG_MIGRATIONS",
        );
        assert_eq!(
            DATA_MIGRATOR.iter().count(),
            EXPECTED_DATA_MIGRATIONS,
            "data migration count changed: update EXPECTED_DATA_MIGRATIONS",
        );
        assert_eq!(
            DATA_CODE_MIGRATIONS.len(),
            EXPECTED_DATA_CODE_MIGRATIONS,
            "data code migration count changed: update EXPECTED_DATA_CODE_MIGRATIONS",
        );
        assert_eq!(
            CATALOG_VERSION.to_string(),
            EXPECTED_CATALOG_VERSION,
            "CATALOG_VERSION changed: update EXPECTED_CATALOG_VERSION",
        );
    }
}
