// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB schema migration helpers for catalog and data databases.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::MySqlPool;

/// Embedded catalog migration files, applied in order.
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_schema.sql",
        include_str!("../../storage-tidb/migrations/001_schema.sql"),
    ),
    (
        "002_backup_metadata_fidelity.sql",
        include_str!("../../storage-tidb/migrations/002_backup_metadata_fidelity.sql"),
    ),
    (
        "003_drop_catalog_stream_data.sql",
        include_str!("../../storage-tidb/migrations/003_drop_catalog_stream_data.sql"),
    ),
    (
        "004_control_plane_leases.sql",
        include_str!("../../storage-tidb/migrations/004_control_plane_leases.sql"),
    ),
    (
        "006_native_ttl_mode.sql",
        include_str!("../../storage-tidb/migrations/006_native_ttl_mode.sql"),
    ),
    (
        "007_native_br_backups.sql",
        include_str!("../../storage-tidb/migrations/007_native_br_backups.sql"),
    ),
    (
        "008_native_index_backup_ids.sql",
        include_str!("../../storage-tidb/migrations/008_native_index_backup_ids.sql"),
    ),
    (
        "009_catalog_native_ttl.sql",
        include_str!("../../storage-tidb/migrations/009_catalog_native_ttl.sql"),
    ),
    (
        "010_session_native_ttl.sql",
        include_str!("../../storage-tidb/migrations/010_session_native_ttl.sql"),
    ),
];

/// Run catalog migrations, skipping already-applied ones.
pub(crate) async fn run_catalog_migrations(pool: &MySqlPool) -> OpResult<()> {
    println!("--- Running catalog migrations...");
    for (filename, sql) in CATALOG_MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        run_sql_script(pool, sql, filename).await?;
        record_migration(pool, filename).await?;
    }
    println!("    Migrations applied.");
    Ok(())
}

/// Run data database migrations.
pub(crate) async fn run_data_migrations(pool: &MySqlPool) -> OpResult<()> {
    let sql = include_str!("../../storage-tidb/data_migrations/001_data_schema.sql");

    println!("--- Initializing data database schema...");
    let initialized: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_name = 'stream_shards' AND table_schema = DATABASE())",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check data schema: {e}")))?;

    if initialized {
        println!("    Data schema already initialized.");
    } else {
        run_sql_script(pool, sql, "data migration").await?;
        println!("    Data schema initialized.");
    }
    ensure_stream_shard_sequence(pool).await?;
    ensure_data_table_ttl(pool).await?;
    Ok(())
}

async fn ensure_stream_shard_sequence(pool: &MySqlPool) -> OpResult<()> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
         WHERE table_name = 'stream_shards' AND table_schema = DATABASE() \
           AND column_name = 'next_sequence_number')",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check stream shard sequence column: {e}")))?;

    if !exists {
        sqlx::query(
            "ALTER TABLE stream_shards \
             ADD COLUMN next_sequence_number BIGINT NOT NULL DEFAULT 0",
        )
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Add stream shard sequence column: {e}")))?;
    }

    sqlx::query(
        "UPDATE stream_shards AS ss \
         JOIN ( \
             SELECT shard_id, COALESCE(MAX(CAST(sequence_number AS UNSIGNED)), 0) AS max_seq \
             FROM stream_records \
             GROUP BY shard_id \
         ) AS sr ON sr.shard_id = ss.shard_id \
         SET ss.next_sequence_number = GREATEST(ss.next_sequence_number, sr.max_seq)",
    )
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Backfill stream shard sequence counters: {e}")))?;

    Ok(())
}

async fn ensure_data_table_ttl(pool: &MySqlPool) -> OpResult<()> {
    for statement in [
        "ALTER TABLE stream_records TTL = `created_at` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h'",
        "ALTER TABLE idempotency_tokens TTL = `created_at` + INTERVAL 600 SECOND TTL_JOB_INTERVAL = '10m'",
    ] {
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Configure TiDB TTL: {e}")))?;
    }
    Ok(())
}

async fn run_sql_script(pool: &MySqlPool, sql: &str, label: &str) -> OpResult<()> {
    for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Migration {label} failed: {e}")))?;
    }
    Ok(())
}

/// Check if a table exists in the current database.
pub(crate) async fn table_exists(pool: &MySqlPool, name: &str) -> OpResult<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_name = ? AND table_schema = DATABASE())",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check table exists: {e}")))?;
    Ok(exists)
}

/// Check if a migration has already been applied.
async fn is_migration_applied(pool: &MySqlPool, filename: &str) -> OpResult<bool> {
    if table_exists(pool, "schema_history").await? {
        let applied: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM schema_history WHERE filename = ?)")
                .bind(filename)
                .fetch_one(pool)
                .await
                .map_err(|e| OpError::Internal(format!("Check migration: {e}")))?;
        return Ok(applied.0);
    }
    Ok(false)
}

/// Record a migration in the `schema_history` table.
async fn record_migration(pool: &MySqlPool, filename: &str) -> OpResult<()> {
    if !table_exists(pool, "schema_history").await? {
        return Ok(());
    }
    sqlx::query("INSERT IGNORE INTO schema_history (filename) VALUES (?)")
        .bind(filename)
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Record migration: {e}")))?;
    Ok(())
}
