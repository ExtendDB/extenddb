// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite schema migration helpers.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::SqlitePool;

/// Embedded catalog/data migration files, applied in order.
pub(crate) const MIGRATIONS: &[(&str, &str)] = &[(
    "001_schema.sql",
    include_str!("../migrations/001_schema.sql"),
)];

/// Check if a table exists in the SQLite database.
pub(crate) async fn table_exists(pool: &SqlitePool, name: &str) -> OpResult<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check table exists: {e}")))?;
    Ok(exists)
}

/// Run all migrations, skipping already-applied ones.
pub(crate) async fn run_migrations(pool: &SqlitePool) -> OpResult<()> {
    // Create schema_history table if it doesn't exist yet (needed for tracking).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_history (
            filename TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        )",
    )
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Create schema_history: {e}")))?;

    println!("--- Running migrations...");
    for (filename, sql) in MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        // Execute statements one by one (SQLite doesn't support multi-statement raw_sql well).
        for stmt in split_sql(sql) {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            sqlx::query(stmt)
                .execute(pool)
                .await
                .map_err(|e| OpError::Internal(format!("Migration {filename} failed: {e}\nSQL: {stmt}")))?;
        }
        record_migration(pool, filename).await?;
    }
    println!("    Migrations applied.");
    Ok(())
}

/// Check if a migration has already been applied.
async fn is_migration_applied(pool: &SqlitePool, filename: &str) -> OpResult<bool> {
    let applied: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schema_history WHERE filename = ?)")
            .bind(filename)
            .fetch_one(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Check migration: {e}")))?;
    Ok(applied)
}

/// Record a migration as applied.
async fn record_migration(pool: &SqlitePool, filename: &str) -> OpResult<()> {
    sqlx::query(
        "INSERT OR IGNORE INTO schema_history (filename) VALUES (?)",
    )
    .bind(filename)
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Record migration: {e}")))?;
    Ok(())
}

/// Split a SQL script into individual statements by semicolon.
/// Handles basic cases — does not parse strings or comments.
fn split_sql(sql: &str) -> Vec<&str> {
    sql.split(';').collect()
}
