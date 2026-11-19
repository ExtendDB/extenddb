// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 1, `baseline`, SQLite: create the v1.0 catalog schema in an
//! empty database and bring every released catalog to it.
//!
//! The body executes `migrations/0001_baseline.sql`, whose statements are all
//! safe to run again, and then the two steps a file cannot make conditional,
//! each only where it is needed. Running the body again on a catalog at a
//! later migration (`--reconverge`) therefore keeps every column and row a
//! later migration added.
//!
//! - `backup_items` is rebuilt with its `id` cursor when the table has no
//!   `id` column. Every released catalog created it without one, and SQLite
//!   cannot add an INTEGER PRIMARY KEY with ALTER TABLE, so the rebuild uses
//!   the documented procedure: a new table, the rows copied with `rowid` as
//!   `id` (numbering them in their stored order), the old table dropped and
//!   the new one renamed. The runner has foreign keys off while a body runs,
//!   so dropping the old table deletes no child rows.
//! - `table_restores.source_table_arn` is added when the column is missing,
//!   as a pre-release build created the table.
//!
//! The rebuild leaves the old table's pages free inside the file until a
//! `VACUUM`, which the note says.
//!
//! The checksum test pins this file together with the SQL file (`SOURCE`)
//! once the migration is released. The module is excluded from rustfmt and
//! clippy so neither can require an edit, and keeps its own small helpers
//! instead of calling the crate's, which may change.

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use sqlx::SqlitePool;

use super::SqliteStores;

/// The file this body executes first.
pub(crate) const FILE: &str = extenddb_storage::file_body!("migrations", "0001_baseline.sql");

/// This module and the file it executes, whose SHA-256 the checksum test
/// pins. The test `migration_1_pins_its_module_and_its_file` checks that it
/// ends with `FILE`.
pub(crate) const SOURCE: &str = concat!(
    include_str!("m0001_baseline.rs"),
    extenddb_storage::file_body!("migrations", "0001_baseline.sql")
);

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0001_baseline/sqlite/1";

/// `backup_items` with its `id` cursor, as the file creates it in an empty
/// database, then the copy, the drop and the rename.
const REBUILD_BACKUP_ITEMS: &str = "\
DROP TABLE IF EXISTS backup_items_new;
CREATE TABLE backup_items_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    backup_arn TEXT NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    pk TEXT NOT NULL,
    sk TEXT,
    item_data TEXT NOT NULL
);
INSERT INTO backup_items_new (id, backup_arn, pk, sk, item_data)
    SELECT rowid, backup_arn, pk, sk, item_data FROM backup_items ORDER BY rowid;
DROP INDEX IF EXISTS idx_backup_items_arn;
DROP TABLE backup_items;
ALTER TABLE backup_items_new RENAME TO backup_items;
CREATE INDEX idx_backup_items_arn ON backup_items (backup_arn);
";

pub(crate) struct Baseline;

#[async_trait]
impl CodeBody<SqliteStores> for Baseline {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &SqliteStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &SqliteStores) -> Result<Done, MigrationError> {
        let pool = &stores.pool;
        sqlx::raw_sql(FILE)
            .execute(pool)
            .await
            .map_err(|e| store_error("migration 1 (0001_baseline.sql)", e))?;
        let mut notes = Vec::new();
        if !has_column(pool, "backup_items", "id").await? {
            let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM backup_items")
                .fetch_one(pool)
                .await
                .map_err(|e| store_error("count backup_items", e))?;
            sqlx::raw_sql(REBUILD_BACKUP_ITEMS)
                .execute(pool)
                .await
                .map_err(|e| store_error("rebuild backup_items", e))?;
            if rows > 0 {
                notes.push(format!(
                    "rebuilt backup_items with an id column, copying {rows} row(s); the old \
                     table's pages stay free inside the database file until a VACUUM"
                ));
            }
        }
        if !has_column(pool, "table_restores", "source_table_arn").await? {
            sqlx::query("ALTER TABLE table_restores ADD COLUMN source_table_arn TEXT")
                .execute(pool)
                .await
                .map_err(|e| store_error("add table_restores.source_table_arn", e))?;
            notes.push(
                "added table_restores.source_table_arn, which a pre-release build left out"
                    .to_owned(),
            );
        }
        Ok(Done { notes })
    }
}

fn store_error(context: &str, e: sqlx::Error) -> MigrationError {
    MigrationError::Store(format!("{context}: {e}"))
}

async fn has_column(pool: &SqlitePool, table: &str, column: &str) -> Result<bool, MigrationError> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pragma_table_info(?) WHERE name = ?)")
        .bind(table)
        .bind(column)
        .fetch_one(pool)
        .await
        .map_err(|e| store_error(&format!("read the columns of {table}"), e))
}
