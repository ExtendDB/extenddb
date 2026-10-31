// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 5, `backup_catalog_index`, SQLite: the catalog index of backups
//! kept in a backup store (`backup_index`), the restore job fields on the
//! restore registrations (`table_restores`), the maintenance claim record
//! (`backup_maintenance`) and the catalog id (`settings.catalog_id`).
//!
//! SQLite has no `ADD COLUMN IF NOT EXISTS`, so the body reads the columns of
//! `table_restores` first and adds each one only when it is missing; the
//! tables and indexes are created `IF NOT EXISTS`; the backfill touches only
//! registrations whose `status` is still NULL; and the catalog id is written
//! only when the row is absent. A second run changes nothing, which makes
//! the migration convergent.
//!
//! The backfill: every registration that exists was written by a release
//! without the job record, so each is a legacy restore. One whose table is
//! not CREATING finished (DONE). One whose table is CREATING is a restore the
//! upgrade's stop interrupted: RUNNING, with a heartbeat at the epoch (not
//! NULL, so the staleness test selects it) and an owner generation no job
//! holds (0), so the first reconciler pass fails it and removes its target.
//!
//! This file's checksum is pinned once the migration is released. It is
//! excluded from rustfmt and clippy so neither can require an edit, and it
//! keeps its own column check instead of calling the crate's helpers, which
//! may change.

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use sqlx::SqlitePool;

use super::SqliteStores;

/// This file, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0005_backup_catalog_index.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0005_backup_catalog_index/sqlite/1";

/// The columns the job record adds to `table_restores`, with their SQLite
/// declarations. None carries NOT NULL: `ADD COLUMN` on a populated table
/// cannot, and the application writes every one of them.
const RESTORE_JOB_COLUMNS: [(&str, &str); 12] = [
    ("status", "TEXT"),
    ("kind", "TEXT"),
    ("store_kind", "TEXT"),
    ("store_root", "TEXT"),
    ("store_id", "TEXT"),
    ("owner_generation", "INTEGER"),
    ("heartbeat_at", "TEXT"),
    ("spool_host", "TEXT"),
    ("file_index", "INTEGER"),
    ("items_applied", "INTEGER"),
    ("failure_reason", "TEXT"),
    ("completed_at", "TEXT"),
];

/// The statements that need no column check.
const GUARDED_SQL: &str = "\
CREATE TABLE IF NOT EXISTS backup_index (
    account_id TEXT NOT NULL,
    backup_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    backup_arn TEXT NOT NULL,
    backup_name TEXT NOT NULL,
    status TEXT NOT NULL,
    store_kind TEXT NOT NULL,
    store_root TEXT NOT NULL,
    store_id TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    item_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    failure_reason TEXT,
    source_table_details_json TEXT NOT NULL,
    owner_generation INTEGER NOT NULL,
    heartbeat_at TEXT NOT NULL,
    PRIMARY KEY (account_id, backup_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_backup_index_backup_id ON backup_index (backup_id);
CREATE INDEX IF NOT EXISTS idx_backup_index_account_table ON backup_index (account_id, table_name);
CREATE INDEX IF NOT EXISTS idx_backup_index_store_status
    ON backup_index (store_kind, store_root, store_id, status);
CREATE TABLE IF NOT EXISTS backup_maintenance (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    holder TEXT NOT NULL,
    owner_generation INTEGER NOT NULL,
    claimed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    heartbeat_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
";

/// The backfill of the registrations that exist, scoped to rows the job
/// record is still empty on.
const BACKFILL_SQL: &str = "\
UPDATE table_restores SET
    kind = 'legacy',
    owner_generation = 0,
    heartbeat_at = '1970-01-01T00:00:00.000Z',
    status = CASE WHEN (SELECT table_status FROM tables t WHERE t.table_id = table_restores.table_id) = 'CREATING'
                  THEN 'RUNNING' ELSE 'DONE' END,
    completed_at = CASE WHEN (SELECT table_status FROM tables t WHERE t.table_id = table_restores.table_id) = 'CREATING'
                        THEN NULL
                        ELSE (SELECT creation_date_time FROM tables t WHERE t.table_id = table_restores.table_id) END
WHERE status IS NULL;
CREATE INDEX IF NOT EXISTS idx_table_restores_status ON table_restores (status, heartbeat_at);
";

pub(crate) struct BackupCatalogIndex;

#[async_trait]
impl CodeBody<SqliteStores> for BackupCatalogIndex {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &SqliteStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &SqliteStores) -> Result<Done, MigrationError> {
        let pool = &stores.pool;
        sqlx::raw_sql(GUARDED_SQL)
            .execute(pool)
            .await
            .map_err(|e| MigrationError::store("create backup_index and backup_maintenance", e))?;
        let mut notes = Vec::new();
        let present = columns_of(pool, "table_restores").await?;
        for (name, declaration) in RESTORE_JOB_COLUMNS {
            if present.iter().any(|c| c == name) {
                continue;
            }
            sqlx::query(&format!("ALTER TABLE table_restores ADD COLUMN {name} {declaration}"))
                .execute(pool)
                .await
                .map_err(|e| MigrationError::store(&format!("add table_restores.{name}"), e))?;
            notes.push(format!("added table_restores.{name}"));
        }
        let running: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
             WHERE r.status IS NULL AND t.table_status = 'CREATING'",
        )
        .fetch_one(pool)
        .await
        .map_err(|e| MigrationError::store("count interrupted restores", e))?;
        let backfilled: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM table_restores WHERE status IS NULL")
                .fetch_one(pool)
                .await
                .map_err(|e| MigrationError::store("count registrations", e))?;
        sqlx::raw_sql(BACKFILL_SQL)
            .execute(pool)
            .await
            .map_err(|e| MigrationError::store("backfill table_restores", e))?;
        if backfilled > 0 {
            notes.push(format!(
                "backfilled {backfilled} restore registration(s) as legacy restores, {running} of \
                 them RUNNING with a dead owner for the reconciler to remove"
            ));
        }
        let wrote_id = sqlx::query(
            "INSERT INTO settings (key, value) SELECT 'catalog_id', ? \
             WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key = 'catalog_id')",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .execute(pool)
        .await
        .map_err(|e| MigrationError::store("write catalog_id", e))?
        .rows_affected();
        if wrote_id > 0 {
            notes.push("wrote settings.catalog_id".to_owned());
        }
        Ok(Done { notes })
    }
}

/// The column names of `table`, from `pragma_table_info`.
async fn columns_of(pool: &SqlitePool, table: &str) -> Result<Vec<String>, MigrationError> {
    sqlx::query_scalar("SELECT name FROM pragma_table_info(?)")
        .bind(table)
        .fetch_all(pool)
        .await
        .map_err(|e| MigrationError::store(&format!("read the columns of {table}"), e))
}
