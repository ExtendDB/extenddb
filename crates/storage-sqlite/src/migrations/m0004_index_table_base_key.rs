// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 4, `index_table_base_key`, SQLite: give every GSI and LSI index
//! table its `idx_base_key_<index_id>` index.
//!
//! Index propagation deletes an item's old index row by its base table key,
//! with no predicate on the index key, and both the index table's PRIMARY KEY
//! and its ordering index lead with the index key, so without this index
//! every propagating write scans the whole index table. CreateTable and
//! UpdateTable have created it with each index table since v0.1.8; index
//! tables created by v0.1.3 to v0.1.7 lack it.
//!
//! Tables are listed from the catalog rows, never by name in the file. The
//! index's columns are the index table's PRIMARY KEY columns after `pk`
//! (`base_pk` and the base sort key columns), read from the table itself, so
//! a damaged attribute definition cannot choose the wrong column. A catalog
//! row whose index table is missing, or whose table is being deleted, is
//! skipped and noted.
//!
//! This file's checksum is pinned once the migration is released. It is
//! excluded from rustfmt and clippy so neither can require an edit, and keeps
//! its own small helpers (`object_exists`, the key column read) instead of
//! calling the crate's, which may change.

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use sqlx::SqlitePool;

use super::SqliteStores;

/// This file, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0004_index_table_base_key.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0004_index_table_base_key/sqlite/1";

pub(crate) struct IndexTableBaseKey;

#[async_trait]
impl CodeBody<SqliteStores> for IndexTableBaseKey {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &SqliteStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &SqliteStores) -> Result<Done, MigrationError> {
        add_base_key_indexes(&stores.pool).await.map(|notes| Done { notes })
    }
}

fn store_error(context: &str, e: sqlx::Error) -> MigrationError {
    MigrationError::Store(format!("{context}: {e}"))
}

async fn object_exists(pool: &SqlitePool, kind: &str, name: &str) -> Result<bool, MigrationError> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?)")
        .bind(kind)
        .bind(name)
        .fetch_one(pool)
        .await
        .map_err(|e| store_error(&format!("look up {kind} {name}"), e))
}

/// Create the missing indexes; returns one note per index created and per
/// catalog row skipped.
async fn add_base_key_indexes(pool: &SqlitePool) -> Result<Vec<String>, MigrationError> {
    let mut notes = Vec::new();
    if !object_exists(pool, "table", "indexes").await? {
        return Ok(notes);
    }
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT t.table_name, t.table_status, i.index_name, i.index_id \
         FROM indexes i JOIN tables t ON t.table_id = i.table_id \
         WHERE i.index_type IN ('GSI', 'LSI') \
         ORDER BY t.table_name, i.index_name",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| store_error("list secondary indexes", e))?;

    for (table_name, table_status, index_name, index_id) in rows {
        let label = format!("{table_name} index {index_name}");
        // The id is interpolated into identifiers, so it must be a UUID.
        if uuid::Uuid::parse_str(&index_id).is_err() {
            notes.push(format!("{label}: skipped, index_id {index_id:?} is not a UUID"));
            continue;
        }
        if table_status == "DELETING" {
            notes.push(format!("{label}: skipped, the table is being deleted"));
            continue;
        }
        let index_table = format!("_ddb_{index_id}");
        if !object_exists(pool, "table", &index_table).await? {
            notes.push(format!("{label}: skipped, its index table {index_table} does not exist"));
            continue;
        }
        let index = format!("idx_base_key_{index_id}");
        if object_exists(pool, "index", &index).await? {
            continue;
        }
        let mut key_columns: Vec<(i64, String)> = sqlx::query_as::<_, (i64, String, String, i64, Option<String>, i64)>(
            &format!("PRAGMA table_info(\"{index_table}\")"),
        )
        .fetch_all(pool)
        .await
        .map_err(|e| store_error(&format!("read the columns of {index_table}"), e))?
        .into_iter()
        .filter(|(.., pk_pos)| *pk_pos > 0)
        .map(|(_, name, _, _, _, pk_pos)| (pk_pos, name))
        .collect();
        key_columns.sort_unstable();
        let base_columns: Vec<String> = key_columns
            .into_iter()
            .map(|(_, name)| name)
            .filter(|name| name.starts_with("base_"))
            .collect();
        if base_columns.first().map(String::as_str) != Some("base_pk") {
            notes.push(format!(
                "{label}: skipped, the PRIMARY KEY of {index_table} has no base_pk column"
            ));
            continue;
        }
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS \"{index}\" ON \"{index_table}\" ({})",
            base_columns.join(", ")
        ))
        .execute(pool)
        .await
        .map_err(|e| store_error(&format!("create {index}"), e))?;
        notes.push(format!("{label}: created {index} on ({})", base_columns.join(", ")));
    }
    Ok(notes)
}
