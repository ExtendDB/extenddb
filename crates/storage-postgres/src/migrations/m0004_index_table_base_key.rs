// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 4, `index_table_base_key`, PostgreSQL: give every GSI and LSI
//! index table a valid `_ddb_<index_id>_base_key_idx`.
//!
//! Index propagation finds an item's old index row by its base table key
//! (`base_pk` and the base sort key columns); without an index leading with
//! those columns that is a scan of the whole index table. CreateTable has
//! created the index with each index table since v0.1.8; index tables created
//! by v0.1.5 and v0.1.6 lack it until a later release's `migrate` built it,
//! and a build that was cancelled left it INVALID.
//!
//! Tables are listed from the catalog rows, never by name in the data
//! database, where vector tables share the `_ddb_` prefix. The index's
//! columns are the index table's PRIMARY KEY columns after `pk`, read from the
//! table itself, so a damaged attribute definition cannot choose the wrong
//! column; the statement is the one CreateTable writes. A catalog row whose
//! table is being deleted, or whose index table is missing, is skipped and
//! noted. The builds run outside a transaction through the index helper, and
//! the ledger row is written only when every index is valid.
//!
//! This file's checksum is pinned once the migration is released. It is
//! excluded from rustfmt and clippy so neither can require an edit.

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};

use super::index_helper::{Built, IndexHelper};
use super::PostgresStores;
use super::connect::sql_error;

/// This file, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0004_index_table_base_key.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0004_index_table_base_key/postgres/1";

pub(crate) struct IndexTableBaseKey;

#[async_trait]
impl CodeBody<PostgresStores> for IndexTableBaseKey {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &PostgresStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &PostgresStores) -> Result<Done, MigrationError> {
        add_base_key_indexes(stores).await.map(|notes| Done { notes })
    }
}

/// Build the missing and invalid indexes; one note per index built and per
/// catalog row skipped.
async fn add_base_key_indexes(stores: &PostgresStores) -> Result<Vec<String>, MigrationError> {
    let mut notes = Vec::new();
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT t.table_name, t.table_status, i.index_name, i.index_id \
         FROM indexes i JOIN tables t ON t.table_id = i.table_id \
         WHERE i.index_type IN ('GSI', 'LSI') \
         ORDER BY t.table_name, i.index_name",
    )
    .fetch_all(&stores.catalog)
    .await
    .map_err(|e| sql_error("list secondary indexes", &e))?;

    let mut helper = IndexHelper::new(stores, &stores.data);
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
        let columns: Vec<String> = sqlx::query_scalar(
            "SELECT a.attname::text \
             FROM pg_index i \
             JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
             WHERE i.indrelid = to_regclass($1) AND i.indisprimary \
             ORDER BY k.ord",
        )
        .bind(format!("public.\"{index_table}\""))
        .fetch_all(&stores.data)
        .await
        .map_err(|e| sql_error(&format!("read the PRIMARY KEY of {index_table}"), &e))?;
        if columns.is_empty() {
            notes.push(format!("{label}: skipped, its index table {index_table} does not exist"));
            continue;
        }
        let base_columns: Vec<String> = columns.into_iter().filter(|c| c.starts_with("base_")).collect();
        if base_columns.first().map(String::as_str) != Some("base_pk") {
            notes.push(format!("{label}: skipped, the PRIMARY KEY of {index_table} has no base_pk column"));
            continue;
        }
        let index = format!("{index_table}_base_key_idx");
        let on = format!("ON \"{index_table}\" ({})", base_columns.join(", "));
        match helper.ensure(&index, &on).await? {
            Built::Existing => {}
            Built::Created => notes.push(format!("{label}: built {index} on ({})", base_columns.join(", "))),
            Built::Rebuilt => notes.push(format!(
                "{label}: rebuilt {index} on ({}), which an earlier build left invalid",
                base_columns.join(", ")
            )),
        }
    }
    helper.finish().await?;
    Ok(notes)
}
