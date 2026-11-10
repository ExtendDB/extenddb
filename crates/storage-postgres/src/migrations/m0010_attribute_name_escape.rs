// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 10, `attribute_name_escape`, PostgreSQL: rewrite the stored
//! documents that carry attribute names through the escape that lets them
//! hold U+0000.
//!
//! PostgreSQL `jsonb` refuses U+0000, so from this migration on the server
//! writes these documents with U+0000 stored as U+0001 U+0001 and U+0001 as
//! U+0001 U+0002, and reads them back through the inverse
//! (`crate::catalog_json`). Releases before it stored the documents raw. A
//! raw name holding U+0001 followed by U+0001 or U+0002 would read back as
//! another name, a raw `a<U+0001><U+0001>z` as `a<U+0000>z`, and a table
//! keyed on it would no longer find its items. This migration applies the
//! escape once to every stored document that holds U+0001. A raw document
//! cannot hold U+0000, which `jsonb` refused, so applying the escape to it is
//! the whole conversion, and a document without U+0001 is left as it is.
//!
//! The catalog body rewrites `tables.key_schema`,
//! `tables.attribute_definitions`, `indexes.key_schema`, `indexes.projection`,
//! `backups.key_schema`, `backups.attribute_definitions`,
//! `backups.vector_indexes`, `backup_definitions.definition`,
//! `vector_indexes.vector_attribute`, `vector_indexes.search_schema`,
//! `vector_indexes.projection` and `backup_index.source_table_details_json`
//! (the source table of a store-backed backup, which migration 6 adds).
//! The data body rewrites
//! `gsi_pending.index_context`: a server stopped with queued index updates
//! leaves their rows, and the next server applies them with the names in
//! them. The item documents are not rewritten.
//!
//! The rewrite is not repeatable. An escaped U+0001 and a raw U+0001 U+0002
//! are the same bytes, so a second run over a document this release wrote
//! would change its names. Each body runs in the runner's transaction and
//! commits with its ledger row, so a store records the migration exactly
//! when its documents were rewritten, and the migration is not convergent:
//! `--reconverge` skips it. `extenddb migrate --forget 10` on a database this
//! migration ran on, followed by a run, rewrites its documents a second time.
//!
//! This file's checksum is pinned once the migration is released. It is
//! excluded from rustfmt and clippy so neither can require an edit, and keeps
//! its own escape instead of calling the crate's, which may change.

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use serde_json::Value;
use sqlx::{PgPool, Row};

use super::PostgresStores;
use super::connect::sql_error;

/// This file, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0010_attribute_name_escape.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0010_attribute_name_escape/postgres/1";

/// The catalog database's documents that carry attribute names, by table.
pub(crate) const CATALOG_DOCUMENTS: &[(&str, &[&str])] = &[
    ("tables", &["key_schema", "attribute_definitions"]),
    ("indexes", &["key_schema", "projection"]),
    ("backups", &["key_schema", "attribute_definitions", "vector_indexes"]),
    ("backup_definitions", &["definition"]),
    ("vector_indexes", &["vector_attribute", "search_schema", "projection"]),
    ("backup_index", &["source_table_details_json"]),
];

/// The data database's documents that carry attribute names.
pub(crate) const DATA_DOCUMENTS: &[(&str, &[&str])] = &[("gsi_pending", &["index_context"])];

/// The catalog body.
pub(crate) struct CatalogDocuments;

/// The data body.
pub(crate) struct QueuedContexts;

#[async_trait]
impl CodeBody<PostgresStores> for CatalogDocuments {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it: every document has an escaped
    /// form.
    async fn detect(&self, _stores: &PostgresStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &PostgresStores) -> Result<Done, MigrationError> {
        rewrite(&stores.catalog, CATALOG_DOCUMENTS).await.map(|notes| Done { notes })
    }
}

#[async_trait]
impl CodeBody<PostgresStores> for QueuedContexts {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &PostgresStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &PostgresStores) -> Result<Done, MigrationError> {
        rewrite(&stores.data, DATA_DOCUMENTS).await.map(|notes| Done { notes })
    }
}

/// The escaped form of one string, or `None` when it holds neither
/// character.
fn escape_str(s: &str) -> Option<String> {
    if !s.bytes().any(|b| b == 0 || b == 1) {
        return None;
    }
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '\u{0}' => out.push_str("\u{1}\u{1}"),
            '\u{1}' => out.push_str("\u{1}\u{2}"),
            other => out.push(other),
        }
    }
    Some(out)
}

/// The escaped form of a document: every object key and string value.
pub(crate) fn escape_document(v: Value) -> Value {
    match v {
        Value::String(s) => match escape_str(&s) {
            Some(t) => Value::String(t),
            None => Value::String(s),
        },
        Value::Array(items) => Value::Array(items.into_iter().map(escape_document).collect()),
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, val) in map {
                let key = escape_str(&k).unwrap_or(k);
                out.insert(key, escape_document(val));
            }
            Value::Object(out)
        }
        other => other,
    }
}

/// Rewrite every document of `documents` that holds U+0001; one note per
/// column with the number of documents rewritten in it.
///
/// `jsonb::text` writes a U+0001 inside a string or key as the six
/// characters `\u0001`, so the rows are found by that text, with the
/// backslash spelled `chr(92)` to read the same under either setting of
/// `standard_conforming_strings`. A string holding that text literally is
/// found too; its document escapes to itself and is not written. The rows
/// are locked when they are found, and each is updated once, by the `ctid`
/// read then.
async fn rewrite(pool: &PgPool, documents: &[(&str, &[&str])]) -> Result<Vec<String>, MigrationError> {
    let mut notes = Vec::new();
    for (table, columns) in documents {
        let found = columns
            .iter()
            .map(|c| format!("position((chr(92) || 'u0001') IN \"{c}\"::text) > 0"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let selected = columns.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
        let rows = sqlx::query(&format!(
            "SELECT ctid::text AS row_ctid, {selected} FROM \"{table}\" WHERE {found} FOR UPDATE"
        ))
        .fetch_all(pool)
        .await
        .map_err(|e| sql_error(&format!("find the documents to rewrite in {table}"), &e))?;

        let set = columns
            .iter()
            .enumerate()
            .map(|(i, c)| format!("\"{c}\" = ${}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let update = format!(
            "UPDATE \"{table}\" SET {set} WHERE ctid = ${}::tid",
            columns.len() + 1
        );
        let mut rewritten = vec![0usize; columns.len()];
        for row in rows {
            let ctid: String = row
                .try_get("row_ctid")
                .map_err(|e| sql_error(&format!("read a row of {table}"), &e))?;
            let mut values = Vec::with_capacity(columns.len());
            let mut changed = false;
            for (i, column) in columns.iter().enumerate() {
                let stored: Option<Value> = row
                    .try_get(*column)
                    .map_err(|e| sql_error(&format!("read {table}.{column}"), &e))?;
                let escaped = stored.clone().map(escape_document);
                if escaped != stored {
                    rewritten[i] += 1;
                    changed = true;
                }
                values.push(escaped);
            }
            if !changed {
                continue;
            }
            let mut query = sqlx::query(&update);
            for value in values {
                query = query.bind(value);
            }
            query
                .bind(&ctid)
                .execute(pool)
                .await
                .map_err(|e| sql_error(&format!("rewrite a row of {table}"), &e))?;
        }
        for (column, count) in columns.iter().zip(rewritten) {
            if count > 0 {
                let what = if count == 1 { "document" } else { "documents" };
                notes.push(format!("{table}.{column}: {count} {what} rewritten"));
            }
        }
    }
    Ok(notes)
}
