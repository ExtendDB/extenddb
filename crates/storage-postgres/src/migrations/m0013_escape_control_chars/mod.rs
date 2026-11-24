// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 13, `escape_control_chars`, PostgreSQL: re-encode the item
//! documents an earlier release wrote, whose strings are stored raw.
//!
//! This release stores every string of an item document (names, map keys,
//! values) through an escape (U+0000 as U+0001 U+0001, U+0001 as U+0001
//! U+0002), so that U+0000, which DynamoDB accepts anywhere a string appears,
//! can be held in `jsonb`. A document an earlier release wrote holds its
//! strings raw, and a raw U+0001 followed by U+0001 or U+0002 reads back
//! differently under the decoder (a lone U+0001 reads back unchanged).
//! Earlier releases could not store U+0000, so for their documents the escape
//! of the stored text is the re-encode, and a document without U+0001 is left
//! alone. The key columns (`pk`, `sk_s`, `base_pk` and the others) are not
//! rewritten: this release stores a key as earlier releases stored it, and
//! refuses a key value that holds U+0000 (`data::key_text`).
//!
//! Two bodies, one per database. The catalog body rewrites
//! `backup_items.item_data`, which lives in the catalog database; the data
//! body rewrites `item_data` of every data table, index table and vector
//! index table, the item images of `gsi_pending` (`old_item`, `new_item`) and
//! the stream records of `stream_records` and `stream_pending`.
//! `gsi_pending.index_context` is not rewritten: catalog migration 11,
//! `attribute_name_escape`, applies the same escape to it once. Tables are
//! listed from the catalog rows; a row whose table is `DELETING`, or whose
//! physical table is missing, is skipped and noted.
//!
//! The escape is not idempotent: an escaped row also holds U+0001, and a
//! legacy U+0001 U+0002 cannot be told from an escaped U+0001. Each body runs
//! in the runner's transaction and commits with its ledger row, so a run that
//! stops leaves the store as it was, and a store whose row is recorded is
//! never rewritten again; the migration is not convergent. A server of this
//! release refuses a store where the migration is pending, so the bodies see
//! only rows earlier releases wrote, provided those servers are stopped:
//! each body's `detect` stops a run, before the run's first write, while
//! another session holds the database the body rewrites (see [`guard`]). A
//! status read does not look for sessions, so the preview runs beside a
//! server as for every other migration.
//!
//! No index entry and no constraint covers the strings of a document, so a
//! rewrite fails only when the database does, for example on a lost
//! connection or a full disk. The catalog body commits before the data body
//! runs. A data body that fails leaves the catalog store at migration 13 and
//! the data store behind it; neither a server of this release nor one of an
//! earlier release starts against the deployment until `extenddb migrate
//! --yes` runs the data body again.
//!
//! The rewrite escapes through this module's own copy of the escape
//! ([`escape`]). This module's checksum covers `mod.rs`, `escape.rs` and
//! `guard.rs`, and is pinned once the migration is released. They are
//! excluded from rustfmt and clippy so neither can require an edit.

mod escape;
mod guard;

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use sqlx::{PgConnection, PgPool};

use super::PostgresStores;

pub(crate) use guard::other_sessions;
#[cfg(test)]
pub(crate) use guard::{IGNORE_CONNECTIONS_ENV, catalog_has_rows, data_has_rows};

/// The text of every file of this module, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = concat!(
    include_str!("mod.rs"),
    include_str!("escape.rs"),
    include_str!("guard.rs"),
);

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0013_escape_control_chars/postgres/1";

/// Rows read per round trip.
const BATCH: usize = 500;

/// The catalog body: `backup_items`.
pub(crate) struct EscapeBackupItems;

/// The data body: the data, index and vector index tables, the item images
/// of `gsi_pending`, `stream_records` and `stream_pending`.
pub(crate) struct EscapeDataRows;

#[async_trait]
impl CodeBody<PostgresStores> for EscapeBackupItems {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Another session on the catalog database, while `backup_items` holds a
    /// row.
    async fn detect(&self, stores: &PostgresStores) -> Result<Vec<String>, MigrationError> {
        if !stores.checks_sessions || !guard::catalog_has_rows(&stores.catalog).await? {
            return Ok(Vec::new());
        }
        other_sessions(&stores.catalog, "catalog").await
    }

    async fn run(&self, stores: &PostgresStores) -> Result<Done, MigrationError> {
        let mut conn = stores
            .catalog
            .acquire()
            .await
            .map_err(|e| MigrationError::store("reach the catalog database", e))?;
        escape_catalog(&mut conn).await.map(|notes| Done { notes })
    }
}

#[async_trait]
impl CodeBody<PostgresStores> for EscapeDataRows {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Another session on the data database, while the catalog lists a table
    /// or `gsi_pending`, `stream_records` or `stream_pending` holds a row.
    async fn detect(&self, stores: &PostgresStores) -> Result<Vec<String>, MigrationError> {
        if !stores.checks_sessions || !guard::data_has_rows(&stores.catalog, &stores.data).await? {
            return Ok(Vec::new());
        }
        other_sessions(&stores.data, "data").await
    }

    async fn run(&self, stores: &PostgresStores) -> Result<Done, MigrationError> {
        let mut conn = stores
            .data
            .acquire()
            .await
            .map_err(|e| MigrationError::store("reach the data database", e))?;
        escape_data(&stores.catalog, &mut conn).await.map(|notes| Done { notes })
    }
}

/// A failed statement as a migration error: `what`, the database's message,
/// and the SQLSTATE, which the runner reads to run the body again after a
/// lock timeout.
pub(crate) fn db_error(what: &str, e: &sqlx::Error) -> MigrationError {
    let message = format!("{what}: {e}");
    match e.as_database_error().and_then(|d| d.code()) {
        Some(code) => MigrationError::Database {
            message,
            sqlstate: code.into_owned(),
        },
        None => MigrationError::Store(message),
    }
}

/// One physical table's rewrite: the jsonb columns that hold its item
/// documents.
struct EscapeWork {
    /// Bare (unquoted) table name, e.g. `_ddb_<id>` or `gsi_pending`.
    table: String,
    columns: Vec<String>,
    /// The DynamoDB table (and index) the physical table holds, for notes.
    ddb_name: Option<String>,
}

impl EscapeWork {
    /// `<table>`, with the DynamoDB name when known: how notes name a table.
    fn label(&self) -> String {
        match &self.ddb_name {
            Some(ddb) => format!("{} ({ddb})", self.table),
            None => self.table.clone(),
        }
    }
}

/// The jsonb columns of `table` in the public schema among `names`, in column
/// order; none when the table does not exist.
async fn jsonb_columns(
    conn: &mut PgConnection,
    table: &str,
    names: &[&str],
) -> Result<Vec<String>, MigrationError> {
    let names: Vec<String> = names.iter().map(|c| (*c).to_owned()).collect();
    sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 AND data_type = 'jsonb' \
           AND column_name::text = ANY($2) \
         ORDER BY ordinal_position",
    )
    .bind(table)
    .bind(names)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| db_error(&format!("list the jsonb columns of {table}"), &e))
}

/// The rewrite of `table`'s columns among `names`, or `None` when the table
/// has none of them.
async fn table_work(
    conn: &mut PgConnection,
    table: &str,
    names: &[&str],
    ddb_name: Option<String>,
) -> Result<Option<EscapeWork>, MigrationError> {
    let columns = jsonb_columns(conn, table, names).await?;
    Ok((!columns.is_empty()).then(|| EscapeWork {
        table: table.to_owned(),
        columns,
        ddb_name,
    }))
}

/// The re-encoded form of a stored jsonb value, or `None` when re-encoding
/// does not change it. The escape only changes strings and object keys
/// containing U+0000 or U+0001, so a plain document and a document whose
/// strings hold the literal six-character text backslash-u-0001 both come
/// back equal and are skipped.
pub(crate) fn reencoded_json(value: &serde_json::Value) -> Option<serde_json::Value> {
    let escaped = escape::escape_document(value.clone());
    (escaped != *value).then_some(escaped)
}

/// Re-encode the rows of one jsonb column whose stored document contains
/// U+0001.
///
/// The candidate ctids are read once, before any row of this column is
/// touched, so no row is visited twice; an update moves a row (a ctid names a
/// row version), always after its value was read. The values are then read
/// in batches of [`BATCH`] by ctid, so a large table never loads into memory.
async fn escape_jsonb_rows(
    conn: &mut PgConnection,
    quoted_table: &str,
    col: &str,
) -> Result<u64, MigrationError> {
    // '\u0001' (six characters) is how jsonb::text renders a raw U+0001 inside
    // a string or key. It also matches a stored string whose content is a
    // literal backslash followed by u0001 (rendered '\\u0001'), which is
    // harmless: that row re-encodes to itself and is skipped below. jsonb text
    // rendering is why SQL replace() is not used here: it would corrupt that
    // literal-backslash case. The backslash is spelled chr(92) so the
    // predicate reads the same under either value of
    // standard_conforming_strings; a plain '\u0001' literal would become the
    // single byte 0x01 with the setting off and match nothing.
    let select_ctids = format!(
        "SELECT ctid::text FROM {quoted_table} \
         WHERE position((chr(92) || 'u0001') IN \"{col}\"::text) > 0 FOR UPDATE"
    );
    let ctids: Vec<String> = sqlx::query_scalar(&select_ctids)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| db_error(&format!("find the rows to re-encode in {quoted_table}"), &e))?;

    let fetch_batch =
        format!("SELECT ctid::text, \"{col}\" FROM {quoted_table} WHERE ctid = ANY($1::tid[])");
    let update_row = format!("UPDATE {quoted_table} SET \"{col}\" = $1 WHERE ctid = $2::tid");
    let mut changed: u64 = 0;
    for batch in ctids.chunks(BATCH) {
        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(&fetch_batch)
            .bind(batch.to_vec())
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| db_error(&format!("read rows of {quoted_table}"), &e))?;
        for (ctid, value) in rows {
            if let Some(escaped) = reencoded_json(&value) {
                sqlx::query(&update_row)
                    .bind(escaped)
                    .bind(&ctid)
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| db_error(&format!("re-encode a row of {quoted_table}"), &e))?;
                changed += 1;
            }
        }
    }
    Ok(changed)
}

/// Rewrite every table of `work` in order; one note per table with a change.
async fn escape_all(
    conn: &mut PgConnection,
    work: &[EscapeWork],
    notes: &mut Vec<String>,
) -> Result<(), MigrationError> {
    for w in work {
        let quoted = format!("\"{}\"", w.table);
        let mut rewritten: u64 = 0;
        for col in &w.columns {
            rewritten += escape_jsonb_rows(conn, &quoted, col).await?;
        }
        if rewritten > 0 {
            notes.push(format!("{}: {rewritten} row update(s)", w.label()));
        }
    }
    Ok(())
}

/// The catalog body on `conn`, the catalog database in the runner's
/// transaction.
pub(crate) async fn escape_catalog(conn: &mut PgConnection) -> Result<Vec<String>, MigrationError> {
    let mut notes = Vec::new();
    let work: Vec<EscapeWork> = table_work(conn, "backup_items", &["item_data"], None)
        .await?
        .into_iter()
        .collect();
    escape_all(conn, &work, &mut notes).await?;
    Ok(notes)
}

/// The data body on `conn`, the data database in the runner's transaction,
/// with the tables listed from `catalog`.
pub(crate) async fn escape_data(
    catalog: &PgPool,
    conn: &mut PgConnection,
) -> Result<Vec<String>, MigrationError> {
    let mut notes = Vec::new();
    let mut work: Vec<EscapeWork> = Vec::new();

    let tables: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT table_id, table_name, account_id, table_status FROM tables \
         ORDER BY table_id COLLATE \"C\"",
    )
    .fetch_all(catalog)
    .await
    .map_err(|e| db_error("list tables", &e))?;
    let deleting: std::collections::HashSet<&str> = tables
        .iter()
        .filter(|(_, _, _, status)| status == "DELETING")
        .map(|(id, _, _, _)| id.as_str())
        .collect();
    // How notes name the DynamoDB table of a physical table.
    let ddb_tables: std::collections::HashMap<&str, String> = tables
        .iter()
        .map(|(id, name, account, _)| (id.as_str(), format!("table {name:?} of account {account}")))
        .collect();
    let ddb_table = |table_id: &str| {
        ddb_tables
            .get(table_id)
            .cloned()
            .unwrap_or_else(|| format!("table id {table_id}"))
    };

    // Every physical table the catalog lists: (physical name, the id of its
    // DynamoDB table, how notes name it).
    let mut physical: Vec<(String, String, String)> = tables
        .iter()
        .map(|(id, _, _, _)| (format!("_ddb_{id}"), id.clone(), ddb_table(id)))
        .collect();
    let indexes: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT index_id, table_id, index_name FROM indexes ORDER BY index_id COLLATE \"C\"",
    )
    .fetch_all(catalog)
    .await
    .map_err(|e| db_error("list indexes", &e))?;
    physical.extend(indexes.iter().map(|(id, table_id, name)| {
        (
            format!("_ddb_{id}"),
            table_id.clone(),
            format!("index {name:?} of {}", ddb_table(table_id)),
        )
    }));
    let vectors: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT index_id, table_id, index_name FROM vector_indexes \
         ORDER BY index_id COLLATE \"C\"",
    )
    .fetch_all(catalog)
    .await
    .map_err(|e| db_error("list vector indexes", &e))?;
    physical.extend(vectors.iter().map(|(id, table_id, name)| {
        (
            format!("_ddb_vec_{id}"),
            table_id.clone(),
            format!("vector index {name:?} of {}", ddb_table(table_id)),
        )
    }));

    for (table, table_id, label) in physical {
        if deleting.contains(table_id.as_str()) {
            notes.push(format!("{label}: skipped, the table is being deleted"));
            continue;
        }
        match table_work(conn, &table, &["item_data"], Some(label.clone())).await? {
            Some(w) => work.push(w),
            None => notes.push(format!("{label}: skipped, its physical table {table} does not exist")),
        }
    }

    // Fixed tables holding item documents. `gsi_pending.index_context` is a
    // catalog document, which migration 11 escaped once with the same
    // mapping; escaping it again would read a U+0001 back as U+0001 U+0002.
    // A stream record waits in `stream_pending` until the numbering moves it
    // to `stream_records` (migration 7), so both hold item documents.
    // `record_bytes`, the numbering's estimate of a batch's size, keeps the
    // length the write sent.
    for (table, columns) in [
        ("gsi_pending", &["old_item", "new_item"][..]),
        ("stream_records", &["record_data"][..]),
        ("stream_pending", &["record_data"][..]),
    ] {
        work.extend(table_work(conn, table, columns, None).await?);
    }

    escape_all(conn, &work, &mut notes).await?;
    Ok(notes)
}
