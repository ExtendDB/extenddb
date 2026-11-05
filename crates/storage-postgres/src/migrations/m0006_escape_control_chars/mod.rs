// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 6, `escape_control_chars`, PostgreSQL: re-encode the rows an
//! earlier release wrote, whose strings are stored raw.
//!
//! This release stores every string through the order-preserving escape in
//! `extenddb_storage::util::escape_control` (U+0000 as U+0001 U+0001, U+0001
//! as U+0001 U+0002), so that U+0000, which DynamoDB accepts anywhere a string
//! appears, can be held in `TEXT` and `jsonb` columns. A row an earlier
//! release wrote holds its strings raw, and a raw U+0001 followed by U+0001 or
//! U+0002 reads back differently under the decoder (a lone U+0001 reads back
//! unchanged). Earlier releases could not store U+0000, so for their rows the
//! escape of the stored text is the re-encode, and a row without U+0001
//! encodes to itself and is left alone.
//!
//! Two bodies, one per database. The catalog body rewrites `backup_items`,
//! which lives in the catalog database; the data body rewrites every data
//! table, index table and vector index table (the text key columns and
//! `item_data`), `gsi_pending` and `stream_records`, and recomputes
//! `gsi_pending.worker_partition` for the rows it changes. Tables are listed
//! from the catalog rows; a row whose table is `DELETING`, or whose physical
//! table is missing, is skipped and noted.
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
//! The key text and document rewrites call the write path's escape and key
//! text functions (`escape_control`, `escape_json_strings`,
//! `key_text::reescape_legacy_composite`, `gsi_queue::worker_partition`), so
//! a migrated row is the row this release writes for the same item.
//!
//! This module's checksum covers `mod.rs`, `keys.rs`, `order.rs` and
//! `guard.rs`, and is pinned once the migration is released. They are
//! excluded from rustfmt and clippy so neither can require an edit.

mod guard;
mod keys;
mod order;

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use sqlx::{PgConnection, PgPool};

use super::PostgresStores;

pub(crate) use guard::other_sessions;
#[cfg(test)]
pub(crate) use guard::{IGNORE_CONNECTIONS_ENV, catalog_has_rows, data_has_rows};
pub(crate) use keys::{BACKUP_ITEMS_COMPOSITE_ROWS, has_composite_partition_key};
pub(crate) use order::{BATCH, rewrite_longest_first, with_detail};

/// The text of every file of this module, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = concat!(
    include_str!("mod.rs"),
    include_str!("keys.rs"),
    include_str!("order.rs"),
    include_str!("guard.rs"),
);

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0006_escape_control_chars/postgres/1";

/// The catalog body: `backup_items`.
pub(crate) struct EscapeBackupItems;

/// The data body: the data, index and vector index tables, `gsi_pending` and
/// `stream_records`.
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
    /// or `gsi_pending` or `stream_records` holds a row.
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

/// A failed statement as a migration error: `what`, the database's message
/// with its DETAIL ([`with_detail`]), and the SQLSTATE, which the runner reads
/// to run the body again after a lock timeout.
pub(crate) fn db_error(what: &str, e: &sqlx::Error) -> MigrationError {
    let message = format!("{what}: {}", with_detail(e));
    match e.as_database_error().and_then(|d| d.code()) {
        Some(code) => MigrationError::Database {
            message,
            sqlstate: code.into_owned(),
        },
        None => MigrationError::Store(message),
    }
}

/// One physical table's rewrite: its text key columns, and the jsonb columns
/// whose rows are re-encoded one by one.
struct EscapeWork {
    /// Bare (unquoted) table name, e.g. `_ddb_<id>` or `gsi_pending`.
    table: String,
    text_columns: Vec<TextKeyColumn>,
    jsonb_columns: Vec<String>,
    /// `gsi_pending` only: recompute `worker_partition` after the re-encode
    /// ([`keys::recompute_worker_partitions`]).
    recompute_worker_partition: bool,
    /// The DynamoDB table (and index) the physical table holds, for errors
    /// and notes.
    ddb_name: Option<String>,
}

impl EscapeWork {
    /// `<table>.<col>`, with the DynamoDB name when known: how errors name a
    /// column.
    fn column(&self, col: &str) -> String {
        match &self.ddb_name {
            Some(ddb) => format!("{}.{col} ({ddb})", self.table),
            None => format!("{}.{col}", self.table),
        }
    }

    /// `<table>`, with the DynamoDB name when known: how notes name a table.
    fn label(&self) -> String {
        match &self.ddb_name {
            Some(ddb) => format!("{} ({ddb})", self.table),
            None => self.table.clone(),
        }
    }
}

/// A text key column and the rows of it that hold a composite partition key.
struct TextKeyColumn {
    name: String,
    /// SQL boolean expression over the row, true where the column holds the
    /// netstring join of several HASH attributes. Those rows are rebuilt one
    /// by one ([`keys::rebuild_composite_rows`]); the others hold one
    /// attribute's text and get the single-statement SQL replace.
    composite_rows: String,
}

impl TextKeyColumn {
    fn new(name: String, composite: bool) -> Self {
        let composite_rows = if composite { "TRUE" } else { "FALSE" }.to_owned();
        Self {
            name,
            composite_rows,
        }
    }
}

/// The columns of `table` in the public schema of type `data_type` among
/// `names` (or, for `None`, the key columns `pk`, `base_pk`, `sk*_s` and
/// `base_sk*_s`), in column order.
async fn columns(
    conn: &mut PgConnection,
    table: &str,
    data_type: &str,
    names: Option<&[&str]>,
) -> Result<Vec<String>, MigrationError> {
    let names: Option<Vec<String>> = names.map(|n| n.iter().map(|c| (*c).to_owned()).collect());
    sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 AND data_type = $2 \
           AND CASE WHEN $3::text[] IS NULL \
               THEN column_name IN ('pk', 'base_pk') OR column_name::text ~ '^(base_)?sk[0-9]*_s$' \
               ELSE column_name::text = ANY($3) END \
         ORDER BY ordinal_position",
    )
    .bind(table)
    .bind(data_type)
    .bind(names)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| db_error(&format!("list the {data_type} columns of {table}"), &e))
}

/// The rewrite of a `_ddb_*` table, whose columns are read from
/// `information_schema.columns` so that exactly the sort key columns this
/// table has (`sk_s`, `sk<N>_s`, `base_sk<N>_s`, ...) are covered. `None`
/// when the physical table does not exist. `composite` names the columns
/// whose key schema has more than one HASH attribute, so they hold a
/// netstring join on every row.
async fn dynamic_table_work(
    conn: &mut PgConnection,
    table: String,
    composite: &[&str],
) -> Result<Option<EscapeWork>, MigrationError> {
    let text_columns = columns(conn, &table, "text", None).await?;
    let jsonb_columns = columns(conn, &table, "jsonb", Some(&["item_data"])).await?;
    if text_columns.is_empty() && jsonb_columns.is_empty() {
        return Ok(None);
    }
    let text_columns = text_columns
        .into_iter()
        .map(|name| {
            let composite = composite.contains(&name.as_str());
            TextKeyColumn::new(name, composite)
        })
        .collect();
    Ok(Some(EscapeWork {
        table,
        text_columns,
        jsonb_columns,
        recompute_worker_partition: false,
        ddb_name: None,
    }))
}

/// The rewrite of a fixed-name table, with the candidate columns filtered
/// through `information_schema.columns`. `None` when the table is absent.
async fn fixed_table_work(
    conn: &mut PgConnection,
    table: &str,
    text: &[&str],
    jsonb: &[&str],
) -> Result<Option<EscapeWork>, MigrationError> {
    let text_columns = if text.is_empty() {
        Vec::new()
    } else {
        columns(conn, table, "text", Some(text)).await?
    };
    let jsonb_columns = columns(conn, table, "jsonb", Some(jsonb)).await?;
    if text_columns.is_empty() && jsonb_columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(EscapeWork {
        table: table.to_owned(),
        text_columns: text_columns
            .into_iter()
            .map(|name| TextKeyColumn::new(name, false))
            .collect(),
        jsonb_columns,
        recompute_worker_partition: false,
        ddb_name: None,
    }))
}

/// The re-encoded form of a stored jsonb value, or `None` when re-encoding
/// does not change it. `escape_json_strings` only changes strings and object
/// keys containing U+0000 or U+0001, so a plain document and a document whose
/// strings hold the literal six-character text backslash-u-0001 both come
/// back equal and are skipped.
pub(crate) fn reencoded_json(value: &serde_json::Value) -> Option<serde_json::Value> {
    let escaped = extenddb_storage::util::escape_json_strings(value.clone());
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

/// Rewrite one table; returns the number of row updates.
async fn escape_rows_in_table(
    conn: &mut PgConnection,
    work: &EscapeWork,
) -> Result<u64, MigrationError> {
    let quoted = format!("\"{}\"", work.table);
    let mut rewritten: u64 = 0;

    // Key text columns holding one attribute's text: TEXT has no escaping
    // layer, so a single SQL statement per column is exact. NULL columns never
    // match (position() is NULL). The statement can fail on the primary key
    // when two legacy keys differ only by a U+0002 after a U+0001, depending
    // on the order it meets them (`order`); it runs under a savepoint, and on
    // a unique violation the column is rewritten again row by row, longest
    // legacy text first. Rows holding a composite key are rebuilt part by
    // part instead; the other columns of the row keep the replace().
    for col in &work.text_columns {
        let TextKeyColumn {
            name,
            composite_rows,
        } = col;
        let what = work.column(name);
        let single_rows = format!("NOT ({composite_rows})");
        let sql = format!(
            "UPDATE {quoted} SET \"{name}\" = replace(\"{name}\", chr(1), chr(1) || chr(2)) \
             WHERE position(chr(1) IN \"{name}\") > 0 AND {single_rows}"
        );
        sqlx::query("SAVEPOINT escape_column")
            .execute(&mut *conn)
            .await
            .map_err(|e| db_error(&format!("savepoint before {what}"), &e))?;
        match sqlx::query(&sql).execute(&mut *conn).await {
            Ok(done) => {
                sqlx::query("RELEASE SAVEPOINT escape_column")
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| db_error(&format!("release the savepoint of {what}"), &e))?;
                rewritten += done.rows_affected();
            }
            Err(e) if crate::pg_util::is_unique_violation(&e) => {
                sqlx::query("ROLLBACK TO SAVEPOINT escape_column")
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| db_error(&format!("roll back {what}"), &e))?;
                // Legacy text holds no U+0000, so the escape is the replace()
                // above.
                rewritten += rewrite_longest_first(conn, &quoted, name, &single_rows, &what, |legacy| {
                    Ok(extenddb_storage::util::escape_control(legacy).into_owned())
                })
                .await?;
            }
            Err(e) => return Err(db_error(&format!("re-encode {what}"), &e)),
        }
        if composite_rows != "FALSE" {
            rewritten +=
                keys::rebuild_composite_rows(conn, &quoted, name, composite_rows, &what).await?;
        }
    }

    for col in &work.jsonb_columns {
        rewritten += escape_jsonb_rows(conn, &quoted, col).await?;
    }
    if work.recompute_worker_partition {
        rewritten += keys::recompute_worker_partitions(conn).await?;
    }
    Ok(rewritten)
}

/// Rewrite every table of `work` in order; one note per table with a change.
async fn escape_all(
    conn: &mut PgConnection,
    work: &[EscapeWork],
    notes: &mut Vec<String>,
) -> Result<(), MigrationError> {
    for w in work {
        let rewritten = escape_rows_in_table(conn, w).await?;
        if rewritten > 0 {
            notes.push(format!("{}: {rewritten} row update(s)", w.label()));
        }
    }
    Ok(())
}

/// The catalog body on `conn`, the catalog database in the runner's
/// transaction. `backup_items.pk` is the data table's `pk` as the backup
/// copied it, so a backup of a table with several HASH attributes holds a
/// netstring join there.
pub(crate) async fn escape_catalog(conn: &mut PgConnection) -> Result<Vec<String>, MigrationError> {
    let mut notes = Vec::new();
    let mut work = Vec::new();
    if let Some(mut w) = fixed_table_work(conn, "backup_items", &["pk", "sk"], &["item_data"]).await? {
        for col in &mut w.text_columns {
            if col.name == "pk" {
                col.composite_rows = BACKUP_ITEMS_COMPOSITE_ROWS.to_owned();
            }
        }
        work.push(w);
    }
    escape_all(conn, &work, &mut notes).await?;
    Ok(notes)
}

/// The data body on `conn`, the data database in the runner's transaction,
/// with the tables listed from `catalog`.
///
/// A partition key column whose key schema has more than one HASH attribute
/// holds a netstring join (`key_text::composite_pk_to_text`): `pk` of a table
/// or index with such a key schema, and `base_pk` of the index and vector
/// tables of a table with one.
pub(crate) async fn escape_data(
    catalog: &PgPool,
    conn: &mut PgConnection,
) -> Result<Vec<String>, MigrationError> {
    let mut notes = Vec::new();
    let mut work: Vec<EscapeWork> = Vec::new();

    type TableRow = (String, String, String, String, serde_json::Value);
    let tables: Vec<TableRow> = sqlx::query_as(
        "SELECT table_id, table_name, account_id, table_status, key_schema FROM tables \
         ORDER BY table_id COLLATE \"C\"",
    )
    .fetch_all(catalog)
    .await
    .map_err(|e| db_error("list tables", &e))?;
    let deleting: std::collections::HashSet<&str> = tables
        .iter()
        .filter(|(_, _, _, status, _)| status == "DELETING")
        .map(|(id, _, _, _, _)| id.as_str())
        .collect();
    let composite_tables: std::collections::HashSet<&str> = tables
        .iter()
        .filter(|(_, _, _, _, ks)| has_composite_partition_key(ks))
        .map(|(id, _, _, _, _)| id.as_str())
        .collect();
    // How errors and notes name the DynamoDB table of a physical table.
    let ddb_tables: std::collections::HashMap<&str, String> = tables
        .iter()
        .map(|(id, name, account, _, _)| (id.as_str(), format!("table {name:?} of account {account}")))
        .collect();
    let ddb_table = |table_id: &str| {
        ddb_tables
            .get(table_id)
            .cloned()
            .unwrap_or_else(|| format!("table id {table_id}"))
    };
    let skip = |label: String, physical: &str, notes: &mut Vec<String>, table_id: &str| {
        if deleting.contains(table_id) {
            notes.push(format!("{label}: skipped, the table is being deleted"));
        } else {
            notes.push(format!("{label}: skipped, its physical table {physical} does not exist"));
        }
    };

    for (id, _, _, _, ks) in &tables {
        let label = ddb_table(id);
        let physical = format!("_ddb_{id}");
        let cols: &[&str] = if has_composite_partition_key(ks) { &["pk"] } else { &[] };
        match dynamic_table_work(conn, physical.clone(), cols).await? {
            Some(mut w) if !deleting.contains(id.as_str()) => {
                w.ddb_name = Some(label);
                work.push(w);
            }
            _ => skip(label, &physical, &mut notes, id),
        }
    }
    let indexes: Vec<(String, String, String, serde_json::Value)> = sqlx::query_as(
        "SELECT index_id, table_id, index_name, key_schema FROM indexes \
         ORDER BY index_id COLLATE \"C\"",
    )
    .fetch_all(catalog)
    .await
    .map_err(|e| db_error("list indexes", &e))?;
    for (id, table_id, name, ks) in &indexes {
        let label = format!("index {name:?} of {}", ddb_table(table_id));
        let physical = format!("_ddb_{id}");
        let mut cols: Vec<&str> = Vec::new();
        if has_composite_partition_key(ks) {
            cols.push("pk");
        }
        if composite_tables.contains(table_id.as_str()) {
            cols.push("base_pk");
        }
        match dynamic_table_work(conn, physical.clone(), &cols).await? {
            Some(mut w) if !deleting.contains(table_id.as_str()) => {
                w.ddb_name = Some(label);
                work.push(w);
            }
            _ => skip(label, &physical, &mut notes, table_id),
        }
    }
    let vectors: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT index_id, table_id, index_name FROM vector_indexes \
         ORDER BY index_id COLLATE \"C\"",
    )
    .fetch_all(catalog)
    .await
    .map_err(|e| db_error("list vector indexes", &e))?;
    for (id, table_id, name) in &vectors {
        let label = format!("vector index {name:?} of {}", ddb_table(table_id));
        let physical = format!("_ddb_vec_{id}");
        let cols: &[&str] = if composite_tables.contains(table_id.as_str()) {
            &["base_pk"]
        } else {
            &[]
        };
        match dynamic_table_work(conn, physical.clone(), cols).await? {
            Some(mut w) if !deleting.contains(table_id.as_str()) => {
                w.ddb_name = Some(label);
                work.push(w);
            }
            _ => skip(label, &physical, &mut notes, table_id),
        }
    }

    // Fixed tables holding item documents.
    if let Some(mut w) =
        fixed_table_work(conn, "gsi_pending", &[], &["old_item", "new_item", "index_context"])
            .await?
    {
        w.recompute_worker_partition = true;
        work.push(w);
    }
    if let Some(w) = fixed_table_work(conn, "stream_records", &[], &["record_data"]).await? {
        work.push(w);
    }

    escape_all(conn, &work, &mut notes).await?;
    Ok(notes)
}
