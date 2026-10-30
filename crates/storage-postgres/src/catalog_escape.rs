// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The one-shot re-encode of the catalog columns that carry attribute names,
//! run by the catalog migration `004_escape_catalog_control_chars.sql`.
//!
//! `catalog_json` stores U+0000 as U+0001 U+0001 and U+0001 as U+0001 U+0002
//! in `tables.key_schema`, `tables.attribute_definitions`,
//! `indexes.key_schema`, `indexes.projection`, `backups.key_schema`,
//! `backups.attribute_definitions` and `backup_definitions.definition`, and
//! its decoder reads a U+0001 followed by U+0001 or U+0002 as such a pair.
//! Earlier releases stored a name containing U+0001 raw (`jsonb` accepts it),
//! so a pre-existing name `a<U+0001><U+0001>z` would read back as
//! `a<U+0000>z` after the upgrade and the table's items would no longer be
//! reachable by key. This migration rewrites every stored document that
//! contains a raw U+0001 through the escape once, so that after it every
//! document in these columns is in the escaped form. A legacy document cannot
//! contain U+0000 (`jsonb` refused it), so applying the escape to the raw
//! stored value is exactly the re-encode, and a document without U+0001
//! encodes to itself and is left alone.
//!
//! The rewrite is not idempotent at the row level: an escaped U+0001 is stored
//! as U+0001 U+0002, the same bytes as a legacy pair, so a row this release
//! wrote must never be rewritten. Three rules follow. Each table is rewritten
//! in one transaction that also inserts a marker (migration name, table name)
//! into the progress table, so a re-run after a crash skips the tables already
//! done; when every table is marked the runner records the migration file in
//! `schema_history` and later runs are no-ops. The migration refuses to run
//! while any other session is connected to the catalog, because a server of
//! this release writing during the rewrite would have its rows rewritten
//! again, and an older server writing after it would leave raw rows behind.
//! And the server refuses to start on a catalog that does not record the
//! migration: the version gate refuses the previous version, and the ledger
//! check refuses a catalog stamped with the current version by another
//! migration file of the same number.
//!
//! Composition with the data-side escape (the `item_data` columns and the
//! key text columns, data migration `004_escape_control_chars`): the alphabet
//! is the same and the tables are disjoint, so the two migrations can run in
//! either order. If that migration later takes these seven columns as more
//! entries of its own table list, a catalog that records this file must be
//! left out of that rewrite, because its documents are already escaped.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::PgPool;

use crate::catalog_json::escape_json;

/// The catalog migration file this re-encode belongs to; the ledger name.
pub(crate) const ESCAPE_CATALOG_MIGRATION: &str = "004_escape_catalog_control_chars.sql";

/// `application_name` the init and migrate commands set on their connections,
/// so [`refuse_if_other_clients_connected`] can leave them out of its count.
pub(crate) const MIGRATE_APPLICATION_NAME: &str = "extenddb-migrate";

/// Environment variable that skips [`refuse_if_other_clients_connected`], for a
/// database whose only other connections are known to be idle (a connection
/// pooler holding server-side connections open, for example).
pub(crate) const IGNORE_CONNECTIONS_ENV: &str = "EXTENDDB_MIGRATE_IGNORE_CONNECTIONS";

/// The progress ledger: one row per (migration, table) rewritten and committed.
const PROGRESS_TABLE: &str = "data_code_migration_progress";

/// The catalog tables and `jsonb` columns the escape covers.
const CATALOG_WORK: &[(&str, &[&str])] = &[
    ("tables", &["key_schema", "attribute_definitions"]),
    ("indexes", &["key_schema", "projection"]),
    ("backups", &["key_schema", "attribute_definitions"]),
    ("backup_definitions", &["definition"]),
];

/// Refuse to run a row-rewriting migration while anything else is connected
/// to the database `pool` reaches. `which` names that database in the refusal.
///
/// The migrate command connects as the application role, which is not a
/// superuser. For sessions of other roles, `PostgreSQL` then shows `datname`,
/// `usename`, and `application_name` but hides `backend_type` and
/// `client_addr`, so a filter on `backend_type` alone misses an operator's
/// `psql` or a server running under another role. A session is counted when it
/// has a role (`usename`), which background workers such as autovacuum never
/// have, and its type is either `client backend` or hidden. Connections
/// carrying [`MIGRATE_APPLICATION_NAME`] are this process's own and are
/// ignored. Managed environments where a pooler keeps idle connections open
/// can set [`IGNORE_CONNECTIONS_ENV`] to skip the check.
pub(crate) async fn refuse_if_other_clients_connected(pool: &PgPool, which: &str) -> OpResult<()> {
    let others: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT usename::text, application_name, client_addr::text \
         FROM pg_stat_activity \
         WHERE datname = current_database() \
           AND usename IS NOT NULL \
           AND coalesce(backend_type, 'client backend') = 'client backend' \
           AND pid <> pg_backend_pid() \
           AND coalesce(application_name, '') <> $1",
    )
    .bind(MIGRATE_APPLICATION_NAME)
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check for other connections: {e}")))?;
    if others.is_empty() {
        return Ok(());
    }
    let mut described: Vec<String> = others
        .iter()
        .map(|(user, app, addr)| {
            format!(
                "{user} from {} ({})",
                addr.as_deref().unwrap_or("no client address shown"),
                match app.as_deref() {
                    Some("") | None => "no application name",
                    Some(a) => a,
                }
            )
        })
        .collect();
    described.sort();
    described.dedup();
    Err(OpError::Internal(format!(
        "{} other connection(s) hold the {which} database: {}. This migration rewrites \
         stored rows and must run with every ExtendDB server stopped and every other \
         session closed. Stop them and run 'extenddb migrate --yes' again. If the only \
         other connections are idle ones held by a connection pooler, set \
         {IGNORE_CONNECTIONS_ENV}=1 to skip this check.",
        others.len(),
        described.join("; ")
    )))
}

/// True when [`ESCAPE_CATALOG_MIGRATION`] still has to run on this catalog and
/// the operator has not set [`IGNORE_CONNECTIONS_ENV`], so the migrate
/// command must hold the catalog alone before it writes anything.
pub(crate) async fn needs_exclusive_access(catalog_pool: &PgPool) -> OpResult<bool> {
    if std::env::var_os(IGNORE_CONNECTIONS_ENV).is_some() {
        return Ok(false);
    }
    Ok(!crate::migrations::is_migration_applied(catalog_pool, ESCAPE_CATALOG_MIGRATION).await?)
}

async fn ensure_progress_table(pool: &PgPool) -> OpResult<()> {
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} (\
             migration_name TEXT NOT NULL, \
             table_name TEXT NOT NULL, \
             completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
             PRIMARY KEY (migration_name, table_name)\
         )"
    ))
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Create migration progress table: {e}")))?;
    Ok(())
}

async fn is_table_marked(pool: &PgPool, table: &str) -> OpResult<bool> {
    sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM {PROGRESS_TABLE} \
         WHERE migration_name = $1 AND table_name = $2)"
    ))
    .bind(ESCAPE_CATALOG_MIGRATION)
    .bind(table)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check migration progress: {e}")))
}

/// The re-encoded form of a stored document, or `None` when the escape does
/// not change it (no raw U+0000 or U+0001 in any key or string).
fn reencoded(value: &serde_json::Value) -> Option<serde_json::Value> {
    let escaped = escape_json(value.clone());
    (escaped != *value).then_some(escaped)
}

/// Re-encode the rows of one column whose stored document contains U+0001 or
/// U+0000, inside the caller's per-table transaction. The candidate ctids are
/// read once and locked before any row is touched, so no row is visited twice.
async fn escape_column(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    col: &str,
) -> OpResult<u64> {
    // `jsonb::text` renders a raw U+0001 inside a string or key as the six
    // characters backslash-u-0001 (U+0000 likewise, though no legacy row holds
    // one). The backslash is spelled chr(92) so the predicate reads the same
    // under either value of standard_conforming_strings. A stored string that
    // contains that literal text also matches, re-encodes to itself and is
    // skipped by `reencoded`.
    let select = format!(
        "SELECT ctid::text FROM \"{table}\" \
         WHERE position((chr(92) || 'u0001') IN \"{col}\"::text) > 0 \
            OR position((chr(92) || 'u0000') IN \"{col}\"::text) > 0 \
         FOR UPDATE"
    );
    let ctids: Vec<String> = sqlx::query_scalar(&select)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| OpError::Internal(format!("Find rows to re-encode in {table}.{col}: {e}")))?;
    let fetch =
        format!("SELECT ctid::text, \"{col}\" FROM \"{table}\" WHERE ctid = ANY($1::tid[])");
    let update = format!("UPDATE \"{table}\" SET \"{col}\" = $1 WHERE ctid = $2::tid");
    let mut changed: u64 = 0;
    for batch in ctids.chunks(500) {
        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(&fetch)
            .bind(batch.to_vec())
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| OpError::Internal(format!("Fetch rows from {table}.{col}: {e}")))?;
        for (ctid, value) in rows {
            if let Some(escaped) = reencoded(&value) {
                sqlx::query(&update)
                    .bind(escaped)
                    .bind(&ctid)
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| {
                        OpError::Internal(format!("Re-encode a row of {table}.{col}: {e}"))
                    })?;
                changed += 1;
            }
        }
    }
    Ok(changed)
}

/// Rewrite one table's columns in one transaction and mark the table done in
/// the progress ledger before committing, so the rewrite and the marker are
/// atomic. Returns the number of column updates (one per column rewritten in
/// a row).
async fn escape_table(pool: &PgPool, table: &str, columns: &[&str]) -> OpResult<u64> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| OpError::Internal(format!("Begin transaction for {table}: {e}")))?;
    let mut rewritten = 0;
    for col in columns {
        rewritten += escape_column(&mut tx, table, col).await?;
    }
    sqlx::query(&format!(
        "INSERT INTO {PROGRESS_TABLE} (migration_name, table_name) \
         VALUES ($1, $2) ON CONFLICT DO NOTHING"
    ))
    .bind(ESCAPE_CATALOG_MIGRATION)
    .bind(table)
    .execute(&mut *tx)
    .await
    .map_err(|e| OpError::Internal(format!("Mark {table} as re-encoded: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| OpError::Internal(format!("Commit re-encode of {table}: {e}")))?;
    Ok(rewritten)
}

/// Re-encode the legacy documents of every catalog column the escape covers
/// (see the module documentation). Refuses while another session holds the
/// catalog unless [`IGNORE_CONNECTIONS_ENV`] is set. Tables absent from this
/// catalog (a `backups` table a deployment never had) are skipped.
pub(crate) async fn escape_legacy_catalog_control_chars(pool: &PgPool) -> OpResult<()> {
    if std::env::var_os(IGNORE_CONNECTIONS_ENV).is_none() {
        refuse_if_other_clients_connected(pool, "catalog").await?;
    }
    ensure_progress_table(pool).await?;
    for (table, columns) in CATALOG_WORK {
        if !crate::migrations::table_exists(pool, table).await? {
            continue;
        }
        if is_table_marked(pool, table).await? {
            println!("      {table}: already re-encoded, skipping.");
            continue;
        }
        let rewritten = escape_table(pool, table, columns).await?;
        println!("      {table}: {rewritten} column update(s)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reencoded_changes_exactly_the_documents_holding_raw_control_chars() {
        assert_eq!(
            reencoded(&json!([{"AttributeName": "pk", "KeyType": "HASH"}])),
            None
        );
        // The six-character text backslash-u-0001 is literal data, not U+0001.
        assert_eq!(reencoded(&json!({"AttributeName": "a\\u0001z"})), None);
        assert_eq!(
            reencoded(&json!([{"AttributeName": "a\u{1}\u{1}z", "KeyType": "HASH"}])),
            Some(json!([{"AttributeName": "a\u{1}\u{2}\u{1}\u{2}z", "KeyType": "HASH"}]))
        );
        assert_eq!(
            reencoded(&json!({"NonKeyAttributes": ["n\u{1}k"], "k\u{1}": 1})),
            Some(json!({"NonKeyAttributes": ["n\u{1}\u{2}k"], "k\u{1}\u{2}": 1}))
        );
    }

    #[test]
    fn the_work_list_names_the_seven_columns_the_decoder_reads() {
        let columns: Vec<String> = CATALOG_WORK
            .iter()
            .flat_map(|(t, cs)| cs.iter().map(move |c| format!("{t}.{c}")))
            .collect();
        assert_eq!(
            columns,
            [
                "tables.key_schema",
                "tables.attribute_definitions",
                "indexes.key_schema",
                "indexes.projection",
                "backups.key_schema",
                "backups.attribute_definitions",
                "backup_definitions.definition",
            ]
        );
    }
}
