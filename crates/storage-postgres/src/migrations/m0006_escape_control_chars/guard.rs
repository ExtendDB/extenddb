// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The sessions check of migration 6's `detect`: the run stops, before its
//! first write, while another session holds a database a body rewrites.
//!
//! The bodies rewrite rows in place and cannot tell a row a running server
//! writes from a legacy row: a server of an earlier release still writing
//! leaves rows in the earlier encoding behind it. `migrate --yes` waits for
//! the servers of releases that take the serving key, and the upgrade manual
//! says to stop every server first; this check finds a server of an earlier
//! release, or any other client, that was not stopped. The runner's own
//! sessions, the migration lock's session and read-only status sessions are
//! left out by `application_name`. A store with nothing to rewrite (no
//! table, no backup, no queued or stream row), such as the one `init`
//! creates, needs no check. Setting [`IGNORE_CONNECTIONS_ENV`] skips the
//! check, for a database whose only other sessions are idle connections a
//! connection pooler holds.
//!
//! The migrator connects as the application role, which is not a superuser.
//! For sessions of other roles PostgreSQL then shows `datname`, `usename`
//! and `application_name` but hides `backend_type` and `client_addr`, so a
//! filter on `backend_type` alone would miss an operator's `psql` or a server
//! running as another role. A session is counted when it has a role
//! (`usename`), which background workers such as autovacuum never have, and
//! its type is `client backend` or hidden.

use extenddb_storage::migrations::MigrationError;
use sqlx::PgPool;

use super::super::connect::{RUNNER_APPLICATION_PREFIX, STATUS_APPLICATION};
use super::db_error;

/// Skips the check when set, to any value.
pub(crate) const IGNORE_CONNECTIONS_ENV: &str = "EXTENDDB_MIGRATE_IGNORE_CONNECTIONS";

/// `application_name` of the migration lock's session (`locks.rs`).
const LOCK_APPLICATION: &str = "extenddb migrate";

/// Whether the public schema of the database `pool` reaches has `table` and
/// the table holds a row.
async fn has_row(pool: &PgPool, table: &str) -> Result<bool, MigrationError> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("public.{table}"))
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(&format!("look for {table}"), &e))?;
    if !exists {
        return Ok(false);
    }
    sqlx::query_scalar(&format!("SELECT EXISTS (SELECT 1 FROM {table})"))
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(&format!("read {table}"), &e))
}

/// Whether the catalog body has rows to rewrite: `backup_items` holds one.
pub(crate) async fn catalog_has_rows(catalog: &PgPool) -> Result<bool, MigrationError> {
    if std::env::var_os(IGNORE_CONNECTIONS_ENV).is_some() {
        return Ok(false);
    }
    has_row(catalog, "backup_items").await
}

/// Whether the data body may have rows to rewrite: the catalog lists a table,
/// or `gsi_pending` or `stream_records` holds a row.
pub(crate) async fn data_has_rows(catalog: &PgPool, data: &PgPool) -> Result<bool, MigrationError> {
    if std::env::var_os(IGNORE_CONNECTIONS_ENV).is_some() {
        return Ok(false);
    }
    Ok(has_row(catalog, "tables").await?
        || has_row(data, "gsi_pending").await?
        || has_row(data, "stream_records").await?)
}

/// One item naming the other sessions on the database `pool` reaches, or none;
/// `which` names the database (`"data"` or `"catalog"`).
pub(crate) async fn other_sessions(pool: &PgPool, which: &str) -> Result<Vec<String>, MigrationError> {
    type Row = (String, Option<String>, Option<String>);
    let (database, others): (String, Vec<Row>) = {
        let database: String = sqlx::query_scalar("SELECT current_database()::text")
            .fetch_one(pool)
            .await
            .map_err(|e| db_error("name the database", &e))?;
        let others = sqlx::query_as(
            "SELECT usename::text, application_name, client_addr::text \
             FROM pg_stat_activity \
             WHERE datname = current_database() \
               AND usename IS NOT NULL \
               AND coalesce(backend_type, 'client backend') = 'client backend' \
               AND pid <> pg_backend_pid() \
               AND NOT starts_with(coalesce(application_name, ''), $1) \
               AND coalesce(application_name, '') NOT IN ($2, $3)",
        )
        .bind(RUNNER_APPLICATION_PREFIX)
        .bind(LOCK_APPLICATION)
        .bind(STATUS_APPLICATION)
        .fetch_all(pool)
        .await
        .map_err(|e| db_error("look for other sessions", &e))?;
        (database, others)
    };
    if others.is_empty() {
        return Ok(Vec::new());
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
    Ok(vec![format!(
        "{} other session(s) hold the {which} database {database}: {}. Migration 6 rewrites \
         stored strings and runs with every ExtendDB server stopped and every other session \
         on the database closed, because a server still writing would leave rows in the \
         earlier encoding. Stop them and run `extenddb migrate --yes` again. If the only \
         other sessions are idle connections a connection pooler holds, set \
         {IGNORE_CONNECTIONS_ENV}=1 to skip this check",
        others.len(),
        described.join("; ")
    )])
}
