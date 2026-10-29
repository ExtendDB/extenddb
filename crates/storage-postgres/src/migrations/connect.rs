// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Connections the runner, the locks and the status open.
//!
//! No session sets a parameter in the startup packet other than
//! `application_name`, which PgBouncer tracks and passes on: a connection
//! pooler such as PgBouncer refuses a login whose `options` names a
//! parameter it does not track. Every setting is sent with `SET LOCAL` inside the
//! transaction that needs it, so a transaction-mode pooler applies it to the
//! backend that runs the transaction, and it ends with that transaction.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use extenddb_storage::migrations::MigrationError;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, PgPool};

/// `application_name` of the status's read-only sessions.
pub(crate) const STATUS_APPLICATION: &str = "extenddb status";

/// Prefix of the `application_name` of a runner's sessions, which apply the
/// bodies. Each runner adds its own suffix, so a later migrator can tell an
/// earlier runner's sessions from its own.
pub(crate) const RUNNER_APPLICATION_PREFIX: &str = "extenddb migrate run ";

/// A new runner session name, unique to one runner.
pub(crate) fn runner_application() -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{RUNNER_APPLICATION_PREFIX}{}", &id[..12])
}

/// One connection as a pool, opened once. A second connect would mean the
/// first was lost, together with any open transaction, and statements would
/// then commit one by one, so it is refused.
///
/// `read_only` is the status's session: it is named [`STATUS_APPLICATION`]
/// and opens one `READ ONLY` transaction at connect, so nothing it runs can
/// write; closing the connection ends it. Otherwise the session keeps the
/// `application_name` of `options` and runs in autocommit until the runner
/// sends `BEGIN`.
pub(crate) async fn one_connection(
    options: PgConnectOptions,
    read_only: bool,
    timeout: Duration,
) -> Result<PgPool, sqlx::Error> {
    let options = if read_only {
        options.application_name(STATUS_APPLICATION)
    } else {
        options
    };
    // `migrate` prints a line for every wait; sqlx's slow-statement warnings
    // would print SQL, with terminal colour codes, between them.
    let options = options.disable_statement_logging();
    let opened = Arc::new(AtomicUsize::new(0));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .acquire_timeout(timeout)
        .test_before_acquire(false)
        .after_connect(move |_conn, _meta| {
            let opened = Arc::clone(&opened);
            Box::pin(async move {
                if opened.fetch_add(1, Ordering::SeqCst) > 0 {
                    return Err(sqlx::Error::Protocol(
                        "the migration connection was lost and is not reopened".to_owned(),
                    ));
                }
                Ok(())
            })
        })
        .connect_with(options)
        .await?;
    if read_only {
        // The pool holds one connection that is never reopened, so the
        // transaction lasts until the pool closes.
        if let Err(e) = sqlx::raw_sql("BEGIN READ ONLY").execute(&pool).await {
            pool.close().await;
            return Err(e);
        }
    }
    Ok(pool)
}

/// The database named in the connect options does not exist (SQLSTATE
/// 3D000).
pub(crate) fn missing_database(e: &sqlx::Error) -> bool {
    sqlstate(e).as_deref() == Some("3D000")
}

/// The SQLSTATE of a database error.
pub(crate) fn sqlstate(e: &sqlx::Error) -> Option<String> {
    e.as_database_error()
        .and_then(|d| d.code().map(|c| c.into_owned()))
}

/// The migration error for a failed statement, keeping its SQLSTATE when
/// the database reported one. Code bodies and the runner map every
/// `sqlx::Error` through this, so the runner can read the code.
pub(crate) fn sql_error(context: &str, e: &sqlx::Error) -> MigrationError {
    MigrationError::coded(context, e, sqlstate(e))
}

/// Whether a table of this name exists in the public schema.
pub(crate) async fn relation_exists(pool: &PgPool, name: &str) -> Result<bool, MigrationError> {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("public.\"{name}\""))
        .fetch_one(pool)
        .await
        .map_err(|e| MigrationError::store(format!("look up {name}"), e))
}
