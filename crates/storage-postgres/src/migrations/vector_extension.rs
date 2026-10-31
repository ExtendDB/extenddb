// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The pgvector extension attempt `migrate` makes on every run, beside the
//! ledger, for an operator who installs pgvector after `init`.
//!
//! pgvector is not a trusted extension (its control file has no `trusted =
//! true`), so a stock server refuses `CREATE EXTENSION vector` from any role
//! that is not a superuser, including the application role that owns the
//! data database. The attempt uses an admin connection only when the caller
//! has one (`--pg-user`), then the application role, which is enough on
//! managed platforms that allow the extension for a database owner. Failure
//! is a notice: a deployment without pgvector initialises and upgrades, and
//! the server refuses vector operations.

use std::time::Duration;

use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection, PgConnection};

/// How long the attempt's connection may take to open, as every other
/// connection a run opens.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long `CREATE EXTENSION` may take once connected: its lock wait is
/// bounded by `lock_timeout`, and this bounds a database that stops
/// answering.
const CREATE_TIMEOUT: Duration = Duration::from_secs(30);

async fn create_as(options: &PgConnectOptions) -> Result<(), String> {
    let options = options.clone().disable_statement_logging();
    let mut conn =
        match tokio::time::timeout(CONNECT_TIMEOUT, PgConnection::connect_with(&options)).await {
            Ok(connected) => connected.map_err(|e| e.to_string())?,
            Err(_) => {
                return Err(format!(
                    "no connection within {} s",
                    CONNECT_TIMEOUT.as_secs()
                ));
            }
        };
    let created = tokio::time::timeout(CREATE_TIMEOUT, create_in_transaction(&mut conn)).await;
    let Ok(created) = created else {
        // The connection is dropped unclosed: the database did not answer.
        return Err(format!(
            "CREATE EXTENSION did not finish within {} s",
            CREATE_TIMEOUT.as_secs()
        ));
    };
    let _ = conn.close().await;
    created.map(|_| ()).map_err(|e| {
        let hint = crate::vector::create_extension_hint(&e);
        format!("{e} ({hint})")
    })
}

/// `CREATE EXTENSION` in a transaction that first sets `client_min_messages`
/// and `lock_timeout`, so the settings reach the backend that runs the
/// statement behind a transaction-mode pooler. No notice for an extension
/// that exists: the line `migrate` prints says so.
async fn create_in_transaction(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    let mut tx = conn.begin().await?;
    sqlx::query("SET LOCAL client_min_messages = warning")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL lock_timeout = '5s'")
        .execute(&mut *tx)
        .await?;
    sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

/// Try to create pgvector in the data database: as `admin` when given (the
/// admin role, with the data database selected), then as the application
/// role (`data`). Returns the line to print.
pub async fn try_create_vector_extension(
    data: &PgConnectOptions,
    admin: Option<&PgConnectOptions>,
) -> String {
    let mut failures = Vec::new();
    if let Some(admin) = admin {
        let admin = admin
            .clone()
            .database(data.get_database().unwrap_or("postgres"));
        match create_as(&admin).await {
            Ok(()) => return "pgvector available; vector indexes are supported.".to_owned(),
            Err(e) => failures.push(format!("as the admin role: {e}")),
        }
    }
    match create_as(data).await {
        Ok(()) => "pgvector available; vector indexes are supported.".to_owned(),
        Err(e) => {
            failures.push(format!("as the application role: {e}"));
            format!(
                "NOTICE: could not create the pgvector extension ({}). Vector indexes are \
                 unavailable until it is installed; pass --pg-user with a role that may create \
                 it to retry.",
                failures.join("; ")
            )
        }
    }
}
