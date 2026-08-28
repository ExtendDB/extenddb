// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra schema migrations.
//!
//! ## Migration Versioning
//!
//! This module uses Flyway-style versioning for migration files:
//!
//! - Format: `V###__description.cql` (e.g., `V001__initial_schema.cql`)
//! - Version: Integer extracted from `V###__` prefix (e.g., 1 from V001)
//! - Description: Text after double underscore (e.g., `initial_schema`)
//!
//! Flyway is a popular database migration tool that uses this naming convention.
//! See: https://flywaydb.org/documentation/concepts/migrations#naming
//!
//! ## Migration Tracking
//!
//! Applied migrations are tracked in the `schema_history` table:
//! - `version` (int): Extracted from filename (V001 → 1)
//! - `description` (text): Extracted from filename (V001__initial_schema.cql → initial_schema)
//! - `applied_at` (timestamp): When the migration was applied
//!
//! Migrations are applied in order by version number. Already-applied migrations
//! are skipped based on the version number in the tracking table.
//!
//! ## Adding New Migrations
//!
//! 1. Create a new file: `migrations/catalog/V###__description.cql`
//! 2. Use the next sequential version number (e.g., V003 after V002)
//! 3. Add the migration to the `CATALOG_MIGRATIONS` array below
//! 4. Migrations are embedded at compile time via `include_str!()`

use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use extenddb_storage::management_store::{OpError, OpResult};
use std::sync::Arc;

type CassandraSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

/// Embedded catalog migration files, applied in order.
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "V001__initial_schema.cql",
        include_str!("../migrations/catalog/V001__initial_schema.cql"),
    ),
    (
        "V002__backup_restore.cql",
        include_str!("../migrations/catalog/V002__backup_restore.cql"),
    ),
    (
        "V003__account_scoped_idempotency.cql",
        include_str!("../migrations/catalog/V003__account_scoped_idempotency.cql"),
    ),
];

/// Embedded data migration files, applied in order.
pub(crate) const DATA_MIGRATIONS: &[(&str, &str)] = &[
    (
        "V001__initial_data_schema.cql",
        include_str!("../migrations/data/V001__initial_data_schema.cql"),
    ),
    (
        "V002__backup_items.cql",
        include_str!("../migrations/data/V002__backup_items.cql"),
    ),
];

/// Run catalog migrations, skipping already-applied ones.
pub async fn run_catalog_migrations(
    session: &Arc<CassandraSession>,
    keyspace: &str,
) -> OpResult<()> {
    println!("--- Running catalog migrations...");

    // Set keyspace context for all subsequent queries
    session
        .query(format!("USE {keyspace}"))
        .await
        .map_err(|e| OpError::Internal(format!("Failed to USE keyspace: {e}")))?;

    for (filename, sql) in CATALOG_MIGRATIONS {
        if is_migration_applied(session, keyspace, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        execute_migration(session, sql).await?;
        record_migration(session, keyspace, filename).await?;
    }
    println!("    Migrations applied.");
    Ok(())
}

/// Run data database migrations.
pub async fn run_data_migrations(session: &Arc<CassandraSession>, keyspace: &str) -> OpResult<()> {
    println!("--- Running data migrations...");

    // Set keyspace context for all subsequent queries
    session
        .query(format!("USE {keyspace}"))
        .await
        .map_err(|e| OpError::Internal(format!("Failed to USE keyspace: {e}")))?;

    for (filename, sql) in DATA_MIGRATIONS {
        if is_migration_applied(session, keyspace, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        execute_migration(session, sql).await?;
        record_migration(session, keyspace, filename).await?;
    }
    println!("    Data migrations applied.");
    Ok(())
}

/// Check if a table exists in the given keyspace.
pub(crate) async fn table_exists(
    session: &Arc<CassandraSession>,
    keyspace: &str,
    table_name: &str,
) -> OpResult<bool> {
    let query =
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?";
    let exists = session
        .query_with_values(query, cdrs_tokio::query_values!(keyspace, table_name))
        .await
        .ok()
        .and_then(|frame| frame.response_body().ok())
        .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
        .is_some_and(|rows| !rows.is_empty());
    Ok(exists)
}

/// Return the union of data migrations pending in any account keyspace.
///
/// A migration is pending if at least one existing account has not recorded it.
/// This keeps a partially completed multi-account upgrade retryable.
pub(crate) async fn pending_data_migrations(
    session: &Arc<CassandraSession>,
    catalog_keyspace: &str,
    account_keyspace_fn: impl Fn(&str) -> String,
) -> OpResult<Vec<String>> {
    let query = format!("SELECT account_id FROM {catalog_keyspace}.accounts");
    let rows = crate::cassandra_util::query_rows::<OpError>(
        &Arc::clone(session),
        &query,
        cdrs_tokio::query_values!(),
        "pending_data_migrations",
    )
    .await?;

    let mut pending = std::collections::BTreeSet::new();
    for row in rows {
        let account_id: String =
            crate::cassandra_util::get_column(&row, "account_id", "pending_data_migrations")?;
        let keyspace = account_keyspace_fn(&account_id);
        for (filename, _sql) in DATA_MIGRATIONS {
            if !is_migration_applied(session, &keyspace, filename).await? {
                pending.insert((*filename).to_owned());
            }
        }
    }
    Ok(pending.into_iter().collect())
}

/// Check if a migration has already been applied.
async fn is_migration_applied(
    session: &Arc<CassandraSession>,
    keyspace: &str,
    filename: &str,
) -> OpResult<bool> {
    // Extract version from filename (V001__description.cql)
    let version: i32 = filename
        .strip_prefix("V")
        .and_then(|s| s.split("__").next())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| OpError::Internal(format!("Invalid migration filename: {filename}")))?;

    let check_cql = format!(
        "SELECT version FROM {keyspace}.schema_history WHERE version = ?"
    );

    let applied = session
        .query_with_values(check_cql, cdrs_tokio::query_values!(version))
        .await
        .ok()
        .and_then(|frame| frame.response_body().ok())
        .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
        .is_some_and(|rows| !rows.is_empty());

    Ok(applied)
}

/// Execute a migration by splitting on semicolons and running each statement.
async fn execute_migration(session: &Arc<CassandraSession>, sql: &str) -> OpResult<()> {
    for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
        let stmt = statement.trim();
        if stmt.is_empty() {
            continue;
        }

        session
            .query(stmt)
            .await
            .map_err(|e| OpError::Internal(format!("Migration failed: {e}")))?;
    }
    Ok(())
}

/// Record a migration in the `schema_history` table.
async fn record_migration(
    session: &Arc<CassandraSession>,
    keyspace: &str,
    filename: &str,
) -> OpResult<()> {
    // Extract version and description from filename
    let version: i32 = filename
        .strip_prefix("V")
        .and_then(|s| s.split("__").next())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| OpError::Internal(format!("Invalid migration filename: {filename}")))?;

    let description = filename
        .split("__")
        .nth(1)
        .and_then(|s| s.strip_suffix(".cql"))
        .unwrap_or("unknown");

    let record_cql = format!(
        "INSERT INTO {keyspace}.schema_history (version, description, applied_at) VALUES (?, ?, toTimestamp(now()))"
    );

    session
        .query_with_values(record_cql, cdrs_tokio::query_values!(version, description))
        .await
        .map_err(|e| OpError::Internal(format!("Record migration: {e}")))?;

    Ok(())
}
