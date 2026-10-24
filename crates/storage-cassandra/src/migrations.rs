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
//! See: <https://flywaydb.org/documentation/concepts/migrations#naming>
//!
//! ## Migration Tracking
//!
//! Applied migrations are tracked in the `schema_history` table:
//! - `version` (int): Extracted from filename (V001 → 1)
//! - `description` (text): Extracted from filename (`V001__initial_schema.cql` → `initial_schema`)
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
    (
        "V004__ttl_backfill_cursor.cql",
        include_str!("../migrations/catalog/V004__ttl_backfill_cursor.cql"),
    ),
    (
        "V005__backup_definitions_and_restore_registration.cql",
        include_str!("../migrations/catalog/V005__backup_definitions_and_restore_registration.cql"),
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
    let version = migration_version(filename)?;

    let check_cql = format!("SELECT version FROM {keyspace}.schema_history WHERE version = ?");

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

    // A migration is DDL (CREATE KEYSPACE/TABLE, etc.). The driver runs queries
    // over a pool of connections and selects one per query, so the statement
    // that records or checks a migration can land on a different connection —
    // and a different coordinator view — from the one that just ran the DDL.
    // cdrs-tokio 9.0.2 exposes no schema-agreement wait, and learns of schema
    // changes asynchronously via SCHEMA_CHANGE events, so a read issued right
    // after a CREATE can hit a coordinator that has not yet applied it and fail
    // with "table ... does not exist". With a single connection the statements
    // serialized on one coordinator and the race was effectively impossible;
    // the multi-connection pool (issue #403) exposes it. Wait for the cluster
    // schema to agree before the next statement so the new object is visible
    // everywhere. This is startup/DDL-only, so the poll cost is irrelevant.
    wait_for_schema_agreement(session).await
}

/// Poll until the cluster agrees on a single schema version.
///
/// Reads `schema_version` from `system.local` (this node) and `system.peers`
/// (every other node); when they are all equal the schema change has
/// propagated and newly created objects are visible through any connection in
/// the pool. On a single-node cluster `system.peers` is empty, so this returns
/// as soon as the local version is readable — which still gives the driver's
/// event loop a beat to apply the change before the next query.
async fn wait_for_schema_agreement(session: &Arc<CassandraSession>) -> OpResult<()> {
    use std::time::{Duration, Instant};

    const TIMEOUT: Duration = Duration::from_secs(30);
    const INTERVAL: Duration = Duration::from_millis(200);

    let deadline = Instant::now() + TIMEOUT;
    loop {
        if schema_versions_agree(session).await? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(OpError::Internal(
                "Timed out waiting for Cassandra schema agreement after a migration".to_owned(),
            ));
        }
        tokio::time::sleep(INTERVAL).await;
    }
}

/// Return true when `system.local` and every `system.peers` row report the same
/// `schema_version`.
async fn schema_versions_agree(session: &Arc<CassandraSession>) -> OpResult<bool> {
    use cdrs_tokio::types::IntoRustByName as _;

    let local = session
        .query("SELECT schema_version FROM system.local")
        .await
        .map_err(|e| OpError::Internal(format!("schema agreement (local): {e}")))?
        .response_body()
        .map_err(|e| OpError::Internal(format!("schema agreement (local): {e}")))?
        .into_rows()
        .unwrap_or_default();

    let Some(local_version) = local.first().and_then(|r| {
        let v: Option<uuid::Uuid> = r.get_r_by_name("schema_version").ok();
        v
    }) else {
        // No readable local version yet; treat as not-yet-agreed and retry.
        return Ok(false);
    };

    let peers = session
        .query("SELECT schema_version FROM system.peers")
        .await
        .map_err(|e| OpError::Internal(format!("schema agreement (peers): {e}")))?
        .response_body()
        .map_err(|e| OpError::Internal(format!("schema agreement (peers): {e}")))?
        .into_rows()
        .unwrap_or_default();

    for peer in &peers {
        let peer_version: Option<uuid::Uuid> = peer.get_r_by_name("schema_version").ok();
        match peer_version {
            Some(v) if v == local_version => {}
            // A peer that disagrees, or whose version is not yet readable, means
            // the change has not propagated. Retry.
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// Record a migration in the `schema_history` table.
async fn record_migration(
    session: &Arc<CassandraSession>,
    keyspace: &str,
    filename: &str,
) -> OpResult<()> {
    // Extract version and description from filename
    let version = migration_version(filename)?;

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

/// The version a migration file records in `schema_history`: the integer of
/// its `V###__` prefix (`V001__initial_schema.cql` -> 1).
fn migration_version(filename: &str) -> OpResult<i32> {
    filename
        .strip_prefix("V")
        .and_then(|s| s.split("__").next())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| OpError::Internal(format!("Invalid migration filename: {filename}")))
}

/// The catalog migration files this binary carries that the catalog's
/// `schema_history` does not record, in order. Unlike the runner's per-file
/// check, a failed read is an error, not "pending".
pub(crate) async fn pending_catalog_migrations(
    session: &Arc<CassandraSession>,
    catalog_keyspace: &str,
) -> Result<Vec<&'static str>, extenddb_storage::error::StorageError> {
    use extenddb_storage::error::StorageError;
    let rows = crate::cassandra_util::query_rows::<StorageError>(
        session,
        &format!("SELECT version FROM {catalog_keyspace}.schema_history"),
        cdrs_tokio::query_values!(),
        "pending_catalog_migrations",
    )
    .await?;
    let mut recorded = std::collections::BTreeSet::new();
    for row in &rows {
        let version: i32 =
            crate::cassandra_util::get_column(row, "version", "pending_catalog_migrations")?;
        recorded.insert(version);
    }
    let mut pending = Vec::new();
    for (filename, _sql) in CATALOG_MIGRATIONS {
        let version = migration_version(filename).map_err(|e| match e {
            OpError::Validation(m)
            | OpError::AlreadyExists(m)
            | OpError::NotFound(m)
            | OpError::HasDependents(m)
            | OpError::Internal(m) => StorageError::Internal(m),
        })?;
        if !recorded.contains(&version) {
            pending.push(*filename);
        }
    }
    Ok(pending)
}

#[cfg(test)]
mod tests {
    #[test]
    fn every_migration_file_has_a_distinct_version() {
        let mut seen = std::collections::BTreeSet::new();
        for (filename, _) in super::CATALOG_MIGRATIONS {
            let v = super::migration_version(filename).expect(filename);
            assert!(seen.insert(v), "{filename}: version {v} repeated");
        }
        assert_eq!(
            super::migration_version("V005__backup_definitions_and_restore_registration.cql")
                .expect("V005"),
            5
        );
        assert!(super::migration_version("005_x.cql").is_err());
    }
}
