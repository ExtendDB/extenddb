// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Diagnostics trait for deployment health checks and verification.

use futures::future::BoxFuture;

/// Result type for diagnostics operations.
pub type DiagResult<T> = Result<T, DiagError>;

/// Error type for diagnostics operations.
#[derive(Debug)]
pub enum DiagError {
    ConnectionFailed(String),
    QueryFailed(String),
    /// The installed backend does not implement the operation.
    Unsupported(String),
    /// The deployment lacks configuration the operation needs; the message
    /// says what to run.
    NotConfigured(String),
}

impl std::fmt::Display for DiagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionFailed(msg) => write!(f, "Connection failed: {msg}"),
            Self::QueryFailed(msg) => write!(f, "Query failed: {msg}"),
            Self::Unsupported(msg) => write!(f, "Not supported: {msg}"),
            Self::NotConfigured(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for DiagError {}

/// A data relation the catalog names, with the catalog object it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRelation {
    /// The relation's name in the data database.
    pub relation: String,
    /// The catalog object that owns the relation, written for an operator, for
    /// example `table 123456789012/orders`.
    pub owner: String,
}

/// What [`DiagnosticsStore::check_storage_relations`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageRelationCheck {
    /// Data relations that no catalog row names, sorted by name.
    pub orphaned: Vec<String>,
    /// Data relations that a catalog row names and the data database does not
    /// have, sorted by relation name.
    pub missing: Vec<CatalogRelation>,
}

/// What [`DiagnosticsStore::drop_orphaned_relations`] did with one relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanDrop {
    /// The relation was dropped here.
    Dropped,
    /// The relation no longer existed when the drop ran: another session, for
    /// example a server's `DeleteTable`, had dropped it first.
    AlreadyGone,
    /// The relation was kept, for the reason given.
    Kept(String),
    /// The drop failed with the error given, and the relation is still there.
    Failed(String),
}

fn unsupported(operation: &str) -> DiagError {
    DiagError::Unsupported(format!(
        "{operation} is not implemented for this storage backend"
    ))
}

/// Diagnostic and verification operations for deployment health checks.
///
/// Used by `ddbo verify` to check catalog integrity and enumerate resources.
pub trait DiagnosticsStore: Send + Sync {
    /// Count the number of `DynamoDB` tables in the catalog.
    fn count_tables(&self) -> BoxFuture<'_, DiagResult<i64>>;

    /// Count the number of secondary indexes in the catalog.
    fn count_indexes(&self) -> BoxFuture<'_, DiagResult<i64>>;

    /// Test connection to the data database.
    ///
    /// Returns the database name on success, or an error if connection fails.
    fn test_data_database_connection(&self) -> BoxFuture<'_, DiagResult<String>>;

    /// Compare the data relations the backend created with the catalog, for
    /// `extenddb catalog-check`.
    ///
    /// A relation is orphaned when no catalog row names it, whatever the row's
    /// status. A relation is missing when a catalog row names it, neither the
    /// row nor its table is being created or deleted, and the data database
    /// does not have the relation.
    ///
    /// The default reports that the backend does not implement the check.
    fn check_storage_relations(&self) -> BoxFuture<'_, DiagResult<StorageRelationCheck>> {
        Box::pin(async { Err(unsupported("checking data relations against the catalog")) })
    }

    /// Drop relations that [`Self::check_storage_relations`] reported as
    /// orphaned, for `extenddb catalog-check --fix`.
    ///
    /// The backend waits for the catalog changes in flight that could name a
    /// relation, checks each relation against the catalog again, and keeps
    /// any relation a catalog row names by then. Each drop waits for its own
    /// lock for a short time only and is reported failed when it cannot get
    /// it. Returns one outcome per relation, in the order given.
    ///
    /// The default reports that the backend does not implement the drop.
    fn drop_orphaned_relations(
        &self,
        _orphaned: Vec<String>,
    ) -> BoxFuture<'_, DiagResult<Vec<(String, OrphanDrop)>>> {
        Box::pin(async { Err(unsupported("dropping orphaned data relations")) })
    }
}
