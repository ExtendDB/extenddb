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
}

impl std::fmt::Display for DiagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionFailed(msg) => write!(f, "Connection failed: {msg}"),
            Self::QueryFailed(msg) => write!(f, "Query failed: {msg}"),
        }
    }
}

impl std::error::Error for DiagError {}

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
}
