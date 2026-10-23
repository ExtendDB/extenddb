// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend operations for ddbo CLI commands.
//!
//! The `OperationsEngine` trait provides backend-specific operations needed
//! by ddbo CLI commands (init, serve, destroy, verify, etc.). These operations
//! support the ddbo platform lifecycle, runtime operations, and diagnostics.
//!
//! This is distinct from:
//! - Data plane operations (`PutItem`, Query) — handled by `DataEngine`
//! - Control plane operations (`CreateTable`) — handled by `TableEngine`
//! - Management operations (IAM, accounts) — handled by `ManagementStore`

use crate::error::StorageError;

/// Backend-specific operations for ddbo CLI commands.
pub trait OperationsEngine: Send + Sync {
    /// Parse a backend-specific connection string into components.
    fn parse_connection_string(&self, s: &str) -> Result<ConnectionParts, StorageError>;

    /// Redact sensitive information from a connection string for logging.
    fn redact_connection_string(&self, s: &str) -> String;

    /// Validate an identifier (database name, table name, etc.) for DDL safety.
    ///
    /// This is used when constructing DDL statements with `format!` where
    /// parameterized queries are not possible (e.g., CREATE DATABASE, DROP DATABASE).
    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError>;

    /// Get the catalog schema version for this backend.
    fn catalog_version(&self) -> String;

    /// Check if a configuration key contains sensitive data that should be redacted.
    fn is_sensitive_key(&self, key: &str) -> bool;
}

/// Parsed connection string components (backend-agnostic).
#[derive(Debug, Clone)]
pub struct ConnectionParts {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

/// Get the operations engine of the installed backend.
///
/// # Errors
///
/// Returns an error if no backend has been installed.
pub fn get_operations_engine() -> Result<&'static dyn OperationsEngine, StorageError> {
    crate::backend::try_backend()
        .map(|b| b.operations)
        .ok_or_else(|| {
            StorageError::Internal(
                "no storage backend installed (set_backend was not called)".into(),
            )
        })
}

// Convenience functions that delegate to the operations engine

/// Get the catalog version for a backend.
pub fn catalog_version() -> Result<String, StorageError> {
    get_operations_engine().map(OperationsEngine::catalog_version)
}

/// Redact sensitive information from a connection string.
pub fn redact_connection_string(s: &str) -> Result<String, StorageError> {
    get_operations_engine().map(|ops| ops.redact_connection_string(s))
}

/// Parse a connection string into components.
pub fn parse_connection_string(s: &str) -> Result<ConnectionParts, StorageError> {
    get_operations_engine()?.parse_connection_string(s)
}

/// Validate an identifier for DDL safety.
pub fn validate_identifier(name: &str, label: &str) -> Result<(), StorageError> {
    get_operations_engine()?.validate_identifier(name, label)
}

/// Check if a configuration key contains sensitive data.
pub fn is_sensitive_key(key: &str) -> Result<bool, StorageError> {
    get_operations_engine().map(|ops| ops.is_sensitive_key(key))
}
