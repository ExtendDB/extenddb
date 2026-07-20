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

/// Get the operations engine for a backend by name.
pub fn get_operations_engine(backend: &str) -> Result<&'static dyn OperationsEngine, StorageError> {
    if let Some(ops) = crate::registry::try_registry().and_then(|r| r.operations.get(backend)) {
        return Ok(*ops);
    }

    let available = list_operations_backends();

    Err(StorageError::Internal(format!(
        "Unknown backend: {backend}. Available backends: {}",
        available.join(", ")
    )))
}

/// List all registered backend names.
#[must_use]
pub fn list_operations_backends() -> Vec<&'static str> {
    crate::registry::try_registry()
        .map(|r| r.operations.keys().copied().collect())
        .unwrap_or_default()
}

// Convenience functions that delegate to the operations engine

/// Get the catalog version for a backend.
pub fn catalog_version(backend: &str) -> Result<String, StorageError> {
    get_operations_engine(backend).map(OperationsEngine::catalog_version)
}

/// Redact sensitive information from a connection string.
pub fn redact_connection_string(backend: &str, s: &str) -> Result<String, StorageError> {
    get_operations_engine(backend).map(|ops| ops.redact_connection_string(s))
}

/// Parse a connection string into components.
pub fn parse_connection_string(backend: &str, s: &str) -> Result<ConnectionParts, StorageError> {
    get_operations_engine(backend)?.parse_connection_string(s)
}

/// Validate an identifier for DDL safety.
pub fn validate_identifier(backend: &str, name: &str, label: &str) -> Result<(), StorageError> {
    get_operations_engine(backend)?.validate_identifier(name, label)
}

/// Check if a configuration key contains sensitive data.
pub fn is_sensitive_key(backend: &str, key: &str) -> Result<bool, StorageError> {
    get_operations_engine(backend).map(|ops| ops.is_sensitive_key(key))
}
