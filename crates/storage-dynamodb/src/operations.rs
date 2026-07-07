// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB backend implementation of `OperationsEngine`.
//!
//! The DynamoDB backend stores its catalog in Postgres, so
//! `parse_connection_string`, `redact_connection_string`, and `is_sensitive_key`
//! operate on Postgres connection-string semantics.
//!
//! `validate_identifier` uses DynamoDB table-name rules (length 3–255,
//! characters from `[A-Za-z0-9_.-]`).
//!
//! `catalog_version` returns the same version as `PostgresOperationsEngine`
//! because the catalog schema itself is Postgres.

use extenddb_storage::error::StorageError;
use extenddb_storage::operations::{ConnectionParts, OperationsEngine};

/// DynamoDB operations engine for ddbo CLI commands.
///
/// This is a unit struct (no fields) because all behavior is either
/// pure logic or delegates to `extenddb_storage_postgres`.
pub struct DynamoOperationsEngine;

impl OperationsEngine for DynamoOperationsEngine {
    /// Parse a Postgres catalog connection string.
    ///
    /// The DynamoDB backend's connection string is a Postgres URL pointing at
    /// the catalog database. Delegates to `extenddb_storage_postgres` parsing.
    fn parse_connection_string(&self, s: &str) -> Result<ConnectionParts, StorageError> {
        let parts = extenddb_storage_postgres::parse_connection_string(s)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(ConnectionParts {
            host: parts.host,
            port: parts.port,
            user: parts.user,
            password: parts.password,
            database: parts.database,
        })
    }

    /// Redact the password from a Postgres connection string.
    ///
    /// Handles `postgresql://user:password@host:port/database` format — same
    /// as the Postgres backend because the catalog URL has that shape.
    fn redact_connection_string(&self, s: &str) -> String {
        // Redact password from postgresql://user:password@host:port/database
        if let Some(at) = s.find('@')
            && let Some(colon) = s[..at].rfind(':')
        {
            let scheme_end = s.find("://").map_or(0, |i| i + 3);
            if colon >= scheme_end {
                return format!("{}:***@{}", &s[..colon], &s[at + 1..]);
            }
        }
        s.to_owned()
    }

    /// Validate a DynamoDB table name / identifier.
    ///
    /// DynamoDB table names must be between 3 and 255 characters and consist
    /// solely of letters (`A–Z`, `a–z`), digits (`0–9`), underscores (`_`),
    /// hyphens (`-`), and dots (`.`).
    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError> {
        let len = name.len();
        if !(3..=255).contains(&len) {
            return Err(StorageError::Validation(format!(
                "{label} '{name}' is not a valid DynamoDB identifier: \
                 length {len} is outside the allowed range 3–255"
            )));
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        {
            return Err(StorageError::Validation(format!(
                "{label} '{name}' is not a valid DynamoDB identifier: \
                 only [A-Za-z0-9_.-] are allowed"
            )));
        }
        Ok(())
    }

    /// Return the catalog schema version.
    ///
    /// The catalog lives in Postgres, so we return the same version string
    /// that `PostgresOperationsEngine::catalog_version()` returns — they share
    /// the same `CATALOG_VERSION` constant.
    fn catalog_version(&self) -> String {
        extenddb_storage_postgres::CATALOG_VERSION.to_string()
    }

    /// Check whether a configuration key holds sensitive data.
    ///
    /// Mirrors the Postgres backend's logic: true for keys whose lowercase form
    /// contains `"connection_string"`, `"password"`, `"secret"`, `"token"`, or
    /// `"encryption_key"`.
    fn is_sensitive_key(&self, key: &str) -> bool {
        let lower = key.to_lowercase();
        [
            "connection_string",
            "password",
            "secret",
            "token",
            "encryption_key",
        ]
        .iter()
        .any(|pattern| lower.contains(pattern))
    }
}
