// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `OperationsEngine` implementation for the SQLite backend.
//!
//! Provides the backend-specific CLI helpers (connection-string parsing,
//! redaction, identifier validation, catalog version) used by `extenddb`
//! lifecycle commands. SQLite connection strings are filesystem paths with no
//! embedded credentials, so redaction is a no-op.

use extenddb_storage::error::StorageError;
use extenddb_storage::operations::{ConnectionParts, OperationsEngine};

use crate::schema::CATALOG_VERSION;

/// Backend operations for the SQLite engine.
pub(crate) struct SqliteOperationsEngine;

impl OperationsEngine for SqliteOperationsEngine {
    fn parse_connection_string(&self, s: &str) -> Result<ConnectionParts, StorageError> {
        // A SQLite "connection string" is just a file path (or :memory:).
        Ok(ConnectionParts {
            host: s.to_owned(),
            port: 0,
            user: String::new(),
            password: String::new(),
            database: s.to_owned(),
        })
    }

    fn redact_connection_string(&self, s: &str) -> String {
        s.to_owned() // No credentials in a SQLite path.
    }

    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError> {
        if name.is_empty() {
            return Err(StorageError::Internal(format!("{label} must not be empty")));
        }
        // Defense-in-depth for any identifier interpolated into DDL. Backend
        // table identifiers are `_ddb_<uuid>` (engine-controlled), but reject
        // characters that could break a quoted identifier regardless.
        if name.contains('"') || name.contains('\0') {
            return Err(StorageError::Internal(format!(
                "{label} contains characters invalid for a SQL identifier"
            )));
        }
        if !name.is_ascii() {
            return Err(StorageError::Internal(format!(
                "{label} must contain only ASCII characters"
            )));
        }
        Ok(())
    }

    fn catalog_version(&self) -> String {
        CATALOG_VERSION.to_string()
    }

    fn is_sensitive_key(&self, _key: &str) -> bool {
        false // SQLite config carries no secrets.
    }
}
