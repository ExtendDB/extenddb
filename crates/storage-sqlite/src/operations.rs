// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite implementation of `OperationsEngine`.

use extenddb_storage::error::StorageError;
use extenddb_storage::operations::{ConnectionParts, OperationsEngine};

/// SQLite operations engine for ExtendDB CLI commands.
pub struct SqliteOperationsEngine;

impl OperationsEngine for SqliteOperationsEngine {
    fn parse_connection_string(&self, s: &str) -> Result<ConnectionParts, StorageError> {
        // SQLite connection strings are just file paths or sqlite:// URLs.
        // We map them to ConnectionParts with dummy host/port/user/password.
        let path = s
            .strip_prefix("sqlite://")
            .or_else(|| s.strip_prefix("sqlite:"))
            .unwrap_or(s);
        let path = path.trim_start_matches('/');
        Ok(ConnectionParts {
            host: "localhost".to_owned(),
            port: 0,
            user: String::new(),
            password: String::new(),
            database: path.to_owned(),
        })
    }

    fn redact_connection_string(&self, s: &str) -> String {
        // SQLite paths don't contain passwords.
        s.to_owned()
    }

    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError> {
        if name.contains('"') {
            return Err(StorageError::Internal(format!(
                "{label} must not contain double quotes"
            )));
        }
        if name.contains('\0') {
            return Err(StorageError::Internal(format!(
                "{label} must not contain null bytes"
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
        crate::engine::CATALOG_VERSION.to_string()
    }

    fn is_sensitive_key(&self, key: &str) -> bool {
        let lower = key.to_lowercase();
        [
            "path",
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
