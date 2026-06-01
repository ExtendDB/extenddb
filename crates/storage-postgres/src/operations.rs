// Copyright 2026 DynamoDB Open contributors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL implementation of `OperationsEngine`.

use extenddb_storage::error::StorageError;
use extenddb_storage::operations::{ConnectionParts, OperationsEngine};

/// PostgreSQL operations engine for ddbo CLI commands.
pub struct PostgresOperationsEngine;

impl OperationsEngine for PostgresOperationsEngine {
    fn parse_connection_string(&self, s: &str) -> Result<ConnectionParts, StorageError> {
        let parts = crate::config::parse_connection_string(s)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Convert ConnParts to ConnectionParts
        Ok(ConnectionParts {
            host: parts.host,
            port: parts.port,
            user: parts.user,
            password: parts.password,
            database: parts.database,
        })
    }

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

    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError> {
        // PostgreSQL identifier validation for format!-based DDL.
        // Rejects double quotes, null bytes, and non-ASCII characters.
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
        crate::CATALOG_VERSION.to_string()
    }

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
