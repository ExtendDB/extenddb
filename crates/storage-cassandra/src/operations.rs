// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra implementation of `OperationsEngine`.

use extenddb_storage::error::StorageError;
use extenddb_storage::operations::{ConnectionParts, OperationsEngine};

/// Cassandra operations engine for CLI commands.
pub struct CassandraOperationsEngine;

impl OperationsEngine for CassandraOperationsEngine {
    fn parse_connection_string(&self, conn_str: &str) -> Result<ConnectionParts, StorageError> {
        // Cassandra connection string format: "host1:9042,host2:9042/keyspace_prefix"
        // Split on '/' to separate hosts from keyspace
        let (hosts_str, keyspace) = if let Some((h, k)) = conn_str.split_once('/') {
            (h, k.to_string())
        } else {
            (conn_str, "extenddb".to_string())
        };

        // Parse the first contact point for compatibility with ConnectionParts
        let first_contact = hosts_str
            .split(',')
            .next()
            .ok_or_else(|| StorageError::Internal("Empty connection string".to_string()))?;

        let parts: Vec<&str> = first_contact.split(':').collect();
        if parts.len() != 2 {
            return Err(StorageError::Internal(format!(
                "Invalid Cassandra contact point format: '{first_contact}'. Expected 'host:port'"
            )));
        }

        let host = parts[0].to_string();
        let port: u16 = parts[1]
            .parse()
            .map_err(|_| StorageError::Internal(format!("Invalid port number: '{}'", parts[1])))?;

        // Use keyspace_prefix as the "database" name for display
        Ok(ConnectionParts {
            host,
            port,
            database: format!("{keyspace}_catalog"),
            user: String::new(),
            password: String::new(),
        })
    }

    fn redact_connection_string(&self, conn_str: &str) -> String {
        // Cassandra connection strings don't contain passwords
        // Just return as-is
        conn_str.to_string()
    }

    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError> {
        // Cassandra identifier rules:
        // - Alphanumeric and underscore only
        // - Cannot start with a digit
        // - Max 48 characters (keyspace) or 48 characters (table)
        // - Case-insensitive (stored lowercase unless quoted)

        if name.is_empty() {
            return Err(StorageError::Internal(format!("{label} cannot be empty")));
        }

        if name.len() > 48 {
            return Err(StorageError::Internal(format!(
                "{label} '{name}' exceeds maximum length of 48 characters"
            )));
        }

        // Check first character (cannot be digit)
        if let Some(first_char) = name.chars().next()
            && first_char.is_ascii_digit()
        {
            return Err(StorageError::Internal(format!(
                "{label} '{name}' cannot start with a digit"
            )));
        }

        // Check all characters (alphanumeric + underscore only)
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(StorageError::Internal(format!(
                "{label} '{name}' contains invalid characters. Only alphanumeric and underscore allowed"
            )));
        }

        Ok(())
    }

    fn catalog_version(&self) -> String {
        "0.0.1".to_string()
    }

    fn is_sensitive_key(&self, key: &str) -> bool {
        key.contains("password") || key.contains("secret")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_connection_string_with_keyspace() {
        let ops = CassandraOperationsEngine;
        let result = ops
            .parse_connection_string("127.0.0.1:9042/extenddb")
            .unwrap();

        assert_eq!(result.host, "127.0.0.1");
        assert_eq!(result.port, 9042);
        assert_eq!(result.database, "extenddb_catalog");
    }

    #[test]
    fn test_parse_connection_string_without_keyspace() {
        let ops = CassandraOperationsEngine;
        let result = ops.parse_connection_string("127.0.0.1:9042").unwrap();

        assert_eq!(result.host, "127.0.0.1");
        assert_eq!(result.port, 9042);
        assert_eq!(result.database, "extenddb_catalog"); // Default keyspace
    }

    #[test]
    fn test_parse_connection_string_multiple_hosts() {
        let ops = CassandraOperationsEngine;
        let result = ops
            .parse_connection_string("host1:9042,host2:9042,host3:9042/production")
            .unwrap();

        // Should parse first host
        assert_eq!(result.host, "host1");
        assert_eq!(result.port, 9042);
        assert_eq!(result.database, "production_catalog");
    }

    #[test]
    fn test_parse_connection_string_invalid_port() {
        let ops = CassandraOperationsEngine;
        let result = ops.parse_connection_string("127.0.0.1:invalid/test");

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid port number")
        );
    }

    #[test]
    fn test_parse_connection_string_empty() {
        let ops = CassandraOperationsEngine;
        let result = ops.parse_connection_string("");

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Empty string splits to [""], which isn't empty, so we get "Invalid Cassandra contact point format"
        assert!(err_msg.contains("Invalid") || err_msg.contains("Empty"));
    }

    #[test]
    fn test_validate_identifier_valid() {
        let ops = CassandraOperationsEngine;
        assert!(ops.validate_identifier("test_keyspace", "keyspace").is_ok());
        assert!(ops.validate_identifier("my_table_123", "table").is_ok());
    }

    #[test]
    fn test_validate_identifier_starts_with_digit() {
        let ops = CassandraOperationsEngine;
        let result = ops.validate_identifier("123_test", "keyspace");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot start with a digit")
        );
    }

    #[test]
    fn test_validate_identifier_too_long() {
        let ops = CassandraOperationsEngine;
        let long_name = "a".repeat(49);
        let result = ops.validate_identifier(&long_name, "keyspace");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("exceeds maximum length")
        );
    }

    #[test]
    fn test_validate_identifier_invalid_chars() {
        let ops = CassandraOperationsEngine;
        let result = ops.validate_identifier("test-keyspace", "keyspace");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid characters")
        );
    }
}
