use extenddb_storage::error::StorageError;
use extenddb_storage::operations::{ConnectionParts, OperationsEngine};

pub struct BigtableOperationsEngine;

// Catalog version is defined as 0.1.0 in bootstrapper.rs.
const CATALOG_VERSION: &str = "0.1.0";

impl OperationsEngine for BigtableOperationsEngine {
    fn parse_connection_string(&self, s: &str) -> Result<ConnectionParts, StorageError> {
        let cfg = crate::config::BigtableStorageConfig::from_connection_string(s)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Map Bigtable config to ConnectionParts.
        // Bigtable doesn't have password or user, so we leave them empty.
        // Database is mapped to instance_id.
        // Host/Port are mapped to emulator_host if present, otherwise default to googleapis.com:443.
        let (host, port) = if let Some(emu) = &cfg.emulator_host {
            if let Some((h, p)) = emu.split_once(':') {
                (h.to_owned(), p.parse().unwrap_or(8086))
            } else {
                (emu.clone(), 8086)
            }
        } else {
            ("bigtable.googleapis.com".to_owned(), 443)
        };

        Ok(ConnectionParts {
            host,
            port,
            user: "".to_string(),
            password: "".to_string(),
            database: cfg.instance_id,
        })
    }

    fn redact_connection_string(&self, s: &str) -> String {
        // Bigtable connection string has no password, so nothing to redact.
        s.to_owned()
    }

    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError> {
        // Bigtable table name validation.
        // BigTable table IDs must match: `[_a-zA-Z0-9][-_.a-zA-Z0-9]*` and be under 50 chars.
        if name.is_empty() {
            return Err(StorageError::Internal(format!("{label} must not be empty")));
        }
        if name.len() > 50 {
            return Err(StorageError::Internal(format!(
                "{label} must be at most 50 characters, got {}",
                name.len()
            )));
        }
        let first = name.chars().next().ok_or_else(|| StorageError::Internal(format!("{label} must not be empty")))?;
        if !first.is_ascii_alphanumeric() && first != '_' {
            return Err(StorageError::Internal(format!(
                "{label} must start with alphanumeric or underscore"
            )));
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.') {
            return Err(StorageError::Internal(format!(
                "{label} contains invalid characters (only alphanumeric, _, -, . allowed)"
            )));
        }
        Ok(())
    }

    fn catalog_version(&self) -> String {
        CATALOG_VERSION.to_string()
    }

    fn is_sensitive_key(&self, key: &str) -> bool {
        let lower = key.to_lowercase();
        lower.contains("credential") || lower.contains("key")
    }
}
