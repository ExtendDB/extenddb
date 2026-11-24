// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend operations for `ExtendDB` CLI commands (`BigtableOperationsEngine`).

use extenddb_storage::error::StorageError;
use extenddb_storage::operations::{ConnectionParts, OperationsEngine};

/// Split an emulator endpoint (`127.0.0.1:8086`, `[::1]:8086`,
/// `http://[::1]:8086`, or bare `localhost`) into `(host, port)`.
#[must_use]
pub fn parse_emulator_host_port(raw: &str) -> (String, u16) {
    let trimmed = raw
        .trim()
        .strip_prefix("http://")
        .or_else(|| raw.trim().strip_prefix("https://"))
        .unwrap_or_else(|| raw.trim());
    let addr = trimmed.split('/').next().unwrap_or(trimmed);
    if let Some(rest) = addr.strip_prefix('[')
        && let Some((ipv6, tail)) = rest.split_once(']')
    {
        let port = tail
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(8086);
        return (ipv6.to_owned(), port);
    }
    if let Some((h, p)) = addr.rsplit_once(':')
        && !h.contains(':')
        && let Ok(port) = p.parse::<u16>()
    {
        return (h.to_owned(), port);
    }
    (addr.to_owned(), 8086)
}

pub struct BigtableOperationsEngine;

impl OperationsEngine for BigtableOperationsEngine {
    fn parse_connection_string(&self, s: &str) -> Result<ConnectionParts, StorageError> {
        let cfg = crate::config::BigtableStorageConfig::from_connection_string(s)
            .map_err(StorageError::Internal)?;

        let (host, port) = if let Some(emu) = cfg.resolve_emulator_host() {
            parse_emulator_host_port(&emu)
        } else {
            ("bigtable.googleapis.com".to_owned(), 443)
        };

        Ok(ConnectionParts {
            host,
            port,
            user: String::new(),
            password: String::new(),
            database: cfg.instance_id,
        })
    }

    fn redact_connection_string(&self, s: &str) -> String {
        s.to_owned()
    }

    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError> {
        if name.is_empty() {
            return Err(StorageError::Internal(format!("{label} must not be empty")));
        }
        if name.len() > 50 {
            return Err(StorageError::Internal(format!(
                "{label} must be at most 50 characters, got {}",
                name.len()
            )));
        }
        let first = name
            .chars()
            .next()
            .ok_or_else(|| StorageError::Internal(format!("{label} must not be empty")))?;
        if !first.is_ascii_alphanumeric() && first != '_' {
            return Err(StorageError::Internal(format!(
                "{label} must start with alphanumeric or underscore"
            )));
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            return Err(StorageError::Internal(format!(
                "{label} contains invalid characters (only alphanumeric, _, -, . allowed)"
            )));
        }
        Ok(())
    }

    fn is_sensitive_key(&self, key: &str) -> bool {
        let lower = key.to_lowercase();
        lower.contains("credential") || lower.contains("key")
    }

    fn setting_default(&self, key: &str) -> Option<String> {
        use extenddb_core::settings_keys::INDEX_PROPAGATION_DELAY_MS;
        match key {
            "control_plane_delay_seconds" => {
                Some(crate::DEFAULT_CONTROL_PLANE_DELAY_SECONDS.to_string())
            }
            INDEX_PROPAGATION_DELAY_MS => {
                Some(crate::DEFAULT_INDEX_PROPAGATION_DELAY_MS.to_string())
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_emulator_host_port_handles_ipv4_ipv6_and_schemes() {
        assert_eq!(
            parse_emulator_host_port("127.0.0.1:8086"),
            ("127.0.0.1".to_owned(), 8086)
        );
        assert_eq!(
            parse_emulator_host_port("[::1]:8086"),
            ("::1".to_owned(), 8086)
        );
        assert_eq!(
            parse_emulator_host_port("http://[::1]:9090"),
            ("::1".to_owned(), 9090)
        );
        assert_eq!(parse_emulator_host_port("[::1]"), ("::1".to_owned(), 8086));
        assert_eq!(parse_emulator_host_port("::1"), ("::1".to_owned(), 8086));
        assert_eq!(
            parse_emulator_host_port("localhost"),
            ("localhost".to_owned(), 8086)
        );
    }
}
