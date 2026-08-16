// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Configuration display and redaction helpers for the console settings page.

use crate::AppConfig;

/// Keys whose values must be redacted in configuration displays.
///
/// The single source of truth for redaction patterns. Consumers (including the
/// console settings page) call [`should_redact`] rather than keeping their own
/// copy of this list.
const REDACTED_CONFIG_KEYS: &[&str] = &[
    "connection_string",
    "encryption_key",
    "password",
    "secret",
    "token",
];

/// Return `true` if a configuration or settings key's value must be redacted
/// before it is displayed.
///
/// Matching is case-insensitive and substring-based, so `DATA_DB_PASSWORD` and
/// `storage.connection_string` both redact.
#[must_use]
pub fn should_redact(key: &str) -> bool {
    let lower = key.to_lowercase();
    REDACTED_CONFIG_KEYS.iter().any(|p| lower.contains(p))
}

/// Return `"••••••••"` if `key` matches a redaction pattern, else `val`.
fn redact_if_sensitive(key: &str, val: &str) -> String {
    if should_redact(key) {
        "••••••••".to_owned()
    } else {
        val.to_owned()
    }
}

/// D9: Build static configuration entries for the console settings page.
///
/// Extracts key-value pairs from the parsed `AppConfig` and pre-redacts
/// sensitive values (connection strings, passwords, keys).
#[must_use]
pub fn build_config_entries(cfg: &AppConfig) -> Vec<(String, String)> {
    let r = redact_if_sensitive;
    let backend = &cfg.storage.backend;
    let mut entries = vec![
        ("server.bind_addr".into(), cfg.server.bind_addr.clone()),
        ("server.port".into(), cfg.server.port.to_string()),
        ("server.region".into(), cfg.server.region.clone()),
        ("server.run_dir".into(), cfg.server.run_dir.clone()),
        (
            "server.tls.enabled".into(),
            cfg.server.tls.enabled.to_string(),
        ),
        (
            "server.tls.cert_path".into(),
            cfg.server.tls.cert_path.clone(),
        ),
        (
            "server.tls.key_path".into(),
            cfg.server.tls.key_path.clone(),
        ),
        (
            "server.throttling_enabled".into(),
            cfg.server
                .throttling_enabled
                .map_or("none".into(), |b| b.to_string()),
        ),
        (
            format!("storage.{backend}.connection_string"),
            r("connection_string", cfg.storage.connection_config()),
        ),
        (
            format!("storage.{backend}.pool_size"),
            cfg.storage.max_connections().to_string(),
        ),
        (
            format!("storage.{backend}.catalog_pool_size"),
            cfg.storage.max_catalog_connections().to_string(),
        ),
        ("auth.provider".into(), cfg.auth.provider.clone()),
        ("logging.level".into(), cfg.logging.level.clone()),
        ("logging.format".into(), cfg.logging.format.clone()),
        ("docs_dir".into(), cfg.docs_dir.clone().unwrap_or_default()),
        (
            "import.paths".into(),
            if cfg.import_config.paths.is_empty() {
                "none".into()
            } else {
                cfg.import_config.paths.join(", ")
            },
        ),
        (
            "export.paths".into(),
            if cfg.export_config.paths.is_empty() {
                "none".into()
            } else {
                cfg.export_config.paths.join(", ")
            },
        ),
    ];

    // Commonly adjusted limits (full list in [limits] section of extenddb.sample.toml).
    let lim = &cfg.limits;
    entries.extend([
        (
            "limits.max_item_size_bytes".into(),
            lim.max_item_size_bytes.to_string(),
        ),
        (
            "limits.max_tables_per_account".into(),
            lim.max_tables_per_account.to_string(),
        ),
        (
            "limits.max_gsis_per_table".into(),
            lim.max_gsis_per_table.to_string(),
        ),
        (
            "limits.allow_multipart_table_keys".into(),
            lim.allow_multipart_table_keys.to_string(),
        ),
        (
            "limits.max_import_file_bytes".into(),
            lim.max_import_file_bytes.to_string(),
        ),
    ]);

    entries
}
