// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Configuration display and redaction helpers for the console settings page,
//! the management settings API and the `extenddb settings` CLI.

use extenddb_core::limits::LimitsConfig;

use crate::AppConfig;

/// Keys whose values must be redacted in configuration displays.
///
/// The single source of truth for redaction patterns. Consumers (the console
/// settings page, the management settings API and the `settings` CLI) call
/// [`should_redact`] rather than keeping their own copy of this list.
const REDACTED_CONFIG_KEYS: &[&str] = &[
    "connection_string",
    "encryption_key",
    "password",
    "secret",
    "token",
];

/// The text printed in place of a redacted value: eight U+2022 bullets.
///
/// The console settings page, the management settings API and the `settings`
/// CLI print this constant, so the three surfaces show the same mask.
pub const MASK: &str = "••••••••";

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

/// Return [`MASK`] if `key` matches a redaction pattern, else `val`.
fn redact_if_sensitive(key: &str, val: &str) -> String {
    if should_redact(key) {
        MASK.to_owned()
    } else {
        val.to_owned()
    }
}

fn display_pool_limit(value: Option<u32>) -> String {
    value.map_or_else(
        || "from connection string / driver default".to_owned(),
        |n| n.to_string(),
    )
}

/// D9: Build static configuration entries for the console settings page.
///
/// Extracts key-value pairs from the parsed `AppConfig` and pre-redacts
/// sensitive values (connection strings, passwords, keys).
#[must_use]
pub fn build_config_entries(cfg: &AppConfig) -> Vec<(String, String)> {
    let r = redact_if_sensitive;
    let backend = &cfg.storage.backend;
    let data_pool_size = display_pool_limit(cfg.storage.max_connections_override());
    let catalog_pool_size = display_pool_limit(cfg.storage.max_catalog_connections_override());
    // The S3 store's settings when it is the selected store, an absent
    // `[bulk.s3]` section taking every default; `none` otherwise.
    let default_s3 = crate::BulkS3Config::default();
    let bulk_s3 = cfg
        .bulk
        .as_ref()
        .filter(|b| b.store == crate::BulkStoreKind::S3)
        .map(|b| b.s3.as_ref().unwrap_or(&default_s3));
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
        (format!("storage.{backend}.pool_size"), data_pool_size),
        (
            format!("storage.{backend}.catalog_pool_size"),
            catalog_pool_size,
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
        (
            "bulk.store".into(),
            cfg.bulk.as_ref().map_or_else(
                || "disabled".into(),
                |b| match b.store {
                    crate::BulkStoreKind::Filesystem => "filesystem".to_owned(),
                    crate::BulkStoreKind::S3 => "s3".to_owned(),
                },
            ),
        ),
        (
            "bulk.filesystem.path".into(),
            cfg.bulk
                .as_ref()
                .and_then(|b| b.filesystem.as_ref())
                .map_or_else(|| "none".into(), |f| f.path.clone()),
        ),
        (
            "bulk.s3.region".into(),
            bulk_s3.map_or_else(
                || "none".into(),
                |s3| {
                    s3.region
                        .clone()
                        .unwrap_or_else(|| cfg.server.region.clone())
                },
            ),
        ),
        (
            "bulk.s3.endpoint_url".into(),
            bulk_s3.map_or_else(
                || "none".into(),
                |s3| s3.endpoint_url.clone().unwrap_or_else(|| "AWS S3".into()),
            ),
        ),
        (
            "bulk.s3.allow_insecure_endpoint".into(),
            bulk_s3.map_or_else(
                || "none".into(),
                |s3| s3.allow_insecure_endpoint.to_string(),
            ),
        ),
        (
            "bulk.s3.force_path_style".into(),
            bulk_s3.map_or_else(|| "none".into(), |s3| s3.force_path_style.to_string()),
        ),
    ];

    entries.extend(limits_entries(&cfg.limits));

    entries
}

/// Commonly adjusted `[limits]` keys (full list in the `[limits]` section of
/// extenddb.sample.toml).
fn limits_entries(lim: &LimitsConfig) -> Vec<(String, String)> {
    vec![
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
        (
            "limits.max_concurrent_bulk_operations".into(),
            lim.max_concurrent_bulk_operations.to_string(),
        ),
        (
            "limits.bulk_operation_retention_days".into(),
            lim.bulk_operation_retention_days.to_string(),
        ),
        (
            "limits.bulk_operation_worker_concurrency".into(),
            lim.bulk_operation_worker_concurrency.to_string(),
        ),
        (
            "limits.bulk_operation_worker_per_account".into(),
            lim.bulk_operation_worker_per_account.to_string(),
        ),
        (
            "limits.bulk_operation_lease_seconds".into(),
            lim.bulk_operation_lease_seconds.to_string(),
        ),
        (
            "limits.bulk_put_batch_size".into(),
            lim.bulk_put_batch_size.to_string(),
        ),
        (
            "limits.bulk_put_write_batch_rows".into(),
            lim.bulk_put_write_batch_rows.to_string(),
        ),
        (
            "limits.max_bulk_input_bytes".into(),
            lim.max_bulk_input_bytes.to_string(),
        ),
        (
            "limits.max_bulk_input_files".into(),
            lim.max_bulk_input_files.to_string(),
        ),
        (
            "limits.max_bulk_input_object_bytes".into(),
            lim.max_bulk_input_object_bytes.to_string(),
        ),
        (
            "limits.max_bulk_line_bytes".into(),
            lim.max_bulk_line_bytes.to_string(),
        ),
        (
            "limits.max_bulk_item_count".into(),
            lim.max_bulk_item_count.to_string(),
        ),
        (
            "limits.max_bulk_report_records".into(),
            lim.max_bulk_report_records.to_string(),
        ),
        (
            "limits.max_bulk_report_bytes".into(),
            lim.max_bulk_report_bytes.to_string(),
        ),
        (
            "limits.max_bulk_failed_starts_per_hour".into(),
            lim.max_bulk_failed_starts_per_hour.to_string(),
        ),
        (
            "limits.bulk_put_write_units_per_second".into(),
            lim.bulk_put_write_units_per_second.to_string(),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::{display_pool_limit, should_redact};

    #[test]
    fn explicit_pool_limit_is_displayed() {
        assert_eq!(display_pool_limit(Some(31)), "31");
    }

    #[test]
    fn absent_pool_limit_identifies_its_source() {
        assert_eq!(
            display_pool_limit(None),
            "from connection string / driver default"
        );
    }

    /// Each of the five patterns redacts as the whole key and as part of a
    /// longer key, in any letter case; a key that contains none of them does
    /// not. The admin guide's settings section names all five.
    #[test]
    fn should_redact_matches_each_pattern_in_any_case() {
        for key in [
            "encryption_key",
            "Encryption_KEY",
            "connection_string",
            "storage.Connection_String",
            "password",
            "DATA_DB_Password",
            "secret",
            "Client_SECRET",
            "token",
            "session_Token",
        ] {
            assert!(should_redact(key), "{key} must redact");
        }
        for key in ["log_level", "Log_Level", "catalog_version"] {
            assert!(!should_redact(key), "{key} must not redact");
        }
    }
}
