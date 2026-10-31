// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pins `extenddb.sample.toml` to the configuration structs of this crate and
//! to `LimitsConfig`. Each `[storage.<backend>]` section is pinned by its
//! backend crate. `docs/configuration.md` is generated from the sample, so a
//! field added to a struct without a sample line fails here before the page
//! can go stale.

use crate::sample_toml_support as support;
use crate::{
    AuthCacheConfigToml, AuthConfig, ImportExportPathConfig, LoggingConfig, ServerConfig, TlsConfig,
};
use extenddb_core::limits::LimitsConfig;

#[test]
fn top_level_keys_are_the_optional_app_config_fields() {
    let root = support::sample();
    assert_eq!(
        support::scalar_keys(&root),
        ["docs_dir", "import_export_root", "max_import_bytes"]
    );
}

/// The sections of the sample are the struct-valued fields of `AppConfig`
/// (`import` and `export` are the serde names of `import_config` and
/// `export_config`).
#[test]
fn root_sections_are_the_app_config_tables() {
    let root = support::sample();
    assert_eq!(
        support::table_keys(&root),
        [
            "auth", "export", "import", "limits", "logging", "server", "storage"
        ]
    );
}

/// Keys whose description carries the "no default" marker (the generator
/// shows them as none with an example) are exactly the keys the pin tests
/// exclude from value comparison plus the keys with no struct default at all:
/// `backend` (read by the custom `StorageConfig` deserializer), the three
/// optional top-level keys, and the two required backend keys.
#[test]
fn no_default_marker_matches_the_example_keys() {
    let mut expected: Vec<&str> = vec![
        // Excluded from value comparison in the pin tests.
        "server.throttling_enabled",
        "storage.postgres.catalog_pool_size",
        "storage.mongodb.max_connections",
        "storage.mongodb.max_catalog_connections",
        "storage.cassandra.username",
        "storage.cassandra.password",
        // No struct default.
        "storage.backend",
        "docs_dir",
        "import_export_root",
        "max_import_bytes",
        "storage.mongodb.connection_string",
        "storage.cassandra.contact_points",
    ];
    expected.sort_unstable();
    assert_eq!(support::no_default_keys(), expected);
}

#[test]
fn storage_section_names_only_the_backend() {
    let root = support::sample();
    let storage = support::table_at(&root, "storage");
    assert_eq!(support::scalar_keys(storage), ["backend"]);
    let mut backends: Vec<&str> = storage
        .iter()
        .filter(|(_, v)| v.is_table())
        .map(|(k, _)| k.as_str())
        .collect();
    backends.sort_unstable();
    assert_eq!(backends, ["cassandra", "mongodb", "postgres", "sqlite"]);
}

#[test]
fn server_section_matches_server_config() {
    let root = support::sample();
    // `throttling_enabled` is `Option<bool>` with no default; the sample shows
    // `false` as the example and its value is not compared.
    support::pin(
        &root,
        "server",
        &["throttling_enabled"],
        &ServerConfig::default(),
    );
}

#[test]
fn server_tls_section_matches_tls_config() {
    let root = support::sample();
    support::pin(&root, "server.tls", &[], &TlsConfig::default());
}

#[test]
fn auth_section_matches_auth_config() {
    let root = support::sample();
    support::pin(&root, "auth", &[], &AuthConfig::default());
}

#[test]
fn auth_cache_section_matches_auth_cache_config() {
    let root = support::sample();
    support::pin(&root, "auth.cache", &[], &AuthCacheConfigToml::default());
}

#[test]
fn limits_section_matches_limits_config() {
    let root = support::sample();
    support::pin(&root, "limits", &[], &LimitsConfig::default());
}

#[test]
fn logging_section_matches_logging_config() {
    let root = support::sample();
    support::pin(&root, "logging", &[], &LoggingConfig::default());
}

#[test]
fn import_and_export_sections_match_path_config() {
    let root = support::sample();
    support::pin(&root, "import", &[], &ImportExportPathConfig::default());
    support::pin(&root, "export", &[], &ImportExportPathConfig::default());
}
