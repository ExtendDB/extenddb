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
    // `false` as the example.
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
