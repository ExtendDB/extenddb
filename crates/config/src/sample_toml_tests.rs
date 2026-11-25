// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pins `extenddb.sample.toml` to the configuration structs of this crate, to
//! `LimitsConfig` and to the backup store structs this crate re-exports. Each
//! `[storage.<backend>]` section is pinned by its backend crate.
//! `docs/configuration.md` is generated from the sample, so a field added to
//! a struct without a sample line fails here before the page can go stale.

use std::collections::BTreeSet;

use crate::backup::{BackupConfig, BackupStoreConfig, FilesystemStoreConfig, S3StoreConfig};
use crate::sample_toml_support as support;
use crate::{
    AppConfig, AuthCacheConfigToml, AuthConfig, EncryptionConfig, ImportExportPathConfig,
    LoggingConfig, ServerConfig, TlsConfig,
};
use extenddb_core::limits::LimitsConfig;

/// The top-level keys and the sections of the sample are the fields of
/// `AppConfig`, under their serde names (`import` and `export` for
/// `import_config` and `export_config`). The sections are its eight
/// struct-valued fields; the tests below pin each one's keys.
#[test]
fn root_keys_are_the_app_config_fields() {
    let root = support::sample();
    assert_eq!(
        support::table_keys(&root),
        [
            "auth", "backup", "export", "import", "limits", "logging", "server", "storage"
        ]
    );
    let keys: BTreeSet<String> = root.keys().cloned().collect();
    assert_eq!(keys, support::struct_fields::<AppConfig>());
}

/// Keys whose description carries the "no default" marker (the generator
/// shows them as none with an example) are exactly the keys the pin tests
/// exclude from value comparison plus the keys with no struct default at all:
/// `backend` (read by the custom `StorageConfig` deserializer), the three
/// optional top-level keys, the two required backend keys and the two
/// required backup store keys.
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
        "auth.encryption.key_file",
        "backup.spool_path",
        "backup.store.s3.region",
        "backup.store.s3.endpoint",
        // No struct default.
        "storage.backend",
        "docs_dir",
        "import_export_root",
        "max_import_bytes",
        "storage.mongodb.connection_string",
        "storage.cassandra.contact_points",
        "backup.store.filesystem.path",
        "backup.store.s3.bucket",
    ];
    expected.sort_unstable();
    assert_eq!(support::no_default_keys(), expected);
}

#[test]
fn storage_section_names_only_the_backend() {
    let root = support::sample();
    let storage = support::table_at(&root, "storage");
    assert_eq!(support::scalar_keys(storage), ["backend"]);
    assert_eq!(
        support::table_keys(storage),
        ["cassandra", "mongodb", "postgres", "sqlite"]
    );
}

#[test]
fn server_section_matches_server_config() {
    let root = support::sample();
    // `throttling_enabled` is `Option<bool>` with no default; the sample shows
    // `false` as the example and its value is not compared.
    support::pin_with_default::<ServerConfig>(&root, "server", &["throttling_enabled"]);
}

#[test]
fn server_tls_section_matches_tls_config() {
    let root = support::sample();
    support::pin_with_default::<TlsConfig>(&root, "server.tls", &[]);
}

#[test]
fn auth_section_matches_auth_config() {
    let root = support::sample();
    support::pin_with_default::<AuthConfig>(&root, "auth", &[]);
}

#[test]
fn auth_cache_section_matches_auth_cache_config() {
    let root = support::sample();
    support::pin_with_default::<AuthCacheConfigToml>(&root, "auth.cache", &[]);
}

#[test]
fn auth_encryption_section_matches_encryption_config() {
    let root = support::sample();
    // `key_file` is `Option<String>` with no default; the sample shows an
    // example path and its value is not compared.
    support::pin_with_default::<EncryptionConfig>(&root, "auth.encryption", &["key_file"]);
}

#[test]
fn limits_section_matches_limits_config() {
    let root = support::sample();
    support::pin_with_default::<LimitsConfig>(&root, "limits", &[]);
}

#[test]
fn logging_section_matches_logging_config() {
    let root = support::sample();
    support::pin_with_default::<LoggingConfig>(&root, "logging", &[]);
}

#[test]
fn backup_section_matches_backup_config() {
    let root = support::sample();
    // `spool_path` is `Option<String>` with no default; the sample shows an
    // example path and its value is not compared.
    support::pin_with_default::<BackupConfig>(&root, "backup", &["spool_path"]);
}

/// `path`, `bucket`, `region` and `endpoint` have no default, so the sample's
/// values for them are examples and are not compared.
#[test]
fn backup_store_sections_match_the_store_config() {
    let root = support::sample();
    support::pin::<BackupStoreConfig>(&root, "backup.store", &[]);
    support::pin::<FilesystemStoreConfig>(&root, "backup.store.filesystem", &["path"]);
    support::pin::<S3StoreConfig>(&root, "backup.store.s3", &["bucket", "region", "endpoint"]);
}

#[test]
fn import_and_export_sections_match_path_config() {
    let root = support::sample();
    support::pin_with_default::<ImportExportPathConfig>(&root, "import", &[]);
    support::pin_with_default::<ImportExportPathConfig>(&root, "export", &[]);
}
