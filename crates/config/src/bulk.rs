// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Bulk operation store configuration (`[bulk]` section).

use serde::Deserialize;

use crate::BulkS3Config;

/// Store root dev mode substitutes when the `[bulk]` section is absent,
/// relative to the process working directory. Named in the dev-mode banner
/// and resolved to its canonical form by the server at startup.
pub const DEV_DEFAULT_BULK_PATH: &str = "./.extenddb/buckets";

/// Configuration for the bulk operation store (`[bulk]` section).
///
/// Bulk put reads its input files and writes its error reports through one
/// store: the filesystem store beneath one root, namespaced per account
/// (`{path}/{account id}/{S3Bucket}/...`), or the S3 store, which reads and
/// writes the buckets the requests name in AWS S3 or an S3-compatible
/// service. The store is independent of the `[import]` and `[export]`
/// sections.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BulkConfig {
    /// Store implementation, `filesystem` (the default) or `s3`.
    #[serde(default)]
    pub store: BulkStoreKind,
    /// Filesystem store settings. Required when `store = "filesystem"`;
    /// enforced at startup rather than by serde so the error names the key.
    pub filesystem: Option<BulkFilesystemConfig>,
    /// S3 store settings. Optional when `store = "s3"`: an absent section
    /// takes every default.
    pub s3: Option<BulkS3Config>,
}

/// Store implementation selector for `[bulk] store`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BulkStoreKind {
    /// Local filesystem store rooted at `[bulk.filesystem] path`.
    #[default]
    Filesystem,
    /// The S3 store configured by `[bulk.s3]`.
    S3,
}

/// Settings for the filesystem bulk store (`[bulk.filesystem]` section).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BulkFilesystemConfig {
    /// The store root. Inputs are read and reports are written beneath
    /// `{path}/{account id}`. Created at startup if missing.
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Narrow view for the `[bulk]` tests. `AppConfig` cannot be
    /// deserialized here because `StorageConfig` requires an installed
    /// backend, which the config crate's unit tests do not have; the
    /// `[bulk]` section deserializes independently of storage, so this
    /// struct exercises the same field the full config carries.
    #[derive(Debug, serde::Deserialize)]
    struct BulkSection {
        bulk: Option<BulkConfig>,
    }

    fn parse_bulk(toml_text: &str) -> Result<BulkSection, config::ConfigError> {
        config::Config::builder()
            .add_source(config::File::from_str(toml_text, config::FileFormat::Toml))
            .build()?
            .try_deserialize()
    }

    #[test]
    fn bulk_section_absent_is_none() {
        let parsed = parse_bulk("").unwrap();
        assert!(parsed.bulk.is_none());
    }

    #[test]
    fn bulk_store_defaults_to_filesystem() {
        let parsed =
            parse_bulk("[bulk]\n[bulk.filesystem]\npath = \"/var/lib/extenddb/buckets\"\n")
                .unwrap();
        let bulk = parsed.bulk.unwrap();
        assert_eq!(bulk.store, BulkStoreKind::Filesystem);
        assert_eq!(bulk.filesystem.unwrap().path, "/var/lib/extenddb/buckets");
    }

    #[test]
    fn bulk_store_s3_deserializes() {
        // `s3` selects the S3 store; its section is optional.
        let parsed = parse_bulk("[bulk]\nstore = \"s3\"\n").unwrap();
        let bulk = parsed.bulk.unwrap();
        assert_eq!(bulk.store, BulkStoreKind::S3);
        assert!(bulk.filesystem.is_none());
    }

    #[test]
    fn bulk_store_unknown_value_is_rejected() {
        assert!(parse_bulk("[bulk]\nstore = \"tape\"\n").is_err());
    }

    /// The configuration layer matches enum values case-insensitively, so
    /// every spelling of `s3` deserializes to the one `S3` variant, every
    /// spelling of `filesystem` to `Filesystem`, and anything else is a
    /// parse error: no spelling reaches the resolution code as a third
    /// value.
    #[test]
    fn bulk_store_case_spellings_land_on_the_two_variants() {
        for spelling in ["s3", "S3"] {
            let parsed = parse_bulk(&format!("[bulk]\nstore = \"{spelling}\"\n")).unwrap();
            assert_eq!(
                parsed.bulk.unwrap().store,
                BulkStoreKind::S3,
                "store = {spelling:?} must land on the S3 variant"
            );
        }
        // Case spellings of filesystem land on Filesystem.
        for spelling in ["filesystem", "Filesystem", "FILESYSTEM"] {
            let parsed = parse_bulk(&format!("[bulk]\nstore = \"{spelling}\"\n")).unwrap();
            assert_eq!(parsed.bulk.unwrap().store, BulkStoreKind::Filesystem);
        }
        // Anything else, whitespace-padded s3 included, is a parse error.
        for spelling in ["s3 ", " s3", "sthree", "s-3"] {
            assert!(
                parse_bulk(&format!("[bulk]\nstore = \"{spelling}\"\n")).is_err(),
                "store = {spelling:?} must be refused by the parser"
            );
        }
    }

    #[test]
    fn bulk_unknown_key_is_rejected() {
        assert!(parse_bulk("[bulk]\nroot = \"/somewhere\"\n").is_err());
        assert!(parse_bulk("[bulk]\n[bulk.filesystem]\npaths = [\"/a\"]\n").is_err());
        assert!(parse_bulk("[bulk]\nstore = \"s3\"\n[bulk.s3]\nprofile = \"loads\"\n").is_err());
        assert!(
            parse_bulk("[bulk]\nstore = \"s3\"\n[bulk.s3]\nendpoint = \"https://a\"\n").is_err()
        );
    }

    #[test]
    fn bulk_environment_overrides_apply() {
        // The same Environment source `load` uses, fed a fixed map instead of
        // the process environment so the test is deterministic and parallel
        // safe. `EXTENDDB__BULK__STORE`, `EXTENDDB__BULK__FILESYSTEM__PATH`
        // and the four `EXTENDDB__BULK__S3__*` variables must override the
        // file values; the booleans arrive as strings and are converted.
        let vars = std::collections::HashMap::from([
            ("EXTENDDB__BULK__STORE".to_owned(), "s3".to_owned()),
            (
                "EXTENDDB__BULK__FILESYSTEM__PATH".to_owned(),
                "/from-env/buckets".to_owned(),
            ),
            (
                "EXTENDDB__BULK__S3__REGION".to_owned(),
                "eu-central-1".to_owned(),
            ),
            (
                "EXTENDDB__BULK__S3__ENDPOINT_URL".to_owned(),
                "http://127.0.0.1:7070".to_owned(),
            ),
            (
                "EXTENDDB__BULK__S3__ALLOW_INSECURE_ENDPOINT".to_owned(),
                "true".to_owned(),
            ),
            (
                "EXTENDDB__BULK__S3__FORCE_PATH_STYLE".to_owned(),
                "true".to_owned(),
            ),
        ]);
        let cfg = config::Config::builder()
            .add_source(config::File::from_str(
                "[bulk]\nstore = \"filesystem\"\n[bulk.filesystem]\npath = \"/from-file\"\n\
                 [bulk.s3]\nregion = \"us-east-1\"\nendpoint_url = \"https://from-file\"\n\
                 allow_insecure_endpoint = false\nforce_path_style = false\n",
                config::FileFormat::Toml,
            ))
            .add_source(
                config::Environment::with_prefix("EXTENDDB")
                    .separator("__")
                    .source(Some(vars)),
            )
            .build()
            .unwrap();
        let parsed: BulkSection = cfg.try_deserialize().unwrap();
        let bulk = parsed.bulk.unwrap();
        assert_eq!(bulk.store, BulkStoreKind::S3);
        assert_eq!(bulk.filesystem.unwrap().path, "/from-env/buckets");
        let s3 = bulk.s3.unwrap();
        assert_eq!(s3.region.as_deref(), Some("eu-central-1"));
        assert_eq!(s3.endpoint_url.as_deref(), Some("http://127.0.0.1:7070"));
        assert!(s3.allow_insecure_endpoint);
        assert!(s3.force_path_style);
    }

    #[test]
    fn bulk_s3_environment_alone_selects_and_configures_the_store() {
        let vars = std::collections::HashMap::from([
            ("EXTENDDB__BULK__STORE".to_owned(), "s3".to_owned()),
            (
                "EXTENDDB__BULK__S3__FORCE_PATH_STYLE".to_owned(),
                "false".to_owned(),
            ),
        ]);
        let cfg = config::Config::builder()
            .add_source(
                config::Environment::with_prefix("EXTENDDB")
                    .separator("__")
                    .source(Some(vars)),
            )
            .build()
            .unwrap();
        let parsed: BulkSection = cfg.try_deserialize().unwrap();
        let bulk = parsed.bulk.unwrap();
        assert_eq!(bulk.store, BulkStoreKind::S3);
        let s3 = bulk.s3.unwrap();
        assert!(!s3.force_path_style);
        assert!(s3.region.is_none() && s3.endpoint_url.is_none());
    }

    #[test]
    fn bulk_environment_alone_creates_the_section() {
        // A deployment can enable the store with environment variables and no
        // `[bulk]` section in the file.
        let vars = std::collections::HashMap::from([(
            "EXTENDDB__BULK__FILESYSTEM__PATH".to_owned(),
            "/env-only/buckets".to_owned(),
        )]);
        let cfg = config::Config::builder()
            .add_source(
                config::Environment::with_prefix("EXTENDDB")
                    .separator("__")
                    .source(Some(vars)),
            )
            .build()
            .unwrap();
        let parsed: BulkSection = cfg.try_deserialize().unwrap();
        let bulk = parsed.bulk.unwrap();
        assert_eq!(bulk.store, BulkStoreKind::Filesystem);
        assert_eq!(bulk.filesystem.unwrap().path, "/env-only/buckets");
    }
}
