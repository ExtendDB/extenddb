// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Configuration shapes for the backup store.
//!
//! These model the store subset of the `[backup]` section:
//!
//! ```toml
//! [backup]
//! store = "filesystem"                  # "filesystem" | "s3"
//!
//! [backup.filesystem]
//! path = "/var/lib/extenddb/backups"    # required when store = "filesystem"
//!
//! [backup.s3]
//! bucket = "my-extenddb-backups"        # required when store = "s3"
//! prefix = "extenddb/"                  # optional
//! region = "us-east-1"                  # optional; SDK chain otherwise
//! endpoint = "http://minio:9000"        # optional; S3-compatible stores
//! force_path_style = false
//! upload_part_size_bytes = 8388608
//! max_concurrent_uploads = 4
//! ```
//!
//! Wiring into `crates/config` and the server startup path is deliberately
//! not done here; this crate only defines the shapes and the
//! [`crate::open`] constructor over them.

use std::path::PathBuf;

use serde::Deserialize;

/// Which store implementation the `[backup]` section selects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackupStoreKind {
    /// Local directory store. The default: local development works with no
    /// configuration, and the backup manual states the failure-domain
    /// consequence.
    #[default]
    Filesystem,
    /// S3 bucket store.
    S3,
}

/// The store subset of the `[backup]` configuration section.
///
/// Composition note for the wiring task: `deny_unknown_fields` does not
/// survive `serde(flatten)`, so embedding this struct into a full `[backup]`
/// section model via flatten would silently stop rejecting unknown fields.
/// Model the full section as its own struct with these fields inline instead
/// of composing by flattening.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupStoreConfig {
    /// Selected store kind; `filesystem` when absent.
    #[serde(default)]
    pub store: BackupStoreKind,
    /// `[backup.filesystem]`, required when `store = "filesystem"`.
    #[serde(default)]
    pub filesystem: Option<FilesystemStoreConfig>,
    /// `[backup.s3]`, required when `store = "s3"`.
    #[serde(default)]
    pub s3: Option<S3StoreConfig>,
}

/// `[backup.filesystem]` settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemStoreConfig {
    /// Root directory for backup objects. Must exist at startup.
    pub path: PathBuf,
}

/// `[backup.s3]` settings. Credentials are absent by design: they come from
/// the standard AWS SDK chain.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3StoreConfig {
    /// Destination bucket.
    pub bucket: String,
    /// Key prefix inside the bucket. Optional; empty means the bucket root.
    #[serde(default)]
    pub prefix: String,
    /// Region override. When absent the SDK chain resolves the region.
    #[serde(default)]
    pub region: Option<String>,
    /// Endpoint override for S3-compatible stores.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Use path-style addressing; required by most S3-compatible stores.
    #[serde(default)]
    pub force_path_style: bool,
    /// Multipart part size in bytes. Defaults to 8 MiB; the enforced minimum
    /// is 5 MiB, the smallest part S3 accepts.
    #[serde(default = "default_upload_part_size_bytes")]
    pub upload_part_size_bytes: u64,
    /// Maximum concurrently in-flight part uploads per put. Defaults to 4.
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
}

fn default_upload_part_size_bytes() -> u64 {
    8 * 1024 * 1024
}

fn default_max_concurrent_uploads() -> usize {
    4
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Arc<dyn BackupStore>` has no `Debug`, so `unwrap_err` cannot be used.
    async fn open_err(config: &BackupStoreConfig) -> crate::StoreError {
        match crate::open(config).await {
            Err(e) => e,
            Ok(_) => panic!("expected open to fail"),
        }
    }

    #[test]
    fn parses_the_documented_filesystem_shape() {
        let config: BackupStoreConfig = toml::from_str(
            r#"
            store = "filesystem"

            [filesystem]
            path = "/var/lib/extenddb/backups"
            "#,
        )
        .unwrap();
        assert_eq!(config.store, BackupStoreKind::Filesystem);
        assert_eq!(
            config.filesystem.unwrap().path,
            PathBuf::from("/var/lib/extenddb/backups")
        );
        assert!(config.s3.is_none());
    }

    #[test]
    fn parses_the_documented_s3_shape_with_defaults() {
        let config: BackupStoreConfig = toml::from_str(
            r#"
            store = "s3"

            [s3]
            bucket = "my-extenddb-backups"
            prefix = "extenddb/"
            region = "us-east-1"
            endpoint = "http://minio:9000"
            force_path_style = true
            "#,
        )
        .unwrap();
        assert_eq!(config.store, BackupStoreKind::S3);
        let s3 = config.s3.unwrap();
        assert_eq!(s3.bucket, "my-extenddb-backups");
        assert_eq!(s3.prefix, "extenddb/");
        assert_eq!(s3.region.as_deref(), Some("us-east-1"));
        assert_eq!(s3.endpoint.as_deref(), Some("http://minio:9000"));
        assert!(s3.force_path_style);
        assert_eq!(s3.upload_part_size_bytes, 8 * 1024 * 1024);
        assert_eq!(s3.max_concurrent_uploads, 4);
    }

    #[test]
    fn store_kind_defaults_to_filesystem() {
        let config: BackupStoreConfig = toml::from_str(
            r#"
            [filesystem]
            path = "/backups"
            "#,
        )
        .unwrap();
        assert_eq!(config.store, BackupStoreKind::Filesystem);
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_level() {
        assert!(toml::from_str::<BackupStoreConfig>("stire = \"s3\"").is_err());
        assert!(
            toml::from_str::<BackupStoreConfig>(
                "store = \"filesystem\"\n[filesystem]\npath = \"/b\"\npth = \"/b\"\n"
            )
            .is_err()
        );
        assert!(
            toml::from_str::<BackupStoreConfig>(
                "store = \"s3\"\n[s3]\nbucket = \"b\"\nbukcet = \"b\"\n"
            )
            .is_err()
        );
    }

    #[test]
    fn unknown_store_kind_is_rejected() {
        assert!(toml::from_str::<BackupStoreConfig>("store = \"gcs\"").is_err());
    }

    #[tokio::test]
    async fn open_requires_the_matching_section() {
        let missing_fs = BackupStoreConfig {
            store: BackupStoreKind::Filesystem,
            filesystem: None,
            s3: None,
        };
        let err = open_err(&missing_fs).await;
        assert!(err.to_string().contains("[backup.filesystem]"), "{err}");

        let missing_s3 = BackupStoreConfig {
            store: BackupStoreKind::S3,
            filesystem: None,
            s3: None,
        };
        let err = open_err(&missing_s3).await;
        assert!(err.to_string().contains("[backup.s3]"), "{err}");
    }

    #[tokio::test]
    async fn open_enforces_the_part_size_floor() {
        let config = BackupStoreConfig {
            store: BackupStoreKind::S3,
            filesystem: None,
            s3: Some(S3StoreConfig {
                bucket: "b".to_owned(),
                prefix: String::new(),
                region: Some("us-east-1".to_owned()),
                endpoint: None,
                force_path_style: false,
                upload_part_size_bytes: 4 * 1024 * 1024,
                max_concurrent_uploads: 4,
            }),
        };
        let err = open_err(&config).await;
        assert!(err.to_string().contains("minimum is 5242880"), "{err}");
    }

    #[tokio::test]
    async fn open_rejects_zero_concurrency() {
        let config = BackupStoreConfig {
            store: BackupStoreKind::S3,
            filesystem: None,
            s3: Some(S3StoreConfig {
                bucket: "b".to_owned(),
                prefix: String::new(),
                region: Some("us-east-1".to_owned()),
                endpoint: None,
                force_path_style: false,
                upload_part_size_bytes: 8 * 1024 * 1024,
                max_concurrent_uploads: 0,
            }),
        };
        let err = open_err(&config).await;
        assert!(err.to_string().contains("max_concurrent_uploads"), "{err}");
    }

    #[tokio::test]
    async fn open_rejects_a_missing_filesystem_root() {
        let config = BackupStoreConfig {
            store: BackupStoreKind::Filesystem,
            filesystem: Some(FilesystemStoreConfig {
                path: PathBuf::from("/nonexistent/extenddb-backup-root"),
            }),
            s3: None,
        };
        let err = open_err(&config).await;
        assert!(err.to_string().contains("not usable"), "{err}");
    }
}
