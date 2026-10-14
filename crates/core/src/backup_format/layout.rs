// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Store key layout for a backup prefix, and backup ARN parsing.
//!
//! A backup lives under `<account_id>/<table_name>/<backup_id>/` in the store
//! and holds the fixed object names defined here plus numbered data files
//! under `data/`. The ARN shape is the one the engine's backup handlers use:
//! `arn:aws:dynamodb:<region>:<account>:table/<table>/backup/<id>`.

use super::FormatError;

/// The `ExtendDB` manifest, written last; its presence marks a complete backup.
pub const EXTENDDB_MANIFEST_FILE: &str = "extenddb-manifest.json";

/// The `DynamoDB` export summary manifest.
pub const SUMMARY_MANIFEST_FILE: &str = "manifest-summary.json";

/// The `DynamoDB` export files manifest.
pub const FILES_MANIFEST_FILE: &str = "manifest-files.json";

/// The prefix that holds only data files, so `ImportTable` against it never
/// sees a manifest.
pub const DATA_PREFIX: &str = "data/";

/// The store key prefix for one backup: `<account_id>/<table_name>/<backup_id>/`.
#[must_use]
pub fn backup_prefix(account_id: &str, table_name: &str, backup_id: &str) -> String {
    format!("{account_id}/{table_name}/{backup_id}/")
}

/// The key of the `index`-th data file within a backup prefix:
/// `data/000001.json.gz` for index 1.
#[must_use]
pub fn data_file_key(index: u32) -> String {
    format!("{DATA_PREFIX}{index:06}.json.gz")
}

/// The components a backup ARN encodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupArnParts {
    /// Region field of the ARN.
    pub region: String,
    /// Account id field of the ARN.
    pub account_id: String,
    /// Table name from the resource path.
    pub table_name: String,
    /// Backup id from the resource path.
    pub backup_id: String,
}

/// Parse a backup ARN of the shape
/// `arn:aws:dynamodb:<region>:<account>:table/<table>/backup/<id>`.
///
/// Every component must be non-empty. Table names cannot contain `/`, so the
/// resource path splits unambiguously.
///
/// # Errors
///
/// Returns [`FormatError::InvalidArn`] when the ARN does not have that shape.
pub fn parse_backup_arn(arn: &str) -> Result<BackupArnParts, FormatError> {
    let invalid = || FormatError::InvalidArn(arn.to_owned());

    let rest = arn.strip_prefix("arn:aws:dynamodb:").ok_or_else(invalid)?;
    let mut fields = rest.splitn(3, ':');
    let region = fields.next().ok_or_else(invalid)?;
    let account_id = fields.next().ok_or_else(invalid)?;
    let resource = fields.next().ok_or_else(invalid)?;

    let path = resource.strip_prefix("table/").ok_or_else(invalid)?;
    let (table_name, backup_id) = path.split_once("/backup/").ok_or_else(invalid)?;

    if region.is_empty()
        || account_id.is_empty()
        || table_name.is_empty()
        || backup_id.is_empty()
        || table_name.contains('/')
        || backup_id.contains('/')
    {
        return Err(invalid());
    }

    Ok(BackupArnParts {
        region: region.to_owned(),
        account_id: account_id.to_owned(),
        table_name: table_name.to_owned(),
        backup_id: backup_id.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_prefix_shape() {
        assert_eq!(
            backup_prefix("123456789012", "Music", "01489602797149-73d8d5bc"),
            "123456789012/Music/01489602797149-73d8d5bc/"
        );
    }

    #[test]
    fn data_file_key_is_zero_padded() {
        assert_eq!(data_file_key(1), "data/000001.json.gz");
        assert_eq!(data_file_key(42), "data/000042.json.gz");
        assert_eq!(data_file_key(1_000_000), "data/1000000.json.gz");
    }

    #[test]
    fn parse_valid_arn() {
        let parts = parse_backup_arn(
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc",
        )
        .unwrap();
        assert_eq!(
            parts,
            BackupArnParts {
                region: "us-east-1".to_owned(),
                account_id: "123456789012".to_owned(),
                table_name: "Music".to_owned(),
                backup_id: "01489602797149-73d8d5bc".to_owned(),
            }
        );
    }

    #[test]
    fn parse_and_prefix_round_trip() {
        let arn = "arn:aws:dynamodb:eu-west-1:000000000001:table/My.Table-2/backup/0123-abcd";
        let parts = parse_backup_arn(arn).unwrap();
        assert_eq!(
            backup_prefix(&parts.account_id, &parts.table_name, &parts.backup_id),
            "000000000001/My.Table-2/0123-abcd/"
        );
        // Reassembling the ARN from the parts reproduces the input.
        let reassembled = format!(
            "arn:aws:dynamodb:{}:{}:table/{}/backup/{}",
            parts.region, parts.account_id, parts.table_name, parts.backup_id
        );
        assert_eq!(reassembled, arn);
    }

    #[test]
    fn parse_rejects_malformed_arns() {
        for arn in [
            "",
            "arn:aws:dynamodb",
            "arn:aws:dynamodb:us-east-1",
            "arn:aws:dynamodb:us-east-1:123456789012",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music",
            "arn:aws:dynamodb:us-east-1::table/Music/backup/1",
            "arn:aws:dynamodb::123456789012:table/Music/backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table//backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/",
            "arn:aws:dynamodb:us-east-1:123456789012:index/Music/backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/a/b",
            "arn:aws:s3:us-east-1:123456789012:table/Music/backup/1",
        ] {
            assert!(
                parse_backup_arn(arn).is_err(),
                "expected rejection for {arn:?}"
            );
        }
    }

    #[test]
    fn fixed_object_names() {
        assert_eq!(EXTENDDB_MANIFEST_FILE, "extenddb-manifest.json");
        assert_eq!(SUMMARY_MANIFEST_FILE, "manifest-summary.json");
        assert_eq!(FILES_MANIFEST_FILE, "manifest-files.json");
        assert_eq!(DATA_PREFIX, "data/");
    }
}
