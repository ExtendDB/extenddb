// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Store key layout for a backup prefix, and the backup ARN.
//!
//! A backup lives under `<account_id>/<table_name>/<backup_id>/` in the store
//! and holds the fixed object names defined here plus numbered data files
//! under `data/`. [`BackupArn`] parses and formats the ARN shape
//! `arn:<partition>:dynamodb:<region>:<account>:table/<table>/backup/<id>`
//! and turns it into a store prefix. It is meant to become the one place that
//! shape lives; today the storage backends still format the ARN themselves
//! (in Rust and in SQL) and the engine's account check still splits it by
//! hand. Moving them onto `BackupArn` is a follow-up in the backup/restore
//! series, not part of the format freeze.
//!
//! Every component is validated on construction, whether it comes from an ARN
//! string or from the engine's own fields, so a prefix can never contain a
//! separator, a traversal component, or a control character: the store's key
//! validation is a second line, not the first.

use super::FormatError;

/// The `ExtendDB` manifest, written last; its presence marks a complete backup.
pub const EXTENDDB_MANIFEST_FILE: &str = "extenddb-manifest.json";

/// The sha256 of the manifest's bytes, written before the manifest. See
/// [`super::manifest_checksum`].
pub const MANIFEST_CHECKSUM_FILE: &str = "extenddb-manifest.sha256";

/// The `DynamoDB` export summary manifest.
pub const SUMMARY_MANIFEST_FILE: &str = "manifest-summary.json";

/// The `DynamoDB` export files manifest.
pub const FILES_MANIFEST_FILE: &str = "manifest-files.json";

/// The prefix that holds only data files, so `ImportTable` against it never
/// sees a manifest.
pub const DATA_PREFIX: &str = "data/";

/// Longest table name `DynamoDB` accepts, kept here as a path-safety bound:
/// a longer component would exceed common filesystem name limits. The
/// engine's configurable minimum length is not enforced here; a shorter name
/// the engine accepted is a valid ARN component.
const MAX_TABLE_NAME_LEN: usize = 255;

/// The key of the `index`-th data file within a backup prefix:
/// `data/000001.json.gz` for index 1.
#[must_use]
pub fn data_file_key(index: u32) -> String {
    format!("{DATA_PREFIX}{index:06}.json.gz")
}

/// A validated backup ARN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupArn {
    partition: String,
    region: String,
    account_id: String,
    table_name: String,
    backup_id: String,
}

impl BackupArn {
    /// Build an ARN from its components, validating each.
    ///
    /// Rules: `partition` and `region` are non-empty lowercase ASCII letters,
    /// digits and `-` (`aws`, `aws-cn`, `aws-us-gov`, `us-east-1`);
    /// `account_id` is non-empty ASCII letters, digits, `_` and `-`;
    /// `table_name` passes the engine's character rule
    /// ([`crate::validation::validate_table_name_chars`]: ASCII letters,
    /// digits, `_`, `.`, `-`), is at most 255 bytes, and is not made only of
    /// dots; `backup_id` is non-empty ASCII letters, digits, `_`, `.` and `-`
    /// and not made only of dots. Nothing may contain `/`, `:`, or
    /// whitespace, so the formatted ARN parses back to the same components
    /// and the store prefix has exactly three segments.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidArn`] naming the component that failed.
    pub fn new(
        partition: &str,
        region: &str,
        account_id: &str,
        table_name: &str,
        backup_id: &str,
    ) -> Result<Self, FormatError> {
        let component = |name: &str, value: &str, ok: fn(char) -> bool| {
            if value.is_empty() || !value.chars().all(ok) {
                return Err(FormatError::InvalidArn(format!("{name} {value:?}")));
            }
            Ok(())
        };
        let lower_dash = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-';
        let word = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '-';
        let name_chars = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-';

        component("partition", partition, lower_dash)?;
        component("region", region, lower_dash)?;
        component("account id", account_id, word)?;
        if table_name.is_empty() || table_name.len() > MAX_TABLE_NAME_LEN {
            return Err(FormatError::InvalidArn(format!(
                "table name {table_name:?} length"
            )));
        }
        // One character rule for table names, shared with CreateTable.
        crate::validation::validate_table_name_chars(table_name)
            .map_err(|_| FormatError::InvalidArn(format!("table name {table_name:?}")))?;
        component("backup id", backup_id, name_chars)?;
        // `.` and `..` are path traversal; longer runs of dots are legal
        // DynamoDB names and legal path components, but a name made only of
        // dots is refused to keep the rule simple.
        for (what, value) in [("table name", table_name), ("backup id", backup_id)] {
            if value.bytes().all(|b| b == b'.') {
                return Err(FormatError::InvalidArn(format!("{what} {value:?}")));
            }
        }
        Ok(Self {
            partition: partition.to_owned(),
            region: region.to_owned(),
            account_id: account_id.to_owned(),
            table_name: table_name.to_owned(),
            backup_id: backup_id.to_owned(),
        })
    }

    /// Parse an ARN of the shape
    /// `arn:<partition>:dynamodb:<region>:<account>:table/<table>/backup/<id>`
    /// and validate its components with the rules of [`Self::new`].
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidArn`] when the string does not have that
    /// shape or a component fails validation.
    pub fn parse(arn: &str) -> Result<Self, FormatError> {
        let invalid = || FormatError::InvalidArn(arn.to_owned());

        let rest = arn.strip_prefix("arn:").ok_or_else(invalid)?;
        let mut fields = rest.splitn(5, ':');
        let partition = fields.next().ok_or_else(invalid)?;
        let service = fields.next().ok_or_else(invalid)?;
        let region = fields.next().ok_or_else(invalid)?;
        let account_id = fields.next().ok_or_else(invalid)?;
        let resource = fields.next().ok_or_else(invalid)?;
        if service != "dynamodb" {
            return Err(invalid());
        }

        let path = resource.strip_prefix("table/").ok_or_else(invalid)?;
        let (table_name, backup_id) = path.split_once("/backup/").ok_or_else(invalid)?;

        // Keep the component-level reason: a mistyped ARN should say which
        // part is wrong.
        Self::new(partition, region, account_id, table_name, backup_id).map_err(|e| match e {
            FormatError::InvalidArn(reason) => FormatError::InvalidArn(format!("{arn}: {reason}")),
            other => other,
        })
    }

    /// The ARN string.
    #[must_use]
    pub fn to_arn_string(&self) -> String {
        format!(
            "arn:{}:dynamodb:{}:{}:table/{}/backup/{}",
            self.partition, self.region, self.account_id, self.table_name, self.backup_id
        )
    }

    /// The store key prefix for this backup:
    /// `<account_id>/<table_name>/<backup_id>/`.
    #[must_use]
    pub fn store_prefix(&self) -> String {
        format!(
            "{}/{}/{}/",
            self.account_id, self.table_name, self.backup_id
        )
    }

    /// Partition field (`aws`, `aws-cn`, `aws-us-gov`).
    #[must_use]
    pub fn partition(&self) -> &str {
        &self.partition
    }

    /// Region field.
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Account id field.
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    /// Table name from the resource path.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Backup id from the resource path.
    #[must_use]
    pub fn backup_id(&self) -> &str {
        &self.backup_id
    }
}

impl std::fmt::Display for BackupArn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_arn_string())
    }
}

impl std::str::FromStr for BackupArn {
    type Err = FormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARN: &str =
        "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc";

    #[test]
    fn store_prefix_shape() {
        let arn = BackupArn::parse(ARN).unwrap();
        assert_eq!(
            arn.store_prefix(),
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
        let arn = BackupArn::parse(ARN).unwrap();
        assert_eq!(arn.partition(), "aws");
        assert_eq!(arn.region(), "us-east-1");
        assert_eq!(arn.account_id(), "123456789012");
        assert_eq!(arn.table_name(), "Music");
        assert_eq!(arn.backup_id(), "01489602797149-73d8d5bc");
        assert_eq!(arn.to_arn_string(), ARN);
        assert_eq!(arn.to_string(), ARN);
        assert_eq!(ARN.parse::<BackupArn>().unwrap(), arn);
    }

    #[test]
    fn other_partitions_parse() {
        for arn in [
            "arn:aws-cn:dynamodb:cn-north-1:123456789012:table/Music/backup/1-a",
            "arn:aws-us-gov:dynamodb:us-gov-west-1:123456789012:table/Music/backup/1-a",
        ] {
            let parsed = BackupArn::parse(arn).unwrap();
            assert_eq!(parsed.to_arn_string(), arn);
        }
    }

    #[test]
    fn new_and_parse_agree() {
        let built = BackupArn::new(
            "aws",
            "eu-west-1",
            "000000000001",
            "My.Table-2",
            "0123-abcd",
        )
        .unwrap();
        let parsed = BackupArn::parse(&built.to_arn_string()).unwrap();
        assert_eq!(parsed, built);
        assert_eq!(built.store_prefix(), "000000000001/My.Table-2/0123-abcd/");
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
            "arn::dynamodb:us-east-1:123456789012:table/Music/backup/1",
            "arn:AWS:dynamodb:us-east-1:123456789012:table/Music/backup/1",
            // Extra colon fields: the account must be field 4, nothing else.
            "arn:aws:dynamodb:us-east-1:evil:123456789012:table/Music/backup/1",
            // Traversal and separators inside components.
            "arn:aws:dynamodb:us-east-1:123456789012:table/../backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/..",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/a\\b",
            "arn:aws:dynamodb:us-east-1:12345678 9012:table/Music/backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Mu sic/backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/1\n",
            // Dot-only names are traversal components.
            "arn:aws:dynamodb:us-east-1:123456789012:table/./backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/..../backup/1",
        ] {
            assert!(
                BackupArn::parse(arn).is_err(),
                "expected rejection for {arn:?}"
            );
        }
        let long = "a".repeat(256);
        assert!(
            BackupArn::parse(&format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{long}/backup/1"
            ))
            .is_err()
        );
    }

    #[test]
    fn new_rejects_bad_components() {
        assert!(BackupArn::new("aws", "us-east-1", "1234", "Music", "a/b").is_err());
        assert!(BackupArn::new("aws", "us-east-1", "12/34", "Music", "1").is_err());
        assert!(BackupArn::new("aws", "us-east-1", "1234", "Music", "...").is_err());
        assert!(BackupArn::new("aws", "us-east-1", "1234", "...", "1").is_err());
        // Short names are the engine's call (its minimum length is
        // configurable); the ARN accepts them.
        BackupArn::new("aws", "us-east-1", "1234", "ab", "1").unwrap();
        // The error names the failing component.
        let err = BackupArn::parse("arn:aws:dynamodb:us-east-1:123456789012:table/Mu sic/backup/1")
            .unwrap_err();
        assert!(err.to_string().contains("table name"), "{err}");
        assert!(BackupArn::new("aws", "us:east", "1234", "Music", "1").is_err());
        assert!(BackupArn::new("", "us-east-1", "1234", "Music", "1").is_err());
        // Dots inside names are fine; DynamoDB allows them.
        BackupArn::new("aws", "us-east-1", "1234", "a.b.c", "1.2").unwrap();
    }

    #[test]
    fn fixed_object_names() {
        assert_eq!(EXTENDDB_MANIFEST_FILE, "extenddb-manifest.json");
        assert_eq!(MANIFEST_CHECKSUM_FILE, "extenddb-manifest.sha256");
        assert_eq!(SUMMARY_MANIFEST_FILE, "manifest-summary.json");
        assert_eq!(FILES_MANIFEST_FILE, "manifest-files.json");
        assert_eq!(DATA_PREFIX, "data/");
    }
}
