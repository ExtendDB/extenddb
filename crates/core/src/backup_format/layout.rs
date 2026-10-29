// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Store key layout for a backup prefix, and the backup ARN.
//!
//! A backup lives under `<account_id>/<table_name>/<backup_id>/` in the store
//! and holds the fixed object names defined here plus numbered data files
//! under `data/`.
//!
//! This module is the one implementation of the backup id and the backup ARN
//! shape `arn:<partition>:dynamodb:<region>:<account>:table/<table>/backup/<id>`.
//! The engine and every storage backend reach it through
//! `extenddb_storage::util` (`check_backup_arn`, `parse_backup_arn`,
//! `is_backup_id`, `format_backup_id`, ...), which re-exports the functions
//! defined here; the field split itself is [`crate::arn::fields`], the same
//! split authorization uses, so the engine and authorization cannot read a
//! different resource from one string. One other reader remains:
//! `extenddb_storage::util::is_backup_resource_arn` splits the string with
//! its own `splitn(6, ':')` to tell a backup ARN from a table ARN. [`BackupArn`] is the owned form the off-host backup format
//! uses: it adds the store-prefix rules (a table name that is only dots is
//! refused, since `.` and `..` are path components) on top of
//! [`check_backup_arn`], and turns the ARN into a store prefix.
//!
//! Every component is validated on construction, whether it comes from an ARN
//! string or from the engine's own fields, so a prefix can never contain a
//! separator, a traversal component, or a control character: the store's key
//! validation is a second line, not the first.

use super::FormatError;
use super::error::truncated;

/// The `ExtendDB` manifest, written last; its presence marks a complete backup.
pub const EXTENDDB_MANIFEST_FILE: &str = "extenddb-manifest.json";

/// The sha256 of the manifest's bytes, written before the manifest. See
/// [`super::verify_manifest_checksum`] and, for the write order,
/// [`super::BackupDocuments`].
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
/// the engine accepted is a valid ARN component. The engine's
/// `max_table_name_length` can be raised above 255; a table with a longer
/// name has a backup ARN `check_backup_arn` accepts and this type refuses,
/// so the off-host backup path cannot write it (the design has `CreateBackup`
/// refuse it up front).
const MAX_TABLE_NAME_LEN: usize = 255;

/// The key of the `index`-th data file within a backup prefix:
/// `data/000001.json.gz` for index 1.
#[must_use]
pub fn data_file_key(index: u32) -> String {
    format!("{DATA_PREFIX}{index:06}.json.gz")
}

// ---------------------------------------------------------------------------
// Backup id
// ---------------------------------------------------------------------------

/// Digits of the epoch-millisecond prefix of a backup id. The service
/// zero-pads to 14 (`01489602797149-73d8d5bc`); 13 digits cover today's
/// instants, so the padding is visible as a leading zero.
pub const BACKUP_ID_MILLIS_DIGITS: usize = 14;

/// Format a backup id from its parts: 14-digit zero-padded epoch
/// milliseconds, `-`, 8 lowercase hex digits. The random suffix is part of
/// the identifier, not decoration: a timestamp alone makes a backup id
/// derivable by anyone who knows roughly when the backup ran, and the suffix
/// keeps two backups created in the same millisecond apart.
/// `extenddb_storage::util::new_backup_id` draws the suffix.
#[must_use]
pub fn format_backup_id(epoch_millis: u64, suffix: u32) -> String {
    format!("{epoch_millis:0BACKUP_ID_MILLIS_DIGITS$}-{suffix:08x}")
}

fn all_digits(s: &str, n: usize) -> bool {
    s.len() == n && s.bytes().all(|b| b.is_ascii_digit())
}

/// Whether `s` is `n` lowercase hex digits.
pub(super) fn lower_hex(s: &str, n: usize) -> bool {
    s.len() == n
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Whether `id` is a backup id this codebase ever issued: 14 or 13 digits,
/// `-`, 8 lowercase hex digits (the 14-digit form is the current one; the
/// 13-digit form is what every backend issued before the shared id), or 13
/// bare digits (epoch milliseconds with no suffix, the form PostgreSQL issued
/// in v0.1.0 to v0.1.2 and MongoDB before its suffix was added). No build has
/// put a UUID in a backup ARN, so none is accepted.
#[must_use]
pub fn is_backup_id(id: &str) -> bool {
    match id.split_once('-') {
        Some((millis, suffix)) => {
            (all_digits(millis, BACKUP_ID_MILLIS_DIGITS)
                || all_digits(millis, BACKUP_ID_MILLIS_DIGITS - 1))
                && lower_hex(suffix, 8)
        }
        None => all_digits(id, BACKUP_ID_MILLIS_DIGITS - 1),
    }
}

/// The epoch milliseconds a backup id (or a whole backup ARN) was minted at,
/// or `None` if it is not in the shape [`format_backup_id`] produces.
#[must_use]
pub fn backup_id_epoch_millis(id_or_arn: &str) -> Option<u64> {
    let id = id_or_arn.rsplit('/').next()?;
    let (millis, suffix) = id.split_once('-')?;
    if !all_digits(millis, BACKUP_ID_MILLIS_DIGITS) || !lower_hex(suffix, 8) {
        return None;
    }
    millis.parse().ok()
}

/// The instant a backup id from [`format_backup_id`] was minted, as epoch
/// seconds with millisecond precision. This is the backup's
/// `BackupCreationDateTime`: the off-host backup job mints the id from the
/// instant its snapshot pinned (design 5.1), and the manifest parser requires
/// `backup_creation_date_time` to equal this value, so the ARN, the manifest
/// and the API agree on one instant.
#[must_use]
#[allow(clippy::cast_precision_loss)] // epoch ms fit f64's 53-bit mantissa
pub fn backup_creation_date_time(id_or_arn: &str) -> Option<f64> {
    backup_id_epoch_millis(id_or_arn).map(|ms| ms as f64 / 1000.0)
}

// ---------------------------------------------------------------------------
// Backup ARN
// ---------------------------------------------------------------------------

/// The parts of a well-formed backup ARN, borrowed from the input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupArnParts<'a> {
    /// The ARN with surrounding ASCII whitespace removed, as the other fields
    /// were read from it.
    pub arn: &'a str,
    pub partition: &'a str,
    pub region: &'a str,
    pub account_id: &'a str,
    pub table_name: &'a str,
    pub backup_id: &'a str,
}

/// Why a string is not a backup ARN. The two classes carry the two texts the
/// service answers with (measured on `DynamoDB`). Which text goes with which
/// input was captured for one input only (a bare 13-digit id is "not
/// valid"), so the split is the parser's: a string that is not an ARN of the
/// right shape, is too long, or carries a character no ARN part allows is
/// `Invalid`; an ARN of the right shape whose backup id is not one is `Id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupArnError {
    /// Not `arn:<partition>:dynamodb:<region>:<account>:table/<name>/backup/<id>`
    /// with a non-empty region, account, and name, over
    /// [`MAX_BACKUP_ARN_LEN`] bytes, with a control or non-ASCII character, or
    /// with a region outside `[a-z0-9-]`, a table name outside
    /// `[a-zA-Z0-9_.-]`, or an account outside `[0-9]`. The service's text:
    /// `Invalid Request: BackupArn is not valid`.
    Invalid,
    /// The backup id is neither the current shape nor one an earlier build
    /// issued (see [`is_backup_id`]). The service's text: `Invalid Backup ARN`.
    Id,
}

impl std::fmt::Display for BackupArnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid => "not a backup ARN",
            Self::Id => "not a backup id",
        })
    }
}

/// The longest `BackupArn` the service model accepts (`BackupArn` shape,
/// botocore 1.33.13: max 1024).
pub const MAX_BACKUP_ARN_LEN: usize = 1024;

/// Whether every byte is in `allowed` or an ASCII alphanumeric.
fn ascii_word(s: &str, allowed: &[u8]) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || allowed.contains(&b))
}

/// The region shape a backup ARN may carry: non-empty `[a-z0-9-]`. The
/// manifest checks a table ARN's region with the same predicate.
pub(super) fn is_region(s: &str) -> bool {
    ascii_word(s, b"-") && !s.bytes().any(|b| b.is_ascii_uppercase())
}

/// Strip surrounding ASCII whitespace from a `BackupArn`. Measured on
/// `DynamoDB`: an ARN with a trailing newline or CRLF resolves, and one with a
/// leading space or tab is refused with `Invalid Backup ARN`. This parser
/// trims both ends and every ASCII whitespace character, as main's parser
/// did; the leading-whitespace case is kept for compatibility with callers
/// of that parser, not because the service accepts it. Every layer that
/// reads a backup ARN from a request, the engine through
/// [`check_backup_arn`] and authorization before it derives the resource,
/// trims with this one function so they act on the same string.
#[must_use]
pub fn trim_backup_arn(arn: &str) -> &str {
    arn.trim_matches(|c: char| c.is_ascii_whitespace())
}

/// Format a backup ARN from its parts. The one place the string is built;
/// `extenddb_storage::util::backup_arn` and [`BackupArn`] call it.
#[must_use]
pub fn format_backup_arn(
    partition: &str,
    region: &str,
    account_id: &str,
    table_name: &str,
    backup_id: &str,
) -> String {
    format!("arn:{partition}:dynamodb:{region}:{account_id}:table/{table_name}/backup/{backup_id}")
}

/// Parse a backup ARN, or `None` if [`check_backup_arn`] refuses it.
#[must_use]
pub fn parse_backup_arn(arn: &str) -> Option<BackupArnParts<'_>> {
    check_backup_arn(arn).ok()
}

/// Parse a backup ARN after trimming surrounding ASCII whitespace with
/// [`trim_backup_arn`] (wider than the service, which resolves a trailing
/// newline or CRLF and refuses leading whitespace; see there), with the
/// length limit below applied to what remains, refusing one that is not exactly six
/// colon-separated fields, not `arn:<partition>:dynamodb:...`, has an empty
/// or malformed region, account, or table name, a resource other than
/// `table/<name>/backup/<id>`, a control or non-ASCII character, more than
/// [`MAX_BACKUP_ARN_LEN`] bytes, or an id that is not a backup id
/// ([`is_backup_id`]). The region and partition are checked for shape only:
/// an ARN in another region of the caller's account resolves on the service,
/// so the engine substitutes its own region rather than refuse it, and the
/// engine decides which partitions it serves.
///
/// The field split is [`crate::arn::fields`], shared with authorization.
///
/// # Errors
///
/// [`BackupArnError`], one class per service text.
pub fn check_backup_arn(arn: &str) -> Result<BackupArnParts<'_>, BackupArnError> {
    let arn = trim_backup_arn(arn);
    if arn.len() > MAX_BACKUP_ARN_LEN || !arn.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(BackupArnError::Invalid);
    }
    let f = crate::arn::fields(arn).ok_or(BackupArnError::Invalid)?;
    if !ascii_word(f.partition, b"-")
        || f.service != "dynamodb"
        || !is_region(f.region)
        || f.account.is_empty()
        || !f.account.bytes().all(|b| b.is_ascii_digit())
        // `fields` keeps any further `:` inside the resource; a backup
        // resource has none.
        || f.resource.contains(':')
    {
        return Err(BackupArnError::Invalid);
    }
    let parts: Vec<&str> = f.resource.split('/').collect();
    let ["table", table_name, "backup", backup_id] = parts.as_slice() else {
        return Err(BackupArnError::Invalid);
    };
    // The table-name alphabet is CreateTable's, from the function CreateTable
    // calls (`validate_table_name_chars`), so a name CreateTable accepts is a
    // name this parser accepts. The engine's `validate_name_member` and
    // `validate_index_name` spell the same alphabet again.
    if table_name.is_empty()
        || crate::validation::validate_table_name_chars(table_name).is_err()
        || backup_id.is_empty()
    {
        return Err(BackupArnError::Invalid);
    }
    if !is_backup_id(backup_id) {
        return Err(BackupArnError::Id);
    }
    Ok(BackupArnParts {
        arn,
        partition: f.partition,
        region: f.region,
        account_id: f.account,
        table_name,
        backup_id,
    })
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
    /// Build an ARN from its components, validating each with the rules of
    /// [`check_backup_arn`] (the components are formatted and parsed back, so
    /// one that carries a `:` or `/` cannot shift the fields) plus the
    /// store-prefix rules: the table name is at most 255 bytes and is not
    /// made only of dots (`.` and `..` are path components; a longer run of
    /// dots is a legal `DynamoDB` name but is refused to keep the rule
    /// simple). The backup id must be one this codebase issues
    /// ([`is_backup_id`]), which no dot-only string is.
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
        let formatted = format_backup_arn(partition, region, account_id, table_name, backup_id);
        let parts = check_backup_arn(&formatted).map_err(|e| {
            FormatError::InvalidArn(format!(
                "{e}: partition {:?}, region {:?}, account id {:?}, table name {:?}, \
                 backup id {:?}",
                truncated(partition),
                truncated(region),
                truncated(account_id),
                truncated(table_name),
                truncated(backup_id)
            ))
        })?;
        // A separator inside a component would have parsed back as different
        // fields; a leading or trailing space would have been trimmed away.
        if (
            parts.partition,
            parts.region,
            parts.account_id,
            parts.table_name,
            parts.backup_id,
        ) != (partition, region, account_id, table_name, backup_id)
        {
            return Err(FormatError::InvalidArn(format!(
                "a component contains a separator or surrounding whitespace: {:?}",
                truncated(&formatted)
            )));
        }
        Self::from_parts(&parts)
    }

    /// Parse an ARN of the shape
    /// `arn:<partition>:dynamodb:<region>:<account>:table/<table>/backup/<id>`
    /// with [`check_backup_arn`] and the store-prefix rules of [`Self::new`].
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidArn`] when the string does not have that
    /// shape or a component fails validation.
    pub fn parse(arn: &str) -> Result<Self, FormatError> {
        // The input may come from an untrusted manifest; report a bounded
        // slice of it, not the whole string.
        let shown = || truncated(trim_backup_arn(arn));
        let parts = check_backup_arn(arn)
            .map_err(|e| FormatError::InvalidArn(format!("{:?}: {e}", shown())))?;
        Self::from_parts(&parts).map_err(|e| match e {
            FormatError::InvalidArn(reason) => {
                FormatError::InvalidArn(format!("{:?}: {reason}", shown()))
            }
            other => other,
        })
    }

    /// The store-prefix rules over already-checked parts: the two rules
    /// `check_backup_arn` does not apply, and the only two inputs it accepts
    /// that this type refuses.
    fn from_parts(parts: &BackupArnParts<'_>) -> Result<Self, FormatError> {
        if parts.table_name.len() > MAX_TABLE_NAME_LEN {
            return Err(FormatError::InvalidArn(format!(
                "table name of {} bytes is longer than {MAX_TABLE_NAME_LEN}",
                parts.table_name.len()
            )));
        }
        if parts.table_name.bytes().all(|b| b == b'.') {
            return Err(FormatError::InvalidArn(format!(
                "table name {:?} is only dots",
                truncated(parts.table_name)
            )));
        }
        Ok(Self {
            partition: parts.partition.to_owned(),
            region: parts.region.to_owned(),
            account_id: parts.account_id.to_owned(),
            table_name: parts.table_name.to_owned(),
            backup_id: parts.backup_id.to_owned(),
        })
    }

    /// The ARN string.
    #[must_use]
    pub fn to_arn_string(&self) -> String {
        format_backup_arn(
            &self.partition,
            &self.region,
            &self.account_id,
            &self.table_name,
            &self.backup_id,
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
            "arn:aws-cn:dynamodb:cn-north-1:123456789012:table/Music/backup/01489602797149-73d8d5bc",
            "arn:aws-us-gov:dynamodb:us-gov-west-1:123456789012:table/Music/backup/01489602797149-73d8d5bc",
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
            "01489602797149-73d8d5bc",
        )
        .unwrap();
        let parsed = BackupArn::parse(&built.to_arn_string()).unwrap();
        assert_eq!(parsed, built);
        assert_eq!(
            built.store_prefix(),
            "000000000001/My.Table-2/01489602797149-73d8d5bc/"
        );
        // Every id shape `is_backup_id` accepts is a valid prefix component.
        for id in ["1489602797149-73d8d5bc", "1489602797149"] {
            BackupArn::new("aws", "eu-west-1", "000000000001", "t", id).unwrap();
        }
    }

    #[test]
    fn parse_rejects_malformed_arns() {
        const ID: &str = "01489602797149-73d8d5bc";
        for arn in [
            "",
            "arn:aws:dynamodb",
            "arn:aws:dynamodb:us-east-1",
            "arn:aws:dynamodb:us-east-1:123456789012",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music",
            &format!("arn:aws:dynamodb:us-east-1::table/Music/backup/{ID}"),
            &format!("arn:aws:dynamodb::123456789012:table/Music/backup/{ID}"),
            &format!("arn:aws:dynamodb:us-east-1:123456789012:table//backup/{ID}"),
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/",
            &format!("arn:aws:dynamodb:us-east-1:123456789012:index/Music/backup/{ID}"),
            &format!("arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/{ID}/b"),
            &format!("arn:aws:s3:us-east-1:123456789012:table/Music/backup/{ID}"),
            &format!("arn::dynamodb:us-east-1:123456789012:table/Music/backup/{ID}"),
            // Extra colon fields: the account must be field 4, nothing else.
            &format!("arn:aws:dynamodb:us-east-1:evil:123456789012:table/Music/backup/{ID}"),
            &format!("arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/{ID}:x"),
            // Traversal and separators inside components.
            &format!("arn:aws:dynamodb:us-east-1:123456789012:table/../backup/{ID}"),
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/..",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/a\\b",
            &format!("arn:aws:dynamodb:us-east-1:12345678 9012:table/Music/backup/{ID}"),
            &format!("arn:aws:dynamodb:us-east-1:123456789012:table/Mu sic/backup/{ID}"),
            // Account is digits; region is lowercase.
            &format!("arn:aws:dynamodb:us-east-1:12345678901x:table/Music/backup/{ID}"),
            &format!("arn:aws:dynamodb:US-EAST-1:123456789012:table/Music/backup/{ID}"),
            // Ids no build issued.
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/a4b1d6c2-0000-4000-8000-000000000000",
            // Dot-only names are traversal components.
            &format!("arn:aws:dynamodb:us-east-1:123456789012:table/./backup/{ID}"),
            &format!("arn:aws:dynamodb:us-east-1:123456789012:table/..../backup/{ID}"),
        ] {
            assert!(
                BackupArn::parse(arn).is_err(),
                "expected rejection for {arn:?}"
            );
        }
        let long = "a".repeat(256);
        assert!(
            BackupArn::parse(&format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{long}/backup/{ID}"
            ))
            .is_err()
        );
        // Surrounding whitespace is trimmed at both ends (the service trims
        // only a trailing newline; `trim_backup_arn`); the parsed ARN is the
        // trimmed one.
        let padded = format!(" {ARN}\n");
        assert_eq!(BackupArn::parse(&padded).unwrap().to_arn_string(), ARN);
    }

    /// `check_backup_arn` and `BackupArn::parse` agree on every input except
    /// the store-prefix rules `BackupArn` adds.
    #[test]
    fn check_and_typed_parse_agree() {
        for arn in [
            ARN,
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/1489602797149-73d8d5bc",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/1489602797149",
            "arn:aws-cn:dynamodb:cn-north-1:123456789012:table/a.b/backup/01489602797149-73d8d5bc",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Mu sic/backup/01489602797149-73d8d5bc",
            "not-an-arn",
        ] {
            let checked = check_backup_arn(arn);
            let typed = BackupArn::parse(arn);
            assert_eq!(checked.is_ok(), typed.is_ok(), "{arn}");
            if let (Ok(c), Ok(t)) = (checked, typed) {
                assert_eq!(c.partition, t.partition());
                assert_eq!(c.region, t.region());
                assert_eq!(c.account_id, t.account_id());
                assert_eq!(c.table_name, t.table_name());
                assert_eq!(c.backup_id, t.backup_id());
            }
        }
        // The two divergences: a dot-only table name and a name over 255
        // bytes are legal ARNs but not store prefixes.
        for arn in [
            "arn:aws:dynamodb:us-east-1:123456789012:table/.../backup/01489602797149-73d8d5bc"
                .to_owned(),
            format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{}/backup/01489602797149-73d8d5bc",
                "a".repeat(256)
            ),
        ] {
            assert!(check_backup_arn(&arn).is_ok(), "{arn}");
            assert!(BackupArn::parse(&arn).is_err(), "{arn}");
        }
        // A 255-byte name is both.
        let max = format!(
            "arn:aws:dynamodb:us-east-1:123456789012:table/{}/backup/01489602797149-73d8d5bc",
            "a".repeat(255)
        );
        assert!(BackupArn::parse(&max).is_ok());
    }

    /// The id rule is a grid over millis length and suffix length, not a
    /// list of instances; and the two error classes map to two service
    /// texts, so they are pinned here where the parser lives.
    #[test]
    fn id_shape_grid_and_error_classes() {
        for millis_len in 0..=16usize {
            for suffix_len in 0..=10usize {
                let id = format!("{}-{}", "1".repeat(millis_len), "a".repeat(suffix_len));
                let expected = (millis_len == 13 || millis_len == 14) && suffix_len == 8;
                assert_eq!(is_backup_id(&id), expected, "{id}");
            }
            let bare = "1".repeat(millis_len);
            assert_eq!(is_backup_id(&bare), millis_len == 13, "{bare}");
        }
        let arn_with =
            |id: &str| format!("arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/{id}");
        assert_eq!(
            check_backup_arn(&arn_with("1")).unwrap_err(),
            BackupArnError::Id
        );
        assert_eq!(
            check_backup_arn(&arn_with("a4b1d6c2-0000-4000-8000-000000000000")).unwrap_err(),
            BackupArnError::Id
        );
        for invalid in [
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc:x",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc/x",
            "arn:aws:dynamodb:US-EAST-1:123456789012:table/Music/backup/01489602797149-73d8d5bc",
            "arn:aws:dynamodb:us-east-1:12345678901x:table/Music/backup/01489602797149-73d8d5bc",
            "arn:aws:s3:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc",
        ] {
            assert_eq!(
                check_backup_arn(invalid).unwrap_err(),
                BackupArnError::Invalid,
                "{invalid}"
            );
        }
        // Over the service maximum, with the trimmed length reported rather
        // than the input.
        let long = arn_with(&format!("{}-73d8d5bc", "1".repeat(1100)));
        let err = BackupArn::parse(&long).unwrap_err().to_string();
        assert!(err.contains("bytes>"), "{err}");
        assert!(err.len() < 200, "{}", err.len());
    }

    #[test]
    fn format_is_the_one_string() {
        let arn = format_backup_arn(
            "aws",
            "us-east-1",
            "123456789012",
            "Music",
            "01489602797149-73d8d5bc",
        );
        assert_eq!(arn, ARN);
        assert_eq!(BackupArn::parse(ARN).unwrap().to_arn_string(), arn);
        assert_eq!(trim_backup_arn(" \t\n"), "");
        assert_eq!(trim_backup_arn(&format!("\t{ARN}\n")), ARN);
        assert_eq!(
            trim_backup_arn("\u{a0}x"),
            "\u{a0}x",
            "non-ASCII whitespace is not trimmed"
        );
    }

    #[test]
    fn ids_are_fourteen_zero_padded_digits_and_eight_hex() {
        assert_eq!(
            format_backup_id(1_489_602_797_149, 0x73d8_d5bc),
            "01489602797149-73d8d5bc"
        );
        assert_eq!(format_backup_id(5, 1), "00000000000005-00000001");
        assert!(is_backup_id(&format_backup_id(5, 1)));
        assert_eq!(backup_id_epoch_millis(ARN), Some(1_489_602_797_149));
        assert!((backup_creation_date_time(ARN).unwrap() - 1_489_602_797.149).abs() < 1e-6);
        for bad in [
            "1489602797149-73d8d5bc",
            "01489602797149-73D8D5BC",
            "01489602797149",
            "0148960279714x-73d8d5bc",
            "01489602797149-73d8d5b",
            "a4b1d6c2-0000-4000-8000-000000000000",
        ] {
            assert_eq!(backup_id_epoch_millis(bad), None, "{bad}");
        }
    }

    #[test]
    fn new_rejects_bad_components() {
        const ID: &str = "01489602797149-73d8d5bc";
        assert!(BackupArn::new("aws", "us-east-1", "1234", "Music", "a/b").is_err());
        assert!(BackupArn::new("aws", "us-east-1", "12/34", "Music", ID).is_err());
        assert!(BackupArn::new("aws", "us-east-1", "1234", "Music", "...").is_err());
        assert!(BackupArn::new("aws", "us-east-1", "1234", "...", ID).is_err());
        // Short names are the engine's call (its minimum length is
        // configurable); the ARN accepts them.
        BackupArn::new("aws", "us-east-1", "1234", "ab", ID).unwrap();
        // The error names the failing input when it is short enough to show
        // whole (`truncated`: 80 bytes).
        let err = BackupArn::parse(&format!(
            "arn:aws:dynamodb:us-east-1:1:table/Mu sic/backup/{ID}"
        ))
        .unwrap_err();
        assert!(err.to_string().contains("Mu sic"), "{err}");
        assert!(BackupArn::new("aws", "us:east", "1234", "Music", ID).is_err());
        assert!(BackupArn::new("", "us-east-1", "1234", "Music", ID).is_err());
        // A component with surrounding whitespace would parse back trimmed.
        // Inner components are refused by the character rules; the round-trip
        // comparison is what catches whitespace at the two ends of the string.
        assert!(BackupArn::new("aws", "us-east-1", "1234", "Music ", ID).is_err());
        assert!(BackupArn::new("aws", "us-east-1", " 1234", "Music", ID).is_err());
        assert!(BackupArn::new(" aws", "us-east-1", "1234", "Music", ID).is_err());
        assert!(BackupArn::new("aws", "us-east-1", "1234", "Music", &format!("{ID}\n")).is_err());
        assert!(BackupArn::new("aws", "us-east-1", "1234", "Music", &format!("{ID} ")).is_err());
        // Dots inside names are fine; DynamoDB allows them.
        BackupArn::new("aws", "us-east-1", "1234", "a.b.c", ID).unwrap();
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
