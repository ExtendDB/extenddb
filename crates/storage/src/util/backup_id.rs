// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The identifier and ARN of a backup, in the service's shape.
//!
//! One implementation for every backend: the id is part of the ARN a client
//! holds, so a backend that formatted it differently would issue ARNs another
//! backend's paging cursor could not place.

use super::arn::ARN_PARTITION;

/// Digits of the epoch-millisecond prefix. The service zero-pads to 14
/// (`01489602797149-73d8d5bc`); 13 digits cover today's instants, so the
/// padding is visible as a leading zero.
const MILLIS_DIGITS: usize = 14;

/// A new backup id: 14-digit zero-padded epoch milliseconds, `-`, 8 lowercase
/// hex digits from a random `u32`.
///
/// The random part is part of the identifier, not decoration: a timestamp
/// alone makes a backup id derivable by anyone who knows roughly when the
/// backup ran. It also keeps two backups created in the same millisecond
/// apart.
///
/// Use [`backup_creation_date_time`] on the returned id for the backup's
/// `BackupCreationDateTime`, so the ARN and the reported time agree and the
/// `ListBackups` cursor can place an ARN whose backup has since been deleted.
#[must_use]
pub fn new_backup_id() -> String {
    use rand::Rng;
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default();
    format_backup_id(millis, rand::rng().random())
}

/// Format a backup id from its parts. Exposed for tests and fixtures.
#[must_use]
pub fn format_backup_id(epoch_millis: u64, suffix: u32) -> String {
    format!("{epoch_millis:0MILLIS_DIGITS$}-{suffix:08x}")
}

/// The epoch milliseconds a backup id (or a whole backup ARN) was minted at,
/// or `None` if it is not in the shape [`new_backup_id`] produces.
#[must_use]
pub fn backup_id_epoch_millis(id_or_arn: &str) -> Option<u64> {
    let id = id_or_arn.rsplit('/').next()?;
    let (millis, suffix) = id.split_once('-')?;
    if millis.len() != MILLIS_DIGITS
        || !millis.bytes().all(|b| b.is_ascii_digit())
        || suffix.len() != 8
        || !suffix
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    millis.parse().ok()
}

/// `BackupCreationDateTime` (epoch seconds, millisecond precision) for a
/// backup id from [`new_backup_id`].
#[must_use]
#[allow(clippy::cast_precision_loss)] // epoch ms fit f64's 53-bit mantissa
pub fn backup_creation_date_time(id_or_arn: &str) -> Option<f64> {
    backup_id_epoch_millis(id_or_arn).map(|ms| ms as f64 / 1000.0)
}

/// The ARN of a backup: `arn:aws:dynamodb:<region>:<account>:table/<table>/backup/<id>`.
#[must_use]
pub fn backup_arn(region: &str, account_id: &str, table_name: &str, backup_id: &str) -> String {
    format!(
        "arn:{ARN_PARTITION}:dynamodb:{region}:{account_id}:table/{table_name}/backup/{backup_id}"
    )
}

/// The parts of a well-formed backup ARN.
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
/// service answers with (measured on DynamoDB). Which text goes with which
/// input was captured for one input only (a bare 13-digit id is "not
/// valid"), so the split is the parser's: a string that is not an ARN of the
/// right shape, is too long, or carries a character no ARN part allows is
/// `Invalid`; an ARN of the right shape whose backup id is not one is
/// `Id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupArnError {
    /// Not `arn:<partition>:dynamodb:<region>:<account>:table/<name>/backup/<id>`
    /// with a non-empty region, account, and name, over
    /// [`MAX_BACKUP_ARN_LEN`] bytes, with a control or non-ASCII character, or
    /// with a region outside `[a-z0-9-]`, a table name outside
    /// `[a-zA-Z0-9_.-]`, or an account outside `[0-9]`. The service's text:
    /// `Invalid Request: BackupArn is not valid`.
    Invalid,
    /// The backup id is neither [`new_backup_id`]'s shape nor one an earlier
    /// build issued (13 digits, `-`, 8 hex digits; 13 bare digits). The
    /// service's text: `Invalid Backup ARN`.
    Id,
}

/// The longest `BackupArn` the service model accepts (`BackupArn` shape,
/// botocore 1.33.13: max 1024).
pub const MAX_BACKUP_ARN_LEN: usize = 1024;

/// Parse a backup ARN, or `None` if [`check_backup_arn`] refuses it.
#[must_use]
pub fn parse_backup_arn(arn: &str) -> Option<BackupArnParts<'_>> {
    check_backup_arn(arn).ok()
}

/// Whether every byte is in `allowed` or an ASCII alphanumeric.
fn ascii_word(s: &str, allowed: &[u8]) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || allowed.contains(&b))
}

/// Whether `id` is a backup id this codebase ever issued: 14 or 13 digits,
/// `-`, 8 lowercase hex digits (the 14-digit form is [`new_backup_id`]'s;
/// the 13-digit form is what every backend issued before the shared id), or
/// 13 bare digits (epoch milliseconds with no suffix, the form PostgreSQL
/// issued in v0.1.0 to v0.1.2 and MongoDB before its suffix was added). No
/// build has put a UUID in a backup ARN, so none is accepted.
#[must_use]
pub fn is_backup_id(id: &str) -> bool {
    let hex = |s: &str, n: usize| {
        s.len() == n
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    let digits = |s: &str, n: usize| s.len() == n && s.bytes().all(|b| b.is_ascii_digit());
    match id.split_once('-') {
        Some((millis, suffix)) => {
            (digits(millis, MILLIS_DIGITS) || digits(millis, MILLIS_DIGITS - 1)) && hex(suffix, 8)
        }
        None => digits(id, MILLIS_DIGITS - 1),
    }
}

/// Parse a backup ARN after trimming surrounding ASCII whitespace (the
/// service resolves an ARN with a trailing newline, measured on DynamoDB;
/// trimming every surrounding ASCII whitespace character is this parser's
/// generalisation of that one measurement, and the length limit below is
/// applied to what remains), refusing one that
/// is not exactly six colon-separated fields, not `arn:<partition>:dynamodb:...`,
/// has an empty or malformed region, account, or table name, a resource
/// other than `table/<name>/backup/<id>`, a control or non-ASCII character,
/// more than [`MAX_BACKUP_ARN_LEN`] bytes, or an id that is not a backup id
/// ([`is_backup_id`]). The region is checked for shape only: an ARN in
/// another region of the caller's account resolves on the service, so the
/// engine substitutes its own region rather than refuse it.
///
/// # Errors
///
/// [`BackupArnError`], one class per service text.
pub fn check_backup_arn(arn: &str) -> Result<BackupArnParts<'_>, BackupArnError> {
    let arn = arn.trim_matches(|c: char| c.is_ascii_whitespace());
    if arn.len() > MAX_BACKUP_ARN_LEN || !arn.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(BackupArnError::Invalid);
    }
    let fields: Vec<&str> = arn.split(':').collect();
    let [prefix, partition, service, region, account_id, resource] = fields.as_slice() else {
        return Err(BackupArnError::Invalid);
    };
    if *prefix != "arn"
        || !ascii_word(partition, b"-")
        || *service != "dynamodb"
        || !ascii_word(region, b"-")
        || region.bytes().any(|b| b.is_ascii_uppercase())
        || account_id.is_empty()
        || !account_id.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(BackupArnError::Invalid);
    }
    let parts: Vec<&str> = resource.split('/').collect();
    let ["table", table_name, "backup", backup_id] = parts.as_slice() else {
        return Err(BackupArnError::Invalid);
    };
    if !ascii_word(table_name, b"_.-") || backup_id.is_empty() {
        return Err(BackupArnError::Invalid);
    }
    if !is_backup_id(backup_id) {
        return Err(BackupArnError::Id);
    }
    Ok(BackupArnParts {
        arn,
        partition,
        region,
        account_id,
        table_name,
        backup_id,
    })
}

/// Whether `arn` names a backup or a path under one: its resource (the sixth
/// colon-separated field onward) is `table/<name>/backup` or begins
/// `table/<name>/backup/`.
///
/// Deliberately broader than [`parse_backup_arn`]: every ARN that parses as a
/// backup ARN matches, and so do truncated and extended forms
/// (`.../backup`, `.../backup/`, `.../backup/<id>/x`, `.../backup/<id>:x`).
/// Partition and service are not checked. `DynamoDB` backups carry no tags
/// (the service's authorization reference lists no `aws:ResourceTag` key for
/// the `backup` resource and `TagResource` takes only `table` and `stream`),
/// so tagging refuses these ARNs and authorization reads no tags for them; one
/// predicate keeps the two sets the same.
#[must_use]
pub fn is_backup_resource_arn(arn: &str) -> bool {
    let Some(resource) = arn.splitn(6, ':').nth(5) else {
        return false;
    };
    let mut segments = resource.split('/');
    segments.next() == Some("table")
        && segments.next().is_some()
        && segments.next() == Some("backup")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_fourteen_zero_padded_digits_and_eight_hex() {
        assert_eq!(
            format_backup_id(1_489_602_797_149, 0x73d8_d5bc),
            "01489602797149-73d8d5bc"
        );
        assert_eq!(format_backup_id(5, 1), "00000000000005-00000001");
        let id = new_backup_id();
        assert_eq!(id.len(), 14 + 1 + 8, "{id}");
        assert!(id.starts_with('0'), "{id}");
        assert!(backup_id_epoch_millis(&id).is_some(), "{id}");
    }

    #[test]
    fn creation_time_comes_from_the_id() {
        let arn = backup_arn("r", "1", "t", "01489602797149-73d8d5bc");
        assert_eq!(backup_id_epoch_millis(&arn), Some(1_489_602_797_149));
        assert!((backup_creation_date_time(&arn).unwrap() - 1_489_602_797.149).abs() < 1e-6);
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
    fn parses_well_formed_arns_only() {
        let arn =
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc";
        let p = parse_backup_arn(arn).unwrap();
        assert_eq!(p.arn, arn);
        assert_eq!(p.account_id, "123456789012");
        assert_eq!(p.region, "us-east-1");
        assert_eq!(p.table_name, "Music");
        assert_eq!(p.backup_id, "01489602797149-73d8d5bc");
        // Ids earlier builds issued still parse: 13 digits with the hex
        // suffix (every backend before the shared id) and 13 bare digits
        // (PostgreSQL v0.1.0 to v0.1.2).
        for id in ["1489602797149-73d8d5bc", "1489602797149"] {
            let arn = format!("arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/{id}");
            assert_eq!(parse_backup_arn(&arn).unwrap().backup_id, id, "{id}");
            assert!(is_backup_id(id), "{id}");
        }
        // Surrounding ASCII whitespace is trimmed (the service resolves an
        // ARN with a trailing newline).
        let padded = format!(" \t{arn}\n");
        let p = check_backup_arn(&padded).unwrap();
        assert_eq!(p.arn, arn);
        assert_eq!(p.backup_id, "01489602797149-73d8d5bc");
        // Another region or partition parses; the engine decides what to do.
        assert_eq!(
            parse_backup_arn(
                "arn:aws-cn:dynamodb:cn-north-1:123456789012:table/Music/backup/01489602797149-73d8d5bc"
            )
            .unwrap()
            .partition,
            "aws-cn"
        );

        for bad in [
            "",
            "not-an-arn",
            "arn:aws:dynamodb",
            "arn:aws:dynamodb:us-east-1",
            "arn:aws:dynamodb:us-east-1::table/Music/backup/01489602797149-73d8d5bc",
            "arn:aws:dynamodb:us-east-1:table/Music/backup/123456789012",
            "arn:aws:dynamodb::123456789012:table/Music/backup/01489602797149-73d8d5bc",
            "arn:aws:s3:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc",
            "xrn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/",
            "arn:aws:dynamodb:us-east-1:123456789012:table//backup/01489602797149-73d8d5bc",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/stream/01489602797149-73d8d5bc",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc/x",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc:x",
            // Region: a slash, `..`, uppercase, a space, empty.
            "arn:aws:dynamodb:us/east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc",
            "arn:aws:dynamodb:..:123456789012:table/Music/backup/01489602797149-73d8d5bc",
            "arn:aws:dynamodb:US-EAST-1:123456789012:table/Music/backup/01489602797149-73d8d5bc",
            // Account: not digits.
            "arn:aws:dynamodb:us-east-1:12345678901x:table/Music/backup/01489602797149-73d8d5bc",
            // Table name outside [a-zA-Z0-9_.-].
            "arn:aws:dynamodb:us-east-1:123456789012:table/Mu sic/backup/01489602797149-73d8d5bc",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Mu*/backup/01489602797149-73d8d5bc",
            // Control, NUL, and non-ASCII characters.
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc\u{0}",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Mus\u{1}ic/backup/01489602797149-73d8d5bc",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Musïc/backup/01489602797149-73d8d5bc",
            // Whitespace inside.
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/ 01489602797149-73d8d5bc",
        ] {
            assert_eq!(
                check_backup_arn(bad),
                Err(BackupArnError::Invalid),
                "{bad:?}"
            );
        }
        // Over the service model's maximum length.
        let long = format!(
            "arn:aws:dynamodb:us-east-1:123456789012:table/{}/backup/01489602797149-73d8d5bc",
            "x".repeat(1000)
        );
        assert_eq!(check_backup_arn(&long), Err(BackupArnError::Invalid));

        // The right shape with an id that is not a backup id.
        for id in [
            "1",
            "*",
            "01489602797149-*",
            "01489602797149",
            "0148960279714-73d8d5b",    // 13 digits, 7 hex
            "01489602797149-73d8d5b",   // 7 hex
            "01489602797149-73d8d5bcd", // 9 hex
            "01489602797149-73D8D5BC",  // uppercase hex
            "0148960279714x-73d8d5bc",
            "148960279714",   // 12 bare digits
            "148960279714x",  // 13 characters, not all digits
            "1489602797149-", // 13 digits and an empty suffix
            // No build issued a UUID, so none resolves.
            "a4b1d6c2-0000-4000-8000-000000000000",
            "A4B1D6C2-0000-4000-8000-000000000000",
            "a4b1d6c200004000800000000000000000",
        ] {
            let arn = format!("arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/{id}");
            assert_eq!(check_backup_arn(&arn), Err(BackupArnError::Id), "{id}");
            assert!(parse_backup_arn(&arn).is_none(), "{id}");
        }
    }

    #[test]
    fn backup_resource_arns_cover_every_parsed_backup_arn_and_its_variants() {
        const P: &str = "arn:aws:dynamodb:us-east-1:123456789012";
        let backups = [
            format!("{P}:table/Music/backup/01489602797149-73d8d5bc"),
            format!("{P}:table/Music/backup/1"),
            format!("{P}:table/Music/backup/"),
            format!("{P}:table/Music/backup"),
            format!("{P}:table/Music/backup/1/x"),
            format!("{P}:table/Music/backup/1:x"),
            format!("{P}:table//backup/1"),
            // A table named `backup` with a backup of it.
            format!("{P}:table/backup/backup/1"),
            "arn:aws:dynamodb:eu-west-1:210987654321:table/Music/backup/1".to_owned(),
            "arn:aws-cn:dynamodb:cn-north-1:123456789012:table/Music/backup/1".to_owned(),
        ];
        for arn in &backups {
            assert!(is_backup_resource_arn(arn), "{arn}");
        }
        let others = [
            String::new(),
            "not-an-arn".to_owned(),
            format!("{P}:table/Music"),
            // A table named `backup`.
            format!("{P}:table/backup"),
            format!("{P}:table/Music/stream/2026-01-01T00:00:00.000"),
            format!("{P}:table/Music/index/byArtist"),
            format!("{P}:table/Music/export/1"),
            format!("{P}:table/Music/backups/1"),
            format!("{P}:table/Music/Backup/1"),
            format!("{P}:backup/Music/backup/1"),
            "arn:aws:dynamodb:us-east-1:table/Music/backup/1".to_owned(),
        ];
        for arn in &others {
            assert!(!is_backup_resource_arn(arn), "{arn}");
        }
        // Superset of what authorization evaluates as a backup resource.
        for arn in backups.iter().chain(&others) {
            if parse_backup_arn(arn).is_some() {
                assert!(is_backup_resource_arn(arn), "{arn}");
            }
        }
    }
}
