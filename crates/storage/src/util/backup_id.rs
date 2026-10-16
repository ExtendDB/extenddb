// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The identifier and ARN of a backup, in the service's shape.
//!
//! One implementation for every backend: the id is part of the ARN a client
//! holds, so a backend that formatted it differently would issue ARNs another
//! backend's paging cursor could not place.

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
    format!("arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/backup/{backup_id}")
}

/// The parts of a well-formed backup ARN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupArnParts<'a> {
    pub partition: &'a str,
    pub region: &'a str,
    pub account_id: &'a str,
    pub table_name: &'a str,
    pub backup_id: &'a str,
}

/// Parse a backup ARN, or `None` if it is malformed: not exactly six
/// colon-separated fields, not `arn:<partition>:dynamodb:...`, an empty
/// region or account, or a resource other than
/// `table/<name>/backup/<id>` with non-empty name and id.
///
/// The backup id is not required to be in [`new_backup_id`]'s shape, so ARNs
/// issued by earlier builds (13-digit timestamps, UUIDs) still parse.
#[must_use]
pub fn parse_backup_arn(arn: &str) -> Option<BackupArnParts<'_>> {
    let fields: Vec<&str> = arn.split(':').collect();
    let [prefix, partition, service, region, account_id, resource] = fields.as_slice() else {
        return None;
    };
    if *prefix != "arn"
        || partition.is_empty()
        || *service != "dynamodb"
        || region.is_empty()
        || account_id.is_empty()
    {
        return None;
    }
    let parts: Vec<&str> = resource.split('/').collect();
    let ["table", table_name, "backup", backup_id] = parts.as_slice() else {
        return None;
    };
    if table_name.is_empty() || backup_id.is_empty() {
        return None;
    }
    Some(BackupArnParts {
        partition,
        region,
        account_id,
        table_name,
        backup_id,
    })
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
        assert_eq!(p.account_id, "123456789012");
        assert_eq!(p.table_name, "Music");
        assert_eq!(p.backup_id, "01489602797149-73d8d5bc");
        for bad in [
            "",
            "not-an-arn",
            "arn:aws:dynamodb",
            "arn:aws:dynamodb:us-east-1",
            "arn:aws:dynamodb:us-east-1::table/Music/backup/1",
            "arn:aws:dynamodb:us-east-1:table/Music/backup/123456789012",
            "arn:aws:dynamodb::123456789012:table/Music/backup/1",
            "arn:aws:s3:us-east-1:123456789012:table/Music/backup/1",
            "xrn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/",
            "arn:aws:dynamodb:us-east-1:123456789012:table//backup/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/stream/1",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/1/x",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/1:x",
        ] {
            assert!(parse_backup_arn(bad).is_none(), "{bad:?}");
        }
    }
}
