// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The identifier and ARN of a backup, in the service's shape.
//!
//! One implementation for every backend: the id is part of the ARN a client
//! holds, so a backend that formatted it differently would issue ARNs another
//! backend's paging cursor could not place. The implementation itself is in
//! `extenddb_core::backup_format`, where the off-host backup format uses it
//! too; the tests here exercise it through this crate's re-exports so the
//! engine-facing contract is pinned where the engine imports it.

use super::arn::ARN_PARTITION;

// The id and ARN shape live in `extenddb_core::backup_format::layout`, which
// the off-host backup format shares; this module adds what needs `rand` or
// the deployment's partition, and re-exports the rest so the engine and the
// backends keep one import path.
pub use extenddb_core::backup_format::{
    BackupArnError, BackupArnParts, MAX_BACKUP_ARN_LEN, backup_creation_date_time,
    backup_id_epoch_millis, check_backup_arn, format_backup_id, is_backup_id, parse_backup_arn,
};

/// A new backup id: 14-digit zero-padded epoch milliseconds, `-`, 8 lowercase
/// hex digits from a random `u32` ([`format_backup_id`]).
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

/// The ARN of a backup in this deployment's partition:
/// `arn:aws:dynamodb:<region>:<account>:table/<table>/backup/<id>`, through
/// the one formatter (`extenddb_core::backup_format::format_backup_arn`).
/// Infallible for callers whose components come from the catalog.
#[must_use]
pub fn backup_arn(region: &str, account_id: &str, table_name: &str, backup_id: &str) -> String {
    extenddb_core::backup_format::format_backup_arn(
        ARN_PARTITION,
        region,
        account_id,
        table_name,
        backup_id,
    )
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
    fn backup_arn_matches_the_typed_form() {
        use extenddb_core::backup_format::BackupArn;
        let id = new_backup_id();
        let arn = backup_arn("us-east-1", "123456789012", "Music", &id);
        let typed =
            BackupArn::new(ARN_PARTITION, "us-east-1", "123456789012", "Music", &id).unwrap();
        assert_eq!(arn, typed.to_arn_string());
        assert_eq!(BackupArn::parse(&arn).unwrap(), typed);
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
