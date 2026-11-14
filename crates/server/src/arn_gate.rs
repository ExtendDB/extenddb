// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The ARN shapes `table_name_gate` refuses before any catalog lookup, with the
//! service's sentences (measured 2026-10-24 unless marked derived).
//!
//! A `TableName` may be a table ARN. On the item operations the service checks
//! the shape, the vendor, the region, the table segment and the account, in
//! that order (the order measured 2026-11-07, except the region before the
//! table segment, which is derived):
//!
//! | `TableName` | sentence at `'tableName'` |
//! |---|---|
//! | not a plain table ARN (too few fields, a resource that is not `table/<name>`, an index, stream or backup suffix) | the `Valid ARN format is ...` frame |
//! | a plain table ARN of another service | `Invalid vendor code` (measured 2026-11-07) |
//! | a bad region segment | `Invalid AWS region` |
//! | a table segment that fails the pattern | `Table name of ARN must satisfy regular expression pattern: ...` |
//! | an account segment that is not 12 ASCII digits, the empty one included | `Invalid subscriber ID` |
//!
//! The control-plane operations in [`CONTROL_PLANE`] check the account first
//! (measured 2026-11-07): an ARN whose account segment is not the caller's
//! gets the cross-account answer, whatever its shape, vendor or region.
//! `authz_resource::cross_account` gives that answer for an ARN `arn::parse`
//! reads, so the gate passes those on. For any other shape the gate gives
//! that layer's answer for an account that does not exist, `Access is denied`
//! (derived for an account the catalog holds). This covers `DescribeTable`
//! with `arn:aws:s3:::<name>`, whose empty account segment is not the
//! caller's (AccessDeniedException, measured 2026-10-24). In the caller's
//! account they answer the shape and the table segment as the item operations
//! do, and a vendor or region fault as `1 validation error detected: Invalid
//! vendor code in '<arn>'` or `... Invalid AWS region in '<arn>'`, with no
//! location (measured 2026-11-06 and 2026-11-07).
//!
//! The other ARN-bearing fields: `SourceTableArn` (`RestoreTableToPointInTime`)
//! and `TableArn` (`ExportTableToPointInTime`) with a bad table segment,
//! `StreamArn` (`DescribeStream`, `GetShardIterator`), `ListStreams`'
//! `ExclusiveStartStreamArn` and a tag operation's
//! stream `ResourceArn` with a bad table segment or U+0000 in the label,
//! `BackupArn` (`DescribeBackup`, `DeleteBackup`, `RestoreTableFromBackup`)
//! with a bad table segment, and a tag operation's table `ResourceArn` with a
//! bad table segment. Each has its own sentence below.

use extenddb_core::arn::{SubResource, fields, parse};
use extenddb_core::error::DynamoDbError;

pub(crate) const PATTERN: &str = "[a-zA-Z0-9_.-]+";

/// The operations whose `TableName` ARN gets the control-plane answers (see
/// the module docs). The account answers were measured 2026-11-07 for each,
/// except `DeleteTable`, which was not sent and takes `UpdateTable`'s
/// answers, and `CreateTable` with a well-formed account of another caller,
/// which was not sent. The region fault was measured 2026-11-06 for each
/// except `CreateBackup`, and the vendor fault and the frame in the caller's
/// account 2026-11-07 for `DescribeTable`, `DescribeTimeToLive`,
/// `DescribeContinuousBackups` and `ListBackups`; the others are derived.
pub(crate) const CONTROL_PLANE: &[&str] = &[
    "CreateTable",
    "DescribeTable",
    "UpdateTable",
    "DeleteTable",
    "DescribeTimeToLive",
    "UpdateTimeToLive",
    "DescribeContinuousBackups",
    "UpdateContinuousBackups",
    "CreateBackup",
    "ListBackups",
];

const FRAME: &str = "Valid ARN format is \
    'arn:<awsPartition>:<vendor>:<region>:<subscriber>:resourceType/resourceName', \
    where Resource name must have length less than or equal to 255, \
    Resource name must have length greater than or equal to 3, \
    Resource name must satisfy regular expression pattern: [a-zA-Z0-9_.-]+";

/// The `RequestItems` map-key constraint list for a key that is an ARN.
pub(crate) const MAP_KEYS_ARN: &str = "Map keys must satisfy constraint: \
    [Valid resource name or resource arn must be given. Valid resource arn format is \
    'arn:<awsPartition>:<vendor>:<region>:<subscriber>:resourceType/resourceName', \
    where Resource name must have length less than or equal to 255, \
    Resource name must have length greater than or equal to 3, \
    Resource name must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]";

pub(crate) fn in_pattern(name: &str) -> bool {
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
}

fn validation(message: String) -> DynamoDbError {
    DynamoDbError::ValidationException(message)
}

fn one_error(value: &str, location: &str, constraint: &str) -> DynamoDbError {
    validation(format!(
        "1 validation error detected: Value '{value}' at '{location}' failed to satisfy \
         constraint: {constraint}"
    ))
}

/// What is wrong with an `arn:` value read as a table ARN, in the order the
/// service checks it on the item operations.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ArnFault {
    /// Not a plain table ARN: too few fields, a resource that is not
    /// `table/<name>`, an index, stream or backup suffix, or a shape
    /// `arn::parse` rejects.
    NotPlainTable,
    /// A plain table ARN of another service.
    Vendor,
    /// A region segment that carries a character outside `[a-z0-9-]`.
    Region,
    /// A table segment that fails the pattern.
    TableSegment,
    /// An account segment that is not 12 ASCII digits, the empty one included.
    Account,
}

/// Inspect an `arn:` value the way the service checks a `TableName` ARN on
/// the item operations. `None` means a well-formed plain table ARN whose
/// segments all pass.
pub(crate) fn table_arn_fault(arn: &str) -> Option<ArnFault> {
    let res = match parse(arn) {
        Some(res) if res.sub.is_none() => res,
        _ => return Some(ArnFault::NotPlainTable),
    };
    if res.service != "dynamodb" {
        return Some(ArnFault::Vendor);
    }
    if !res
        .region
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Some(ArnFault::Region);
    }
    if !in_pattern(&res.table) {
        return Some(ArnFault::TableSegment);
    }
    // 11 and 13 digits measured 2026-11-06, the empty account 2026-11-07.
    (!extenddb_core::arn::is_account_id(&res.account)).then_some(ArnFault::Account)
}

fn arn_constraint(fault: ArnFault) -> &'static str {
    match fault {
        ArnFault::NotPlainTable => FRAME,
        ArnFault::Vendor => "Invalid vendor code",
        ArnFault::Region => "Invalid AWS region",
        ArnFault::TableSegment => {
            "Table name of ARN must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
        }
        ArnFault::Account => "Invalid subscriber ID",
    }
}

/// A `TableName` that is an ARN, for `operation` called from `account_id`.
/// `Ok` passes the value on: a well-formed table ARN to the handlers, which
/// resolve it, and on a control-plane operation an ARN of another account
/// that `arn::parse` reads to `authz_resource::cross_account`.
pub(crate) fn check_table_name_arn(
    operation: &str,
    arn: &str,
    account_id: &str,
) -> Result<(), DynamoDbError> {
    let control_plane = CONTROL_PLANE.contains(&operation);
    if control_plane && fields(arn).is_some_and(|f| f.account != account_id) {
        if parse(arn).is_some() {
            return Ok(());
        }
        // `cross_account` cannot read this shape: give its answer for an
        // account that does not exist (it names no principal).
        return Err(crate::authz_resource::cross_account::refusal(
            operation, operation, arn, false, "",
        ));
    }
    let Some(fault) = table_arn_fault(arn) else {
        return Ok(());
    };
    if control_plane {
        let segment = match fault {
            ArnFault::Vendor => Some("vendor code"),
            ArnFault::Region => Some("AWS region"),
            _ => None,
        };
        if let Some(segment) = segment {
            return Err(validation(format!(
                "1 validation error detected: Invalid {segment} in '{arn}'"
            )));
        }
    }
    Err(one_error(arn, "tableName", arn_constraint(fault)))
}

/// A `TransactItems` member's `TableName` that is an ARN, at `location`
/// (`transactItems.<n>.member.<op>.tableName`). A plain table ARN with a bad
/// table segment is measured; the other faults take the same constraints as a
/// top-level `TableName` (derived).
pub(crate) fn check_transact_member_arn(arn: &str, location: &str) -> Result<(), DynamoDbError> {
    match table_arn_fault(arn) {
        None => Ok(()),
        Some(fault) => Err(one_error(arn, location, arn_constraint(fault))),
    }
}

/// `SourceTableArn` of `RestoreTableToPointInTime` and `TableArn` of
/// `ExportTableToPointInTime` with a table segment that fails the pattern.
/// The server refuses every `RestoreTableToPointInTime` before this code runs.
pub(crate) fn check_table_arn_field(operation: &str, arn: &str) -> Result<(), DynamoDbError> {
    let bad = matches!(parse(arn), Some(res) if res.sub.is_none() && !in_pattern(&res.table));
    if !bad {
        return Ok(());
    }
    Err(validation(
        match operation {
            "RestoreTableToPointInTime" => "Invalid Request: Invalid TableArn: Invalid Table ARN",
            _ => "Invalid TableArn: Invalid Table ARN",
        }
        .to_owned(),
    ))
}

/// A stream ARN whose table segment fails the pattern or whose label carries
/// U+0000: `Invalid StreamArn` (measured for `DescribeStream`,
/// `GetShardIterator` and `TagResource`, and on 2026-11-06 for the
/// `ListStreams` `ExclusiveStartStreamArn`, with and without `TableName`).
pub(crate) fn check_stream_arn(arn: &str) -> Result<(), DynamoDbError> {
    match parse(arn) {
        Some(res)
            if matches!(&res.sub, Some(SubResource::Stream(label))
                if !in_pattern(&res.table) || label.contains('\u{0}')) =>
        {
            Err(validation("Invalid StreamArn".to_owned()))
        }
        _ => Ok(()),
    }
}

/// A backup ARN whose table segment fails the pattern: `Invalid Backup ARN`
/// (measured for `DescribeBackup`, `DeleteBackup` and
/// `RestoreTableFromBackup`). U+0000 in the backup id keeps the engine's
/// `Invalid Request: BackupArn is not valid`, as measured.
pub(crate) fn check_backup_arn(arn: &str) -> Result<(), DynamoDbError> {
    match parse(arn.trim_matches(|c: char| c.is_ascii_whitespace())) {
        Some(res) if matches!(res.sub, Some(SubResource::Backup(_))) && !in_pattern(&res.table) => {
            Err(validation("Invalid Backup ARN".to_owned()))
        }
        _ => Ok(()),
    }
}

/// A tag operation's `ResourceArn`: a table ARN with a bad table segment is
/// `Invalid TableArn: Invalid ResourceArn provided as input <arn>` (measured
/// for `TagResource`; `UntagResource` and `ListTagsOfResource` the same), a
/// stream ARN with a bad table segment or U+0000 in the label is
/// `Invalid StreamArn`. An index or backup ARN keeps the shape check's verdict.
pub(crate) fn check_tag_resource_arn(arn: &str) -> Result<(), DynamoDbError> {
    check_stream_arn(arn)?;
    match parse(arn) {
        Some(res) if res.sub.is_none() && !in_pattern(&res.table) => Err(validation(format!(
            "Invalid TableArn: Invalid ResourceArn provided as input {arn}"
        ))),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCT: &str = "123456789012";
    fn table(name: &str) -> String {
        format!("arn:aws:dynamodb:us-east-1:{ACCT}:table/{name}")
    }
    fn message(r: Result<(), DynamoDbError>) -> Option<String> {
        match r {
            Ok(()) => None,
            Err(DynamoDbError::ValidationException(m)) => Some(m),
            Err(other) => panic!("{other:?}"),
        }
    }

    /// The gate's answer for `arn` on `op`, called from `ACCT`.
    fn arn_check(op: &str, arn: &str) -> Result<(), DynamoDbError> {
        check_table_name_arn(op, arn, ACCT)
    }
    fn access_denied(r: Result<(), DynamoDbError>) -> bool {
        matches!(r, Err(DynamoDbError::AccessDeniedException(m)) if m == "Access is denied")
    }
    fn at_table_name(arn: &str, constraint: &str) -> String {
        format!(
            "1 validation error detected: Value '{arn}' at 'tableName' failed to satisfy \
             constraint: {constraint}"
        )
    }

    #[test]
    fn a_plain_table_arn_with_a_bad_segment_gets_the_table_name_of_arn_sentence() {
        let arn = table("t367\u{0}name");
        let m = message(arn_check("PutItem", &arn)).expect("refused");
        assert_eq!(
            m,
            at_table_name(
                &arn,
                "Table name of ARN must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
            )
        );
        assert!(arn_check("PutItem", &table("t367name")).is_ok());
    }

    #[test]
    fn region_and_account_faults_name_their_segment() {
        let region = format!("arn:aws:dynamodb:us-ea\u{0}st-1:{ACCT}:table/t367name");
        assert!(
            message(arn_check("PutItem", &region))
                .unwrap()
                .ends_with("failed to satisfy constraint: Invalid AWS region")
        );
        let account = "arn:aws:dynamodb:us-east-1:1234\u{0}56789012:table/t367name";
        assert!(
            message(arn_check("PutItem", account))
                .unwrap()
                .ends_with("failed to satisfy constraint: Invalid subscriber ID")
        );
    }

    #[test]
    fn an_account_segment_of_eleven_or_thirteen_digits_is_an_account_fault() {
        for account in ["12345678901", "1234567890123", "12345678901x", ""] {
            let arn = format!("arn:aws:dynamodb:us-east-1:{account}:table/t367name");
            for op in ["GetItem", "PutItem", "UpdateItem", "Query", "Scan"] {
                assert!(
                    message(arn_check(op, &arn))
                        .unwrap()
                        .ends_with("failed to satisfy constraint: Invalid subscriber ID"),
                    "{op} {arn}"
                );
            }
            let m = message(check_transact_member_arn(
                &arn,
                "transactItems.1.member.get.tableName",
            ))
            .unwrap();
            assert!(m.ends_with("constraint: Invalid subscriber ID"), "{m}");
        }
        assert!(arn_check("PutItem", &table("t367name")).is_ok());
    }

    /// Measured 2026-11-07 with GetItem and TransactGetItems: the shape first,
    /// then the vendor, the region, the table segment and the account.
    #[test]
    fn item_operations_check_the_shape_and_vendor_before_the_segments() {
        let cases = [
            ("arn:aws:dynamodb:us-east-1:1234\u{0}:tbl", FRAME),
            ("arn:aws:dynamodb:us-east-1:12345678901:table/t/", FRAME),
            (
                "arn:aws:dynamodb:us-east-1:1234567890123:table/t/index/gsi1",
                FRAME,
            ),
            ("arn:aws:dynamodb:us-ea\u{0}st-1:123456789012:tbl", FRAME),
            ("arn:aws:s3:us-east-1:1234567890123:t367name", FRAME),
            (
                "arn:aws:s3:us-east-1:12345678901x:table/t",
                "Invalid vendor code",
            ),
            (
                "arn:aws:s3:us-ea\u{0}st-1:123456789012:table/t",
                "Invalid vendor code",
            ),
            (
                "arn:aws:dynamodb:us-ea\u{0}st-1:1234\u{0}:table/t367name",
                "Invalid AWS region",
            ),
            (
                "arn:aws:dynamodb:us-east-1:12345678901:table/t367\u{0}name",
                "Table name of ARN must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
            ),
            (
                "arn:aws:dynamodb:us-east-1:1234\u{0}:table/t367\u{0}name",
                "Table name of ARN must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
            ),
            (
                "arn:aws:dynamodb:us-east-1::table/t367name",
                "Invalid subscriber ID",
            ),
        ];
        for (arn, constraint) in cases {
            assert_eq!(
                message(arn_check("GetItem", arn)).as_deref(),
                Some(at_table_name(arn, constraint).as_str()),
                "{arn:?}"
            );
            let m = message(check_transact_member_arn(
                arn,
                "transactItems.1.member.get.tableName",
            ))
            .unwrap();
            assert!(m.ends_with(&format!("constraint: {constraint}")), "{m}");
        }
    }

    /// Measured 2026-11-07: an ARN of another account gets the cross-account
    /// answer before any other check. An ARN `arn::parse` reads is passed on to
    /// `cross_account`; for any other shape the gate gives its answer.
    #[test]
    fn control_plane_operations_answer_another_account_before_the_shape() {
        for op in CONTROL_PLANE {
            for account in [
                "1234\u{0}",
                "12345678901x",
                "12345678901",
                "1234567890123",
                "999999999999",
            ] {
                for resource in [
                    "table/t367name",
                    "table/t/index/gsi1",
                    "table/t/backup/01-ab",
                ] {
                    let arn = format!("arn:aws:dynamodb:us-east-1:{account}:{resource}");
                    assert!(arn_check(op, &arn).is_ok(), "{op} {arn:?}");
                }
                let s3 = format!("arn:aws:s3:us-east-1:{account}:table/t");
                assert!(arn_check(op, &s3).is_ok(), "{op} {s3:?}");
                for unread in [
                    format!("arn:aws:dynamodb:us-east-1:{account}:tbl"),
                    format!("arn:aws:dynamodb:us-east-1:{account}:table/t/"),
                    format!("arn:aws:dynamodb:us-east-1:{account}:table/t/stream/a/b"),
                    format!("arn:aws:dynamodb:us-ea\u{0}st-1:{account}:tbl"),
                    format!("arn:aws:s3:us-east-1:{account}:t"),
                ] {
                    assert!(access_denied(arn_check(op, &unread)), "{op} {unread:?}");
                }
            }
            assert!(
                access_denied(arn_check(op, "arn:aws:s3:::t367\u{0}name")),
                "{op}"
            );
        }
        // The item operations keep their answers for the same shapes.
        let unread = "arn:aws:dynamodb:us-east-1:1234\u{0}:tbl";
        assert_eq!(
            message(arn_check("PutItem", unread)).unwrap(),
            at_table_name(unread, FRAME)
        );
    }

    #[test]
    fn control_plane_operations_word_vendor_and_region_faults_alone() {
        let region = format!("arn:aws:dynamodb:us-ea\u{0}st-1:{ACCT}:table/t367name");
        let vendor = format!("arn:aws:s3:us-ea\u{0}st-1:{ACCT}:table/t367\u{0}name");
        for op in CONTROL_PLANE {
            assert_eq!(
                message(arn_check(op, &region)).unwrap(),
                format!("1 validation error detected: Invalid AWS region in '{region}'"),
                "{op}"
            );
            assert_eq!(
                message(arn_check(op, &vendor)).unwrap(),
                format!("1 validation error detected: Invalid vendor code in '{vendor}'"),
                "{op}"
            );
            // The shape and the table segment are the item operations' answers.
            for arn in [
                format!("arn:aws:dynamodb:us-ea\u{0}st-1:{ACCT}:tbl"),
                format!("arn:aws:s3:us-east-1:{ACCT}:t"),
                format!("{}/index/gsi1", table("t367name")),
                format!("{}/", table("t367name")),
            ] {
                assert_eq!(
                    message(arn_check(op, &arn)).unwrap(),
                    at_table_name(&arn, FRAME),
                    "{op}"
                );
            }
            let bad = table("t367\u{0}name");
            assert!(
                message(arn_check(op, &bad))
                    .unwrap()
                    .contains("Table name of ARN must satisfy"),
                "{op}"
            );
        }
    }

    #[test]
    fn other_shapes_get_the_valid_arn_format_frame() {
        for arn in [
            "arn:aws:s3:::t367\u{0}name".to_owned(),
            format!("{}/index/i\u{0}x", table("t367name")),
            format!("{}/index/ix", table("t367\u{0}name")),
            format!("{}/stream/2026-10-24T00:00:00.000", table("t367\u{0}name")),
            format!("{}/index/ix", table("t367name")),
            "arn:\u{0}".to_owned(),
            format!("{}/", table("t367\u{0}name")),
        ] {
            let m = message(arn_check("Query", &arn)).unwrap_or_else(|| panic!("{arn:?}"));
            assert_eq!(m, at_table_name(&arn, FRAME));
        }
        let m = message(check_transact_member_arn(
            &table("t367\u{0}name"),
            "transactItems.1.member.put.tableName",
        ))
        .unwrap();
        assert!(m.contains("at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Table name of ARN"), "{m}");
        assert!(
            message(arn_check(
                "DescribeTable",
                &format!("{}/index/ix", table("t367\u{0}name"))
            ))
            .is_some()
        );
    }

    #[test]
    fn table_arn_fields_stream_backup_and_tag_arns() {
        let bad = table("t367\u{0}name");
        assert_eq!(
            message(check_table_arn_field("RestoreTableToPointInTime", &bad)).unwrap(),
            "Invalid Request: Invalid TableArn: Invalid Table ARN"
        );
        assert_eq!(
            message(check_table_arn_field("ExportTableToPointInTime", &bad)).unwrap(),
            "Invalid TableArn: Invalid Table ARN"
        );
        assert!(check_table_arn_field("ExportTableToPointInTime", &table("ok")).is_ok());
        let stream = format!("{bad}/stream/2026-10-24T00:00:00.000");
        assert_eq!(
            message(check_stream_arn(&stream)).unwrap(),
            "Invalid StreamArn"
        );
        let label = format!("{}/stream/2026\u{0}", table("ok"));
        assert_eq!(
            message(check_stream_arn(&label)).unwrap(),
            "Invalid StreamArn"
        );
        assert!(check_stream_arn(&format!("{}/stream/2026", table("ok"))).is_ok());
        let backup = format!("{bad}/backup/01700000000000-abcdef12");
        assert_eq!(
            message(check_backup_arn(&backup)).unwrap(),
            "Invalid Backup ARN"
        );
        let backup_id = format!("{}/backup/0170\u{0}", table("ok"));
        assert!(check_backup_arn(&backup_id).is_ok(), "left to the engine");
        assert_eq!(
            message(check_tag_resource_arn(&bad)).unwrap(),
            format!("Invalid TableArn: Invalid ResourceArn provided as input {bad}")
        );
        assert_eq!(
            message(check_tag_resource_arn(&stream)).unwrap(),
            "Invalid StreamArn"
        );
        assert!(check_tag_resource_arn(&format!("{bad}/index/i")).is_ok());
    }
}
