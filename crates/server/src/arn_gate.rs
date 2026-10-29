// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The ARN shapes `table_name_gate` refuses before any catalog lookup, with the
//! service's sentences (measured 2026-10-24 unless marked derived).
//!
//! A `TableName` may be a table ARN. The service checks the ARN's shape and
//! each of its segments before it resolves the table:
//!
//! | `TableName` | sentence at `'tableName'` |
//! |---|---|
//! | a table ARN whose table segment fails the pattern | `Table name of ARN must satisfy regular expression pattern: ...` |
//! | a dynamodb ARN with a bad region segment | `Invalid AWS region` |
//! | a dynamodb ARN with a bad account segment | `Invalid subscriber ID` |
//! | anything else that is not a plain table ARN (another service, an index or stream suffix, too few fields) | the `Valid ARN format is ...` frame |
//!
//! `DescribeTable` with an ARN of another service is `AccessDeniedException`
//! on the service; that answer is left to the engine, so the frame is not
//! applied to it there. The frame for a well-formed index or stream ARN in
//! `TableName` is derived from the measured index ARN with a bad segment.
//!
//! The other ARN-bearing fields: `SourceTableArn` (`RestoreTableToPointInTime`)
//! and `TableArn` (`ExportTableToPointInTime`) with a bad table segment,
//! `StreamArn` (`DescribeStream`, `GetShardIterator`) and a tag operation's
//! stream `ResourceArn` with a bad table segment or U+0000 in the label,
//! `BackupArn` (`DescribeBackup`, `DeleteBackup`, `RestoreTableFromBackup`)
//! with a bad table segment, and a tag operation's table `ResourceArn` with a
//! bad table segment. Each has its own sentence below.

use extenddb_core::arn::{SubResource, fields, parse};
use extenddb_core::error::DynamoDbError;

pub(crate) const PATTERN: &str = "[a-zA-Z0-9_.-]+";

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

/// What is wrong with an `arn:` value read as a table ARN.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ArnFault {
    /// A plain table ARN whose table segment fails the pattern.
    TableSegment,
    /// A dynamodb ARN whose region segment carries a character outside `[a-z0-9-]`.
    Region,
    /// A dynamodb ARN whose account segment carries a character that is not a digit.
    Account,
    /// Not a plain table ARN of this service: too few fields, another service,
    /// an index or stream or backup suffix, or a shape `arn::parse` rejects.
    NotPlainTable,
}

/// Inspect an `arn:` value the way the service checks a `TableName` ARN.
/// `None` means a well-formed plain table ARN whose segments all pass.
pub(crate) fn table_arn_fault(arn: &str) -> Option<ArnFault> {
    let Some(f) = fields(arn) else {
        return Some(ArnFault::NotPlainTable);
    };
    if !f
        .region
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Some(ArnFault::Region);
    }
    if !f.account.chars().all(|c| c.is_ascii_digit()) {
        return Some(ArnFault::Account);
    }
    if f.service != "dynamodb" {
        return Some(ArnFault::NotPlainTable);
    }
    match parse(arn) {
        Some(res) if res.sub.is_none() => {
            (!in_pattern(&res.table)).then_some(ArnFault::TableSegment)
        }
        _ => Some(ArnFault::NotPlainTable),
    }
}

fn arn_constraint(fault: ArnFault) -> &'static str {
    match fault {
        ArnFault::TableSegment => {
            "Table name of ARN must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
        }
        ArnFault::Region => "Invalid AWS region",
        ArnFault::Account => "Invalid subscriber ID",
        ArnFault::NotPlainTable => FRAME,
    }
}

/// A `TableName` that is an ARN, for `operation`. `Ok` passes the value on to
/// the handlers, which resolve a well-formed table ARN.
pub(crate) fn check_table_name_arn(operation: &str, arn: &str) -> Result<(), DynamoDbError> {
    let Some(fault) = table_arn_fault(arn) else {
        return Ok(());
    };
    // DescribeTable answers a foreign-service ARN with AccessDeniedException on
    // the service; the engine's own answer stands for that one shape.
    if operation == "DescribeTable"
        && fault == ArnFault::NotPlainTable
        && fields(arn).is_some_and(|f| f.service != "dynamodb")
    {
        return Ok(());
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
/// `GetShardIterator` and `TagResource`).
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

    const ACCT: &str = "964157134968";
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

    #[test]
    fn a_plain_table_arn_with_a_bad_segment_gets_the_table_name_of_arn_sentence() {
        let arn = table("t367\u{0}name");
        let m = message(check_table_name_arn("PutItem", &arn)).expect("refused");
        assert_eq!(
            m,
            format!(
                "1 validation error detected: Value '{arn}' at 'tableName' failed to satisfy \
                 constraint: Table name of ARN must satisfy regular expression pattern: \
                 [a-zA-Z0-9_.-]+"
            )
        );
        assert!(check_table_name_arn("PutItem", &table("t367name")).is_ok());
    }

    #[test]
    fn region_and_account_faults_name_their_segment() {
        let region = format!("arn:aws:dynamodb:us-ea\u{0}st-1:{ACCT}:table/t367name");
        assert!(
            message(check_table_name_arn("PutItem", &region))
                .unwrap()
                .ends_with("failed to satisfy constraint: Invalid AWS region")
        );
        let account = "arn:aws:dynamodb:us-east-1:1234\u{0}56789012:table/t367name";
        assert!(
            message(check_table_name_arn("PutItem", account))
                .unwrap()
                .ends_with("failed to satisfy constraint: Invalid subscriber ID")
        );
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
            let m =
                message(check_table_name_arn("Query", &arn)).unwrap_or_else(|| panic!("{arn:?}"));
            assert_eq!(
                m,
                format!(
                    "1 validation error detected: Value '{arn}' at 'tableName' failed to satisfy \
                     constraint: {FRAME}"
                )
            );
        }
        let m = message(check_transact_member_arn(
            &table("t367\u{0}name"),
            "transactItems.1.member.put.tableName",
        ))
        .unwrap();
        assert!(m.contains("at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Table name of ARN"), "{m}");
    }

    #[test]
    fn describe_table_leaves_a_foreign_service_arn_to_the_engine() {
        assert!(check_table_name_arn("DescribeTable", "arn:aws:s3:::t367\u{0}name").is_ok());
        assert!(
            check_table_name_arn(
                "DescribeTable",
                &format!("{}/index/ix", table("t367\u{0}name"))
            )
            .is_err()
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
