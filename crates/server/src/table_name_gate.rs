// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The table name checks that run before any catalog lookup.
//!
//! A request's table names reach storage before the engine validates them:
//! authorization looks up each table's key schema (for `dynamodb:LeadingKeys`)
//! and the resource tags for the table ARN it builds from the name. On
//! PostgreSQL a name containing U+0000 cannot be bound as a `TEXT` parameter
//! (`invalid byte sequence for encoding "UTF8": 0x00`), so those lookups failed
//! and the request was answered 500 `Internal error during authorization`.
//! The service answers 400 `ValidationException`, and it does so before the
//! IAM decision: a principal with no DynamoDB permission gets the same
//! `ValidationException` for a bad name and `AccessDeniedException` for a real
//! one (measured 2026-10-24 with `DescribeTable`, `PutItem`, `Scan` and
//! `BatchWriteItem`). This module runs first in `authorize_request`.
//!
//! Sentences (measured 2026-10-24 unless marked otherwise):
//!
//! - A bare name outside `[a-zA-Z0-9_.-]+`: `Value '<name>' at '<location>'
//!   failed to satisfy constraint: Member must satisfy regular expression
//!   pattern: ...`. When the name is also shorter than 3 or longer than 255
//!   characters, or UTF-16 code units in the operations of `UTF16_LENGTH`,
//!   the message lists both clauses, the length first, joined by
//!   `; ` under `2 validation errors detected:` (measured at `tableName`;
//!   derived for the other locations). Locations: `tableName` for `TableName`,
//!   `targetTableName`, `sourceTableName`, `tableCreationParameters.tableName`,
//!   `exclusiveStartTableName`, `indexName` for `Query`, `Scan` and `SearchVectors`, and
//!   `transactItems.<n>.member.<op>.tableName` (1-based, `put`, `get`,
//!   `delete`, `update`, `conditionCheck`). A name inside the pattern but of a
//!   bad length is left to the engine handlers, for the reason given in
//!   `extenddb_core::validation::table_name_length`. Beside a name outside the
//!   pattern, `TransactWriteItems` lists the other actions' names of a bad
//!   length here (measured 2026-11-13 and 2026-11-14). An empty
//!   `DescribeTable` name fails both clauses (measured 2026-11-18).
//! - `ListStreams` has its own validator: `TableName must be at least 3
//!   characters long and at most 255 characters long`, measured for a U+0000
//!   name, and on 2026-11-04 for a table ARN and for `abc:def`; applied to
//!   any other character outside the pattern (derived). The gate also gives
//!   it for a name of 1, 2 or more than 255 characters, which the engine does
//!   not check (measured 2026-11-07 with 1, 2 and 256; 3 and 255 pass). Its
//!   `ExclusiveStartStreamArn` takes the stream ARN rules of `arn_gate`
//!   (measured 2026-11-06), after `TableName`: with both bad the service
//!   answers the `TableName` sentence in either member order (measured
//!   2026-11-07). U+0000 in the start ARN's account, region or
//!   partition is accepted by the service and by storage, which binds only
//!   the table segment and the label.
//! - A `RequestItems` key outside the pattern: `at 'requestItems'` with the
//!   map-key constraint list. The service renders its own dump of the whole
//!   map inside `Value '...'`; the first offending key the parsed map yields
//!   is rendered alone here. That is request order only when `serde_json`'s
//!   `preserve_order` feature is in the build (the MongoDB driver turns it
//!   on), and key order otherwise. A key that is an ARN takes the ARN
//!   constraint list (`arn_gate::MAP_KEYS_ARN`).
//! - ARN-shaped values: see `arn_gate`.
//!
//! The service reads `SourceTableName`, `TargetTableName` (both restores),
//! `TableCreationParameters.TableName` (`ImportTable`),
//! `ExclusiveStartTableName` and the `ListStreams` `TableName` as names, never
//! as ARNs; the service model gives each the `TableName` shape. For an ARN in
//! `ImportTable` it answers with the authorization of
//! `table/<value as written>` (measured 2026-11-03), and for an ARN in
//! `SourceTableName` with the pattern sentence at `sourceTableName` for a
//! caller allowed every action (measured 2026-11-04). So the ARN rules do not
//! apply in these members: a value that starts with `arn:` and carries no
//! U+0000 is left to those later checks, and one that carries U+0000 gets the
//! member's own sentence (derived), as a bare name outside the pattern does.
//! `ListStreams` refuses every value outside the pattern, as measured.

use extenddb_core::error::{DynamoDbError, validation_error, validation_errors};
use extenddb_core::validation::table_name_length::{
    MAP_KEYS_NAME, count_clause, length_clause, transact_name_location,
};
use serde_json::Value;

use crate::arn_gate::{self, PATTERN, in_pattern};

const LIST_STREAMS_NAME: &str =
    "TableName must be at least 3 characters long and at most 255 characters long";

/// The service's table name bounds. The gate lists a length clause only
/// beside a pattern fault, and that request is refused whatever the
/// deployment's bounds.
const NAME_BOUNDS: (usize, usize) = (3, 255);

/// The operations that count a table name in UTF-16 code units when they
/// choose its length clause, so a character above U+FFFF counts twice
/// (measured 2026-11-14 and 2026-11-18). The item operations count
/// characters, as the gate does for every operation not listed here.
const UTF16_LENGTH: [&str; 4] = ["CreateTable", "DeleteTable", "DescribeTable", "UpdateTable"];

/// Refuse a table name outside the service's character pattern, in any field
/// the operation carries one, before the name is used in any lookup. A
/// missing, non-string or empty name passes, except an empty `DescribeTable`
/// name. `account_id` is the caller's account, which the control-plane ARN
/// rules compare with the ARN's.
pub(crate) fn check_table_name_chars(
    operation: &str,
    input: &Value,
    account_id: &str,
) -> Result<(), DynamoDbError> {
    match operation {
        "BatchGetItem" | "BatchWriteItem" => check_request_items_keys(operation, input),
        "TransactGetItems" => check_transact_items(input, &["Get"], false),
        "TransactWriteItems" => {
            check_transact_items(input, &["Put", "Delete", "Update", "ConditionCheck"], true)
        }
        "TagResource" | "UntagResource" | "ListTagsOfResource" => {
            with_str(input, "ResourceArn", arn_gate::check_tag_resource_arn)
        }
        "DescribeStream" | "GetShardIterator" => {
            with_str(input, "StreamArn", arn_gate::check_stream_arn)
        }
        "DescribeBackup" | "DeleteBackup" => {
            with_str(input, "BackupArn", arn_gate::check_backup_arn)
        }
        "RestoreTableFromBackup" => {
            with_str(input, "BackupArn", arn_gate::check_backup_arn)?;
            check_name_member(input.get("TargetTableName"), "targetTableName")
        }
        // The server refuses every RestoreTableToPointInTime before this
        // code runs.
        "RestoreTableToPointInTime" => {
            check_name_member(input.get("SourceTableName"), "sourceTableName")?;
            check_name_member(input.get("TargetTableName"), "targetTableName")?;
            with_str(input, "SourceTableArn", |a| {
                arn_gate::check_table_arn_field(operation, a)
            })
        }
        "ExportTableToPointInTime" => with_str(input, "TableArn", |a| {
            arn_gate::check_table_arn_field(operation, a)
        }),
        "ImportTable" => check_name_member(
            input
                .get("TableCreationParameters")
                .and_then(|p| p.get("TableName")),
            "tableCreationParameters.tableName",
        ),
        "ListTables" => check_name_member(
            input.get("ExclusiveStartTableName"),
            "exclusiveStartTableName",
        ),
        "ListStreams" => {
            match input.get("TableName").and_then(Value::as_str) {
                Some(name)
                    if !name.is_empty()
                        && (!in_pattern(name) || !(3..=255).contains(&name.len())) =>
                {
                    return Err(DynamoDbError::ValidationException(
                        LIST_STREAMS_NAME.to_owned(),
                    ));
                }
                _ => {}
            }
            // Storage binds the start ARN's table segment and label; the
            // service refuses U+0000 in either (measured 2026-11-06).
            with_str(input, "ExclusiveStartStreamArn", arn_gate::check_stream_arn)
        }
        "Query" | "Scan" | "SearchVectors" => {
            check_name(input.get("TableName"), "tableName", operation, account_id)?;
            match input.get("IndexName").and_then(Value::as_str) {
                // Pattern only: the length clauses for an index name are unmeasured.
                Some(index) if !index.is_empty() && !in_pattern(index) => {
                    Err(one_error(index, "indexName", &pattern_clause()))
                }
                _ => Ok(()),
            }
        }
        _ => check_name(input.get("TableName"), "tableName", operation, account_id),
    }
}

fn with_str(
    input: &Value,
    field: &str,
    check: impl Fn(&str) -> Result<(), DynamoDbError>,
) -> Result<(), DynamoDbError> {
    match input.get(field).and_then(Value::as_str) {
        Some(s) if !s.is_empty() => check(s),
        _ => Ok(()),
    }
}

fn pattern_clause() -> String {
    format!("Member must satisfy regular expression pattern: {PATTERN}")
}

fn one_error(value: &str, location: &str, constraint: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(validation_error(value, location, constraint))
}

/// A table name field: a bare name outside the pattern is refused with the
/// pattern clause (and the length clause when that fails too); an `arn:` value
/// goes through the ARN rules.
fn check_name(
    value: Option<&Value>,
    location: &str,
    operation: &str,
    account_id: &str,
) -> Result<(), DynamoDbError> {
    let Some(name) = value.and_then(Value::as_str) else {
        return Ok(());
    };
    if name.starts_with("arn:") {
        return arn_gate::check_table_name_arn(operation, name, account_id);
    }
    // The service reads an empty DescribeTable name as one that fails the
    // length and the pattern (measured 2026-11-18).
    if in_pattern(name) && !(name.is_empty() && operation == "DescribeTable") {
        return Ok(());
    }
    let utf16 = UTF16_LENGTH.contains(&operation);
    Err(bare_names_error(&[(name, location.to_owned())], utf16))
}

/// A member the service reads as a name (see the module docs): a value
/// outside the pattern is refused as a bare name is, unless it starts with
/// `arn:` and carries no U+0000, which is passed on.
fn check_name_member(value: Option<&Value>, location: &str) -> Result<(), DynamoDbError> {
    let Some(name) = value.and_then(Value::as_str) else {
        return Ok(());
    };
    if name.is_empty() || in_pattern(name) {
        return Ok(());
    }
    if name.starts_with("arn:") && !name.contains('\u{0}') {
        return Ok(());
    }
    Err(bare_name_error(name, location))
}

/// The service lists every failed constraint of a member in one message, the
/// length clause before the pattern clause.
fn bare_name_error(name: &str, location: &str) -> DynamoDbError {
    bare_names_error(&[(name, location.to_owned())], false)
}

/// [`bare_name_error`] for several members, in the order given. A name inside
/// the pattern contributes its length clause alone.
fn bare_names_error(names: &[(&str, String)], utf16: bool) -> DynamoDbError {
    let pattern = pattern_clause();
    let mut faults: Vec<(&str, &str, &str)> = Vec::new();
    for (name, location) in names {
        let length = if utf16 {
            count_clause(name.encode_utf16().count(), NAME_BOUNDS)
        } else {
            length_clause(name, NAME_BOUNDS)
        };
        if let Some(clause) = length {
            faults.push((name, location, clause));
        }
        if name.is_empty() || !in_pattern(name) {
            faults.push((name, location, &pattern));
        }
    }
    DynamoDbError::ValidationException(validation_errors(&faults))
}

/// A `RequestItems` key outside the pattern, or an ARN key that the ARN rules
/// refuse. The sentence from `at 'requestItems'` onward is the service's; the
/// offending key alone is rendered where the service renders the whole map.
/// The engine handlers give a key of a bad length the same sentence, after
/// the request is read.
///
/// `BatchWriteItem` refuses a key for its account segment only when the
/// segment carries U+0000; any other bad account segment is passed on to the
/// cross-account answer for an account that does not exist, `Access is
/// denied`. Measured 2026-11-06: a letter and 13 digits got `Access is denied`
/// on every request, and U+0000 and 11 digits got either that or the map-key
/// constraint from one request to the next. `BatchGetItem` refused all four
/// here on every request.
fn check_request_items_keys(operation: &str, input: &Value) -> Result<(), DynamoDbError> {
    let Some(map) = input.get("RequestItems").and_then(Value::as_object) else {
        return Ok(());
    };
    for key in map.keys() {
        if key.is_empty() {
            continue;
        }
        if key.starts_with("arn:") {
            let refused = match arn_gate::table_arn_fault(key) {
                None => false,
                Some(arn_gate::ArnFault::Account) if operation == "BatchWriteItem" => {
                    extenddb_core::arn::fields(key).is_some_and(|f| f.account.contains('\u{0}'))
                }
                Some(_) => true,
            };
            if refused {
                return Err(one_error(key, "requestItems", arn_gate::MAP_KEYS_ARN));
            }
        } else if !in_pattern(key) {
            return Err(one_error(key, "requestItems", MAP_KEYS_NAME));
        }
    }
    Ok(())
}

/// A `TransactItems` member whose sub-operation names a table outside the
/// pattern, or by an ARN that the ARN rules refuse. With `list_lengths`
/// (`TransactWriteItems`) the message lists every action's name outside the
/// pattern and every name of a bad length, as the service does (measured
/// 2026-11-13 and 2026-11-14), up to the first ARN that the ARN rules refuse.
/// The list ends at that ARN. The service also lists the ARN's fault and the
/// later actions' faults (measured 2026-11-25), and this check leaves them
/// out. Names of a bad length alone are left to the engine, which checks them
/// after the request is read.
fn check_transact_items(
    input: &Value,
    subs: &[&str],
    list_lengths: bool,
) -> Result<(), DynamoDbError> {
    let Some(items) = input.get("TransactItems").and_then(Value::as_array) else {
        return Ok(());
    };
    let mut names: Vec<(&str, String)> = Vec::new();
    let mut outside = false;
    for (i, item) in items.iter().enumerate() {
        for sub in subs {
            let Some(name) = item
                .get(sub)
                .and_then(|o| o.get("TableName"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            if name.is_empty() {
                continue;
            }
            let location = transact_name_location(i, sub);
            if name.starts_with("arn:") {
                if let Err(e) = arn_gate::check_transact_member_arn(name, &location) {
                    return Err(if outside {
                        bare_names_error(&names, false)
                    } else {
                        e
                    });
                }
            } else if !in_pattern(name) {
                if !list_lengths {
                    return Err(bare_name_error(name, &location));
                }
                outside = true;
                names.push((name, location));
            } else if list_lengths && length_clause(name, NAME_BOUNDS).is_some() {
                names.push((name, location));
            }
        }
    }
    if outside {
        return Err(bare_names_error(&names, false));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NUL: &str = "t367\u{0}name";
    const ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/t367\u{0}name";
    const PAT: &str = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+";

    fn message(operation: &str, input: Value) -> Option<String> {
        match check_table_name_chars(operation, &input, "123456789012") {
            Ok(()) => None,
            Err(DynamoDbError::ValidationException(m)) => Some(m),
            Err(other) => panic!("unexpected error {other:?}"),
        }
    }

    fn one(value: &str, location: &str, constraint: &str) -> String {
        format!(
            "1 validation error detected: Value '{value}' at '{location}' failed to satisfy \
             constraint: {constraint}"
        )
    }

    #[test]
    fn nul_in_table_name_is_refused_with_the_pattern_message() {
        let m = message("DescribeTable", json!({ "TableName": NUL })).expect("refused");
        assert_eq!(m, one(NUL, "tableName", PAT));
    }

    #[test]
    fn other_characters_outside_the_pattern_are_refused_and_plain_names_pass() {
        for name in [
            "a b",
            "tab\tle",
            "t/name",
            "t:name",
            "caf\u{e9}",
            "\u{0}t367name",
        ] {
            assert!(
                message("PutItem", json!({ "TableName": name })).is_some(),
                "{name:?}"
            );
        }
        for name in ["abc", "Table_1.a-b", &"x".repeat(300), "ab"] {
            assert!(
                message("Query", json!({ "TableName": name })).is_none(),
                "{name:?}"
            );
        }
        for input in [
            json!({}),
            json!({ "TableName": "" }),
            json!({ "TableName": null }),
            json!({ "TableName": 7 }),
            json!({ "TableName": "arn:aws:dynamodb:us-east-1:123456789012:table/t" }),
        ] {
            assert!(message("GetItem", input.clone()).is_none(), "{input}");
        }
    }

    #[test]
    fn a_name_failing_two_constraints_lists_the_length_clause_first() {
        let m = message("DescribeTable", json!({ "TableName": "\u{0}" })).expect("refused");
        assert_eq!(
            m,
            format!(
                "2 validation errors detected: Value '\u{0}' at 'tableName' failed to satisfy \
                 constraint: Member must have length greater than or equal to 3; \
                 Value '\u{0}' at 'tableName' failed to satisfy constraint: {PAT}"
            )
        );
        let long = format!("{}\u{0}", "x".repeat(300));
        let m = message("CreateTable", json!({ "TableName": long })).expect("refused");
        assert!(m.starts_with("2 validation errors detected: "), "{m}");
        assert!(
            m.contains("Member must have length less than or equal to 255; "),
            "{m}"
        );
    }

    #[test]
    fn the_other_name_fields_report_their_own_location() {
        let cases = [
            (
                "RestoreTableFromBackup",
                json!({ "TargetTableName": NUL }),
                "targetTableName",
            ),
            (
                "RestoreTableToPointInTime",
                json!({ "SourceTableName": NUL }),
                "sourceTableName",
            ),
            (
                "RestoreTableToPointInTime",
                json!({ "TargetTableName": NUL }),
                "targetTableName",
            ),
            (
                "ImportTable",
                json!({ "TableCreationParameters": { "TableName": NUL } }),
                "tableCreationParameters.tableName",
            ),
            (
                "ListTables",
                json!({ "ExclusiveStartTableName": NUL }),
                "exclusiveStartTableName",
            ),
            ("ListBackups", json!({ "TableName": NUL }), "tableName"),
            ("UpdateTimeToLive", json!({ "TableName": NUL }), "tableName"),
        ];
        for (op, input, location) in cases {
            assert_eq!(
                message(op, input).as_deref(),
                Some(one(NUL, location, PAT).as_str()),
                "{op}"
            );
        }
        assert_eq!(
            message(
                "Query",
                json!({ "TableName": "ok", "IndexName": "g\u{0}2" })
            )
            .unwrap(),
            one("g\u{0}2", "indexName", PAT)
        );
        assert!(message("Scan", json!({ "TableName": "ok", "IndexName": "gsi1" })).is_none());
        assert_eq!(
            message("ListStreams", json!({ "TableName": NUL })).unwrap(),
            "TableName must be at least 3 characters long and at most 255 characters long"
        );
    }

    #[test]
    fn search_vectors_index_name_outside_the_pattern_is_refused_as_query_is() {
        for bad in ["vidx/extra", "g\u{0}2", "a b", "v%2fx"] {
            let input = json!({ "TableName": "ok", "IndexName": bad });
            let expected = one(bad, "indexName", PAT);
            assert_eq!(message("SearchVectors", input.clone()).unwrap(), expected);
            assert_eq!(message("Query", input).unwrap(), expected);
        }
        for input in [
            json!({ "TableName": "ok", "IndexName": "vidx" }),
            json!({ "TableName": "ok", "IndexName": "" }),
            json!({ "TableName": "ok" }),
        ] {
            assert!(message("SearchVectors", input.clone()).is_none(), "{input}");
        }
        // The table name is checked as for any operation.
        assert_eq!(
            message(
                "SearchVectors",
                json!({ "TableName": NUL, "IndexName": "vidx" })
            )
            .unwrap(),
            one(NUL, "tableName", PAT)
        );
    }

    #[test]
    fn members_read_as_names_pass_an_arn_on_unless_it_carries_nul() {
        let good = "arn:aws:dynamodb:us-east-1:123456789012:table/t367name";
        let index = format!("{good}/index/ix");
        let cases = [
            (
                "RestoreTableFromBackup",
                "TargetTableName",
                "targetTableName",
            ),
            (
                "RestoreTableToPointInTime",
                "SourceTableName",
                "sourceTableName",
            ),
            (
                "RestoreTableToPointInTime",
                "TargetTableName",
                "targetTableName",
            ),
            (
                "ListTables",
                "ExclusiveStartTableName",
                "exclusiveStartTableName",
            ),
        ];
        for (op, member, location) in cases {
            for passed in [good, index.as_str(), "arn:aws:s3:::bucket"] {
                assert!(
                    message(op, json!({ member: passed })).is_none(),
                    "{op} {passed}"
                );
            }
            assert_eq!(
                message(op, json!({ member: ARN })).as_deref(),
                Some(one(ARN, location, PAT).as_str()),
                "{op}"
            );
        }
        let import = |name: &str| json!({ "TableCreationParameters": { "TableName": name } });
        assert!(message("ImportTable", import(good)).is_none());
        assert_eq!(
            message("ImportTable", import(ARN)).unwrap(),
            one(ARN, "tableCreationParameters.tableName", PAT)
        );
    }

    #[test]
    fn list_streams_refuses_an_arn_with_its_own_sentence() {
        for name in [
            "arn:aws:dynamodb:us-east-1:123456789012:table/t367name",
            ARN,
        ] {
            assert_eq!(
                message("ListStreams", json!({ "TableName": name })).unwrap(),
                LIST_STREAMS_NAME,
                "{name}"
            );
        }
        assert!(message("ListStreams", json!({ "TableName": "t367name" })).is_none());
        assert!(message("ListStreams", json!({})).is_none());
    }

    /// Measured 2026-11-07: 1, 2 and 256 characters get the sentence, alone
    /// and beside a bad `ExclusiveStartStreamArn`, in either member order; 3
    /// and 255 characters pass.
    #[test]
    fn list_streams_refuses_a_name_of_a_bad_length_before_the_start_arn() {
        let start = "arn:aws:dynamodb:us-east-1:123456789012:table/t367name/stream/2026\u{0}";
        for name in [
            "a".to_owned(),
            "ab".to_owned(),
            "a".repeat(256),
            NUL.to_owned(),
        ] {
            for input in [
                json!({ "TableName": name }),
                json!({ "TableName": name, "ExclusiveStartStreamArn": start }),
                json!({ "ExclusiveStartStreamArn": start, "TableName": name }),
            ] {
                assert_eq!(
                    message("ListStreams", input).as_deref(),
                    Some(LIST_STREAMS_NAME),
                    "{name:?}"
                );
            }
        }
        for name in ["abc".to_owned(), "a".repeat(255)] {
            assert!(message("ListStreams", json!({ "TableName": name })).is_none());
            assert_eq!(
                message(
                    "ListStreams",
                    json!({ "TableName": name, "ExclusiveStartStreamArn": start })
                )
                .as_deref(),
                Some("Invalid StreamArn")
            );
        }
    }

    #[test]
    fn arn_bearing_fields_are_checked_before_authorization() {
        let m = message("DescribeTable", json!({ "TableName": ARN })).expect("refused");
        assert!(m.contains("Table name of ARN must satisfy"), "{m}");
        assert_eq!(
            message(
                "RestoreTableToPointInTime",
                json!({ "SourceTableArn": ARN })
            )
            .unwrap(),
            "Invalid Request: Invalid TableArn: Invalid Table ARN"
        );
        assert_eq!(
            message("ExportTableToPointInTime", json!({ "TableArn": ARN })).unwrap(),
            "Invalid TableArn: Invalid Table ARN"
        );
        let stream = format!("{ARN}/stream/2026-10-24T00:00:00.000");
        for op in ["DescribeStream", "GetShardIterator"] {
            assert_eq!(
                message(op, json!({ "StreamArn": stream })).unwrap(),
                "Invalid StreamArn"
            );
        }
        let backup = format!("{ARN}/backup/01700000000000-abcdef12");
        for op in ["DescribeBackup", "DeleteBackup", "RestoreTableFromBackup"] {
            assert_eq!(
                message(op, json!({ "BackupArn": backup })).unwrap(),
                "Invalid Backup ARN"
            );
        }
        for op in ["TagResource", "UntagResource", "ListTagsOfResource"] {
            assert_eq!(
                message(op, json!({ "ResourceArn": ARN })).unwrap(),
                format!("Invalid TableArn: Invalid ResourceArn provided as input {ARN}")
            );
            assert_eq!(
                message(op, json!({ "ResourceArn": stream })).unwrap(),
                "Invalid StreamArn"
            );
            assert!(message(op, json!({ "ResourceArn": format!("{ARN}/index/i") })).is_none());
            assert!(message(op, json!({})).is_none());
        }
    }

    #[test]
    fn list_streams_checks_the_exclusive_start_stream_arn() {
        let own = "arn:aws:dynamodb:us-east-1:123456789012:table/t367name";
        for start in [
            format!("{own}/stream/2026\u{0}"),
            format!("{ARN}/stream/2026-11-06T00:00:00.000"),
        ] {
            for input in [
                json!({ "ExclusiveStartStreamArn": start }),
                json!({ "TableName": "t367name", "ExclusiveStartStreamArn": start }),
            ] {
                assert_eq!(
                    message("ListStreams", input).as_deref(),
                    Some("Invalid StreamArn"),
                    "{start:?}"
                );
            }
        }
        for start in [
            "arn:aws:dynamodb:us-east-1:1234\u{0}:table/t367name/stream/2026-11-06T00:00:00.000",
            "arn:aws:dynamodb:us-ea\u{0}st-1:123456789012:table/t367name/stream/2026",
            "arn:aws:dynamodb:us-east-1:123456789012:table/t367name/stream/2026",
        ] {
            assert!(
                message("ListStreams", json!({ "ExclusiveStartStreamArn": start })).is_none(),
                "{start:?}"
            );
        }
    }

    #[test]
    fn batch_write_passes_an_account_segment_without_nul_on() {
        let key = |account: &str| format!("arn:aws:dynamodb:us-east-1:{account}:table/t367name");
        for account in ["12345678901x", "12345678901", "1234567890123"] {
            let k = key(account);
            assert!(
                message(
                    "BatchWriteItem",
                    json!({ "RequestItems": { k.as_str(): [] } })
                )
                .is_none(),
                "{k}"
            );
            assert_eq!(
                message(
                    "BatchGetItem",
                    json!({ "RequestItems": { k.as_str(): {} } })
                )
                .unwrap(),
                one(&k, "requestItems", arn_gate::MAP_KEYS_ARN)
            );
        }
        let k = key("1234\u{0}");
        for op in ["BatchWriteItem", "BatchGetItem"] {
            assert_eq!(
                message(op, json!({ "RequestItems": { k.as_str(): [] } })).unwrap(),
                one(&k, "requestItems", arn_gate::MAP_KEYS_ARN),
                "{op}"
            );
        }
    }

    #[test]
    fn batch_request_items_keys_are_refused_at_request_items() {
        for op in ["BatchWriteItem", "BatchGetItem"] {
            let m = message(op, json!({ "RequestItems": { "ok": {}, NUL: {} } })).expect("refused");
            assert_eq!(m, one(NUL, "requestItems", MAP_KEYS_NAME));
            let m = message(op, json!({ "RequestItems": { ARN: {} } })).expect("refused");
            assert_eq!(m, one(ARN, "requestItems", arn_gate::MAP_KEYS_ARN));
            assert!(message(op, json!({ "RequestItems": { "ok": {} } })).is_none());
            assert!(message(op, json!({ "RequestItems": [] })).is_none());
        }
    }

    #[test]
    fn transact_members_are_refused_with_their_member_index_and_operation() {
        let m = message(
            "TransactWriteItems",
            json!({ "TransactItems": [
                { "Put": { "TableName": "okay", "Item": {} } },
                { "Delete": { "TableName": NUL, "Key": {} } },
            ] }),
        )
        .expect("refused");
        assert_eq!(m, one(NUL, "transactItems.2.member.delete.tableName", PAT));
        for (sub, member) in [("Update", "update"), ("ConditionCheck", "conditionCheck")] {
            let m = message(
                "TransactWriteItems",
                json!({ "TransactItems": [ { sub: { "TableName": NUL } } ] }),
            )
            .expect("refused");
            assert_eq!(
                m,
                one(
                    NUL,
                    &format!("transactItems.1.member.{member}.tableName"),
                    PAT
                )
            );
        }
        let m = message(
            "TransactGetItems",
            json!({ "TransactItems": [ { "Get": { "TableName": NUL, "Key": {} } } ] }),
        )
        .expect("refused");
        assert_eq!(m, one(NUL, "transactItems.1.member.get.tableName", PAT));
        let m = message(
            "TransactWriteItems",
            json!({ "TransactItems": [ { "Put": { "TableName": ARN, "Item": {} } } ] }),
        )
        .expect("refused");
        assert_eq!(
            m,
            one(
                ARN,
                "transactItems.1.member.put.tableName",
                "Table name of ARN must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
            )
        );
        assert!(
            message(
                "TransactWriteItems",
                json!({ "TransactItems": [ { "Delete": { "TableName": "ok" } } ] })
            )
            .is_none()
        );
    }

    /// Issue #1056 rows the gate answers (measured 2026-11-13, 2026-11-14 and
    /// 2026-11-18). A name of a bad length alone passes on to the engine, and
    /// every operation but `DescribeTable` passes an empty name on.
    #[test]
    fn bad_lengths_beside_a_pattern_fault_and_the_empty_describe_table_name() {
        let len = "Member must have length greater than or equal to 3";
        let at = |v: &str, l: &str, c: &str| {
            format!("Value '{v}' at '{l}' failed to satisfy constraint: {c}")
        };
        let put = |t: &str| json!({ "Put": { "TableName": t, "Item": {} } });
        let del = |t: &str| json!({ "Delete": { "TableName": t, "Key": {} } });
        let n256 = "n".repeat(256);
        let rows = [
            (
                "DescribeTable",
                json!({ "TableName": "" }),
                Some(format!(
                    "2 validation errors detected: {}; {}",
                    at("", "tableName", len),
                    at("", "tableName", PAT)
                )),
            ),
            (
                "TransactWriteItems",
                json!({ "TransactItems": [put("ab"), del("bad name")] }),
                Some(format!(
                    "2 validation errors detected: {}; {}",
                    at("ab", "transactItems.1.member.put.tableName", len),
                    at("bad name", "transactItems.2.member.delete.tableName", PAT)
                )),
            ),
            (
                "TransactWriteItems",
                json!({ "TransactItems": [put("ab"), del(&n256)] }),
                None,
            ),
            ("GetItem", json!({ "TableName": "ab" }), None),
            (
                "BatchGetItem",
                json!({ "RequestItems": { "ab": {} } }),
                None,
            ),
            (
                "BatchWriteItem",
                json!({ "RequestItems": { "ab": [] } }),
                None,
            ),
            ("CreateTable", json!({ "TableName": "", "Tags": "x" }), None),
            (
                "CreateTable",
                json!({ "TableName": n256, "Tags": "x" }),
                None,
            ),
        ];
        for (op, input, expected) in rows {
            assert_eq!(message(op, input.clone()), expected, "{op} {input}");
        }
        for op in [
            "CreateTable",
            "UpdateTable",
            "DeleteTable",
            "PutItem",
            "GetItem",
            "Query",
            "DescribeTimeToLive",
        ] {
            assert_eq!(message(op, json!({ "TableName": "" })), None, "{op}");
        }
    }

    /// Issue #1019 (measured 2026-11-14 and 2026-11-18), with Query and Scan as
    /// operations that keep the character count.
    #[test]
    fn four_operations_count_a_name_in_utf16_code_units() {
        let long = Some("Member must have length less than or equal to 255");
        let utf16 = ["CreateTable", "DescribeTable", "DeleteTable", "UpdateTable"];
        let (emoji, e_acute) = ("\u{1F600}", "\u{e9}");
        // (operations, name, the clause listed before the pattern clause)
        let rows = [
            (&utf16[..], emoji.repeat(2), None),
            (&utf16[..], emoji.repeat(128), long),
            (&utf16[..], e_acute.repeat(200), None),
            (&["Query", "Scan"][..], emoji.repeat(128), None),
        ];
        for (ops, name, length) in rows {
            let expected = match length {
                Some(clause) => format!(
                    "2 validation errors detected: Value '{name}' at 'tableName' failed to \
                     satisfy constraint: {clause}; Value '{name}' at 'tableName' failed to \
                     satisfy constraint: {PAT}"
                ),
                None => one(&name, "tableName", PAT),
            };
            for op in ops {
                assert_eq!(
                    message(op, json!({ "TableName": name })).as_deref(),
                    Some(expected.as_str()),
                    "{op} {} characters",
                    name.chars().count()
                );
            }
        }
    }
}
