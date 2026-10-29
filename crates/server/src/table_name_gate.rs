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
//! one (measured 2026-10-24). This module runs first in `authorize_request`.
//!
//! Sentences (measured 2026-09-18 and 2026-10-24; derived cases are marked):
//!
//! - A bare name outside `[a-zA-Z0-9_.-]+`: `Value '<name>' at '<location>'
//!   failed to satisfy constraint: Member must satisfy regular expression
//!   pattern: ...`. When the name is also shorter than 3 or longer than 255
//!   characters the message lists both clauses, the length first, joined by
//!   `; ` under `2 validation errors detected:` (measured at `tableName`;
//!   derived for the other locations). Locations: `tableName` for `TableName`,
//!   `targetTableName`, `sourceTableName`, `tableCreationParameters.tableName`,
//!   `exclusiveStartTableName`, `indexName` for `Query` and `Scan`, and
//!   `transactItems.<n>.member.<op>.tableName` (1-based, `put`, `get`,
//!   `delete`, `update`, `conditionCheck`). A name inside the pattern but of a
//!   bad length is left to the engine handlers, whose answer the service's
//!   matches already.
//! - `ListStreams` has its own validator: `TableName must be at least 3
//!   characters long and at most 255 characters long` for a U+0000 name
//!   (measured); applied to any character outside the pattern (derived).
//! - A `RequestItems` key outside the pattern: `at 'requestItems'` with the
//!   map-key constraint list. The service renders its own dump of the whole
//!   map inside `Value '...'` and names no key first; the first offending key
//!   in request order (`serde_json` keeps it) is rendered alone here. A key
//!   that is an ARN takes the ARN constraint list (`arn_gate::MAP_KEYS_ARN`).
//! - ARN-shaped values: see `arn_gate`.

use extenddb_core::error::DynamoDbError;
use serde_json::Value;

use crate::arn_gate::{self, PATTERN, in_pattern};

const MAP_KEYS_NAME: &str = "Map keys must satisfy constraint: \
    [Member must have length less than or equal to 255, \
    Member must have length greater than or equal to 3, \
    Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]";

/// Refuse a table name outside the service's character pattern, in any field
/// the operation carries one, before the name is used in any lookup. A
/// missing, non-string or empty name passes.
pub(crate) fn check_table_name_chars(operation: &str, input: &Value) -> Result<(), DynamoDbError> {
    match operation {
        "BatchGetItem" | "BatchWriteItem" => check_request_items_keys(input),
        "TransactGetItems" => check_transact_items(input, &["Get"]),
        "TransactWriteItems" => {
            check_transact_items(input, &["Put", "Delete", "Update", "ConditionCheck"])
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
            check_name(input.get("TargetTableName"), "targetTableName", operation)
        }
        "RestoreTableToPointInTime" => {
            check_name(input.get("SourceTableName"), "sourceTableName", operation)?;
            check_name(input.get("TargetTableName"), "targetTableName", operation)?;
            with_str(input, "SourceTableArn", |a| {
                arn_gate::check_table_arn_field(operation, a)
            })
        }
        "ExportTableToPointInTime" => with_str(input, "TableArn", |a| {
            arn_gate::check_table_arn_field(operation, a)
        }),
        "ImportTable" => check_name(
            input
                .get("TableCreationParameters")
                .and_then(|p| p.get("TableName")),
            "tableCreationParameters.tableName",
            operation,
        ),
        "ListTables" => check_name(
            input.get("ExclusiveStartTableName"),
            "exclusiveStartTableName",
            operation,
        ),
        "ListStreams" => match input.get("TableName").and_then(Value::as_str) {
            Some(name) if name.starts_with("arn:") => {
                arn_gate::check_table_name_arn(operation, name)
            }
            Some(name) if !name.is_empty() && !in_pattern(name) => {
                Err(DynamoDbError::ValidationException(
                    "TableName must be at least 3 characters long and at most 255 characters long"
                        .to_owned(),
                ))
            }
            _ => Ok(()),
        },
        "Query" | "Scan" => {
            check_name(input.get("TableName"), "tableName", operation)?;
            match input.get("IndexName").and_then(Value::as_str) {
                // Pattern only: the length clauses for an index name are unmeasured.
                Some(index) if !index.is_empty() && !in_pattern(index) => {
                    Err(one_error(index, "indexName", &pattern_clause()))
                }
                _ => Ok(()),
            }
        }
        _ => check_name(input.get("TableName"), "tableName", operation),
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
    DynamoDbError::ValidationException(format!(
        "1 validation error detected: Value '{value}' at '{location}' failed to satisfy \
         constraint: {constraint}"
    ))
}

/// A table name field: a bare name outside the pattern is refused with the
/// pattern clause (and the length clause when that fails too); an `arn:` value
/// goes through the ARN rules.
fn check_name(value: Option<&Value>, location: &str, operation: &str) -> Result<(), DynamoDbError> {
    let Some(name) = value.and_then(Value::as_str) else {
        return Ok(());
    };
    if name.starts_with("arn:") {
        return arn_gate::check_table_name_arn(operation, name);
    }
    if name.is_empty() || in_pattern(name) {
        return Ok(());
    }
    Err(bare_name_error(name, location))
}

/// The service lists every failed constraint of a member in one message, the
/// length clause before the pattern clause.
fn bare_name_error(name: &str, location: &str) -> DynamoDbError {
    let length = name.chars().count();
    let mut clauses: Vec<String> = Vec::with_capacity(2);
    if length < 3 {
        clauses.push("Member must have length greater than or equal to 3".to_owned());
    } else if length > 255 {
        clauses.push("Member must have length less than or equal to 255".to_owned());
    }
    clauses.push(pattern_clause());
    let n = clauses.len();
    DynamoDbError::ValidationException(format!(
        "{n} validation error{} detected: {}",
        if n == 1 { "" } else { "s" },
        clauses
            .iter()
            .map(|c| format!("Value '{name}' at '{location}' failed to satisfy constraint: {c}"))
            .collect::<Vec<_>>()
            .join("; ")
    ))
}

/// A `RequestItems` key outside the pattern, or an ARN key that is not a
/// well-formed plain table ARN. The sentence from `at 'requestItems'` onward
/// is the service's; the offending key alone is rendered where the service
/// renders the whole map.
fn check_request_items_keys(input: &Value) -> Result<(), DynamoDbError> {
    let Some(map) = input.get("RequestItems").and_then(Value::as_object) else {
        return Ok(());
    };
    for key in map.keys() {
        if key.is_empty() {
            continue;
        }
        if key.starts_with("arn:") {
            if arn_gate::table_arn_fault(key).is_some() {
                return Err(one_error(key, "requestItems", arn_gate::MAP_KEYS_ARN));
            }
        } else if !in_pattern(key) {
            return Err(one_error(key, "requestItems", MAP_KEYS_NAME));
        }
    }
    Ok(())
}

/// A `TransactItems` member whose sub-operation names a table outside the
/// pattern, or by an ARN that is not a well-formed plain table ARN.
fn check_transact_items(input: &Value, subs: &[&str]) -> Result<(), DynamoDbError> {
    let Some(items) = input.get("TransactItems").and_then(Value::as_array) else {
        return Ok(());
    };
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
            let location = format!(
                "transactItems.{}.member.{}.tableName",
                i + 1,
                lower_camel(sub)
            );
            if name.starts_with("arn:") {
                arn_gate::check_transact_member_arn(name, &location)?;
            } else if !in_pattern(name) {
                return Err(bare_name_error(name, &location));
            }
        }
    }
    Ok(())
}

fn lower_camel(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_ascii_lowercase().to_string() + c.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NUL: &str = "t367\u{0}name";
    const ARN: &str = "arn:aws:dynamodb:us-east-1:964157134968:table/t367\u{0}name";
    const PAT: &str = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+";

    fn message(operation: &str, input: Value) -> Option<String> {
        match check_table_name_chars(operation, &input) {
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
                { "Put": { "TableName": "ok", "Item": {} } },
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
}
