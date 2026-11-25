// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Error message templates captured from real `DynamoDB`.
//! These are the exact messages `DynamoDB` returns — tenet 4 (errors are contracts).

use crate::validation::table_name_length::{TOO_LONG, TOO_SHORT};

/// Format a single validation constraint error in `DynamoDB`'s exact format.
#[must_use]
pub fn validation_error(value: &str, field: &str, constraint: &str) -> String {
    format!(
        "1 validation error detected: Value '{value}' at '{field}' failed to satisfy constraint: {constraint}"
    )
}

/// Format multiple validation constraint errors in `DynamoDB`'s exact format.
#[must_use]
pub fn validation_errors(errors: &[(&str, &str, &str)]) -> String {
    let details: Vec<String> = errors
        .iter()
        .map(|(value, field, constraint)| {
            format!("Value '{value}' at '{field}' failed to satisfy constraint: {constraint}")
        })
        .collect();
    validation_errors_detected(&details)
}

/// The service's envelope for one or more constraint clauses: `N validation
/// error(s) detected: ` and the clauses joined by `; `.
#[must_use]
pub fn validation_errors_detected(clauses: &[String]) -> String {
    let count = clauses.len();
    format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        clauses.join("; ")
    )
}

/// The service's answer to a `StreamArn` that is not a stream ARN of the
/// right shape (`DescribeStream` and `GetShardIterator`, measured 2026-10-23,
/// us-east-1), and to a `GetShardIterator` whose `StreamArn` is absent, null
/// or empty (measured 2026-11-23). The server checks the shape before
/// authorization; the `GetShardIterator` handler checks the presence.
pub const INVALID_STREAM_ARN: &str = "Invalid StreamArn";

/// Keys for error message templates. Compile-time checked — no stringly-typed lookups.
#[derive(Debug, Clone, Copy)]
pub enum ErrorMessageKey {
    TableNameTooShort,
    TableNameTooLong,
    TableNameNull,
    TableNameEmpty,
    /// An empty name in `GetItem` and `PutItem` (measured 2026-11-18).
    ItemTableNameEmpty,
    TableNameInvalidChars,
    TableNotFound,
    TableAlreadyExists,
    TableInUse,
    MissingKeySchema,
    KeySchemaTooMany,
    KeySchemaFirstNotHash,
    AttrDefNotInKey,
}

/// Build an error message from a key and arguments.
///
/// # Panics
/// Panics if a variant that requires arguments is called with an empty `args` slice.
/// This is a programming error — all call sites must provide the required arguments.
#[must_use]
pub fn error_message(key: ErrorMessageKey, args: &[&str]) -> String {
    match key {
        ErrorMessageKey::TableNameTooShort => validation_error(args[0], "tableName", TOO_SHORT),
        ErrorMessageKey::TableNameTooLong => validation_error(args[0], "tableName", TOO_LONG),
        ErrorMessageKey::TableNameNull => {
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null".to_owned()
        }
        ErrorMessageKey::TableNameEmpty => {
            "1 validation error detected: Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned()
        }
        ErrorMessageKey::ItemTableNameEmpty => {
            "1 validation error detected: Value at 'TableName' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned()
        }
        ErrorMessageKey::TableNameInvalidChars => {
            format!(
                "1 validation error detected: Value '{}' at 'tableName' failed to satisfy constraint: \
                 Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
                args[0]
            )
        }
        ErrorMessageKey::TableNotFound => {
            "Requested resource not found".to_owned()
        }
        ErrorMessageKey::TableAlreadyExists => {
            format!("Table already exists: {}", args[0])
        }
        ErrorMessageKey::TableInUse => {
            format!("Table is being created, updated, or deleted: {}", args[0])
        }
        ErrorMessageKey::MissingKeySchema => {
            "1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: \
             Member must not be null"
                .to_owned()
        }
        ErrorMessageKey::KeySchemaTooMany => {
            "Too many KeySchema attributes defined for table".to_owned()
        }
        ErrorMessageKey::KeySchemaFirstNotHash => {
            "First KeySchemaElement is not a HASH type".to_owned()
        }
        ErrorMessageKey::AttrDefNotInKey => {
            format!(
                "Number of attributes in KeySchema does not exactly match number of attributes defined in AttributeDefinitions: {}",
                args[0]
            )
        }
    }
}
