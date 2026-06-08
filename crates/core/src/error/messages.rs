// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Error message templates captured from real `DynamoDB`.
//! These are the exact messages `DynamoDB` returns — tenet 4 (errors are contracts).

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
    let count = errors.len();
    let details: Vec<String> = errors
        .iter()
        .map(|(value, field, constraint)| {
            format!("Value '{value}' at '{field}' failed to satisfy constraint: {constraint}")
        })
        .collect();
    format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        details.join("; ")
    )
}

/// Keys for error message templates. Compile-time checked — no stringly-typed lookups.
#[derive(Debug, Clone, Copy)]
pub enum ErrorMessageKey {
    TableNameTooShort,
    TableNameTooLong,
    TableNameNull,
    TableNameEmpty,
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
        ErrorMessageKey::TableNameTooShort => {
            format!("1 validation error detected: Value '{}' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3", args[0])
        }
        ErrorMessageKey::TableNameTooLong => {
            format!("1 validation error detected: Value '{}' at 'tableName' failed to satisfy constraint: Member must have length less than or equal to 255", args[0])
        }
        ErrorMessageKey::TableNameNull => {
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null".to_owned()
        }
        ErrorMessageKey::TableNameEmpty => {
            "1 validation error detected: Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned()
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
