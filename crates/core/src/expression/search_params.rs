// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `ExpressionAttributeNames` and `ExpressionAttributeValues` checks of a
//! `SearchVectors` request.
//!
//! The service checks both maps once `TopK` and `SearchVector` are valid and
//! before it reads the projection or the search condition, so a fault here
//! answers ahead of a dimension mismatch, a missing table and any expression
//! error. In order:
//!
//! 1. the names: an empty attribute name anywhere in the map, then a name of
//!    65536 bytes or more, then an empty map, then each key (empty, longer than
//!    255 bytes, not `#` followed by letters, digits or `_`);
//! 2. the values: an empty map, then each key, as for the names with `:`;
//! 3. names with neither a search condition nor a projection, then values
//!    without a search condition.
//!
//! After the expressions parse, every name must be used by the search
//! condition or the projection. That check answers ahead of the unused-values
//! check and ahead of the index schema checks.
//!
//! The key messages of this endpoint differ from those of the other
//! operations, which `serde_helpers` produces: an empty key has its own
//! message, and a key that is too long reports its size in both maps.
//!
//! Pure synchronous Rust: no async, no I/O.

use std::collections::{BTreeSet, HashMap};

use crate::error::DynamoDbError;
use crate::serde_helpers::is_placeholder_ident;
use crate::types::AttributeValue;

/// The longest key either map takes, in bytes, prefix included.
const MAX_KEY_BYTES: usize = 255;

/// An attribute name must be shorter than this many bytes.
const NAME_LIMIT_BYTES: usize = 65536;

const NAMES: &str = "ExpressionAttributeNames";
const VALUES: &str = "ExpressionAttributeValues";

/// The start of the unused-values message of both search condition grammars.
const UNUSED_VALUES: &str = "Value provided in ExpressionAttributeValues unused in expressions";

fn invalid(msg: String) -> DynamoDbError {
    DynamoDbError::ValidationException(msg)
}

/// Check one map's size and keys, in the order the service checks them.
fn check_keys<'a>(
    field: &str,
    prefix: char,
    mut keys: impl ExactSizeIterator<Item = &'a String>,
) -> Result<(), DynamoDbError> {
    if keys.len() == 0 {
        return Err(invalid(format!("{field} must not be empty")));
    }
    keys.try_for_each(|key| {
        if key.is_empty() {
            return Err(invalid(format!(
                "{field} contains invalid key: The expression attribute map contains an empty key;"
            )));
        }
        if key.len() > MAX_KEY_BYTES {
            return Err(invalid(format!(
                "{field} contains invalid key: The expression attribute map contains a key that is \
                 too long; size of key: {}",
                key.len()
            )));
        }
        if !key.strip_prefix(prefix).is_some_and(is_placeholder_ident) {
            return Err(invalid(format!(
                "{field} contains invalid key: Syntax error; key: \"{key}\""
            )));
        }
        Ok(())
    })
}

fn check_names(names: &HashMap<String, String>) -> Result<(), DynamoDbError> {
    if let Some((key, _)) = names.iter().find(|(_, name)| name.is_empty()) {
        return Err(invalid(format!(
            "{NAMES} contains invalid value: Empty attribute name for key {key}"
        )));
    }
    if let Some((key, _)) = names
        .iter()
        .find(|(_, name)| name.len() >= NAME_LIMIT_BYTES)
    {
        return Err(invalid(format!(
            "Attribute name is too large, must be less than {NAME_LIMIT_BYTES} bytes for key {key}"
        )));
    }
    check_keys(NAMES, '#', names.keys())
}

/// Check the two maps of a `SearchVectors` request before either expression
/// is read.
///
/// # Errors
///
/// Returns the `ValidationException` of the first failed check, in the order
/// the module documentation gives.
pub fn check_search_maps(
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    has_condition: bool,
    has_projection: bool,
) -> Result<(), DynamoDbError> {
    if let Some(names) = names {
        check_names(names)?;
    }
    if let Some(values) = values {
        check_keys(VALUES, ':', values.keys())?;
    }
    if names.is_some() && !has_condition && !has_projection {
        return Err(invalid(format!(
            "{NAMES} can only be specified when using expressions"
        )));
    }
    if values.is_some() && !has_condition {
        return Err(invalid(format!(
            "{VALUES} can only be specified when using expressions"
        )));
    }
    Ok(())
}

/// Every `#name` written in `exprs`.
fn written_names<'a>(exprs: &[&'a str]) -> BTreeSet<&'a str> {
    let mut found = BTreeSet::new();
    for expr in exprs {
        let bytes = expr.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'#' {
                let end = bytes[i + 1..]
                    .iter()
                    .position(|b| !(b.is_ascii_alphanumeric() || *b == b'_'))
                    .map_or(bytes.len(), |n| i + 1 + n);
                found.insert(&expr[i..end]);
                i = end.max(i + 1);
            } else {
                i += 1;
            }
        }
    }
    found
}

/// Every name in `names` must be written in one of `exprs` (the search
/// condition and the projection). The unused keys are listed in sorted order.
///
/// # Errors
///
/// Returns the unused-names `ValidationException`.
pub fn check_unused_names(
    names: Option<&HashMap<String, String>>,
    exprs: &[&str],
) -> Result<(), DynamoDbError> {
    let Some(names) = names else {
        return Ok(());
    };
    let written = written_names(exprs);
    let unused: BTreeSet<&str> = names
        .keys()
        .map(String::as_str)
        .filter(|k| !written.contains(k))
        .collect();
    if unused.is_empty() {
        return Ok(());
    }
    Err(invalid(format!(
        "Value provided in {NAMES} unused in expressions: keys: {{{}}}",
        unused.into_iter().collect::<Vec<_>>().join(", ")
    )))
}

/// Apply the unused-names check to the result of parsing a search condition.
///
/// A parse failure answers first, except the unused-values failure, which
/// both grammars report last and the service reports after the unused names.
///
/// # Errors
///
/// Returns the parse error, or the unused-names error, as above.
pub fn with_unused_names<T>(
    parsed: Result<T, DynamoDbError>,
    names: Option<&HashMap<String, String>>,
    exprs: &[&str],
) -> Result<T, DynamoDbError> {
    match parsed {
        Ok(parsed) => check_unused_names(names, exprs).map(|()| parsed),
        Err(DynamoDbError::ValidationException(msg)) if msg.starts_with(UNUSED_VALUES) => {
            check_unused_names(names, exprs)?;
            Err(DynamoDbError::ValidationException(msg))
        }
        Err(other) => Err(other),
    }
}

#[cfg(test)]
#[path = "search_params_tests.rs"]
mod tests;
