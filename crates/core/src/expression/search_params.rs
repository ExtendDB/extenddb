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
//! 1. the names: an empty attribute name anywhere in the map, then a name
//!    longer than `max_attribute_name_bytes` of `[limits]` (65535 by
//!    default), then an empty map, then each key (empty, longer than
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
use crate::expression::validate_expression_param_usage;
use crate::limits::LimitsConfig;
use crate::serde_helpers::validate_placeholder_key;
use crate::types::AttributeValue;
use crate::validation::{AttributeNameRequestForm, validate_expression_attribute_name_sizes};

const NAMES: &str = "ExpressionAttributeNames";
const VALUES: &str = "ExpressionAttributeValues";

/// The start of the unused-values message of both search condition grammars.
const UNUSED_VALUES: &str = "Value provided in ExpressionAttributeValues unused in expressions";

fn invalid(msg: String) -> DynamoDbError {
    DynamoDbError::ValidationException(msg)
}

/// Check one map's size and keys, in the order the service checks them. An
/// empty key has its own message on this endpoint; every other key goes
/// through the check the other operations use, with the key size in the
/// too-long message of both maps.
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
        validate_placeholder_key(key, prefix, field, true).map_err(invalid)
    })
}

fn check_names(
    names: &HashMap<String, String>,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    if let Some((key, _)) = names.iter().find(|(_, name)| name.is_empty()) {
        return Err(invalid(format!(
            "{NAMES} contains invalid value: Empty attribute name for key {key}"
        )));
    }
    validate_expression_attribute_name_sizes(
        Some(names),
        None,
        limits,
        AttributeNameRequestForm::SearchRequest,
    )?;
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
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    if let Some(names) = names {
        check_names(names, limits)?;
    }
    if let Some(values) = values {
        check_keys(VALUES, ':', values.keys())?;
    }
    // Both maps are known not to be empty here, so the shared check refuses
    // exactly a map that no expression of the request can use.
    validate_expression_param_usage(
        names,
        has_condition || has_projection,
        values,
        has_condition,
        &[],
    )
}

/// Refuse the keys of `map` that `used` does not hold, listed in sorted
/// order. `field` names the map in the message. Both grammars of the search
/// condition and the projection check use it.
///
/// # Errors
///
/// Returns the unused-keys `ValidationException`.
pub fn check_unused_keys<V>(
    field: &str,
    map: Option<&HashMap<String, V>>,
    used: &BTreeSet<&str>,
) -> Result<(), DynamoDbError> {
    let unused: BTreeSet<&str> = map
        .into_iter()
        .flat_map(HashMap::keys)
        .map(String::as_str)
        .filter(|k| !used.contains(k))
        .collect();
    if unused.is_empty() {
        return Ok(());
    }
    Err(invalid(format!(
        "Value provided in {field} unused in expressions: keys: {{{}}}",
        unused.into_iter().collect::<Vec<_>>().join(", ")
    )))
}

/// Every name in `names` must be used by the search condition, whose
/// `#name` tokens are `condition_names`, or by `projection`, a projection
/// that has already parsed.
///
/// # Errors
///
/// Returns the unused-names `ValidationException`, or the projection's
/// tokenizer error, which the earlier parse of the projection has answered.
pub fn check_unused_names(
    names: Option<&HashMap<String, String>>,
    condition_names: &BTreeSet<&str>,
    projection: &str,
) -> Result<(), DynamoDbError> {
    if names.is_none() {
        return Ok(());
    }
    let projection_names: Vec<String> =
        crate::expression::tokenize_with_limit(projection, usize::MAX)?
            .into_iter()
            .filter_map(|t| match t {
                crate::expression::Token::NameRef(name) => Some(format!("#{name}")),
                _ => None,
            })
            .collect();
    let mut used = condition_names.clone();
    used.extend(projection_names.iter().map(String::as_str));
    check_unused_keys(NAMES, names, &used)
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
    condition_names: &BTreeSet<&str>,
    projection: &str,
) -> Result<T, DynamoDbError> {
    match parsed {
        Ok(parsed) => check_unused_names(names, condition_names, projection).map(|()| parsed),
        Err(DynamoDbError::ValidationException(msg)) if msg.starts_with(UNUSED_VALUES) => {
            check_unused_names(names, condition_names, projection)?;
            Err(DynamoDbError::ValidationException(msg))
        }
        Err(other) => Err(other),
    }
}

#[cfg(test)]
#[path = "search_params_tests.rs"]
mod tests;
