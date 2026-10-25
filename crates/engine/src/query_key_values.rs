// SPDX-License-Identifier: Apache-2.0

//! Checks on the values a `Query` carries, as the service applies them before
//! any row is read.
//!
//! Measured on 2026-10-24 (tables with S/S, S/N and B/B keys, a GSI and an
//! LSI):
//!
//! 1. an `N` value in `ExpressionAttributeValues` that is not a number answers
//!    `The parameter cannot be converted to a numeric value: <value>` (or the
//!    overflow, underflow and 38-digit messages). Every value in the map is
//!    checked, whether the key condition, the filter or nothing reads it. The
//!    check runs at request time, after the `Limit` check and the expression
//!    and non-expression parameter mix check, and before the table is looked
//!    up; see [`validate_query_request_body`];
//! 2. a key condition value whose type is not the key attribute's
//!    `AttributeDefinitions` type answers `One or more parameter values were
//!    invalid: Condition parameter type does not match schema type`, on the
//!    partition and the sort key, on a table, a GSI and an LSI, for `=`, the
//!    ordering operators, `BETWEEN` and `begins_with`. The check runs after the
//!    key schema, filter, projection, unused value and `Select` checks and
//!    before the `ExclusiveStartKey` check; see
//!    [`validate_key_condition_types`].
//!
//! Without these checks a backend compares the value's inner text with the
//! stored key and ignores the type tag, so `{"N": "bob"}` on an S key reads the
//! `bob` partition, while authorization binds no `dynamodb:LeadingKeys` value
//! to a number it cannot parse.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition,
};
use extenddb_core::types::{AttributeDefinition, AttributeValue, ScalarAttributeType};
use serde_json::Value;

/// The service's text for a key condition value of the wrong type.
pub const KEY_CONDITION_TYPE_MISMATCH: &str = "One or more parameter values were invalid: Condition parameter type does not match schema type";

/// The service's text for a `Limit` below 1 on `Query`.
pub const LIMIT_BELOW_ONE: &str = "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1";

/// The bare numeric message the number validator uses for an empty `N`.
const EMPTY_NUMBER: &str = "The parameter cannot be converted to a numeric value";

/// The `Query` checks that run on the request body alone, before the body is
/// deserialized and before the table is looked up, in the service's order:
/// `Limit`, the expression and non-expression parameter mix, the
/// `ReturnConsumedCapacity` enum, every number in `ExpressionAttributeValues`,
/// then the `Select` enum (measured 2026-10-24: a `Limit` of 0 and a parameter
/// mix are reported ahead of an invalid enum value of either field, an invalid
/// `ReturnConsumedCapacity` ahead of an `N` value that is not a number, and
/// that value ahead of an invalid `Select`).
///
/// The checks read the raw body, so a field that does not deserialize (an
/// invalid `Select`, for one) does not stop the ones before it. A field of the
/// wrong JSON shape is left to the deserializer, which runs next.
///
/// The server runs the same function ahead of the IAM decision, so a principal
/// whose `dynamodb:LeadingKeys` fence cannot bind a number that does not parse
/// gets the request's `ValidationException` rather than
/// `AccessDeniedException`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn validate_query_request_body(body: &Value) -> Result<(), DynamoDbError> {
    if let Some(limit) = body.get("Limit").and_then(Value::as_i64)
        && limit < 1
    {
        return Err(DynamoDbError::ValidationException(
            LIMIT_BELOW_ONE.to_owned(),
        ));
    }

    let kce_present = present(body, "KeyConditionExpression");
    // KeyConditions combines with FilterExpression and ProjectionExpression, so
    // it joins the mix check only when KeyConditionExpression is also present.
    extenddb_core::validation::validate_no_expression_param_mixing(
        &[
            ("AttributesToGet", non_empty(body, "AttributesToGet")),
            ("QueryFilter", non_empty(body, "QueryFilter")),
            ("ConditionalOperator", present(body, "ConditionalOperator")),
            (
                "KeyConditions",
                non_empty(body, "KeyConditions") && kce_present,
            ),
        ],
        &[
            (
                "ProjectionExpression",
                present(body, "ProjectionExpression"),
            ),
            ("FilterExpression", present(body, "FilterExpression")),
            ("KeyConditionExpression", kce_present),
        ],
    )?;

    // Query stops at the first invalid enum, ReturnConsumedCapacity ahead of
    // Select (measured 2026-08-24); the numbers sit between the two.
    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "ReturnConsumedCapacity",
            valid: &["INDEXES", "TOTAL", "NONE"],
            clause: crate::EnumClause::Named("returnConsumedCapacity"),
        }],
    )?;

    if let Some(raw) = body.get("ExpressionAttributeValues")
        && let Ok(values) = serde_json::from_value::<HashMap<String, AttributeValue>>(raw.clone())
    {
        validate_expression_attribute_value_numbers(&values)?;
    }

    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "Select",
            valid: &[
                "SPECIFIC_ATTRIBUTES",
                "COUNT",
                "ALL_ATTRIBUTES",
                "ALL_PROJECTED_ATTRIBUTES",
            ],
            clause: crate::EnumClause::Named("select"),
        }],
    )
}

/// Whether `field` is in the body with a value other than `null`, as a typed
/// `Option` field would deserialize to `Some`.
fn present(body: &Value, field: &str) -> bool {
    body.get(field).is_some_and(|v| !v.is_null())
}

/// Whether `field` is a non-empty JSON object or array in the body.
fn non_empty(body: &Value, field: &str) -> bool {
    match body.get(field) {
        Some(Value::Object(map)) => !map.is_empty(),
        Some(Value::Array(items)) => !items.is_empty(),
        _ => false,
    }
}

/// Refuse the first `N` value in `ExpressionAttributeValues` (including `NS`
/// members and values nested in `L` and `M`) that is not a number, with the
/// bare numeric message: no `ExpressionAttributeValues contains invalid value`
/// wrapper, which the service uses on `UpdateItem` and not on `Query`. The
/// placeholders are visited in sorted order so the reported value is
/// deterministic when several are invalid. An empty `N` answers the message
/// with the colon and nothing after it, as the service does on `Query`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn validate_expression_attribute_value_numbers(
    values: &HashMap<String, AttributeValue>,
) -> Result<(), DynamoDbError> {
    let mut keys: Vec<&String> = values.keys().collect();
    keys.sort();
    for key in keys {
        match extenddb_core::validation::validate_attribute_number(&values[key]) {
            Ok(()) => {}
            Err(DynamoDbError::ValidationException(msg)) if msg == EMPTY_NUMBER => {
                return Err(DynamoDbError::ValidationException(format!(
                    "{EMPTY_NUMBER}: "
                )));
            }
            Err(other) => return Err(other),
        }
    }
    Ok(())
}

/// Refuse a key condition value whose type is not the key's
/// `AttributeDefinitions` type.
///
/// Every value the condition compares is checked: the partition key value(s),
/// the sort key value (or both `BETWEEN` bounds, or the `begins_with` prefix)
/// and the extra equality conditions of a multi-attribute key schema. A path
/// that does not resolve to an attribute name, a value that is not a
/// placeholder, and a key attribute with no definition are left to the checks
/// that already refuse them. When any compared placeholder is undefined the
/// whole check is skipped: the service reports the undefined placeholder
/// first, and the storage layer does so here.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with
/// [`KEY_CONDITION_TYPE_MISMATCH`].
pub fn validate_key_condition_types(
    key_condition: &KeyCondition,
    maps: &ExpressionMaps,
    attribute_definitions: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    let mut pairs: Vec<(&[PathElement], &Expr)> =
        vec![(&key_condition.pk_path, &key_condition.pk_value)];
    for (path, value) in &key_condition.extra_pk_conditions {
        pairs.push((path, value));
    }
    match &key_condition.sk_condition {
        Some(SortKeyCondition::Compare { path, value, .. }) => pairs.push((path, value)),
        Some(SortKeyCondition::Between { path, low, high }) => {
            pairs.push((path, low));
            pairs.push((path, high));
        }
        Some(SortKeyCondition::BeginsWith { path, prefix }) => pairs.push((path, prefix)),
        None => {}
    }
    for (path, value) in &key_condition.extra_sk_conditions {
        pairs.push((path, value));
    }

    let any_undefined = pairs.iter().any(
        |(_, value)| matches!(value, Expr::Placeholder(name) if !maps.values.contains_key(name)),
    );
    if any_undefined {
        return Ok(());
    }

    for (path, value) in pairs {
        let Some(attr_name) = resolve_attr_name(path, &maps.names) else {
            continue;
        };
        let Some(expected) = attribute_definitions
            .iter()
            .find(|ad| ad.attribute_name == attr_name)
            .map(|ad| ad.attribute_type)
        else {
            continue;
        };
        let Expr::Placeholder(name) = value else {
            continue;
        };
        let Some(av) = maps.values.get(name) else {
            continue;
        };
        if !type_matches(expected, av) {
            return Err(DynamoDbError::ValidationException(
                KEY_CONDITION_TYPE_MISMATCH.to_owned(),
            ));
        }
    }
    Ok(())
}

fn type_matches(expected: ScalarAttributeType, value: &AttributeValue) -> bool {
    matches!(
        (expected, value),
        (ScalarAttributeType::S, AttributeValue::S(_))
            | (ScalarAttributeType::N, AttributeValue::N(_))
            | (ScalarAttributeType::B, AttributeValue::B(_))
    )
}

/// The top-level attribute name of a key path, through `#name` references.
fn resolve_attr_name(path: &[PathElement], names: &HashMap<String, String>) -> Option<String> {
    match path.first()? {
        PathElement::Attribute(name) => match name.strip_prefix('#') {
            Some(reference) => names.get(reference).cloned(),
            None => Some(name.clone()),
        },
        PathElement::Index(_) => None,
    }
}

#[cfg(test)]
mod tests;
