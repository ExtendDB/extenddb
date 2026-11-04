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
use extenddb_core::types::{AttributeDefinition, AttributeValue, QueryInput, ScalarAttributeType};
use serde::de::DeserializeOwned;
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
/// invalid `Select`, for one) does not stop the ones before it. When a check
/// fails, the body is deserialized as a [`QueryInput`], and a
/// `SerializationException` from that (a field of the wrong JSON shape
/// anywhere in the body) is the answer instead, as the service reports a shape
/// fault ahead of every validation (measured 2026-10-25: a `Limit` of 0 with a
/// string `ExpressionAttributeValues`, a string `ExpressionAttributeNames` with
/// an `N` value that is not a number, and a `Limit` of 0 with `Select: 5` all
/// answer `SerializationException`). A `ValidationException` the deserializer
/// raises (an empty set, a duplicate, an invalid `AttributeValue`) does not
/// preempt the check. A body that passes every check is not deserialized here.
///
/// The server runs the same function ahead of the IAM decision, so a principal
/// whose `dynamodb:LeadingKeys` fence cannot bind a number that does not parse
/// gets the request's `ValidationException` rather than
/// `AccessDeniedException`. That call goes through this function too, so it
/// answers the shape fault first as well.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text, or
/// `DynamoDbError::SerializationException` for a body of the wrong shape.
pub fn validate_query_request_body(body: &Value) -> Result<(), DynamoDbError> {
    query_request_checks(body).map_err(|check| shape_fault_first::<QueryInput>(body, check))
}

/// When a request-time check has failed with `check`, the deserializer's
/// `SerializationException` for `body` if there is one, else `check`. The two
/// enum fields stay in the body: an enum value that is a string but not a
/// member deserializes to a `ValidationException`, which this ignores, and one
/// of another JSON type is a shape fault the service reports ahead of the
/// checks (measured 2026-10-25: `Select: 5` with a `Limit` of 0 answers
/// `SerializationException: NUMBER_VALUE cannot be converted to String`).
///
/// An AttributeValue with an unknown type descriptor is not a shape fault to
/// the service: `Limit: 0` beside `{"X": "1"}` answers the `Limit`
/// `ValidationException` (measured 2026-10-25), so the deserializer's
/// `SerializationException` for that message does not preempt the check.
pub(crate) fn shape_fault_first<T: DeserializeOwned>(
    body: &Value,
    check: DynamoDbError,
) -> DynamoDbError {
    match serde_json::from_value::<T>(body.clone()) {
        Err(e) => {
            let fault = crate::deserialize_error(e);
            if is_shape_fault(&fault) { fault } else { check }
        }
        Ok(_) => check,
    }
}

/// Whether `fault`, from [`crate::deserialize_error`], is a fault in the shape
/// of the body (a field of the wrong JSON type), which the service reports
/// ahead of every check, rather than an attribute value the deserializer
/// refuses with a `ValidationException` or an unknown type descriptor; see
/// [`shape_fault_first`].
pub(crate) fn is_shape_fault(fault: &DynamoDbError) -> bool {
    matches!(fault, DynamoDbError::SerializationException(msg) if !msg.contains(UNKNOWN_TYPE_DESCRIPTOR))
}

/// The deserializer's text for an AttributeValue whose type descriptor is not
/// one of the service's.
const UNKNOWN_TYPE_DESCRIPTOR: &str = "unknown AttributeValue type descriptor";

/// The `Query` checks in the service's order; see
/// [`validate_query_request_body`].
fn query_request_checks(body: &Value) -> Result<(), DynamoDbError> {
    if let Some(limit) = body.get("Limit").and_then(Value::as_i64)
        && limit < 1
    {
        // Beside a value that is not one AttributeValue the service answers
        // its older form, with the value echoed (measured 2026-10-25 12:56 UTC
        // on Query and Scan, and 2026-10-31 for the key and the lists).
        if has_unreadable_attribute_value(body, QUERY_CONDITIONS) {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '{}' at 'limit' failed to satisfy constraint: \
                 Member must have value greater than or equal to 1",
                limit.clamp(i64::from(i32::MIN), i64::from(i32::MAX))
            )));
        }
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
pub(crate) fn present(body: &Value, field: &str) -> bool {
    body.get(field).is_some_and(|v| !v.is_null())
}

/// Whether `field` is a non-empty JSON object or array in the body.
pub(crate) fn non_empty(body: &Value, field: &str) -> bool {
    match body.get(field) {
        Some(Value::Object(map)) => !map.is_empty(),
        Some(Value::Array(items)) => !items.is_empty(),
        _ => false,
    }
}

/// The type descriptors an AttributeValue may carry.
const TYPE_DESCRIPTORS: &[&str] = &["S", "N", "B", "SS", "NS", "BS", "M", "L", "NULL", "BOOL"];

/// The legacy condition maps of `Scan` whose `AttributeValueList` entries
/// [`has_unreadable_attribute_value`] reads.
pub(crate) const SCAN_CONDITIONS: &[&str] = &["ScanFilter"];

/// The legacy condition maps of `Query` whose `AttributeValueList` entries
/// [`has_unreadable_attribute_value`] reads.
pub(crate) const QUERY_CONDITIONS: &[&str] = &["KeyConditions", "QueryFilter"];

/// Whether the request holds an object that is not one AttributeValue: empty,
/// with a type descriptor the service does not have (a lower-case `s`
/// included), or with more than one, at the top or as a member of an `L` or
/// `M` at any depth. The values read are the `ExpressionAttributeValues`
/// entries, the `ExclusiveStartKey` values and the `AttributeValueList`
/// entries of the `conditions` maps. Beside such a value the service reports a
/// range fault in its older form, and beside a readable value, of the wrong
/// type for a key or not, the newer form.
///
/// Measured on 2026-10-25 at 12:56 UTC: `Limit: 0` beside an entry `{}`,
/// `{"X": "1"}` or `{"S": "x", "N": "1"}` answers `Value '0' at 'limit'` on
/// Query and Scan, and beside an empty or duplicate `NS` the newer form. On
/// 2026-10-30 at 12:40 UTC on Scan and 2026-10-31 at 12:36 UTC on Query:
/// beside an entry `{"L": [{}]}`, `{"M": {"k": {"X": "1"}}}` or `{"s": "x"}`,
/// the older form. On 2026-10-31 between 23:54 and 23:56 UTC: beside `{}` or
/// `{"X": "1"}` as an `ExclusiveStartKey` value or a `ScanFilter`
/// `AttributeValueList` entry on Scan, and `{}` as an `ExclusiveStartKey`
/// value or a `KeyConditions` or `QueryFilter` `AttributeValueList` entry on
/// Query, the older form, for a `Segment` fault too; beside `{"N": "1"}` as
/// the value of an `S` key, `{"N": "1"}` in a `ScanFilter` list and `{"S":
/// "p0"}` in a `KeyConditions` list, the newer form. An `L` or `M` member of
/// an `ExclusiveStartKey` value or of a list entry is read as one of an
/// `ExpressionAttributeValues` entry is; that is not measured.
///
/// A value that is not an object is a shape fault, reported ahead of the
/// checks.
pub(crate) fn has_unreadable_attribute_value(body: &Value, conditions: &[&str]) -> bool {
    let in_map = |field: &str| {
        body.get(field)
            .and_then(Value::as_object)
            .is_some_and(|values| values.values().any(is_unreadable_attribute_value))
    };
    let in_lists = |field: &&str| {
        body.get(*field)
            .and_then(Value::as_object)
            .is_some_and(|conditions| {
                conditions.values().any(|condition| {
                    condition
                        .get("AttributeValueList")
                        .and_then(Value::as_array)
                        .is_some_and(|list| list.iter().any(is_unreadable_attribute_value))
                })
            })
    };
    in_map("ExpressionAttributeValues")
        || in_map("ExclusiveStartKey")
        || conditions.iter().any(in_lists)
}

/// Whether `entry` is an object that is not one AttributeValue, or an `L` or
/// `M` with such a member; see [`has_unreadable_attribute_value`].
fn is_unreadable_attribute_value(entry: &Value) -> bool {
    let Some(descriptors) = entry.as_object() else {
        return false;
    };
    let mut fields = descriptors.iter();
    match (fields.next(), fields.next()) {
        (Some((descriptor, value)), None) => match descriptor.as_str() {
            "L" => value
                .as_array()
                .is_some_and(|members| members.iter().any(is_unreadable_attribute_value)),
            "M" => value
                .as_object()
                .is_some_and(|members| members.values().any(is_unreadable_attribute_value)),
            descriptor => !TYPE_DESCRIPTORS.contains(&descriptor),
        },
        _ => true,
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
/// A Unicode decimal digit (general category `Nd`) counts as the digit it
/// stands for, as the service reads it (measured 2026-10-25: a fullwidth or
/// Arabic-Indic digit is accepted, thirty nine Devanagari ones answer the 38
/// digit message); see [`crate::decimal_digits`]. The message echoes the value
/// as the request wrote it.
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
        validate_value_numbers(&values[key])?;
    }
    Ok(())
}

/// Every number inside one attribute value (`N`, `NS` members, and the values
/// nested in `L` and `M`), with Unicode decimal digits folded for the check.
fn validate_value_numbers(value: &AttributeValue) -> Result<(), DynamoDbError> {
    match value {
        AttributeValue::N(n) => validate_number_text(n),
        AttributeValue::NS(set) => set.iter().try_for_each(|n| validate_number_text(n)),
        AttributeValue::L(list) => list.iter().try_for_each(validate_value_numbers),
        AttributeValue::M(map) => map.values().try_for_each(validate_value_numbers),
        _ => Ok(()),
    }
}

/// One number text against the number rules, with the service's `Query`
/// message for an empty value and the original text echoed for a malformed one.
pub(crate) fn validate_number_text(text: &str) -> Result<(), DynamoDbError> {
    let folded = crate::decimal_digits::fold_decimal_digits(text);
    match extenddb_core::validation::number::validate_and_normalize_number(&folded) {
        Ok(_) => Ok(()),
        Err(DynamoDbError::ValidationException(msg)) if msg.starts_with(EMPTY_NUMBER) => Err(
            DynamoDbError::ValidationException(format!("{EMPTY_NUMBER}: {text}")),
        ),
        Err(other) => Err(other),
    }
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
