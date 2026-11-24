// SPDX-License-Identifier: Apache-2.0

//! Checks on the values a `Query` carries, as the service applies them before
//! any row is read.
//!
//! Measured on 2026-10-24 (tables with S/S, S/N and B/B keys, a GSI and an
//! LSI) and on 2026-11-11:
//!
//! 1. an `N` value in `ExpressionAttributeValues` that is not a number answers
//!    `The parameter cannot be converted to a numeric value: <value>` (or the
//!    overflow, underflow and 38-digit messages). Every value in the map is
//!    checked, whether the key condition, the filter or nothing reads it. The
//!    check runs at request time, before the table is looked up, in the order
//!    [`validate_query_request_body`] gives;
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

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition,
};
use extenddb_core::types::{AttributeDefinition, QueryInput};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

mod unreadable;
mod value_fault;

pub(crate) use unreadable::{
    QUERY_CONDITIONS, QUERY_VALUE_EXPRESSIONS, SCAN_CONDITIONS, has_unreadable, older_form_checks,
    older_form_request_checks, readable_for_shape_check, value_shape_fault,
};
pub(crate) use value_fault::{Fault, SetFault, first_fault};

/// The service's text for a key condition value of the wrong type.
pub const KEY_CONDITION_TYPE_MISMATCH: &str = "One or more parameter values were invalid: Condition parameter type does not match schema type";

/// The members of the two enums the request-time checks read.
pub(crate) const RETURN_CONSUMED_CAPACITY: &[&str] = &["INDEXES", "TOTAL", "NONE"];
pub(crate) const SELECT: &[&str] = &[
    "SPECIFIC_ATTRIBUTES",
    "COUNT",
    "ALL_ATTRIBUTES",
    "ALL_PROJECTED_ATTRIBUTES",
];

/// The `Query` checks that run on the request body alone, before the body is
/// deserialized and before the table is looked up, in the service's order:
/// the `Limit` range (at least 1, the newer form `1 validation error
/// detected: Value at 'Limit' failed to satisfy constraint: Member must have
/// value greater than or equal to 1`), the `ReturnConsumedCapacity` enum, the
/// expression and non-expression parameter mix, the `ExpressionAttributeNames`
/// map (see `validate_names`), every number in `ExpressionAttributeValues`,
/// then the `Select` enum (measured 2026-10-24 and 2026-11-11: a `Limit` of 0
/// is reported ahead of an invalid enum value of either field and of a
/// parameter mix, an invalid `ReturnConsumedCapacity` ahead of a mix with
/// `AttributesToGet`, `QueryFilter`, `ConditionalOperator` or `KeyConditions`
/// and ahead of an `N` value that is not a number, a mix ahead of that value,
/// and that value ahead of an invalid `Select`. Measured 2026-11-19 in either
/// member order: an invalid names map is reported after a `Limit` of 0, an
/// invalid `ReturnConsumedCapacity` and a mix, and ahead of an `N` value that
/// is not a number, a set with a duplicate member and an invalid `Select`).
///
/// A request that holds a value the service cannot read as one type (in
/// `ExpressionAttributeValues`, `ExclusiveStartKey` or a `KeyConditions` or
/// `QueryFilter` list; see `has_unreadable`), or whose `Limit` is a float or
/// an integer outside int32, takes the service's older validation path, with
/// the `Limit` fault in its older form, `Value '-2147483648' at 'limit' ...`,
/// the value echoed as the service reads it (see `int32_member::ranges`); see
/// `older_form_checks`. Measured 2026-11-23 (us-east-1, each request twice):
/// `Limit: -2147483649` beside a readable value answers `Value '-2147483648'
/// at 'limit'`, and `Limit: 0.0` answers `Value '0' at 'limit'`.
///
/// The checks read the raw body, so a field that does not deserialize (an
/// invalid `Select`, for one) does not stop the ones before it. When a check
/// fails, each member of the body is deserialized on its own as part of a
/// [`QueryInput`] (see `shape_fault_first`), and a `SerializationException`
/// from that (a member of the wrong JSON type, in any member order) is the
/// answer instead, as the service reports a shape fault ahead of every
/// validation (measured 2026-10-25: a `Limit` of 0 with a
/// string `ExpressionAttributeValues`, a string `ExpressionAttributeNames` with
/// an `N` value that is not a number, and a `Limit` of 0 with `Select: 5` all
/// answer `SerializationException`). A `ValidationException` the deserializer
/// raises (an empty set, a duplicate, an invalid `AttributeValue`) does not
/// preempt the check. In a value the service cannot read as one type, only the
/// parts it cannot read are replaced for that deserialization, so a JSON-type
/// fault in the rest of the value is still a shape fault; see
/// `readable_for_shape_check`. A body that passes every check is not
/// deserialized here.
///
/// The server runs the same function ahead of the IAM decision, so a principal
/// whose `dynamodb:LeadingKeys` fence cannot bind a number that does not parse
/// gets the request's `ValidationException` rather than
/// `AccessDeniedException`. That call goes through this function too, so it
/// answers the shape fault first as well. It comes after the server's
/// cross-account refusal, so a `Query` that names a table ARN in another
/// account answers `AccessDeniedException` without these checks, where the
/// service makes them first for a body it can read (measured 2026-11-12: with
/// such an ARN, `{"N": "bob"}` answers the numeric message and a `Limit` of 0
/// the `Limit` text).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text, or
/// `DynamoDbError::SerializationException` for a body of the wrong shape.
pub fn validate_query_request_body(body: &Value) -> Result<(), DynamoDbError> {
    query_request_checks(body).map_err(|check| {
        shape_fault_first::<QueryInput>(&readable_for_shape_check(body, QUERY_CONDITIONS), check)
    })
}

/// When a request-time check has failed with `check`, the deserializer's
/// `SerializationException` for a member of `body` if there is one, else
/// `check`. The service reports a member of the wrong JSON type ahead of the
/// checks whatever the member order (measured 2026-11-23, each body twice:
/// `Select: 5` beside `{"X": "1"}` or an empty `NS`, and `{"S": 5}` after an
/// empty `NS` in another value, each with a `Limit` of 0, answer
/// `SerializationException: NUMBER_VALUE cannot be converted to String` on
/// `Query`, and the first of them on `Scan` too). So each member is
/// deserialized on its own as a `T`, beside the body's `TableName` (the one
/// member `T` must have), and each `ExpressionAttributeValues` entry on its
/// own. The answer is the first shape fault in the map's iteration order (see
/// [`first_fault`]). Inside one member or entry, the deserializer stops at the
/// first fault.
///
/// These are not shape faults here:
///
/// - an enum value that is a string but not a member, which deserializes to a
///   `ValidationException`. One of another JSON type is a shape fault
///   (measured 2026-10-25: `Select: 5` with a `Limit` of 0 answers
///   `SerializationException: NUMBER_VALUE cannot be converted to String`);
/// - an AttributeValue with an unknown type descriptor, a value the service
///   cannot read as one type: `Limit: 0` beside `{"X": "1"}` answers the
///   `Limit` `ValidationException` (measured 2026-10-25 and 2026-11-23);
/// - a member the service reads from a JSON type that `T` refuses: a string
///   in `ConsistentRead` or `ScanIndexForward` that the service reads as a
///   boolean (see [`BOOLEAN_TEXTS`]), and any number in `Limit`, `Segment` or
///   `TotalSegments`. Measured 2026-11-23, beside `Select: "NOPE"`:
///   `ConsistentRead: "true"` or `"T"`, `ScanIndexForward: "true"` or `"0"`,
///   and `Limit: 1.5` answer the `Select` text on `Query`, and
///   `ConsistentRead: "true"`, `Segment: 1.5` with `TotalSegments: 2`, and
///   `TotalSegments: 2.5` with `Segment: 0` answer it on `Scan`.
///   `ConsistentRead: "yes"` with a table that does not exist answers
///   `ResourceNotFoundException`. `ConsistentRead: "on"` answers
///   `SerializationException: Unexpected token received from parser`, and
///   `ConsistentRead: 1` `SerializationException: NUMBER_VALUE cannot be
///   converted to Boolean`. The handler's own deserialization still refuses
///   every string in `ConsistentRead` and `ScanIndexForward`.
///
/// `Limit`, `Segment` and `TotalSegments` of another JSON type are shape
/// faults with the texts of `int32_member::read` (`STRING_VALUE cannot be
/// converted to Integer`, measured 2026-11-23 on `Query` and `Scan`), which
/// the handlers read them with.
pub(crate) fn shape_fault_first<T: DeserializeOwned>(
    body: &Value,
    check: DynamoDbError,
) -> DynamoDbError {
    let read = |name: &str, value: Value| {
        let mut one: Map<String, Value> = body
            .get("TableName")
            .map(|table_name| ("TableName".to_owned(), table_name.clone()))
            .into_iter()
            .collect();
        one.insert(name.to_owned(), value);
        shape_fault::<T>(Value::Object(one))
    };
    body.as_object()
        .into_iter()
        .flatten()
        .find_map(|(name, value)| match (name.as_str(), value) {
            ("ConsistentRead" | "ScanIndexForward", Value::String(text))
                if BOOLEAN_TEXTS
                    .iter()
                    .any(|word| word.eq_ignore_ascii_case(text)) =>
            {
                None
            }
            ("Limit" | "Segment" | "TotalSegments", member) => {
                crate::int32_member::read(Some(member)).err()
            }
            ("ExpressionAttributeValues", Value::Object(entries)) => {
                entries.iter().find_map(|(key, entry)| {
                    read(
                        name,
                        Value::Object(Map::from_iter([(key.clone(), entry.clone())])),
                    )
                })
            }
            _ => read(name, value.clone()),
        })
        .unwrap_or(check)
}

/// The strings the service reads as a boolean, in any ASCII case: the first
/// five as true, the others as false. It refuses every other string with
/// `SerializationException`. Measured 2026-11-10 for each of them in
/// `UpdateContinuousBackups` `PointInTimeRecoveryEnabled`, 2026-11-11 for
/// `"yes"` and `"TRUE"` in `GetItem` `ConsistentRead` and `"no"` in `Query`
/// `ScanIndexForward`, and 2026-11-23 on `Query` for `"T"`, `"0"` and the
/// refused `"on"`.
const BOOLEAN_TEXTS: [&str; 10] = ["true", "t", "yes", "y", "1", "false", "f", "no", "n", "0"];

/// The deserializer's `SerializationException` for `body` as a `T`, unless it
/// is the one for an AttributeValue with an unknown type descriptor.
fn shape_fault<T: DeserializeOwned>(body: Value) -> Option<DynamoDbError> {
    serde_json::from_value::<T>(body)
        .map_err(crate::deserialize_error)
        .err()
        .filter(is_shape_fault)
}

/// Whether `fault`, from [`crate::deserialize_error`], is a fault in the shape
/// of the body (a member of the wrong JSON type), which the service reports
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
    let ranges = crate::int32_member::ranges(body, &crate::int32_member::QUERY_RANGES);
    let values = body
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object);
    if ranges.older_form || has_unreadable(body, QUERY_CONDITIONS) {
        return older_form_checks(
            body,
            values,
            &ranges.faults,
            validate_no_parameter_mix,
            QUERY_VALUE_EXPRESSIONS,
        );
    }
    if !ranges.faults.is_empty() {
        return Err(crate::int32_member::newer_form(None, &ranges.faults));
    }

    // Query stops at the first invalid enum: ReturnConsumedCapacity ahead of
    // the parameter mix, Select after the numbers (measured 2026-08-24 and
    // 2026-11-11).
    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "ReturnConsumedCapacity",
            valid: RETURN_CONSUMED_CAPACITY,
            clause: crate::EnumClause::Named("returnConsumedCapacity"),
        }],
    )?;

    validate_no_parameter_mix(body)?;
    validate_names(body)?;

    if let Some(values) = values {
        validate_expression_attribute_value_numbers(values)?;
    }

    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "Select",
            valid: SELECT,
            clause: crate::EnumClause::Named("select"),
        }],
    )
}

/// The expression and non-expression parameter mix check.
fn validate_no_parameter_mix(body: &Value) -> Result<(), DynamoDbError> {
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
    )
}

/// The checks the `ExpressionAttributeNames` deserializer makes, on the raw
/// body: an empty map, a key that is not `#` and a name, a key over 255 bytes,
/// and an empty name, with the deserializer's text. A map or name of another
/// JSON type is a shape fault, which this leaves to the shape fault check.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the deserializer's text.
pub(crate) fn validate_names(body: &Value) -> Result<(), DynamoDbError> {
    let Some(names) = body.get("ExpressionAttributeNames") else {
        return Ok(());
    };
    match extenddb_core::serde_helpers::deserialize_expression_names(names)
        .map_err(crate::deserialize_error)
    {
        Err(fault @ DynamoDbError::ValidationException(_)) => Err(fault),
        _ => Ok(()),
    }
}

/// The key checks of [`validate_names`] alone: an empty map, a key that is not
/// `#` and a name, and a key over 255 bytes. Each name is replaced with a
/// name that is not empty, so an empty name passes here. Beside a value the
/// service cannot read as one type, the service makes the key checks ahead of
/// the values and reports an empty name after them; see
/// `older_form_checks`.
fn validate_name_keys(body: &Value) -> Result<(), DynamoDbError> {
    match body.get("ExpressionAttributeNames") {
        Some(Value::Object(names)) => {
            let keys: Map<String, Value> = names
                .keys()
                .map(|key| (key.clone(), Value::from("a")))
                .collect();
            validate_names(&serde_json::json!({ "ExpressionAttributeNames": keys }))
        }
        _ => validate_names(body),
    }
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

/// The entries of `values` by placeholder, so the reported value is
/// deterministic when several are invalid.
pub(crate) fn sorted_entries(values: &Map<String, Value>) -> Vec<(&String, &Value)> {
    let mut entries: Vec<(&String, &Value)> = values.iter().collect();
    entries.sort_by_key(|(key, _)| *key);
    entries
}

/// Refuse the first `N` value in `ExpressionAttributeValues` (including `NS`
/// members and values nested in `L` and `M`) that is not a number, with the
/// bare numeric message. On `Query` the service adds the
/// `ExpressionAttributeValues contains invalid value` wrapper only for a value
/// it cannot read as one type or beside a `Limit` above 2147483647 (measured
/// 2026-11-12). The placeholders are visited in sorted
/// order and each value on its own, so a value of the wrong JSON shape does
/// not stop the check of another (the service picks among several invalid
/// values by an order of its own: measured 2026-11-11, `:a` and `:b` with
/// numbers that are not numbers named either over ten requests). An empty `N`
/// answers the message with the colon and nothing after it, as the service
/// does on `Query`.
///
/// A Unicode decimal digit in the Basic Multilingual Plane counts as the
/// digit it stands for, as the service reads it (measured 2026-10-25: a
/// fullwidth or Arabic-Indic digit is accepted, thirty nine Devanagari ones
/// answer the 38 digit message); see [`crate::decimal_digits`]. The message
/// echoes the value with each such digit folded to ASCII.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub(crate) fn validate_expression_attribute_value_numbers(
    values: &Map<String, Value>,
) -> Result<(), DynamoDbError> {
    match sorted_entries(values)
        .into_iter()
        .find_map(|(_, value)| first_fault(value, None, false))
    {
        Some(fault) => Err(DynamoDbError::ValidationException(
            fault.request_time_text(),
        )),
        None => Ok(()),
    }
}

/// Refuse a key condition value whose type is not the key's
/// `AttributeDefinitions` type.
///
/// Every value the condition compares is checked: the partition key value(s),
/// the sort key value (or both `BETWEEN` bounds, or the `begins_with` prefix)
/// and the extra equality conditions of a multi-attribute key schema. Each one
/// goes through the check the key schema classifier applies to a clause
/// (`check_key_value_type` in `query.rs`). A path that does not resolve to an
/// attribute name, a value that is not a placeholder, and a key attribute with
/// no definition are left to the checks that already refuse them. When any
/// compared placeholder is undefined the whole check is skipped: the service
/// reports the undefined placeholder first, and the storage layer does so
/// here.
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
        crate::query::check_key_value_type(path, value, attribute_definitions, maps)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod request_order_tests;

#[cfg(test)]
mod unreadable_tests;

#[cfg(test)]
mod names_tests;
