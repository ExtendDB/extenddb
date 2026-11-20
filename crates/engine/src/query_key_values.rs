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

use unreadable::{readable_for_shape_check, unreadable, unreadable_value_checks};

/// The service's text for a key condition value of the wrong type.
pub const KEY_CONDITION_TYPE_MISMATCH: &str = "One or more parameter values were invalid: Condition parameter type does not match schema type";

/// The service's text for a `Limit` below 1 on `Query`.
pub const LIMIT_BELOW_ONE: &str = "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1";

/// The bare numeric message the number validator uses for an empty `N`.
const EMPTY_NUMBER: &str = "The parameter cannot be converted to a numeric value";

/// The service's texts for a value it cannot read as one type.
const NULL_VALUE: &str = "The supplied Item contains a null AttributeValue";
const EMPTY_VALUE: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";
const SEVERAL_TYPES: &str = "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes";

/// The type descriptors of an AttributeValue.
const TYPE_DESCRIPTORS: [&str; 10] = ["S", "N", "B", "SS", "NS", "BS", "M", "L", "NULL", "BOOL"];

/// The members of the two enums the request-time checks read.
const RETURN_CONSUMED_CAPACITY: &[&str] = &["INDEXES", "TOTAL", "NONE"];
const SELECT: &[&str] = &[
    "SPECIFIC_ATTRIBUTES",
    "COUNT",
    "ALL_ATTRIBUTES",
    "ALL_PROJECTED_ATTRIBUTES",
];

/// The `Query` checks that run on the request body alone, before the body is
/// deserialized and before the table is looked up, in the service's order:
/// `Limit`, the `ReturnConsumedCapacity` enum, the expression and
/// non-expression parameter mix, the `ExpressionAttributeNames` map (see
/// `validate_names`), every number in `ExpressionAttributeValues`, then the
/// `Select` enum (measured 2026-10-24 and 2026-11-11: a `Limit` of 0 is
/// reported ahead of an invalid enum value of either field and of a parameter
/// mix, an invalid `ReturnConsumedCapacity` ahead of a mix with
/// `AttributesToGet`, `QueryFilter`, `ConditionalOperator` or `KeyConditions`
/// and ahead of an `N` value that is not a number, a mix ahead of that value,
/// and that value ahead of an invalid `Select`. Measured 2026-11-19 in either
/// member order: an invalid names map is reported after a `Limit` of 0, an
/// invalid `ReturnConsumedCapacity` and a mix, and ahead of an `N` value that
/// is not a number, a set with a duplicate member and an invalid `Select`).
///
/// A request whose `ExpressionAttributeValues` holds a value the service
/// cannot read as one type takes another order; see
/// `unreadable_value_checks`.
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
    query_request_checks(body)
        .map_err(|check| shape_fault_first::<QueryInput>(&readable_for_shape_check(body), check))
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
        Err(e) => match crate::deserialize_error(e) {
            DynamoDbError::SerializationException(msg) if msg.contains(UNKNOWN_TYPE_DESCRIPTOR) => {
                check
            }
            shape @ DynamoDbError::SerializationException(_) => shape,
            _ => check,
        },
        Ok(_) => check,
    }
}

/// The deserializer's text for an AttributeValue whose type descriptor is not
/// one of the service's.
const UNKNOWN_TYPE_DESCRIPTOR: &str = "unknown AttributeValue type descriptor";

/// The `Query` checks in the service's order; see
/// [`validate_query_request_body`].
fn query_request_checks(body: &Value) -> Result<(), DynamoDbError> {
    let values = body
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object);
    if let Some(values) = values
        && values.values().any(unreadable)
    {
        return unreadable_value_checks(body, values);
    }

    if let Some(limit) = body.get("Limit").and_then(Value::as_i64)
        && limit < 1
    {
        return Err(DynamoDbError::ValidationException(
            LIMIT_BELOW_ONE.to_owned(),
        ));
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
/// `unreadable_value_checks`.
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
fn sorted_entries(values: &Map<String, Value>) -> Vec<(&String, &Value)> {
    let mut entries: Vec<(&String, &Value)> = values.iter().collect();
    entries.sort_by_key(|(key, _)| *key);
    entries
}

/// The first fault inside one value, in document order for `L` items and in
/// key order for `M` members.
enum Fault<'a> {
    /// A null, with the member name when it is the value of an `M` member.
    Null(Option<&'a str>),
    /// No type descriptor the service knows.
    Empty,
    /// More than one type descriptor, with no fault in their payloads.
    SeveralTypes,
    /// An `N` text or `NS` member that is not a number, as the request wrote it.
    NotANumber(&'a str),
    /// A number the number rules refuse for its magnitude or precision, with
    /// their text.
    Refused(String),
}

impl Fault<'_> {
    /// The request-time number check's text: the value echoed with each
    /// decimal digit folded to ASCII (measured 2026-11-11: `{"N": "\u{ff11}bob"}`
    /// answers `The parameter cannot be converted to a numeric value: 1bob`,
    /// `\u{661}\u{662}x` answers `12x`), and the colon kept after an empty one.
    fn request_time_text(self) -> String {
        match self {
            Fault::NotANumber(text) => format!(
                "{EMPTY_NUMBER}: {}",
                crate::decimal_digits::fold_decimal_digits(text)
            ),
            other => other.reason(),
        }
    }

    /// The text [`unreadable_value_checks`] wraps: a number echoed as written
    /// and no colon after an empty one (measured 2026-11-11).
    fn reason(self) -> String {
        match self {
            Fault::Null(None) => NULL_VALUE.to_owned(),
            Fault::Null(Some(member)) => format!("{NULL_VALUE}: {member} is null"),
            Fault::Empty => EMPTY_VALUE.to_owned(),
            Fault::SeveralTypes => SEVERAL_TYPES.to_owned(),
            Fault::NotANumber("") => EMPTY_NUMBER.to_owned(),
            Fault::NotANumber(text) => format!("{EMPTY_NUMBER}: {text}"),
            Fault::Refused(msg) => msg,
        }
    }
}

/// The first fault in `value` (`member` is its name when it is the value of an
/// `M` member): a null or no type descriptor, then the numbers in `N`, `NS`
/// and every value an `L` or `M` holds, then more than one type descriptor.
/// `L` items are visited in document order and `M` members in key order: this
/// build of `serde_json` keeps an object's members sorted by key. A value of
/// another JSON type, and an `N` or `NS` payload of the wrong JSON type, are
/// shape faults the deserializer reports. A type descriptor with a null
/// payload counts as absent, as the service reads it (measured 2026-11-19:
/// `{"S": null}` answers the empty value text, and `{"S": null, "N": "1"}` in
/// a `FilterExpression` is accepted).
///
/// With several type descriptors, the service reads each payload in the order
/// of [`TYPE_DESCRIPTORS`], whatever order the request wrote them in, and
/// answers the more than one datatypes text only when no payload has a fault
/// (measured 2026-11-19: `{"N": "bob", "S": "x"}` and `{"S": "x", "N": "bob"}`
/// answer the numeric text for `bob`, `{"N": "1", "S": "x"}` the more than one
/// datatypes text, `{"S": "x", "L": [null]}` the null text, and `N` is read
/// ahead of `NS`, `NS` and `M` ahead of `L`).
fn first_fault<'a>(value: &'a Value, member: Option<&'a str>) -> Option<Fault<'a>> {
    let Value::Object(map) = value else {
        return value.is_null().then_some(Fault::Null(member));
    };
    let payloads: Vec<(&str, &Value)> = TYPE_DESCRIPTORS
        .iter()
        .filter_map(|&descriptor| Some((descriptor, map.get(descriptor).filter(|p| !p.is_null())?)))
        .collect();
    let fault = payloads
        .iter()
        .find_map(|&(descriptor, payload)| match (descriptor, payload) {
            ("N", Value::String(text)) => number_fault(text),
            ("NS", Value::Array(members)) => members
                .iter()
                .filter_map(Value::as_str)
                .find_map(number_fault),
            ("L", Value::Array(items)) => items.iter().find_map(|item| first_fault(item, None)),
            ("M", Value::Object(members)) => members
                .iter()
                .find_map(|(name, inner)| first_fault(inner, Some(name))),
            _ => None,
        });
    match payloads.len() {
        0 => Some(Fault::Empty),
        1 => fault,
        _ => fault.or(Some(Fault::SeveralTypes)),
    }
}

/// `text` against the number rules, with Unicode decimal digits folded for the
/// check; see [`crate::decimal_digits`].
fn number_fault(text: &str) -> Option<Fault<'_>> {
    let folded = crate::decimal_digits::fold_decimal_digits(text);
    match extenddb_core::validation::number::validate_and_normalize_number(&folded) {
        Ok(_) => None,
        Err(DynamoDbError::ValidationException(msg)) if msg.starts_with(EMPTY_NUMBER) => {
            Some(Fault::NotANumber(text))
        }
        Err(DynamoDbError::ValidationException(msg)) => Some(Fault::Refused(msg)),
        Err(other) => Some(Fault::Refused(other.to_string())),
    }
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
fn validate_expression_attribute_value_numbers(
    values: &Map<String, Value>,
) -> Result<(), DynamoDbError> {
    match sorted_entries(values)
        .into_iter()
        .find_map(|(_, value)| first_fault(value, None))
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
