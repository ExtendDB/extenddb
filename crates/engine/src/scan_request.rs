// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Scan` checks that run on the request body alone, before the body is
//! deserialized and before the table is looked up, in the service's order.
//!
//! Measured 2026-10-25, 2026-10-30 and 2026-10-31 (us-east-1, each request
//! twice):
//!
//! 1. the constraints the service aggregates into one message: the length of
//!    `AttributesToGet` (at least 1) and the ranges of `Limit` (at least 1),
//!    `Segment` (0 to 999999) and `TotalSegments` (1 to 1000000), each checked
//!    when present, whether or not the other segment field is, every fault in
//!    one message: `1 validation error detected: Value at 'Limit' failed to
//!    satisfy constraint: Member must have value greater than or equal to 1`,
//!    or `2 validation errors detected: ...; ...` in the order
//!    `AttributesToGet`, `Limit`, `Segment`, `TotalSegments`. `Limit: 0` with
//!    `AttributesToGet: []` answers the length clause and then the `Limit`
//!    clause; the empty list alone, with a `FilterExpression`, and with a
//!    `FilterExpression` and a number fault answers the length clause alone,
//!    so the length comes ahead of the number check. A float or a value
//!    outside int32 in any of the three number fields, or a value that is not
//!    one AttributeValue in `ExpressionAttributeValues`, in
//!    `ExclusiveStartKey` or in a `ScanFilter` `AttributeValueList`,
//!    switches the message to the older form, `Value '-2147483648' at 'limit'
//!    ...`, with the value saturated or truncated, in the order
//!    `totalSegments`, `segment`, `limit`; see `validate_constraints`;
//! 2. the expression and non-expression parameter mix, in which a `ScanFilter`
//!    object counts as present whether or not it is empty (`ScanFilter: {}`
//!    with a `FilterExpression` answers the mix, `ScanFilter: {}` alone is
//!    accepted); `AttributesToGet: []` with `ScanFilter: {}` and a
//!    `FilterExpression` answers the length clause, so the constraints come
//!    ahead of the mix;
//! 3. the `ReturnConsumedCapacity` enum;
//! 4. every number in `ExpressionAttributeValues`, `NS` members and values
//!    nested in `L` and `M` included, whether a `FilterExpression` reads the
//!    value, nothing reads it, or the request carries no expression at all; a
//!    Unicode decimal digit counts as the digit it stands for;
//! 5. the nesting depth of `ExpressionAttributeValues`: a value nested 32
//!    levels deep in `L` answers `Nesting Levels have exceeded supported
//!    limits: Attributes in the item have nested levels beyond supported
//!    limit`, 31 levels pass, and a malformed number anywhere is reported
//!    first. The check counts `M` levels the same way as `L` levels; only `L`
//!    was measured with a valid number at the bottom;
//! 6. the `Select` enum, after the nesting check (32 levels with `Select:
//!    "BAD"` answers the nesting text). Both enums invalid answers the
//!    `ReturnConsumedCapacity` error alone, so the two are not aggregated.
//!
//! The numbers in `ExclusiveStartKey` are checked by the handler at the key's
//! own stage, after the checks the service makes ahead of them (measured
//! 2026-10-31 between 23:54 and 23:56 UTC with `{"pk": {"N": "bob"}, "sk":
//! {"S": "x"}}` as the key): each of the six checks above, the `Select` rule
//! for `SPECIFIC_ATTRIBUTES`, the `FilterExpression` syntax and the
//! unused-value check answers its own text ahead of the number text, and a
//! malformed number in `ExpressionAttributeValues` is reported ahead of one in
//! the key. The number text is the value check's, with no filter and on an
//! index scan too (measured 2026-10-30). Inside the key a number fault
//! anywhere, an `NS` member or a value nested in the `L` of a non-key
//! attribute included, comes ahead of a value that is empty or has an unknown
//! type descriptor, in either member order; with two number faults the first
//! member is named (`{"pk": {"N": "bob"}, "sk": {"N": "fred"}}` names `bob`).
//! See [`StartKeyBody`].
//!
//! Not measured, and checked in the order listed above: the `AttributesToGet`
//! length clause beside a `Segment` or `TotalSegments` fault without a `Limit`
//! fault (listed first, as beside `Limit`), the older form of the length
//! clause (the empty list echoed, `Value '[]' at 'attributesToGet' ...`, and
//! listed last, as the older form reverses the newer one), the length clause
//! against the enums, `M` levels in the nesting count, and the nesting of a
//! value an expression reads or of a request with no expression. Not measured
//! at the key's stage: the key's checks against the table lookup, the index
//! resolution, the `ConsistentRead` and `Select` index rules and the `Segment`
//! pairing, which the handler makes ahead of them; a key whose only fault is
//! an empty or unknown-descriptor value against the filter syntax and
//! unused-value checks (the handler reports it after them, with the service's
//! prefix `The provided starting key is invalid: ` on the deserializer's
//! text, measured for the empty value alone); and which of two number faults
//! is named when
//! the request lists the members the other way round (the check reads members
//! in sorted order, which names `bob` either way).
//!
//! The JSON parser accepts 128 levels of nesting, which is 62 `L` levels; a
//! body nested deeper is a `SerializationException` from the parser, ahead of
//! every check here. The service answers the nesting text from 32 to 62 levels
//! and, from 63 to 100 levels, `ExpressionAttributeValues contains invalid
//! value: Nesting Levels have exceeded supported limits: Attributes in the
//! item have nested levels beyond supported limit for key :d` (measured
//! 2026-10-30), which this module does not reach.
//!
//! Each of these is reported ahead of the next one, ahead of the table lookup,
//! and ahead of the checks the handler makes after deserializing the body
//! (Segment without TotalSegments and the reverse, Segment not below
//! TotalSegments, the undefined placeholder, unused value, filter syntax,
//! IndexName, ConsistentRead, Select with ProjectionExpression and
//! ExclusiveStartKey checks). A field of the wrong JSON type anywhere in the
//! body is reported ahead of all of them, as a `SerializationException`.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, Item, ScanInput, saturated_int32};
use serde_json::Value;

use crate::query_key_values::{
    SCAN_CONDITIONS, has_unreadable_attribute_value, is_shape_fault, non_empty, present,
    shape_fault_first, validate_expression_attribute_value_numbers, validate_number_text,
};

/// The `Scan` checks that run on the raw request body, in the service's order:
/// the `AttributesToGet` length and the `Limit`, `Segment` and `TotalSegments`
/// ranges in one message, the expression and non-expression parameter mix, the
/// `ReturnConsumedCapacity` enum, every number in `ExpressionAttributeValues`,
/// the nesting depth of `ExpressionAttributeValues`, then the `Select` enum.
/// The numbers in `ExclusiveStartKey` are checked later, by [`StartKeyBody`].
///
/// The checks read the raw body, so a field that does not deserialize (an
/// invalid `Select`, for one) does not stop the ones before it. When a check
/// fails, the body is deserialized as a [`ScanInput`] and a
/// `SerializationException` from that (a field of the wrong JSON shape
/// anywhere in the body) is the answer instead, as the service reports a shape
/// fault ahead of every validation; see [`shape_fault_first`]. A body that
/// passes every check is not deserialized here.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text, or
/// `DynamoDbError::SerializationException` for a body of the wrong shape.
pub fn validate_scan_request_body(body: &Value) -> Result<(), DynamoDbError> {
    scan_request_checks(body).map_err(|check| shape_fault_first::<ScanInput>(body, check))
}

/// The `Scan` checks in the service's order; see
/// [`validate_scan_request_body`].
fn scan_request_checks(body: &Value) -> Result<(), DynamoDbError> {
    validate_constraints(body)?;

    extenddb_core::validation::validate_no_expression_param_mixing(
        &[
            ("AttributesToGet", non_empty(body, "AttributesToGet")),
            // An empty ScanFilter object counts as present, as a typed field
            // that deserialized to an empty map is.
            ("ScanFilter", present(body, "ScanFilter")),
            ("ConditionalOperator", present(body, "ConditionalOperator")),
        ],
        &[
            (
                "ProjectionExpression",
                present(body, "ProjectionExpression"),
            ),
            ("FilterExpression", present(body, "FilterExpression")),
        ],
    )?;

    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "ReturnConsumedCapacity",
            valid: &["INDEXES", "TOTAL", "NONE"],
            clause: crate::EnumClause::Named("returnConsumedCapacity"),
        }],
    )?;

    if let Some(values) = attribute_value_map(body, "ExpressionAttributeValues") {
        validate_expression_attribute_value_numbers(&values)?;
        extenddb_core::validation::validate_attribute_values_nesting_depth(values.values())?;
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

/// `field` of the body read as a map of attribute values, or `None` when the
/// field is absent or is not one; [`shape_fault_first`] reports the shape.
fn attribute_value_map(body: &Value, field: &str) -> Option<HashMap<String, AttributeValue>> {
    serde_json::from_value(body.get(field)?.clone()).ok()
}

/// The service's prefix on a fault in an `ExclusiveStartKey` value (measured
/// 2026-10-31 between 23:54 and 23:56 UTC: `{"pk": {}, "sk": {"S": "x"}}`
/// answers `The provided starting key is invalid: Supplied AttributeValue is
/// empty, must contain exactly one of the supported datatypes`).
const START_KEY_PREFIX: &str = "The provided starting key is invalid: ";

/// The `ExclusiveStartKey` of a `Scan` body as the request wrote it, with the
/// checks the service makes on the key at the key's own stage: after the
/// request-time checks of [`validate_scan_request_body`], the `Select` rule for
/// `SPECIFIC_ATTRIBUTES`, the `FilterExpression` syntax and the unused-value
/// check, and ahead of the key schema (measured 2026-10-31 between 23:54 and
/// 23:56 UTC; see the module doc).
///
/// A key whose value is empty, carries an unknown type descriptor or more than
/// one, or is a set that is empty or has duplicates does not deserialize as a
/// map of attribute values. [`StartKeyBody::take`] takes such a key out of the
/// body, so that the typed deserialization of the body does not report it
/// ahead of the checks the service makes first, and [`StartKeyBody::validate`]
/// reports it at the key's stage, after the key's numbers: `{"pk": {}, "sk":
/// {"N": "bob"}}` and `{"sk": {"N": "bob"}, "pk": {}}` answer the number text
/// for `bob`. A key of the wrong JSON shape (not an object, a value that is
/// not an object, an `N` that is not a string) stays in the body, as the
/// service reports a shape fault ahead of every check; see
/// [`shape_fault_first`].
pub(crate) struct StartKeyBody {
    /// The key as the request wrote it; `None` when the body has none.
    raw: Option<Value>,
    /// The deserializer's fault in a key taken out of the body, to report
    /// after the key's numbers. A `ValidationException` carries the service's
    /// prefix for the key (measured for an empty value; the texts for more
    /// than one descriptor and for an empty or duplicate set follow it,
    /// unmeasured). The `SerializationException` for an unknown type
    /// descriptor keeps the deserializer's text (unmeasured on its own; beside
    /// a `Limit` fault or a number fault the service reports those).
    deferred: Option<DynamoDbError>,
}

impl StartKeyBody {
    /// Reads the key from `body` and takes it out of the body when it does not
    /// deserialize for a reason that is not a shape fault.
    pub(crate) fn take(body: &mut Value) -> Self {
        let raw = body.get("ExclusiveStartKey").cloned();
        let deferred = match raw
            .as_ref()
            .map(|key| serde_json::from_value::<Item>(key.clone()))
        {
            Some(Err(e)) => match crate::deserialize_error(e) {
                shape if is_shape_fault(&shape) => None,
                DynamoDbError::ValidationException(msg) => Some(
                    DynamoDbError::ValidationException(format!("{START_KEY_PREFIX}{msg}")),
                ),
                other => Some(other),
            },
            Some(Ok(_)) | None => None,
        };
        if deferred.is_some()
            && let Some(fields) = body.as_object_mut()
        {
            fields.remove("ExclusiveStartKey");
        }
        Self { raw, deferred }
    }

    /// The key's checks at its own stage: every number in the key with the
    /// value check's rules and text (an `N`, the members of an `NS`, and the
    /// values nested in `L` and `M`, in the sorted order of the key's members,
    /// so that two faults name the same one on every backend), then the
    /// deserializer's fault in a key [`StartKeyBody::take`] took out of the
    /// body.
    ///
    /// # Errors
    ///
    /// Returns `DynamoDbError::ValidationException` with the service's text, or
    /// the deserializer's `SerializationException` for an unknown type
    /// descriptor.
    pub(crate) fn validate(&self) -> Result<(), DynamoDbError> {
        if let Some(members) = self.raw.as_ref().and_then(Value::as_object) {
            let mut names: Vec<&String> = members.keys().collect();
            names.sort();
            for name in names {
                validate_raw_value_numbers(&members[name])?;
            }
        }
        match &self.deferred {
            Some(fault) => Err(fault.clone()),
            None => Ok(()),
        }
    }
}

/// Every number in one raw attribute value: an `N` text, the members of an
/// `NS`, and the values nested in `L` and `M`. A value that is not one
/// attribute value, or whose inner JSON is not of the descriptor's shape, holds
/// no number to check here; the deserializer reports it.
fn validate_raw_value_numbers(value: &Value) -> Result<(), DynamoDbError> {
    let Some(fields) = value.as_object() else {
        return Ok(());
    };
    let mut fields = fields.iter();
    let (Some((descriptor, inner)), None) = (fields.next(), fields.next()) else {
        return Ok(());
    };
    match (descriptor.as_str(), inner) {
        ("N", Value::String(text)) => validate_number_text(text),
        ("NS", Value::Array(texts)) => texts
            .iter()
            .filter_map(Value::as_str)
            .try_for_each(validate_number_text),
        ("L", Value::Array(members)) => members.iter().try_for_each(validate_raw_value_numbers),
        ("M", Value::Object(members)) => members.values().try_for_each(validate_raw_value_numbers),
        _ => Ok(()),
    }
}

/// The constraints the service aggregates into one message: the length of
/// `AttributesToGet` and the ranges of `Limit`, `Segment` and `TotalSegments`,
/// every fault in one message, in one of the service's two forms (measured
/// 2026-10-25 and 2026-10-30, us-east-1).
///
/// When each number field present is an integer inside the int32 range, the
/// newer form: `Value at '<Field>' failed to satisfy constraint: ...` with the
/// field capitalised and no value echoed, in the order `AttributesToGet`,
/// `Limit`, `Segment`, `TotalSegments` (`Limit: 0` with `AttributesToGet: []`
/// answers the length clause, then the `Limit` clause, measured 2026-10-30
/// 12:40 UTC; the length clause beside a `Segment` or `TotalSegments` fault
/// alone is not measured). When any of the three is a float or outside int32,
/// or a value in `ExpressionAttributeValues`, `ExclusiveStartKey` or a
/// `ScanFilter` `AttributeValueList` is not one AttributeValue (see
/// [`has_unreadable_attribute_value`]), the older form: `Value '<n>' at
/// '<field>' ...` with the value as [`saturated_int32`] reads it and the field
/// in lower camel case, in the reverse of the newer form's order:
/// `totalSegments`, `segment`, `limit` (measured 2026-10-30 12:40 UTC with a
/// fault in each of the three, and on 2026-10-25 with `segment` then `limit`
/// and `totalSegments` then `limit`). The older form of the length clause is
/// not measured: the code echoes the empty list, `Value '[]' at
/// 'attributesToGet' ...`, and lists it last.
fn validate_constraints(body: &Value) -> Result<(), DynamoDbError> {
    let numbers: Vec<(&RangeField, &serde_json::Number)> = RANGE_FIELDS
        .iter()
        .filter_map(|range| {
            body.get(range.field)
                .and_then(Value::as_number)
                .map(|number| (range, number))
        })
        .collect();
    let older_form = has_unreadable_attribute_value(body, SCAN_CONDITIONS)
        || numbers.iter().any(|(_, number)| {
            number
                .as_i64()
                .is_none_or(|value| i32::try_from(value).is_err())
        });
    // Each fault as its newer-form and its older-form clause, in the newer
    // form's order.
    let mut faults: Vec<(String, String)> = Vec::new();
    if body
        .get("AttributesToGet")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        faults.push((
            "Value at 'AttributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned(),
            "Value '[]' at 'attributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned(),
        ));
    }
    for (range, number) in numbers {
        let value = saturated_int32(number);
        let constraint = if value < range.min {
            format!("greater than or equal to {}", range.min)
        } else if let Some(max) = range.max.filter(|max| value > *max) {
            format!("less than or equal to {max}")
        } else {
            continue;
        };
        faults.push((
            format!(
                "Value at '{}' failed to satisfy constraint: Member must have value {constraint}",
                range.field
            ),
            format!(
                "Value '{value}' at '{}' failed to satisfy constraint: Member must have value {constraint}",
                range.older_field
            ),
        ));
    }
    if faults.is_empty() {
        return Ok(());
    }
    let clauses: Vec<&str> = if older_form {
        faults
            .iter()
            .rev()
            .map(|(_, older)| older.as_str())
            .collect()
    } else {
        faults.iter().map(|(newer, _)| newer.as_str()).collect()
    };
    let count = clauses.len();
    Err(DynamoDbError::ValidationException(format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        clauses.join("; ")
    )))
}

/// One range-checked `Scan` field: its name in the newer and the older form of
/// the fault, and its bounds.
struct RangeField {
    field: &'static str,
    older_field: &'static str,
    min: i32,
    max: Option<i32>,
}

/// The range-checked fields in the order the newer form reports them.
const RANGE_FIELDS: [RangeField; 3] = [
    RangeField {
        field: "Limit",
        older_field: "limit",
        min: 1,
        max: None,
    },
    RangeField {
        field: "Segment",
        older_field: "segment",
        min: 0,
        max: Some(999_999),
    },
    RangeField {
        field: "TotalSegments",
        older_field: "totalSegments",
        min: 1,
        max: Some(1_000_000),
    },
];

#[cfg(test)]
mod range_tests;
#[cfg(test)]
mod request_tests;
#[cfg(test)]
mod tests;
