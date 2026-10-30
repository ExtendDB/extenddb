// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Scan` checks that run on the request body alone, before the body is
//! deserialized and before the table is looked up, in the service's order.
//!
//! Measured 2026-10-25 and 2026-10-30 (us-east-1, each request twice):
//!
//! 1. the range constraints on `Limit` (at least 1), `Segment` (0 to 999999)
//!    and `TotalSegments` (1 to 1000000), each checked when present, whether or
//!    not the other segment field is, every fault in one message: `1
//!    validation error detected: Value at 'Limit' failed to satisfy
//!    constraint: Member must have value greater than or equal to 1`, or `2
//!    validation errors detected: ...; ...` in the order `Limit`, `Segment`,
//!    `TotalSegments`. A float or a value outside int32 in any of the three,
//!    or a value that is not one AttributeValue in `ExpressionAttributeValues`,
//!    in `ExclusiveStartKey` or in a `ScanFilter` `AttributeValueList`,
//!    switches the message to the older form, `Value '-2147483648' at 'limit'
//!    ...`, with the value saturated or truncated, in the order
//!    `totalSegments`, `segment`, `limit`; see `validate_ranges`;
//! 2. the length constraint on `AttributesToGet`: an empty list answers `1
//!    validation error detected: Value at 'AttributesToGet' failed to satisfy
//!    constraint: Member must have length greater than or equal to 1`, alone,
//!    with a `FilterExpression`, and with a `FilterExpression` and a number
//!    fault, so it comes ahead of the number check;
//! 3. the expression and non-expression parameter mix, in which a `ScanFilter`
//!    object counts as present whether or not it is empty (`ScanFilter: {}`
//!    with a `FilterExpression` answers the mix, `ScanFilter: {}` alone is
//!    accepted);
//! 4. the `ReturnConsumedCapacity` enum;
//! 5. every number in `ExpressionAttributeValues`, `NS` members and values
//!    nested in `L` and `M` included, whether a `FilterExpression` reads the
//!    value, nothing reads it, or the request carries no expression at all; a
//!    Unicode decimal digit counts as the digit it stands for; then every
//!    number in the `ExclusiveStartKey` values, which answers the same text as
//!    a malformed number in a value;
//! 6. the nesting depth of `ExpressionAttributeValues`: a value nested 32
//!    levels deep in `L` answers `Nesting Levels have exceeded supported
//!    limits: Attributes in the item have nested levels beyond supported
//!    limit`, 31 levels pass, and a malformed number anywhere is reported
//!    first. The check counts `M` levels the same way as `L` levels; only `L`
//!    was measured with a valid number at the bottom;
//! 7. the `Select` enum. Both enums invalid answers the `ReturnConsumedCapacity`
//!    error alone, so the two are not aggregated.
//!
//! Not measured, and checked in the order listed above: the `AttributesToGet`
//! length against the range constraints and against the enums, the
//! `ExclusiveStartKey` number check against the `ExpressionAttributeValues`
//! one, and the nesting check against the `Select` enum. A body nested deeper
//! than the JSON parser accepts is a `SerializationException` from the parser,
//! ahead of every check here.
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
use extenddb_core::types::{AttributeValue, ScanInput, saturated_int32};
use serde_json::Value;

use crate::query_key_values::{
    SCAN_CONDITIONS, has_unreadable_attribute_value, non_empty, present, shape_fault_first,
    validate_expression_attribute_value_numbers,
};

/// The `Scan` checks that run on the raw request body, in the service's order:
/// the `Limit`, `Segment` and `TotalSegments` ranges, the `AttributesToGet`
/// length, the expression and non-expression parameter mix, the
/// `ReturnConsumedCapacity` enum, every number in `ExpressionAttributeValues`
/// and `ExclusiveStartKey`, the nesting depth of `ExpressionAttributeValues`,
/// then the `Select` enum.
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
    validate_ranges(body)?;
    validate_attributes_to_get_length(body)?;

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

    let values = attribute_value_map(body, "ExpressionAttributeValues");
    if let Some(values) = &values {
        validate_expression_attribute_value_numbers(values)?;
    }
    // The key's values are numbers checked with the same rules and text.
    if let Some(key) = attribute_value_map(body, "ExclusiveStartKey") {
        validate_expression_attribute_value_numbers(&key)?;
    }
    if let Some(values) = &values {
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

/// The length constraint on `AttributesToGet`: an empty list is refused in the
/// newer form of the constraint text, with no value echoed (measured
/// 2026-10-25 10:38 and 10:55 UTC: alone, with a `FilterExpression`, and with
/// a `FilterExpression` and a malformed number).
fn validate_attributes_to_get_length(body: &Value) -> Result<(), DynamoDbError> {
    if body
        .get("AttributesToGet")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        return Err(DynamoDbError::ValidationException(
            "1 validation error detected: Value at 'AttributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned(),
        ));
    }
    Ok(())
}

/// `field` of the body read as a map of attribute values, or `None` when the
/// field is absent or is not one; [`shape_fault_first`] reports the shape.
fn attribute_value_map(body: &Value, field: &str) -> Option<HashMap<String, AttributeValue>> {
    serde_json::from_value(body.get(field)?.clone()).ok()
}

/// The range constraints on `Limit`, `Segment` and `TotalSegments`, every
/// fault in one message, in one of the service's two forms (measured
/// 2026-10-25, us-east-1).
///
/// When each field present is an integer inside the int32 range, the newer
/// form: `Value at '<Field>' failed to satisfy constraint: ...` with the field
/// capitalised and no value echoed, in the order `Limit`, `Segment`,
/// `TotalSegments`. When any of the three is a float or outside int32, or a
/// value in `ExpressionAttributeValues`, `ExclusiveStartKey` or a `ScanFilter`
/// `AttributeValueList` is not one AttributeValue (see
/// [`has_unreadable_attribute_value`]), the older form: `Value '<n>' at
/// '<field>' ...` with the value as [`saturated_int32`] reads it and the field
/// in lower camel case, in the reverse of the newer form's order:
/// `totalSegments`, `segment`, `limit` (measured 2026-10-30 12:40 UTC with a
/// fault in each of the three, and on 2026-10-25 with `segment` then `limit`
/// and `totalSegments` then `limit`).
fn validate_ranges(body: &Value) -> Result<(), DynamoDbError> {
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
    let faults: Vec<(&RangeField, i32, String)> = numbers
        .into_iter()
        .filter_map(|(range, number)| {
            let value = saturated_int32(number);
            if value < range.min {
                Some((
                    range,
                    value,
                    format!("greater than or equal to {}", range.min),
                ))
            } else {
                range
                    .max
                    .filter(|max| value > *max)
                    .map(|max| (range, value, format!("less than or equal to {max}")))
            }
        })
        .collect();
    if faults.is_empty() {
        return Ok(());
    }
    let clauses: Vec<String> = if older_form {
        faults
            .iter()
            .rev()
            .map(|(range, value, constraint)| {
                format!(
                    "Value '{value}' at '{}' failed to satisfy constraint: Member must have value {constraint}",
                    range.older_field
                )
            })
            .collect()
    } else {
        faults
            .iter()
            .map(|(range, _, constraint)| {
                format!(
                    "Value at '{}' failed to satisfy constraint: Member must have value {constraint}",
                    range.field
                )
            })
            .collect()
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
