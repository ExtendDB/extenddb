// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Scan` checks that run on the request body alone, before the body is
//! deserialized and before the table is looked up.
//!
//! Measured 2026-10-25, 2026-11-19 and 2026-11-23 (us-east-1, each request
//! twice), for a body whose `ExpressionAttributeValues` the service can read,
//! the service answers in this order:
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
//! 2. the `ReturnConsumedCapacity` enum;
//! 3. the expression and non-expression parameter mix;
//! 4. the `ExpressionAttributeNames` checks (see `validate_names`);
//! 5. every number in `ExpressionAttributeValues`, `NS` members and values
//!    nested in `L` and `M` included, whether a `FilterExpression` reads the
//!    value, nothing reads it, or the request carries no expression at all; a
//!    Unicode decimal digit counts as the digit it stands for;
//! 6. an empty set and a set with a duplicate member (see `validate_sets`);
//! 7. the `Select` enum. Both enums invalid answers the
//!    `ReturnConsumedCapacity` error alone.
//!
//! A body whose `ExpressionAttributeValues` holds a value the service cannot
//! read as one type (a null, no type descriptor, an unknown one, more than
//! one, a null payload) takes another order on the service. An invalid
//! `Select` and `ReturnConsumedCapacity` are reported together, with the
//! range faults in an older form, then the mix, the names and the values.
//! Here the range checks above come first, then `unreadable_value_checks`.
//!
//! On the service the undefined placeholder, unused value and filter syntax
//! checks also come ahead of the `Select` enum and the table lookup (measured
//! 2026-11-19 and 2026-11-23: `{"Select": "bad", "FilterExpression": "a =
//! :undef"}` answers the undefined placeholder text). Here those checks stay
//! in the handler, after the table lookup, beside the `Segment` pairing,
//! `IndexName`, `ConsistentRead`, `Select` with `ProjectionExpression` and
//! `ExclusiveStartKey` checks.
//!
//! A member of the wrong JSON type is reported ahead of all of these, as a
//! `SerializationException`; see `shape_fault_first`.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, ScanInput, saturated_int32};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::query_key_values::{
    RETURN_CONSUMED_CAPACITY, SCAN_CONDITIONS, SELECT, has_unreadable_attribute_value, non_empty,
    present, readable_for_shape_check, shape_fault_first, sorted_entries, unreadable,
    unreadable_value_checks, validate_expression_attribute_value_numbers, validate_names,
};

/// The `Scan` checks that run on the raw request body, in the order the
/// module documentation gives.
///
/// The checks read the raw body, so a field that does not deserialize (an
/// invalid `Select`, for one) does not stop the ones before it. When a check
/// fails, each member of the body is deserialized on its own as part of a
/// [`ScanInput`], and a `SerializationException` from that is the answer
/// instead, as the service reports a shape fault ahead of every validation;
/// see `shape_fault_first`. For that read the parts of an
/// `ExpressionAttributeValues` value that the service cannot read are
/// replaced (see `readable_for_shape_check`), so a null value is not a shape
/// fault. A body that passes every check is not deserialized here.
///
/// The server runs the same function ahead of the IAM decision.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text, or
/// `DynamoDbError::SerializationException` for a body of the wrong shape.
pub fn validate_scan_request_body(body: &Value) -> Result<(), DynamoDbError> {
    scan_request_checks(body)
        .map_err(|check| shape_fault_first::<ScanInput>(&readable_for_shape_check(body), check))
}

/// The `Scan` checks in the service's order; see
/// [`validate_scan_request_body`].
fn scan_request_checks(body: &Value) -> Result<(), DynamoDbError> {
    validate_ranges(body)?;

    let values = body
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object);
    if let Some(values) = values
        && values.values().any(unreadable)
    {
        return unreadable_value_checks(body, values, validate_parameter_mix);
    }

    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "ReturnConsumedCapacity",
            valid: RETURN_CONSUMED_CAPACITY,
            clause: crate::EnumClause::Named("returnConsumedCapacity"),
        }],
    )?;

    validate_parameter_mix(body)?;
    validate_names(body)?;

    if let Some(values) = values {
        validate_expression_attribute_value_numbers(values)?;
        validate_sets(values)?;
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
fn validate_parameter_mix(body: &Value) -> Result<(), DynamoDbError> {
    extenddb_core::validation::validate_no_expression_param_mixing(
        &[
            ("AttributesToGet", non_empty(body, "AttributesToGet")),
            ("ScanFilter", non_empty(body, "ScanFilter")),
            ("ConditionalOperator", present(body, "ConditionalOperator")),
        ],
        &[
            (
                "ProjectionExpression",
                present(body, "ProjectionExpression"),
            ),
            ("FilterExpression", present(body, "FilterExpression")),
        ],
    )
}

/// The faults the `AttributeValue` deserializer reports as a
/// `ValidationException`, such as an empty set and a set with a duplicate
/// member, with the deserializer's text. The values are visited in
/// placeholder order. The service reports these after the
/// `ExpressionAttributeNames` checks and ahead of the `Select` enum (measured
/// 2026-11-19 and 2026-11-23: a duplicate or an empty `NS`, a duplicate `SS`
/// or `BS`, and a duplicate `NS` in an `L` beside `Select: "NOPE"` answer the
/// set text). Beside a number fault in another value, the service named
/// either value between two identical requests (measured 2026-11-19), and
/// this check comes after the numbers. A value of the wrong JSON type is left
/// to the shape fault check.
fn validate_sets(values: &Map<String, Value>) -> Result<(), DynamoDbError> {
    match sorted_entries(values).into_iter().find_map(|(_, value)| {
        match AttributeValue::deserialize(value).map_err(crate::deserialize_error) {
            Err(fault @ DynamoDbError::ValidationException(_)) => Some(fault),
            _ => None,
        }
    }) {
        Some(fault) => Err(fault),
        None => Ok(()),
    }
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
mod tests;
