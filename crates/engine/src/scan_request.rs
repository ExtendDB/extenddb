// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Scan` checks that run on the request body alone, before the body is
//! deserialized and before the table is looked up, in the service's order.
//!
//! Measured 2026-10-25 (us-east-1, each request twice):
//!
//! 1. the range constraints on `Limit` (at least 1), `Segment` (0 to 999999)
//!    and `TotalSegments` (1 to 1000000), each checked when present, whether or
//!    not the other segment field is. A `Limit` below 1 inside the int32 range
//!    answers `1 validation error detected: Value at 'Limit' failed to satisfy
//!    constraint: Member must have value greater than or equal to 1`; below
//!    int32 the service saturates the value and answers the older form
//!    `Value '-2147483648' at 'limit' ...`. The service aggregates several
//!    range faults into one message; this module reports the first in the
//!    order `Limit`, `Segment`, `TotalSegments`, with the `Segment` and
//!    `TotalSegments` texts the previous release used;
//! 2. the expression and non-expression parameter mix;
//! 3. the `ReturnConsumedCapacity` enum;
//! 4. every number in `ExpressionAttributeValues`, `NS` members and values
//!    nested in `L` and `M` included, whether a `FilterExpression` reads the
//!    value, nothing reads it, or the request carries no expression at all; a
//!    Unicode decimal digit counts as the digit it stands for;
//! 5. the `Select` enum. Both enums invalid answers the `ReturnConsumedCapacity`
//!    error alone, so the two are not aggregated.
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
use extenddb_core::types::{AttributeValue, ScanInput};
use serde_json::Value;

use crate::query_key_values::{
    non_empty, present, shape_fault_first, validate_expression_attribute_value_numbers,
};

/// The `Scan` checks that run on the raw request body, in the service's order:
/// the `Limit`, `Segment` and `TotalSegments` ranges, the expression and
/// non-expression parameter mix, the `ReturnConsumedCapacity` enum, every
/// number in `ExpressionAttributeValues`, then the `Select` enum.
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
    )?;

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

/// The range constraints on `Limit`, `Segment` and `TotalSegments`, the first
/// fault in that order.
///
/// `Limit` answers the service's text: inside the int32 range the field is
/// capitalised and the value is not echoed; below int32 the service saturates
/// the value and uses the older `Value '<n>' at 'limit'` form (measured
/// 2026-10-25, `Limit: -2147483649`). `Segment` and `TotalSegments` keep the
/// texts of the previous release.
fn validate_ranges(body: &Value) -> Result<(), DynamoDbError> {
    let get = |field: &str| body.get(field).and_then(Value::as_i64);
    if let Some(limit) = get("Limit")
        && limit < 1
    {
        let text = if limit >= i64::from(i32::MIN) {
            "Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1".to_owned()
        } else {
            format!(
                "Value '{}' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1",
                i32::MIN
            )
        };
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: {text}"
        )));
    }
    if let Some(segment) = get("Segment") {
        if segment < 0 {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '{segment}' at 'segment' failed to satisfy constraint: Member must have value greater than or equal to 0"
            )));
        }
        if segment > 999_999 {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '{segment}' at 'segment' failed to satisfy constraint: Member must have value less than or equal to 999999"
            )));
        }
    }
    if let Some(total) = get("TotalSegments") {
        if total < 1 {
            return Err(DynamoDbError::ValidationException(
                "The parameter TotalSegments should be greater than or equal to 1".to_owned(),
            ));
        }
        if total > 1_000_000 {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '{total}' at 'totalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
