// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Scan` checks that run on the request body alone, before the body is
//! deserialized and before the table is looked up, in the service's order.
//!
//! Measured 2026-10-25 (us-east-1, each request twice):
//!
//! 1. the range constraints on `Limit` (at least 1), `Segment` (0 to 999999)
//!    and `TotalSegments` (1 to 1000000), aggregated into one message in that
//!    order with no value echoed: `2 validation errors detected: Value at
//!    'Limit' failed to satisfy constraint: Member must have value greater than
//!    or equal to 1; Value at 'Segment' ...`. Each field is checked when
//!    present, whether or not the other segment field is;
//! 2. the expression and non-expression parameter mix;
//! 3. the `ReturnConsumedCapacity` enum;
//! 4. every number in `ExpressionAttributeValues`, `NS` members and values
//!    nested in `L` and `M` included, whether a `FilterExpression` reads the
//!    value, nothing reads it, or the request carries no expression at all;
//! 5. the `Select` enum. Both enums invalid answers the `ReturnConsumedCapacity`
//!    error alone, so the two are not aggregated.
//!
//! Each of these is reported ahead of the next one, ahead of the table lookup,
//! and ahead of the checks the handler makes after deserializing the body
//! (Segment without TotalSegments and the reverse, Segment not below
//! TotalSegments, the undefined placeholder, unused value, filter syntax,
//! IndexName, ConsistentRead, Select with ProjectionExpression and
//! ExclusiveStartKey checks).

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::AttributeValue;
use serde_json::Value;

use crate::query_key_values::{non_empty, present, validate_expression_attribute_value_numbers};

/// The `Scan` checks that run on the raw request body, in the service's order:
/// the `Limit`, `Segment` and `TotalSegments` ranges, the expression and
/// non-expression parameter mix, the `ReturnConsumedCapacity` enum, every
/// number in `ExpressionAttributeValues`, then the `Select` enum.
///
/// The checks read the raw body, so a field that does not deserialize (an
/// invalid `Select`, for one) does not stop the ones before it. A field of the
/// wrong JSON shape is left to the deserializer, which runs next.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn validate_scan_request_body(body: &Value) -> Result<(), DynamoDbError> {
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

/// The range constraints on `Limit`, `Segment` and `TotalSegments`, aggregated
/// in that order as the service reports them. The field name is capitalised
/// and the value is not echoed, unlike the `select` and `segment` style the
/// other `1 validation error detected` messages use.
fn validate_ranges(body: &Value) -> Result<(), DynamoDbError> {
    let mut errors: Vec<String> = Vec::new();
    let mut check = |field: &str, below: i64, above: i64| {
        let Some(value) = body.get(field).and_then(Value::as_i64) else {
            return;
        };
        if value < below {
            errors.push(format!(
                "Value at '{field}' failed to satisfy constraint: Member must have value greater than or equal to {below}"
            ));
        } else if value > above {
            errors.push(format!(
                "Value at '{field}' failed to satisfy constraint: Member must have value less than or equal to {above}"
            ));
        }
    };
    check("Limit", 1, i64::MAX);
    check("Segment", 0, 999_999);
    check("TotalSegments", 1, 1_000_000);
    if errors.is_empty() {
        return Ok(());
    }
    let count = errors.len();
    Err(DynamoDbError::ValidationException(format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        errors.join("; ")
    )))
}

#[cfg(test)]
mod tests;
