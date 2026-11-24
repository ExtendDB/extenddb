// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Scan` checks that run on the request body alone, before the body is
//! deserialized and before the table is looked up.
//!
//! Measured 2026-10-25, 2026-10-31, 2026-11-19 and 2026-11-23 (us-east-1,
//! each request twice), for a body whose values the service can read and
//! whose `Limit`, `Segment` and `TotalSegments` are integers inside int32,
//! the service answers in this order:
//!
//! 1. the range constraints on `Limit` (at least 1), `Segment` (0 to 999999)
//!    and `TotalSegments` (1 to 1000000), each checked when present, whether or
//!    not the other segment field is, every fault in one message, the field
//!    capitalised and no value echoed: `1 validation error detected: Value at
//!    'Limit' failed to satisfy constraint: Member must have value greater
//!    than or equal to 1`, or `3 validation errors detected: Value at 'Limit'
//!    ...; Value at 'Segment' ...; Value at 'TotalSegments' ...` in that
//!    order; see `int32_member::ranges`;
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
//! A body on the service's older validation path takes another order: one
//! whose `ExpressionAttributeValues`, `ExclusiveStartKey` or `ScanFilter`
//! `AttributeValueList` holds a value the service cannot read as one type (a
//! null, no type descriptor, an unknown one, more than one, a null payload),
//! or whose `Limit`, `Segment` or `TotalSegments` is a float or an integer
//! outside int32. An invalid `Select`, the `TotalSegments` fault, an invalid
//! `ReturnConsumedCapacity`, the `Segment` fault and the `Limit` fault are
//! reported together in one message, in that order, each value echoed as the
//! service reads it (a fraction truncated toward zero, a value outside int32
//! saturated: `Value '-2147483648' at 'limit' ...`), then the mix, the names
//! and the values, an empty or duplicate set among them; see
//! `older_form_checks`.
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
use extenddb_core::types::ScanInput;
use serde_json::{Map, Value};

use crate::int32_member::{SCAN_RANGES, newer_form, ranges};
use crate::query_key_values::{
    RETURN_CONSUMED_CAPACITY, SCAN_CONDITIONS, SCAN_VALUE_EXPRESSIONS, SELECT, deserializer_fault,
    has_unreadable, non_empty, older_form_checks, present, readable_for_shape_check,
    shape_fault_first, sorted_entries, validate_expression_attribute_value_numbers, validate_names,
};

/// The `Scan` checks that run on the raw request body, in the order the
/// module documentation gives.
///
/// The checks read the raw body, so a field that does not deserialize (an
/// invalid `Select`, for one) does not stop the ones before it. When a check
/// fails, each member of the body is deserialized on its own as part of a
/// [`ScanInput`], and a `SerializationException` from that is the answer
/// instead, as the service reports a shape fault ahead of every validation;
/// see `shape_fault_first`. For that read the parts of a value that the
/// service cannot read are replaced (see `readable_for_shape_check`), so a
/// null value is not a shape fault. A body that passes every check is not
/// deserialized here; the handler reads `Limit`, `Segment` and
/// `TotalSegments` with `int32_member::saturate` before it deserializes.
///
/// The server runs the same function ahead of the IAM decision.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text, or
/// `DynamoDbError::SerializationException` for a body of the wrong shape.
pub fn validate_scan_request_body(body: &Value) -> Result<(), DynamoDbError> {
    scan_request_checks(body).map_err(|check| {
        shape_fault_first::<ScanInput>(&readable_for_shape_check(body, SCAN_CONDITIONS), check)
    })
}

/// The `Scan` checks in the service's order; see
/// [`validate_scan_request_body`].
fn scan_request_checks(body: &Value) -> Result<(), DynamoDbError> {
    let ranges = ranges(body, &SCAN_RANGES);
    let values = body
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object);
    if ranges.older_form || has_unreadable(body, SCAN_CONDITIONS) {
        return older_form_checks(
            body,
            values,
            &ranges.faults,
            validate_parameter_mix,
            SCAN_VALUE_EXPRESSIONS,
        );
    }
    if !ranges.faults.is_empty() {
        return Err(newer_form(&ranges.faults));
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
/// member, with the deserializer's text (see `deserializer_fault`). The
/// values are visited in placeholder order. The service reports these after
/// the `ExpressionAttributeNames` checks and ahead of the `Select` enum
/// (measured 2026-11-19 and 2026-11-23: a duplicate or an empty `NS`, a
/// duplicate `SS` or `BS`, and a duplicate `NS` in an `L` beside `Select:
/// "NOPE"` answer the set text). Beside a number fault in another value, the
/// service named either value between two identical requests (measured
/// 2026-11-19), and this check comes after the numbers. A value of the wrong
/// JSON type is left to the shape fault check. On the older validation path
/// the sets are checked with the values, with that path's texts (see
/// `older_form_checks`).
fn validate_sets(values: &Map<String, Value>) -> Result<(), DynamoDbError> {
    match sorted_entries(values)
        .into_iter()
        .find_map(|(_, value)| deserializer_fault(value))
    {
        Some(fault) => Err(fault),
        None => Ok(()),
    }
}

#[cfg(test)]
mod range_tests;
#[cfg(test)]
mod tests;
