// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checks `handle_query` makes on a request before it reads the table.

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::QueryInput;

use super::{check_request_parameters, parse_filter_expression};
use crate::expression_helpers::build_expression_maps;

/// A `Query` request read from its body, with its expressions parsed.
pub(super) struct CheckedRequest {
    pub(super) input: QueryInput,
    pub(super) maps: ExpressionMaps,
    pub(super) filter_expression: Option<Expr>,
    pub(super) key_condition_expression: Option<KeyCondition>,
}

/// Read a `Query` body and refuse its faults that come before the table is
/// read, in the order the service reports them.
pub(super) fn check_request(
    mut body: Value,
    limits: &LimitsConfig,
) -> Result<CheckedRequest, DynamoDbError> {
    // Pre-scanned before typed deserialization, sequentially: Query stops at
    // the FIRST invalid enum, checking ReturnConsumedCapacity ahead of Select.
    // Measured 2026-08-24 (us-east-1): both invalid answers "1 validation
    // error detected" naming returnConsumedCapacity alone. (Scan is the
    // opposite on both counts: it aggregates, select-first; see handle_scan.)
    crate::validate_enum_fields(
        &body,
        &[crate::EnumField {
            json_name: "ReturnConsumedCapacity",
            valid: &["INDEXES", "TOTAL", "NONE"],
            clause: crate::EnumClause::Named("returnConsumedCapacity"),
        }],
    )?;
    // The service checks Select after Limit, the request's parameters, the
    // FilterExpression and the KeyConditionExpression (measured 2026-11-11),
    // so an invalid Select is left out of the typed input and its error waits
    // for those checks below.
    let select_check = crate::validate_enum_fields(
        &body,
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
    );
    if select_check.is_err()
        && let Some(fields) = body.as_object_mut()
    {
        fields.remove("Select");
    }
    // An invalid Select still comes ahead of a body that does not
    // deserialize, as it did when it was checked before deserialization.
    let input: QueryInput = match serde_json::from_value(body) {
        Ok(input) => input,
        Err(err) => {
            select_check?;
            return Err(crate::deserialize_error(err));
        }
    };

    // Validate Limit >= 1 (REQ-QUERY-001). The service checks Limit ahead of
    // every other fault measured (2026-11-11, us-east-1).
    if let Some(limit) = input.limit
        && limit < 1
    {
        return Err(DynamoDbError::ValidationException(
                "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1".to_owned(),
            ));
    }

    // Build expression maps from request (used for expression-based parameters)
    let maps = build_expression_maps(
        input.expression_attribute_names.as_ref(),
        input.expression_attribute_values.as_ref(),
    );

    // The service checks a Query in this order (measured 2026-11-11 in
    // us-east-1): Limit, then the request's parameters, then every
    // FilterExpression fault, then every KeyConditionExpression fault, then
    // Select, the table, the index and ConsistentRead.
    check_request_parameters(&input)?;
    let filter_expression = parse_filter_expression(&input, &maps, limits)?;
    let key_condition_expression = input
        .key_condition_expression
        .as_deref()
        .map(|kce| crate::expression_helpers::parse_key_condition_expr(kce, &maps, limits))
        .transpose()?;
    select_check?;
    Ok(CheckedRequest {
        input,
        maps,
        filter_expression,
        key_condition_expression,
    })
}
