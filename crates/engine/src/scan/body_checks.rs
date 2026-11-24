// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checks of a `Scan` body ahead of the read, in the service's order:
//! the handler runs them first, and the server runs them again ahead of the
//! IAM decision (see `scan_request::validate_before_authorization`).

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{Expr, ExpressionKind, ExpressionMaps};
use extenddb_core::types::ScanInput;

use crate::legacy_filter::desugar_filter;
use crate::scan_request::{
    CheckOrder, ExpressionValuesBody, ScanFilterBody, SelectBody, StartKeyBody, member_orders,
};

/// The parts of a `Scan` body that [`checked_body`] hands to the checks and
/// the read after it.
pub(crate) struct CheckedBody {
    pub(super) order: CheckOrder,
    pub(super) input: ScanInput,
    pub(super) has_proj_expr: bool,
    pub(super) legacy_filter: Option<(Expr, ExpressionMaps)>,
    pub(super) start_key: StartKeyBody,
}

/// The checks of a `Scan` body in the order of [`super::handle_scan`], up to the
/// `Select` enum in order R and up to the faulty `ExpressionAttributeValues`
/// entries and an empty name in order U. `raw_body` is the request text
/// `body` was parsed from (see `scan_request::member_orders`).
///
/// The service answers an invalid `Select` (order R) and an entry with an
/// unknown type descriptor (order U) with its `ValidationException` to a
/// principal that may not Scan the table (measured 2026-11-19 with a role
/// allowed only `DescribeTable`). It makes every other check here first, so
/// the server runs them all ahead of the IAM decision; see
/// `scan_request::validate_before_authorization`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text, or
/// `DynamoDbError::SerializationException` for a body of the wrong shape.
pub(crate) fn checked_body(mut body: Value, raw_body: &[u8]) -> Result<CheckedBody, DynamoDbError> {
    let order = crate::scan_request::request_checks(&body)?;
    // Limit, Segment and TotalSegments are read as the service reads them,
    // from any JSON number the body parser reads; see `int32_member`.
    crate::int32_member::saturate(&mut body, &crate::int32_member::SCAN_RANGES)?;
    // A key, an ExpressionAttributeValues entry or empty name and a ScanFilter
    // list value (order U), or a Select value (order R), that the service
    // reports later is held back here, so that the typed deserialization does
    // not report it ahead of the checks the service makes first.
    let orders = member_orders(raw_body);
    let start_key = StartKeyBody::take(&mut body, orders.key.as_deref());
    let expression_values = ExpressionValuesBody::take(&mut body, order, orders.values.as_deref());
    let scan_filter_values = ScanFilterBody::take(&mut body, order, orders.filter.as_deref());
    let select = SelectBody::take(&mut body, order);
    let input: ScanInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    let has_cond_op = input.conditional_operator.is_some();

    let has_filter_expr = input
        .filter_expression
        .as_ref()
        .is_some_and(|s| !s.is_empty());
    let has_proj_expr = input
        .projection_expression
        .as_ref()
        .is_some_and(|s| !s.is_empty());
    extenddb_core::expression::validate_expression_param_usage(
        input.expression_attribute_names.as_ref(),
        has_proj_expr || has_filter_expr,
        input.expression_attribute_values.as_ref(),
        has_filter_expr,
        &[ExpressionKind::Filter],
    )?;

    // Order U: a faulty ScanFilter list value, after the no-expression checks
    // and ahead of ConditionalOperator, the value count and the key's faults
    // (measured 2026-11-05; see `scan_request::ScanFilterBody`).
    scan_filter_values.validate()?;

    // ConditionalOperator requires a ScanFilter with two or more conditions.
    extenddb_core::validation::validate_conditional_operator_usage(
        has_cond_op,
        input.scan_filter.as_ref().map_or(0, HashMap::len),
    )?;

    // Desugar a legacy ScanFilter: its value count is a request parameter
    // check, ahead of every fault in the key (measured 2026-11-05). The mix
    // check refuses a ScanFilter beside a FilterExpression, so the two
    // branches below never both apply.
    let legacy_filter = match input.scan_filter {
        Some(ref sf) if !sf.is_empty() => {
            let cond_op = input.conditional_operator.unwrap_or_default();
            Some(desugar_filter(sf, cond_op)?)
        }
        _ => None,
    };

    match order {
        CheckOrder::Unreadable => {
            // The key's faults, then the faulty ExpressionAttributeValues
            // entries in the fitted order (each in the invalid-value form, an
            // entry nested too deep among them) and an empty name (measured
            // 2026-10-31, 2026-11-05, 2026-11-19 and 2026-11-24; see
            // `scan_request::StartKeyBody` and `ExpressionValuesBody`).
            start_key.validate(order)?;
            expression_values.validate()?;
        }
        CheckOrder::Readable => {
            // The Select enum, after the no-expression check and the nesting
            // check (measured 2026-11-05). The service makes the filter
            // syntax, an undefined value and the unused values ahead of it
            // (measured 2026-11-19, #1391).
            select.validate()?;
        }
    }
    Ok(CheckedBody {
        order,
        input,
        has_proj_expr,
        legacy_filter,
        start_key,
    })
}
