// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The request-level expression checks of `SearchVectors`, shared by the
//! equality-only grammar and the extended grammar.
//!
//! The handler calls [`check`] once `TopK` and `SearchVector` are valid. It
//! checks both expression maps (see `extenddb_core::expression::search_params`),
//! then the projection, so a projection fault answers ahead of any search
//! condition fault. A request without a search condition then gets the
//! unused-names check here; a request with one gets it after its condition
//! parses, through [`equality_conditions`] or the extended parser.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::search_params::{
    check_search_maps, check_unused_names, with_unused_names,
};
use extenddb_core::expression::{
    ExpressionMaps, Projection, SearchCondition, validate_search_condition_expression,
};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::AttributeValue;

/// Check the expression maps and the projection of a `SearchVectors`
/// request.
///
/// # Errors
///
/// Returns the `ValidationException` of the first failed check.
pub(crate) fn check(
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    condition: Option<&str>,
    projection: Option<&str>,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    check_search_maps(names, values, condition.is_some(), projection.is_some())?;
    if let Some(projection) = projection {
        let paths = crate::expression_helpers::parse_projection_expr(projection, limits)?;
        let names = names
            .into_iter()
            .flatten()
            .map(|(k, v)| (k.trim_start_matches('#').to_owned(), v.clone()))
            .collect();
        Projection::compile(&paths, &ExpressionMaps::new(names, HashMap::new()), true)?;
    }
    if condition.is_none() {
        check_unused_names(names, &[projection.unwrap_or_default()])?;
    }
    Ok(())
}

/// The equality-only grammar's conditions, with the unused-names check.
///
/// # Errors
///
/// Returns the `ValidationException` of the first failed check.
pub(crate) fn equality_conditions(
    expr: &str,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    projection: Option<&str>,
) -> Result<Vec<SearchCondition>, DynamoDbError> {
    with_unused_names(
        validate_search_condition_expression(expr, names, values),
        names,
        &[expr, projection.unwrap_or_default()],
    )
}
