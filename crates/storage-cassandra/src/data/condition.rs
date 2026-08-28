// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition expression evaluation helpers for Cassandra backend.

use extenddb_core::expression::{self, Expr, ExpressionMaps};
use extenddb_core::types::AttributeValue;
use extenddb_storage::error::StorageError;
use std::collections::BTreeMap;

/// Evaluate a condition expression against an item.
///
/// Returns `Ok(())` if the condition passes or is `None`.
/// Returns `Err(StorageError::ConditionFailed)` if the condition fails.
///
/// For non-existent items, pass an empty BTreeMap as the item.
pub(crate) fn check_condition(
    condition: Option<&Expr>,
    item: &BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
) -> Result<(), StorageError> {
    if let Some(cond) = condition {
        let passed = expression::evaluate_condition(cond, item, maps)
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        if !passed {
            return Err(StorageError::ConditionFailed(None));
        }
    }
    Ok(())
}
