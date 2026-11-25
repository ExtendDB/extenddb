// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `SearchVectors` request body and the checks that need no table.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{AttributeValue, ReturnConsumedCapacity};
use extenddb_storage::{StorageEngine, VectorSearchEngine};

use super::condition;

/// Minimum length of a vector index name.
pub(super) const MIN_INDEX_NAME_LENGTH: usize = 3;
/// Maximum number of nearest neighbors a single search may request.
pub(super) const MAX_TOP_K: i64 = 100;
/// Maximum number of elements in a search vector.
pub(super) const MAX_SEARCH_VECTOR_LENGTH: usize = 4096;
/// `SearchVectors` request body.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchVectorsInput {
    #[serde(rename = "TableName")]
    pub(super) table_name: String,
    #[serde(rename = "IndexName")]
    pub(super) index_name: String,
    #[serde(rename = "SearchVector")]
    pub(super) search_vector: Vec<AttributeValue>,
    #[serde(rename = "TopK")]
    pub(super) top_k: i64,
    #[serde(rename = "SearchConditionExpression")]
    pub(super) search_condition_expression: Option<String>,
    #[serde(rename = "ProjectionExpression")]
    pub(super) projection_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames")]
    pub(super) expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues")]
    pub(super) expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub(super) return_consumed_capacity: ReturnConsumedCapacity,
}

/// A `SearchVectors` request whose checks that need no table passed.
pub(super) struct Checked<'s> {
    pub(super) input: SearchVectorsInput,
    pub(super) query_vector: Vec<f32>,
    pub(super) parsed: condition::Parsed,
    pub(super) vector_search: &'s dyn VectorSearchEngine,
}

/// Check a `SearchVectors` request as far as the checks need no table.
///
/// # Errors
///
/// Returns the error of the first failed check.
pub(super) async fn check<'s>(
    body: &Value,
    storage: &'s Arc<dyn StorageEngine>,
    account_id: &str,
    limits: &LimitsConfig,
) -> Result<Checked<'s>, DynamoDbError> {
    let input = SearchVectorsInput::deserialize(body).map_err(crate::deserialize_error)?;

    let vector_search = crate::vector_gate::ensure_search_supported(storage.as_vector_search())?;

    // Request-shape validation runs before the table lookup, so a malformed
    // request against a missing table reports the validation error rather than
    // ResourceNotFound.

    // IndexName length.
    if input.index_name.len() < MIN_INDEX_NAME_LENGTH {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value at 'IndexName' failed to satisfy constraint: \
             Member must have length greater than or equal to {MIN_INDEX_NAME_LENGTH}"
        )));
    }

    // SearchVector length and element types.
    let query_vector = parse_search_vector(&input.search_vector)?;

    // TopK bounds. The lower bound reports the standard constraint message; the
    // upper bound reports the documented range message.
    if input.top_k < 1 {
        return Err(DynamoDbError::ValidationException(
            "1 validation error detected: Value at 'TopK' failed to satisfy constraint: \
             Member must have value greater than or equal to 1"
                .to_owned(),
        ));
    }
    if input.top_k > MAX_TOP_K {
        return Err(DynamoDbError::ValidationException(format!(
            "Provided TopK value '{}' is out of valid range. \
             The value must be between 1 and {MAX_TOP_K} inclusive",
            input.top_k
        )));
    }

    crate::search_vectors_params::check(
        input.expression_attribute_names.as_ref(),
        input.expression_attribute_values.as_ref(),
        input.search_condition_expression.as_deref(),
        input.projection_expression.as_deref(),
        limits,
    )?;

    let parsed = condition::parse(
        storage,
        vector_search,
        account_id,
        input.search_condition_expression.as_deref(),
        input.expression_attribute_names.as_ref(),
        input.expression_attribute_values.as_ref(),
        input.projection_expression.as_deref(),
        limits,
    )
    .await?;

    Ok(Checked {
        input,
        query_vector,
        parsed,
        vector_search,
    })
}

/// Validate a `SearchVector` and convert it into `f32`.
///
/// Checks the length bounds (1..=`MAX_SEARCH_VECTOR_LENGTH`) and that every
/// element is a finite number before converting.
pub(super) fn parse_search_vector(values: &[AttributeValue]) -> Result<Vec<f32>, DynamoDbError> {
    if values.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "1 validation error detected: Value at 'SearchVector' failed to satisfy constraint: \
             Member must have length greater than or equal to 1"
                .to_owned(),
        ));
    }
    if values.len() > MAX_SEARCH_VECTOR_LENGTH {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value at 'SearchVector' failed to satisfy constraint: \
             Member must have length less than or equal to {MAX_SEARCH_VECTOR_LENGTH}"
        )));
    }
    let mut out = Vec::with_capacity(values.len());
    for v in values {
        match v {
            AttributeValue::N(n) => {
                let f = n.parse::<f32>().map_err(|_| {
                    DynamoDbError::ValidationException(
                        "Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute".to_owned(),
                    )
                })?;
                if !f.is_finite() {
                    return Err(DynamoDbError::ValidationException(
                        "Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute".to_owned(),
                    ));
                }
                out.push(f);
            }
            _ => {
                return Err(DynamoDbError::ValidationException(
                    "Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute".to_owned(),
                ));
            }
        }
    }
    Ok(out)
}
