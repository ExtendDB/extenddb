// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `SearchVectors` request body and the checks of its `IndexName`,
//! `SearchVector` and `TopK`.

use std::collections::HashMap;

use serde::Deserialize;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, ReturnConsumedCapacity};

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
