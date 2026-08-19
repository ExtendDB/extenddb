// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SearchVectors` operation handler.
//!
//! Runs a similarity search over a vector index: parses the query vector, an
//! optional single-equality prefilter, and an optional projection, calls the
//! storage layer for the top-k nearest neighbors, and returns each item with
//! its similarity score.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    ExpressionMaps, Projection, validate_conditions_against_search_schema,
    validate_search_condition_expression,
};
use extenddb_core::types::{
    AttributeValue, DescribeTableInput, Item, ReturnConsumedCapacity, SearchSchemaElementType,
    item_size_bytes,
};

use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;
use crate::serialize_output;
use crate::{DispatchMetrics, DispatchResult};

/// Minimum length of a vector index name.
const MIN_INDEX_NAME_LENGTH: usize = 3;
/// Maximum number of nearest neighbors a single search may request.
const MAX_TOP_K: i64 = 100;
/// Maximum number of elements in a search vector.
const MAX_SEARCH_VECTOR_LENGTH: usize = 4096;
/// `SearchVectors` request body.
#[derive(Debug, Clone, Deserialize)]
struct SearchVectorsInput {
    #[serde(rename = "TableName")]
    table_name: String,
    #[serde(rename = "IndexName")]
    index_name: String,
    #[serde(rename = "SearchVector")]
    search_vector: Vec<AttributeValue>,
    #[serde(rename = "TopK")]
    top_k: i64,
    #[serde(rename = "SearchConditionExpression")]
    search_condition_expression: Option<String>,
    #[serde(rename = "ProjectionExpression")]
    projection_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames")]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues")]
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: ReturnConsumedCapacity,
}

/// A single search result: the item plus its similarity score.
#[derive(Debug, Serialize)]
struct SearchResult {
    #[serde(rename = "Item")]
    item: Item,
    #[serde(rename = "Score")]
    score: f64,
}

/// `SearchVectors` response body.
#[derive(Debug, Serialize)]
struct SearchVectorsOutput {
    #[serde(rename = "SearchResults")]
    search_results: Vec<SearchResult>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    consumed_capacity: Option<VectorCapacity>,
}

/// Consumed capacity for a vector search, reported as `VectorSearchUnits`.
#[derive(Debug, Serialize)]
struct VectorCapacity {
    /// Verified against the service 2026-08-05: the response field is
    /// `VectorSearchRequestBytes`, a byte count. There is no units field.
    #[serde(rename = "VectorSearchRequestBytes")]
    vector_search_request_bytes: f64,
}

/// Handle a `SearchVectors` request.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables/indexes, or
/// storage errors.
pub async fn handle_search_vectors(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    let input: SearchVectorsInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    let vector_search =
        crate::vector_gate::ensure_search_supported(ctx.storage.as_vector_search())?;

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

    // Structural validation of the filter expression (schema-independent).
    let conditions = match input.search_condition_expression.as_deref() {
        Some(expr) => validate_search_condition_expression(
            expr,
            input.expression_attribute_names.as_ref(),
            input.expression_attribute_values.as_ref(),
        )?,
        None => Vec::new(),
    };

    let key_info = ctx
        .table_key_info(&input.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Schema-aware validation needs the vector index metadata (dimension and
    // search schema) plus the table attribute definitions for type checks.
    let table = ctx
        .storage
        .describe_table(
            &ctx.account_id,
            DescribeTableInput {
                table_name: input.table_name.clone(),
            },
        )
        .await
        .map_err(storage_err_to_dynamo)?;
    let vector_index = table
        .vector_indexes
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .find(|vi| vi.index_name == input.index_name)
        .ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "The table does not have the specified index: {}",
                input.index_name
            ))
        })?;

    if query_vector.len() != vector_index.dimensions as usize {
        return Err(DynamoDbError::ValidationException(format!(
            "Input search vector dimension {} does not match vector index dimension {}",
            query_vector.len(),
            vector_index.dimensions
        )));
    }

    let (hash_key, filters) = resolve_search_scope(
        &conditions,
        vector_index.search_schema.as_deref(),
        &table.attribute_definitions,
    )?;

    let search_output = vector_search
        .search_vectors(extenddb_storage::VectorSearch {
            key_info: &key_info,
            index_name: &input.index_name,
            query_vector: &query_vector,
            top_k: input.top_k,
            hash_key,
            filters: &filters,
        })
        .await
        .map_err(storage_err_to_dynamo)?;
    let hits = search_output.hits;

    // Bytes read from the index for the returned items, excluding the vector
    // component (the stored item already omits the vector attribute).
    let non_vector_bytes: usize = hits.iter().map(|h| item_size_bytes(&h.item)).sum();

    // Compile the projection once, if supplied.
    let compiled_projection = if let Some(ref proj_str) = input.projection_expression {
        let paths = crate::expression_helpers::parse_projection_expr(proj_str, &ctx.limits)?;
        let names = input
            .expression_attribute_names
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k.trim_start_matches('#').to_owned(), v))
            .collect();
        let proj_maps = ExpressionMaps::new(names, HashMap::new());
        Some(Projection::compile(&paths, &proj_maps, true)?)
    } else {
        None
    };

    let search_results: Vec<SearchResult> = hits
        .into_iter()
        .map(|hit| {
            let item = match compiled_projection.as_ref() {
                Some(proj) => proj.apply(&hit.item),
                None => hit.item,
            };
            SearchResult {
                item,
                score: hit.score,
            }
        })
        .collect();

    // Kept for the dispatch metric only. `SearchVectorsOutput` deliberately does
    // NOT carry a `Count` field: measured against the live service on 2026-08-10,
    // the response contains only `SearchResults` and `ConsumedCapacity`, across
    // five parameter variations (no projection, ReturnConsumedCapacity=INDEXES,
    // a ProjectionExpression, and a TopK larger than the item count).
    let count = search_results.len() as i64;

    // The service reports `ConsumedCapacity.VectorSearchRequestBytes`, a byte
    // figure, not a unit figure. See `search_request_bytes` for the measured
    // model and why exact parity is not achievable.
    let request_bytes = search_request_bytes(vector_index.dimensions, non_vector_bytes);
    let consumed_capacity = match input.return_consumed_capacity {
        ReturnConsumedCapacity::None => None,
        _ => Some(VectorCapacity {
            vector_search_request_bytes: request_bytes,
        }),
    };

    let output = SearchVectorsOutput {
        search_results,
        consumed_capacity,
    };

    let body = serialize_output(&output)?;
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            read_capacity_units: request_bytes,
            returned_item_count: count as u64,
            index_name: Some(input.index_name),
            ..Default::default()
        },
    })
}

/// Validate a `SearchVector` and convert it into `f32`.
///
/// Checks the length bounds (1..=`MAX_SEARCH_VECTOR_LENGTH`) and that every
/// element is a finite number before converting.
fn parse_search_vector(values: &[AttributeValue]) -> Result<Vec<f32>, DynamoDbError> {
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
                        "Search vector contains invalid values".to_owned(),
                    )
                })?;
                if !f.is_finite() {
                    return Err(DynamoDbError::ValidationException(
                        "Search vector contains invalid values".to_owned(),
                    ));
                }
                out.push(f);
            }
            _ => {
                return Err(DynamoDbError::ValidationException(
                    "Search vector contains invalid values".to_owned(),
                ));
            }
        }
    }
    Ok(out)
}

/// Bytes reported as `ConsumedCapacity.VectorSearchRequestBytes` for one search.
///
/// `returned_non_vector_bytes` is the summed stored size of the items actually
/// returned, taken before any projection is applied.
///
/// Measured against the service on 2026-08-05 in us-east-1. Three properties
/// hold, and this reproduces all three:
///
///  * A 1 KiB floor. A 4-dimension index reported exactly 1024 for every TopK
///    from 1 to 100, for 1 to 100 returned results, and with 4 to 204 items in
///    the index.
///  * A per-dimension term that is independent of how many items the index
///    holds, so this is not a scan-cost model: growing a 4096-dimension index
///    from 3 to 12 items moved the figure by 2 bytes, which was the returned
///    key's length rather than the extra items. It is also unaffected by the
///    query vector's wire width, since the same search with 1-character and
///    21-character numbers (49 KB versus 127 KB of JSON) both reported 75201.
///    The vector is metered per dimension, not per byte sent.
///  * Plus the stored bytes of the returned items, excluding the searched
///    vector. Adding one item with a 2000-byte non-vector attribute raised the
///    figure by exactly 1999. Projection does not reduce it, so a caller must
///    sum the items before projecting, not after.
///
/// Exact parity is NOT achievable and must not be asserted. The service is not
/// deterministic here: byte-identical requests against an unchanged index return
/// one of two values separated by a fixed `dimensions * 3.111` offset, a ratio of
/// 1.176. Twenty-four samples at 1024 dimensions gave 18067 and 21253; at 2048
/// they gave 36058 and 42429; the mix varies between runs and persists with 10
/// items in the index. This reproduces the lower and more frequent mode.
fn search_request_bytes(dimensions: u32, returned_non_vector_bytes: usize) -> f64 {
    /// Derived from the lower mode: 18067 at 1024 dimensions and 36058 at 2048,
    /// both 17.6 bytes per dimension once the returned item is subtracted.
    const BYTES_PER_DIMENSION: f64 = 17.6;
    /// Observed floor. A 4-dimension search never reported less than this.
    const MIN_SEARCH_BYTES: f64 = 1024.0;

    (BYTES_PER_DIMENSION * f64::from(dimensions) + returned_non_vector_bytes as f64)
        .max(MIN_SEARCH_BYTES)
}

/// Resolved search scope: the partition-scoping HASH equality, if the index
/// declares one, and the remaining inline-filter equalities.
type SearchScope<'a> = (
    Option<(&'a str, &'a AttributeValue)>,
    Vec<(&'a str, &'a AttributeValue)>,
);

/// Validate a search's conditions against the index search schema, then split
/// them into the partition scope and the remaining inline filters.
///
/// The index's HASH element scopes the search to one partition; the remaining
/// conditions narrow within it. Declaring a HASH element is optional, but when
/// the index has one the service requires the search to supply it.
///
/// Validation runs unconditionally, including when the caller supplied no
/// `SearchConditionExpression` at all. That is the whole point of doing it here
/// rather than at the call site behind a non-empty check: an index declaring a
/// HASH element and a search with no expression is a validation failure, and
/// skipping the check for empty conditions would return `None` for `hash_key`
/// and hand the backend an unscoped search. `VectorSearch::hash_key` promises
/// backend authors that `Some` is a mandatory predicate whenever the index
/// declares a HASH element, so that promise has to hold on every path.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when the conditions reference an
/// attribute outside the search schema, omit a declared HASH attribute, or carry
/// a value whose type disagrees with the table's attribute definitions.
fn resolve_search_scope<'a>(
    conditions: &'a [extenddb_core::expression::SearchCondition],
    search_schema: Option<&'a [extenddb_core::types::SearchSchemaElement]>,
    attribute_definitions: &[extenddb_core::types::AttributeDefinition],
) -> Result<SearchScope<'a>, DynamoDbError> {
    validate_conditions_against_search_schema(conditions, search_schema, attribute_definitions)?;

    let hash_attr = search_schema
        .unwrap_or_default()
        .iter()
        .find(|e| e.element_type == SearchSchemaElementType::Hash)
        .map(|e| e.attribute_name.as_str());

    let hash_key: Option<(&str, &AttributeValue)> = hash_attr.and_then(|name| {
        conditions
            .iter()
            .find(|c| c.attribute_name == name)
            .map(|c| (c.attribute_name.as_str(), &c.value))
    });

    // The validation above is what makes this hold; assert it so a future change
    // that reintroduces a conditional guard fails here rather than silently
    // serving an unscoped search.
    debug_assert!(
        hash_attr.is_none() || hash_key.is_some(),
        "index declares a HASH element but the resolved scope has no hash_key"
    );

    let filters: Vec<(&str, &AttributeValue)> = conditions
        .iter()
        .filter(|c| Some(c.attribute_name.as_str()) != hash_attr)
        .map(|c| (c.attribute_name.as_str(), &c.value))
        .collect();

    Ok((hash_key, filters))
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::expression::SearchCondition;
    use extenddb_core::types::{
        AttributeDefinition, ScalarAttributeType, SearchSchemaElement, SearchSchemaElementType,
    };

    fn hash_schema() -> Vec<SearchSchemaElement> {
        vec![SearchSchemaElement {
            attribute_name: "Country".to_owned(),
            element_type: SearchSchemaElementType::Hash,
        }]
    }

    fn country_defs() -> Vec<AttributeDefinition> {
        vec![AttributeDefinition {
            attribute_name: "Country".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }]
    }

    fn country_cond(v: &str) -> Vec<SearchCondition> {
        vec![SearchCondition {
            attribute_name: "Country".to_owned(),
            value: AttributeValue::S(v.to_owned()),
        }]
    }

    /// The defect this guards: an index declaring a HASH element, searched with
    /// no `SearchConditionExpression` at all, must be refused. Resolving the
    /// scope without validating first would yield `hash_key: None` and hand the
    /// backend an unscoped search, contradicting the `VectorSearch::hash_key`
    /// contract that backends may treat `Some` as a mandatory predicate.
    #[test]
    fn hash_index_searched_with_no_conditions_is_refused() {
        let schema = hash_schema();
        let err = resolve_search_scope(&[], Some(&schema), &country_defs()).unwrap_err();
        let DynamoDbError::ValidationException(msg) = err else {
            panic!("expected ValidationException");
        };
        assert!(
            msg.contains("SearchConditionExpression must have all HASH attributes"),
            "got: {msg}"
        );
    }

    /// Converse control: an index with no search schema has no scope to require,
    /// so no conditions is valid and the resolved scope is empty. Without this,
    /// the test above would also pass for a function that refused every search.
    #[test]
    fn index_without_search_schema_allows_no_conditions() {
        let (hash_key, filters) = resolve_search_scope(&[], None, &country_defs()).unwrap();
        assert!(hash_key.is_none());
        assert!(filters.is_empty());
    }

    /// The scope is populated when supplied, and the HASH attribute is not also
    /// repeated as an inline filter.
    #[test]
    fn hash_condition_becomes_the_scope_and_not_a_filter() {
        let conds = country_cond("USA");
        let schema = hash_schema();
        let (hash_key, filters) =
            resolve_search_scope(&conds, Some(&schema), &country_defs()).unwrap();
        assert_eq!(hash_key.map(|(n, _)| n), Some("Country"));
        assert!(
            filters.is_empty(),
            "HASH attribute must not be repeated as an inline filter, got {filters:?}"
        );
    }

    #[test]
    fn parse_search_vector_ok() {
        let v = vec![
            AttributeValue::N("0.1".to_owned()),
            AttributeValue::N("-2".to_owned()),
        ];
        assert_eq!(parse_search_vector(&v).unwrap(), vec![0.1f32, -2.0]);
    }

    #[test]
    fn parse_search_vector_rejects_empty_with_length_message() {
        let err = parse_search_vector(&[]).unwrap_err();
        let DynamoDbError::ValidationException(msg) = err else {
            panic!("expected ValidationException");
        };
        assert!(msg.contains(
            "Value at 'SearchVector' failed to satisfy constraint: \
             Member must have length greater than or equal to 1"
        ));
    }

    #[test]
    fn parse_search_vector_rejects_over_max_length() {
        let big: Vec<AttributeValue> = (0..=MAX_SEARCH_VECTOR_LENGTH)
            .map(|i| AttributeValue::N(i.to_string()))
            .collect();
        let err = parse_search_vector(&big).unwrap_err();
        let DynamoDbError::ValidationException(msg) = err else {
            panic!("expected ValidationException");
        };
        assert!(msg.contains("Member must have length less than or equal to 4096"));
    }

    #[test]
    fn parse_search_vector_rejects_non_number() {
        let err = parse_search_vector(&[AttributeValue::S("x".to_owned())]).unwrap_err();
        let DynamoDbError::ValidationException(msg) = err else {
            panic!("expected ValidationException");
        };
        assert_eq!(msg, "Search vector contains invalid values");
    }

    #[test]
    fn parse_search_vector_rejects_non_finite() {
        assert!(parse_search_vector(&[AttributeValue::N("NaN".to_owned())]).is_err());
        assert!(parse_search_vector(&[AttributeValue::N("inf".to_owned())]).is_err());
    }

    /// A 4-dimension search reported exactly 1024 for every TopK from 1 to 100
    /// and for 4 to 204 items in the index, so the floor dominates at low
    /// dimensions rather than anything proportional.
    #[test]
    fn search_bytes_have_a_one_kib_floor() {
        assert!((search_request_bytes(4, 0) - 1024.0).abs() < f64::EPSILON);
        assert!((search_request_bytes(1, 100) - 1024.0).abs() < f64::EPSILON);
    }

    /// Independent of items scanned, so the only inputs are dimensions and the
    /// bytes of the items actually returned.
    #[test]
    fn search_bytes_scale_per_dimension_above_the_floor() {
        let a = search_request_bytes(1024, 0);
        let b = search_request_bytes(2048, 0);
        assert!(a > 1024.0, "1024 dimensions must clear the floor: {a}");
        // Doubling the dimensions doubles the per-dimension term exactly.
        assert!((b - 2.0 * a).abs() < 1.0, "{a} then {b}");
    }

    /// Adding one item with a 2000-byte non-vector attribute raised the service's
    /// figure by exactly 1999, so returned bytes pass through one-for-one.
    #[test]
    fn returned_item_bytes_pass_through_one_for_one() {
        let base = search_request_bytes(1024, 0);
        assert!((search_request_bytes(1024, 2000) - (base + 2000.0)).abs() < f64::EPSILON);
    }

    /// Checks the model against the service's own numbers, deliberately with a
    /// tolerance rather than equality: the service returns one of two values for
    /// a byte-identical request (18067 or 21253 at 1024 dimensions, 36058 or
    /// 42429 at 2048), so asserting equality would encode a coin flip. Roughly
    /// 20 bytes of returned item accompanied each observation.
    #[test]
    fn model_tracks_the_observed_lower_mode() {
        for (dimensions, observed) in [(1024_u32, 18067.0_f64), (2048, 36058.0)] {
            let modelled = search_request_bytes(dimensions, 20);
            let error = (modelled - observed).abs() / observed;
            assert!(
                error < 0.01,
                "{dimensions} dimensions: modelled {modelled} against observed {observed} \
                 is {:.2}% out",
                error * 100.0
            );
        }
    }
}
