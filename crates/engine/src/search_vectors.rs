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
    consumed_capacity: Option<extenddb_core::types::VectorCapacity>,
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
        // An index that is not ACTIVE cannot serve, and the service reports that by
        // saying the table does not have the index at all: measured four times
        // against DynamoDB on 2026-08-11 while an index sat in CREATING, which
        // returned exactly this message rather than naming the status.
        //
        // Filtered here rather than excluded from DescribeTable, because
        // DescribeTable MUST still report a CREATING index with its status and
        // `Backfilling` member; only the search path treats it as absent.
        //
        // This is load-bearing now that the backfill runs asynchronously. While it
        // was awaited inline inside one transaction, a partially populated index was
        // unobservable; a search can now arrive mid-build and must be refused rather
        // than answered from incomplete data.
        .filter(|vi| vi.index_status.is_active())
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
    // component, which is metered separately as the query's own dimension cost.
    //
    // Subtracted explicitly rather than assumed absent, because this is computed
    // from what the BACKEND returned, before the response projection runs. A
    // backend is free to hand over the vector (the SQLite path does, since it holds
    // the projected item verbatim as the GSI path does), so the figure must not rest
    // on an assumption about how a backend stores its rows. An earlier version of
    // this comment asserted the vector was already absent and was wrong, inflating
    // the billed figure by roughly 10 to 15 KB per hit at 1024 dimensions.
    let non_vector_bytes: usize = hits
        .iter()
        .map(|h| {
            let total = item_size_bytes(&h.item);
            let vector = key_info
                .vector_indexes
                .iter()
                .find(|vi| vi.index_name == input.index_name)
                .and_then(|vi| h.item.get(&vi.vector_attribute_name))
                .map_or(0, extenddb_core::types::attribute_value_size);
            total.saturating_sub(vector)
        })
        .sum();

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
                // A supplied ProjectionExpression already restricts the item to the
                // paths it names, so naming the vector attribute keeps it and not
                // naming it drops it. Nothing extra to do.
                Some(proj) => proj.apply(&hit.item),
                // With no ProjectionExpression the vector attribute is withheld.
                // The service returns it only when it is explicitly asked for, even
                // under a Projection of ALL, so returning it by default would be
                // both a parity divergence and a large one: a 1024-dimension vector
                // serialises to roughly 10 to 15 KB per hit.
                None => {
                    let mut item = hit.item;
                    item.remove(&vector_index.vector_attribute.attribute_name);
                    item
                }
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
    //
    // `vectors_returned` is counted from the projected results rather than inferred
    // from whether the expression mentions the attribute, so an alias, a nested
    // path, or an item that simply has no vector all count correctly. The service
    // charges one float32 per dimension for each returned item that carries it.
    let vectors_returned = search_results
        .iter()
        .filter(|r| {
            r.item
                .contains_key(&vector_index.vector_attribute.attribute_name)
        })
        .count();
    let request_bytes = search_request_bytes(
        vector_index.dimensions,
        non_vector_bytes,
        search_results.len(),
        vectors_returned,
    );
    let consumed_capacity = match input.return_consumed_capacity {
        ReturnConsumedCapacity::None => None,
        _ => Some(extenddb_core::types::VectorCapacity {
            vector_search_request_bytes: Some(request_bytes),
            vector_write_request_bytes: None,
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
/// returned, taken before any projection is applied. `returned_count` is how many
/// items are returned, and `vectors_returned` how many of those include the vector
/// attribute in the response projection.
///
/// Re-derived on 2026-08-11 in us-east-1 by sweeping dimensions 16, 64, 128, 256 and
/// 512 against key-only items, at TopK 1, 10 and 25, four samples per point. The
/// previous model used 17.6 bytes per dimension and no per-result term, which
/// reported 2748.8 where the service reported 6196.0 for the same 128-dimension
/// search: a ratio of 2.25, far outside the 1.176 bimodal spread that had been used
/// to excuse imprecision. Three terms were measured, each independently:
///
///  * **Per dimension: 30.6875.** Exact at every dimension tested, not a fit.
///    Intercepts were 491, 1964, 3928, 7856 and 15712 for 16 to 512 dimensions,
///    each exactly `dimensions * 30.6875`.
///  * **Per returned result: 72 bytes** with key-only items, flat across dimensions
///    64 to 512. The 16-dimension row measured 52.8 only because the 1 KiB floor
///    clipped its TopK 1 point.
///  * **Per returned vector: 4 bytes per dimension.** Projecting the vector added
///    exactly `40 * dimensions` at TopK 10, i.e. `4 * dimensions` per returned item,
///    which is one float32 per dimension. The previous model ignored this entirely,
///    so projecting the vector doubled the service's figure and changed nothing here.
///  * The **1 KiB floor** is unchanged and was reconfirmed: a 16-dimension search at
///    TopK 1 reported exactly 1024.
///
/// The 72 is split into a fixed 65 plus the returned item's own bytes, because the
/// caller already supplies `returned_non_vector_bytes` and a key-only item
/// contributes roughly 7 of those. That split is the one derived rather than
/// directly measured quantity here: it is corroborated by the richer items used in
/// an earlier probe, whose per-result figure was 102.6 for items carrying about
/// 30 bytes more, but it rests on ExtendDB's own accounting of stored size matching
/// the service's, which cannot be true in general.
///
/// Exact parity therefore remains out of reach, and the earlier note that the
/// service is bimodal still stands as a caution even though this sweep saw a single
/// value at every one of its 60 sample points.
///
/// One term is known to be missing. Every observation above came from an UNSCOPED
/// search. A HASH-scoped search on a 128-dimension index reported 3675, which is
/// below the 3928 that the per-dimension term alone accounts for, implying the
/// service charges less when the SearchSchema confines the search to one partition.
/// That leaves this model roughly 38% HIGH on a scoped search, where it was 40% low
/// on everything before. The direction of the error is now conservative rather than
/// under-reporting, and the scoping term is not characterised: doing so needs a
/// sweep over partition counts and selectivities, which has not been run.
fn search_request_bytes(
    dimensions: u32,
    returned_non_vector_bytes: usize,
    returned_count: usize,
    vectors_returned: usize,
) -> f64 {
    /// Exact at 16, 64, 128, 256 and 512 dimensions.
    const BYTES_PER_DIMENSION: f64 = 30.6875;
    /// One float32 per dimension for each returned item carrying the vector.
    const BYTES_PER_RETURNED_VECTOR_DIMENSION: f64 = 4.0;
    /// The 72 measured per result, less the bytes a key-only item contributes
    /// through `returned_non_vector_bytes`.
    const PER_RESULT_OVERHEAD: f64 = 65.0;
    /// Observed floor. A 16-dimension search never reported less than this.
    const MIN_SEARCH_BYTES: f64 = 1024.0;

    let dims = f64::from(dimensions);
    (BYTES_PER_DIMENSION * dims
        + PER_RESULT_OVERHEAD * returned_count as f64
        + returned_non_vector_bytes as f64
        + BYTES_PER_RETURNED_VECTOR_DIMENSION * dims * vectors_returned as f64)
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

    // Refuse rather than search unscoped, and do it BEFORE the schema validation
    // below so the caller gets the service's own wording. An earlier comment here
    // asserted that "validation upstream guarantees it is present", which was
    // untrue: no such validation existed, so a search that omitted the condition
    // ran without a partition scope and returned HTTP 200 with ZERO results. That
    // is the worst available failure mode, a silent wrong answer, and it told the
    // caller "no matches" for a request the service rejects outright.
    //
    // The service distinguishes the two missing-HASH failure modes, both
    // measured against the service 2026-08-19 in us-east-1 (table
    // msgprobe-hashvix): omitting the SearchConditionExpression entirely gets
    // the message below, while an expression that is supplied but omits the
    // HASH attribute gets the validator's "must have all HASH attributes in
    // configured SearchSchema". So this guard fires only for the empty case,
    // and the omits-HASH case falls through to the validator.
    if hash_attr.is_some() && conditions.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "SearchConditionExpression must be provided when SearchSchema has a HASH key"
                .to_owned(),
        ));
    }

    validate_conditions_against_search_schema(conditions, search_schema, attribute_definitions)?;

    // The validation above is what makes this hold; assert it so a future
    // change that weakens the validator fails here rather than silently
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
        assert_eq!(
            msg, "SearchConditionExpression must be provided when SearchSchema has a HASH key",
            "must surface the service's own wording, measured 2026-08-11 in us-east-1"
        );
    }

    /// The OTHER missing-HASH mode gets a different message. An expression that
    /// IS supplied but omits the HASH attribute is refused by the schema
    /// validator with its own wording, not the "must be provided" text above.
    /// Both strings measured against the service 2026-08-19 in us-east-1: the
    /// service distinguishes these cases, so a resolver that collapses them into
    /// one message diverges from it. This test fails against both prior
    /// resolutions (the pre-#286 single inline guard and a guard keyed off
    /// hash_key instead of expression presence).
    #[test]
    fn expression_omitting_hash_gets_the_validator_message() {
        let schema = hash_schema();
        let conds = vec![SearchCondition {
            attribute_name: "Category".to_owned(),
            value: AttributeValue::S("x".to_owned()),
        }];
        let mut defs = country_defs();
        defs.push(AttributeDefinition {
            attribute_name: "Category".to_owned(),
            attribute_type: ScalarAttributeType::S,
        });
        let err = resolve_search_scope(&conds, Some(&schema), &defs).unwrap_err();
        let DynamoDbError::ValidationException(msg) = err else {
            panic!("expected ValidationException");
        };
        assert!(
            msg.contains(
                "SearchConditionExpression must have all HASH attributes in configured SearchSchema"
            ),
            "expected the validator's measured wording, got: {msg}"
        );
        assert!(
            !msg.contains("must be provided when SearchSchema has a HASH key"),
            "the two failure modes must not share a message: {msg}"
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

    /// A 16-dimension search at TopK 1 reported exactly 1024, so the floor
    /// dominates at low dimensions rather than anything proportional.
    #[test]
    fn search_bytes_have_a_one_kib_floor() {
        assert!((search_request_bytes(4, 0, 0, 0) - 1024.0).abs() < f64::EPSILON);
        assert!((search_request_bytes(1, 100, 1, 0) - 1024.0).abs() < f64::EPSILON);
    }

    /// The per-dimension term is linear, verified against the service at 16, 64,
    /// 128, 256, 512, 1024 and 2048 dimensions.
    #[test]
    fn search_bytes_scale_per_dimension_above_the_floor() {
        let a = search_request_bytes(1024, 0, 0, 0);
        let b = search_request_bytes(2048, 0, 0, 0);
        assert!(a > 1024.0, "1024 dimensions must clear the floor: {a}");
        assert!((b - 2.0 * a).abs() < 1.0, "{a} then {b}");
    }

    /// Adding one item with a 2000-byte non-vector attribute raised the service's
    /// figure by exactly 1999, so returned bytes pass through one-for-one.
    #[test]
    fn returned_item_bytes_pass_through_one_for_one() {
        let base = search_request_bytes(1024, 0, 1, 0);
        assert!((search_request_bytes(1024, 2000, 1, 0) - (base + 2000.0)).abs() < f64::EPSILON);
    }

    /// Each returned result costs a fixed amount beyond its own bytes.
    ///
    /// The previous model had no per-result term at all, so TopK made no difference
    /// to the figure. The service moved by exactly 72 bytes per additional result at
    /// every dimension from 64 to 2048, with key-only items.
    #[test]
    fn each_returned_result_adds_a_fixed_cost() {
        let one = search_request_bytes(1024, 0, 1, 0);
        let eleven = search_request_bytes(1024, 0, 11, 0);
        let per_result = (eleven - one) / 10.0;
        assert!(
            (per_result - 65.0).abs() < f64::EPSILON,
            "expected the fixed per-result overhead, got {per_result}"
        );
    }

    /// Returning the vector costs one float32 per dimension per item.
    ///
    /// This term was absent entirely: projecting the vector doubled the service's
    /// figure (6196 to 11316 at 128 dimensions) and changed nothing here. Measured
    /// as exactly `40 * dimensions` at TopK 10, i.e. `4 * dimensions` per item.
    #[test]
    fn returning_the_vector_costs_four_bytes_per_dimension_per_item() {
        let without = search_request_bytes(128, 0, 10, 0);
        let with = search_request_bytes(128, 0, 10, 10);
        assert!(
            (with - without - 40.0 * 128.0).abs() < f64::EPSILON,
            "expected 4 bytes per dimension per returned vector: {without} then {with}"
        );
        // Partial projection scales with how many results carry the vector.
        let half = search_request_bytes(128, 0, 10, 5);
        assert!(
            (half - without - 20.0 * 128.0).abs() < f64::EPSILON,
            "{half}"
        );
    }

    /// Checks the model against the service's own numbers with a tolerance.
    ///
    /// Equality is not asserted, for two measured reasons. Byte-identical requests
    /// against an unchanged index return one of a small number of values: six
    /// samples at 512 dimensions gave 16418 and 16433. And the figure depends on how
    /// many items the INDEX holds, which this model does not carry: at 512
    /// dimensions and TopK 10, a 30-item index reported 15824 and a 60-item index
    /// 16418, about 19.8 bytes per item. That dependence is why two sweeps
    /// disagreed by 3% at the same dimension, and it contradicts an earlier comment
    /// here which asserted the figure was independent of items scanned.
    ///
    /// The constant is set from the 60-item observations, which are internally exact
    /// at five dimensions, so the model errs slightly high on very small indexes
    /// rather than low on realistic ones.
    ///
    /// Observations are TopK 10 against key-only items, so `returned_non_vector_bytes`
    /// is passed as a small figure rather than zero.
    #[test]
    fn model_tracks_the_measured_service_figures() {
        // (dimensions, items in index, observed bytes at TopK 10)
        for (dimensions, observed) in [
            (512_u32, 16433.0_f64),
            (1024, 31206.0),
            (2048, 61692.0),
            (128, 4648.0),
            (64, 2684.0),
        ] {
            let modelled = search_request_bytes(dimensions, 70, 10, 0);
            let error = (modelled - observed).abs() / observed;
            assert!(
                error < 0.05,
                "{dimensions} dimensions: modelled {modelled} against observed {observed} \
                 is {:.2}% out",
                error * 100.0
            );
        }
    }

    /// The old constant is far enough out to be worth pinning as a regression guard.
    ///
    /// 17.6 bytes per dimension reported 2748.8 where the service reported 6196.0,
    /// so anything near the old value must fail this.
    #[test]
    fn the_superseded_constant_would_not_pass() {
        let modelled = search_request_bytes(128, 70, 10, 0);
        assert!(
            modelled > 4000.0,
            "the model must not regress toward the old 17.6 per dimension: {modelled}"
        );
    }
}
