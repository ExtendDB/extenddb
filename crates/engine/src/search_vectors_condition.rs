// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `SearchConditionExpression` of a `SearchVectors` request: the grammar
//! that parses it, and the scope it hands the backend.

use std::collections::HashMap;
use std::sync::Arc;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{SearchCondition, validate_conditions_against_search_schema};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, SearchSchemaElement, SearchSchemaElementType,
};
use extenddb_storage::{VectorItemFilter, VectorSearchEngine};

use crate::search_vectors_extended::Extended;

/// A search condition parsed by one of the two grammars.
pub(crate) struct Parsed {
    extended: Option<Extended>,
    conditions: Vec<SearchCondition>,
}

/// The HASH equality that selects the partition, the inline-filter
/// equalities, and the filter the backend applies to each candidate.
pub(crate) type Scope<'a> = (
    Option<(&'a str, &'a AttributeValue)>,
    Vec<(&'a str, &'a AttributeValue)>,
    Option<Arc<dyn VectorItemFilter>>,
);

/// Structural validation of the filter expression (schema-independent).
/// An account the extended grammar is enabled for gets that parser instead.
///
/// # Errors
///
/// Returns the `ValidationException` of the first failed check.
pub(crate) async fn parse(
    vector_search: &dyn VectorSearchEngine,
    account_id: &str,
    expr: Option<&str>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    projection: Option<&str>,
) -> Result<Parsed, DynamoDbError> {
    let extended = crate::search_vectors_extended::parse_request(
        vector_search,
        account_id,
        expr,
        names,
        values,
        projection,
    )
    .await?;
    let conditions = match expr {
        Some(_) if extended.is_some() => Vec::new(),
        Some(expr) => {
            crate::search_vectors_params::equality_conditions(expr, names, values, projection)?
        }
        None => Vec::new(),
    };
    Ok(Parsed {
        extended,
        conditions,
    })
}

/// The scope of the search, once the schema checks pass.
///
/// # Errors
///
/// Returns the `ValidationException` of the first failed schema check.
pub(crate) fn resolve<'a>(
    parsed: &'a Parsed,
    search_schema: Option<&'a [SearchSchemaElement]>,
    attribute_definitions: &[AttributeDefinition],
) -> Result<Scope<'a>, DynamoDbError> {
    Ok(match &parsed.extended {
        Some(extended) => {
            let (hash_key, item_filter) = crate::search_vectors_extended::resolve_scope(
                extended,
                search_schema,
                attribute_definitions,
            )?;
            (hash_key, Vec::new(), Some(item_filter))
        }
        None => {
            let (hash_key, filters) =
                resolve_search_scope(&parsed.conditions, search_schema, attribute_definitions)?;
            (hash_key, filters, None)
        }
    })
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
pub(crate) fn resolve_search_scope<'a>(
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
