// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `SearchConditionExpression` of a `SearchVectors` request: the grammar
//! that parses it, and the scope it hands the backend.
//!
//! A condition is parsed with both grammars. When both refuse it with the
//! same error, or both accept it and give the backend the same scope, the
//! answer does not depend on the `search_inequality_filters` setting and the
//! setting is not read. Otherwise the setting decides, and is read only
//! then.
//!
//! A condition that the extended grammar parses still goes to the backend as
//! equality pairs when that gives the same results, so the backend can apply
//! it in its own query. The service compares inline-filter strings and binary
//! values exactly and inline-filter numbers as binary64, and the equality
//! pairs compare every value exactly. So the pairs carry a condition of
//! equalities when no inline-filter value is a number; any other condition
//! goes to the backend as an item filter.

use std::collections::HashMap;
use std::sync::Arc;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{SearchCondition, validate_conditions_against_search_schema};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, SearchSchemaElement, SearchSchemaElementType,
};
use extenddb_storage::{StorageEngine, VectorItemFilter, VectorSearchEngine};

#[cfg(feature = "search-inequality-filters")]
use crate::search_vectors_extended::Extended;

/// A search condition parsed by the grammars that accept it.
pub(crate) enum Parsed {
    /// The equality-only grammar's conditions; empty without a condition.
    Equality(Vec<SearchCondition>),
    /// A condition only the extended grammar serves for the caller.
    #[cfg(feature = "search-inequality-filters")]
    Extended(Extended),
    /// A condition both grammars accept. Which one serves it is decided once
    /// the index's schema is known.
    #[cfg(feature = "search-inequality-filters")]
    Either(Extended, Vec<SearchCondition>),
}

/// The HASH equality that selects the partition, the inline-filter
/// equalities, and the filter the backend applies to each candidate.
pub(crate) type Scope<'a> = (
    Option<(&'a str, &'a AttributeValue)>,
    Vec<(&'a str, &'a AttributeValue)>,
    Option<Arc<dyn VectorItemFilter>>,
);

/// Structural validation of the filter expression (schema-independent),
/// with each grammar that may serve the caller.
///
/// Taking the backend's vector search engine means the grammar is only
/// parsed once the backend has been found to serve vector search: a backend
/// without it refuses the request before this runs.
///
/// # Errors
///
/// Returns the `ValidationException` of the first failed check, and
/// `ServiceUnavailableException` when the answer depends on the setting and
/// the setting cannot be read.
pub(crate) async fn parse(
    storage: &Arc<dyn StorageEngine>,
    _served_by: &dyn VectorSearchEngine,
    account_id: &str,
    expr: Option<&str>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    projection: Option<&str>,
) -> Result<Parsed, DynamoDbError> {
    let Some(expr) = expr else {
        return Ok(Parsed::Equality(Vec::new()));
    };
    let equality =
        crate::search_vectors_params::equality_conditions(expr, names, values, projection);
    #[cfg(feature = "search-inequality-filters")]
    {
        let extended = crate::search_vectors_extended::parse(expr, names, values, projection);
        match (extended, equality) {
            (Ok(extended), Ok(conditions)) => Ok(Parsed::Either(extended, conditions)),
            (Err(a), Err(b)) if same_error(&a, &b) => Err(a),
            (extended, equality) => {
                if crate::search_vectors_extended::enabled_for(storage, account_id).await? {
                    extended.map(Parsed::Extended)
                } else {
                    equality.map(Parsed::Equality)
                }
            }
        }
    }
    #[cfg(not(feature = "search-inequality-filters"))]
    {
        let _ = (storage, account_id);
        equality.map(Parsed::Equality)
    }
}

/// The scope of the search, once the schema checks pass.
///
/// # Errors
///
/// Returns the `ValidationException` of the first failed schema check, and
/// `ServiceUnavailableException` when the answer depends on the setting and
/// the setting cannot be read.
pub(crate) async fn resolve<'a>(
    parsed: &'a Parsed,
    search_schema: Option<&'a [SearchSchemaElement]>,
    attribute_definitions: &[AttributeDefinition],
    storage: &Arc<dyn StorageEngine>,
    account_id: &str,
) -> Result<Scope<'a>, DynamoDbError> {
    match parsed {
        Parsed::Equality(conditions) => {
            let _ = (storage, account_id);
            equality_scope(conditions, search_schema, attribute_definitions)
        }
        #[cfg(feature = "search-inequality-filters")]
        Parsed::Extended(extended) => {
            extended_scope(extended, None, search_schema, attribute_definitions)
        }
        #[cfg(feature = "search-inequality-filters")]
        Parsed::Either(extended, conditions) => {
            let equality = equality_scope(conditions, search_schema, attribute_definitions);
            let extended = extended_scope(
                extended,
                Some(conditions),
                search_schema,
                attribute_definitions,
            );
            if same_scope(&equality, &extended)
                || !crate::search_vectors_extended::enabled_for(storage, account_id).await?
            {
                equality
            } else {
                extended
            }
        }
    }
}

fn equality_scope<'a>(
    conditions: &'a [SearchCondition],
    search_schema: Option<&'a [SearchSchemaElement]>,
    attribute_definitions: &[AttributeDefinition],
) -> Result<Scope<'a>, DynamoDbError> {
    let (hash_key, filters) =
        resolve_search_scope(conditions, search_schema, attribute_definitions)?;
    Ok((hash_key, filters, None))
}

/// The scope of an extended-grammar condition: the equality pairs of
/// `conditions` when they give the same results, else the item filter.
#[cfg(feature = "search-inequality-filters")]
fn extended_scope<'a>(
    extended: &'a Extended,
    conditions: Option<&'a [SearchCondition]>,
    search_schema: Option<&'a [SearchSchemaElement]>,
    attribute_definitions: &[AttributeDefinition],
) -> Result<Scope<'a>, DynamoDbError> {
    let (hash_key, item_filter) = crate::search_vectors_extended::resolve_scope(
        extended,
        search_schema,
        attribute_definitions,
    )?;
    Ok(
        match conditions.and_then(|c| equality_pairs(c, search_schema)) {
            Some(filters) => (hash_key, filters, None),
            None => (hash_key, Vec::new(), Some(item_filter)),
        },
    )
}

/// Whether two scopes give the backend the same search, or two refusals
/// the same error. A scope with an item filter is never the same as
/// another.
#[cfg(feature = "search-inequality-filters")]
fn same_scope(a: &Result<Scope<'_>, DynamoDbError>, b: &Result<Scope<'_>, DynamoDbError>) -> bool {
    match (a, b) {
        (Ok((h1, f1, None)), Ok((h2, f2, None))) => h1 == h2 && f1 == f2,
        (Err(a), Err(b)) => same_error(a, b),
        _ => false,
    }
}

#[cfg(feature = "search-inequality-filters")]
fn same_error(a: &DynamoDbError, b: &DynamoDbError) -> bool {
    a.full_error_type() == b.full_error_type() && a.message() == b.message()
}

/// The inline-filter equalities of an extended-grammar condition that the
/// equality pairs express with the same results, or `None` when the backend
/// needs the item filter. The HASH equality selects the partition and is not
/// among them.
#[cfg(feature = "search-inequality-filters")]
fn equality_pairs<'a>(
    conditions: &'a [SearchCondition],
    search_schema: Option<&[SearchSchemaElement]>,
) -> Option<Vec<(&'a str, &'a AttributeValue)>> {
    let hash_attr = search_schema
        .unwrap_or_default()
        .iter()
        .find(|e| e.element_type == SearchSchemaElementType::Hash)
        .map(|e| e.attribute_name.as_str());
    let mut filters: Vec<(&str, &AttributeValue)> = Vec::with_capacity(conditions.len());
    for c in conditions {
        if Some(c.attribute_name.as_str()) == hash_attr {
            continue;
        }
        if !matches!(c.value, AttributeValue::S(_) | AttributeValue::B(_)) {
            return None;
        }
        filters.push((c.attribute_name.as_str(), &c.value));
    }
    (!conditions.is_empty()).then_some(filters)
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

#[cfg(all(test, feature = "search-inequality-filters"))]
#[path = "search_vectors_condition_tests.rs"]
mod tests;
