// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Structural validation for the vector search filter expression.
//!
//! The vector search API accepts an optional filter expression that restricts
//! candidates before ranking. The grammar is deliberately narrow: a
//! conjunction (`AND`) of equality conditions (`name = :value`) over the
//! attributes declared in the index search schema, with at most one partition
//! key and a bounded number of inline-filter keys.
//!
//! This module performs the structural checks that do not need table or index
//! metadata (comparator, logical operator, nesting, duplicates, count, and
//! placeholder resolution). Schema-aware checks (attribute membership, required
//! partition key, and value-type agreement) run in the engine once the index
//! search schema is known.
//!
//! Pure synchronous Rust: no async, no I/O.

use std::collections::HashMap;

use crate::error::DynamoDbError;
use crate::types::{
    AttributeDefinition, AttributeValue, ScalarAttributeType, SearchSchemaElement,
    SearchSchemaElementType,
};

#[path = "search_condition_phases.rs"]
mod phases;

/// Maximum number of partition-key conditions allowed in a filter expression.
pub const MAX_PARTITION_KEY_CONDITIONS: usize = 1;

/// Maximum number of inline-filter conditions allowed in a filter expression.
/// Measured against the live service 2026-10-29 in us-east-1: 19 conditions are
/// accepted and the 20th is refused, the same 18 as the search schema cap.
pub const MAX_INLINE_FILTER_CONDITIONS: usize = 18;

/// Maximum total number of equality conditions in a filter expression.
pub const MAX_SEARCH_CONDITIONS: usize =
    MAX_PARTITION_KEY_CONDITIONS + MAX_INLINE_FILTER_CONDITIONS;

/// A single resolved equality condition: `attribute_name = value`.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchCondition {
    /// The resolved attribute name (expression-attribute-name aliases applied).
    pub attribute_name: String,
    /// The resolved attribute value (expression-attribute-value applied).
    pub value: AttributeValue,
}

fn invalid(msg: impl Into<String>) -> DynamoDbError {
    DynamoDbError::ValidationException(msg.into())
}

/// Validate a vector search filter expression and return its resolved
/// equality conditions.
///
/// Performs only the schema-independent structural checks. The returned
/// conditions carry resolved attribute names and values for schema-aware
/// validation by the caller.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when the expression uses a
/// disallowed comparator or logical operator, references a nested attribute,
/// repeats an attribute, exceeds the condition count, references an undefined
/// value placeholder, or leaves a supplied value placeholder unused. Syntax and
/// name and value resolution are checked over the whole expression before the
/// count; the comparator, nested-path and duplicate checks run per comparison
/// after it.
pub fn validate_search_condition_expression(
    expr: &str,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
) -> Result<Vec<SearchCondition>, DynamoDbError> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Err(invalid(
            "Invalid SearchConditionExpression: the expression must not be empty",
        ));
    }

    // Phase 1 checks the whole expression and phase 2 walks the comparisons; see
    // search_condition_phases.rs for the measured order.
    let comparisons = phases::whole_expression(trimmed, names, values)?;
    let referenced_values: Vec<String> = comparisons
        .iter()
        .filter_map(|comparison| comparison.placeholder.clone())
        .collect();
    let conditions = phases::each_comparison(comparisons)?;

    // Every supplied value placeholder must be referenced by the expression.
    if let Some(vals) = values {
        for key in vals.keys() {
            let normalized_key = if key.starts_with(':') {
                key.clone()
            } else {
                format!(":{key}")
            };
            if !referenced_values.contains(&normalized_key) {
                return Err(invalid(
                    "Value provided in ExpressionAttributeValues unused in expressions",
                ));
            }
        }
    }

    Ok(conditions)
}

/// Validate resolved filter conditions against a vector index search schema.
///
/// Enforces that every referenced attribute belongs to the search schema, that
/// all partition-key (`HASH`) attributes are present, and that each supplied
/// value agrees with the attribute type declared in the table.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when a condition references an
/// attribute outside the search schema, omits a required partition key, or
/// supplies a value whose type disagrees with the search schema.
pub fn validate_conditions_against_search_schema(
    conditions: &[SearchCondition],
    search_schema: Option<&[SearchSchemaElement]>,
    attribute_definitions: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    let schema = search_schema.unwrap_or(&[]);

    // Every partition-key (HASH) element must be present in the conditions.
    //
    // Ordered BEFORE the out-of-schema check deliberately: measured against the
    // service 2026-08-19 (us-east-1), an expression whose only attribute is
    // out-of-schema AND which omits the HASH is refused for the missing HASH,
    // not the unknown attribute. Missing-HASH takes precedence.
    for element in schema
        .iter()
        .filter(|element| element.element_type == SearchSchemaElementType::Hash)
    {
        let present = conditions
            .iter()
            .any(|condition| condition.attribute_name == element.attribute_name);
        if !present {
            return Err(invalid(
                // Measured against the service 2026-08-19 (us-east-1): an
                // expression that omits a declared HASH attribute is refused
                // with exactly this text.
                "SearchConditionExpression must have all HASH attributes in configured SearchSchema",
            ));
        }
    }

    // Every referenced attribute must be part of the index search schema.
    //
    // Wording measured against the service on 2026-08-11. It names the offending
    // attribute, which the previous text did not, so a caller with several
    // conditions had to work out which one was at fault. The service's own grammar
    // slip ("attributes that is not") is reproduced deliberately: parity means
    // matching what clients actually receive, not correcting it.
    for condition in conditions {
        let in_schema = schema
            .iter()
            .any(|element| element.attribute_name == condition.attribute_name);
        if !in_schema {
            return Err(invalid(format!(
                "SearchConditionExpression must not contain any attributes that is not in \
                 SearchSchema. Invalid attribute: {}",
                condition.attribute_name
            )));
        }
    }

    // Each value type must agree with the declared attribute type.
    for condition in conditions {
        if let Some(definition) = attribute_definitions
            .iter()
            .find(|definition| definition.attribute_name == condition.attribute_name)
            && !value_matches_scalar_type(&condition.value, definition.attribute_type)
        {
            return Err(invalid(format!(
                "Search condition value for attribute '{}' does not match type in search schema",
                condition.attribute_name
            )));
        }
    }

    Ok(())
}

/// Whether an attribute value matches a scalar key type (`S`, `N`, or `B`).
fn value_matches_scalar_type(value: &AttributeValue, scalar_type: ScalarAttributeType) -> bool {
    matches!(
        (value, scalar_type),
        (AttributeValue::S(_), ScalarAttributeType::S)
            | (AttributeValue::N(_), ScalarAttributeType::N)
            | (AttributeValue::B(_), ScalarAttributeType::B)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn values(pairs: &[(&str, &str)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), AttributeValue::S((*v).to_owned())))
            .collect()
    }

    fn err<T: std::fmt::Debug>(r: Result<T, DynamoDbError>) -> String {
        match r.unwrap_err() {
            DynamoDbError::ValidationException(m) => m,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    /// A bare reserved word is refused, with the service's wording.
    ///
    /// The check already existed for `ProjectionExpression`, where its message is
    /// byte-identical to the service, but was never applied here, so
    /// `bucket = :b` was accepted and returned results for a request the service
    /// refuses. `bucket` is the specific word that exposed it against the live
    /// service on 2026-08-11.
    #[test]
    fn a_bare_reserved_keyword_is_refused() {
        let v = values(&[(":b", "4")]);
        let message = err(validate_search_condition_expression(
            "bucket = :b",
            None,
            Some(&v),
        ));
        assert_eq!(
            message,
            "Invalid SearchConditionExpression: Attribute name is a reserved keyword; \
             reserved keyword: bucket"
        );
    }

    /// Aliasing is the documented escape hatch, so the rule must apply to the bare
    /// form only. Without this, the fix above would make reserved-word attributes
    /// unusable rather than merely requiring an alias.
    #[test]
    fn an_aliased_reserved_keyword_is_accepted() {
        let n = names(&[("#b", "bucket")]);
        let v = values(&[(":b", "4")]);
        let conditions = validate_search_condition_expression("#b = :b", Some(&n), Some(&v))
            .expect("an aliased reserved word must be allowed");
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].attribute_name, "bucket");
    }

    /// The condition is rejected when it names an attribute outside the schema, and
    /// the message names WHICH attribute, as the service's does.
    #[test]
    fn an_attribute_outside_the_search_schema_is_named_in_the_error() {
        let conditions = vec![
            SearchCondition {
                attribute_name: "cat".to_owned(),
                value: AttributeValue::S("alpha".to_owned()),
            },
            SearchCondition {
                attribute_name: "payload".to_owned(),
                value: AttributeValue::S("nope".to_owned()),
            },
        ];
        let schema = [SearchSchemaElement {
            attribute_name: "cat".to_owned(),
            element_type: SearchSchemaElementType::Hash,
        }];
        let message = err(validate_conditions_against_search_schema(
            &conditions,
            Some(&schema),
            &[],
        ));
        assert_eq!(
            message,
            "SearchConditionExpression must not contain any attributes that is not in \
             SearchSchema. Invalid attribute: payload"
        );
    }

    #[test]
    fn single_literal_equality_resolves() {
        let v = values(&[(":cat", "Electronics")]);
        let out = validate_search_condition_expression("Category = :cat", None, Some(&v)).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].attribute_name, "Category");
        assert_eq!(out[0].value, AttributeValue::S("Electronics".to_owned()));
    }

    #[test]
    fn resolves_name_and_value_aliases() {
        let n = names(&[("#country", "Country")]);
        let v = values(&[(":c", "USA")]);
        let out =
            validate_search_condition_expression("#country = :c", Some(&n), Some(&v)).unwrap();
        assert_eq!(out[0].attribute_name, "Country");
    }

    #[test]
    fn empty_expression_is_invalid() {
        assert!(
            err(validate_search_condition_expression("", None, None))
                .contains("Invalid SearchConditionExpression")
        );
    }

    #[test]
    fn rejects_non_equality_comparator() {
        let n = names(&[("#country", "Country")]);
        let v = values(&[(":c", "USA")]);
        assert_eq!(
            err(validate_search_condition_expression(
                "#country <> :c",
                Some(&n),
                Some(&v)
            )),
            "Invalid SearchConditionExpression: Invalid comparator used in SearchConditionExpression"
        );
    }

    #[test]
    fn rejects_logical_or() {
        let n = names(&[("#country", "Country"), ("#category", "Category")]);
        let v = values(&[(":c", "USA"), (":cat", "Electronics")]);
        assert_eq!(
            err(validate_search_condition_expression(
                "#country = :c OR #category = :cat",
                Some(&n),
                Some(&v)
            )),
            "Invalid operator used in SearchConditionExpression"
        );
    }

    #[test]
    fn rejects_nested_attribute() {
        let n = names(&[
            ("#country", "Country"),
            ("#product", "Product"),
            ("#category", "Category"),
        ]);
        let v = values(&[(":c", "USA"), (":cat", "Electronics")]);
        assert!(
            err(validate_search_condition_expression(
                "#country = :c AND #product.#category = :cat",
                Some(&n),
                Some(&v)
            ))
            .contains("cannot have conditions on nested attributes")
        );
    }

    #[test]
    fn rejects_duplicate_attribute() {
        let n = names(&[("#country", "Country"), ("#category", "Category")]);
        let v = values(&[(":c", "USA"), (":cat1", "Electronics"), (":cat2", "Books")]);
        assert!(
            err(validate_search_condition_expression(
                "#country = :c AND #category = :cat1 AND #category = :cat2",
                Some(&n),
                Some(&v)
            ))
            .contains("must only contain one condition per attribute")
        );
    }

    #[test]
    fn rejects_missing_value_placeholder() {
        assert_eq!(
            err(validate_search_condition_expression(
                "Category = :cat",
                None,
                None
            )),
            "Invalid SearchConditionExpression: An expression attribute value used in \
             expression is not defined; attribute value: :cat"
        );
    }

    #[test]
    fn rejects_unused_value_placeholder() {
        let v = values(&[(":cat", "Electronics"), (":unused", "x")]);
        assert_eq!(
            err(validate_search_condition_expression(
                "Category = :cat",
                None,
                Some(&v)
            )),
            "Value provided in ExpressionAttributeValues unused in expressions"
        );
    }

    #[test]
    fn accepts_partition_key_plus_inline_filters() {
        let n = names(&[
            ("#country", "Country"),
            ("#category", "Category"),
            ("#brand", "Brand"),
        ]);
        let v = values(&[(":c", "USA"), (":cat", "Electronics"), (":brand", "Apple")]);
        let out = validate_search_condition_expression(
            "#country = :c AND #category = :cat AND #brand = :brand",
            Some(&n),
            Some(&v),
        )
        .unwrap();
        assert_eq!(out.len(), 3);
    }

    fn schema() -> Vec<SearchSchemaElement> {
        vec![
            SearchSchemaElement {
                attribute_name: "Country".to_owned(),
                element_type: SearchSchemaElementType::Hash,
            },
            SearchSchemaElement {
                attribute_name: "Category".to_owned(),
                element_type: SearchSchemaElementType::InlineFilter,
            },
        ]
    }

    fn defs() -> Vec<AttributeDefinition> {
        vec![
            AttributeDefinition {
                attribute_name: "Country".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "Category".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ]
    }

    fn cond(name: &str, value: AttributeValue) -> SearchCondition {
        SearchCondition {
            attribute_name: name.to_owned(),
            value,
        }
    }

    #[test]
    fn schema_accepts_valid_conditions() {
        let conds = vec![
            cond("Country", AttributeValue::S("USA".to_owned())),
            cond("Category", AttributeValue::S("Electronics".to_owned())),
        ];
        validate_conditions_against_search_schema(&conds, Some(&schema()), &defs()).unwrap();
    }

    #[test]
    fn schema_rejects_unknown_attribute() {
        let conds = vec![
            cond("Country", AttributeValue::S("USA".to_owned())),
            cond("Price", AttributeValue::N("100".to_owned())),
        ];
        assert!(
            err(validate_conditions_against_search_schema(
                &conds,
                Some(&schema()),
                &defs()
            ))
            .contains("SearchConditionExpression must not contain any attributes")
        );
    }

    #[test]
    fn schema_requires_partition_key() {
        let conds = vec![cond(
            "Category",
            AttributeValue::S("Electronics".to_owned()),
        )];
        assert!(
            err(validate_conditions_against_search_schema(
                &conds,
                Some(&schema()),
                &defs()
            ))
            .contains("SearchConditionExpression must have all HASH attributes")
        );
    }

    #[test]
    fn schema_rejects_type_mismatch() {
        let conds = vec![cond("Country", AttributeValue::N("123".to_owned()))];
        assert!(
            err(validate_conditions_against_search_schema(
                &conds,
                Some(&schema()),
                &defs()
            ))
            .contains("does not match type in search schema")
        );
    }

    /// An absent `SearchConditionExpression` against an index that declares a
    /// HASH element is itself a validation failure, so the caller may run this
    /// unconditionally rather than guarding on a non-empty condition list.
    /// Guarding it would let such a search reach a backend with no partition
    /// scope, while `VectorSearch::hash_key` promises backends that `Some` is a
    /// mandatory predicate whenever the index declares a HASH element.
    #[test]
    fn schema_requires_partition_key_when_no_conditions_supplied() {
        assert!(
            err(validate_conditions_against_search_schema(
                &[],
                Some(&schema()),
                &defs()
            ))
            .contains("SearchConditionExpression must have all HASH attributes")
        );
    }

    /// Converse control: with no search schema there is no HASH element to
    /// require, so an absent expression stays valid. Without this the test
    /// above would also pass for a function that rejected every empty input.
    #[test]
    fn no_schema_and_no_conditions_is_valid() {
        validate_conditions_against_search_schema(&[], None, &defs()).unwrap();
        validate_conditions_against_search_schema(&[], Some(&[]), &defs()).unwrap();
    }
}

#[cfg(test)]
#[path = "search_condition_limit_tests.rs"]
mod limit_tests;

#[cfg(test)]
#[path = "search_condition_order_tests.rs"]
mod order_tests;
