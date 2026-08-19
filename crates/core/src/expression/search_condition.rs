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

/// Maximum number of partition-key conditions allowed in a filter expression.
pub const MAX_PARTITION_KEY_CONDITIONS: usize = 1;

/// Maximum number of inline-filter conditions allowed in a filter expression.
pub const MAX_INLINE_FILTER_CONDITIONS: usize = 20;

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
/// value placeholder, or leaves a supplied value placeholder unused.
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

    // Only equality is supported. Every other comparator contains one of these.
    if trimmed.contains(['<', '>', '!']) {
        return Err(invalid(
            "Invalid comparator used in SearchConditionExpression",
        ));
    }

    // Normalize `=` into its own whitespace-delimited token, then split.
    let normalized = trimmed.replace('=', " = ");
    let tokens: Vec<&str> = normalized.split_whitespace().collect();

    // Only AND is allowed to join conditions.
    for tok in &tokens {
        let upper = tok.to_ascii_uppercase();
        if matches!(upper.as_str(), "OR" | "NOT" | "BETWEEN" | "IN") {
            return Err(invalid(
                "Invalid operator used in SearchConditionExpression",
            ));
        }
    }

    let mut conditions: Vec<SearchCondition> = Vec::new();
    let mut referenced_values: Vec<String> = Vec::new();
    let mut index = 0;

    loop {
        let lhs = tokens
            .get(index)
            .ok_or_else(|| invalid("Invalid SearchConditionExpression"))?;
        let eq = tokens
            .get(index + 1)
            .ok_or_else(|| invalid("Invalid SearchConditionExpression"))?;
        let rhs = tokens
            .get(index + 2)
            .ok_or_else(|| invalid("Invalid SearchConditionExpression"))?;
        if *eq != "=" {
            return Err(invalid("Invalid SearchConditionExpression"));
        }

        // Nested attribute access is not permitted in the filter expression.
        if lhs.contains('.') || lhs.contains('[') {
            return Err(invalid(
                "SearchConditionExpression cannot have conditions on nested attributes",
            ));
        }

        // Resolve the attribute name (#alias via ExpressionAttributeNames).
        let attr_name = if let Some(alias) = lhs.strip_prefix('#') {
            names
                .and_then(|m| m.get(&format!("#{alias}")).or_else(|| m.get(alias)))
                .cloned()
                .ok_or_else(|| {
                    invalid(format!(
                        "An expression attribute name used in the expression is not defined: #{alias}"
                    ))
                })?
        } else {
            (*lhs).to_owned()
        };
        if attr_name.contains('.') || attr_name.contains('[') {
            return Err(invalid(
                "SearchConditionExpression cannot have conditions on nested attributes",
            ));
        }

        // Resolve the value placeholder (:name via ExpressionAttributeValues).
        let placeholder = rhs
            .strip_prefix(':')
            .ok_or_else(|| invalid("Invalid SearchConditionExpression"))?;
        let value = values
            .and_then(|m| {
                m.get(&format!(":{placeholder}"))
                    .or_else(|| m.get(placeholder))
            })
            .cloned()
            .ok_or_else(|| {
                invalid(format!(
                    "Invalid SearchConditionExpression: An expression attribute value used in \
                     expression is not defined; attribute value: :{placeholder}"
                ))
            })?;
        referenced_values.push(format!(":{placeholder}"));

        // At most one condition per attribute.
        if conditions.iter().any(|c| c.attribute_name == attr_name) {
            return Err(invalid(
                "SearchConditionExpression must only contain one condition per attribute",
            ));
        }
        conditions.push(SearchCondition {
            attribute_name: attr_name,
            value,
        });

        match tokens.get(index + 3) {
            None => break,
            Some(tok) if tok.eq_ignore_ascii_case("AND") => index += 4,
            Some(_) => return Err(invalid("Invalid SearchConditionExpression")),
        }
    }

    if conditions.len() > MAX_SEARCH_CONDITIONS {
        return Err(invalid(format!(
            "Invalid SearchConditionExpression: SearchConditionExpression cannot have more than \
             {MAX_PARTITION_KEY_CONDITIONS} partition key and more than \
             {MAX_INLINE_FILTER_CONDITIONS} inline filter key attributes"
        )));
    }

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

    // Every referenced attribute must be part of the index search schema.
    for condition in conditions {
        let in_schema = schema
            .iter()
            .any(|element| element.attribute_name == condition.attribute_name);
        if !in_schema {
            return Err(invalid(
                "SearchConditionExpression must not contain any attributes outside the vector \
                 index search schema",
            ));
        }
    }

    // Every partition-key (HASH) element must be present in the conditions.
    for element in schema
        .iter()
        .filter(|element| element.element_type == SearchSchemaElementType::Hash)
    {
        let present = conditions
            .iter()
            .any(|condition| condition.attribute_name == element.attribute_name);
        if !present {
            return Err(invalid(
                "SearchConditionExpression must have all HASH attributes",
            ));
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
            "Invalid comparator used in SearchConditionExpression"
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
    fn rejects_too_many_conditions() {
        let expr = (0..=MAX_SEARCH_CONDITIONS)
            .map(|i| format!("filter_{i} = :val_{i}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let v: HashMap<String, AttributeValue> = (0..=MAX_SEARCH_CONDITIONS)
            .map(|i| (format!(":val_{i}"), AttributeValue::S(format!("v{i}"))))
            .collect();
        let message = err(validate_search_condition_expression(&expr, None, Some(&v)));
        assert!(message.contains("Invalid SearchConditionExpression"));
        assert!(
            message.contains(
                "cannot have more than 1 partition key and more than 20 inline filter key"
            )
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
}
