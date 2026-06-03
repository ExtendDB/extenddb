// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Legacy `Expected` parameter desugaring.
//!
//! Converts the pre-expression `Expected` map into a `ConditionExpression` AST
//! and `ExpressionMaps`. This allows the storage layer to use a single code path
//! for both legacy and expression-based conditions.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps, PathElement};
use extenddb_core::types::{AttributeValue, ConditionalOperator, ExpectedAttributeValue};

/// Desugar a legacy `Expected` map into a condition expression AST and maps.
///
/// `DynamoDB` rejects requests that specify both `Expected` and `ConditionExpression`.
/// The caller must enforce this before calling this function.
///
/// # Errors
///
/// Returns `ValidationException` for invalid `Expected` entries.
pub fn desugar_expected(
    expected: &HashMap<String, ExpectedAttributeValue>,
    conditional_operator: ConditionalOperator,
) -> Result<(Expr, ExpressionMaps), DynamoDbError> {
    if expected.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "Expected must not be empty".to_owned(),
        ));
    }

    let mut conditions = Vec::new();
    let mut value_map = HashMap::new();
    let mut placeholder_counter = 0u32;

    // Sort by attribute name for deterministic condition ordering.
    let mut sorted_entries: Vec<_> = expected.iter().collect();
    sorted_entries.sort_by_key(|(name, _)| *name);

    for (attr_name, expected_val) in sorted_entries {
        let path = Expr::Path(vec![PathElement::Attribute(attr_name.clone())]);

        let condition = desugar_one(
            &path,
            expected_val,
            &mut value_map,
            &mut placeholder_counter,
        )?;
        conditions.push(condition);
    }

    let combined = combine_conditions(conditions, conditional_operator);
    let maps = ExpressionMaps::new(HashMap::new(), value_map);

    Ok((combined, maps))
}

/// Desugar a single `ExpectedAttributeValue` into a condition expression.
#[allow(clippy::too_many_lines)]
fn desugar_one(
    path: &Expr,
    expected: &ExpectedAttributeValue,
    values: &mut HashMap<String, AttributeValue>,
    counter: &mut u32,
) -> Result<Expr, DynamoDbError> {
    // Case 1: Exists = true/false
    if let Some(exists) = expected.exists {
        if expected.comparison_operator.is_some() {
            return Err(DynamoDbError::ValidationException(
                "One or more parameter values were invalid: Exists and ComparisonOperator cannot be used together"
                    .to_owned(),
            ));
        }
        if !exists && expected.value.is_some() {
            return Err(DynamoDbError::ValidationException(
                "One or more parameter values were invalid: Value cannot be used when Exists is set to false"
                    .to_owned(),
            ));
        }
        // Exists: true + Value → EQ comparison (legacy shorthand)
        if exists && let Some(val) = &expected.value {
            let placeholder = next_placeholder(counter);
            values.insert(placeholder.clone(), val.clone());
            return Ok(Expr::Compare {
                left: Box::new(path.clone()),
                op: CompareOp::Eq,
                right: Box::new(Expr::Placeholder(placeholder)),
            });
        }
        let fn_name = if exists {
            "attribute_exists"
        } else {
            "attribute_not_exists"
        };
        return Ok(Expr::Function {
            name: fn_name.to_owned(),
            args: vec![path.clone()],
        });
    }

    // Case 2: Value shorthand (no ComparisonOperator) → EQ
    if expected.comparison_operator.is_none() {
        let val = expected.value.as_ref().ok_or_else(|| {
            DynamoDbError::ValidationException(
                "One or more parameter values were invalid: Value or ComparisonOperator must be specified"
                    .to_owned(),
            )
        })?;
        let placeholder = next_placeholder(counter);
        values.insert(placeholder.clone(), val.clone());
        return Ok(Expr::Compare {
            left: Box::new(path.clone()),
            op: CompareOp::Eq,
            right: Box::new(Expr::Placeholder(placeholder)),
        });
    }

    // Case 3: ComparisonOperator with AttributeValueList (or Value fallback).
    // comparison_operator is guaranteed Some here: Case 1 handles exists,
    // Case 2 handles comparison_operator.is_none().
    // Real DynamoDB accepts Value as a single-element AttributeValueList when
    // ComparisonOperator is present. The Java SDK uses this pattern for
    // Expected conditions on set attributes (REQ-DATA-013).
    let Some(op_str) = expected.comparison_operator.as_deref() else {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Value or ComparisonOperator must be specified"
                .to_owned(),
        ));
    };
    let value_as_list;
    let vals: &[AttributeValue] = match expected.attribute_value_list.as_deref() {
        Some(list) if !list.is_empty() => list,
        _ => match &expected.value {
            Some(v) => {
                value_as_list = [v.clone()];
                &value_as_list
            }
            None => &[],
        },
    };

    match op_str {
        "EQ" | "NE" | "LE" | "LT" | "GE" | "GT" => {
            if vals.len() != 1 {
                return Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: {op_str} requires exactly one AttributeValueList member"
                )));
            }
            let op = match op_str {
                "EQ" => CompareOp::Eq,
                "NE" => CompareOp::Ne,
                "LE" => CompareOp::Le,
                "LT" => CompareOp::Lt,
                "GE" => CompareOp::Ge,
                "GT" => CompareOp::Gt,
                _ => unreachable!(),
            };
            let placeholder = next_placeholder(counter);
            values.insert(placeholder.clone(), vals[0].clone());
            Ok(Expr::Compare {
                left: Box::new(path.clone()),
                op,
                right: Box::new(Expr::Placeholder(placeholder)),
            })
        }
        "BETWEEN" => {
            if vals.len() != 2 {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: BETWEEN requires exactly two AttributeValueList members"
                        .to_owned(),
                ));
            }
            let p_low = next_placeholder(counter);
            let p_high = next_placeholder(counter);
            values.insert(p_low.clone(), vals[0].clone());
            values.insert(p_high.clone(), vals[1].clone());
            Ok(Expr::Between {
                operand: Box::new(path.clone()),
                low: Box::new(Expr::Placeholder(p_low)),
                high: Box::new(Expr::Placeholder(p_high)),
            })
        }
        "BEGINS_WITH" => {
            if vals.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: BEGINS_WITH requires exactly one AttributeValueList member"
                        .to_owned(),
                ));
            }
            let placeholder = next_placeholder(counter);
            values.insert(placeholder.clone(), vals[0].clone());
            Ok(Expr::Function {
                name: "begins_with".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })
        }
        "CONTAINS" => {
            if vals.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: CONTAINS requires exactly one AttributeValueList member"
                        .to_owned(),
                ));
            }
            let placeholder = next_placeholder(counter);
            values.insert(placeholder.clone(), vals[0].clone());
            Ok(Expr::Function {
                name: "contains".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })
        }
        "NOT_CONTAINS" => {
            if vals.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: NOT_CONTAINS requires exactly one AttributeValueList member"
                        .to_owned(),
                ));
            }
            let placeholder = next_placeholder(counter);
            values.insert(placeholder.clone(), vals[0].clone());
            Ok(Expr::Not(Box::new(Expr::Function {
                name: "contains".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })))
        }
        "IN" => {
            if vals.is_empty() {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: IN requires at least one AttributeValueList member"
                        .to_owned(),
                ));
            }
            let list: Vec<Expr> = vals
                .iter()
                .map(|v| {
                    let p = next_placeholder(counter);
                    values.insert(p.clone(), v.clone());
                    Expr::Placeholder(p)
                })
                .collect();
            Ok(Expr::In {
                operand: Box::new(path.clone()),
                list,
            })
        }
        "NOT_NULL" => Ok(Expr::Function {
            name: "attribute_exists".to_owned(),
            args: vec![path.clone()],
        }),
        "NULL" => Ok(Expr::Function {
            name: "attribute_not_exists".to_owned(),
            args: vec![path.clone()],
        }),
        other => Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: unknown ComparisonOperator: {other}"
        ))),
    }
}

/// Combine multiple conditions with AND or OR.
fn combine_conditions(mut conditions: Vec<Expr>, op: ConditionalOperator) -> Expr {
    debug_assert!(!conditions.is_empty());
    let mut result = conditions.remove(0);
    for cond in conditions {
        result = match op {
            ConditionalOperator::And => Expr::And(Box::new(result), Box::new(cond)),
            ConditionalOperator::Or => Expr::Or(Box::new(result), Box::new(cond)),
        };
    }
    result
}

/// Generate a unique placeholder name for desugared values.
fn next_placeholder(counter: &mut u32) -> String {
    let name = format!("__expected_{counter}");
    *counter += 1;
    name
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::ConditionalOperator;

    fn make_expected(
        value: Option<AttributeValue>,
        comparison_operator: Option<&str>,
        attribute_value_list: Option<Vec<AttributeValue>>,
    ) -> ExpectedAttributeValue {
        ExpectedAttributeValue {
            value,
            exists: None,
            comparison_operator: comparison_operator.map(String::from),
            attribute_value_list,
        }
    }

    #[test]
    fn eq_with_value_and_comparison_operator() {
        // Java SDK pattern: ComparisonOperator=EQ + Value (no AttributeValueList).
        // Real DynamoDB treats Value as a single-element AttributeValueList.
        let mut expected = HashMap::new();
        expected.insert(
            "attr".to_owned(),
            make_expected(
                Some(AttributeValue::S("hello".to_owned())),
                Some("EQ"),
                None,
            ),
        );
        let result = desugar_expected(&expected, ConditionalOperator::And);
        assert!(
            result.is_ok(),
            "should accept Value + ComparisonOperator=EQ"
        );
    }

    #[test]
    fn eq_with_attribute_value_list() {
        // Standard pattern: ComparisonOperator=EQ + AttributeValueList=[value].
        let mut expected = HashMap::new();
        expected.insert(
            "attr".to_owned(),
            make_expected(
                None,
                Some("EQ"),
                Some(vec![AttributeValue::S("hello".to_owned())]),
            ),
        );
        let result = desugar_expected(&expected, ConditionalOperator::And);
        assert!(result.is_ok());
    }

    #[test]
    fn eq_with_no_value_and_no_list_fails() {
        // ComparisonOperator=EQ with neither Value nor AttributeValueList.
        let mut expected = HashMap::new();
        expected.insert("attr".to_owned(), make_expected(None, Some("EQ"), None));
        let result = desugar_expected(&expected, ConditionalOperator::And);
        assert!(result.is_err());
    }

    #[test]
    fn value_shorthand_without_comparison_operator() {
        // Legacy shorthand: Value only (no ComparisonOperator) → EQ.
        let mut expected = HashMap::new();
        expected.insert(
            "attr".to_owned(),
            make_expected(Some(AttributeValue::N("42".to_owned())), None, None),
        );
        let result = desugar_expected(&expected, ConditionalOperator::And);
        assert!(result.is_ok());
    }
}
