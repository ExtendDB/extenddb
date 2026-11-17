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
use extenddb_core::validation::number::operations::single_write;

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
            attr_name,
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

/// The text `DynamoDB` gives a `ComparisonOperator` with the wrong number of values.
fn invalid_argument_count(op: &str) -> String {
    format!(
        "One or more parameter values were invalid: Invalid number of argument(s) for the {op} ComparisonOperator"
    )
}

/// Desugar a single `ExpectedAttributeValue` into a condition expression.
#[allow(clippy::too_many_lines)]
fn desugar_one(
    attr_name: &str,
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

    // Case 3: ComparisonOperator. `Value` counts as a list of one (the Java
    // SDK sends `Value` for Expected conditions on set attributes,
    // REQ-DATA-013), an empty list as zero values, and NULL and NOT_NULL take
    // zero values (measured in us-east-1 on 2026-11-14 and 2026-11-17).
    let Some(op_str) = expected.comparison_operator.as_deref() else {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Value or ComparisonOperator must be specified"
                .to_owned(),
        ));
    };
    let value_as_list;
    let vals: &[AttributeValue] = match (expected.attribute_value_list.as_deref(), &expected.value)
    {
        (Some(_), Some(_)) => {
            return Err(single_write(format!(
                "One or more parameter values were invalid: Value and AttributeValueList cannot be used together for Attribute: {attr_name}"
            )));
        }
        (None, None) if matches!(op_str, "NULL" | "NOT_NULL") => &[],
        (None, None) => {
            return Err(single_write(format!(
                "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: {op_str} for Attribute: {attr_name}"
            )));
        }
        (Some(list), None) => list,
        (None, Some(v)) => {
            value_as_list = [v.clone()];
            &value_as_list
        }
    };
    let count_ok = match op_str {
        "EQ" | "NE" | "LE" | "LT" | "GE" | "GT" | "BEGINS_WITH" | "CONTAINS" | "NOT_CONTAINS" => {
            vals.len() == 1
        }
        "BETWEEN" => vals.len() == 2,
        "IN" => !vals.is_empty(),
        "NULL" | "NOT_NULL" => vals.is_empty(),
        other => {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: unknown ComparisonOperator: {other}"
            )));
        }
    };
    if !count_ok {
        return Err(single_write(invalid_argument_count(op_str)));
    }

    match op_str {
        "EQ" | "NE" | "LE" | "LT" | "GE" | "GT" => {
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
            let placeholder = next_placeholder(counter);
            values.insert(placeholder.clone(), vals[0].clone());
            Ok(Expr::Function {
                name: "begins_with".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })
        }
        "CONTAINS" => {
            let placeholder = next_placeholder(counter);
            values.insert(placeholder.clone(), vals[0].clone());
            Ok(Expr::Function {
                name: "contains".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })
        }
        "NOT_CONTAINS" => {
            let placeholder = next_placeholder(counter);
            values.insert(placeholder.clone(), vals[0].clone());
            Ok(Expr::Not(Box::new(Expr::Function {
                name: "contains".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })))
        }
        "IN" => {
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
        _ => unreachable!(),
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
        comparison_operator: Option<&str>,
        attribute_value_list: Option<Vec<AttributeValue>>,
        value: Option<AttributeValue>,
    ) -> ExpectedAttributeValue {
        ExpectedAttributeValue {
            value,
            exists: None,
            comparison_operator: comparison_operator.map(String::from),
            attribute_value_list,
        }
    }

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_owned())
    }

    fn desugar_tested(
        tested: ExpectedAttributeValue,
    ) -> Result<(Expr, ExpressionMaps), DynamoDbError> {
        let mut expected = HashMap::new();
        expected.insert("tested".to_owned(), tested);
        desugar_expected(&expected, ConditionalOperator::And)
    }

    fn message(result: Result<(Expr, ExpressionMaps), DynamoDbError>) -> String {
        match result {
            Err(DynamoDbError::ValidationException(text)) => text,
            other => panic!("expected a ValidationException, got {other:?}"),
        }
    }

    /// The texts measured in us-east-1 for an entry `tested` of a PutItem,
    /// UpdateItem or DeleteItem: a wrong number of values (2026-11-13,
    /// 2026-11-14 and 2026-11-17), both `Value` and `AttributeValueList`
    /// (2026-11-17), and neither (2026-11-14).
    #[test]
    fn refused_entries_get_the_measured_text() {
        const PREFIX: &str =
            "1 validation error detected: One or more parameter values were invalid: ";
        const BOTH: &str =
            "Value and AttributeValueList cannot be used together for Attribute: tested";
        const NEITHER: &str = "Value or AttributeValueList must be used with ComparisonOperator: EQ for Attribute: tested";
        let count =
            |op: &str| format!("Invalid number of argument(s) for the {op} ComparisonOperator");
        let two = || Some(vec![s("READY"), s("OTHER")]);
        let mut rows: Vec<(ExpectedAttributeValue, String)> = vec![
            (make_expected(Some("EQ"), Some(vec![]), None), count("EQ")),
            (make_expected(Some("EQ"), two(), None), count("EQ")),
            (make_expected(Some("NE"), two(), None), count("NE")),
            (make_expected(Some("IN"), Some(vec![]), None), count("IN")),
            (
                make_expected(Some("BETWEEN"), None, Some(s("READY"))),
                count("BETWEEN"),
            ),
            (
                make_expected(Some("BETWEEN"), Some(vec![s("x")]), None),
                count("BETWEEN"),
            ),
            (
                make_expected(Some("BETWEEN"), Some(vec![s("A"), s("M"), s("Z")]), None),
                count("BETWEEN"),
            ),
            (
                make_expected(Some("NULL"), None, Some(s("READY"))),
                count("NULL"),
            ),
            (
                make_expected(Some("NULL"), Some(vec![s("READY")]), None),
                count("NULL"),
            ),
            (
                make_expected(Some("NOT_NULL"), None, Some(s("READY"))),
                count("NOT_NULL"),
            ),
            (
                make_expected(Some("EQ"), Some(vec![s("READY")]), Some(s("READY"))),
                BOTH.to_owned(),
            ),
            (
                make_expected(Some("EQ"), Some(vec![]), Some(s("READY"))),
                BOTH.to_owned(),
            ),
            (
                make_expected(Some("NOT_NULL"), Some(vec![s("READY")]), Some(s("READY"))),
                BOTH.to_owned(),
            ),
            (make_expected(Some("EQ"), None, None), NEITHER.to_owned()),
            // An unknown operator gets the text of its value shape first
            // (measured 2026-11-17).
            (
                make_expected(Some("FOO"), None, None),
                "Value or AttributeValueList must be used with ComparisonOperator: FOO for Attribute: tested".to_owned(),
            ),
            (
                make_expected(Some("FOO"), Some(vec![s("READY")]), Some(s("READY"))),
                BOTH.to_owned(),
            ),
        ];
        for op in [
            "LE",
            "LT",
            "GE",
            "GT",
            "BEGINS_WITH",
            "CONTAINS",
            "NOT_CONTAINS",
        ] {
            rows.push((make_expected(Some(op), Some(vec![]), None), count(op)));
            rows.push((make_expected(Some(op), two(), None), count(op)));
        }
        for (tested, want) in rows {
            let label = format!("{tested:?}");
            let got = message(desugar_tested(tested));
            assert_eq!(got, format!("{PREFIX}{want}"), "{label}");
        }
    }

    /// A wrong entry is refused beside a valid one, with no `ConditionalOperator`
    /// (measured 2026-11-14): the two-entry PutItem of #1042.
    #[test]
    fn wrong_entry_beside_a_valid_one_is_refused() {
        let mut expected = HashMap::new();
        expected.insert(
            "a".to_owned(),
            make_expected(Some("BETWEEN"), Some(vec![s("x")]), None),
        );
        expected.insert(
            "b".to_owned(),
            make_expected(Some("EQ"), Some(vec![s("y")]), None),
        );
        assert_eq!(
            message(desugar_expected(&expected, ConditionalOperator::And)),
            format!(
                "1 validation error detected: {}",
                invalid_argument_count("BETWEEN")
            )
        );
    }

    /// The shapes that desugar. The service applied EQ, NE and IN with a
    /// `Value` and BETWEEN with two values on 2026-11-14, and NOT_NULL with an
    /// empty list or no field on 2026-11-17; NULL with no field was refused by
    /// its condition on the seeded item (2026-11-17). One value with the other
    /// operators, a list with IN and a one-value list with EQ are the operators'
    /// documented shape.
    #[test]
    fn accepted_entries_desugar() {
        let one = |v: &str| Some(vec![s(v)]);
        let rows = vec![
            make_expected(Some("EQ"), None, Some(s("READY"))),
            make_expected(Some("EQ"), one("READY"), None),
            make_expected(Some("NE"), None, Some(s("READY"))),
            make_expected(Some("IN"), None, Some(s("READY"))),
            make_expected(Some("IN"), Some(vec![s("a"), s("b"), s("c")]), None),
            make_expected(Some("BETWEEN"), Some(vec![s("a"), s("z")]), None),
            make_expected(Some("LE"), one("z"), None),
            make_expected(Some("LT"), one("z"), None),
            make_expected(Some("GE"), one("a"), None),
            make_expected(Some("GT"), one("a"), None),
            make_expected(Some("BEGINS_WITH"), one("R"), None),
            make_expected(Some("CONTAINS"), one("R"), None),
            make_expected(Some("NOT_CONTAINS"), one("x"), None),
            make_expected(Some("NULL"), None, None),
            make_expected(Some("NOT_NULL"), None, None),
            make_expected(Some("NOT_NULL"), Some(vec![]), None),
        ];
        for tested in rows {
            let label = format!("{tested:?}");
            let result = desugar_tested(tested);
            assert!(result.is_ok(), "{label}: {result:?}");
        }
    }

    #[test]
    fn value_shorthand_without_comparison_operator() {
        // Legacy shorthand: Value only (no ComparisonOperator) → EQ.
        let mut expected = HashMap::new();
        expected.insert(
            "attr".to_owned(),
            make_expected(None, None, Some(AttributeValue::N("42".to_owned()))),
        );
        let result = desugar_expected(&expected, ConditionalOperator::And);
        assert!(result.is_ok());
    }
}
