// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Legacy filter parameter desugaring.
//!
//! Converts pre-expression parameters (`KeyConditions`, `QueryFilter`, `ScanFilter`)
//! into expression-based equivalents.

use std::cmp::Ordering;
use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    CompareOp, Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition,
    attribute_type_code, compare_orderable,
};
use extenddb_core::types::{AttributeValue, Condition, ConditionalOperator};

/// Desugar legacy `KeyConditions` into a `KeyCondition` struct and expression maps.
///
/// The hash key must have `ComparisonOperator` = "EQ".
/// The optional range key supports: EQ, LE, LT, GE, GT, `BEGINS_WITH`, BETWEEN.
pub fn desugar_key_conditions(
    conditions: &HashMap<String, Condition>,
    key_schema: &[(String, bool)], // Vec of (attr_name, is_hash)
) -> Result<(KeyCondition, ExpressionMaps), DynamoDbError> {
    // Find the hash key condition
    let hash_attr = key_schema
        .iter()
        .find(|(_, is_hash)| *is_hash)
        .map(|(name, _)| name.as_str())
        .ok_or_else(|| {
            DynamoDbError::ValidationException("No hash key in key schema".to_owned())
        })?;

    validate_legacy_conditions(conditions)?;

    let hash_cond = conditions.get(hash_attr).ok_or_else(|| {
        DynamoDbError::ValidationException(format!(
            "Query condition missed key schema element: {hash_attr}"
        ))
    })?;

    if hash_cond.comparison_operator != "EQ" {
        return Err(DynamoDbError::ValidationException(
            "Query key condition not supported".to_owned(),
        ));
    }

    let mut values = HashMap::new();
    let pk_placeholder = "_kc_pk".to_owned();
    values.insert(
        pk_placeholder.clone(),
        hash_cond.attribute_value_list[0].clone(),
    );

    let pk_path = vec![PathElement::Attribute(hash_attr.to_owned())];
    let pk_value = Expr::Placeholder(pk_placeholder.clone());

    // Find optional range key condition
    let range_attr = key_schema
        .iter()
        .find(|(_, is_hash)| !*is_hash)
        .map(|(name, _)| name.as_str());

    let sk_condition = if let Some(range_name) = range_attr {
        if let Some(range_cond) = conditions.get(range_name) {
            Some(desugar_sk_condition(range_cond, range_name, &mut values)?)
        } else {
            None
        }
    } else {
        None
    };

    // Check for non-key attributes in KeyConditions (invalid)
    for attr_name in conditions.keys() {
        if attr_name != hash_attr && range_attr != Some(attr_name.as_str()) {
            return Err(DynamoDbError::ValidationException(format!(
                "Query condition missed key schema element: {hash_attr}"
            )));
        }
    }

    let maps = ExpressionMaps::new(HashMap::new(), values);

    Ok((
        KeyCondition {
            pk_path,
            pk_value,
            sk_condition,
            extra_pk_conditions: Vec::new(),
            extra_sk_conditions: Vec::new(),
        },
        maps,
    ))
}

fn desugar_sk_condition(
    cond: &Condition,
    range_name: &str,
    values: &mut HashMap<String, AttributeValue>,
) -> Result<SortKeyCondition, DynamoDbError> {
    let path = vec![PathElement::Attribute(range_name.to_owned())];
    match cond.comparison_operator.as_str() {
        "EQ" | "LE" | "LT" | "GE" | "GT" => {
            let placeholder = "_kc_sk".to_owned();
            values.insert(placeholder.clone(), cond.attribute_value_list[0].clone());
            let op = match cond.comparison_operator.as_str() {
                "EQ" => CompareOp::Eq,
                "LE" => CompareOp::Le,
                "LT" => CompareOp::Lt,
                "GE" => CompareOp::Ge,
                "GT" => CompareOp::Gt,
                _ => unreachable!(),
            };
            Ok(SortKeyCondition::Compare {
                path,
                op,
                value: Expr::Placeholder(placeholder),
            })
        }
        "BETWEEN" => {
            let lo_placeholder = "_kc_sk_lo".to_owned();
            let hi_placeholder = "_kc_sk_hi".to_owned();
            values.insert(lo_placeholder.clone(), cond.attribute_value_list[0].clone());
            values.insert(hi_placeholder.clone(), cond.attribute_value_list[1].clone());
            Ok(SortKeyCondition::Between {
                path,
                low: Expr::Placeholder(lo_placeholder),
                high: Expr::Placeholder(hi_placeholder),
            })
        }
        "BEGINS_WITH" => {
            let placeholder = "_kc_sk".to_owned();
            values.insert(placeholder.clone(), cond.attribute_value_list[0].clone());
            Ok(SortKeyCondition::BeginsWith {
                path,
                prefix: Expr::Placeholder(placeholder),
            })
        }
        // NE, NULL, NOT_NULL, CONTAINS, NOT_CONTAINS and IN pass the value
        // checks and are then refused on a key (measured 2026-10-23).
        _ => Err(DynamoDbError::ValidationException(format!(
            "{INVALID_PARAMS}Attempted conditional constraint is not an indexable operation"
        ))),
    }
}

/// Desugar legacy `QueryFilter` or `ScanFilter` into a condition expression AST.
///
/// Each entry maps an attribute name to a Condition with `ComparisonOperator` and values.
/// Multiple entries are combined with the `ConditionalOperator` (default AND).
pub fn desugar_filter(
    filter: &HashMap<String, Condition>,
    conditional_operator: ConditionalOperator,
) -> Result<(Expr, ExpressionMaps), DynamoDbError> {
    if filter.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "Filter must not be empty".to_owned(),
        ));
    }

    let mut conditions = Vec::new();
    let mut value_map: HashMap<String, AttributeValue> = HashMap::new();
    let mut counter = 0u32;

    let mut sorted_entries: Vec<_> = filter.iter().collect();
    sorted_entries.sort_by_key(|(name, _)| *name);

    for (attr_name, cond) in sorted_entries {
        let path = Expr::Path(vec![PathElement::Attribute(attr_name.clone())]);
        let expr = desugar_one_filter_condition(&path, cond, &mut value_map, &mut counter)?;
        conditions.push(expr);
    }

    let combined = if conditions.len() == 1 {
        conditions.into_iter().next().unwrap()
    } else {
        match conditional_operator {
            ConditionalOperator::And => conditions
                .into_iter()
                .reduce(|acc, c| Expr::And(Box::new(acc), Box::new(c)))
                .unwrap(),
            ConditionalOperator::Or => conditions
                .into_iter()
                .reduce(|acc, c| Expr::Or(Box::new(acc), Box::new(c)))
                .unwrap(),
        }
    };

    let maps = ExpressionMaps::new(HashMap::new(), value_map);
    Ok((combined, maps))
}

const INVALID_PARAMS: &str = "One or more parameter values were invalid: ";

/// Validate one legacy condition (`ScanFilter`, `QueryFilter` or
/// `KeyConditions` entry) the way the service does at request time, before
/// the table is looked up and before any row is read, with its messages
/// (measured 2026-10-23 on empty tables). In order:
///
/// 1. the operator is one of the thirteen (`Invalid comparison operator`);
/// 2. the value count fits the operator: none for `NULL` and `NOT_NULL`, two
///    for `BETWEEN`, one or more for `IN`, one otherwise (`Invalid number of
///    argument(s) for the <op> ComparisonOperator`);
/// 3. the first value's type is one the operator takes: `EQ` and `NE` take
///    every type, `BEGINS_WITH` takes `S` and `B`, `CONTAINS` and
///    `NOT_CONTAINS` take `S`, `N`, `B`, `BOOL` and `NULL`, the ordering
///    operators, `BETWEEN` and `IN` take `S`, `N` and `B` (`ComparisonOperator
///    <op> is not valid for <type> AttributeValue type`);
/// 4. every value has the first value's type (`AttributeValues inside
///    AttributeValueList must be of same type`);
/// 5. a `BETWEEN` lower bound is not above its upper bound (`The BETWEEN
///    condition was provided a range where the lower bound is greater than
///    the upper bound`).
///
/// The order of step 2 against step 3 within one condition was not measured;
/// each was measured against a well-formed other.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text,
/// prefixed `One or more parameter values were invalid: `.
pub fn validate_legacy_condition(
    operator: &str,
    values: &[AttributeValue],
) -> Result<(), DynamoDbError> {
    let refuse = |text: String| {
        Err(DynamoDbError::ValidationException(format!(
            "{INVALID_PARAMS}{text}"
        )))
    };
    let count_ok = match operator {
        "NULL" | "NOT_NULL" => values.is_empty(),
        "BETWEEN" => values.len() == 2,
        "IN" => !values.is_empty(),
        "EQ" | "NE" | "LE" | "LT" | "GE" | "GT" | "CONTAINS" | "NOT_CONTAINS" | "BEGINS_WITH" => {
            values.len() == 1
        }
        _ => return refuse("Invalid comparison operator".to_owned()),
    };
    if !count_ok {
        return refuse(format!(
            "Invalid number of argument(s) for the {operator} ComparisonOperator"
        ));
    }
    let Some(first) = values.first() else {
        return Ok(());
    };
    let scalar = matches!(
        first,
        AttributeValue::S(_) | AttributeValue::N(_) | AttributeValue::B(_)
    );
    let type_ok = match operator {
        "EQ" | "NE" => true,
        "BEGINS_WITH" => matches!(first, AttributeValue::S(_) | AttributeValue::B(_)),
        "CONTAINS" | "NOT_CONTAINS" => {
            scalar || matches!(first, AttributeValue::Bool(_) | AttributeValue::Null)
        }
        _ => scalar,
    };
    if !type_ok {
        return refuse(format!(
            "ComparisonOperator {operator} is not valid for {} AttributeValue type",
            attribute_type_code(first)
        ));
    }
    if values
        .iter()
        .any(|v| attribute_type_code(v) != attribute_type_code(first))
    {
        return refuse("AttributeValues inside AttributeValueList must be of same type".to_owned());
    }
    if operator == "BETWEEN" && compare_orderable(&values[0], &values[1]) == Some(Ordering::Greater)
    {
        return refuse(
            "The BETWEEN condition was provided a range where the lower bound is greater than the upper bound"
                .to_owned(),
        );
    }
    Ok(())
}

/// Run [`validate_legacy_condition`] over every entry of a legacy condition
/// map, in attribute name order, so `KeyConditions` can be checked before the
/// table is looked up (the service refuses a bad `QueryFilter` value before it
/// reports a missing table; a bad `KeyConditions` value was measured ahead of
/// every key schema error).
///
/// # Errors
///
/// The first entry's error, in attribute name order.
pub fn validate_legacy_conditions(
    conditions: &HashMap<String, Condition>,
) -> Result<(), DynamoDbError> {
    let mut names: Vec<&String> = conditions.keys().collect();
    names.sort();
    for name in names {
        let cond = &conditions[name];
        validate_legacy_condition(&cond.comparison_operator, &cond.attribute_value_list)?;
    }
    Ok(())
}

fn desugar_one_filter_condition(
    path: &Expr,
    cond: &Condition,
    values: &mut HashMap<String, AttributeValue>,
    counter: &mut u32,
) -> Result<Expr, DynamoDbError> {
    validate_legacy_condition(&cond.comparison_operator, &cond.attribute_value_list)?;
    match cond.comparison_operator.as_str() {
        "EQ" | "NE" | "LE" | "LT" | "GE" | "GT" => {
            let placeholder = format!("_f{counter}");
            *counter += 1;
            values.insert(placeholder.clone(), cond.attribute_value_list[0].clone());
            let op = match cond.comparison_operator.as_str() {
                "EQ" => CompareOp::Eq,
                "NE" => CompareOp::Ne,
                "LE" => CompareOp::Le,
                "LT" => CompareOp::Lt,
                "GE" => CompareOp::Ge,
                "GT" => CompareOp::Gt,
                _ => unreachable!(),
            };
            Ok(Expr::Compare {
                op,
                left: Box::new(path.clone()),
                right: Box::new(Expr::Placeholder(placeholder)),
            })
        }
        "BETWEEN" => {
            let lo = format!("_f{counter}");
            *counter += 1;
            let hi = format!("_f{counter}");
            *counter += 1;
            values.insert(lo.clone(), cond.attribute_value_list[0].clone());
            values.insert(hi.clone(), cond.attribute_value_list[1].clone());
            Ok(Expr::Between {
                operand: Box::new(path.clone()),
                low: Box::new(Expr::Placeholder(lo)),
                high: Box::new(Expr::Placeholder(hi)),
            })
        }
        "BEGINS_WITH" => {
            let placeholder = format!("_f{counter}");
            *counter += 1;
            values.insert(placeholder.clone(), cond.attribute_value_list[0].clone());
            Ok(Expr::Function {
                name: "begins_with".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })
        }
        "CONTAINS" => {
            let placeholder = format!("_f{counter}");
            *counter += 1;
            values.insert(placeholder.clone(), cond.attribute_value_list[0].clone());
            Ok(Expr::Function {
                name: "contains".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })
        }
        "NOT_CONTAINS" => {
            let placeholder = format!("_f{counter}");
            *counter += 1;
            values.insert(placeholder.clone(), cond.attribute_value_list[0].clone());
            Ok(Expr::Not(Box::new(Expr::Function {
                name: "contains".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })))
        }
        "NULL" => Ok(Expr::Function {
            name: "attribute_not_exists".to_owned(),
            args: vec![path.clone()],
        }),
        "NOT_NULL" => Ok(Expr::Function {
            name: "attribute_exists".to_owned(),
            args: vec![path.clone()],
        }),
        "IN" => {
            let placeholders: Vec<Expr> = cond
                .attribute_value_list
                .iter()
                .map(|v| {
                    let placeholder = format!("_f{counter}");
                    *counter += 1;
                    values.insert(placeholder.clone(), v.clone());
                    Expr::Placeholder(placeholder)
                })
                .collect();
            Ok(Expr::In {
                operand: Box::new(path.clone()),
                list: placeholders,
            })
        }
        // validate_legacy_condition refused every other operator above.
        _ => Err(DynamoDbError::ValidationException(format!(
            "{INVALID_PARAMS}Invalid comparison operator"
        ))),
    }
}

#[cfg(test)]
#[path = "legacy_filter_tests.rs"]
mod tests;
