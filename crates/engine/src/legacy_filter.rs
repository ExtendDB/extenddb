// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Legacy filter parameter desugaring.
//!
//! Converts pre-expression parameters (`KeyConditions`, `QueryFilter`, `ScanFilter`)
//! into expression-based equivalents.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    CompareOp, Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition,
    attribute_type_code,
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

    if hash_cond.attribute_value_list.len() != 1 {
        return Err(DynamoDbError::ValidationException(
            "Query key condition must have exactly one value for EQ".to_owned(),
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
            if cond.attribute_value_list.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "Invalid number of values for comparison".to_owned(),
                ));
            }
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
            if cond.attribute_value_list.len() != 2 {
                return Err(DynamoDbError::ValidationException(
                    "BETWEEN requires exactly 2 values".to_owned(),
                ));
            }
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
            if cond.attribute_value_list.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "BEGINS_WITH requires exactly 1 value".to_owned(),
                ));
            }
            let placeholder = "_kc_sk".to_owned();
            values.insert(placeholder.clone(), cond.attribute_value_list[0].clone());
            Ok(SortKeyCondition::BeginsWith {
                path,
                prefix: Expr::Placeholder(placeholder),
            })
        }
        other => Err(DynamoDbError::ValidationException(format!(
            "Unsupported KeyCondition operator: {other}"
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

/// Refuse a legacy `ComparisonOperator` whose `AttributeValueList` holds a
/// value of a type the operator does not take, with the service's message,
/// before the condition is desugared and before any row is read.
///
/// The service checks this at request time, on an empty table too (measured
/// 2026-10-23: `BEGINS_WITH` with `N`, `LT` and `GT` with `BOOL`, and
/// `BETWEEN` with `L` then `N`, which names the `L`). `EQ` and `NE` take every
/// type; `NULL` and `NOT_NULL` take no value; `BEGINS_WITH` takes `S` and `B`;
/// the ordering operators, `BETWEEN`, `IN`, `CONTAINS` and `NOT_CONTAINS` take
/// `S`, `N` and `B`. The first value of a type the operator does not take is
/// the one named.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the text
/// `One or more parameter values were invalid: ComparisonOperator <op> is not
/// valid for <type> AttributeValue type`.
pub fn check_legacy_operand_types(
    operator: &str,
    values: &[AttributeValue],
) -> Result<(), DynamoDbError> {
    let allowed = |v: &AttributeValue| match operator {
        "EQ" | "NE" | "NULL" | "NOT_NULL" => true,
        "BEGINS_WITH" => matches!(v, AttributeValue::S(_) | AttributeValue::B(_)),
        "LE" | "LT" | "GE" | "GT" | "BETWEEN" | "IN" | "CONTAINS" | "NOT_CONTAINS" => {
            matches!(
                v,
                AttributeValue::S(_) | AttributeValue::N(_) | AttributeValue::B(_)
            )
        }
        _ => true,
    };
    match values.iter().find(|v| !allowed(v)) {
        Some(bad) => Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: ComparisonOperator {operator} is not valid for {} AttributeValue type",
            attribute_type_code(bad)
        ))),
        None => Ok(()),
    }
}

fn desugar_one_filter_condition(
    path: &Expr,
    cond: &Condition,
    values: &mut HashMap<String, AttributeValue>,
    counter: &mut u32,
) -> Result<Expr, DynamoDbError> {
    check_legacy_operand_types(&cond.comparison_operator, &cond.attribute_value_list)?;
    match cond.comparison_operator.as_str() {
        "EQ" | "NE" | "LE" | "LT" | "GE" | "GT" => {
            if cond.attribute_value_list.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "Invalid number of attribute values for comparison".to_owned(),
                ));
            }
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
            if cond.attribute_value_list.len() != 2 {
                return Err(DynamoDbError::ValidationException(
                    "BETWEEN requires exactly 2 values".to_owned(),
                ));
            }
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
            if cond.attribute_value_list.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "BEGINS_WITH requires exactly 1 value".to_owned(),
                ));
            }
            let placeholder = format!("_f{counter}");
            *counter += 1;
            values.insert(placeholder.clone(), cond.attribute_value_list[0].clone());
            Ok(Expr::Function {
                name: "begins_with".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })
        }
        "CONTAINS" => {
            if cond.attribute_value_list.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "CONTAINS requires exactly 1 value".to_owned(),
                ));
            }
            let placeholder = format!("_f{counter}");
            *counter += 1;
            values.insert(placeholder.clone(), cond.attribute_value_list[0].clone());
            Ok(Expr::Function {
                name: "contains".to_owned(),
                args: vec![path.clone(), Expr::Placeholder(placeholder)],
            })
        }
        "NOT_CONTAINS" => {
            if cond.attribute_value_list.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "NOT_CONTAINS requires exactly 1 value".to_owned(),
                ));
            }
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
            if cond.attribute_value_list.is_empty() {
                return Err(DynamoDbError::ValidationException(
                    "IN requires at least 1 value".to_owned(),
                ));
            }
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
        other => Err(DynamoDbError::ValidationException(format!(
            "Unsupported filter operator: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(op: &str, values: &[AttributeValue]) -> Option<String> {
        match check_legacy_operand_types(op, values) {
            Ok(()) => None,
            Err(DynamoDbError::ValidationException(m)) => Some(m),
            Err(other) => panic!("unexpected error {other:?}"),
        }
    }

    fn refusal(op: &str, ty: &str) -> String {
        format!(
            "One or more parameter values were invalid: ComparisonOperator {op} is not valid for {ty} AttributeValue type"
        )
    }

    #[test]
    fn measured_rows_carry_the_service_message() {
        let n = AttributeValue::N("1".to_owned());
        let b = AttributeValue::Bool(true);
        let l = AttributeValue::L(Vec::new());
        assert_eq!(
            msg("BEGINS_WITH", std::slice::from_ref(&n)),
            Some(refusal("BEGINS_WITH", "N"))
        );
        assert_eq!(
            msg("LT", std::slice::from_ref(&b)),
            Some(refusal("LT", "BOOL"))
        );
        assert_eq!(msg("GT", &[b]), Some(refusal("GT", "BOOL")));
        assert_eq!(msg("BETWEEN", &[l, n]), Some(refusal("BETWEEN", "L")));
    }

    #[test]
    fn accepted_types_pass() {
        let s = AttributeValue::S("p".to_owned());
        let n = AttributeValue::N("1".to_owned());
        let bin = AttributeValue::B(vec![1]);
        assert_eq!(msg("BEGINS_WITH", std::slice::from_ref(&s)), None);
        assert_eq!(msg("BEGINS_WITH", std::slice::from_ref(&bin)), None);
        assert_eq!(msg("BETWEEN", &[n.clone(), n.clone()]), None);
        assert_eq!(msg("IN", &[s.clone(), n.clone(), bin]), None);
        assert_eq!(msg("EQ", &[AttributeValue::L(Vec::new())]), None);
        assert_eq!(msg("NE", &[AttributeValue::Bool(false)]), None);
        assert_eq!(msg("NULL", &[]), None);
        assert_eq!(msg("CONTAINS", &[n]), None);
    }

    #[test]
    fn desugar_filter_refuses_before_desugaring() {
        let mut filter = HashMap::new();
        filter.insert(
            "f".to_owned(),
            Condition {
                comparison_operator: "BEGINS_WITH".to_owned(),
                attribute_value_list: vec![AttributeValue::N("1".to_owned())],
            },
        );
        let err = desugar_filter(&filter, ConditionalOperator::And).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m) if *m == refusal("BEGINS_WITH", "N")),
            "{err:?}"
        );
    }
}
