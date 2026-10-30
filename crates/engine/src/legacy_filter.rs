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
    let hash_count = key_schema.iter().filter(|(_, is_hash)| *is_hash).count();
    let range_count = key_schema.len() - hash_count;
    if hash_count > 1 || range_count > 1 {
        return desugar_multipart_key_conditions(conditions, key_schema);
    }

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

/// Desugar legacy `KeyConditions` on a key schema with several HASH or several
/// RANGE attributes into the same `KeyCondition` shape the expression parser
/// produces, so `KeyCondition::resolve_multipart` classifies both forms by one
/// rule (measured on the service 2026-10-21: `KeyConditions` on such an index
/// answer the same refusals as the equivalent `KeyConditionExpression`).
fn desugar_multipart_key_conditions(
    conditions: &HashMap<String, Condition>,
    key_schema: &[(String, bool)],
) -> Result<(KeyCondition, ExpressionMaps), DynamoDbError> {
    let mut values = HashMap::new();
    let mut equalities: Vec<(Vec<PathElement>, Expr)> = Vec::new();
    let mut non_equalities: Vec<(String, SortKeyCondition)> = Vec::new();

    // Key-schema attributes first, in key-schema order, then any other name,
    // so placeholder numbering and clause order are deterministic.
    let mut ordered: Vec<&String> = key_schema
        .iter()
        .map(|(name, _)| name)
        .filter(|name| conditions.contains_key(*name))
        .collect();
    let mut others: Vec<&String> = conditions
        .keys()
        .filter(|name| !key_schema.iter().any(|(k, _)| k == *name))
        .collect();
    others.sort();
    ordered.extend(others);

    for (i, attr) in ordered.into_iter().enumerate() {
        let cond = &conditions[attr];
        if cond.comparison_operator == "EQ" {
            if cond.attribute_value_list.len() != 1 {
                return Err(DynamoDbError::ValidationException(
                    "Query key condition must have exactly one value for EQ".to_owned(),
                ));
            }
            let placeholder = format!("_kc_eq{i}");
            values.insert(placeholder.clone(), cond.attribute_value_list[0].clone());
            equalities.push((
                vec![PathElement::Attribute(attr.clone())],
                Expr::Placeholder(placeholder),
            ));
        } else {
            let mut own_values = HashMap::new();
            let sk = desugar_sk_condition(cond, attr, &mut own_values)?;
            // Rename this condition's placeholders so two non-equalities
            // cannot collide on `_kc_sk`.
            let sk = rename_placeholders(sk, &own_values, &format!("_kc_ne{i}_"), &mut values);
            non_equalities.push((attr.clone(), sk));
        }
    }

    let hash_attrs: Vec<&str> = key_schema
        .iter()
        .filter(|(_, is_hash)| *is_hash)
        .map(|(name, _)| name.as_str())
        .collect();
    let range_attrs: Vec<&str> = key_schema
        .iter()
        .filter(|(_, is_hash)| !*is_hash)
        .map(|(name, _)| name.as_str())
        .collect();

    // Two non-equalities: the service names the RANGE attributes before the
    // last conditioned one that lack an equality (measured: `h EQ, a GT,
    // b GT` is `RANGE key attributes a must have equality conditions
    // specified in the query because a condition is present on key attribute
    // b`).
    if non_equalities.len() > 1 {
        let conditioned: Vec<usize> = conditions
            .keys()
            .filter_map(|name| range_attrs.iter().position(|r| r == name))
            .collect();
        if let Some(&last) = conditioned.iter().max() {
            let without_eq: Vec<&str> = range_attrs[..last]
                .iter()
                .enumerate()
                .filter(|(_, attr)| {
                    conditions
                        .get(**attr)
                        .is_none_or(|c| c.comparison_operator != "EQ")
                })
                .map(|(_, attr)| *attr)
                .collect();
            if !without_eq.is_empty() {
                return Err(DynamoDbError::ValidationException(format!(
                    "RANGE key attributes {} must have equality conditions specified in the \
                     query because a condition is present on key attribute {}",
                    without_eq.join(", "),
                    range_attrs[last]
                )));
            }
        }
        return Err(DynamoDbError::ValidationException(
            "Query key condition not supported".to_owned(),
        ));
    }

    let Some((pk_path, pk_value)) = equalities.first().cloned() else {
        // No equality at all: every HASH attribute lacks one.
        return Err(DynamoDbError::ValidationException(format!(
            "HASH key attributes {} must have equality conditions specified in the query",
            hash_attrs.join(", ")
        )));
    };
    let extra_pk_conditions = equalities.into_iter().skip(1).collect();
    let sk_condition = non_equalities.into_iter().next().map(|(_, sk)| sk);

    Ok((
        KeyCondition {
            pk_path,
            pk_value,
            sk_condition,
            extra_pk_conditions,
            extra_sk_conditions: Vec::new(),
        },
        ExpressionMaps::new(HashMap::new(), values),
    ))
}

/// Move `own_values` into `values` under `prefix`-ed names and rewrite the
/// condition's placeholders to match.
fn rename_placeholders(
    sk: SortKeyCondition,
    own_values: &HashMap<String, AttributeValue>,
    prefix: &str,
    values: &mut HashMap<String, AttributeValue>,
) -> SortKeyCondition {
    let mut rename = |expr: Expr| -> Expr {
        match expr {
            Expr::Placeholder(name) => {
                let new_name = format!("{prefix}{name}");
                if let Some(v) = own_values.get(&name) {
                    values.insert(new_name.clone(), v.clone());
                }
                Expr::Placeholder(new_name)
            }
            other => other,
        }
    };
    match sk {
        SortKeyCondition::Compare { path, op, value } => SortKeyCondition::Compare {
            path,
            op,
            value: rename(value),
        },
        SortKeyCondition::Between { path, low, high } => SortKeyCondition::Between {
            path,
            low: rename(low),
            high: rename(high),
        },
        SortKeyCondition::BeginsWith { path, prefix: p } => SortKeyCondition::BeginsWith {
            path,
            prefix: rename(p),
        },
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

fn desugar_one_filter_condition(
    path: &Expr,
    cond: &Condition,
    values: &mut HashMap<String, AttributeValue>,
    counter: &mut u32,
) -> Result<Expr, DynamoDbError> {
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
