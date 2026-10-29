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
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Condition, ConditionalOperator, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use extenddb_core::validation::{validate_attribute_number, validate_no_empty_key_value};

/// Desugar legacy `KeyConditions` into a `KeyCondition` struct and expression maps.
///
/// Runs the checks that need the key schema, in the order the service applies
/// them (measured 2026-10-24 on S/S, S/N, hash-only and GSI key schemas):
///
/// 1. the keys are walked in schema order; an absent hash key answers
///    `Query condition missed key schema element: <hash>`, a present key whose
///    value type differs from its `AttributeDefinitions` type answers
///    `One or more parameter values were invalid: Condition parameter type
///    does not match schema type`, and a hash key with an operator other than
///    `EQ` answers `Query key condition not supported` (the type is checked
///    first: `pk LT N` on an S key names the type);
/// 2. a condition on an attribute outside the key schema answers `Query
///    condition missed key schema element: <hash>` (the service names the
///    first absent key, and refuses three conditions on a two-key schema with
///    its own message; neither is changed here);
/// 3. an empty `S` or `B` key value, hash key first, answers `One or more
///    parameter values are not valid. The AttributeValue for a key attribute
///    cannot contain an empty string value. Key: <name>` (measured for every
///    range operator and both `BETWEEN` bounds; the binary wording is the
///    shared key check's and was not measured on this path).
///
/// The request-time checks ([`validate_key_conditions`]) ran before the table
/// lookup, so a non-indexable operator never reaches this function.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn desugar_key_conditions(
    conditions: &HashMap<String, Condition>,
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
) -> Result<(KeyCondition, ExpressionMaps), DynamoDbError> {
    let hash_attr = key_schema
        .iter()
        .find(|ks| ks.key_type == KeyType::Hash)
        .map(|ks| ks.attribute_name.as_str())
        .ok_or_else(|| {
            DynamoDbError::ValidationException("No hash key in key schema".to_owned())
        })?;
    let range_attr = key_schema
        .iter()
        .find(|ks| ks.key_type == KeyType::Range)
        .map(|ks| ks.attribute_name.as_str());

    validate_key_conditions(conditions)?;

    let hash_cond = conditions.get(hash_attr).ok_or_else(|| {
        DynamoDbError::ValidationException(format!(
            "Query condition missed key schema element: {hash_attr}"
        ))
    })?;
    check_key_schema_type(hash_attr, hash_cond, attribute_definitions)?;
    if hash_cond.comparison_operator != "EQ" {
        return Err(DynamoDbError::ValidationException(
            "Query key condition not supported".to_owned(),
        ));
    }
    let range_cond = range_attr.and_then(|name| conditions.get(name).map(|cond| (name, cond)));
    if let Some((name, cond)) = range_cond {
        check_key_schema_type(name, cond, attribute_definitions)?;
    }

    // Check for non-key attributes in KeyConditions (invalid)
    for attr_name in conditions.keys() {
        if attr_name != hash_attr && range_attr != Some(attr_name.as_str()) {
            return Err(DynamoDbError::ValidationException(format!(
                "Query condition missed key schema element: {hash_attr}"
            )));
        }
    }

    for value in &hash_cond.attribute_value_list {
        validate_no_empty_key_value(hash_attr, value)?;
    }
    if let Some((name, cond)) = range_cond {
        for value in &cond.attribute_value_list {
            validate_no_empty_key_value(name, value)?;
        }
    }

    let mut values = HashMap::new();
    let pk_placeholder = "_kc_pk".to_owned();
    values.insert(
        pk_placeholder.clone(),
        hash_cond.attribute_value_list[0].clone(),
    );

    let pk_path = vec![PathElement::Attribute(hash_attr.to_owned())];
    let pk_value = Expr::Placeholder(pk_placeholder.clone());

    let sk_condition = match range_cond {
        Some((name, cond)) => Some(desugar_sk_condition(cond, name, &mut values)?),
        None => None,
    };

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

/// Refuse a key condition value whose type is not the key's
/// `AttributeDefinitions` type (measured 2026-10-24: `pk EQ N`, `EQ B`, `EQ
/// BOOL` on an S key, `sk EQ S` and `BEGINS_WITH S` on an N key, every range
/// operator, GSI keys; the message names no attribute). A key with no
/// definition is left to the storage layer, as before.
fn check_key_schema_type(
    attr_name: &str,
    cond: &Condition,
    attribute_definitions: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    let Some(expected) = attribute_definitions
        .iter()
        .find(|ad| ad.attribute_name == attr_name)
        .map(|ad| ad.attribute_type)
    else {
        return Ok(());
    };
    let mismatch = cond.attribute_value_list.iter().any(|value| {
        !matches!(
            (expected, value),
            (ScalarAttributeType::S, AttributeValue::S(_))
                | (ScalarAttributeType::N, AttributeValue::N(_))
                | (ScalarAttributeType::B, AttributeValue::B(_))
        )
    });
    if mismatch {
        return Err(DynamoDbError::ValidationException(format!(
            "{INVALID_PARAMS}Condition parameter type does not match schema type"
        )));
    }
    Ok(())
}

/// Operators the service refuses on any key condition entry, whichever key
/// the entry names, with `Attempted conditional constraint is not an
/// indexable operation` (measured 2026-10-23 on the range key and 2026-10-24
/// on the hash key: `pk NE`, `pk IN`, `pk NULL`, `pk NOT_NULL`, `pk
/// CONTAINS`).
const NON_INDEXABLE: [&str; 6] = ["NE", "NULL", "NOT_NULL", "CONTAINS", "NOT_CONTAINS", "IN"];

/// Request-time checks for legacy `KeyConditions`: [`validate_legacy_conditions`]
/// on every entry, then the non-indexable operator refusal on every entry.
/// Both ran before the table lookup on the service: a non-indexable operator
/// was named ahead of the Select checks that are themselves reported on a
/// table that does not exist, ahead of a missing or surplus key schema
/// element, and ahead of a schema type mismatch on another entry (measured
/// 2026-10-24).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn validate_key_conditions(
    conditions: &HashMap<String, Condition>,
) -> Result<(), DynamoDbError> {
    validate_legacy_conditions(conditions)?;
    if conditions
        .values()
        .any(|cond| NON_INDEXABLE.contains(&cond.comparison_operator.as_str()))
    {
        return Err(DynamoDbError::ValidationException(format!(
            "{INVALID_PARAMS}Attempted conditional constraint is not an indexable operation"
        )));
    }
    Ok(())
}

/// Refuse a legacy `QueryFilter` entry on a key attribute of the queried
/// table or index: `QueryFilter can only contain non-primary key attributes:
/// Primary key attribute: <name>`, the hash key named first (measured
/// 2026-10-24: table keys on a table query, index keys on a GSI query; table
/// keys pass on a GSI query and index keys pass on a table query). The
/// service reports it after the table and index lookup, the key schema and
/// empty key value checks and the Select checks, and before the key value
/// size check and the `ExclusiveStartKey` check, so the caller runs it after
/// the Select checks.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` naming the key attribute.
pub fn reject_query_filter_on_key(
    filter: &HashMap<String, Condition>,
    key_schema: &[KeySchemaElement],
) -> Result<(), DynamoDbError> {
    let hash_first = key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Hash)
        .chain(key_schema.iter().filter(|ks| ks.key_type == KeyType::Range));
    for ks in hash_first {
        if filter.contains_key(&ks.attribute_name) {
            return Err(DynamoDbError::ValidationException(format!(
                "QueryFilter can only contain non-primary key attributes: Primary key attribute: {}",
                ks.attribute_name
            )));
        }
    }
    Ok(())
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
        // validate_key_conditions refused every other operator at request time.
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

/// The item-path number check, with one difference in wording. A legacy
/// condition value `{"N": ""}` answers `The parameter cannot be converted to a
/// numeric value: `, the separator kept with nothing after it, where the item
/// path answers the text without the separator. Measured on a ScanFilter `EQ`
/// condition; every other number error answers the item-path text.
fn validate_legacy_number(value: &AttributeValue) -> Result<(), DynamoDbError> {
    if matches!(value, AttributeValue::N(n) if n.is_empty()) {
        return Err(DynamoDbError::ValidationException(
            "The parameter cannot be converted to a numeric value: ".to_owned(),
        ));
    }
    validate_attribute_number(value)
}

/// Validate one legacy condition (`ScanFilter`, `QueryFilter` or
/// `KeyConditions` entry) the way the service does at request time, before
/// the table is looked up and before any row is read, with its messages
/// (measured 2026-10-23 and 2026-10-24 on empty tables). In order:
///
/// 1. every `N` in the value list, including inside `NS`, `L` and `M`, is a
///    valid number (`The parameter cannot be converted to a numeric value:
///    <raw>`, or the overflow, underflow and 38-digit messages; no prefix);
/// 2. the operator is one of the thirteen (`Invalid comparison operator`);
/// 3. the value count fits the operator: none for `NULL` and `NOT_NULL`, two
///    for `BETWEEN`, one or more for `IN`, one otherwise (`Invalid number of
///    argument(s) for the <op> ComparisonOperator`);
/// 4. the first value's type is one the operator takes: `EQ` and `NE` take
///    every type, `BEGINS_WITH` takes `S` and `B`, `CONTAINS` and
///    `NOT_CONTAINS` take `S`, `N`, `B`, `BOOL` and `NULL`, the ordering
///    operators, `BETWEEN` and `IN` take `S`, `N` and `B` (`ComparisonOperator
///    <op> is not valid for <type> AttributeValue type`);
/// 5. every value has the first value's type (`AttributeValues inside
///    AttributeValueList must be of same type`);
/// 6. a `BETWEEN` lower bound is not above its upper bound (`The BETWEEN
///    condition was provided a range where the lower bound is greater than
///    the upper bound`).
///
/// Step 1 was measured ahead of each of steps 2 to 6 (unknown operator, two
/// values, one value for NULL, mixed types, BEGINS_WITH N, inverted range).
/// The order of step 3 against step 4 within one condition was not measured;
/// each was measured against a well-formed other.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text,
/// prefixed `One or more parameter values were invalid: ` for steps 2 to 6.
pub fn validate_legacy_condition(
    operator: &str,
    values: &[AttributeValue],
) -> Result<(), DynamoDbError> {
    for value in values {
        validate_legacy_number(value)?;
    }
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
