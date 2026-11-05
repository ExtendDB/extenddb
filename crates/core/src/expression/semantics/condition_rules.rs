// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The condition-expression rules of the semantic pass. See the module
//! documentation in the parent module for the order the service checks them in.

use super::super::ast::Expr;
use super::super::kind::ExpressionKind;
use super::super::resolver::{
    ExpressionMaps, attribute_type_code, validate_ordering_operand_types,
};
use super::errors::{
    invalid_function_name, not_allowed_this_way, operand_count_err, operand_type_err,
    requires_document_path, validation, with_kind,
};
use super::refs::{check_path_refs, check_value_defined};
use super::{CONDITION_FUNCTIONS, UPDATE_FUNCTIONS};
use crate::error::DynamoDbError;
use crate::types::AttributeValue;

/// Validate a parsed condition, filter, or key-condition tree against the
/// service's semantic rules, in the service's order.
///
/// `kind` names the parameter in the messages (`Invalid ConditionExpression:`,
/// `Invalid FilterExpression:`).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for the first rule the
/// expression breaks in the order described in the module documentation.
pub fn validate_condition_semantics(
    expr: &Expr,
    maps: &ExpressionMaps,
    kind: ExpressionKind,
) -> Result<(), DynamoDbError> {
    walk_condition(expr, maps, kind)
}

/// A node in boolean position: the root, or an operand of AND, OR, NOT.
fn walk_condition(
    expr: &Expr,
    maps: &ExpressionMaps,
    kind: ExpressionKind,
) -> Result<(), DynamoDbError> {
    match expr {
        Expr::And(l, r) | Expr::Or(l, r) => {
            walk_condition(l, maps, kind)?;
            walk_condition(r, maps, kind)
        }
        Expr::Not(inner) => walk_condition(inner, maps, kind),
        Expr::Compare { left, right, .. } => {
            walk_condition_operand(left, maps, kind)?;
            walk_condition_operand(right, maps, kind)?;
            validate_ordering_operand_types(expr, maps).map_err(|e| with_kind(e, kind))
        }
        Expr::Between { operand, low, high } => {
            walk_condition_operand(operand, maps, kind)?;
            walk_condition_operand(low, maps, kind)?;
            walk_condition_operand(high, maps, kind)?;
            validate_ordering_operand_types(expr, maps).map_err(|e| with_kind(e, kind))?;
            // The bounds are checked at request time, so a condition that
            // fails them is refused before the other expressions are checked.
            match super::super::evaluator::between_bounds_fault(low, high, maps) {
                Some(msg) => Err(validation(kind, &msg)),
                None => Ok(()),
            }
        }
        Expr::In { operand, list } => {
            walk_condition_operand(operand, maps, kind)?;
            for item in list {
                walk_condition_operand(item, maps, kind)?;
            }
            Ok(())
        }
        Expr::Function { name, args } => walk_condition_function(name, args, maps, kind, false),
        // A bare path or placeholder in condition position is a syntax error
        // the grammar pass already reported; `Expected` desugaring never
        // produces one either.
        Expr::Path(_) | Expr::Placeholder(_) | Expr::Arithmetic { .. } => Ok(()),
    }
}

/// A node in operand position: a side of a comparison, a BETWEEN bound, an
/// IN member, or a function operand.
fn walk_condition_operand(
    expr: &Expr,
    maps: &ExpressionMaps,
    kind: ExpressionKind,
) -> Result<(), DynamoDbError> {
    match expr {
        Expr::Path(path) => check_path_refs(path, maps, kind),
        Expr::Placeholder(name) => check_value_defined(name, maps, kind),
        Expr::Function { name, args } => walk_condition_function(name, args, maps, kind, true),
        _ => Ok(()),
    }
}

/// The checks on a condition function call, after its operands: the name,
/// then its position (`size` returns a value and is refused in condition
/// position; the five boolean functions are refused in operand position:
/// `attribute_type(pk, :v) = :v` reports the position, not the operand type),
/// then the operand count, the document-path rule, and the operand types.
fn walk_condition_function(
    name: &str,
    args: &[Expr],
    maps: &ExpressionMaps,
    kind: ExpressionKind,
    operand_position: bool,
) -> Result<(), DynamoDbError> {
    for arg in args {
        walk_condition_operand(arg, maps, kind)?;
    }
    if !CONDITION_FUNCTIONS.contains(&name) {
        if UPDATE_FUNCTIONS.contains(&name) {
            return Err(validation(
                kind,
                &format!("The function is not allowed in a condition expression; function: {name}"),
            ));
        }
        return Err(invalid_function_name(kind, name));
    }
    if operand_position != (name == "size") {
        return Err(not_allowed_this_way(kind, name));
    }
    let expected = match name {
        "attribute_exists" | "attribute_not_exists" | "size" => 1,
        _ => 2,
    };
    if args.len() != expected {
        return Err(operand_count_err(kind, name, args.len()));
    }
    if matches!(name, "attribute_exists" | "attribute_not_exists")
        && !matches!(args[0], Expr::Path(_))
    {
        return Err(requires_document_path(kind, name));
    }
    if name == "begins_with"
        && let Expr::Placeholder(p) = &args[1]
        && let Some(val) = maps.values.get(p)
        && !matches!(val, AttributeValue::S(_) | AttributeValue::B(_))
    {
        return Err(operand_type_err(kind, name, attribute_type_code(val)));
    }
    if name == "attribute_type"
        && let Expr::Placeholder(p) = &args[1]
        && let Some(val) = maps.values.get(p)
    {
        match val {
            AttributeValue::S(type_name) => {
                const VALID: [&str; 10] =
                    ["S", "SS", "N", "NS", "B", "BS", "BOOL", "NULL", "L", "M"];
                if !VALID.contains(&type_name.as_str()) {
                    return Err(validation(
                        kind,
                        &format!(
                            "Invalid attribute type name found; type: {type_name}, valid types: {{S,SS,N,NS,B,BS,BOOL,NULL,L,M}}"
                        ),
                    ));
                }
            }
            other => return Err(operand_type_err(kind, name, attribute_type_code(other))),
        }
    }
    Ok(())
}
