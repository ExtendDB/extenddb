// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The update-expression rules of the semantic pass. See the module
//! documentation in the parent module for the order the service checks them in.

use super::super::ast::{ArithOp, Expr, UpdateAction};
use super::super::kind::ExpressionKind;
use super::super::resolver::{ExpressionMaps, attribute_type_code};
use super::super::update_parser::{ParsedUpdate, repeated_section_err};
use super::errors::{
    invalid_function_name, operand_count_err, operand_type_err, requires_document_path, validation,
};
use super::refs::{check_path_refs, check_value_defined};
use super::{CONDITION_FUNCTIONS, UPDATE_FUNCTIONS};
use crate::error::DynamoDbError;
use crate::types::AttributeValue;

/// Validate a parsed update expression against the service's semantic rules,
/// in the service's order, then reject overlapping paths.
///
/// Messages carry the `Invalid UpdateExpression:` prefix.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for the first rule the
/// expression breaks in the order described in the module documentation.
pub fn validate_update_semantics(
    parsed: &ParsedUpdate,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    let kind = ExpressionKind::Update;
    let mut seen: Vec<&'static str> = Vec::new();
    for section in &parsed.sections {
        for action in &parsed.actions[section.actions.clone()] {
            match action {
                UpdateAction::Set { path, value } => {
                    check_path_refs(path, maps, kind)?;
                    walk_set_value(value, maps, false)?;
                }
                UpdateAction::Remove { path } => check_path_refs(path, maps, kind)?,
                UpdateAction::Add { path, value } => {
                    check_path_refs(path, maps, kind)?;
                    check_add_delete_value("ADD", value, maps)?;
                }
                UpdateAction::Delete { path, value } => {
                    check_path_refs(path, maps, kind)?;
                    check_add_delete_value("DELETE", value, maps)?;
                }
            }
        }
        if seen.contains(&section.keyword) {
            return Err(repeated_section_err(section.keyword));
        }
        seen.push(section.keyword);
    }
    super::super::resolver::validate_update_paths_disjoint(&parsed.actions, maps)
}

/// Walk a SET value. `nested` is true inside a function's operands or an
/// arithmetic operand, where `list_append` is refused.
fn walk_set_value(expr: &Expr, maps: &ExpressionMaps, nested: bool) -> Result<(), DynamoDbError> {
    let kind = ExpressionKind::Update;
    match expr {
        Expr::Path(path) => check_path_refs(path, maps, kind),
        Expr::Placeholder(name) => check_value_defined(name, maps, kind),
        Expr::Arithmetic { left, op, right } => {
            walk_set_value(left, maps, true)?;
            walk_set_value(right, maps, true)?;
            let symbol = match op {
                ArithOp::Add => "+",
                ArithOp::Sub => "-",
            };
            for operand in [left.as_ref(), right.as_ref()] {
                if let Expr::Placeholder(name) = operand
                    && let Some(val) = maps.values.get(name)
                    && !matches!(val, AttributeValue::N(_))
                {
                    return Err(operand_type_err(kind, symbol, attribute_type_code(val)));
                }
            }
            Ok(())
        }
        Expr::Function { name, args } => {
            for arg in args {
                walk_set_value(arg, maps, true)?;
            }
            if nested && name == "list_append" {
                return Err(validation(
                    kind,
                    "The function is not allowed to be used this way in an expression; function: list_append",
                ));
            }
            if !UPDATE_FUNCTIONS.contains(&name.as_str()) {
                if CONDITION_FUNCTIONS.contains(&name.as_str()) {
                    return Err(validation(
                        kind,
                        &format!(
                            "The function is not allowed in an update expression; function: {name}"
                        ),
                    ));
                }
                return Err(invalid_function_name(kind, name));
            }
            if args.len() != 2 {
                return Err(operand_count_err(kind, name, args.len()));
            }
            if name == "if_not_exists" && !matches!(args[0], Expr::Path(_)) {
                return Err(requires_document_path(kind, name));
            }
            if name == "list_append" {
                for arg in args {
                    if let Expr::Placeholder(p) = arg
                        && let Some(val) = maps.values.get(p)
                        && !matches!(val, AttributeValue::L(_))
                    {
                        return Err(operand_type_err(kind, name, attribute_type_code(val)));
                    }
                }
            }
            Ok(())
        }
        // The update grammar produces no other node kinds in a SET value.
        _ => Ok(()),
    }
}

/// `ADD` takes a number or a set; `DELETE` takes a set. The service names
/// the offending type in its own vocabulary and the same allowed set for
/// both operators (measured: `DELETE s :n` reports `NUMBER`, `typeSet:
/// ALLOWED_FOR_ADD_OPERAND`).
fn check_add_delete_value(
    operator: &str,
    value: &Expr,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    let kind = ExpressionKind::Update;
    let Expr::Placeholder(name) = value else {
        return Ok(());
    };
    check_value_defined(name, maps, kind)?;
    let Some(val) = maps.values.get(name) else {
        return Ok(());
    };
    let type_name = match val {
        AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_) => return Ok(()),
        AttributeValue::N(_) if operator == "ADD" => return Ok(()),
        AttributeValue::N(_) => "NUMBER",
        AttributeValue::S(_) => "STRING",
        AttributeValue::B(_) => "Binary",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::L(_) => "LIST",
        AttributeValue::M(_) => "MAP",
    };
    Err(validation(
        kind,
        &format!(
            "Incorrect operand type for operator or function; operator: {operator}, operand type: {type_name}, typeSet: ALLOWED_FOR_ADD_OPERAND"
        ),
    ))
}
