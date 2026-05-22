// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Update expression evaluator.
//!
//! Applies parsed `UpdateAction`s to an item, modifying it in place.
//! Supports SET (with arithmetic and `if_not_exists`) and REMOVE.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::DynamoDbError;
use crate::types::AttributeValue;

use super::ast::{ArithOp, Expr, PathElement, UpdateAction};
use super::resolver::{ExpressionMaps, resolve_element_name, resolve_path};

/// Apply a list of update actions to an item.
///
/// Modifies `item` in place. Actions are applied in order: all SET actions
/// first, then all REMOVE actions (matching `DynamoDB`'s documented behavior).
///
/// # Errors
///
/// Returns `ValidationException` for unresolvable placeholders or type errors.
pub fn apply_update(
    actions: &[UpdateAction],
    item: &mut BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    // DynamoDB applies actions in order: SET, REMOVE, ADD, DELETE
    for action in actions {
        if let UpdateAction::Set { path, value } = action {
            let resolved_value = evaluate_set_value(value, item, maps)?;
            set_path(item, path, resolved_value, maps)?;
        }
    }
    for action in actions {
        if let UpdateAction::Remove { path } = action {
            remove_path(item, path, maps)?;
        }
    }
    for action in actions {
        if let UpdateAction::Add { path, value } = action {
            let resolved_value = evaluate_set_value(value, item, maps)?;
            apply_add(item, path, resolved_value, maps)?;
        }
    }
    for action in actions {
        if let UpdateAction::Delete { path, value } = action {
            let resolved_value = evaluate_set_value(value, item, maps)?;
            apply_delete(item, path, &resolved_value, maps)?;
        }
    }
    Ok(())
}

/// Evaluate a SET value expression to produce an `AttributeValue`.
fn evaluate_set_value(
    expr: &Expr,
    item: &BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
) -> Result<AttributeValue, DynamoDbError> {
    match expr {
        Expr::Placeholder(name) => Ok(maps.resolve_value_for(name, "UpdateExpression")?.clone()),
        Expr::Path(elements) => {
            resolve_path_to_value(elements, item, maps)?
                .cloned()
                .ok_or_else(|| {
                    DynamoDbError::ValidationException(
                        "The provided expression refers to an attribute that does not exist in the item"
                            .to_owned(),
                    )
                })
        }
        Expr::Arithmetic { left, op, right } => {
            let lv = evaluate_set_value(left, item, maps)?;
            let rv = evaluate_set_value(right, item, maps)?;
            evaluate_arithmetic(&lv, &rv, *op)
        }
        Expr::Function { name, args } => evaluate_set_function(name, args, item, maps),
        _ => Err(DynamoDbError::ValidationException(
            "Invalid UpdateExpression: unsupported value expression".to_owned(),
        )),
    }
}

/// Evaluate arithmetic: N + N or N - N.
fn evaluate_arithmetic(
    left: &AttributeValue,
    right: &AttributeValue,
    op: ArithOp,
) -> Result<AttributeValue, DynamoDbError> {
    let (AttributeValue::N(l), AttributeValue::N(r)) = (left, right) else {
        return Err(DynamoDbError::ValidationException(
            "An operand in the update expression has an incorrect data type".to_owned(),
        ));
    };

    let ld: bigdecimal::BigDecimal = l.parse().map_err(|_| {
        DynamoDbError::ValidationException("Invalid numeric value in expression".to_owned())
    })?;
    let rd: bigdecimal::BigDecimal = r.parse().map_err(|_| {
        DynamoDbError::ValidationException("Invalid numeric value in expression".to_owned())
    })?;

    let result = match op {
        ArithOp::Add => ld + rd,
        ArithOp::Sub => ld - rd,
    };

    let result_str = result.to_string();
    // Validate the result is within DynamoDB's number range
    crate::validation::number::validate_and_normalize_number(&result_str).map_err(|_| {
        DynamoDbError::ValidationException(
            "Number overflow. Attempting to store a number with magnitude larger than supported range".to_owned(),
        )
    })?;

    Ok(AttributeValue::N(result_str))
}

/// Evaluate SET functions: `if_not_exists(path, value)`.
fn evaluate_set_function(
    name: &str,
    args: &[Expr],
    item: &BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
) -> Result<AttributeValue, DynamoDbError> {
    match name {
        "if_not_exists" => {
            if args.len() != 2 {
                return Err(DynamoDbError::ValidationException(
                    "Invalid UpdateExpression: if_not_exists requires exactly two arguments"
                        .to_owned(),
                ));
            }
            // If the path exists, return its value; otherwise return the default
            if let Expr::Path(elements) = &args[0] {
                if let Some(existing) = resolve_path_to_value(elements, item, maps)? {
                    return Ok(existing.clone());
                }
            }
            evaluate_set_value(&args[1], item, maps)
        }
        "list_append" => {
            if args.len() != 2 {
                return Err(DynamoDbError::ValidationException(
                    "Invalid UpdateExpression: list_append requires exactly two arguments"
                        .to_owned(),
                ));
            }
            let left = evaluate_set_value(&args[0], item, maps)?;
            let right = evaluate_set_value(&args[1], item, maps)?;
            match (left, right) {
                (AttributeValue::L(mut a), AttributeValue::L(b)) => {
                    a.extend(b);
                    Ok(AttributeValue::L(a))
                }
                _ => Err(DynamoDbError::ValidationException(
                    "An operand in the update expression has an incorrect data type".to_owned(),
                )),
            }
        }
        _ => Err(DynamoDbError::ValidationException(format!(
            "Invalid UpdateExpression: unknown function '{name}'"
        ))),
    }
}

/// Apply an ADD action to an item.
///
/// `DynamoDB` ADD semantics:
/// - If the attribute doesn't exist and the value is a number, set it.
/// - If the attribute exists and is a number, add the value to it.
/// - If the value is a set (SS/NS/BS), union it with the existing set (or create it).
///
/// Supports nested paths (e.g. `ADD myMap.counter :inc`).
fn apply_add(
    item: &mut BTreeMap<String, AttributeValue>,
    path: &[PathElement],
    value: AttributeValue,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    let target = navigate_to_parent_map_or_create(item, path, maps)?;
    let attr_name = resolve_attr_name(
        path.last()
            .ok_or_else(|| DynamoDbError::ValidationException("Empty path in ADD".to_owned()))?,
        maps,
    )?;
    let existing = target.get(&attr_name);

    let new_value = match (&existing, &value) {
        // Number or set: set if missing
        (
            None,
            AttributeValue::N(_)
            | AttributeValue::SS(_)
            | AttributeValue::NS(_)
            | AttributeValue::BS(_),
        ) => value,
        // Number: add to existing
        (Some(AttributeValue::N(existing_n)), AttributeValue::N(add_n)) => {
            let ed: bigdecimal::BigDecimal = existing_n.parse().map_err(|_| {
                DynamoDbError::ValidationException("Invalid numeric value in expression".to_owned())
            })?;
            let ad: bigdecimal::BigDecimal = add_n.parse().map_err(|_| {
                DynamoDbError::ValidationException("Invalid numeric value in expression".to_owned())
            })?;
            let result_str = (ed + ad).to_string();
            crate::validation::number::validate_and_normalize_number(&result_str).map_err(
                |_| {
                    DynamoDbError::ValidationException(
                        "Number overflow. Attempting to store a number with magnitude larger than supported range".to_owned(),
                    )
                },
            )?;
            AttributeValue::N(result_str)
        }
        // Set: union with existing
        (Some(AttributeValue::SS(existing_set)), AttributeValue::SS(add_set)) => {
            let mut merged = existing_set.clone();
            merged.extend(add_set.iter().cloned());
            AttributeValue::SS(merged)
        }
        (Some(AttributeValue::NS(existing_set)), AttributeValue::NS(add_set)) => {
            let mut merged = existing_set.clone();
            merged.extend(add_set.iter().cloned());
            AttributeValue::NS(merged)
        }
        (Some(AttributeValue::BS(existing_set)), AttributeValue::BS(add_set)) => {
            let mut merged = existing_set.clone();
            merged.extend(add_set.iter().cloned());
            AttributeValue::BS(merged)
        }
        _ => {
            return Err(DynamoDbError::ValidationException(
                "An operand in the update expression has an incorrect data type".to_owned(),
            ));
        }
    };

    target.insert(attr_name, new_value);
    Ok(())
}

/// Apply a DELETE action to an item.
///
/// `DynamoDB` DELETE semantics: removes elements from a set.
/// The value must be a set of the same type as the existing attribute.
/// Supports nested paths (e.g. `DELETE myMap.tags :removeTags`).
fn apply_delete(
    item: &mut BTreeMap<String, AttributeValue>,
    path: &[PathElement],
    value: &AttributeValue,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    let Some(target) = navigate_to_parent_map(item, path, maps)? else {
        return Ok(()); // Parent path doesn't exist — no-op
    };
    let attr_name = resolve_attr_name(
        path.last()
            .ok_or_else(|| DynamoDbError::ValidationException("Empty path in DELETE".to_owned()))?,
        maps,
    )?;
    let Some(existing) = target.get(&attr_name) else {
        return Ok(()); // No-op if attribute doesn't exist
    };

    let new_value = match (existing, value) {
        (AttributeValue::SS(existing_set), AttributeValue::SS(remove_set)) => {
            let remaining: BTreeSet<_> = existing_set.difference(remove_set).cloned().collect();
            if remaining.is_empty() {
                target.remove(&attr_name);
                return Ok(());
            }
            AttributeValue::SS(remaining)
        }
        (AttributeValue::NS(existing_set), AttributeValue::NS(remove_set)) => {
            let remaining: BTreeSet<_> = existing_set.difference(remove_set).cloned().collect();
            if remaining.is_empty() {
                target.remove(&attr_name);
                return Ok(());
            }
            AttributeValue::NS(remaining)
        }
        (AttributeValue::BS(existing_set), AttributeValue::BS(remove_set)) => {
            let remaining: BTreeSet<_> = existing_set.difference(remove_set).cloned().collect();
            if remaining.is_empty() {
                target.remove(&attr_name);
                return Ok(());
            }
            AttributeValue::BS(remaining)
        }
        _ => {
            return Err(DynamoDbError::ValidationException(
                "An operand in the update expression has an incorrect data type".to_owned(),
            ));
        }
    };

    target.insert(attr_name, new_value);
    Ok(())
}

/// Navigate a document path to the parent map of the leaf element, creating
/// intermediate maps as needed. Used by ADD which must create the path if absent.
///
/// For a single-element path, returns the item itself.
fn navigate_to_parent_map_or_create<'a>(
    item: &'a mut BTreeMap<String, AttributeValue>,
    path: &[PathElement],
    maps: &ExpressionMaps,
) -> Result<&'a mut BTreeMap<String, AttributeValue>, DynamoDbError> {
    if path.len() <= 1 {
        return Ok(item);
    }
    let first_name = resolve_attr_name(&path[0], maps)?;
    let mut current = item
        .entry(first_name)
        .or_insert_with(|| AttributeValue::M(BTreeMap::new()));
    for element in &path[1..path.len() - 1] {
        match element {
            PathElement::Attribute(_) => {
                let name = resolve_attr_name(element, maps)?;
                if let AttributeValue::M(map) = current {
                    current = map
                        .entry(name)
                        .or_insert_with(|| AttributeValue::M(BTreeMap::new()));
                } else {
                    return Err(DynamoDbError::ValidationException(
                        "The document path provided in the update expression is invalid for update"
                            .to_owned(),
                    ));
                }
            }
            PathElement::Index(idx) => {
                if let AttributeValue::L(list) = current {
                    if *idx < list.len() {
                        current = &mut list[*idx];
                    } else {
                        return Err(DynamoDbError::ValidationException(
                            "The provided expression refers to an attribute that does not exist in the item"
                                .to_owned(),
                        ));
                    }
                } else {
                    return Err(DynamoDbError::ValidationException(
                        "The document path provided in the update expression is invalid for update"
                            .to_owned(),
                    ));
                }
            }
        }
    }
    match current {
        AttributeValue::M(map) => Ok(map),
        _ => Err(DynamoDbError::ValidationException(
            "The document path provided in the update expression is invalid for update".to_owned(),
        )),
    }
}

/// Navigate a document path to the parent map of the leaf element without
/// creating intermediate maps. Returns `None` if the path doesn't exist.
/// Used by DELETE which is a no-op when the path is absent.
fn navigate_to_parent_map<'a>(
    item: &'a mut BTreeMap<String, AttributeValue>,
    path: &[PathElement],
    maps: &ExpressionMaps,
) -> Result<Option<&'a mut BTreeMap<String, AttributeValue>>, DynamoDbError> {
    if path.len() <= 1 {
        return Ok(Some(item));
    }
    let first_name = resolve_attr_name(&path[0], maps)?;
    let Some(mut current) = item.get_mut(&first_name) else {
        return Ok(None);
    };
    for element in &path[1..path.len() - 1] {
        match element {
            PathElement::Attribute(_) => {
                let name = resolve_attr_name(element, maps)?;
                if let AttributeValue::M(map) = current {
                    let Some(next) = map.get_mut(&name) else {
                        return Ok(None);
                    };
                    current = next;
                } else {
                    return Ok(None);
                }
            }
            PathElement::Index(idx) => {
                if let AttributeValue::L(list) = current {
                    if *idx < list.len() {
                        current = &mut list[*idx];
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
        }
    }
    match current {
        AttributeValue::M(map) => Ok(Some(map)),
        _ => Ok(None),
    }
}

/// Set a value at a document path, creating intermediate maps as needed.
fn set_path(
    item: &mut BTreeMap<String, AttributeValue>,
    path: &[PathElement],
    value: AttributeValue,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    if path.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "Invalid UpdateExpression: empty path".to_owned(),
        ));
    }

    let first_name = resolve_attr_name(&path[0], maps)?;

    if path.len() == 1 {
        item.insert(first_name, value);
        return Ok(());
    }

    // DynamoDB rejects SET into a path where the parent doesn't exist
    let Some(current) = item.get_mut(&first_name) else {
        return Err(DynamoDbError::ValidationException(
            "The document path provided in the update expression is invalid for update".to_owned(),
        ));
    };

    set_nested(current, &path[1..], value, maps)
}

fn set_nested(
    current: &mut AttributeValue,
    path: &[PathElement],
    value: AttributeValue,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    if path.len() == 1 {
        match (&path[0], current) {
            (PathElement::Attribute(_), AttributeValue::M(map)) => {
                let name = resolve_attr_name(&path[0], maps)?;
                map.insert(name, value);
            }
            (PathElement::Index(idx), AttributeValue::L(list)) => {
                if *idx < list.len() {
                    list[*idx] = value;
                } else {
                    list.push(value);
                }
            }
            _ => {
                return Err(DynamoDbError::ValidationException(
                    "The document path provided in the update expression is invalid for update"
                        .to_owned(),
                ));
            }
        }
        return Ok(());
    }

    match (&path[0], current) {
        (PathElement::Attribute(_), AttributeValue::M(map)) => {
            let name = resolve_attr_name(&path[0], maps)?;
            match map.get_mut(&name) {
                Some(entry) => set_nested(entry, &path[1..], value, maps),
                None => Err(DynamoDbError::ValidationException(
                    "The document path provided in the update expression is invalid for update"
                        .to_owned(),
                )),
            }
        }
        (PathElement::Index(idx), AttributeValue::L(list)) => {
            if *idx < list.len() {
                set_nested(&mut list[*idx], &path[1..], value, maps)
            } else {
                Err(DynamoDbError::ValidationException(
                    "The provided expression refers to an attribute that does not exist in the item"
                        .to_owned(),
                ))
            }
        }
        _ => Err(DynamoDbError::ValidationException(
            "The document path provided in the update expression is invalid for update".to_owned(),
        )),
    }
}

/// Remove a value at a document path.
fn remove_path(
    item: &mut BTreeMap<String, AttributeValue>,
    path: &[PathElement],
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    if path.is_empty() {
        return Ok(());
    }

    let first_name = resolve_attr_name(&path[0], maps)?;

    if path.len() == 1 {
        item.remove(&first_name);
        return Ok(());
    }

    let Some(current) = item.get_mut(&first_name) else {
        return Ok(()); // Path doesn't exist — REMOVE is a no-op
    };

    remove_nested(current, &path[1..], maps)
}

fn remove_nested(
    current: &mut AttributeValue,
    path: &[PathElement],
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    if path.len() == 1 {
        match (&path[0], current) {
            (PathElement::Attribute(_), AttributeValue::M(map)) => {
                let name = resolve_attr_name(&path[0], maps)?;
                map.remove(&name);
            }
            (PathElement::Index(idx), AttributeValue::L(list)) if *idx < list.len() => {
                list.remove(*idx);
            }
            _ => {} // No-op for type mismatch
        }
        return Ok(());
    }

    match (&path[0], current) {
        (PathElement::Attribute(_), AttributeValue::M(map)) => {
            let name = resolve_attr_name(&path[0], maps)?;
            if let Some(next) = map.get_mut(&name) {
                remove_nested(next, &path[1..], maps)?;
            }
        }
        (PathElement::Index(idx), AttributeValue::L(list)) if *idx < list.len() => {
            remove_nested(&mut list[*idx], &path[1..], maps)?;
        }
        _ => {} // No-op
    }
    Ok(())
}

/// Resolve a path element to an attribute name string.
fn resolve_attr_name(
    element: &PathElement,
    maps: &ExpressionMaps,
) -> Result<String, DynamoDbError> {
    Ok(resolve_element_name(element, maps)?.into_owned())
}

/// Resolve a document path to a value reference.
///
/// Delegates to the shared `resolve_path` in `resolver.rs`.
fn resolve_path_to_value<'a>(
    elements: &[PathElement],
    item: &'a BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
) -> Result<Option<&'a AttributeValue>, DynamoDbError> {
    resolve_path(elements, item, maps)
}

#[cfg(test)]
#[path = "update_evaluator_tests.rs"]
mod tests;
