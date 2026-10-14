// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Semantic validation of parsed `UpdateExpression` and `ConditionExpression`
//! trees, run after the grammar pass and before any table is read.
//!
//! Amazon DynamoDB reports every syntax error first, then walks the parsed
//! tree once, in expression order, checking each node after its operands
//! (a post-order walk), and only then checks overlapping paths and unused
//! names and values. The order of the messages below was measured against
//! the service on 2026-10-14; each rule names the request that pinned it.
//!
//! Update expression, per node:
//! - a document path: every `#name` element is defined;
//! - a `:value` placeholder: defined;
//! - `+` and `-`: after both operands, a placeholder operand must be a
//!   number (`SET a = :s + :n` with a string `:s`);
//! - a function call: after its operands, `list_append` may not be an
//!   operand of a function or of `+`/`-` (`if_not_exists(list_append(:a,
//!   :b), :v)`); the name must be `if_not_exists` or `list_append` in that
//!   case (`IF_NOT_EXISTS(x, :v)` is refused); a condition function is
//!   refused by name (`SET x = size(x)`); both take two operands; the first
//!   operand of `if_not_exists` is a document path; a placeholder operand of
//!   `list_append` is a list;
//! - an `ADD` or `DELETE` action: after the path and the placeholder, the
//!   value is a number or a set for `ADD` and a set for `DELETE`;
//! - a section: after its actions, the keyword may not have appeared before
//!   (`SET a = :v SET b = :q` reports `:q`, `SET a = :v SET b = :v ADD n :s`
//!   reports the repeated `SET`).
//!
//! Condition expression, per node:
//! - operands of a comparison, `BETWEEN`, or `IN` are walked first; a
//!   function in operand position must be `size`, and `size` in condition
//!   position is refused (`attribute_exists(pk) = :v`, `size(pk)`);
//! - a function call: after its operands, the name must be one of the six
//!   condition functions as written (`ATTRIBUTE_EXISTS(pk)` is refused),
//!   `if_not_exists` and `list_append` are refused by name, the operand count
//!   is fixed per function, `attribute_exists` and `attribute_not_exists`
//!   take a document path (`attribute_type(:v, :t)` is accepted), the second operand of
//!   `begins_with` is a string or binary, and the second operand of
//!   `attribute_type` is a string naming a type;
//! - a comparison or `BETWEEN` with an ordering operator: a placeholder
//!   operand is a string, number, or binary.

use super::ast::{ArithOp, Expr, PathElement, UpdateAction};
use super::kind::ExpressionKind;
use super::resolver::{ExpressionMaps, attribute_type_code, validate_ordering_operand_types};
use super::update_parser::{ParsedUpdate, repeated_section_err};
use crate::error::DynamoDbError;
use crate::types::AttributeValue;

const CONDITION_FUNCTIONS: [&str; 6] = [
    "attribute_exists",
    "attribute_not_exists",
    "attribute_type",
    "begins_with",
    "contains",
    "size",
];

const UPDATE_FUNCTIONS: [&str; 2] = ["if_not_exists", "list_append"];

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
    super::resolver::validate_update_paths_disjoint(&parsed.actions, maps)
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
            validate_ordering_operand_types(expr, maps).map_err(|e| with_kind(e, kind))
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

fn check_path_refs(
    path: &[PathElement],
    maps: &ExpressionMaps,
    kind: ExpressionKind,
) -> Result<(), DynamoDbError> {
    for element in path {
        if let PathElement::Attribute(name) = element
            && let Some(ref_name) = name.strip_prefix('#')
            && !maps.names.contains_key(ref_name)
        {
            return Err(validation(
                kind,
                &format!(
                    "An expression attribute name used in the document path is not defined; attribute name: #{ref_name}"
                ),
            ));
        }
    }
    Ok(())
}

fn check_value_defined(
    name: &str,
    maps: &ExpressionMaps,
    kind: ExpressionKind,
) -> Result<(), DynamoDbError> {
    if maps.values.contains_key(name) {
        Ok(())
    } else {
        Err(validation(
            kind,
            &format!(
                "An expression attribute value used in expression is not defined; attribute value: :{name}"
            ),
        ))
    }
}

fn validation(kind: ExpressionKind, msg: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!("Invalid {kind}: {msg}"))
}

fn with_kind(err: DynamoDbError, kind: ExpressionKind) -> DynamoDbError {
    match err {
        DynamoDbError::ValidationException(msg) if !msg.starts_with("Invalid ") => {
            validation(kind, &msg)
        }
        other => other,
    }
}

fn invalid_function_name(kind: ExpressionKind, name: &str) -> DynamoDbError {
    validation(kind, &format!("Invalid function name; function: {name}"))
}

fn not_allowed_this_way(kind: ExpressionKind, name: &str) -> DynamoDbError {
    validation(
        kind,
        &format!(
            "The function is not allowed to be used this way in an expression; function: {name}"
        ),
    )
}

fn operand_count_err(kind: ExpressionKind, name: &str, count: usize) -> DynamoDbError {
    validation(
        kind,
        &format!(
            "Incorrect number of operands for operator or function; operator or function: {name}, number of operands: {count}"
        ),
    )
}

fn requires_document_path(kind: ExpressionKind, name: &str) -> DynamoDbError {
    validation(
        kind,
        &format!("Operator or function requires a document path; operator or function: {name}"),
    )
}

fn operand_type_err(kind: ExpressionKind, name: &str, type_code: &str) -> DynamoDbError {
    validation(
        kind,
        &format!(
            "Incorrect operand type for operator or function; operator or function: {name}, operand type: {type_code}"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::parser::parse_condition;
    use crate::expression::tokenizer::tokenize;
    use crate::expression::update_parser::parse_update_sections;
    use std::collections::HashMap;

    const U: &str = "Invalid UpdateExpression: ";
    const C: &str = "Invalid ConditionExpression: ";

    fn maps(names: &[(&str, &str)], values: &[(&str, AttributeValue)]) -> ExpressionMaps {
        ExpressionMaps::new(
            names
                .iter()
                .map(|(k, v)| (k.trim_start_matches('#').to_owned(), (*v).to_owned()))
                .collect::<HashMap<_, _>>(),
            values
                .iter()
                .map(|(k, v)| (k.trim_start_matches(':').to_owned(), v.clone()))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn n() -> AttributeValue {
        AttributeValue::N("1".to_owned())
    }
    fn s() -> AttributeValue {
        AttributeValue::S("x".to_owned())
    }
    fn l() -> AttributeValue {
        AttributeValue::L(vec![])
    }

    fn update_err(expr: &str, m: &ExpressionMaps) -> String {
        let tokens = tokenize(expr).unwrap();
        let parsed = parse_update_sections(&tokens, Some(expr)).unwrap();
        match validate_update_semantics(&parsed, m) {
            Ok(()) => "OK".to_owned(),
            Err(DynamoDbError::ValidationException(msg)) => msg,
            Err(other) => panic!("unexpected {other:?}"),
        }
    }

    fn condition_err(expr: &str, m: &ExpressionMaps) -> String {
        let tokens = tokenize(expr).unwrap();
        let parsed = parse_condition(&tokens).unwrap();
        match validate_condition_semantics(&parsed, m, ExpressionKind::Condition) {
            Ok(()) => "OK".to_owned(),
            Err(DynamoDbError::ValidationException(msg)) => msg,
            Err(other) => panic!("unexpected {other:?}"),
        }
    }

    // Every expectation below is a response captured from Amazon DynamoDB
    // on 2026-10-14 (issue #108 notes, probe-fix-round-*.py).

    #[test]
    fn update_refs_in_expression_order() {
        let m = maps(&[], &[(":v", n())]);
        assert_eq!(
            update_err("SET a = :q, #n = :v", &m),
            format!(
                "{U}An expression attribute value used in expression is not defined; attribute value: :q"
            )
        );
        assert_eq!(
            update_err("SET #n = :q", &m),
            format!(
                "{U}An expression attribute name used in the document path is not defined; attribute name: #n"
            )
        );
    }

    #[test]
    fn repeated_section_after_its_own_actions() {
        let m = maps(&[], &[(":v", n()), (":s", s()), (":n", n())]);
        assert_eq!(
            update_err("SET a = :v SET b = :q", &m),
            format!(
                "{U}An expression attribute value used in expression is not defined; attribute value: :q"
            )
        );
        assert_eq!(
            update_err("SET a = :v SET b = :v ADD n :s", &m),
            format!("{U}The \"SET\" section can only be used once in an update expression;")
        );
        assert_eq!(
            update_err("ADD n :s ADD m :n", &m),
            format!(
                "{U}Incorrect operand type for operator or function; operator: ADD, operand type: STRING, typeSet: ALLOWED_FOR_ADD_OPERAND"
            )
        );
        assert_eq!(
            update_err("SET a = :n SET b = :s + :n", &m),
            format!(
                "{U}Incorrect operand type for operator or function; operator or function: +, operand type: S"
            )
        );
    }

    #[test]
    fn function_names_are_case_sensitive() {
        let m = maps(&[], &[(":v", n())]);
        assert_eq!(
            update_err("SET x = IF_NOT_EXISTS(x, :v)", &m),
            format!("{U}Invalid function name; function: IF_NOT_EXISTS")
        );
        assert_eq!(
            update_err("SET x = foo(x, :v)", &m),
            format!("{U}Invalid function name; function: foo")
        );
        assert_eq!(
            update_err("SET x = attribute_exists(x)", &m),
            format!(
                "{U}The function is not allowed in an update expression; function: attribute_exists"
            )
        );
        assert_eq!(
            update_err("SET x = IF_NOT_EXISTS(x, :q)", &m),
            format!(
                "{U}An expression attribute value used in expression is not defined; attribute value: :q"
            )
        );
        assert_eq!(update_err("SET x = if_not_exists(x, :v)", &m), "OK");
    }

    #[test]
    fn nested_list_append_before_its_siblings() {
        let m = maps(&[], &[(":a", l()), (":b", l()), (":n", n())]);
        assert_eq!(
            update_err("SET l = if_not_exists(list_append(:a, :b), :q)", &m),
            format!(
                "{U}The function is not allowed to be used this way in an expression; function: list_append"
            )
        );
        assert_eq!(
            update_err("SET l = if_not_exists(list_append(:q, :b), :a)", &m),
            format!(
                "{U}An expression attribute value used in expression is not defined; attribute value: :q"
            )
        );
        assert_eq!(
            update_err("SET l = list_append(:a, :a) + :n", &m),
            format!(
                "{U}The function is not allowed to be used this way in an expression; function: list_append"
            )
        );
        assert_eq!(
            update_err("SET l = list_append(if_not_exists(l, :a), :b)", &m),
            "OK"
        );
    }

    #[test]
    fn operand_counts_and_document_path() {
        let m = maps(&[], &[(":a", l()), (":s", s())]);
        assert_eq!(
            update_err("SET a = if_not_exists(:a)", &m),
            format!(
                "{U}Incorrect number of operands for operator or function; operator or function: if_not_exists, number of operands: 1"
            )
        );
        assert_eq!(
            update_err("SET a = if_not_exists(a, :q, :q2)", &m),
            format!(
                "{U}An expression attribute value used in expression is not defined; attribute value: :q"
            )
        );
        assert_eq!(
            update_err("SET l = if_not_exists(:a, :a)", &m),
            format!(
                "{U}Operator or function requires a document path; operator or function: if_not_exists"
            )
        );
        assert_eq!(
            update_err("SET l = list_append(:s, :a)", &m),
            format!(
                "{U}Incorrect operand type for operator or function; operator or function: list_append, operand type: S"
            )
        );
    }

    #[test]
    fn add_and_delete_operand_types() {
        let m = maps(
            &[],
            &[
                (":s", s()),
                (":n", n()),
                (":b", AttributeValue::B(vec![1])),
                (
                    ":ss",
                    AttributeValue::SS(std::iter::once("a".to_owned()).collect()),
                ),
                (":l", l()),
            ],
        );
        assert_eq!(
            update_err("ADD n :s", &m),
            format!(
                "{U}Incorrect operand type for operator or function; operator: ADD, operand type: STRING, typeSet: ALLOWED_FOR_ADD_OPERAND"
            )
        );
        assert_eq!(
            update_err("ADD n :b", &m),
            format!(
                "{U}Incorrect operand type for operator or function; operator: ADD, operand type: Binary, typeSet: ALLOWED_FOR_ADD_OPERAND"
            )
        );
        assert_eq!(
            update_err("DELETE s :n", &m),
            format!(
                "{U}Incorrect operand type for operator or function; operator: DELETE, operand type: NUMBER, typeSet: ALLOWED_FOR_ADD_OPERAND"
            )
        );
        assert_eq!(
            update_err("DELETE s :l", &m),
            format!(
                "{U}Incorrect operand type for operator or function; operator: DELETE, operand type: LIST, typeSet: ALLOWED_FOR_ADD_OPERAND"
            )
        );
        assert_eq!(update_err("ADD n :n, s :ss DELETE t :ss", &m), "OK");
        assert_eq!(
            update_err("ADD n :q", &m),
            format!(
                "{U}An expression attribute value used in expression is not defined; attribute value: :q"
            )
        );
    }

    #[test]
    fn overlap_after_the_walk() {
        let m = maps(&[], &[(":v", n()), (":s", s())]);
        assert_eq!(
            update_err("SET a = :v, a.b = :v ADD n :s", &m),
            format!(
                "{U}Incorrect operand type for operator or function; operator: ADD, operand type: STRING, typeSet: ALLOWED_FOR_ADD_OPERAND"
            )
        );
        assert_eq!(
            update_err("SET a = :v, a.b = :v", &m),
            format!(
                "{U}Two document paths overlap with each other; must remove or rewrite one of these paths; path one: [a], path two: [a, b]"
            )
        );
    }

    #[test]
    fn condition_function_names_and_positions() {
        let m = maps(&[], &[(":v", s()), (":n", n())]);
        assert_eq!(
            condition_err("ATTRIBUTE_EXISTS(pk)", &m),
            format!("{C}Invalid function name; function: ATTRIBUTE_EXISTS")
        );
        assert_eq!(
            condition_err("foo(pk) AND bar(pk)", &m),
            format!("{C}Invalid function name; function: foo")
        );
        assert_eq!(
            condition_err("if_not_exists(a, :v) = :v", &m),
            format!(
                "{C}The function is not allowed in a condition expression; function: if_not_exists"
            )
        );
        assert_eq!(
            condition_err("attribute_exists(pk) = :v", &m),
            format!(
                "{C}The function is not allowed to be used this way in an expression; function: attribute_exists"
            )
        );
        assert_eq!(
            condition_err("size(pk)", &m),
            format!(
                "{C}The function is not allowed to be used this way in an expression; function: size"
            )
        );
        assert_eq!(
            condition_err("NOT size(pk)", &m),
            format!(
                "{C}The function is not allowed to be used this way in an expression; function: size"
            )
        );
        assert_eq!(
            condition_err("size(attribute_exists(pk)) > :n", &m),
            format!(
                "{C}The function is not allowed to be used this way in an expression; function: attribute_exists"
            )
        );
        assert_eq!(
            condition_err("attribute_exists(size(pk))", &m),
            format!(
                "{C}Operator or function requires a document path; operator or function: attribute_exists"
            )
        );
        assert_eq!(
            condition_err("attribute_exists(:v)", &m),
            format!(
                "{C}Operator or function requires a document path; operator or function: attribute_exists"
            )
        );
        assert_eq!(
            condition_err("foo(#n)", &m),
            format!(
                "{C}An expression attribute name used in the document path is not defined; attribute name: #n"
            )
        );
        assert_eq!(
            condition_err(":q = #n", &m),
            format!(
                "{C}An expression attribute value used in expression is not defined; attribute value: :q"
            )
        );
        assert_eq!(
            condition_err("attribute_type(pk, :v) = :v", &m),
            format!(
                "{C}The function is not allowed to be used this way in an expression; function: attribute_type"
            )
        );
        assert_eq!(condition_err("size(pk) = size(pk)", &m), "OK");
        assert_eq!(condition_err("begins_with(:v, pk)", &m), "OK");
        assert_eq!(
            condition_err("size(pk) > :n AND attribute_exists(pk)", &m),
            "OK"
        );
    }

    #[test]
    fn condition_operand_counts_and_types() {
        let m = maps(
            &[],
            &[
                (":n", n()),
                (":t", AttributeValue::S("S".to_owned())),
                (":xx", AttributeValue::S("XX".to_owned())),
                (":l", l()),
            ],
        );
        assert_eq!(
            condition_err("attribute_exists(pk, pk)", &m),
            format!(
                "{C}Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"
            )
        );
        assert_eq!(
            condition_err("begins_with(pk)", &m),
            format!(
                "{C}Incorrect number of operands for operator or function; operator or function: begins_with, number of operands: 1"
            )
        );
        assert_eq!(
            condition_err("size(pk, pk) > :n", &m),
            format!(
                "{C}Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"
            )
        );
        assert_eq!(
            condition_err("begins_with(pk, :n)", &m),
            format!(
                "{C}Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"
            )
        );
        assert_eq!(
            condition_err("attribute_type(pk, :n)", &m),
            format!(
                "{C}Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"
            )
        );
        assert_eq!(
            condition_err("attribute_type(pk, :xx)", &m),
            format!(
                "{C}Invalid attribute type name found; type: XX, valid types: {{S,SS,N,NS,B,BS,BOOL,NULL,L,M}}"
            )
        );
        assert_eq!(
            condition_err("pk > :l", &m),
            format!(
                "{C}Incorrect operand type for operator or function; operator or function: >, operand type: L"
            )
        );
        assert_eq!(condition_err("attribute_type(pk, :t)", &m), "OK");
    }
}
