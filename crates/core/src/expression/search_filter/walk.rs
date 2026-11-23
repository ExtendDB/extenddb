// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checks the condition grammar applies while it builds the tree, before
//! any search rule.
//!
//! The walk is post-order and left to right: the operands of a node are
//! checked before the node. The first failure is the answer. So in
//! `year > :t` the reserved word in the left operand is reported, not the
//! ordering of a BOOL value.

use std::collections::HashMap;

use super::grammar::{Call, Element, Node, Operand};
use crate::expression::CompareOp;
use crate::expression::resolver::{attribute_type_code, number_value};
use crate::types::AttributeValue;

/// The most values an IN list takes.
pub(super) const MAX_IN_OPERANDS: usize = 100;
/// The most operators an expression takes.
pub(super) const MAX_OPERATORS: usize = 300;
/// The combined size limit: the expression plus every substitution it makes.
pub(super) const MAX_COMBINED_BYTES: usize = 1_048_576;

/// The request's substitution maps, keyed as the request keys them.
pub(super) struct Maps<'a> {
    pub(super) names: Option<&'a HashMap<String, String>>,
    pub(super) values: Option<&'a HashMap<String, AttributeValue>>,
}

impl<'a> Maps<'a> {
    pub(super) fn name(&self, reference: &str) -> Option<&'a str> {
        self.names
            .and_then(|m| m.get(reference))
            .map(String::as_str)
    }

    pub(super) fn value(&self, reference: &str) -> Option<&'a AttributeValue> {
        self.values.and_then(|m| m.get(reference))
    }
}

/// Run the tree-building checks over `node`.
///
/// The tree can be as deep as the expression is long, so the walk keeps its
/// own stack rather than recursing. AND, OR and NOT carry no check of their
/// own, so visiting the leaves left to right is the post-order walk.
///
/// # Errors
///
/// Returns the first failure's message body, without the expression prefix.
pub(super) fn check(node: &Node, maps: &Maps<'_>) -> Result<(), String> {
    let mut pending = vec![node];
    while let Some(node) = pending.pop() {
        match node {
            Node::Or(l, r) | Node::And(l, r) => {
                pending.push(r);
                pending.push(l);
            }
            Node::Not(inner) => pending.push(inner),
            leaf => check_leaf(leaf, maps)?,
        }
    }
    Ok(())
}

fn check_leaf(node: &Node, maps: &Maps<'_>) -> Result<(), String> {
    match node {
        Node::Or(..) | Node::And(..) | Node::Not(_) => Ok(()),
        Node::Compare { left, op, right } => {
            operand(left, maps)?;
            operand(right, maps)?;
            if let Some(symbol) = ordering_symbol(*op) {
                for side in [left, right] {
                    unordered_value(side, maps, symbol)?;
                }
            }
            distinct_first_operand(compare_symbol(*op), left, [right], maps)
        }
        Node::Between { subject, low, high } => {
            for o in [subject, low, high] {
                operand(o, maps)?;
            }
            for o in [subject, low, high] {
                unordered_value(o, maps, "BETWEEN")?;
            }
            distinct_first_operand("BETWEEN", subject, [low, high], maps)?;
            between_bounds(low, high, maps)
        }
        Node::In { subject, list } => {
            operand(subject, maps)?;
            for o in list {
                operand(o, maps)?;
            }
            if list.len() > MAX_IN_OPERANDS {
                return Err(format!(
                    "The IN operator is provided with too many operands; number of operands: {}",
                    list.len()
                ));
            }
            distinct_first_operand("IN", subject, list.iter(), maps)
        }
        Node::Function(call) => calls(Visit::Exit(call, true), maps),
    }
}

/// A step of the operand walk: check an operand, or run a call's own checks
/// once its operands passed. The flag is true for a call that stands as a
/// condition.
enum Visit<'a> {
    Enter(&'a Operand),
    Exit(&'a Call, bool),
}

fn operand(o: &Operand, maps: &Maps<'_>) -> Result<(), String> {
    calls(Visit::Enter(o), maps)
}

/// Check an operand tree post-order, left to right, with its own stack: a
/// call's operands before the call. Calls nest as deep as the expression is
/// long.
fn calls(start: Visit<'_>, maps: &Maps<'_>) -> Result<(), String> {
    let mut pending = Vec::new();
    match start {
        Visit::Exit(call, condition) => {
            pending.push(Visit::Exit(call, condition));
            pending.extend(call.args.iter().rev().map(Visit::Enter));
        }
        enter @ Visit::Enter(_) => pending.push(enter),
    }
    while let Some(step) = pending.pop() {
        match step {
            Visit::Enter(Operand::Path(elements)) => path(elements, maps)?,
            Visit::Enter(Operand::Value(reference)) => {
                if maps.value(reference).is_none() {
                    return Err(format!(
                        "An expression attribute value used in expression is not defined; \
                         attribute value: {reference}"
                    ));
                }
            }
            Visit::Enter(Operand::Call(call)) => {
                pending.push(Visit::Exit(call, false));
                pending.extend(call.args.iter().rev().map(Visit::Enter));
            }
            Visit::Exit(call, condition) => function(call, condition, maps)?,
        }
    }
    Ok(())
}

fn path(elements: &[Element], maps: &Maps<'_>) -> Result<(), String> {
    for element in elements {
        let Element::Name(name) = element else {
            continue;
        };
        if name.starts_with('#') {
            if maps.name(name).is_none() {
                return Err(format!(
                    "An expression attribute name used in the document path is not defined; \
                     attribute name: {name}"
                ));
            }
        } else if crate::expression::reserved_words::is_reserved(name) {
            return Err(format!(
                "Attribute name is a reserved keyword; reserved keyword: {name}"
            ));
        }
    }
    Ok(())
}

/// The condition functions and the number of operands each takes.
fn arity(name: &str) -> Option<usize> {
    match name {
        "attribute_exists" | "attribute_not_exists" | "size" => Some(1),
        "attribute_type" | "begins_with" | "contains" => Some(2),
        _ => None,
    }
}

/// The types `attribute_type` accepts as its second operand, in the order the
/// service lists them.
const TYPE_NAMES: [&str; 10] = ["S", "SS", "N", "NS", "B", "BS", "BOOL", "NULL", "L", "M"];

/// `condition` is true when the call stands as a condition and false when it
/// is an operand. The operands were already checked by [`calls`].
fn function(call: &Call, condition: bool, maps: &Maps<'_>) -> Result<(), String> {
    let name = call.name.as_str();
    let Some(expected) = arity(name) else {
        return Err(if matches!(name, "if_not_exists" | "list_append") {
            format!("The function is not allowed in a condition expression; function: {name}")
        } else {
            format!("Invalid function name; function: {name}")
        });
    };
    if call.args.len() != expected {
        return Err(format!(
            "Incorrect number of operands for operator or function; operator or function: \
             {name}, number of operands: {}",
            call.args.len()
        ));
    }
    // Only size returns a value; every other function is a condition.
    if condition == (name == "size") {
        return Err(format!(
            "The function is not allowed to be used this way in an expression; function: {name}"
        ));
    }
    for arg in &call.args {
        if let Operand::Call(inner) = arg
            && inner.name != "size"
        {
            return Err(format!(
                "The function is not allowed to be used this way in an expression; \
                 function: {}",
                inner.name
            ));
        }
    }
    if !matches!(call.args[0], Operand::Path(_)) {
        return Err(format!(
            "Operator or function requires a document path; operator or function: {name}"
        ));
    }
    match name {
        "begins_with" => {
            if let Some(value) = literal(&call.args[1], maps)
                && !matches!(value, AttributeValue::S(_) | AttributeValue::B(_))
            {
                return Err(operand_type(name, value));
            }
        }
        "attribute_type" => match literal(&call.args[1], maps) {
            Some(AttributeValue::S(type_name)) if TYPE_NAMES.contains(&type_name.as_str()) => {}
            Some(AttributeValue::S(type_name)) => {
                return Err(format!(
                    "Invalid attribute type name found; type: {type_name}, valid types: {{{}}}",
                    TYPE_NAMES.join(",")
                ));
            }
            Some(other) => return Err(operand_type(name, other)),
            None => {}
        },
        _ => {}
    }
    if expected == 2 {
        distinct_first_operand(name, &call.args[0], [&call.args[1]], maps)?;
    }
    Ok(())
}

fn literal<'a>(o: &Operand, maps: &Maps<'a>) -> Option<&'a AttributeValue> {
    match o {
        Operand::Value(reference) => maps.value(reference),
        Operand::Path(_) | Operand::Call(_) => None,
    }
}

fn operand_type(operator: &str, value: &AttributeValue) -> String {
    format!(
        "Incorrect operand type for operator or function; operator or function: {operator}, \
         operand type: {}",
        attribute_type_code(value)
    )
}

fn ordering_symbol(op: CompareOp) -> Option<&'static str> {
    match op {
        CompareOp::Lt => Some("<"),
        CompareOp::Le => Some("<="),
        CompareOp::Gt => Some(">"),
        CompareOp::Ge => Some(">="),
        CompareOp::Eq | CompareOp::Ne => None,
    }
}

pub(super) fn compare_symbol(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "=",
        CompareOp::Ne => "<>",
        other => ordering_symbol(other).unwrap_or_default(),
    }
}

/// An ordering operator takes only S, N and B values.
fn unordered_value(o: &Operand, maps: &Maps<'_>, operator: &str) -> Result<(), String> {
    match literal(o, maps) {
        Some(value)
            if !matches!(
                value,
                AttributeValue::S(_) | AttributeValue::N(_) | AttributeValue::B(_)
            ) =>
        {
            Err(operand_type(operator, value))
        }
        _ => Ok(()),
    }
}

/// The attribute must not be repeated among the other operands:
/// `a BETWEEN a AND :n`.
fn distinct_first_operand<'o>(
    operator: &str,
    first: &Operand,
    rest: impl IntoIterator<Item = &'o Operand>,
    maps: &Maps<'_>,
) -> Result<(), String> {
    let Operand::Path(first_path) = first else {
        return Ok(());
    };
    let resolved = resolved_path(first_path, maps);
    for other in rest {
        if let Operand::Path(p) = other
            && resolved_path(p, maps) == resolved
        {
            return Err(format!(
                "The first operand must be distinct from the remaining operands for this \
                 operator or function; operator: {operator}, first operand: [{resolved}]"
            ));
        }
    }
    Ok(())
}

/// A path with each `#name` resolved, elements joined with `.` and indexes
/// written `[n]`.
pub(super) fn resolved_path(elements: &[Element], maps: &Maps<'_>) -> String {
    let mut out = String::new();
    for element in elements {
        match element {
            Element::Name(name) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(maps.name(name).unwrap_or(name));
            }
            Element::Index(digits) => {
                out.push('[');
                out.push_str(digits);
                out.push(']');
            }
        }
    }
    out
}

/// When both bounds are values: the same type, then the lower bound not
/// above the upper bound.
fn between_bounds(low: &Operand, high: &Operand, maps: &Maps<'_>) -> Result<(), String> {
    let (Some(lo), Some(hi)) = (literal(low, maps), literal(high, maps)) else {
        return Ok(());
    };
    let bounds = || {
        format!(
            "lower bound operand: AttributeValue: {{{}}}, upper bound operand: \
             AttributeValue: {{{}}}",
            shown(lo),
            shown(hi)
        )
    };
    let above = match (lo, hi) {
        (AttributeValue::S(l), AttributeValue::S(h)) => l > h,
        (AttributeValue::B(l), AttributeValue::B(h)) => l > h,
        (AttributeValue::N(l), AttributeValue::N(h)) => match (number_value(l), number_value(h)) {
            (Some(l), Some(h)) => l > h,
            _ => false,
        },
        _ => {
            return Err(format!(
                "The BETWEEN operator requires same data type for lower and upper bounds; {}",
                bounds()
            ));
        }
    };
    if above {
        return Err(format!(
            "The BETWEEN operator requires upper bound to be greater than or equal to lower \
             bound; {}",
            bounds()
        ));
    }
    Ok(())
}

/// A bound as the BETWEEN messages show it: `N:5`, `S:US`, or `B:` and the
/// base64 of the bytes.
fn shown(value: &AttributeValue) -> String {
    use base64::Engine as _;
    match value {
        AttributeValue::S(s) => format!("S:{s}"),
        AttributeValue::N(n) => format!("N:{n}"),
        AttributeValue::B(b) => {
            format!("B:{}", base64::engine::general_purpose::STANDARD.encode(b))
        }
        other => attribute_type_code(other).to_owned(),
    }
}

/// The number of operators in `node`: each AND, OR, NOT, comparison, BETWEEN,
/// IN and function is one, a function used as an operand included.
/// Parentheses are none.
pub(super) fn operators(node: &Node) -> usize {
    let (mut calls, mut nodes) = (0, 0);
    each_operand(
        node,
        |o| {
            if matches!(o, Operand::Call(_)) {
                calls += 1;
            }
        },
        |_| nodes += 1,
    );
    calls + nodes
}

/// Visit every node and every operand of `node`, calls and their operands
/// included, with an explicit stack.
fn each_operand<'a>(
    node: &'a Node,
    mut on_operand: impl FnMut(&'a Operand),
    mut on_node: impl FnMut(&'a Node),
) {
    let mut nodes = vec![node];
    let mut operands: Vec<&'a Operand> = Vec::new();
    while let Some(node) = nodes.pop() {
        on_node(node);
        match node {
            Node::Or(l, r) | Node::And(l, r) => nodes.extend([&**l, &**r]),
            Node::Not(inner) => nodes.push(inner),
            Node::Compare { left, right, .. } => operands.extend([left, right]),
            Node::Between { subject, low, high } => operands.extend([subject, low, high]),
            Node::In { subject, list } => {
                operands.push(subject);
                operands.extend(list);
            }
            Node::Function(call) => operands.extend(&call.args),
        }
        while let Some(o) = operands.pop() {
            on_operand(o);
            if let Operand::Call(call) = o {
                operands.extend(&call.args);
            }
        }
    }
}

/// The expression's bytes plus the bytes each substitution adds, counted at
/// every use.
pub(super) fn combined_bytes(src: &str, node: &Node, maps: &Maps<'_>) -> usize {
    let mut total = src.len();
    each_operand(
        node,
        |o| {
            total += match o {
                Operand::Path(elements) => elements
                    .iter()
                    .map(|e| match e {
                        Element::Name(n) if n.starts_with('#') => maps.name(n).map_or(0, str::len),
                        Element::Name(_) | Element::Index(_) => 0,
                    })
                    .sum(),
                Operand::Value(reference) => maps
                    .value(reference)
                    .map_or(0, crate::types::attribute_value_size),
                Operand::Call(_) => 0,
            };
        },
        |_| {},
    );
    total
}
