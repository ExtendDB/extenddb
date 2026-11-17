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
/// # Errors
///
/// Returns the first failure's message body, without the expression prefix.
pub(super) fn check(node: &Node, maps: &Maps<'_>) -> Result<(), String> {
    match node {
        Node::Or(l, r) | Node::And(l, r) => {
            check(l, maps)?;
            check(r, maps)
        }
        Node::Not(inner) => check(inner, maps),
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
        Node::Function(call) => function(call, true, maps),
    }
}

fn operand(o: &Operand, maps: &Maps<'_>) -> Result<(), String> {
    match o {
        Operand::Path(elements) => path(elements, maps),
        Operand::Value(reference) => match maps.value(reference) {
            Some(_) => Ok(()),
            None => Err(format!(
                "An expression attribute value used in expression is not defined; \
                 attribute value: {reference}"
            )),
        },
        Operand::Call(call) => function(call, false, maps),
    }
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
/// is an operand.
fn function(call: &Call, condition: bool, maps: &Maps<'_>) -> Result<(), String> {
    for arg in &call.args {
        operand(arg, maps)?;
    }
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
    fn in_operand(o: &Operand) -> usize {
        match o {
            Operand::Call(call) => 1 + call.args.iter().map(in_operand).sum::<usize>(),
            Operand::Path(_) | Operand::Value(_) => 0,
        }
    }
    match node {
        Node::Or(l, r) | Node::And(l, r) => 1 + operators(l) + operators(r),
        Node::Not(inner) => 1 + operators(inner),
        Node::Compare { left, right, .. } => 1 + in_operand(left) + in_operand(right),
        Node::Between { subject, low, high } => {
            1 + in_operand(subject) + in_operand(low) + in_operand(high)
        }
        Node::In { subject, list } => {
            1 + in_operand(subject) + list.iter().map(in_operand).sum::<usize>()
        }
        Node::Function(call) => 1 + call.args.iter().map(in_operand).sum::<usize>(),
    }
}

/// The expression's bytes plus the bytes each substitution adds, counted at
/// every use.
pub(super) fn combined_bytes(src: &str, node: &Node, maps: &Maps<'_>) -> usize {
    fn in_operand(o: &Operand, maps: &Maps<'_>) -> usize {
        match o {
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
            Operand::Call(call) => call.args.iter().map(|a| in_operand(a, maps)).sum(),
        }
    }
    fn in_node(node: &Node, maps: &Maps<'_>) -> usize {
        match node {
            Node::Or(l, r) | Node::And(l, r) => in_node(l, maps) + in_node(r, maps),
            Node::Not(inner) => in_node(inner, maps),
            Node::Compare { left, right, .. } => in_operand(left, maps) + in_operand(right, maps),
            Node::Between { subject, low, high } => [subject, low, high]
                .into_iter()
                .map(|o| in_operand(o, maps))
                .sum(),
            Node::In { subject, list } => {
                in_operand(subject, maps) + list.iter().map(|o| in_operand(o, maps)).sum::<usize>()
            }
            Node::Function(call) => call.args.iter().map(|a| in_operand(a, maps)).sum(),
        }
    }
    src.len() + in_node(node, maps)
}
