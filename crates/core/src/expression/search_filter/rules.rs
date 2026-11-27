// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The search rules, applied once the tree is built: one condition for each
//! comparison, BETWEEN, IN and function, in written order, and the condition
//! limit.

use super::grammar::{Call, Element, Node, Operand};
use super::walk::Maps;
use crate::expression::{CompareOp, Expr, PathElement};
use crate::types::AttributeValue;

/// The operator of one condition, as written. `:v < a` is [`Operator::Lt`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Between,
    In,
    Exists,
    NotExists,
}

impl Operator {
    /// `<`, `<=`, `>`, `>=` and BETWEEN compare by order, which only a Number
    /// attribute has in a search.
    #[must_use]
    pub fn is_ordering(self) -> bool {
        matches!(
            self,
            Self::Lt | Self::Le | Self::Gt | Self::Ge | Self::Between
        )
    }

    /// The operator as the schema messages name it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::Between => "BETWEEN",
            Self::In => "IN",
            Self::Exists => "attribute_exists",
            Self::NotExists => "attribute_not_exists",
        }
    }

    fn of(op: CompareOp) -> Self {
        match op {
            CompareOp::Eq => Self::Eq,
            CompareOp::Ne => Self::Ne,
            CompareOp::Lt => Self::Lt,
            CompareOp::Le => Self::Le,
            CompareOp::Gt => Self::Gt,
            CompareOp::Ge => Self::Ge,
        }
    }
}

/// One condition on one attribute.
#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    /// The attribute, with a `#name` resolved.
    pub attribute: String,
    pub operator: Operator,
    /// The values in written order: one for a comparison, the bounds for
    /// BETWEEN, the list for IN, none for a function.
    pub values: Vec<AttributeValue>,
    /// True when every operator above the condition is AND, so every item the
    /// search returns satisfies it.
    pub must_match: bool,
}

/// Collect the conditions of `root` in written order.
///
/// # Errors
///
/// Returns the first failure's message body, without the expression prefix.
pub(super) fn collect(
    root: &Node,
    maps: &Maps<'_>,
    max_conditions: usize,
) -> Result<Vec<Condition>, String> {
    let mut out = Vec::new();
    walk(root, true, maps, max_conditions, root, &mut out)?;
    Ok(out)
}

fn walk(
    node: &Node,
    must_match: bool,
    maps: &Maps<'_>,
    max: usize,
    root: &Node,
    out: &mut Vec<Condition>,
) -> Result<(), String> {
    let condition = match node {
        Node::And(l, r) => {
            walk(l, must_match, maps, max, root, out)?;
            return walk(r, must_match, maps, max, root, out);
        }
        // Either side of an OR is enough, and NOT inverts, so a condition
        // below them need not hold for every returned item.
        Node::Or(l, r) => {
            walk(l, false, maps, max, root, out)?;
            return walk(r, false, maps, max, root, out);
        }
        Node::Not(inner) => return walk(inner, false, maps, max, root, out),
        Node::Compare { left, op, right } => {
            let (path, value) = match (left, right) {
                (Operand::Path(p), Operand::Value(v)) | (Operand::Value(v), Operand::Path(p)) => {
                    (p, v)
                }
                (Operand::Path(_), Operand::Path(_)) => {
                    return Err("Invalid condition in SearchConditionExpression: Multiple \
                                attribute names used in one condition"
                        .to_owned());
                }
                (Operand::Value(_), Operand::Value(_)) => return Err(no_attribute_names()),
                (Operand::Call(call), _) | (_, Operand::Call(call)) => {
                    return Err(unsupported(call));
                }
            };
            let values = vec![value_of(value, maps)?];
            condition(path, Operator::of(*op), values, must_match, maps)?
        }
        Node::In { subject, list } => {
            let values = bound_values(list, maps, IN_LIST_ATTRIBUTE)?;
            let path = subject_path(subject)?;
            condition(path, Operator::In, values, must_match, maps)?
        }
        Node::Between { subject, low, high } => {
            let bounds = [low.clone(), high.clone()];
            let values = bound_values(&bounds, maps, BETWEEN_BOUND_ATTRIBUTE)?;
            let path = subject_path(subject)?;
            condition(path, Operator::Between, values, must_match, maps)?
        }
        Node::Function(call) => {
            let operator = match call.name.as_str() {
                "attribute_exists" => Operator::Exists,
                "attribute_not_exists" => Operator::NotExists,
                _ => return Err(unsupported(call)),
            };
            let Some(Operand::Path(path)) = call.args.first() else {
                return Err(format!(
                    "Operator or function requires a document path; operator or function: {}",
                    call.name
                ));
            };
            condition(path, operator, Vec::new(), must_match, maps)?
        }
    };
    // The condition is checked first, so a malformed condition past the limit
    // gets its own message. The count covers the whole expression.
    if out.len() >= max {
        return Err(format!(
            "The expression contains too many conditions; number of conditions: {}, \
             maximum: {max}",
            count(root)
        ));
    }
    out.push(condition);
    Ok(())
}

const IN_LIST_ATTRIBUTE: &str = "The IN operator list must contain only expression attribute \
                                 values (:value), not attribute names";
const BETWEEN_BOUND_ATTRIBUTE: &str = "The BETWEEN operator bounds must be expression \
                                       attribute values (:value), not attribute names";

fn no_attribute_names() -> String {
    "Invalid condition in SearchConditionExpression: No attribute names used in a condition"
        .to_owned()
}

fn unsupported(call: &Call) -> String {
    format!(
        "The function is not allowed in a search condition; function: {}",
        call.name
    )
}

/// The values of an IN list or of BETWEEN bounds, checked before the attribute.
fn bound_values(
    operands: &[Operand],
    maps: &Maps<'_>,
    path_message: &str,
) -> Result<Vec<AttributeValue>, String> {
    operands
        .iter()
        .map(|o| match o {
            Operand::Value(v) => value_of(v, maps),
            Operand::Path(_) => Err(path_message.to_owned()),
            Operand::Call(call) => Err(unsupported(call)),
        })
        .collect()
}

fn subject_path(subject: &Operand) -> Result<&[Element], String> {
    match subject {
        Operand::Path(p) => Ok(p),
        Operand::Value(_) => Err(no_attribute_names()),
        Operand::Call(call) => Err(unsupported(call)),
    }
}

fn value_of(reference: &str, maps: &Maps<'_>) -> Result<AttributeValue, String> {
    // The tree-building checks already refused an undefined value.
    maps.value(reference).cloned().ok_or_else(|| {
        format!(
            "An expression attribute value used in expression is not defined; attribute value: \
             {reference}"
        )
    })
}

fn condition(
    path: &[Element],
    operator: Operator,
    values: Vec<AttributeValue>,
    must_match: bool,
    maps: &Maps<'_>,
) -> Result<Condition, String> {
    let [Element::Name(first)] = path else {
        return Err(
            "SearchConditionExpression cannot have conditions on nested attributes".to_owned(),
        );
    };
    Ok(Condition {
        attribute: maps.name(first).unwrap_or(first).to_owned(),
        operator,
        values,
        must_match,
    })
}

/// A comparison, BETWEEN, IN and function count one; AND, OR and NOT none.
fn count(node: &Node) -> usize {
    match node {
        Node::And(l, r) | Node::Or(l, r) => count(l) + count(r),
        Node::Not(inner) => count(inner),
        Node::Compare { .. } | Node::Between { .. } | Node::In { .. } | Node::Function(_) => 1,
    }
}

/// The tree as the condition evaluator reads it. `#name` and `:value`
/// references stay as written; the evaluator resolves them through
/// [`crate::expression::ExpressionMaps`].
pub(super) fn lower(node: &Node) -> Expr {
    match node {
        Node::Or(l, r) => Expr::Or(Box::new(lower(l)), Box::new(lower(r))),
        Node::And(l, r) => Expr::And(Box::new(lower(l)), Box::new(lower(r))),
        Node::Not(inner) => Expr::Not(Box::new(lower(inner))),
        Node::Compare { left, op, right } => Expr::Compare {
            left: Box::new(lower_operand(left)),
            op: *op,
            right: Box::new(lower_operand(right)),
        },
        Node::Between { subject, low, high } => Expr::Between {
            operand: Box::new(lower_operand(subject)),
            low: Box::new(lower_operand(low)),
            high: Box::new(lower_operand(high)),
        },
        Node::In { subject, list } => Expr::In {
            operand: Box::new(lower_operand(subject)),
            list: list.iter().map(lower_operand).collect(),
        },
        Node::Function(call) => lower_call(call),
    }
}

fn lower_call(call: &Call) -> Expr {
    Expr::Function {
        name: call.name.clone(),
        args: call.args.iter().map(lower_operand).collect(),
    }
}

fn lower_operand(o: &Operand) -> Expr {
    match o {
        Operand::Path(elements) => Expr::Path(
            elements
                .iter()
                .map(|e| match e {
                    Element::Name(n) => PathElement::Attribute(n.clone()),
                    Element::Index(d) => PathElement::Index(d.parse().unwrap_or(usize::MAX)),
                })
                .collect(),
        ),
        Operand::Value(reference) => {
            Expr::Placeholder(reference.trim_start_matches(':').to_owned())
        }
        Operand::Call(call) => lower_call(call),
    }
}
