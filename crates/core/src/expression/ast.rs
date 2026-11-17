// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Expression AST types for condition and update expressions.

use super::resolver::{ExpressionMaps, resolve_name_ref};
use crate::types::KeySchemaElement;

/// A single element in a document path.
#[derive(Debug, Clone, PartialEq)]
pub enum PathElement {
    /// Attribute name or resolved `#name` reference.
    Attribute(String),
    /// List index dereference: `[0]`.
    Index(usize),
}

/// Comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Arithmetic operators (used in SET expressions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
}

/// Expression AST node.
///
/// Used for both condition expressions and value expressions within
/// update actions. The parser produces this tree; the evaluator walks it.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Document path: `address.city`, `tags[0]`, `#n`.
    Path(Vec<PathElement>),
    /// Placeholder reference: `:val1`.
    Placeholder(String),
    /// Binary comparison: `price > :min`.
    Compare {
        left: Box<Expr>,
        op: CompareOp,
        right: Box<Expr>,
    },
    /// Logical AND.
    And(Box<Expr>, Box<Expr>),
    /// Logical OR.
    Or(Box<Expr>, Box<Expr>),
    /// Logical NOT.
    Not(Box<Expr>),
    /// Function call: `attribute_exists(path)`, `attribute_not_exists(path)`.
    Function { name: String, args: Vec<Expr> },
    /// Arithmetic: `price + :tax` (SET expressions only).
    Arithmetic {
        left: Box<Expr>,
        op: ArithOp,
        right: Box<Expr>,
    },
    /// `operand BETWEEN operand AND operand`.
    Between {
        operand: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// `operand IN (operand, operand, ...)`.
    In { operand: Box<Expr>, list: Vec<Expr> },
}

/// A single update action from an `UpdateExpression`.
#[derive(Debug, Clone, PartialEq)]
pub enum UpdateAction {
    /// `SET path = value_expr`
    Set { path: Vec<PathElement>, value: Expr },
    /// `REMOVE path`
    Remove { path: Vec<PathElement> },
    /// `ADD path value` — adds to a number or inserts into a set.
    Add { path: Vec<PathElement>, value: Expr },
    /// `DELETE path value` — removes elements from a set.
    Delete { path: Vec<PathElement>, value: Expr },
}

impl UpdateAction {
    /// Whether this action can only take data away: `REMOVE` or `DELETE`.
    #[must_use]
    pub fn removes_only(&self) -> bool {
        matches!(self, Self::Remove { .. } | Self::Delete { .. })
    }
}

/// Whether an update made of these actions writes nothing when the item does
/// not exist.
///
/// Amazon DynamoDB creates the item for `SET`, `ADD`, and for an update with
/// no actions at all (a key-only item), but an update whose actions are all
/// `REMOVE` or `DELETE` on a missing key leaves the table unchanged: no item,
/// no stream record, no `Attributes` in the response for any `ReturnValues`
/// (measured 2026-10-24, us-east-1, hash and hash-plus-range tables).
#[must_use]
pub fn removes_only(actions: &[UpdateAction]) -> bool {
    !actions.is_empty() && actions.iter().all(UpdateAction::removes_only)
}

/// Whether `condition` holds exactly when the item does not exist.
///
/// Every stored item carries its key attributes. So `attribute_not_exists` on
/// a key attribute holds against no item and fails against every item,
/// whatever else the item holds, and `attribute_exists` on one is its
/// opposite. `AND`, `OR` and `NOT` of such terms keep that property when their
/// operands agree, for example `attribute_not_exists(pk) AND
/// attribute_not_exists(sk)` or `NOT attribute_exists(pk)`. A backend can
/// decide such a condition by whether the key is taken, without reading the
/// item. A `#name` that does not resolve answers false, so that the request
/// takes the path that reports the validation error.
#[must_use]
pub fn requires_absent_item(
    condition: &Expr,
    key_schema: &[KeySchemaElement],
    maps: &ExpressionMaps,
) -> bool {
    holds_when_absent(condition, key_schema, maps) == Some(true)
}

/// `Some(true)` when `condition` holds exactly when the item does not exist,
/// `Some(false)` when it holds exactly when the item exists, and `None` when
/// the key alone does not decide it.
fn holds_when_absent(
    condition: &Expr,
    key_schema: &[KeySchemaElement],
    maps: &ExpressionMaps,
) -> Option<bool> {
    match condition {
        Expr::And(a, b) | Expr::Or(a, b) => {
            let a = holds_when_absent(a, key_schema, maps)?;
            (holds_when_absent(b, key_schema, maps)? == a).then_some(a)
        }
        Expr::Not(e) => holds_when_absent(e, key_schema, maps).map(|absent| !absent),
        Expr::Function { name, args } => {
            let absent = match name.as_str() {
                "attribute_not_exists" => true,
                "attribute_exists" => false,
                _ => return None,
            };
            let [Expr::Path(path)] = args.as_slice() else {
                return None;
            };
            let [PathElement::Attribute(attr)] = path.as_slice() else {
                return None;
            };
            let attr = resolve_name_ref(attr, maps).ok()?;
            key_schema
                .iter()
                .any(|k| k.attribute_name == attr.as_ref())
                .then_some(absent)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::removes_only;
    use crate::expression::tokenizer::tokenize;
    use crate::expression::update_parser::parse_update;

    fn actions(expr: &str) -> Vec<super::UpdateAction> {
        parse_update(&tokenize(expr).expect("tokens")).expect("actions")
    }

    #[test]
    fn remove_and_delete_actions_remove_only() {
        assert!(removes_only(&actions("REMOVE a")));
        assert!(removes_only(&actions("REMOVE a, b")));
        assert!(removes_only(&actions("REMOVE a.b[0]")));
        assert!(removes_only(&actions("DELETE s :v")));
        assert!(removes_only(&actions("REMOVE a DELETE s :v")));
    }

    #[test]
    fn set_add_or_no_actions_create_the_item() {
        assert!(!removes_only(&[]));
        assert!(!removes_only(&actions("SET a = :v")));
        assert!(!removes_only(&actions("ADD n :v")));
        assert!(!removes_only(&actions("REMOVE a SET b = :v")));
        assert!(!removes_only(&actions("DELETE s :v ADD n :w")));
    }
}

#[cfg(test)]
mod requires_absent_item_tests {
    use super::requires_absent_item;
    use crate::expression::parser::parse_condition;
    use crate::expression::resolver::ExpressionMaps;
    use crate::expression::tokenizer::tokenize;
    use crate::types::{KeySchemaElement, KeyType};

    fn check(expr: &str, names: &[(&str, &str)]) -> bool {
        let schema = [
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            },
        ];
        let cond = parse_condition(&tokenize(expr).expect("tokens")).expect("condition");
        let mut maps = ExpressionMaps::default();
        for (k, v) in names {
            maps.names.insert((*k).to_owned(), (*v).to_owned());
        }
        requires_absent_item(&cond, &schema, &maps)
    }

    #[test]
    fn terms_on_key_attributes_that_agree_require_an_absent_item() {
        assert!(check("attribute_not_exists(pk)", &[]));
        assert!(check("attribute_not_exists(sk)", &[]));
        assert!(check("attribute_not_exists(#k)", &[("k", "pk")]));
        assert!(check(
            "attribute_not_exists(pk) AND attribute_not_exists(sk)",
            &[]
        ));
        assert!(check(
            "attribute_not_exists(pk) OR attribute_not_exists(sk)",
            &[]
        ));
        assert!(check("NOT attribute_exists(pk)", &[]));
        assert!(check(
            "(NOT attribute_exists(pk)) AND (attribute_not_exists(sk))",
            &[]
        ));
        assert!(check(
            "NOT (attribute_exists(pk) OR attribute_exists(sk))",
            &[]
        ));
    }

    #[test]
    fn other_conditions_do_not() {
        assert!(!check("attribute_exists(pk)", &[]));
        assert!(!check("NOT attribute_not_exists(pk)", &[]));
        assert!(!check("attribute_not_exists(other)", &[]));
        assert!(!check("attribute_not_exists(#k)", &[("k", "other")]));
        assert!(!check("attribute_not_exists(#missing)", &[]));
        assert!(!check("attribute_not_exists(pk.x)", &[]));
        assert!(!check("attribute_not_exists(pk[0])", &[]));
        assert!(!check(
            "attribute_not_exists(pk) AND attribute_not_exists(other)",
            &[]
        ));
        assert!(!check(
            "attribute_not_exists(pk) AND attribute_exists(sk)",
            &[]
        ));
        assert!(!check(
            "attribute_not_exists(pk) OR attribute_exists(sk)",
            &[]
        ));
        assert!(!check("attribute_type(pk, :t)", &[]));
        assert!(!check("pk = :v", &[]));
    }
}
