// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Expression AST types for condition and update expressions.

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
