// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Compile-time analyzer for filter-pushdown eligibility.
//!
//! For a DDB [`Expr`] and its accompanying [`ExpressionMaps`], returns
//! [`Pushable::Yes`] when the expression can be safely compiled to a
//! MongoDB filter and evaluated by the storage layer, or
//! [`Pushable::No`] with a reason string when at least one subexpression
//! must fall back to session-scoped in-Rust evaluation.
//!
//! Whole-condition all-or-nothing: if any subexpression is not pushable,
//! the entire expression falls back. You cannot cherry-pick — evaluating
//! part of an `AND` / `OR` in the storage layer and part in the
//! application layer would confuse the composition semantics.
//!
//! **Pushable subset** (see `todo.md` A5 for full rationale):
//!
//! - `attribute_exists(path)`, `attribute_not_exists(path)`
//! - `attribute_type(path, :t)` for any type tag
//! - `begins_with(path, :prefix)` where `:prefix` is `S`
//! - `contains(path, :val)` where `:val` is `S`
//! - `path <op> :v` where `:v` is `S`, or `:v` is `B` and op is `Eq` / `Ne`
//! - `AND`, `OR` of pushable subexpressions
//! - `NOT attribute_exists(path)` / `NOT attribute_not_exists(path)` only
//!
//! **Not pushable** (falls back to in-Rust):
//!
//! - Any operand of type `N` in any position (numbers are stored as
//!   strings, so MongoDB comparators evaluate lexicographically —
//!   `"10" > "9"` is false string-wise, true numerically)
//! - `size(...)` (MongoDB's `$strLenBytes` and `$strLenCP` don't match
//!   DDB's UTF-16 code unit count for strings)
//! - `NOT` around anything except `attribute_exists` /
//!   `attribute_not_exists` (three-valued logic on missing paths
//!   diverges from MongoDB's `$nor` semantics)
//! - `IN` and `BETWEEN` — the compiler emits them but the analyzer
//!   currently marks them non-pushable pending proptest coverage.
//!   Restoring them is a follow-up; the in-Rust fallback path is
//!   already correct.
//! - `path <lt|le|gt|ge> :v` where `:v` is `B` (base64 string ordering
//!   ≠ bytewise byte ordering across mismatched lengths)
//! - Any operand type the analyzer doesn't yet classify

use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps};
use extenddb_core::types::AttributeValue;

/// Outcome of the pushdown analyzer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pushable {
    /// The expression compiles to a MongoDB filter with semantics that
    /// match `extenddb_core::expression::evaluate_condition`.
    Yes,
    /// At least one subexpression falls outside the pushable subset.
    /// The caller must evaluate the whole expression in-Rust.
    No(&'static str),
}

impl Pushable {
    pub fn is_yes(&self) -> bool {
        matches!(self, Pushable::Yes)
    }
}

/// Decide whether a compiled MongoDB filter for `expr` will agree with
/// `evaluate_condition(expr, item, maps)` for every item.
///
/// Conservative: unknown constructs return `Pushable::No`.
pub fn is_pushable(expr: &Expr, maps: &ExpressionMaps) -> Pushable {
    walk(expr, maps)
}

fn walk(expr: &Expr, maps: &ExpressionMaps) -> Pushable {
    match expr {
        Expr::Function { name, args } => match name.to_lowercase().as_str() {
            "attribute_exists" | "attribute_not_exists" => {
                // Always pushable; missing-path semantics match MongoDB's
                // `$exists`.
                if args.len() == 1 {
                    Pushable::Yes
                } else {
                    Pushable::No("attribute_exists/not_exists arity")
                }
            }
            "attribute_type" => {
                if args.len() != 2 {
                    return Pushable::No("attribute_type arity");
                }
                // The type argument must resolve to a String (the type
                // tag: "S", "N", "B", "BOOL", "NULL", "L", "M", "SS",
                // "NS", "BS"). Anything else is a parse-time error but
                // we defensively check.
                if !arg_resolves_to_scalar_type(&args[1], maps, AttrKind::S) {
                    return Pushable::No("attribute_type non-string tag");
                }
                Pushable::Yes
            }
            "begins_with" => {
                if args.len() != 2 {
                    return Pushable::No("begins_with arity");
                }
                if !arg_resolves_to_scalar_type(&args[1], maps, AttrKind::S) {
                    return Pushable::No("begins_with non-string prefix");
                }
                Pushable::Yes
            }
            "contains" => {
                if args.len() != 2 {
                    return Pushable::No("contains arity");
                }
                // Only S operands. B is emittable but not yet
                // proptest-covered; N is not pushable because number
                // storage is string-based.
                match value_kind(&args[1], maps) {
                    Some(AttrKind::S) => Pushable::Yes,
                    Some(AttrKind::N) => Pushable::No("contains on N operand"),
                    Some(AttrKind::B) => Pushable::No("contains on B operand (not yet covered)"),
                    _ => Pushable::No("contains on unsupported operand type"),
                }
            }
            "size" => Pushable::No("size() — UTF-16 mismatch with MongoDB"),
            _ => Pushable::No("unknown function"),
        },
        Expr::Compare { left, op, right } => {
            // Both operands must be pushable operand types. Numbers
            // anywhere → not pushable. Sets / lists / maps in the
            // operand position → not pushable in the current subset.
            let left_kind = operand_kind(left, maps);
            let right_kind = operand_kind(right, maps);
            let (Some(lk), Some(rk)) = (left_kind, right_kind) else {
                return Pushable::No("Compare with un-inferrable operand kind");
            };
            // Number anywhere disqualifies.
            if matches!(lk, AttrKind::N) || matches!(rk, AttrKind::N) {
                return Pushable::No("Compare with N operand");
            }
            match (lk, rk, op) {
                // Field <op> S-value: any comparator OK (lex matches wire form).
                (AttrKind::Field, AttrKind::S, _) | (AttrKind::S, AttrKind::Field, _) => {
                    Pushable::Yes
                }
                // Field = / <> B-value: OK. Ordering on B not OK
                // (base64 string ordering ≠ bytewise).
                (AttrKind::Field, AttrKind::B, CompareOp::Eq | CompareOp::Ne)
                | (AttrKind::B, AttrKind::Field, CompareOp::Eq | CompareOp::Ne) => Pushable::Yes,
                (AttrKind::Field, AttrKind::B, _) | (AttrKind::B, AttrKind::Field, _) => {
                    Pushable::No("ordering comparator on B operand")
                }
                // Field = / <> BOOL / NULL: OK.
                (AttrKind::Field, AttrKind::Bool, CompareOp::Eq | CompareOp::Ne)
                | (AttrKind::Bool, AttrKind::Field, CompareOp::Eq | CompareOp::Ne)
                | (AttrKind::Field, AttrKind::Null, CompareOp::Eq | CompareOp::Ne)
                | (AttrKind::Null, AttrKind::Field, CompareOp::Eq | CompareOp::Ne) => Pushable::Yes,
                (AttrKind::Field, AttrKind::Bool | AttrKind::Null, _)
                | (AttrKind::Bool | AttrKind::Null, AttrKind::Field, _) => {
                    Pushable::No("ordering on BOOL / NULL operand")
                }
                // Field vs. Field: the compiler emits $expr; pushable.
                (AttrKind::Field, AttrKind::Field, _) => Pushable::Yes,
                // Two literals — pushable but degenerate.
                _ => Pushable::No("Compare with unusual operand kinds"),
            }
        }
        Expr::And(l, r) => match (walk(l, maps), walk(r, maps)) {
            (Pushable::Yes, Pushable::Yes) => Pushable::Yes,
            (Pushable::No(r), _) | (_, Pushable::No(r)) => Pushable::No(r),
        },
        Expr::Or(l, r) => match (walk(l, maps), walk(r, maps)) {
            (Pushable::Yes, Pushable::Yes) => Pushable::Yes,
            (Pushable::No(r), _) | (_, Pushable::No(r)) => Pushable::No(r),
        },
        Expr::Not(inner) => {
            // Only pushable when inner is exactly an existence check.
            // Everything else — comparisons, functions, nested logic —
            // is disallowed because MongoDB's $nor on missing paths
            // returns true where DDB's three-valued logic returns false.
            match inner.as_ref() {
                Expr::Function { name, args } if args.len() == 1 => {
                    let n = name.to_lowercase();
                    if n == "attribute_exists" || n == "attribute_not_exists" {
                        Pushable::Yes
                    } else {
                        Pushable::No("NOT around non-existence function")
                    }
                }
                _ => Pushable::No("NOT around non-existence expression"),
            }
        }
        Expr::Between { .. } => Pushable::No("BETWEEN — analyzer coverage pending"),
        Expr::In { .. } => Pushable::No("IN — analyzer coverage pending"),
        Expr::Path(_) | Expr::Placeholder(_) | Expr::Arithmetic { .. } => {
            Pushable::No("bare path/placeholder/arithmetic at top level")
        }
    }
}

/// The compiler's operand-kind classification, used by the analyzer to
/// reason about type-mixing rules.
#[derive(Debug, Clone, Copy)]
enum AttrKind {
    /// Reference to a document field (`Expr::Path`).
    Field,
    S,
    N,
    B,
    Bool,
    Null,
}

fn operand_kind(expr: &Expr, maps: &ExpressionMaps) -> Option<AttrKind> {
    match expr {
        Expr::Path(_) => Some(AttrKind::Field),
        Expr::Placeholder(_) => value_kind(expr, maps),
        _ => None,
    }
}

fn value_kind(expr: &Expr, maps: &ExpressionMaps) -> Option<AttrKind> {
    let Expr::Placeholder(name) = expr else {
        return None;
    };
    let av = maps.resolve_value(name).ok()?;
    Some(match av {
        AttributeValue::S(_) => AttrKind::S,
        AttributeValue::N(_) => AttrKind::N,
        AttributeValue::B(_) => AttrKind::B,
        AttributeValue::Bool(_) => AttrKind::Bool,
        AttributeValue::Null => AttrKind::Null,
        _ => return None,
    })
}

fn arg_resolves_to_scalar_type(expr: &Expr, maps: &ExpressionMaps, expected: AttrKind) -> bool {
    matches!(
        (value_kind(expr, maps), expected),
        (Some(AttrKind::S), AttrKind::S)
            | (Some(AttrKind::N), AttrKind::N)
            | (Some(AttrKind::B), AttrKind::B)
            | (Some(AttrKind::Bool), AttrKind::Bool)
            | (Some(AttrKind::Null), AttrKind::Null)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::expression::PathElement;
    use std::collections::HashMap;

    fn maps_with(values: &[(&str, AttributeValue)]) -> ExpressionMaps {
        let mut m = HashMap::new();
        for (k, v) in values {
            m.insert((*k).to_string(), v.clone());
        }
        ExpressionMaps::new(HashMap::new(), m)
    }

    fn path(name: &str) -> Expr {
        Expr::Path(vec![PathElement::Attribute(name.to_string())])
    }

    #[test]
    fn attribute_exists_is_pushable() {
        let expr = Expr::Function {
            name: "attribute_exists".into(),
            args: vec![path("a")],
        };
        assert_eq!(is_pushable(&expr, &maps_with(&[])), Pushable::Yes);
    }

    #[test]
    fn size_is_not_pushable() {
        let expr = Expr::Function {
            name: "size".into(),
            args: vec![path("a")],
        };
        assert!(!is_pushable(&expr, &maps_with(&[])).is_yes());
    }

    #[test]
    fn number_operand_is_not_pushable() {
        let expr = Expr::Compare {
            left: Box::new(path("a")),
            op: CompareOp::Eq,
            right: Box::new(Expr::Placeholder(":n".into())),
        };
        let maps = maps_with(&[(":n", AttributeValue::N("42".into()))]);
        assert!(!is_pushable(&expr, &maps).is_yes());
    }

    #[test]
    fn string_equality_is_pushable() {
        let expr = Expr::Compare {
            left: Box::new(path("a")),
            op: CompareOp::Eq,
            right: Box::new(Expr::Placeholder(":s".into())),
        };
        let maps = maps_with(&[(":s", AttributeValue::S("x".into()))]);
        assert_eq!(is_pushable(&expr, &maps), Pushable::Yes);
    }

    #[test]
    fn binary_equality_is_pushable() {
        let expr = Expr::Compare {
            left: Box::new(path("a")),
            op: CompareOp::Eq,
            right: Box::new(Expr::Placeholder(":b".into())),
        };
        let maps = maps_with(&[(":b", AttributeValue::B(vec![0, 1, 2]))]);
        assert_eq!(is_pushable(&expr, &maps), Pushable::Yes);
    }

    #[test]
    fn binary_ordering_is_not_pushable() {
        let expr = Expr::Compare {
            left: Box::new(path("a")),
            op: CompareOp::Lt,
            right: Box::new(Expr::Placeholder(":b".into())),
        };
        let maps = maps_with(&[(":b", AttributeValue::B(vec![0]))]);
        assert!(!is_pushable(&expr, &maps).is_yes());
    }

    #[test]
    fn not_attribute_exists_is_pushable() {
        let expr = Expr::Not(Box::new(Expr::Function {
            name: "attribute_exists".into(),
            args: vec![path("a")],
        }));
        assert_eq!(is_pushable(&expr, &maps_with(&[])), Pushable::Yes);
    }

    #[test]
    fn not_around_comparison_is_not_pushable() {
        let expr = Expr::Not(Box::new(Expr::Compare {
            left: Box::new(path("a")),
            op: CompareOp::Eq,
            right: Box::new(Expr::Placeholder(":s".into())),
        }));
        let maps = maps_with(&[(":s", AttributeValue::S("x".into()))]);
        assert!(!is_pushable(&expr, &maps).is_yes());
    }

    #[test]
    fn and_of_two_pushable_is_pushable() {
        let expr = Expr::And(
            Box::new(Expr::Function {
                name: "attribute_exists".into(),
                args: vec![path("a")],
            }),
            Box::new(Expr::Function {
                name: "attribute_exists".into(),
                args: vec![path("b")],
            }),
        );
        assert_eq!(is_pushable(&expr, &maps_with(&[])), Pushable::Yes);
    }

    #[test]
    fn and_taints_on_either_side() {
        let expr = Expr::And(
            Box::new(Expr::Function {
                name: "attribute_exists".into(),
                args: vec![path("a")],
            }),
            Box::new(Expr::Function {
                name: "size".into(),
                args: vec![path("b")],
            }),
        );
        assert!(!is_pushable(&expr, &maps_with(&[])).is_yes());
    }

    #[test]
    fn between_is_not_pushable() {
        let expr = Expr::Between {
            operand: Box::new(path("a")),
            low: Box::new(Expr::Placeholder(":lo".into())),
            high: Box::new(Expr::Placeholder(":hi".into())),
        };
        let maps = maps_with(&[
            (":lo", AttributeValue::S("a".into())),
            (":hi", AttributeValue::S("z".into())),
        ]);
        // BETWEEN is currently non-pushable pending analyzer coverage;
        // this test locks in that decision.
        assert!(!is_pushable(&expr, &maps).is_yes());
    }
}
