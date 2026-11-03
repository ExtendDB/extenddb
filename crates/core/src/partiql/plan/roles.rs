// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What each condition of a WHERE clause is to the key reading.

use super::{KeyNames, SortCondition};
use crate::partiql::ast::{BooleanComparison, CompareOp, Operand, Path, WhereExpr};

/// What one condition of a disjunct is to the key reading.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Role<'a> {
    /// An equality of the partition key with a value.
    Partition(&'a Operand),
    /// The comparison with a boolean that key conditions of the disjunct are
    /// under (see `split`). It adds no condition of its own.
    Compared(BooleanComparison),
    /// A key condition on the sort key.
    Sort(SortCondition<'a>),
    /// Any other predicate on a key attribute.
    OnKey,
    /// A key attribute compared with something that is not a value.
    KeyAndPath(&'a str),
    /// A path into a key attribute.
    IntoKey(&'a str),
    /// A condition on other attributes.
    Filter,
}

impl Role<'_> {
    /// Whether an OR with this condition on either side is split.
    pub(super) fn splits(self) -> bool {
        !matches!(self, Role::IntoKey(_) | Role::Filter)
    }
}

/// The role of one condition (not an AND).
pub(super) fn role<'a>(expr: &'a WhereExpr, keys: KeyNames<'_>) -> Role<'a> {
    match expr {
        WhereExpr::Compare(l, op, r) => {
            if let Some(key) = bare_key(l, keys)
                && is_value(r)
            {
                return key_comparison(key, *op, r, keys);
            }
            if let Some(key) = bare_key(r, keys)
                && is_value(l)
            {
                return key_comparison(key, mirror(*op), l, keys);
            }
            comparison_role(&[l, r], keys)
        }
        WhereExpr::Between(v, low, high) => match bare_key(v, keys) {
            Some(key) if is_value(low) && is_value(high) => {
                if Some(key) == keys.sort {
                    Role::Sort(SortCondition::Between(low, high))
                } else {
                    Role::OnKey
                }
            }
            _ => comparison_role(&[v, low, high], keys),
        },
        WhereExpr::BeginsWith(v, prefix) => match bare_key(v, keys) {
            Some(key) if is_value(prefix) => {
                if Some(key) == keys.sort {
                    Role::Sort(SortCondition::BeginsWith(prefix))
                } else {
                    Role::OnKey
                }
            }
            _ => comparison_role(&[v, prefix], keys),
        },
        // An IN on a key attribute with values only is split into one
        // equality per value before a role is asked for. With a path in the
        // list it is one more predicate on the key (measured 2026-11-02:
        // `pk = 'a' AND sk IN [a]` runs).
        WhereExpr::In(v, items) => {
            if bare_key(v, keys).is_some() {
                return Role::OnKey;
            }
            let operands: Vec<&Operand> = std::iter::once(v).chain(items).collect();
            comparison_role(&operands, keys)
        }
        // A NOT over an AND or an OR is not read for key conditions: it stays
        // in the disjunct's filter (measured 2026-11-02: `pk = 'a' AND NOT
        // (sk = 'x' AND a = 1) OR pk = 'a' AND sk = 'y'` is refused as
        // overlapping, so its first disjunct reads partition 'a' by key). A
        // path into a key attribute under it gets the refusal a key compared
        // with a path gets (measured 2026-11-02: `pk = 'a' AND NOT (sk.q = 1
        // AND n = 1)` names sk in the filter refusal). See [`named_key`] for
        // the key it names.
        WhereExpr::Not(_) => negation_role(expr, keys),
        // A comparison with a boolean that keeps the condition is the
        // condition, and one that negates it is read as a NOT. A key
        // condition compared with a boolean is read by `split` before a role
        // is asked for.
        WhereExpr::ComparedWithBoolean(inner, comparison) => {
            if comparison.negates() {
                negation_role(expr, keys)
            } else {
                role(inner, keys)
            }
        }
        other => other_role(other, keys),
    }
}

/// Whether a condition is a key condition, or an AND of key conditions: an
/// equality of the partition key with a value, or a key condition on the
/// sort key.
pub(super) fn is_key_condition(expr: &WhereExpr, keys: KeyNames<'_>) -> bool {
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            WhereExpr::And(a, b) => {
                pending.push(a);
                pending.push(b);
            }
            other => {
                if !matches!(role(other, keys), Role::Partition(_) | Role::Sort(_)) {
                    return false;
                }
            }
        }
    }
    true
}

/// The role of a negated condition: a NOT over an AND or an OR stays in
/// the filter (see [`role`]), and any other is read as what it holds.
fn negation_role<'a>(expr: &'a WhereExpr, keys: KeyNames<'_>) -> Role<'a> {
    if matches!(
        expr.without_nots().1,
        WhereExpr::And(..) | WhereExpr::Or(..)
    ) {
        match named_key(expr, keys) {
            Some(key) => Role::KeyAndPath(key),
            None => Role::Filter,
        }
    } else {
        other_role(expr, keys)
    }
}

/// The role of a condition that is no key condition: a predicate on a key
/// attribute, a path into one, or a filter.
fn other_role<'a>(expr: &'a WhereExpr, keys: KeyNames<'_>) -> Role<'a> {
    if mentions_key(expr, keys) {
        Role::OnKey
    } else if let Some(key) = path_into_key(expr, keys) {
        Role::IntoKey(key)
    } else {
        Role::Filter
    }
}

/// A comparison of `key` with a value, the key on the left.
pub(super) fn key_comparison<'a>(
    key: &str,
    op: CompareOp,
    value: &'a Operand,
    keys: KeyNames<'_>,
) -> Role<'a> {
    if key == keys.partition {
        if op == CompareOp::Eq {
            Role::Partition(value)
        } else {
            Role::OnKey
        }
    } else if op == CompareOp::Ne {
        Role::OnKey
    } else {
        Role::Sort(SortCondition::Compare(op, value))
    }
}

/// The role of a comparison that is not a key compared with values.
fn comparison_role<'a>(operands: &[&'a Operand], keys: KeyNames<'_>) -> Role<'a> {
    if let Some(key) = operands.iter().find_map(|op| bare_key(op, keys)) {
        return Role::KeyAndPath(key);
    }
    if operands.iter().any(|op| operand_key(op, keys).is_some()) {
        return Role::OnKey;
    }
    match operands
        .iter()
        .find_map(|op| operand_path_into_key(op, keys))
    {
        Some(key) => Role::IntoKey(key),
        None => Role::Filter,
    }
}

/// The comparison with its two sides swapped: `1 < sk` is `sk > 1`.
fn mirror(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Ne => CompareOp::Ne,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
    }
}

/// The key attribute an operand names as a bare path.
pub(super) fn bare_key<'a>(op: &'a Operand, keys: KeyNames<'_>) -> Option<&'a str> {
    simple_path(op).filter(|name| keys.is_key(name))
}

/// The attribute name when the operand is a bare attribute path, else `None`.
fn simple_path(op: &Operand) -> Option<&str> {
    match op {
        Operand::Path(Path { root, steps }) if steps.is_empty() => Some(root.as_str()),
        _ => None,
    }
}

pub(super) fn is_value(op: &Operand) -> bool {
    matches!(op, Operand::Literal(_) | Operand::Parameter(_))
}

/// The key attribute of the first path into one in the operand.
fn operand_path_into_key<'a>(op: &'a Operand, keys: KeyNames<'_>) -> Option<&'a str> {
    match op {
        Operand::Path(path) if !path.steps.is_empty() && keys.is_key(&path.root) => {
            Some(path.root.as_str())
        }
        Operand::Function(_, args) => args.iter().find_map(|a| operand_path_into_key(a, keys)),
        Operand::Arith(l, _, r) => {
            operand_path_into_key(l, keys).or_else(|| operand_path_into_key(r, keys))
        }
        _ => None,
    }
}

/// What a condition tests: an operand, or the path of an `IS [NOT] MISSING`.
enum Leaf<'a> {
    Operand(&'a Operand),
    Missing(&'a Path),
}

/// The leaves of a condition, left to right. An explicit stack keeps a run
/// of NOTs and a long AND or OR chain off the call stack.
fn leaves(expr: &WhereExpr) -> Vec<Leaf<'_>> {
    let mut out = Vec::new();
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            WhereExpr::Compare(a, _, b)
            | WhereExpr::BeginsWith(a, b)
            | WhereExpr::Contains(a, b)
            | WhereExpr::AttributeType(a, b) => {
                out.push(Leaf::Operand(a));
                out.push(Leaf::Operand(b));
            }
            WhereExpr::Between(v, low, high) => {
                out.extend([v, low, high].map(Leaf::Operand));
            }
            WhereExpr::In(v, items) => {
                out.push(Leaf::Operand(v));
                out.extend(items.iter().map(Leaf::Operand));
            }
            WhereExpr::Function(_, args) => out.extend(args.iter().map(Leaf::Operand)),
            WhereExpr::IsMissing(path, _) => out.push(Leaf::Missing(path)),
            WhereExpr::IsType(operand, ..) => out.push(Leaf::Operand(operand)),
            WhereExpr::And(a, b) | WhereExpr::Or(a, b) => {
                pending.push(b);
                pending.push(a);
            }
            WhereExpr::Not(inner) | WhereExpr::ComparedWithBoolean(inner, _) => pending.push(inner),
        }
    }
    out
}

/// Whether a condition tests a key attribute itself anywhere.
fn mentions_key(expr: &WhereExpr, keys: KeyNames<'_>) -> bool {
    first_key(expr, keys).is_some()
}

/// The first key attribute a condition tests itself, left to right.
fn first_key<'a>(expr: &'a WhereExpr, keys: KeyNames<'_>) -> Option<&'a str> {
    leaves(expr).into_iter().find_map(|leaf| match leaf {
        Leaf::Operand(op) => operand_key(op, keys),
        Leaf::Missing(path) => path.as_simple().filter(|name| keys.is_key(name)),
    })
}

/// The first key attribute an operand is or holds as a function's argument.
fn operand_key<'a>(op: &'a Operand, keys: KeyNames<'_>) -> Option<&'a str> {
    match op {
        Operand::Path(_) => bare_key(op, keys),
        Operand::Function(_, args) => args.iter().find_map(|a| operand_key(a, keys)),
        Operand::Arith(l, _, r) => operand_key(l, keys).or_else(|| operand_key(r, keys)),
        Operand::Literal(_) | Operand::Parameter(_) => None,
    }
}

/// The key attribute the filter refusal of a NOT over an AND or an OR names:
/// the sort key when the condition names it, itself or as the root of a
/// path, else the partition key (measured 2026-11-02: `pk = 'a' AND NOT
/// (sk.q = 1 AND pk = b)` and `pk = 'a' AND NOT (pk = b AND sk.q = 1)` name
/// sk).
fn named_key<'a>(expr: &'a WhereExpr, keys: KeyNames<'_>) -> Option<&'a str> {
    let mut named = Vec::new();
    for leaf in leaves(expr) {
        match leaf {
            Leaf::Operand(op) => key_roots(op, keys, &mut named),
            Leaf::Missing(path) if keys.is_key(&path.root) => named.push(path.root.as_str()),
            Leaf::Missing(_) => {}
        }
    }
    named
        .iter()
        .find(|name| keys.sort == Some(**name))
        .or_else(|| named.first())
        .copied()
}

/// Every key attribute an operand names, itself or as the root of a path.
fn key_roots<'a>(op: &'a Operand, keys: KeyNames<'_>, out: &mut Vec<&'a str>) {
    match op {
        Operand::Path(path) if keys.is_key(&path.root) => out.push(path.root.as_str()),
        Operand::Function(_, args) => {
            for arg in args {
                key_roots(arg, keys, out);
            }
        }
        Operand::Arith(l, _, r) => {
            key_roots(l, keys, out);
            key_roots(r, keys, out);
        }
        Operand::Path(_) | Operand::Literal(_) | Operand::Parameter(_) => {}
    }
}

/// The key attribute of the first path into one in a condition.
fn path_into_key<'a>(expr: &'a WhereExpr, keys: KeyNames<'_>) -> Option<&'a str> {
    leaves(expr).into_iter().find_map(|leaf| match leaf {
        Leaf::Operand(op) => operand_path_into_key(op, keys),
        Leaf::Missing(path) => {
            (!path.steps.is_empty() && keys.is_key(&path.root)).then_some(path.root.as_str())
        }
    })
}
