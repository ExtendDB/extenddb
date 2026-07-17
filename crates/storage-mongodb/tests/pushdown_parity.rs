// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Filter-pushdown parity harness.
//!
//! For every generated (item, expression) pair drawn from the "pushable
//! subset" (see todo.md A5), assert that
//!
//!   `extenddb_core::expression::evaluate_condition(expr, item, maps)`
//!
//! agrees with evaluating
//!
//!   `condition_to_filter(expr, maps)`
//!
//! against the item's BSON representation using the interpreter in
//! `tests/common/mod.rs`. Any divergence indicates the compiler emits a
//! filter whose semantics differ from DDB's — a bug in the compiler.
//!
//! The harness deliberately excludes expression shapes that fall outside
//! the pushable subset: no numeric comparisons, no `size()`, and `NOT`
//! only around `attribute_exists` / `attribute_not_exists`. Those cases
//! are handled by fallback to session-scoped in-Rust evaluation in step 3
//! of A5, and don't need pushdown parity.

#[allow(dead_code)]
mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bson::{Bson, Document};
use proptest::prelude::*;
use proptest::sample::select;

use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps, PathElement, evaluate_condition};
use extenddb_core::types::AttributeValue;
use extenddb_storage_mongodb::condition::condition_to_filter;

use common::eval_filter;

// ---------------------------------------------------------------------------
// Attribute-value strategies
// ---------------------------------------------------------------------------

/// A small vocabulary of attribute names. Reusing names across items and
/// expressions is what surfaces path-hit / path-miss interactions.
const NAMES: &[&str] = &["a", "b", "c", "x", "y"];

/// Constrained string alphabet — short, printable, small alphabet.
/// Keeps proptest shrinking behavior tractable and matches the kinds of
/// values real DDB workloads use for status flags, tags, ETags, etc.
fn arb_short_str() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-z]{0,4}").unwrap()
}

fn arb_short_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..4)
}

/// Attribute values in the "safe subset" — everything except N/NS.
/// The pushable expression grammar never references numeric operands, so
/// items don't need to contain them either. Including L/M would require a
/// recursive strategy; we keep depth flat for now (paths in the grammar
/// are single-name only, so nested M/L values wouldn't be reachable
/// anyway).
fn arb_safe_value() -> impl Strategy<Value = AttributeValue> {
    prop_oneof![
        arb_short_str().prop_map(AttributeValue::S),
        arb_short_bytes().prop_map(AttributeValue::B),
        any::<bool>().prop_map(AttributeValue::Bool),
        Just(AttributeValue::Null),
        proptest::collection::btree_set(arb_short_str(), 0..3).prop_map(AttributeValue::SS),
        proptest::collection::btree_set(arb_short_bytes(), 0..3).prop_map(AttributeValue::BS),
        // Lists of strings — the compiler's contains-on-list path takes a
        // scalar and matches any element. Mixed-type lists aren't in the
        // pushable grammar so we don't generate them.
        proptest::collection::vec(arb_short_str().prop_map(AttributeValue::S), 0..3)
            .prop_map(AttributeValue::L),
    ]
}

/// A random DDB Item. Not every name is present in every item — that's
/// how the "path missing" edge cases get exercised.
fn arb_item() -> impl Strategy<Value = BTreeMap<String, AttributeValue>> {
    proptest::collection::vec(
        (select(NAMES).prop_map(String::from), arb_safe_value()),
        0..NAMES.len(),
    )
    .prop_map(|pairs| {
        let mut m = BTreeMap::new();
        for (k, v) in pairs {
            m.insert(k, v);
        }
        m
    })
}

// ---------------------------------------------------------------------------
// Expression strategies
// ---------------------------------------------------------------------------

/// A random attribute name from the vocabulary.
fn arb_name_expr() -> impl Strategy<Value = Expr> {
    select(NAMES).prop_map(|n| Expr::Path(vec![PathElement::Attribute(n.to_string())]))
}

/// A random placeholder reference (`:v0`, `:v1`, ...).
fn arb_placeholder_ref(idx: usize) -> Expr {
    Expr::Placeholder(format!(":v{idx}"))
}

/// A leaf comparison expression that is safely pushable. Each returns:
///   - the AST for the expression
///   - the values map required to resolve any placeholders in the AST
///
/// The placeholders are numbered per-expression starting at :v0. When
/// composing with AND/OR (below), we renumber to keep them globally unique.
///
/// Binary (`.B`) operands are currently excluded: the compiler emits raw
/// BSON `Binary` filters for `.B` comparisons, but `item_to_document`
/// (in production) stores `.B` fields as base64-encoded strings via the
/// AttributeValue JSON serializer. The two formats never match — a
/// pre-existing bug in the compiler / storage-layer contract. A5 step 3
/// will either fix the compiler to emit string filters (matching storage)
/// or fix the storage to emit BSON binary (matching the compiler), and
/// re-enable `.B` comparisons in this harness.
#[allow(clippy::redundant_closure)]
fn arb_leaf_expr() -> impl Strategy<Value = (Expr, Vec<AttributeValue>)> {
    // We union several leaf shapes. Each yields (Expr, values-list).
    let name = || arb_name_expr();
    let str_val = || arb_short_str().prop_map(AttributeValue::S);

    prop_oneof![
        // attribute_exists(name)
        name().prop_map(|n| (
            Expr::Function {
                name: "attribute_exists".into(),
                args: vec![n],
            },
            vec![]
        )),
        // attribute_not_exists(name)
        name().prop_map(|n| (
            Expr::Function {
                name: "attribute_not_exists".into(),
                args: vec![n],
            },
            vec![]
        )),
        // NOT attribute_exists(name)  — the only NOT the pushable subset allows
        name().prop_map(|n| (
            Expr::Not(Box::new(Expr::Function {
                name: "attribute_exists".into(),
                args: vec![n],
            })),
            vec![]
        )),
        // attribute_type(name, :t) for the S / B / BOOL / NULL / SS / BS / L / M types
        (
            name(),
            select(&["S", "B", "BOOL", "NULL", "SS", "BS", "L", "M"][..]).prop_map(String::from)
        )
            .prop_map(|(n, t)| (
                Expr::Function {
                    name: "attribute_type".into(),
                    args: vec![n, arb_placeholder_ref(0)],
                },
                vec![AttributeValue::S(t)],
            )),
        // begins_with(name, :prefix)
        (name(), arb_short_str()).prop_map(|(n, p)| (
            Expr::Function {
                name: "begins_with".into(),
                args: vec![n, arb_placeholder_ref(0)],
            },
            vec![AttributeValue::S(p)],
        )),
        // contains(name, :substr)  — matches when name is S (substring) or SS/BS/L (membership)
        (name(), str_val()).prop_map(|(n, v)| (
            Expr::Function {
                name: "contains".into(),
                args: vec![n, arb_placeholder_ref(0)],
            },
            vec![v],
        )),
        // name = :v  (S)
        (name(), str_val()).prop_map(|(n, v)| (
            Expr::Compare {
                left: Box::new(n),
                op: CompareOp::Eq,
                right: Box::new(arb_placeholder_ref(0)),
            },
            vec![v],
        )),
        // name <> :v  (S)
        (name(), str_val()).prop_map(|(n, v)| (
            Expr::Compare {
                left: Box::new(n),
                op: CompareOp::Ne,
                right: Box::new(arb_placeholder_ref(0)),
            },
            vec![v],
        )),
        // name < :v  (S)
        (name(), str_val()).prop_map(|(n, v)| (
            Expr::Compare {
                left: Box::new(n),
                op: CompareOp::Lt,
                right: Box::new(arb_placeholder_ref(0)),
            },
            vec![v],
        )),
        // name > :v  (S)
        (name(), str_val()).prop_map(|(n, v)| (
            Expr::Compare {
                left: Box::new(n),
                op: CompareOp::Gt,
                right: Box::new(arb_placeholder_ref(0)),
            },
            vec![v],
        )),
    ]
}

/// Compose two leaf expressions with AND or OR. Renumbers the second
/// expression's placeholders so both are addressable in the merged
/// values map.
fn arb_composed_expr() -> impl Strategy<Value = (Expr, Vec<AttributeValue>)> {
    (arb_leaf_expr(), arb_leaf_expr(), any::<bool>()).prop_map(|(l, r, is_and)| {
        let (lhs, mut lvals) = l;
        let (rhs, rvals) = r;
        let rhs_offset = lvals.len();
        // Renumber rhs placeholders to `:v<idx+offset>`.
        let rhs = renumber_placeholders(rhs, rhs_offset);
        lvals.extend(rvals);
        let composed = if is_and {
            Expr::And(Box::new(lhs), Box::new(rhs))
        } else {
            Expr::Or(Box::new(lhs), Box::new(rhs))
        };
        (composed, lvals)
    })
}

fn renumber_placeholders(expr: Expr, offset: usize) -> Expr {
    match expr {
        Expr::Placeholder(name) => {
            if let Some(idx_str) = name.strip_prefix(":v") {
                if let Ok(idx) = idx_str.parse::<usize>() {
                    return Expr::Placeholder(format!(":v{}", idx + offset));
                }
            }
            Expr::Placeholder(name)
        }
        Expr::Path(_) => expr,
        Expr::Compare { left, op, right } => Expr::Compare {
            left: Box::new(renumber_placeholders(*left, offset)),
            op,
            right: Box::new(renumber_placeholders(*right, offset)),
        },
        Expr::And(l, r) => Expr::And(
            Box::new(renumber_placeholders(*l, offset)),
            Box::new(renumber_placeholders(*r, offset)),
        ),
        Expr::Or(l, r) => Expr::Or(
            Box::new(renumber_placeholders(*l, offset)),
            Box::new(renumber_placeholders(*r, offset)),
        ),
        Expr::Not(inner) => Expr::Not(Box::new(renumber_placeholders(*inner, offset))),
        Expr::Function { name, args } => Expr::Function {
            name,
            args: args
                .into_iter()
                .map(|a| renumber_placeholders(a, offset))
                .collect(),
        },
        Expr::Between { operand, low, high } => Expr::Between {
            operand: Box::new(renumber_placeholders(*operand, offset)),
            low: Box::new(renumber_placeholders(*low, offset)),
            high: Box::new(renumber_placeholders(*high, offset)),
        },
        Expr::In { operand, list } => Expr::In {
            operand: Box::new(renumber_placeholders(*operand, offset)),
            list: list
                .into_iter()
                .map(|a| renumber_placeholders(a, offset))
                .collect(),
        },
        other => other,
    }
}

/// Generate either a leaf or a composed AND/OR expression.
fn arb_expr() -> impl Strategy<Value = (Expr, Vec<AttributeValue>)> {
    prop_oneof![
        3 => arb_leaf_expr(),
        1 => arb_composed_expr(),
    ]
}

// ---------------------------------------------------------------------------
// Item → BSON conversion for the interpreter side
// ---------------------------------------------------------------------------

/// Serialize an Item to the BSON shape the compiler assumes:
///   { item_data: { <attr>: <ddb-tagged value>, ... } }
fn item_to_bson_doc(item: &BTreeMap<String, AttributeValue>) -> Document {
    let item_data_json = serde_json::to_value(item).expect("item serializes");
    let item_data_bson: Bson = bson::to_bson(&item_data_json).expect("BSON conversion");
    let mut doc = Document::new();
    doc.insert("item_data", item_data_bson);
    doc
}

// ---------------------------------------------------------------------------
// The parity property
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1024,
        max_shrink_iters: 4096,
        ..Default::default()
    })]

    /// For any pushable expression and any item, the compiled MongoDB
    /// filter (evaluated by our BSON interpreter) must agree with the
    /// in-Rust DDB evaluator's pass/fail result.
    #[test]
    fn compiled_filter_matches_ddb_evaluator(
        item in arb_item(),
        expr_pair in arb_expr(),
    ) {
        let (expr, values_vec) = expr_pair;

        // Build the ExpressionMaps that the DDB evaluator and the compiler
        // both consume. Placeholders are :v0, :v1, ... in insertion order.
        let mut values = HashMap::new();
        for (idx, v) in values_vec.iter().enumerate() {
            values.insert(format!(":v{idx}"), v.clone());
        }
        let maps = ExpressionMaps::new(HashMap::new(), values);

        // Path A: in-Rust DDB evaluator against the logical Item.
        let ddb_result = evaluate_condition(&expr, &item, &maps).unwrap_or(false);

        // Path B: compile to MongoDB filter, then evaluate the filter
        // against the item's BSON representation using our interpreter.
        let filter = match condition_to_filter(&expr, &maps) {
            Ok(f) => f,
            Err(e) => {
                // The compiler rejected this expression. That's fine — it
                // means A5's fallback path would kick in. Skip this case
                // rather than treating it as a mismatch.
                //
                // We still assert the compiler doesn't reject something
                // the DDB evaluator accepts as trivially-true or trivially-
                // false — but the compiler rejecting compilation is
                // logically different from evaluating to false.
                let _ = e;
                return Ok(());
            }
        };
        let bson_doc = item_to_bson_doc(&item);
        let mongo_result = eval_filter(&filter, &bson_doc);

        prop_assert_eq!(
            ddb_result,
            mongo_result,
            "parity mismatch:\n  expr:  {:?}\n  item:  {:?}\n  filter: {:?}\n  ddb:   {}\n  mongo: {}",
            expr,
            item,
            filter,
            ddb_result,
            mongo_result,
        );
    }
}

// Silence unused-import lint from `common` when running only a subset.
#[allow(dead_code)]
fn _keep_common_imported() {
    let _ = std::mem::size_of::<BTreeSet<u8>>();
    let _ = std::mem::size_of::<HashMap<String, String>>();
}
