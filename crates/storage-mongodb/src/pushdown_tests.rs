// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

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

fn aliased_maps(resolved_name: &str, value: AttributeValue) -> ExpressionMaps {
    let mut names = HashMap::new();
    names.insert("name".to_owned(), resolved_name.to_owned());
    let mut values = HashMap::new();
    values.insert(":value".to_owned(), value);
    ExpressionMaps::new(names, values)
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
fn dotted_attribute_alias_is_not_pushable() {
    let expr = Expr::Compare {
        left: Box::new(path("#name")),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder(":value".into())),
    };
    let maps = aliased_maps("a.b", AttributeValue::S("value".into()));

    assert_eq!(
        is_pushable(&expr, &maps),
        Pushable::No("literal attribute name contains '.'")
    );
}

#[test]
fn dotted_attribute_alias_in_function_is_not_pushable() {
    let expr = Expr::Function {
        name: "attribute_exists".into(),
        args: vec![path("#name")],
    };
    let maps = aliased_maps("a.b", AttributeValue::S("unused".into()));

    assert!(!is_pushable(&expr, &maps).is_yes());
}

#[test]
fn dollar_prefixed_attribute_in_condition_is_not_pushable() {
    let expr = Expr::Compare {
        left: Box::new(path("$foo")),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder(":value".into())),
    };
    let maps = maps_with(&[(":value", AttributeValue::S("value".into()))]);

    assert_eq!(
        is_pushable(&expr, &maps),
        Pushable::No("literal attribute name begins with '$'")
    );
}

#[test]
fn nested_document_path_without_literal_dot_remains_pushable() {
    let expr = Expr::Compare {
        left: Box::new(Expr::Path(vec![
            PathElement::Attribute("a".into()),
            PathElement::Attribute("b".into()),
        ])),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder(":value".into())),
    };
    let maps = maps_with(&[(":value", AttributeValue::S("value".into()))]);

    assert_eq!(is_pushable(&expr, &maps), Pushable::Yes);
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
fn field_vs_field_is_not_pushable() {
    // Both operands are plain fields whose runtime types are unknown at
    // compile time. Pushing $expr would compare tagged subdocuments
    // (lexical for Numbers), so this must fall back to the in-Rust
    // evaluator — for every comparator, not just ordering ones.
    for op in [
        CompareOp::Eq,
        CompareOp::Ne,
        CompareOp::Lt,
        CompareOp::Le,
        CompareOp::Gt,
        CompareOp::Ge,
    ] {
        let expr = Expr::Compare {
            left: Box::new(path("counter_a")),
            op,
            right: Box::new(path("counter_b")),
        };
        assert!(
            !is_pushable(&expr, &maps_with(&[])).is_yes(),
            "Field vs Field must not be pushable for {op:?}"
        );
    }
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

// ── D-M6 exclusion tests ────────────────────────────────────────
//
// The condition compiler in `condition.rs` has latent correctness
// bugs on numeric operands, set/list/map equality, mixed-type IN
// lists, and unvalidated attribute_type tags. The A5 analyzer is
// supposed to keep every one of those input shapes out of the
// pushdown path. These tests lock in that boundary so a future
// widening of the compiler doesn't accidentally admit a buggy
// shape without the analyzer being updated.

#[test]
fn numeric_compare_is_not_pushable() {
    // Numeric operands would compile to BSON string comparisons —
    // the analyzer must reject.
    let maps = maps_with(&[(":n", AttributeValue::N("42".into()))]);
    let expr = Expr::Compare {
        left: Box::new(path("a")),
        op: CompareOp::Lt,
        right: Box::new(Expr::Placeholder(":n".into())),
    };
    assert!(!is_pushable(&expr, &maps).is_yes());
}

#[test]
fn in_is_not_pushable() {
    // IN with mixed-type list uses the first literal's type for
    // all entries — analyzer must reject entirely.
    let maps = maps_with(&[
        (":a", AttributeValue::S("x".into())),
        (":b", AttributeValue::N("1".into())),
    ]);
    let expr = Expr::In {
        operand: Box::new(path("a")),
        list: vec![
            Expr::Placeholder(":a".into()),
            Expr::Placeholder(":b".into()),
        ],
    };
    assert!(!is_pushable(&expr, &maps).is_yes());
}

#[test]
fn set_equality_is_not_pushable() {
    // Eq/Ne on SS/NS/BS compiles to Bson::Null in the compiler.
    // Analyzer classifies as `un-inferrable operand kind` and
    // must reject.
    let ss: std::collections::BTreeSet<String> =
        ["a".to_owned(), "b".to_owned()].into_iter().collect();
    let maps = maps_with(&[(":s", AttributeValue::SS(ss))]);
    let expr = Expr::Compare {
        left: Box::new(path("tags")),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder(":s".into())),
    };
    assert!(!is_pushable(&expr, &maps).is_yes());
}

#[test]
fn attribute_type_valid_tag_is_pushable() {
    // "S" is a valid DDB type tag — analyzer admits.
    let expr = Expr::Function {
        name: "attribute_type".to_owned(),
        args: vec![path("a"), Expr::Placeholder(":t".into())],
    };
    let maps = maps_with(&[(":t", AttributeValue::S("S".into()))]);
    assert!(is_pushable(&expr, &maps).is_yes());
}

#[test]
fn attribute_type_invalid_tag_is_not_pushable() {
    // An arbitrary string as the tag is refused so the compiler
    // never assembles `field.$evil` mongo path fragments.
    let expr = Expr::Function {
        name: "attribute_type".to_owned(),
        args: vec![path("a"), Expr::Placeholder(":t".into())],
    };
    let maps = maps_with(&[(":t", AttributeValue::S("$ne".into()))]);
    assert!(!is_pushable(&expr, &maps).is_yes());
}
