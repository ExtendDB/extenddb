// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the reading of a WHERE clause.

use super::*;
use crate::partiql::ast::Statement;
use crate::partiql::parse;

fn where_of(sql: &str) -> WhereExpr {
    match parse(sql).unwrap() {
        Statement::Select(s) => s.where_clause.unwrap(),
        _ => panic!("not a select"),
    }
}

const KEYS: KeyNames<'static> = KeyNames {
    partition: "pk",
    sort: Some("sk"),
};

const HASH_ONLY: KeyNames<'static> = KeyNames {
    partition: "pk",
    sort: None,
};

fn literal(op: &Operand) -> Option<AttributeValue> {
    match op {
        Operand::Literal(v) => Some(v.clone()),
        _ => None,
    }
}

fn select(condition: &str) -> String {
    format!("SELECT * FROM \"t\" WHERE {condition}")
}

/// The reads of a WHERE as (partition, sort condition kind, filter
/// length) and whether every disjunct reads by key.
fn reading(condition: &str, keys: KeyNames<'_>) -> (Vec<String>, bool) {
    let w = where_of(&select(condition));
    let analysis = WhereAnalysis::new(&w, keys, &literal);
    let reads = analysis
        .reads()
        .iter()
        .map(|read| {
            let value = |op: &Operand| match literal(op) {
                Some(AttributeValue::S(s)) => s,
                Some(AttributeValue::N(n)) => n,
                other => format!("{other:?}"),
            };
            let sort = match &read.sort {
                None => String::new(),
                Some(SortCondition::Compare(op, v)) => format!(" sk {op:?} {}", value(v)),
                Some(SortCondition::Between(l, h)) => {
                    format!(" sk between {} {}", value(l), value(h))
                }
                Some(SortCondition::BeginsWith(p)) => format!(" sk begins {}", value(p)),
            };
            format!("{}{sort} +{}", value(read.partition), read.filter.len())
        })
        .collect();
    (reads, analysis.is_keyed())
}

#[test]
fn a_disjunct_reads_by_key_with_one_partition_equality() {
    assert_eq!(reading("pk = 'a'", KEYS), (vec!["a +0".to_owned()], true));
    assert_eq!(
        reading("pk = 'a' AND pk = 'a' AND x = 1", KEYS),
        (vec!["a +1".to_owned()], true)
    );
    assert_eq!(
        reading("'a' = pk", HASH_ONLY),
        (vec!["a +0".to_owned()], true)
    );
    for condition in [
        "pk = 'a' AND pk = 'b'",
        "pk = 'a' AND pk = 1",
        "pk = 1 AND pk > 'a'",
        "pk = 1 AND NOT pk = 'a'",
        "pk = 'a' AND pk <> 1",
        "pk = 'a' AND size(pk) = 1",
        "pk = 'a' AND attribute_exists(pk)",
        "pk > 'a'",
        "begins_with(pk, 'a')",
        "pk BETWEEN 'a' AND 'b'",
        "sk = 1",
        "x = 1",
        "NOT (pk = 1 AND sk = 1)",
        "pk = a",
    ] {
        assert_eq!(reading(condition, KEYS), (vec![], false), "{condition}");
    }
}

#[test]
fn the_sort_key_holds_one_key_condition_or_nothing() {
    for (condition, read) in [
        ("pk = 'a' AND sk = 'x'", "a sk Eq x +0"),
        ("pk = 'a' AND sk > 'x'", "a sk Gt x +0"),
        ("pk = 'a' AND 'x' < sk", "a sk Gt x +0"),
        ("pk = 'a' AND 'x' >= sk", "a sk Le x +0"),
        ("pk = 'a' AND sk BETWEEN 'x' AND 'y'", "a sk between x y +0"),
        ("pk = 'a' AND begins_with(sk, 'x')", "a sk begins x +0"),
        ("pk = 'a' AND sk = 'x' AND 'x' = sk", "a sk Eq x +0"),
        ("1 = sk AND 'a' = pk", "a sk Eq 1 +0"),
        // A key compared with a path, or a path into a key, is a filter.
        ("pk = 'a' AND sk = 1 AND sk = a", "a sk Eq 1 +1"),
        ("pk = 'a' AND sk.x = 1", "a +1"),
    ] {
        assert_eq!(
            reading(condition, KEYS),
            (vec![read.to_owned()], true),
            "{condition}"
        );
    }
    for condition in [
        "pk = 'a' AND sk = 'x' AND sk = 1",
        "pk = 'a' AND sk < 1 AND sk > 0",
        "pk = 'a' AND sk > 'a' AND sk = 1",
        "pk = 'a' AND begins_with(sk, 'x') AND sk = 1",
        "pk = 'a' AND begins_with(sk, 'x') AND begins_with(sk, 'y')",
        "pk = 'a' AND sk = 1 AND NOT sk = 'y'",
        "pk = 'a' AND sk = 1 AND contains(sk, 'x')",
        "pk = 'a' AND sk = 1 AND attribute_exists(sk)",
        "pk = 'a' AND sk = 1 AND sk <> 'y'",
        "pk = 'a' AND sk = 1 AND sk IS NOT MISSING",
        "pk = 'a' AND size(sk) = 1",
        "pk = 'a' AND NOT sk IN ['1', '3']",
        "pk = 'a' AND NOT sk BETWEEN 'a' AND 'b'",
        // An IN with a path in its list is one more predicate on the key.
        "pk = 'a' AND sk IN [a]",
        "pk = 'a' AND sk IN ['x', a]",
        "pk = 'a' AND pk IN [a]",
    ] {
        assert_eq!(reading(condition, KEYS), (vec![], false), "{condition}");
    }
}

#[test]
fn a_key_condition_compared_with_a_boolean_is_read_by_key() {
    // The read, and whether it returns nothing: the literal alone decides.
    let reads = |condition: &str, keys: KeyNames<'_>| {
        let w = where_of(&select(condition));
        let analysis = WhereAnalysis::new(&w, keys, &literal);
        let nothing: Vec<bool> = analysis
            .reads()
            .iter()
            .map(KeyRead::returns_nothing)
            .collect();
        (reading(condition, keys), nothing)
    };
    let one = |read: &str| (vec![read.to_owned()], true);
    for (condition, keys, read, nothing) in [
        ("pk = 'a' = false", HASH_ONLY, one("a +0"), vec![true]),
        ("pk = 'a' <> false", HASH_ONLY, one("a +0"), vec![true]),
        ("pk = 'a' <> true", HASH_ONLY, one("a +0"), vec![false]),
        ("pk = 'a' = true", HASH_ONLY, one("a +0"), vec![false]),
        ("'a' = pk = false", KEYS, one("a +0"), vec![true]),
        ("(pk = 'a') = false", KEYS, one("a +0"), vec![true]),
        (
            "pk = 'a' AND sk = 'x' = false",
            KEYS,
            one("a sk Eq x +0"),
            vec![true],
        ),
        (
            "pk = 'a' AND sk > 'x' <> true",
            KEYS,
            one("a sk Gt x +0"),
            vec![false],
        ),
        (
            "(pk = 'a' AND sk = 'x') <> true",
            KEYS,
            one("a sk Eq x +0"),
            vec![false],
        ),
        ("pk = 'a' <> true AND n = 1", KEYS, one("a +1"), vec![false]),
        ("pk = 'a' = false AND n = 1", KEYS, one("a +1"), vec![true]),
        (
            "pk = 'a' = false AND sk = 'x'",
            KEYS,
            one("a sk Eq x +0"),
            vec![true],
        ),
        (
            "pk = 'a' AND pk = 'a' = false",
            KEYS,
            one("a +0"),
            vec![true],
        ),
        (
            "pk = 'a' = false OR pk = 'b'",
            KEYS,
            (vec!["a +0".to_owned(), "b +0".to_owned()], true),
            vec![true, false],
        ),
        (
            "pk = 'a' <> true OR pk = 'b'",
            KEYS,
            (vec!["a +0".to_owned(), "b +0".to_owned()], true),
            vec![false, false],
        ),
    ] {
        assert_eq!(reads(condition, keys), (read, nothing), "{condition}");
    }
    // A condition that is no key condition keeps its meaning: an IN, a NOT,
    // two comparisons with false, a filter, and another partition value.
    for condition in [
        "pk IN ['a'] <> true",
        "pk IN ['a'] = false",
        "NOT pk = 'a'",
        "pk = 'a' = false = false",
        "pk = 'b' AND pk = 'a' = false",
    ] {
        assert_eq!(reading(condition, KEYS), (vec![], false), "{condition}");
    }
    assert_eq!(
        reading("pk = 'a' AND n = 1 <> true", KEYS),
        (vec!["a +1".to_owned()], true)
    );
}

#[test]
fn a_key_condition_compared_with_a_boolean_is_written_by_its_literal() {
    let keyed = |condition: &str| keyed_condition(&where_of(&select(condition)), KEYS);
    let pk_a = where_of(&select("pk = 'a'"));
    // Compared with true: the key condition itself.
    assert_eq!(keyed("pk = 'a' <> true"), pk_a);
    assert_eq!(keyed("pk = 'a' = true"), pk_a);
    // Compared with false: the key condition and its negation, which no item
    // passes.
    let never = WhereExpr::And(
        Box::new(pk_a.clone()),
        Box::new(WhereExpr::Not(Box::new(pk_a.clone()))),
    );
    assert_eq!(keyed("pk = 'a' <> false"), never);
    assert_eq!(
        keyed("pk = 'a' = false OR n = 1"),
        WhereExpr::Or(Box::new(never), Box::new(where_of(&select("n = 1"))))
    );
    // Anything else is left as it is.
    let w = where_of(&select("pk IN ['a'] <> true"));
    assert_eq!(keyed_condition(&w, KEYS), w);
}

#[test]
fn a_not_over_an_and_or_an_or_stays_in_the_filter() {
    for (condition, read) in [
        ("pk = 'a' AND NOT (sk = 'x' AND a = 1)", "a +1"),
        ("pk = 'a' AND NOT (sk = 'x' OR a = 1)", "a +1"),
        ("pk = 'a' AND NOT NOT (sk = 'x' AND a = 1)", "a +1"),
        (
            "pk = 'a' AND sk = 'y' AND NOT (pk = 'b' AND a = 1)",
            "a sk Eq y +1",
        ),
        ("pk = 'a' AND NOT (a = 1 AND b = 2)", "a +1"),
    ] {
        assert_eq!(
            reading(condition, KEYS),
            (vec![read.to_owned()], true),
            "{condition}"
        );
    }
    // A NOT over one condition on a key makes a scan.
    for condition in [
        "pk = 'a' AND NOT (sk = 'x')",
        "pk = 'a' AND NOT NOT sk = 'x'",
    ] {
        assert_eq!(reading(condition, KEYS), (vec![], false), "{condition}");
    }
}

#[test]
fn key_values_are_ordered_by_bytes_or_by_number() {
    let s = |v: &str| AttributeValue::S(v.to_owned());
    let n = |v: &str| AttributeValue::N(v.to_owned());
    let compare_key_values = |a: &AttributeValue, b: &AttributeValue| {
        Bound::from_value(a)?.compare(&Bound::from_value(b)?)
    };
    assert_eq!(compare_key_values(&s("a"), &s("zz")), Some(Ordering::Less));
    assert_eq!(
        compare_key_values(&s("b"), &s("a")),
        Some(Ordering::Greater)
    );
    assert_eq!(
        compare_key_values(&n("10"), &n("9")),
        Some(Ordering::Greater)
    );
    assert_eq!(
        compare_key_values(&n("1.0"), &n("1")),
        Some(Ordering::Equal)
    );
    assert_eq!(
        compare_key_values(
            &AttributeValue::B(vec![0xff]),
            &AttributeValue::B(vec![0x01, 0x02])
        ),
        Some(Ordering::Greater)
    );
    assert_eq!(compare_key_values(&s("1"), &n("1")), None);
}

#[test]
fn disjuncts_are_read_in_order_until_the_first_scan() {
    for (condition, reads, keyed) in [
        ("a = 1 OR pk = 1", vec![], false),
        ("pk = 1 OR a = 1", vec!["1 +0"], false),
        ("pk = 'a' OR a = 1 OR pk = 1", vec!["a +0"], false),
        ("pk = 'a' OR pk = 1 OR a = 1", vec!["a +0", "1 +0"], false),
        ("pk = 'a' AND (pk = 1 OR a = 1)", vec![], false),
        (
            "pk = 'a' AND (a = 1 OR sk = 1)",
            vec!["a +1", "a sk Eq 1 +0"],
            true,
        ),
        ("pk = 'a' AND (sk = 'x' OR a = 1) AND sk = 1", vec![], false),
        (
            "pk IN [1, 'b'] AND sk = 1",
            vec!["1 sk Eq 1 +0", "b sk Eq 1 +0"],
            true,
        ),
        (
            "pk = 'a' AND sk IN ['x', 'y']",
            vec!["a sk Eq x +0", "a sk Eq y +0"],
            true,
        ),
        (
            "(pk = 1 AND sk = 'x') OR (pk = 'b' AND sk = 1)",
            vec!["1 sk Eq x +0", "b sk Eq 1 +0"],
            true,
        ),
        ("pk = 'a' OR sk = 1", vec!["a +0"], false),
        (
            "pk = 'a' AND sk = 'x' OR sk = 1",
            vec!["a sk Eq x +0"],
            false,
        ),
        // An OR with no key attribute on either side is one condition.
        ("pk = 'a' AND (x = 1 OR y = 2)", vec!["a +1"], true),
    ] {
        let reads: Vec<String> = reads.into_iter().map(str::to_owned).collect();
        assert_eq!(reading(condition, KEYS), (reads, keyed), "{condition}");
    }
}

#[test]
fn the_shared_filter_is_the_conditions_beside_the_keys() {
    let shared = |condition: &str| {
        let w = where_of(&select(condition));
        WhereAnalysis::new(&w, KEYS, &literal)
            .shared_filter()
            .map(|f| f.len())
    };
    assert_eq!(shared("pk IN ['a', 'b'] AND x = 1 AND y = 2"), Some(2));
    assert_eq!(shared("(pk = 'a' AND x = 1) OR pk = 'b'"), Some(0));
    assert_eq!(shared("pk = 'a' AND (x = 1 OR y = 2)"), Some(1));
    assert_eq!(shared("pk = 'a' AND x = 1 OR y = 1"), None);
}

#[test]
fn a_key_against_a_path_or_into_a_key_is_named_in_the_filter() {
    let refusal = |condition: &str, keys: KeyNames<'_>| {
        let w = where_of(&select(condition));
        let analysis = WhereAnalysis::new(&w, keys, &literal);
        let filter = analysis.shared_filter().unwrap();
        scalar_key_refusal(&filter, keys).or_else(|| filter_key_refusal(&filter, keys))
    };
    let path = |key: &str| {
        Some(format!(
            "Filter Expression can only contain non-primary key attributes: Primary key \
             attribute: {key}"
        ))
    };
    let scalar = |key: &str| {
        Some(format!(
            "Key attributes must be scalars; list random access '[]' and map lookup '.' \
             are not allowed: Key {key}"
        ))
    };
    for (condition, want) in [
        ("pk = 'a' AND sk = a", path("sk")),
        ("pk = 'a' AND a = sk", path("sk")),
        ("pk = 'a' AND sk = 'x' AND sk = a", path("sk")),
        ("pk = 'a' AND a = 'x' AND sk = a", path("sk")),
        ("pk = 'a' AND sk = pk", path("sk")),
        ("pk = 'a' AND pk = a", path("pk")),
        ("pk = 'a' AND sk.x = 1", scalar("sk")),
        ("pk = 'a' AND sk[0] = 1", scalar("sk")),
        ("pk = 'a' AND a = 1", None),
        ("pk = 'a' AND NOT (sk = 'x' AND a = 1)", path("sk")),
        ("pk = 'a' AND NOT (a = 1 OR pk = 'b')", path("pk")),
        // Under a NOT over an AND or an OR, a path into a key attribute
        // gets the filter refusal.
        ("pk = 'a' AND NOT (a = 1 AND sk.x = 1)", path("sk")),
        ("pk = 'a' AND NOT (sk.q = 1 AND n = 1)", path("sk")),
        ("pk = 'a' AND NOT (n = 1 OR sk[0] = 1)", path("sk")),
        ("pk = 'a' AND NOT (pk.q = 1 AND n = 1)", path("pk")),
        ("pk = 'a' AND NOT (a = 1 AND b = 2)", None),
        // Naming both keys, it names the sort key.
        ("pk = 'a' AND NOT (sk.q = 1 AND pk = b)", path("sk")),
        ("pk = 'a' AND NOT (pk = b AND sk.q = 1)", path("sk")),
    ] {
        assert_eq!(refusal(condition, KEYS), want, "{condition}");
    }
    assert_eq!(refusal("pk = 'a' AND a = pk", HASH_ONLY), path("pk"));
    assert_eq!(refusal("pk = 'a' AND pk.x = 1", HASH_ONLY), scalar("pk"));
}

#[test]
fn a_path_into_a_key_is_refused_anywhere_in_the_where() {
    let refusal = |condition: &str| {
        let w = where_of(&select(condition));
        WhereAnalysis::new(&w, KEYS, &literal).path_into_key_refusal(KEYS)
    };
    let scalar = |key: &str| Some(scalar_message(key));
    for (condition, want) in [
        // In a scan, and in a disjunct read by key after one.
        ("sk.x = 1", scalar("sk")),
        ("x = 1 OR (pk = 'a' AND sk.x = 1)", scalar("sk")),
        // In the filter of a disjunct read by key.
        ("pk = 'a' AND sk.x = 1", scalar("sk")),
        ("(pk = 'a' AND sk.x = 1) OR x = 1", scalar("sk")),
        ("pk = 'a' AND pk[0] = 1", scalar("pk")),
        // The disjuncts read by key come first, then statement order.
        ("(pk = 'a' OR sk.x = 1) AND pk.y = 1", scalar("pk")),
        ("x = 1 OR sk.x = 1 OR pk.y = 1", scalar("sk")),
        // Under a NOT over an AND or an OR it is the filter refusal's.
        ("pk = 'a' AND NOT (sk.q = 1 AND n = 1)", None),
        ("pk = 'a' AND sk = 'x'", None),
        ("x = 1 OR sk = a", None),
    ] {
        assert_eq!(refusal(condition), want, "{condition}");
    }
}

#[test]
fn disjuncts_are_read_by_partition_then_by_where_their_range_starts() {
    let order = |condition: &str, keys: KeyNames<'_>| {
        let w = where_of(&select(condition));
        let analysis = WhereAnalysis::new(&w, keys, &literal);
        let mut reads: Vec<(usize, &KeyRead<'_>)> = analysis.reads().iter().enumerate().collect();
        reads.sort_by(|a, b| read_order(a.1, b.1, &literal));
        reads.into_iter().map(|(i, _)| i).collect::<Vec<_>>()
    };
    assert_eq!(order("pk IN ['c', 'a', 'b']", HASH_ONLY), vec![1, 2, 0]);
    assert_eq!(order("pk IN [10, 9, 100]", HASH_ONLY), vec![1, 0, 2]);
    assert_eq!(
        order("(pk = 'b' AND sk = 'x') OR (pk = 'a' AND sk > 'a')", KEYS),
        vec![1, 0]
    );
    assert_eq!(
        order(
            "(pk = 'a' AND sk > 'y') OR (pk = 'a' AND sk = 'x') OR (pk = 'a' AND sk < 'b')",
            KEYS
        ),
        vec![2, 1, 0]
    );
    assert_eq!(
        order("pk = 'b' OR (pk = 'a' AND sk BETWEEN 'x' AND 'y')", KEYS),
        vec![1, 0]
    );
    // Values of two types keep statement order.
    assert_eq!(order("pk = 'b' OR pk = 1", HASH_ONLY), vec![0, 1]);
}

#[test]
fn a_write_condition_pins_a_key_written_either_way_round() {
    let equality = |condition: &str| {
        let w = where_of(&select(condition));
        key_equality(&w, KEYS).map(|e| match e {
            KeyEquality::Partition(v) => ("pk", literal(v)),
            KeyEquality::Sort(v) => ("sk", literal(v)),
        })
    };
    let s = |v: &str| Some(AttributeValue::S(v.to_owned()));
    assert_eq!(equality("pk = 'a'"), Some(("pk", s("a"))));
    assert_eq!(equality("'a' = pk"), Some(("pk", s("a"))));
    assert_eq!(equality("'x' = sk"), Some(("sk", s("x"))));
    // A comparison with a boolean that keeps the condition is the condition.
    assert_eq!(equality("pk = 'a' = true"), Some(("pk", s("a"))));
    assert_eq!(equality("pk = 'a' <> false"), Some(("pk", s("a"))));
    for condition in [
        "sk > 'x'",
        "pk <> 'a'",
        "pk = a",
        "x = 'a'",
        "pk IN ['a']",
        "pk = 'a' = false",
        "pk = 'a' <> true",
    ] {
        assert_eq!(equality(condition), None, "{condition}");
    }
}

#[test]
fn values_are_the_same_by_value() {
    let n = |v: &str| Operand::Literal(AttributeValue::N(v.to_owned()));
    let s = |v: &str| Operand::Literal(AttributeValue::S(v.to_owned()));
    assert!(same_value(&n("1"), &n("1.0"), &literal));
    assert!(!same_value(&n("1"), &s("1"), &literal));
    assert!(same_value(
        &Operand::Parameter(0),
        &Operand::Parameter(0),
        &literal
    ));
    assert!(!same_value(
        &Operand::Parameter(0),
        &Operand::Parameter(1),
        &literal
    ));
}

mod overlap;
