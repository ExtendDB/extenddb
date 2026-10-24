// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Key conditions with no equality clause on the HASH attribute. Every
//! expected message is a row measured on the service: the issue table on
//! 2026-10-21, everything else on 2026-10-23 (the issue's fold files
//! `FOLD-427.md` rows `427-*` and `FOLD-427-R.md` rows from
//! `service-427-review.jsonl` and `service-427-review-2.jsonl`).

use std::collections::HashMap;

use super::*;
use crate::expression::{collect_key_condition_refs, parse_key_condition, tokenize};
use crate::types::KeyType;

const WANT_ONE_PER_KEY: &str = "Invalid KeyConditionExpression: KeyConditionExpressions must \
                                only contain one condition per key";
const MISSED_PK: &str = "Query condition missed key schema element: pk";
const MISSED_G: &str = "Query condition missed key schema element: g";

fn element(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

/// HASH `pk`, RANGE `sk`: the base table of the issue.
fn table_schema() -> Vec<KeySchemaElement> {
    vec![element("pk", KeyType::Hash), element("sk", KeyType::Range)]
}

/// HASH `pk`, RANGE `ls`: a local secondary index on the table above.
fn lsi_schema() -> Vec<KeySchemaElement> {
    vec![element("pk", KeyType::Hash), element("ls", KeyType::Range)]
}

/// HASH `g`, RANGE `s`: the single-attribute index of the issue.
fn gsi_schema() -> Vec<KeySchemaElement> {
    vec![element("g", KeyType::Hash), element("s", KeyType::Range)]
}

/// HASH `h`, RANGE `a`, `b`, `c`, `d`: the multi-attribute index of the issue.
fn multi_range_schema() -> Vec<KeySchemaElement> {
    vec![
        element("h", KeyType::Hash),
        element("a", KeyType::Range),
        element("b", KeyType::Range),
        element("c", KeyType::Range),
        element("d", KeyType::Range),
    ]
}

/// HASH `h1`, `h2`, RANGE `r`.
fn multi_hash_schema() -> Vec<KeySchemaElement> {
    vec![
        element("h1", KeyType::Hash),
        element("h2", KeyType::Hash),
        element("r", KeyType::Range),
    ]
}

fn parse(expr: &str) -> KeyCondition {
    let tokens = tokenize(expr).unwrap();
    parse_key_condition(&tokens).unwrap_or_else(|e| panic!("{expr}: {e:?}"))
}

fn refusal(expr: &str, schema: &[KeySchemaElement]) -> String {
    refusal_with_names(expr, schema, &HashMap::new())
}

fn refusal_with_names(
    expr: &str,
    schema: &[KeySchemaElement],
    names: &HashMap<String, String>,
) -> String {
    match parse(expr).refuse_without_hash_equality(schema, names) {
        Err(DynamoDbError::ValidationException(msg)) => msg,
        other => panic!("{expr}: expected a refusal, got {other:?}"),
    }
}

fn assert_all(rows: &[(&str, &str)], schema: &[KeySchemaElement]) {
    for (expr, want) in rows {
        assert_eq!(refusal(expr, schema), *want, "{expr}");
    }
}

#[test]
fn clauses_without_an_equality_parse_without_a_partition_key_condition() {
    for expr in [
        "pk > :x",
        "pk >= :x",
        "pk < :x",
        "pk <= :x",
        ":x < pk",
        "begins_with(pk, :x)",
        "pk BETWEEN :a AND :b",
        "(pk > :x)",
        "pk > :x AND sk > :y",
        "begins_with(pk, :x) AND sk BETWEEN :a AND :b",
    ] {
        let kc = parse(expr);
        assert!(kc.pk_path.is_empty(), "{expr}");
        assert!(kc.extra_pk_conditions.is_empty(), "{expr}");
        assert!(kc.sk_condition.is_some(), "{expr}");
    }
}

#[test]
fn the_operands_of_both_clauses_count_as_used_references() {
    let kc = parse("#a > :x AND zz BETWEEN :lo AND :hi");
    let (names, values) = collect_key_condition_refs(&kc);
    assert!(names.contains("a"));
    for value in ["x", "lo", "hi"] {
        assert!(values.contains(value), "{value}");
    }
}

#[test]
fn key_conditions_with_an_equality_on_the_hash_attribute_are_not_refused() {
    let schema = table_schema();
    for expr in [
        "pk = :p",
        "pk = :p AND sk > :s",
        "sk > :s AND pk = :p",
        "sk = :s AND pk = :p",
        "pk = :p AND begins_with(sk, :s)",
        "pk = :p AND sk BETWEEN :a AND :b",
        // Two conditions on one key: the service refuses these; ExtendDB
        // does not yet and this change leaves them alone.
        "pk = :p AND pk > :x",
        "pk > :x AND pk = :p",
        // Three clauses are outside this rule.
        "a = :a AND b = :b AND pk > :x",
    ] {
        let kc = parse(expr);
        assert!(
            !kc.lacks_hash_equality(&schema, &HashMap::new()),
            "lacks: {expr}"
        );
        assert!(
            kc.refuse_without_hash_equality(&schema, &HashMap::new())
                .is_ok(),
            "{expr}"
        );
    }
}

#[test]
fn a_single_equality_on_another_attribute_is_left_to_the_partition_key_check() {
    // `sk = :x` and `zz = :x` carry an equality, so they keep the engine's
    // existing `missed key schema element` check.
    let schema = table_schema();
    for expr in [
        "sk = :x",
        "zz = :x",
        "zz = :x AND sk > :y",
        "zz = :x AND yy = :y",
    ] {
        assert!(
            !parse(expr).lacks_hash_equality(&schema, &HashMap::new()),
            "{expr}"
        );
    }
}

#[test]
fn single_clause_on_the_partition_key_is_not_supported() {
    assert_all(
        &[
            ("pk > :x", NOT_SUPPORTED),
            ("pk >= :x", NOT_SUPPORTED),
            ("pk < :x", NOT_SUPPORTED),
            ("pk <= :x", NOT_SUPPORTED),
            (":x < pk", NOT_SUPPORTED),
            (":x >= pk", NOT_SUPPORTED),
            ("begins_with(pk, :x)", NOT_SUPPORTED),
            ("pk BETWEEN :x AND :y", NOT_SUPPORTED),
            ("(pk > :x)", NOT_SUPPORTED),
        ],
        &table_schema(),
    );
}

#[test]
fn single_clause_on_an_index_or_a_hash_only_table_is_not_supported() {
    assert_all(
        &[
            ("g > :x", NOT_SUPPORTED),
            ("begins_with(g, :x)", NOT_SUPPORTED),
            ("g BETWEEN :x AND :y", NOT_SUPPORTED),
        ],
        &gsi_schema(),
    );
    assert_all(
        &[
            ("pk > :x", NOT_SUPPORTED),
            ("begins_with(pk, :x)", NOT_SUPPORTED),
        ],
        &[element("pk", KeyType::Hash)],
    );
    // An LSI shares the table's partition key (rows 427-5 and 427-5b).
    assert_eq!(refusal("pk > :x", &lsi_schema()), NOT_SUPPORTED);
    assert_eq!(refusal("ls > :x", &lsi_schema()), MISSED_PK);
}

#[test]
fn single_clause_on_a_sort_key_or_a_non_key_attribute_misses_the_partition_key() {
    assert_all(
        &[
            ("sk > :x", MISSED_PK),
            ("sk <= :x", MISSED_PK),
            (":x > sk", MISSED_PK),
            ("begins_with(sk, :x)", MISSED_PK),
            ("sk BETWEEN :a AND :b", MISSED_PK),
            ("z > :x", MISSED_PK),
        ],
        &table_schema(),
    );
    assert_eq!(refusal("s > :x", &gsi_schema()), MISSED_G);
}

#[test]
fn defined_name_references_are_resolved_before_the_wording_is_chosen() {
    let schema = table_schema();
    let mut names = HashMap::new();
    for (reference, attr) in [("p", "pk"), ("s", "sk"), ("z", "zz")] {
        names.insert(reference.to_owned(), attr.to_owned());
    }
    // Rows 2a to 2d of service-427-review.jsonl.
    for (expr, want) in [
        ("#p > :x", NOT_SUPPORTED),
        ("begins_with(#p, :x)", NOT_SUPPORTED),
        ("#s > :x", MISSED_PK),
        ("#z > :x", MISSED_PK),
        ("begins_with(#s, :x)", MISSED_PK),
        ("#s > :x AND #z > :y", MISSED_PK),
        ("#z > :x AND #p > :y", NOT_SUPPORTED),
    ] {
        assert_eq!(refusal_with_names(expr, &schema, &names), want, "{expr}");
    }
}

#[test]
fn an_undefined_name_reference_is_refused_by_name_on_every_schema() {
    // Row 427-7, and `#n > :x AND sk > :y` from service-427-review-2.jsonl.
    const WANT: &str = "Invalid KeyConditionExpression: An expression attribute name used in \
                        the document path is not defined; attribute name: #nope";
    for schema in [table_schema(), gsi_schema(), multi_range_schema()] {
        assert_eq!(refusal("#nope > :x", &schema), WANT);
        assert_eq!(refusal("begins_with(#nope, :x)", &schema), WANT);
    }
    assert_eq!(
        refusal("#n > :x AND sk > :y", &table_schema()),
        "Invalid KeyConditionExpression: An expression attribute name used in \
         the document path is not defined; attribute name: #n"
    );
    let mut names = HashMap::new();
    names.insert("other".to_owned(), "pk".to_owned());
    assert_eq!(
        refusal_with_names("#nope > :x", &table_schema(), &names),
        WANT
    );
}

#[test]
fn two_clauses_where_a_non_equality_names_the_partition_key_are_not_supported() {
    // Rows of service-427-review-2.jsonl, table with HASH `pk`, RANGE `sk`.
    assert_all(
        &[
            ("pk > :x AND sk > :y", NOT_SUPPORTED),
            ("sk > :x AND pk > :y", NOT_SUPPORTED),
            ("pk > :x AND zz > :y", NOT_SUPPORTED),
            ("zz > :x AND pk > :y", NOT_SUPPORTED),
            ("pk >= :x AND sk = :y", NOT_SUPPORTED),
            ("sk = :x AND pk > :y", NOT_SUPPORTED),
            ("pk > :x AND sk = :y", NOT_SUPPORTED),
            ("begins_with(pk, :x) AND sk = :y", NOT_SUPPORTED),
            ("pk BETWEEN :x AND :y AND sk = :x", NOT_SUPPORTED),
            ("zz = :x AND pk > :y", NOT_SUPPORTED),
        ],
        &table_schema(),
    );
    // GSI with HASH `g` and RANGE `s` (the service rows use RANGE `gs`).
    assert_all(
        &[
            ("g > :x AND s > :y", NOT_SUPPORTED),
            ("g > :x AND s = :y", NOT_SUPPORTED),
        ],
        &gsi_schema(),
    );
}

#[test]
fn two_clauses_where_none_names_the_partition_key_miss_it() {
    assert_all(
        &[
            ("sk > :x AND zz > :y", MISSED_PK),
            ("zz > :x AND sk > :y", MISSED_PK),
        ],
        &table_schema(),
    );
    // The same rows on the GSI, plus the two that name the table's key.
    assert_all(
        &[
            ("s > :x AND zz > :y", MISSED_G),
            ("pk > :x AND s > :y", MISSED_G),
        ],
        &gsi_schema(),
    );
}

#[test]
fn an_equality_on_a_non_key_attribute_with_a_non_key_clause_is_not_refused_here() {
    // `zz = :x AND sk > :y`, `zz = :x AND yy = :y`, `sk = :x AND zz = :y` and
    // `pk = :x AND s > :y` on the GSI carry an equality; the engine's
    // partition key check answers them (`missed key schema element`).
    let table = table_schema();
    for expr in [
        "zz = :x AND sk > :y",
        "zz = :x AND yy = :y",
        "sk = :x AND zz = :y",
    ] {
        assert!(
            !parse(expr).lacks_hash_equality(&table, &HashMap::new()),
            "{expr}"
        );
    }
    assert!(!parse("pk = :x AND s > :y").lacks_hash_equality(&gsi_schema(), &HashMap::new()));
}

#[test]
fn two_conditions_on_one_key_are_refused_as_such() {
    // Rows `pk > :x AND pk < :y` and `sk > :x AND sk < :y`.
    for expr in [
        "pk > :x AND pk < :y",
        "sk > :x AND sk < :y",
        "zz >= :x AND zz <= :y",
        "begins_with(pk, :x) AND pk > :y",
    ] {
        assert_eq!(refusal(expr, &table_schema()), WANT_ONE_PER_KEY, "{expr}");
    }
    let mut names = HashMap::new();
    names.insert("a".to_owned(), "pk".to_owned());
    assert_eq!(
        refusal_with_names("#a > :x AND pk < :y", &table_schema(), &names),
        WANT_ONE_PER_KEY
    );
    // The check does not depend on the schema.
    assert_eq!(
        refusal("h > :x AND h < :y", &multi_range_schema()),
        WANT_ONE_PER_KEY
    );
}

#[test]
fn multi_attribute_index_names_the_hash_attribute_for_every_clause() {
    // Rows `3 multi-idx *` and `4 multi-idx h > :n` of service-427-review.jsonl.
    let want = "HASH key attributes h must have equality conditions specified in the query";
    for expr in [
        "h > :h",
        "begins_with(h, :h)",
        "h <= :h",
        ":h < h",
        "a > :x",
        "d > :x",
        "z > :x",
        "begins_with(a, :x)",
        "a BETWEEN :x AND :y",
    ] {
        assert_eq!(refusal(expr, &multi_range_schema()), want, "{expr}");
    }
}

#[test]
fn multi_hash_index_names_every_hash_attribute() {
    // Rows `3 two-hash *` of service-427-review.jsonl.
    let want = "HASH key attributes h1, h2 must have equality conditions specified in the query";
    for expr in ["h1 > :x", "h2 > :x", "r > :x", "z > :x"] {
        assert_eq!(refusal(expr, &multi_hash_schema()), want, "{expr}");
    }
    let mut names = HashMap::new();
    names.insert("a".to_owned(), "h1".to_owned());
    assert_eq!(
        refusal_with_names("#a > :x", &multi_hash_schema(), &names),
        want
    );
}

#[test]
fn two_range_attributes_with_one_hash_attribute_count_as_multi_attribute() {
    let schema = vec![
        element("h", KeyType::Hash),
        element("a", KeyType::Range),
        element("b", KeyType::Range),
    ];
    assert_eq!(
        refusal("h > :h", &schema),
        "HASH key attributes h must have equality conditions specified in the query"
    );
}

#[test]
fn a_multi_attribute_schema_with_an_equality_is_left_to_the_classifier() {
    let kc = parse("h = :h AND a > :x");
    assert!(!kc.lacks_hash_equality(&multi_range_schema(), &HashMap::new()));
}
