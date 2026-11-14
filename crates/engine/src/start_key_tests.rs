// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for [`validate_query_start_key_bounds`] (issue #440).

use std::collections::HashMap;

use extenddb_core::expression::CompareOp;

use super::*;

pub(super) const OUTSIDE: &str =
    "The provided starting key is outside query boundaries based on provided conditions";
pub(super) const EXCLUSIVE_INVALID: &str = "The provided exclusive start key is invalid";
pub(super) const AT_MOST_ONE_ROW: &str =
    "The query can return at most one row and cannot be restarted";
pub(super) const RANGE_PREDICATE: &str =
    "The provided starting key does not match the range key predicate";

pub(super) fn attr(name: &str) -> Vec<PathElement> {
    vec![PathElement::Attribute(name.to_owned())]
}

pub(super) fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

pub(super) fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_owned())
}

pub(super) fn ph(name: &str) -> Expr {
    Expr::Placeholder(name.to_owned())
}

pub(super) fn start_key(entries: &[(&str, AttributeValue)]) -> Item {
    entries
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

pub(super) fn maps(values: &[(&str, AttributeValue)], names: &[(&str, &str)]) -> ExpressionMaps {
    ExpressionMaps::new(
        names
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect::<HashMap<_, _>>(),
        values
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect::<HashMap<_, _>>(),
    )
}

pub(super) fn schema(hash: &str, range: Option<&str>) -> Vec<KeySchemaElement> {
    let mut out = vec![KeySchemaElement {
        attribute_name: hash.to_owned(),
        key_type: KeyType::Hash,
    }];
    if let Some(r) = range {
        out.push(KeySchemaElement {
            attribute_name: r.to_owned(),
            key_type: KeyType::Range,
        });
    }
    out
}

pub(super) fn condition(pk: &str, placeholder: &str) -> KeyCondition {
    KeyCondition {
        pk_path: attr(pk),
        pk_value: ph(placeholder),
        extra_pk_conditions: Vec::new(),
        sk_condition: None,
        extra_sk_conditions: Vec::new(),
    }
}

pub(super) fn with_sk(mut kc: KeyCondition, sk: SortKeyCondition) -> KeyCondition {
    kc.sk_condition = Some(sk);
    kc
}

pub(super) fn compare(path: &str, op: CompareOp, value: &str) -> SortKeyCondition {
    SortKeyCondition::Compare {
        path: attr(path),
        op,
        value: ph(value),
    }
}

pub(super) fn message(result: Result<(), DynamoDbError>) -> Option<String> {
    match result {
        Err(DynamoDbError::ValidationException(m)) => Some(m),
        Err(other) => panic!("unexpected error {other:?}"),
        Ok(()) => None,
    }
}

pub(super) fn base(key_schema: &[KeySchemaElement]) -> QueryReadShape<'_> {
    QueryReadShape {
        key_schema,
        on_index: false,
        forward: true,
    }
}

pub(super) fn check(
    key: &Item,
    kc: &KeyCondition,
    m: &ExpressionMaps,
    schema: &[KeySchemaElement],
) -> Option<String> {
    message(validate_query_start_key_bounds(key, kc, m, base(schema)))
}

pub(super) fn check_with(
    key: &Item,
    kc: &KeyCondition,
    m: &ExpressionMaps,
    shape: QueryReadShape<'_>,
) -> Option<String> {
    message(validate_query_start_key_bounds(key, kc, m, shape))
}

// --- Partition (issue row, measured) ---

#[test]
fn start_key_from_another_partition_is_rejected_with_the_service_message() {
    let key = start_key(&[("pk", s("bob")), ("sk", s("x"))]);
    let m = maps(&[("p", s("alice"))], &[]);
    assert_eq!(
        check(&key, &condition("pk", "p"), &m, &schema("pk", Some("sk"))).as_deref(),
        Some(OUTSIDE)
    );
}

#[test]
fn start_key_from_the_queried_partition_is_accepted() {
    let key = start_key(&[("pk", s("alice")), ("sk", s("x"))]);
    let m = maps(&[("p", s("alice"))], &[]);
    assert_eq!(
        check(&key, &condition("pk", "p"), &m, &schema("pk", Some("sk"))),
        None
    );
}

#[test]
fn partition_name_given_through_an_expression_attribute_name_is_resolved() {
    let kc = condition("#k", "p");
    let m = maps(&[("p", s("alice"))], &[("k", "pk")]);
    let sch = schema("pk", Some("sk"));
    let other = start_key(&[("pk", s("bob")), ("sk", s("x"))]);
    assert_eq!(check(&other, &kc, &m, &sch).as_deref(), Some(OUTSIDE));
    let same = start_key(&[("pk", s("alice")), ("sk", s("x"))]);
    assert_eq!(check(&same, &kc, &m, &sch), None);
}

#[test]
fn number_partition_values_compare_as_normalised_strings() {
    // Both sides are normalised at deserialization, before this function runs;
    // that path is guarded by the wire test
    // `test_number_key_equal_after_normalisation_is_accepted`.
    let kc = condition("pk", "p");
    let m = maps(&[("p", n("7"))], &[]);
    let sch = schema("pk", Some("sk"));
    let same = start_key(&[("pk", n("7")), ("sk", s("x"))]);
    assert_eq!(check(&same, &kc, &m, &sch), None);
    let other = start_key(&[("pk", n("8")), ("sk", s("x"))]);
    assert_eq!(check(&other, &kc, &m, &sch).as_deref(), Some(OUTSIDE));
}

#[test]
fn binary_partition_values_compare_by_bytes() {
    let b = |v: &[u8]| AttributeValue::B(v.to_vec());
    let kc = condition("pk", "p");
    let m = maps(&[("p", b(&[1, 2]))], &[]);
    let sch = schema("pk", Some("sk"));
    assert_eq!(
        check(
            &start_key(&[("pk", b(&[1, 2])), ("sk", s("x"))]),
            &kc,
            &m,
            &sch
        ),
        None
    );
    assert_eq!(
        check(
            &start_key(&[("pk", b(&[1, 3])), ("sk", s("x"))]),
            &kc,
            &m,
            &sch
        )
        .as_deref(),
        Some(OUTSIDE)
    );
}

#[test]
fn a_condition_value_of_another_type_is_left_to_the_existing_handling() {
    let kc = condition("pk", "p");
    let m = maps(&[("p", n("1"))], &[]);
    let key = start_key(&[("pk", s("alice")), ("sk", s("x"))]);
    assert_eq!(check(&key, &kc, &m, &schema("pk", Some("sk"))), None);
}

#[test]
fn an_undefined_value_placeholder_is_left_to_the_existing_handling() {
    let kc = condition("pk", "missing");
    let m = maps(&[], &[]);
    let key = start_key(&[("pk", s("alice")), ("sk", s("x"))]);
    assert_eq!(check(&key, &kc, &m, &schema("pk", Some("sk"))), None);
}

// --- Hash-only table versus every index (measured) ---

#[test]
fn base_query_on_a_table_without_a_range_key_refuses_every_start_key() {
    let kc = condition("pk", "p");
    let m = maps(&[("p", s("alice"))], &[]);
    let sch = schema("pk", None);
    assert_eq!(
        check(&start_key(&[("pk", s("bob"))]), &kc, &m, &sch).as_deref(),
        Some(EXCLUSIVE_INVALID)
    );
    // Measured 2026-10-24: pk = alice with the start key {pk: alice}, where
    // alice is the one item, is refused too.
    assert_eq!(
        check(&start_key(&[("pk", s("alice"))]), &kc, &m, &sch).as_deref(),
        Some(EXCLUSIVE_INVALID)
    );
}

#[test]
fn every_index_query_uses_the_partition_sentence_whatever_the_schemas_look_like() {
    // Hash-only GSI (no RANGE element) on a table with a sort key.
    let kc = condition("h", "h");
    let m = maps(&[("h", s("h1"))], &[]);
    let hash_only_index = schema("h", None);
    let on_index = QueryReadShape {
        key_schema: &hash_only_index,
        on_index: true,
        forward: true,
    };
    let other = start_key(&[("pk", s("alice")), ("sk", s("z")), ("h", s("h2"))]);
    assert_eq!(
        check_with(&other, &kc, &m, on_index).as_deref(),
        Some(OUTSIDE)
    );
    // Same index partition, a base key that is not in the index: accepted.
    let same = start_key(&[("pk", s("nobody")), ("sk", s("q")), ("h", s("h1"))]);
    assert_eq!(check_with(&same, &kc, &m, on_index), None);

    // GSI with a RANGE element on a table without one.
    let kc = condition("g", "g");
    let m = maps(&[("g", s("one"))], &[]);
    let ranged_index = schema("g", Some("gs"));
    let on_index = QueryReadShape {
        key_schema: &ranged_index,
        on_index: true,
        forward: true,
    };
    let other = start_key(&[("pk", s("p2")), ("g", s("two")), ("gs", s("x"))]);
    assert_eq!(
        check_with(&other, &kc, &m, on_index).as_deref(),
        Some(OUTSIDE)
    );
}

#[test]
fn index_query_compares_the_index_partition_and_ignores_the_base_key() {
    let kc = condition("g", "g");
    let m = maps(&[("g", s("one"))], &[]);
    let sch = schema("g", Some("gs"));
    let shape = QueryReadShape {
        key_schema: &sch,
        on_index: true,
        forward: true,
    };
    let other_base = start_key(&[("pk", s("p9")), ("g", s("one")), ("gs", s("x"))]);
    assert_eq!(check_with(&other_base, &kc, &m, shape), None);
    let other_index_partition = start_key(&[("pk", s("p1")), ("g", s("two")), ("gs", s("x"))]);
    assert_eq!(
        check_with(&other_index_partition, &kc, &m, shape).as_deref(),
        Some(OUTSIDE)
    );
}

#[test]
fn every_attribute_of_a_multi_attribute_partition_is_compared() {
    let mut kc = condition("a", "pa");
    kc.extra_pk_conditions.push((attr("b"), ph("pb")));
    let m = maps(&[("pa", s("1")), ("pb", s("2"))], &[]);
    let hash = |name: &str, key_type| KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    };
    let sch = vec![
        hash("a", KeyType::Hash),
        hash("b", KeyType::Hash),
        hash("id", KeyType::Range),
    ];
    let shape = QueryReadShape {
        key_schema: &sch,
        on_index: true,
        forward: true,
    };
    let ok = start_key(&[("a", s("1")), ("b", s("2")), ("id", s("x"))]);
    assert_eq!(check_with(&ok, &kc, &m, shape), None);
    let first_differs = start_key(&[("a", s("9")), ("b", s("2")), ("id", s("x"))]);
    assert_eq!(
        check_with(&first_differs, &kc, &m, shape).as_deref(),
        Some(OUTSIDE)
    );
    let second_differs = start_key(&[("a", s("1")), ("b", s("9")), ("id", s("x"))]);
    assert_eq!(
        check_with(&second_differs, &kc, &m, shape).as_deref(),
        Some(OUTSIDE)
    );
}
