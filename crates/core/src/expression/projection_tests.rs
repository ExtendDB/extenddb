// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::resolver::ExpressionMaps;
use crate::expression::tokenizer::tokenize;
use std::collections::HashMap;

fn project(
    expr_str: &str,
    item: &Item,
    names: HashMap<String, String>,
) -> Result<Item, DynamoDbError> {
    let tokens = tokenize(expr_str)?;
    let paths = parse_projection(&tokens)?;
    let maps = ExpressionMaps::new(names, HashMap::new());
    apply_projection(item, &paths, &maps)
}

fn sample_item() -> Item {
    let mut inner = BTreeMap::new();
    inner.insert("city".into(), AttributeValue::S("NYC".into()));
    inner.insert("zip".into(), AttributeValue::S("10001".into()));
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("key1".into()));
    item.insert("name".into(), AttributeValue::S("Alice".into()));
    item.insert("age".into(), AttributeValue::N("30".into()));
    item.insert("address".into(), AttributeValue::M(inner));
    item
}

#[test]
fn project_single_attribute() {
    let item = sample_item();
    let result = project("name", &item, HashMap::new()).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.get("name"), Some(&AttributeValue::S("Alice".into())));
}

#[test]
fn project_multiple_attributes() {
    let item = sample_item();
    let result = project("name, age", &item, HashMap::new()).unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains_key("name"));
    assert!(result.contains_key("age"));
}

#[test]
fn project_missing_attribute_omitted() {
    let item = sample_item();
    let result = project("name, missing", &item, HashMap::new()).unwrap();
    assert_eq!(result.len(), 1);
    assert!(result.contains_key("name"));
}

#[test]
fn project_nested_path() {
    let item = sample_item();
    let result = project("address.city", &item, HashMap::new()).unwrap();
    assert!(result.contains_key("address"));
    if let Some(AttributeValue::M(m)) = result.get("address") {
        assert_eq!(m.get("city"), Some(&AttributeValue::S("NYC".into())));
        assert!(!m.contains_key("zip")); // Only city projected
    } else {
        panic!("Expected M");
    }
}

#[test]
fn project_with_name_ref() {
    let item = sample_item();
    let mut names = HashMap::new();
    names.insert("n".into(), "name".into());
    let result = project("#n", &item, names).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.get("name"), Some(&AttributeValue::S("Alice".into())));
}

#[test]
fn project_empty_expression() {
    let item = sample_item();
    let tokens = tokenize("").unwrap();
    let paths = parse_projection(&tokens).unwrap();
    assert!(paths.is_empty());
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    let result = apply_projection(&item, &paths, &maps).unwrap();
    assert!(result.is_empty());
}

#[test]
fn project_all_attributes() {
    let item = sample_item();
    let result = project("pk, name, age, address", &item, HashMap::new()).unwrap();
    assert_eq!(result.len(), 4);
}

#[test]
fn project_list_index() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("k1".into()));
    item.insert(
        "mylist".into(),
        AttributeValue::L(vec![
            AttributeValue::S("zero".into()),
            AttributeValue::S("one".into()),
            AttributeValue::S("two".into()),
        ]),
    );

    let result = project("mylist[0]", &item, HashMap::new()).unwrap();
    assert_eq!(result.len(), 1);
    match result.get("mylist") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0], AttributeValue::S("zero".into()));
        }
        other => panic!("Expected L, got {other:?}"),
    }

    let result = project("mylist[1]", &item, HashMap::new()).unwrap();
    match result.get("mylist") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0], AttributeValue::S("one".into()));
        }
        other => panic!("Expected L, got {other:?}"),
    }

    // Out-of-bounds index returns empty
    let result = project("mylist[5]", &item, HashMap::new()).unwrap();
    assert!(result.is_empty());
}

fn list_item() -> Item {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("p1".into()));
    item.insert(
        "mylist".into(),
        AttributeValue::L(vec![
            AttributeValue::S("zero".into()),
            AttributeValue::S("one".into()),
            AttributeValue::S("two".into()),
            AttributeValue::S("three".into()),
        ]),
    );
    item.insert(
        "with_null".into(),
        AttributeValue::L(vec![
            AttributeValue::S("keep0".into()),
            AttributeValue::Null,
            AttributeValue::S("keep2".into()),
        ]),
    );
    item
}

fn assert_list(result: &Item, key: &str, expected: &[AttributeValue]) {
    match result.get(key) {
        Some(AttributeValue::L(list)) => assert_eq!(list.as_slice(), expected),
        other => panic!("Expected L for {key}, got {other:?}"),
    }
}

#[test]
fn project_two_list_indices_compacted() {
    let item = list_item();
    let result = project("mylist[0], mylist[2]", &item, HashMap::new()).unwrap();
    assert_list(
        &result,
        "mylist",
        &[
            AttributeValue::S("zero".into()),
            AttributeValue::S("two".into()),
        ],
    );
}

#[test]
fn project_list_indices_ordered_by_index_not_expression() {
    let item = list_item();
    // Reversed expression order still comes out in ascending index order.
    let result = project("mylist[2], mylist[0]", &item, HashMap::new()).unwrap();
    assert_list(
        &result,
        "mylist",
        &[
            AttributeValue::S("zero".into()),
            AttributeValue::S("two".into()),
        ],
    );
}

#[test]
fn project_list_index_gap_compacted() {
    let item = list_item();
    let result = project("mylist[1], mylist[3]", &item, HashMap::new()).unwrap();
    assert_list(
        &result,
        "mylist",
        &[
            AttributeValue::S("one".into()),
            AttributeValue::S("three".into()),
        ],
    );
}

#[test]
fn project_null_element_by_index_preserved() {
    let item = list_item();
    let result = project("with_null[1]", &item, HashMap::new()).unwrap();
    assert_list(&result, "with_null", &[AttributeValue::Null]);
}

#[test]
fn project_unselected_null_dropped() {
    let item = list_item();
    let result = project("with_null[0], with_null[2]", &item, HashMap::new()).unwrap();
    assert_list(
        &result,
        "with_null",
        &[
            AttributeValue::S("keep0".into()),
            AttributeValue::S("keep2".into()),
        ],
    );
}

#[test]
fn project_whole_list_preserves_null() {
    let item = list_item();
    let result = project("with_null", &item, HashMap::new()).unwrap();
    assert_list(
        &result,
        "with_null",
        &[
            AttributeValue::S("keep0".into()),
            AttributeValue::Null,
            AttributeValue::S("keep2".into()),
        ],
    );
}

#[test]
fn project_list_of_maps_subfield_multi() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("p".into()));
    let mk = |v: &str, x: &str| {
        let mut m = BTreeMap::new();
        m.insert("val".into(), AttributeValue::S(v.into()));
        m.insert("x".into(), AttributeValue::S(x.into()));
        AttributeValue::M(m)
    };
    item.insert(
        "lom".into(),
        AttributeValue::L(vec![mk("a0", "x0"), mk("a1", "x1"), mk("a2", "x2")]),
    );

    let result = project("lom[0].val, lom[2].val", &item, HashMap::new()).unwrap();
    let only_val = |v: &str| {
        let mut m = BTreeMap::new();
        m.insert("val".into(), AttributeValue::S(v.into()));
        AttributeValue::M(m)
    };
    assert_list(&result, "lom", &[only_val("a0"), only_val("a2")]);
}

#[test]
fn project_same_index_merges_subfields() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("p".into()));
    let mut m = BTreeMap::new();
    m.insert("a".into(), AttributeValue::S("av".into()));
    m.insert("b".into(), AttributeValue::S("bv".into()));
    m.insert("c".into(), AttributeValue::S("cv".into()));
    item.insert("l".into(), AttributeValue::L(vec![AttributeValue::M(m)]));

    // Two paths selecting the same element merge their subfields.
    let result = project("l[0].a, l[0].c", &item, HashMap::new()).unwrap();
    let mut expected = BTreeMap::new();
    expected.insert("a".into(), AttributeValue::S("av".into()));
    expected.insert("c".into(), AttributeValue::S("cv".into()));
    assert_list(&result, "l", &[AttributeValue::M(expected)]);
}

fn check_overlap(expr: &str, names: HashMap<String, String>) -> Result<(), DynamoDbError> {
    let tokens = tokenize(expr).unwrap();
    let paths = parse_projection(&tokens).unwrap();
    let maps = ExpressionMaps::new(names, HashMap::new());
    Projection::compile(&paths, &maps, true).map(|_| ())
}

#[test]
fn compile_without_overlap_rejection_accepts_duplicates() {
    // Desugared AttributesToGet keeps the legacy accept-duplicates behavior.
    let tokens = tokenize("a, a").unwrap();
    let paths = parse_projection(&tokens).unwrap();
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    assert!(Projection::compile(&paths, &maps, false).is_ok());
}

#[test]
fn compiled_projection_reusable_across_items() {
    let tokens = tokenize("name").unwrap();
    let paths = parse_projection(&tokens).unwrap();
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    let proj = Projection::compile(&paths, &maps, true).unwrap();
    let item = sample_item();
    for _ in 0..2 {
        let out = proj.apply(&item);
        assert_eq!(out.len(), 1);
        assert!(out.contains_key("name"));
    }
}

fn overlap_msg(expr: &str) -> String {
    match check_overlap(expr, HashMap::new()) {
        Err(DynamoDbError::ValidationException(m)) => m,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

#[test]
fn overlap_parent_then_child() {
    assert_eq!(
        overlap_msg("a, a.b"),
        "Invalid ProjectionExpression: Two document paths overlap with each other; \
         must remove or rewrite one of these paths; path one: [a], path two: [a, b]"
    );
}

#[test]
fn overlap_child_then_parent() {
    assert_eq!(
        overlap_msg("a.b, a"),
        "Invalid ProjectionExpression: Two document paths overlap with each other; \
         must remove or rewrite one of these paths; path one: [a, b], path two: [a]"
    );
}

#[test]
fn overlap_exact_duplicate() {
    assert_eq!(
        overlap_msg("a, a"),
        "Invalid ProjectionExpression: Two document paths overlap with each other; \
         must remove or rewrite one of these paths; path one: [a], path two: [a]"
    );
}

#[test]
fn overlap_index_path_rendering() {
    assert_eq!(
        overlap_msg("a[0], a"),
        "Invalid ProjectionExpression: Two document paths overlap with each other; \
         must remove or rewrite one of these paths; path one: [a, [0]], path two: [a]"
    );
    assert_eq!(
        overlap_msg("a[0].b, a[0]"),
        "Invalid ProjectionExpression: Two document paths overlap with each other; \
         must remove or rewrite one of these paths; path one: [a, [0], b], path two: [a, [0]]"
    );
}

#[test]
fn overlap_reports_first_pair_in_order() {
    assert_eq!(
        overlap_msg("x, a.b, a"),
        "Invalid ProjectionExpression: Two document paths overlap with each other; \
         must remove or rewrite one of these paths; path one: [a, b], path two: [a]"
    );
}

#[test]
fn overlap_uses_resolved_names() {
    let mut names = HashMap::new();
    names.insert("p".into(), "a".into());
    names.insert("q".into(), "b".into());
    match check_overlap("#p, #p.#q", names) {
        Err(DynamoDbError::ValidationException(m)) => assert_eq!(
            m,
            "Invalid ProjectionExpression: Two document paths overlap with each other; \
             must remove or rewrite one of these paths; path one: [a], path two: [a, b]"
        ),
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

#[test]
fn no_overlap_for_siblings() {
    assert!(check_overlap("a.b, a.c", HashMap::new()).is_ok());
    assert!(check_overlap("a[0], a[1]", HashMap::new()).is_ok());
    assert!(check_overlap("a, b, c", HashMap::new()).is_ok());
}

#[test]
fn project_list_index_into_map_preserves_structure() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("p".into()));
    let mut map0 = BTreeMap::new();
    map0.insert("val".into(), AttributeValue::S("target".into()));
    map0.insert("x".into(), AttributeValue::S("0".into()));
    let mut map1 = BTreeMap::new();
    map1.insert("x".into(), AttributeValue::S("0".into()));
    item.insert(
        "a_list".into(),
        AttributeValue::L(vec![AttributeValue::M(map0), AttributeValue::M(map1)]),
    );

    let result = project("a_list[0].val", &item, HashMap::new()).unwrap();
    // Should preserve: {"a_list": [{"val": "target"}]}
    match result.get("a_list") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list.len(), 1);
            match &list[0] {
                AttributeValue::M(m) => {
                    assert_eq!(m.get("val"), Some(&AttributeValue::S("target".into())));
                    assert!(!m.contains_key("x"));
                }
                other => panic!("Expected M, got {other:?}"),
            }
        }
        other => panic!("Expected L, got {other:?}"),
    }
}

// Syntax error wording measured 2026-10-25 (us-east-1) on GetItem.

fn proj_msg(src: &str) -> String {
    match parse_projection_from(&tokenize(src).unwrap(), src) {
        Err(DynamoDbError::ValidationException(m)) => m,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

#[test]
fn doubled_comma_quotes_the_source_window() {
    // [d27] `#v, , #v`
    assert_eq!(
        proj_msg("#v, , #v"),
        "Invalid ProjectionExpression: Syntax error; token: \",\", near: \", , #v\""
    );
}

#[test]
fn trailing_comma_is_eof_with_the_comma_as_window() {
    // [d28] `#v,`
    assert_eq!(
        proj_msg("#v,"),
        "Invalid ProjectionExpression: Syntax error; token: \"<EOF>\", near: \",\""
    );
}

#[test]
fn other_projection_texts_are_unchanged() {
    assert_eq!(
        proj_msg("#v #w"),
        "Invalid ProjectionExpression: unexpected token at position 1"
    );
    assert_eq!(proj_msg("#v, )"), "Invalid expression: expected attribute name");
}
