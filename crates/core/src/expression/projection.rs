// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ProjectionExpression` parser and evaluator.
//!
//! Parses a comma-separated list of document paths and applies them to an item,
//! returning only the requested attributes.

use std::collections::BTreeMap;

use super::ast::PathElement;
use super::resolver::{ExpressionMaps, resolve_element_name, resolve_name_ref};
use super::tokenizer::Token;
use crate::error::DynamoDbError;
use crate::types::{AttributeValue, Item};

/// Parse a `ProjectionExpression` token stream into a list of document paths.
///
/// Grammar: `path ( ',' path )*`
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors.
pub fn parse_projection(tokens: &[Token]) -> Result<Vec<Vec<PathElement>>, DynamoDbError> {
    if tokens.is_empty() {
        return Ok(Vec::new());
    }
    let mut pos = 0;
    let mut paths = vec![super::parser_common::parse_path(tokens, &mut pos)?];
    while pos < tokens.len() {
        if tokens[pos] != Token::Comma {
            return Err(DynamoDbError::ValidationException(format!(
                "Invalid ProjectionExpression: unexpected token at position {pos}"
            )));
        }
        pos += 1;
        paths.push(super::parser_common::parse_path(tokens, &mut pos)?);
    }
    Ok(paths)
}

/// A node in the projection trie.
///
/// Each node selects part of the item. `terminal` means the whole value at
/// this position is projected. Otherwise the node descends via `attrs` (for a
/// map) or `indices` (for a list). A value is either a map or a list, so only
/// one of the two child maps is populated on a given node in practice.
///
/// `indices` is a `BTreeMap` so selected list elements come out in ascending
/// original-index order, which is how DynamoDB compacts list projections.
#[derive(Default)]
struct ProjNode {
    terminal: bool,
    attrs: BTreeMap<String, ProjNode>,
    indices: BTreeMap<usize, ProjNode>,
}

/// Apply a projection to an item, returning only the requested attributes.
///
/// List elements selected by index are returned in a new list compacted in
/// ascending original-index order (matching Amazon DynamoDB), not in the order
/// the indices appear in the expression. Map keys not on a projected path are
/// dropped. The structure of the original item is otherwise preserved.
///
/// # Errors
///
/// Returns `ValidationException` for unresolvable `#name` references or a path
/// that starts with an index.
pub fn apply_projection(
    item: &Item,
    paths: &[Vec<PathElement>],
    maps: &ExpressionMaps,
) -> Result<Item, DynamoDbError> {
    let root = build_trie(paths, maps)?;

    let mut result = BTreeMap::new();
    for (name, child) in &root.attrs {
        if let Some(val) = item.get(name)
            && let Some(projected) = project_value(val, child)
        {
            result.insert(name.clone(), projected);
        }
    }
    Ok(result)
}

/// A path element after `#name` resolution, used for overlap detection.
#[derive(PartialEq, Eq)]
enum ResolvedElement {
    Attr(String),
    Index(usize),
}

/// Reject a `ProjectionExpression` whose paths overlap, matching Amazon DynamoDB.
///
/// Two paths overlap when one is a prefix of the other (this includes equal
/// paths), for example `a` and `a.b`, or `a[0]` and `a[0].b`, or a duplicate
/// `a` and `a`. Sibling paths such as `a.b` and `a.c`, or `a[0]` and `a[1]`,
/// do not overlap. `#name` references are resolved first, so the reported
/// paths use the resolved attribute names. The first offending pair in
/// expression order is reported.
///
/// # Errors
///
/// Returns `ValidationException` with the overlap message, or an unresolvable
/// `#name` reference error.
pub fn detect_overlapping_paths(
    paths: &[Vec<PathElement>],
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    let mut resolved: Vec<Vec<ResolvedElement>> = Vec::with_capacity(paths.len());
    for path in paths {
        if path.is_empty() {
            continue;
        }
        let mut elems = Vec::with_capacity(path.len());
        for element in path {
            match element {
                PathElement::Attribute(raw) => {
                    elems.push(ResolvedElement::Attr(
                        resolve_name_ref(raw, maps)?.into_owned(),
                    ));
                }
                PathElement::Index(idx) => elems.push(ResolvedElement::Index(*idx)),
            }
        }
        resolved.push(elems);
    }

    for i in 0..resolved.len() {
        for j in (i + 1)..resolved.len() {
            if is_prefix(&resolved[i], &resolved[j]) || is_prefix(&resolved[j], &resolved[i]) {
                return Err(DynamoDbError::ValidationException(format!(
                    "Invalid ProjectionExpression: Two document paths overlap with each other; \
                     must remove or rewrite one of these paths; path one: {}, path two: {}",
                    render_path(&resolved[i]),
                    render_path(&resolved[j]),
                )));
            }
        }
    }
    Ok(())
}

/// Return true when `a` is a prefix of `b` (equal paths included).
fn is_prefix(a: &[ResolvedElement], b: &[ResolvedElement]) -> bool {
    a.len() <= b.len() && a == &b[..a.len()]
}

/// Render a resolved path as DynamoDB does: `[a, [0], b]`.
fn render_path(path: &[ResolvedElement]) -> String {
    let parts: Vec<String> = path
        .iter()
        .map(|el| match el {
            ResolvedElement::Attr(name) => name.clone(),
            ResolvedElement::Index(idx) => format!("[{idx}]"),
        })
        .collect();
    format!("[{}]", parts.join(", "))
}

/// Build the projection trie from the parsed paths, resolving `#name` refs.
fn build_trie(
    paths: &[Vec<PathElement>],
    maps: &ExpressionMaps,
) -> Result<ProjNode, DynamoDbError> {
    let mut root = ProjNode::default();
    for path in paths {
        if path.is_empty() {
            continue;
        }
        let mut node = &mut root;
        for (i, element) in path.iter().enumerate() {
            match element {
                PathElement::Attribute(_) => {
                    // The first element must be an attribute; `resolve_element_name`
                    // rejects an index-start path, matching the prior behavior.
                    let name = if i == 0 {
                        resolve_element_name(element, maps)?
                    } else {
                        let PathElement::Attribute(raw) = element else {
                            unreachable!()
                        };
                        resolve_name_ref(raw, maps)?
                    };
                    node = node.attrs.entry(name.into_owned()).or_default();
                }
                PathElement::Index(idx) => {
                    if i == 0 {
                        return Err(DynamoDbError::ValidationException(
                            "Invalid expression: path cannot start with an index".to_owned(),
                        ));
                    }
                    node = node.indices.entry(*idx).or_default();
                }
            }
        }
        node.terminal = true;
    }
    Ok(root)
}

/// Project a single value against a trie node.
///
/// Returns `None` when nothing is selected (missing key, out-of-bounds index,
/// or a path that does not match the value's type), so the caller omits the
/// attribute entirely.
fn project_value(value: &AttributeValue, node: &ProjNode) -> Option<AttributeValue> {
    if node.terminal {
        return Some(value.clone());
    }

    if !node.attrs.is_empty() {
        let AttributeValue::M(map) = value else {
            return None;
        };
        let mut out = BTreeMap::new();
        for (name, child) in &node.attrs {
            if let Some(child_val) = map.get(name)
                && let Some(projected) = project_value(child_val, child)
            {
                out.insert(name.clone(), projected);
            }
        }
        return (!out.is_empty()).then_some(AttributeValue::M(out));
    }

    if !node.indices.is_empty() {
        let AttributeValue::L(list) = value else {
            return None;
        };
        let mut out = Vec::new();
        // BTreeMap iterates indices ascending, compacting the projected list.
        for (idx, child) in &node.indices {
            if let Some(element) = list.get(*idx)
                && let Some(projected) = project_value(element, child)
            {
                out.push(projected);
            }
        }
        return (!out.is_empty()).then_some(AttributeValue::L(out));
    }

    None
}

#[cfg(test)]
mod tests {
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
        detect_overlapping_paths(&paths, &maps)
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
}
