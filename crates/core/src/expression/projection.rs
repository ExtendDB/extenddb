// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ProjectionExpression` parser and evaluator.
//!
//! Parses a comma-separated list of document paths and applies them to an item,
//! returning only the requested attributes.

use std::collections::BTreeMap;

use super::ast::PathElement;
use super::resolver::{ExpressionMaps, resolve_element_name, resolve_path};
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

/// Apply a projection to an item, returning only the requested attributes.
///
/// # Errors
///
/// Returns `ValidationException` for unresolvable `#name` references.
pub fn apply_projection(
    item: &Item,
    paths: &[Vec<PathElement>],
    maps: &ExpressionMaps,
) -> Result<Item, DynamoDbError> {
    let mut result = BTreeMap::new();

    for path in paths {
        if path.is_empty() {
            continue;
        }
        let top_name = resolve_element_name(&path[0], maps)?;
        if path.len() == 1 {
            // Top-level attribute
            if let Some(val) = item.get(top_name.as_ref()) {
                result.insert(top_name.into_owned(), val.clone());
            }
        } else {
            // Nested path — resolve the value and insert at the top level
            // with the nested structure preserved
            if let Some(val) = resolve_path(path, item, maps)? {
                insert_nested(&mut result, path, maps, val)?;
            }
        }
    }

    Ok(result)
}

/// Insert a value at a nested path in the result item, creating intermediate
/// maps/lists as needed.
///
/// DynamoDB projection semantics for list indices: projecting `mylist[N]`
/// returns `{"mylist": [value]}` — a single-element list wrapping the value.
fn insert_nested(
    result: &mut Item,
    path: &[PathElement],
    maps: &ExpressionMaps,
    value: &AttributeValue,
) -> Result<(), DynamoDbError> {
    if path.is_empty() {
        return Ok(());
    }

    let top_name = resolve_element_name(&path[0], maps)?.into_owned();

    if path.len() == 1 {
        result.insert(top_name, value.clone());
        return Ok(());
    }

    if path.len() == 2 {
        if let PathElement::Index(_) = &path[1] {
            // mylist[N] → {"mylist": [value]}
            result.insert(top_name, AttributeValue::L(vec![value.clone()]));
            return Ok(());
        }
    }

    // For nested paths, we need to build the intermediate structure
    let entry = result
        .entry(top_name)
        .or_insert_with(|| AttributeValue::M(BTreeMap::new()));

    let mut current = entry;
    for (i, element) in path[1..path.len() - 1].iter().enumerate() {
        match element {
            PathElement::Attribute(name) => {
                let resolved = super::resolver::resolve_name_ref(name, maps)?;
                if let AttributeValue::M(map) = current {
                    current = map
                        .entry(resolved.into_owned())
                        .or_insert_with(|| AttributeValue::M(BTreeMap::new()));
                } else {
                    return Ok(());
                }
            }
            PathElement::Index(_) => {
                // Intermediate list index: build the remaining nested structure
                // and wrap it in a single-element list.
                // e.g., for path a.b[0].c with value "target":
                //   remaining path after [0] is [.c]
                //   build {"c": "target"} then wrap → [{"c": "target"}]
                // i is the index within path[1..len-1], so the actual path
                // index is 1 + i. Remaining elements start at 1 + i + 1.
                let remaining = &path[1 + i + 1..];
                let inner = build_remaining_structure(remaining, maps, value)?;
                *current = AttributeValue::L(vec![inner]);
                return Ok(());
            }
        }
    }

    // Set the final value
    match &path[path.len() - 1] {
        PathElement::Attribute(name) => {
            let resolved = super::resolver::resolve_name_ref(name, maps)?;
            if let AttributeValue::M(map) = current {
                map.insert(resolved.into_owned(), value.clone());
            }
        }
        PathElement::Index(_) => {
            // List index at leaf of a deeper path — wrap in single-element list
            *current = AttributeValue::L(vec![value.clone()]);
        }
    }

    Ok(())
}

/// Build a nested attribute structure from a remaining path suffix and a leaf value.
///
/// For path elements `[Attribute("c"), Attribute("d")]` and value `"x"`,
/// produces `{"c": {"d": "x"}}`.
/// For path elements `[Index(_), Attribute("c")]` and value `"x"`,
/// produces `[{"c": "x"}]`.
fn build_remaining_structure(
    remaining: &[PathElement],
    maps: &ExpressionMaps,
    value: &AttributeValue,
) -> Result<AttributeValue, DynamoDbError> {
    if remaining.is_empty() {
        return Ok(value.clone());
    }

    match &remaining[0] {
        PathElement::Attribute(name) => {
            let resolved = super::resolver::resolve_name_ref(name, maps)?;
            let inner = build_remaining_structure(&remaining[1..], maps, value)?;
            let mut map = BTreeMap::new();
            map.insert(resolved.into_owned(), inner);
            Ok(AttributeValue::M(map))
        }
        PathElement::Index(_) => {
            let inner = build_remaining_structure(&remaining[1..], maps, value)?;
            Ok(AttributeValue::L(vec![inner]))
        }
    }
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

    #[test]
    fn project_intermediate_list_index_preserves_nested_structure() {
        // Path: a.b[0].c where a.b = [{"c": "target", "d": "other"}]
        // Expected: {"a": {"b": [{"c": "target"}]}}
        let mut inner_map = BTreeMap::new();
        inner_map.insert("c".into(), AttributeValue::S("target".into()));
        inner_map.insert("d".into(), AttributeValue::S("other".into()));

        let mut a_map = BTreeMap::new();
        a_map.insert(
            "b".into(),
            AttributeValue::L(vec![AttributeValue::M(inner_map)]),
        );

        let mut item = BTreeMap::new();
        item.insert("pk".into(), AttributeValue::S("k1".into()));
        item.insert("a".into(), AttributeValue::M(a_map));

        let result = project("a.b[0].c", &item, HashMap::new()).unwrap();

        // Should have {"a": {"b": [{"c": "target"}]}}
        let a_val = result.get("a").expect("should have 'a'");
        if let AttributeValue::M(a) = a_val {
            let b_val = a.get("b").expect("should have 'b'");
            if let AttributeValue::L(list) = b_val {
                assert_eq!(list.len(), 1);
                if let AttributeValue::M(elem) = &list[0] {
                    assert_eq!(elem.get("c"), Some(&AttributeValue::S("target".into())));
                    assert!(!elem.contains_key("d")); // "d" not projected
                } else {
                    panic!("Expected list element to be a Map, got {:?}", list[0]);
                }
            } else {
                panic!("Expected 'b' to be a List, got {b_val:?}");
            }
        } else {
            panic!("Expected 'a' to be a Map, got {a_val:?}");
        }
    }
}
