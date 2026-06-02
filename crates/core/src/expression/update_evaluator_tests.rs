// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::resolver::ExpressionMaps;
use crate::expression::tokenizer::tokenize;
use crate::expression::update_parser::parse_update;
use std::collections::HashMap;

fn apply(
    expr_str: &str,
    item: &mut BTreeMap<String, AttributeValue>,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
) -> Result<(), DynamoDbError> {
    let tokens = tokenize(expr_str)?;
    let actions = parse_update(&tokens)?;
    let maps = ExpressionMaps::new(names, values);
    apply_update(&actions, item, &maps)
}

#[test]
fn set_new_attribute() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("key1".into()));
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("hello".into()));
    apply("SET greeting = :v", &mut item, HashMap::new(), values).unwrap();
    assert_eq!(
        item.get("greeting"),
        Some(&AttributeValue::S("hello".into()))
    );
}

#[test]
fn set_overwrite_attribute() {
    let mut item = BTreeMap::new();
    item.insert("name".into(), AttributeValue::S("old".into()));
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("new".into()));
    apply("SET name = :v", &mut item, HashMap::new(), values).unwrap();
    assert_eq!(item.get("name"), Some(&AttributeValue::S("new".into())));
}

#[test]
fn set_arithmetic_add() {
    let mut item = BTreeMap::new();
    item.insert("counter".into(), AttributeValue::N("10".into()));
    let mut values = HashMap::new();
    values.insert("inc".into(), AttributeValue::N("5".into()));
    apply(
        "SET counter = counter + :inc",
        &mut item,
        HashMap::new(),
        values,
    )
    .unwrap();
    assert_eq!(item.get("counter"), Some(&AttributeValue::N("15".into())));
}

#[test]
fn set_arithmetic_sub() {
    let mut item = BTreeMap::new();
    item.insert("stock".into(), AttributeValue::N("100".into()));
    let mut values = HashMap::new();
    values.insert("dec".into(), AttributeValue::N("3".into()));
    apply(
        "SET stock = stock - :dec",
        &mut item,
        HashMap::new(),
        values,
    )
    .unwrap();
    assert_eq!(item.get("stock"), Some(&AttributeValue::N("97".into())));
}

#[test]
fn set_if_not_exists_absent() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("key1".into()));
    let mut values = HashMap::new();
    values.insert("d".into(), AttributeValue::N("0".into()));
    apply(
        "SET counter = if_not_exists(counter, :d)",
        &mut item,
        HashMap::new(),
        values,
    )
    .unwrap();
    assert_eq!(item.get("counter"), Some(&AttributeValue::N("0".into())));
}

#[test]
fn set_if_not_exists_present() {
    let mut item = BTreeMap::new();
    item.insert("counter".into(), AttributeValue::N("42".into()));
    let mut values = HashMap::new();
    values.insert("d".into(), AttributeValue::N("0".into()));
    apply(
        "SET counter = if_not_exists(counter, :d)",
        &mut item,
        HashMap::new(),
        values,
    )
    .unwrap();
    assert_eq!(item.get("counter"), Some(&AttributeValue::N("42".into())));
}

#[test]
fn remove_attribute() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("key1".into()));
    item.insert("temp".into(), AttributeValue::S("gone".into()));
    apply("REMOVE temp", &mut item, HashMap::new(), HashMap::new()).unwrap();
    assert!(!item.contains_key("temp"));
}

#[test]
fn remove_nonexistent_is_noop() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("key1".into()));
    let before = item.clone();
    apply("REMOVE missing", &mut item, HashMap::new(), HashMap::new()).unwrap();
    assert_eq!(item, before);
}

#[test]
fn add_to_number() {
    let mut item = BTreeMap::new();
    item.insert("counter".into(), AttributeValue::N("10".into()));
    let mut values = HashMap::new();
    values.insert("inc".into(), AttributeValue::N("5".into()));
    apply("ADD counter :inc", &mut item, HashMap::new(), values).unwrap();
    assert_eq!(item.get("counter"), Some(&AttributeValue::N("15".into())));
}

#[test]
fn add_creates_number_if_absent() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("key1".into()));
    let mut values = HashMap::new();
    values.insert("inc".into(), AttributeValue::N("1".into()));
    apply("ADD counter :inc", &mut item, HashMap::new(), values).unwrap();
    assert_eq!(item.get("counter"), Some(&AttributeValue::N("1".into())));
}

#[test]
fn add_to_string_set() {
    let mut item = BTreeMap::new();
    let mut set = BTreeSet::new();
    set.insert("red".into());
    item.insert("colors".into(), AttributeValue::SS(set));
    let mut add_set = BTreeSet::new();
    add_set.insert("blue".into());
    let mut values = HashMap::new();
    values.insert("c".into(), AttributeValue::SS(add_set));
    apply("ADD colors :c", &mut item, HashMap::new(), values).unwrap();
    if let Some(AttributeValue::SS(s)) = item.get("colors") {
        assert!(s.contains("red"));
        assert!(s.contains("blue"));
    } else {
        panic!("Expected SS");
    }
}

#[test]
fn delete_from_string_set() {
    let mut item = BTreeMap::new();
    let mut set = BTreeSet::new();
    set.insert("red".into());
    set.insert("blue".into());
    set.insert("green".into());
    item.insert("colors".into(), AttributeValue::SS(set));
    let mut rm_set = BTreeSet::new();
    rm_set.insert("blue".into());
    let mut values = HashMap::new();
    values.insert("rm".into(), AttributeValue::SS(rm_set));
    apply("DELETE colors :rm", &mut item, HashMap::new(), values).unwrap();
    if let Some(AttributeValue::SS(s)) = item.get("colors") {
        assert!(s.contains("red"));
        assert!(s.contains("green"));
        assert!(!s.contains("blue"));
    } else {
        panic!("Expected SS");
    }
}

#[test]
fn delete_all_elements_removes_attribute() {
    let mut item = BTreeMap::new();
    let mut set = BTreeSet::new();
    set.insert("only".into());
    item.insert("tags".into(), AttributeValue::SS(set));
    let mut rm_set = BTreeSet::new();
    rm_set.insert("only".into());
    let mut values = HashMap::new();
    values.insert("rm".into(), AttributeValue::SS(rm_set));
    apply("DELETE tags :rm", &mut item, HashMap::new(), values).unwrap();
    assert!(!item.contains_key("tags"));
}

#[test]
fn set_nested_path() {
    let mut inner = BTreeMap::new();
    inner.insert("city".into(), AttributeValue::S("old".into()));
    let mut item = BTreeMap::new();
    item.insert("address".into(), AttributeValue::M(inner));
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("NYC".into()));
    apply("SET address.city = :v", &mut item, HashMap::new(), values).unwrap();
    if let Some(AttributeValue::M(m)) = item.get("address") {
        assert_eq!(m.get("city"), Some(&AttributeValue::S("NYC".into())));
    } else {
        panic!("Expected M");
    }
}

#[test]
fn set_list_index() {
    let mut item = BTreeMap::new();
    item.insert(
        "items".into(),
        AttributeValue::L(vec![
            AttributeValue::S("a".into()),
            AttributeValue::S("b".into()),
        ]),
    );
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("x".into()));
    apply("SET items[0] = :v", &mut item, HashMap::new(), values).unwrap();
    if let Some(AttributeValue::L(l)) = item.get("items") {
        assert_eq!(l[0], AttributeValue::S("x".into()));
        assert_eq!(l[1], AttributeValue::S("b".into()));
    } else {
        panic!("Expected L");
    }
}

#[test]
fn list_append_function() {
    let mut item = BTreeMap::new();
    item.insert(
        "tags".into(),
        AttributeValue::L(vec![AttributeValue::S("a".into())]),
    );
    let mut values = HashMap::new();
    values.insert(
        "new".into(),
        AttributeValue::L(vec![AttributeValue::S("b".into())]),
    );
    apply(
        "SET tags = list_append(tags, :new)",
        &mut item,
        HashMap::new(),
        values,
    )
    .unwrap();
    if let Some(AttributeValue::L(l)) = item.get("tags") {
        assert_eq!(l.len(), 2);
        assert_eq!(l[1], AttributeValue::S("b".into()));
    } else {
        panic!("Expected L");
    }
}

#[test]
fn name_ref_in_update() {
    let mut item = BTreeMap::new();
    item.insert("status".into(), AttributeValue::S("old".into()));
    let mut names = HashMap::new();
    names.insert("s".into(), "status".into());
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("new".into()));
    apply("SET #s = :v", &mut item, names, values).unwrap();
    assert_eq!(item.get("status"), Some(&AttributeValue::S("new".into())));
}

#[test]
fn set_list_index_zero_on_empty_list() {
    let mut item = BTreeMap::new();
    item.insert("mylist".into(), AttributeValue::L(vec![]));
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("hello".into()));
    apply("SET mylist[0] = :v", &mut item, HashMap::new(), values).unwrap();
    assert_eq!(
        item.get("mylist"),
        Some(&AttributeValue::L(vec![AttributeValue::S("hello".into())]))
    );
}

#[test]
fn set_list_index_beyond_bounds_appends() {
    let mut item = BTreeMap::new();
    item.insert(
        "mylist".into(),
        AttributeValue::L(vec![
            AttributeValue::S("a".into()),
            AttributeValue::S("b".into()),
            AttributeValue::S("c".into()),
        ]),
    );
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("appended".into()));
    apply("SET mylist[99] = :v", &mut item, HashMap::new(), values).unwrap();
    let list = match item.get("mylist") {
        Some(AttributeValue::L(l)) => l,
        _ => panic!("expected list"),
    };
    assert_eq!(list.len(), 4);
    assert_eq!(list[3], AttributeValue::S("appended".into()));
}

#[test]
fn set_list_index_within_bounds_replaces() {
    let mut item = BTreeMap::new();
    item.insert(
        "mylist".into(),
        AttributeValue::L(vec![
            AttributeValue::S("a".into()),
            AttributeValue::S("b".into()),
        ]),
    );
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("replaced".into()));
    apply("SET mylist[1] = :v", &mut item, HashMap::new(), values).unwrap();
    let list = match item.get("mylist") {
        Some(AttributeValue::L(l)) => l,
        _ => panic!("expected list"),
    };
    assert_eq!(list[1], AttributeValue::S("replaced".into()));
}

#[test]
fn set_intermediate_map_path_missing_fails() {
    let mut item = BTreeMap::new();
    let mut inner = BTreeMap::new();
    inner.insert("x".into(), AttributeValue::S("exists".into()));
    item.insert("a".into(), AttributeValue::M(inner));
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("hello".into()));
    let result = apply("SET a.b.c = :v", &mut item, HashMap::new(), values);
    assert!(result.is_err());
}

#[test]
fn set_snapshot_semantics_second_clause_reads_old_value() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("key1".into()));
    item.insert("a".into(), AttributeValue::S("OLD".into()));
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::S("NEW".into()));
    apply("SET a = :v, b = a", &mut item, HashMap::new(), values).unwrap();
    assert_eq!(item.get("a"), Some(&AttributeValue::S("NEW".into())));
    assert_eq!(item.get("b"), Some(&AttributeValue::S("OLD".into())));
}

#[test]
fn set_parenthesised_arithmetic() {
    let mut item = BTreeMap::new();
    item.insert("pk".into(), AttributeValue::S("key1".into()));
    item.insert("c".into(), AttributeValue::N("10".into()));
    let mut values = HashMap::new();
    values.insert("v".into(), AttributeValue::N("3".into()));
    apply("SET c = (c - :v)", &mut item, HashMap::new(), values).unwrap();
    assert_eq!(item.get("c"), Some(&AttributeValue::N("7".into())));
}
