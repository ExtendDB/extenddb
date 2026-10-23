// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::resolver::ExpressionMaps;
use crate::expression::tokenizer::tokenize;
use crate::expression::update_parser::parse_update;
use crate::types::{Projection, ProjectionType};
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

/// Vector-validated variants. These exist to prove that validation follows the
/// evaluated image rather than the expression form, so no SET syntax can
/// smuggle a malformed vector past the write path.
mod vector_validated {
    use super::*;
    use crate::types::{ScalarAttributeType, VectorIndexKeyInfo};

    const DIMS: u32 = 3;

    fn index() -> VectorIndexKeyInfo {
        VectorIndexKeyInfo {
            index_name: "vidx".to_owned(),
            dimensions: DIMS,
            vector_attribute_name: "emb".to_owned(),
            search_schema: Vec::new(),
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        }
    }

    fn num_list(values: &[&str]) -> AttributeValue {
        AttributeValue::L(
            values
                .iter()
                .map(|v| AttributeValue::N((*v).to_owned()))
                .collect(),
        )
    }

    fn apply_validated(
        expr_str: &str,
        item: &mut BTreeMap<String, AttributeValue>,
        values: HashMap<String, AttributeValue>,
    ) -> Result<(), DynamoDbError> {
        let tokens = tokenize(expr_str)?;
        let actions = parse_update(&tokens)?;
        let maps = ExpressionMaps::new(HashMap::new(), values);
        apply_update_validated(&actions, item, &maps, &[index()], &[])
    }

    fn base_item() -> BTreeMap<String, AttributeValue> {
        let mut item = BTreeMap::new();
        item.insert("pk".into(), AttributeValue::S("k1".into()));
        item
    }

    /// Control: the form the old expression-matching check did cover.
    #[test]
    fn bare_placeholder_wrong_dimension_is_rejected() {
        let mut values = HashMap::new();
        values.insert("v".into(), num_list(&["1", "2"]));
        let err = apply_validated("SET emb = :v", &mut base_item(), values).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m) if m.contains("Expected: 3, Actual: 2")),
            "unexpected error: {err:?}"
        );
    }

    /// `list_append` was invisible to the old check: the value it produces
    /// exists only after evaluation. Two 2-element lists concatenate to 4,
    /// which must be rejected against a 3-dimension index.
    #[test]
    fn list_append_wrong_dimension_is_rejected() {
        let mut values = HashMap::new();
        values.insert("a".into(), num_list(&["1", "2"]));
        values.insert("b".into(), num_list(&["3", "4"]));
        let err =
            apply_validated("SET emb = list_append(:a, :b)", &mut base_item(), values).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m) if m.contains("Expected: 3, Actual: 4")),
            "unexpected error: {err:?}"
        );
    }

    /// `list_append` reaching the declared dimension must still be accepted,
    /// so the guard discriminates on the value rather than on the syntax.
    #[test]
    fn list_append_correct_dimension_is_accepted() {
        let mut values = HashMap::new();
        values.insert("a".into(), num_list(&["1", "2"]));
        values.insert("b".into(), num_list(&["3"]));
        let mut item = base_item();
        apply_validated("SET emb = list_append(:a, :b)", &mut item, values).unwrap();
        assert_eq!(item.get("emb"), Some(&num_list(&["1", "2", "3"])));
    }

    /// `if_not_exists` was also invisible: on a fresh item it resolves to the
    /// placeholder, which here is the wrong dimension.
    #[test]
    fn if_not_exists_wrong_dimension_is_rejected() {
        let mut values = HashMap::new();
        values.insert("v".into(), num_list(&["1", "2", "3", "4"]));
        let err = apply_validated("SET emb = if_not_exists(emb, :v)", &mut base_item(), values)
            .unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m) if m.contains("Actual: 4")),
            "unexpected error: {err:?}"
        );
    }

    /// Copying another attribute never mentions a placeholder at all, so the
    /// old check saw nothing to validate.
    #[test]
    fn attribute_copy_of_non_vector_is_rejected() {
        let mut item = base_item();
        item.insert("other".into(), AttributeValue::S("not-a-vector".into()));
        let err = apply_validated("SET emb = other", &mut item, HashMap::new()).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m) if m.contains("Invalid type for parameter emb")),
            "unexpected error: {err:?}"
        );
    }

    /// Removing the vector attribute leaves no vector in the image, which is
    /// legal: the validator is presence-conditional and must not demand one.
    #[test]
    fn removing_the_vector_attribute_is_allowed() {
        let mut item = base_item();
        item.insert("emb".into(), num_list(&["1", "2", "3"]));
        apply_validated("REMOVE emb", &mut item, HashMap::new()).unwrap();
        assert!(!item.contains_key("emb"));
    }

    /// An update that does not touch the vector attribute at all must pass
    /// even when the stored vector is absent.
    #[test]
    fn unrelated_update_is_allowed() {
        let mut values = HashMap::new();
        values.insert("v".into(), AttributeValue::S("x".into()));
        let mut item = base_item();
        apply_validated("SET label = :v", &mut item, values).unwrap();
        assert_eq!(item.get("label"), Some(&AttributeValue::S("x".into())));
    }

    /// Appending to an already-full vector overflows the declared dimension.
    /// `apply_update` alone succeeds here (both operands are valid lists), so
    /// only image-level validation can catch it. This also pins the
    /// pre-update-snapshot semantics: the RHS reads the stored vector.
    #[test]
    fn appending_to_a_full_vector_overflows_and_is_rejected() {
        let mut values = HashMap::new();
        values.insert("one".into(), num_list(&["4"]));
        let mut item = base_item();
        item.insert("emb".into(), num_list(&["1", "2", "3"]));
        let err =
            apply_validated("SET emb = list_append(emb, :one)", &mut item, values).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m) if m.contains("Expected: 3, Actual: 4")),
            "unexpected error: {err:?}"
        );
    }

    /// Search-schema attributes are validated on the image too, not just the
    /// vector itself.
    #[test]
    fn search_schema_attribute_wrong_type_is_rejected() {
        let index = VectorIndexKeyInfo {
            index_name: "vidx".to_owned(),
            dimensions: DIMS,
            vector_attribute_name: "emb".to_owned(),
            search_schema: vec![crate::types::SearchSchemaElement {
                attribute_name: "tenant".to_owned(),
                element_type: crate::types::SearchSchemaElementType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        };
        let defs = [crate::types::AttributeDefinition {
            attribute_name: "tenant".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];
        let mut values = HashMap::new();
        values.insert("t".into(), AttributeValue::N("1".into()));
        let tokens = tokenize("SET tenant = :t").unwrap();
        let actions = parse_update(&tokens).unwrap();
        let maps = ExpressionMaps::new(HashMap::new(), values);
        let mut item = base_item();
        let err = apply_update_validated(&actions, &mut item, &maps, &[index], &defs).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(_)),
            "unexpected error: {err:?}"
        );
    }

    /// The type-mismatch message, byte-exact against the service (measured
    /// 2026-08-14, probe table vixdelta-1786706774). Note the periods rather
    /// than colons, and the trailing IndexName clause.
    #[test]
    fn search_schema_type_mismatch_message_matches_the_service_exactly() {
        let index = VectorIndexKeyInfo {
            index_name: "vidx".to_owned(),
            dimensions: DIMS,
            vector_attribute_name: "emb".to_owned(),
            search_schema: vec![crate::types::SearchSchemaElement {
                attribute_name: "tenant".to_owned(),
                element_type: crate::types::SearchSchemaElementType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        };
        let defs = [crate::types::AttributeDefinition {
            attribute_name: "tenant".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];
        let mut values = HashMap::new();
        values.insert("t".into(), AttributeValue::N("1".into()));
        let tokens = tokenize("SET tenant = :t").unwrap();
        let actions = parse_update(&tokens).unwrap();
        let maps = ExpressionMaps::new(HashMap::new(), values);
        let mut item = base_item();
        let err = apply_update_validated(&actions, &mut item, &maps, &[index], &defs).unwrap_err();
        assert_eq!(
            format!("{err}"),
            "One or more parameter values were invalid. Attribute 'tenant' type mismatch. \
             Expected: S, Actual: N. IndexName: vidx"
        );
    }

    /// An update that does not touch the mistyped attribute passes: the
    /// service validates what the write CHANGES, not the whole stored image
    /// (measured 2026-08-14: a pre-existing invalid value does not poison
    /// unrelated updates). Whole-image validation would make items the
    /// backfill deliberately skipped permanently un-updatable.
    #[test]
    fn unrelated_update_is_not_rejected_by_a_pre_existing_mistyped_value() {
        let index = VectorIndexKeyInfo {
            index_name: "vidx".to_owned(),
            dimensions: DIMS,
            vector_attribute_name: "emb".to_owned(),
            search_schema: vec![crate::types::SearchSchemaElement {
                attribute_name: "tenant".to_owned(),
                element_type: crate::types::SearchSchemaElementType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        };
        let defs = [crate::types::AttributeDefinition {
            attribute_name: "tenant".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];
        // The stored item already carries tenant as N: wrong for the declared
        // S, present before this update runs.
        let mut item = base_item();
        item.insert("tenant".into(), AttributeValue::N("7".into()));

        let mut values = HashMap::new();
        values.insert("v".into(), AttributeValue::S("x".into()));
        let tokens = tokenize("SET unrelated = :v").unwrap();
        let actions = parse_update(&tokens).unwrap();
        let maps = ExpressionMaps::new(HashMap::new(), values);
        apply_update_validated(
            &actions,
            &mut item,
            &maps,
            std::slice::from_ref(&index),
            &defs,
        )
        .expect("an unrelated update must not re-validate the untouched value");

        // Internal semantics (not a service measurement): re-setting the SAME
        // invalid value is also unchanged, so it passes too.
        let mut values = HashMap::new();
        values.insert("t".into(), AttributeValue::N("7".into()));
        let tokens = tokenize("SET tenant = :t").unwrap();
        let actions = parse_update(&tokens).unwrap();
        let maps = ExpressionMaps::new(HashMap::new(), values);
        apply_update_validated(
            &actions,
            &mut item,
            &maps,
            std::slice::from_ref(&index),
            &defs,
        )
        .expect("re-setting the identical value is not a change");

        // Control: actually CHANGING the mistyped value to another wrong-typed
        // value is rejected, so the two allowances above discriminate.
        let mut values = HashMap::new();
        values.insert("t".into(), AttributeValue::N("8".into()));
        let tokens = tokenize("SET tenant = :t").unwrap();
        let actions = parse_update(&tokens).unwrap();
        let maps = ExpressionMaps::new(HashMap::new(), values);
        apply_update_validated(&actions, &mut item, &maps, &[index], &defs)
            .expect_err("changing to another wrong-typed value must still reject");
    }
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
