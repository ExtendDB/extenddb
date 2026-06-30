// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Update expression tests — return values, combined expressions, list index operations.
//! Mirrors Java `UpdateExpressionTests` (continued from `update_expressions.rs`).

use crate::test_base::*;

// ─── Return Values ───────────────────────────────────────────

#[tokio::test]
async fn return_values_updated_old() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("v".into(), n(1));
    item.insert("unchanged".into(), s("same"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    let resp = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET v = :new")
        .expression_attribute_values(":new", n(2))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedOld)
        .send()
        .await
        .unwrap();

    let attrs = resp
        .attributes()
        .expect("UPDATED_OLD should return attributes");
    assert_eq!(attrs.get("v").unwrap(), &n(1));
    assert!(
        attrs.get("unchanged").is_none(),
        "UPDATED_OLD should not include unchanged attributes"
    );
}

#[tokio::test]
async fn return_values_updated_new() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("v".into(), n(1));
    item.insert("unchanged".into(), s("same"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    let resp = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET v = :new")
        .expression_attribute_values(":new", n(2))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
        .send()
        .await
        .unwrap();

    let attrs = resp
        .attributes()
        .expect("UPDATED_NEW should return attributes");
    assert_eq!(attrs.get("v").unwrap(), &n(2));
    assert!(
        attrs.get("unchanged").is_none(),
        "UPDATED_NEW should not include unchanged attributes"
    );
}

// ─── Combined Expressions ────────────────────────────────────

#[tokio::test]
async fn add_and_delete_combined() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("score".into(), n(100));
    item.insert("tags".into(), ss(&["a", "b", "c"]));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression("ADD score :inc DELETE tags :rem")
        .expression_attribute_values(":inc", n(50))
        .expression_attribute_values(":rem", ss(&["b"]))
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let got = resp.item().unwrap();
    assert_eq!(got.get("score").unwrap(), &n(150));
    if let aws_sdk_dynamodb::types::AttributeValue::Ss(vals) = got.get("tags").unwrap() {
        let set: std::collections::HashSet<_> = vals.iter().collect();
        assert_eq!(set.len(), 2);
        assert!(!set.contains(&"b".to_string()));
    } else {
        panic!("Expected SS type");
    }
}

#[tokio::test]
async fn set_add_remove_delete_all_four() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("counter".into(), n(10));
    item.insert("tags".into(), ss(&["x", "y"]));
    item.insert("old_field".into(), s("bye"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression("SET new_field = :nf ADD #c :inc REMOVE old_field DELETE tags :rem")
        .expression_attribute_names("#c", "counter")
        .expression_attribute_values(":nf", s("hello"))
        .expression_attribute_values(":inc", n(5))
        .expression_attribute_values(":rem", ss(&["x"]))
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let got = resp.item().unwrap();
    assert_eq!(got.get("new_field").unwrap(), &s("hello"));
    assert_eq!(got.get("counter").unwrap(), &n(15));
    assert!(got.get("old_field").is_none());
    if let aws_sdk_dynamodb::types::AttributeValue::Ss(vals) = got.get("tags").unwrap() {
        assert_eq!(vals.len(), 1);
        assert!(vals.contains(&"y".to_string()));
    } else {
        panic!("Expected SS type");
    }
}

// ─── SET with list index ─────────────────────────────────────

#[tokio::test]
async fn set_list_element_by_index() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("items".into(), list_val(vec![s("x"), s("y"), s("z")]));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression("SET #i[1] = :v")
        .expression_attribute_names("#i", "items")
        .expression_attribute_values(":v", s("replaced"))
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let got = resp.item().unwrap();
    if let aws_sdk_dynamodb::types::AttributeValue::L(vals) = got.get("items").unwrap() {
        assert_eq!(vals[0], s("x"));
        assert_eq!(vals[1], s("replaced"));
        assert_eq!(vals[2], s("z"));
    } else {
        panic!("Expected L type");
    }
}

// ─── REMOVE list element ─────────────────────────────────────

#[tokio::test]
async fn remove_list_element_by_index() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("items".into(), list_val(vec![s("a"), s("b"), s("c")]));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression("REMOVE #i[1]")
        .expression_attribute_names("#i", "items")
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let got = resp.item().unwrap();
    if let aws_sdk_dynamodb::types::AttributeValue::L(vals) = got.get("items").unwrap() {
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0], s("a"));
        assert_eq!(vals[1], s("c"));
    } else {
        panic!("Expected L type");
    }
}
