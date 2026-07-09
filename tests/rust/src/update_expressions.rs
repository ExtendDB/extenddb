// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Update expression edge case tests — SET, REMOVE, ADD, DELETE with various types.
//! Mirrors Java `UpdateExpressionTests`.

use crate::test_base::*;

// ─── SET Expression ──────────────────────────────────────────

#[tokio::test]
async fn set_arithmetic_decrement() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("counter".into(), n(15));
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
        .update_expression("SET #c = #c - :dec")
        .expression_attribute_names("#c", "counter")
        .expression_attribute_values(":dec", n(3))
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
    assert_eq!(resp.item().unwrap().get("counter").unwrap(), &n(12));
}

#[tokio::test]
async fn set_list_append() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("tags".into(), list_val(vec![s("a"), s("b")]));
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
        .update_expression("SET tags = list_append(tags, :new)")
        .expression_attribute_values(":new", list_val(vec![s("c")]))
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
    if let aws_sdk_dynamodb::types::AttributeValue::L(vals) = got.get("tags").unwrap() {
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[2], s("c"));
    } else {
        panic!("Expected L type");
    }
}

#[tokio::test]
async fn set_list_prepend() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("tags".into(), list_val(vec![s("b"), s("c")]));
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
        .update_expression("SET tags = list_append(:new, tags)")
        .expression_attribute_values(":new", list_val(vec![s("a")]))
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
    if let aws_sdk_dynamodb::types::AttributeValue::L(vals) = got.get("tags").unwrap() {
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], s("a"));
    } else {
        panic!("Expected L type");
    }
}

#[tokio::test]
async fn set_nested_attribute() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    let mut inner = std::collections::HashMap::new();
    inner.insert("level".into(), n(1));
    item.insert("meta".into(), map_val(inner));
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
        .update_expression("SET meta.#l = :v")
        .expression_attribute_names("#l", "level")
        .expression_attribute_values(":v", n(2))
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
    if let aws_sdk_dynamodb::types::AttributeValue::M(m) = got.get("meta").unwrap() {
        assert_eq!(m.get("level").unwrap(), &n(2));
    } else {
        panic!("Expected M type");
    }
}

// ─── REMOVE Expression ───────────────────────────────────────

#[tokio::test]
async fn remove_multiple_attributes() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("a".into(), s("1"));
    item.insert("b".into(), s("2"));
    item.insert("c".into(), s("3"));
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
        .update_expression("REMOVE a, c")
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
    assert!(got.get("a").is_none());
    assert!(got.get("b").is_some());
    assert!(got.get("c").is_none());
}

#[tokio::test]
async fn remove_non_existent_attribute_succeeds() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    // Should not error.
    c.update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("REMOVE nonExistent")
        .send()
        .await
        .unwrap();
}

// ─── ADD Expression ──────────────────────────────────────────

#[tokio::test]
async fn add_creates_new_number_attribute() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);
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
        .update_expression("ADD newNum :val")
        .expression_attribute_values(":val", n(42))
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
    assert_eq!(resp.item().unwrap().get("newNum").unwrap(), &n(42));
}

#[tokio::test]
async fn add_to_number_set() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("nums".into(), ns(&[1, 2, 3]));
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
        .update_expression("ADD nums :v")
        .expression_attribute_values(":v", ns(&[4, 5]))
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
    if let aws_sdk_dynamodb::types::AttributeValue::Ns(vals) =
        resp.item().unwrap().get("nums").unwrap()
    {
        let set: std::collections::HashSet<&String> = vals.iter().collect();
        assert_eq!(set.len(), 5);
    } else {
        panic!("Expected NS type");
    }
}

// ─── DELETE Expression ───────────────────────────────────────

#[tokio::test]
async fn delete_from_number_set() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("nums".into(), ns(&[10, 20, 30]));
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
        .update_expression("DELETE nums :v")
        .expression_attribute_values(":v", ns(&[20]))
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
    if let aws_sdk_dynamodb::types::AttributeValue::Ns(vals) =
        resp.item().unwrap().get("nums").unwrap()
    {
        let set: std::collections::HashSet<&String> = vals.iter().collect();
        assert_eq!(set.len(), 2);
        assert!(!set.contains(&"20".to_string()));
    } else {
        panic!("Expected NS type");
    }
}
