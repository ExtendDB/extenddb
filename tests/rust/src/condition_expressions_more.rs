// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition expression tests — functions, logical operators, and complex conditions.

use crate::test_base::*;

// ========== begins_with / contains / size ==========

#[tokio::test]
async fn condition_begins_with() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("name".into(), s("prefix_value"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("begins_with(#n, :prefix)")
        .expression_attribute_names("#n", "name")
        .expression_attribute_values(":prefix", s("prefix"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_contains_string() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("description".into(), s("hello world"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("contains(description, :sub)")
        .expression_attribute_values(":sub", s("world"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_size_function() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("data".into(), s("abc"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET #d = :new")
        .condition_expression("size(#d) = :sz")
        .expression_attribute_names("#d", "data")
        .expression_attribute_values(":sz", n(3))
        .expression_attribute_values(":new", s("abcd"))
        .send()
        .await
        .unwrap();
}

// ========== Logical operators (AND, OR, NOT) ==========

#[tokio::test]
async fn condition_and_both_true() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("a".into(), n(10));
    item.insert("b".into(), n(20));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("a = :av AND b = :bv")
        .expression_attribute_values(":av", n(10))
        .expression_attribute_values(":bv", n(20))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_and_one_false() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("a".into(), n(10));
    item.insert("b".into(), n(20));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    let err = c
        .delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("a = :av AND b = :bv")
        .expression_attribute_values(":av", n(10))
        .expression_attribute_values(":bv", n(999))
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}

#[tokio::test]
async fn condition_or_one_true() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("a".into(), n(10));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("a = :right OR a = :wrong")
        .expression_attribute_values(":right", n(10))
        .expression_attribute_values(":wrong", n(999))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_not() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("status".into(), s("active"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("NOT (#s = :blocked)")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":blocked", s("blocked"))
        .send()
        .await
        .unwrap();
}

// ========== Put with condition (idempotent insert) ==========

#[tokio::test]
async fn put_item_if_not_exists() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);

    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .condition_expression("attribute_not_exists(#h)")
        .expression_attribute_names("#h", HASH_KEY_S)
        .send()
        .await
        .unwrap();

    let err = c
        .put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .condition_expression("attribute_not_exists(#h)")
        .expression_attribute_names("#h", HASH_KEY_S)
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}

// ========== attribute_type ==========

#[tokio::test]
async fn condition_attribute_type() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("count".into(), n(42));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET #c = :new")
        .condition_expression("attribute_type(#c, :t)")
        .expression_attribute_names("#c", "count")
        .expression_attribute_values(":t", s("N"))
        .expression_attribute_values(":new", n(43))
        .send()
        .await
        .unwrap();
}

// ========== Nested condition with parentheses ==========

#[tokio::test]
async fn condition_nested_parentheses() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("a".into(), n(1));
    item.insert("b".into(), n(2));
    item.insert("c".into(), n(3));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("(a = :a AND b = :b) OR c = :c")
        .expression_attribute_values(":a", n(1))
        .expression_attribute_values(":b", n(2))
        .expression_attribute_values(":c", n(999))
        .send()
        .await
        .unwrap();
}

// ========== Field-vs-field comparison (both operands are paths) ==========

#[tokio::test]
async fn condition_field_vs_field_true() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("lo".into(), n(5));
    item.insert("hi".into(), n(10));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    // Both operands are document paths: `lo < hi` (5 < 10) holds, so the
    // delete proceeds. Exercises a comparison whose right-hand side is an
    // attribute reference rather than an expression-attribute-value.
    let key = get_key(table, &item);
    c.delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("lo < hi")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_field_vs_field_false() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("lo".into(), n(5));
    item.insert("hi".into(), n(10));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    // `hi < lo` (10 < 5) is false, so the guard must reject the write. Pins
    // that field-vs-field comparisons evaluate both sides as stored values,
    // not lexically.
    let key = get_key(table, &item);
    let err = c
        .delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("hi < lo")
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}
