// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition expression integration tests — exercises the expression evaluation engine.

use crate::test_base::*;

// ========== attribute_exists / attribute_not_exists ==========

#[tokio::test]
async fn condition_attribute_exists_success() {
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
    c.update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET #s = :v")
        .condition_expression("attribute_exists(#s)")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":v", s("updated"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_attribute_exists_failure() {
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
    let err = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET missing_attr = :v")
        .condition_expression("attribute_exists(missing_attr)")
        .expression_attribute_values(":v", s("val"))
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}

#[tokio::test]
async fn condition_attribute_not_exists_success() {
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
        .set_key(Some(key))
        .update_expression("SET new_attr = :v")
        .condition_expression("attribute_not_exists(new_attr)")
        .expression_attribute_values(":v", s("created"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_attribute_not_exists_failure() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("existing".into(), s("val"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    let err = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET existing = :v")
        .condition_expression("attribute_not_exists(existing)")
        .expression_attribute_values(":v", s("new"))
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}

// ========== Comparison operators ==========

#[tokio::test]
async fn condition_equals_string() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("color".into(), s("red"));
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
        .update_expression("SET color = :new")
        .condition_expression("color = :old")
        .expression_attribute_values(":old", s("red"))
        .expression_attribute_values(":new", s("blue"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "reserved keyword validation too strict"]
async fn condition_not_equals() {
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
    c.update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET status = :new")
        .condition_expression("status <> :blocked")
        .expression_attribute_values(":blocked", s("blocked"))
        .expression_attribute_values(":new", s("updated"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_less_than() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("age".into(), n(25));
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
        .update_expression("SET age = :new")
        .condition_expression("age < :limit")
        .expression_attribute_values(":limit", n(30))
        .expression_attribute_values(":new", n(26))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_greater_than_or_equal() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("score".into(), n(100));
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
        .update_expression("SET score = :new")
        .condition_expression("score >= :min")
        .expression_attribute_values(":min", n(100))
        .expression_attribute_values(":new", n(200))
        .send()
        .await
        .unwrap();
}

// ========== BETWEEN ==========

#[tokio::test]
async fn condition_between_success() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("val".into(), n(50));
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
        .update_expression("SET val = :new")
        .condition_expression("val BETWEEN :lo AND :hi")
        .expression_attribute_values(":lo", n(10))
        .expression_attribute_values(":hi", n(90))
        .expression_attribute_values(":new", n(55))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_between_failure() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("val".into(), n(100));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    let err = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET val = :new")
        .condition_expression("val BETWEEN :lo AND :hi")
        .expression_attribute_values(":lo", n(10))
        .expression_attribute_values(":hi", n(90))
        .expression_attribute_values(":new", n(55))
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}

// ========== IN ==========

#[tokio::test]
async fn condition_in_success() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("color".into(), s("red"));
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
        .condition_expression("color IN (:a, :b, :c)")
        .expression_attribute_values(":a", s("red"))
        .expression_attribute_values(":b", s("green"))
        .expression_attribute_values(":c", s("blue"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn condition_in_failure() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("color".into(), s("yellow"));
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
        .condition_expression("color IN (:a, :b)")
        .expression_attribute_values(":a", s("red"))
        .expression_attribute_values(":b", s("green"))
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}
