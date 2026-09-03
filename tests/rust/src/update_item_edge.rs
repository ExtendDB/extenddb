// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Additional update item edge case tests — mirrors remaining Java `UpdateItemTests`.

use crate::test_base::*;

#[tokio::test]
async fn update_with_nested_map() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    // SET a nested map attribute
    let nested = map_val(
        [("inner".to_string(), s("nested_value"))]
            .into_iter()
            .collect(),
    );
    c.update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key.clone()))
        .update_expression("SET nested_map = :m")
        .expression_attribute_values(":m", nested.clone())
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let item = get.item().unwrap();
    let map = item.get("nested_map").unwrap().as_m().unwrap();
    assert_eq!(map.get("inner").unwrap().as_s().unwrap(), "nested_value");
}

#[tokio::test]
async fn update_literal_dotted_attribute_and_nested_path_with_alias() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("a.b".into(), s("before"));
    item.insert(
        "a".into(),
        map_val(
            [("b".to_string(), s("nested-before"))]
                .into_iter()
                .collect(),
        ),
    );
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    c.update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key.clone()))
        .update_expression("SET #dot = :after")
        .expression_attribute_names("#dot", "a.b")
        .expression_attribute_values(":after", s("after"))
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let item = get.item().unwrap();
    assert_eq!(item.get("a.b").unwrap().as_s().unwrap(), "after");
    assert_eq!(
        item.get("a")
            .unwrap()
            .as_m()
            .unwrap()
            .get("b")
            .unwrap()
            .as_s()
            .unwrap(),
        "nested-before"
    );
}

#[tokio::test]
async fn update_literal_dollar_prefixed_attribute_with_alias() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("$foo".into(), s("before"));
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    c.update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key.clone()))
        .update_expression("SET #dollar = :after")
        .expression_attribute_names("#dollar", "$foo")
        .expression_attribute_values(":after", s("after"))
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.item().unwrap().get("$foo").unwrap().as_s().unwrap(),
        "after"
    );
}

#[tokio::test]
async fn update_with_nested_list() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    let list = list_val(vec![s("a"), s("b"), n(3)]);
    c.update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key.clone()))
        .update_expression("SET my_list = :l")
        .expression_attribute_values(":l", list)
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let item = get.item().unwrap();
    let l = item.get("my_list").unwrap().as_l().unwrap();
    assert_eq!(l.len(), 3);
}

#[tokio::test]
async fn boolean_attribute_update() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    c.update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key.clone()))
        .update_expression("SET bool_attr = :b")
        .expression_attribute_values(":b", bool_val(true))
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.item()
            .unwrap()
            .get("bool_attr")
            .unwrap()
            .as_bool()
            .unwrap(),
        &true
    );
}

#[tokio::test]
async fn null_attribute_update() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    c.update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key.clone()))
        .update_expression("SET null_attr = :n")
        .expression_attribute_values(":n", null_val())
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.item()
            .unwrap()
            .get("null_attr")
            .unwrap()
            .as_null()
            .unwrap(),
        &true
    );
}

#[tokio::test]
async fn conditional_update_with_expression() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("status".into(), s("active"));
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    // Condition fails
    let err = c
        .update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key.clone()))
        .update_expression("SET #s = :new")
        .condition_expression("#s = :old")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":old", s("inactive"))
        .expression_attribute_values(":new", s("archived"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));

    // Condition succeeds
    c.update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key.clone()))
        .update_expression("SET #s = :new")
        .condition_expression("#s = :old")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":old", s("active"))
        .expression_attribute_values(":new", s("archived"))
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.item().unwrap().get("status").unwrap().as_s().unwrap(),
        "archived"
    );
}

#[tokio::test]
async fn update_item_gsi() {
    let c = client();
    let t = tables().await;
    let item = create_item_with_gsi(&t.simple_key_string_gsi);
    let key = get_key(&t.simple_key_string_gsi, &item);

    c.put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    c.update_item()
        .table_name(&t.simple_key_string_gsi)
        .set_key(Some(key.clone()))
        .update_expression("SET extra = :v")
        .expression_attribute_values(":v", s("gsi_update"))
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string_gsi)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.item().unwrap().get("extra").unwrap().as_s().unwrap(),
        "gsi_update"
    );
}

#[tokio::test]
async fn add_to_string_attribute_fails() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("strAttr".into(), s("hello"));
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    // ADD on a string attribute should fail
    let err = c
        .update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .update_expression("ADD strAttr :v")
        .expression_attribute_values(":v", n(1))
        .send()
        .await
        .unwrap_err();
    assert!(err_code(&err).is_some());
}

#[tokio::test]
async fn update_non_existent_item_creates_it() {
    let c = client();
    let t = tables().await;
    let unique = uuid::Uuid::new_v4().to_string();

    c.update_item()
        .table_name(&t.simple_key_string)
        .key(HASH_KEY_S, s(&unique))
        .update_expression("SET newAttr = :v")
        .expression_attribute_values(":v", s("created"))
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string)
        .key(HASH_KEY_S, s(&unique))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let item = get.item().unwrap();
    assert_eq!(item.get("newAttr").unwrap().as_s().unwrap(), "created");
}

#[tokio::test]
async fn update_item_return_values_on_ccf() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("attr1".into(), s("value1"));
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    let err = c
        .update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .update_expression("SET attr1 = :new")
        .condition_expression("attr1 = :old")
        .expression_attribute_values(":old", s("wrong"))
        .expression_attribute_values(":new", s("updated"))
        .return_values_on_condition_check_failure(
            aws_sdk_dynamodb::types::ReturnValuesOnConditionCheckFailure::AllOld,
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}
