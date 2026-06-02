// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! UpdateItem integration tests.

use crate::test_base::*;

#[tokio::test]
async fn update_item_set_new_attribute() {
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
        .update_expression("SET newAttr = :v")
        .expression_attribute_values(":v", s("hello"))
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
    let got = resp.item().expect("Item should exist");
    assert_eq!(got.get("newAttr").unwrap(), &s("hello"));
}

#[tokio::test]
async fn update_item_overwrite_existing_attribute() {
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
        .set_key(Some(key.clone()))
        .update_expression("SET color = :v")
        .expression_attribute_values(":v", s("blue"))
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
    let got = resp.item().expect("Item should exist");
    assert_eq!(got.get("color").unwrap(), &s("blue"));
}

#[tokio::test]
async fn update_item_remove_attribute() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("toRemove".into(), s("gone"));
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
        .update_expression("REMOVE toRemove")
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
    let got = resp.item().expect("Item should exist");
    assert!(got.get("toRemove").is_none(), "Attribute should be removed");
}

#[tokio::test]
async fn update_item_add_to_number() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("counter".into(), n(10));
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
        .update_expression("SET #c = #c + :inc")
        .expression_attribute_names("#c", "counter")
        .expression_attribute_values(":inc", n(5))
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
    let got = resp.item().expect("Item should exist");
    assert_eq!(got.get("counter").unwrap(), &n(15));
}

#[tokio::test]
async fn update_item_add_to_string_set() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("tags".into(), ss(&["a", "b"]));
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
        .update_expression("ADD tags :v")
        .expression_attribute_values(":v", ss(&["c"]))
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
    let got = resp.item().expect("Item should exist");
    if let aws_sdk_dynamodb::types::AttributeValue::Ss(vals) = got.get("tags").unwrap() {
        let set: std::collections::HashSet<_> = vals.iter().collect();
        assert!(set.contains(&"a".to_string()));
        assert!(set.contains(&"b".to_string()));
        assert!(set.contains(&"c".to_string()));
        assert_eq!(set.len(), 3);
    } else {
        panic!("Expected SS type");
    }
}

#[tokio::test]
async fn update_item_delete_from_string_set() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
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
        .update_expression("DELETE tags :v")
        .expression_attribute_values(":v", ss(&["b"]))
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
    let got = resp.item().expect("Item should exist");
    if let aws_sdk_dynamodb::types::AttributeValue::Ss(vals) = got.get("tags").unwrap() {
        let set: std::collections::HashSet<_> = vals.iter().collect();
        assert!(set.contains(&"a".to_string()));
        assert!(set.contains(&"c".to_string()));
        assert!(!set.contains(&"b".to_string()));
        assert_eq!(set.len(), 2);
    } else {
        panic!("Expected SS type");
    }
}

#[tokio::test]
async fn update_item_return_values_all_new() {
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
    let resp = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET extra = :v")
        .expression_attribute_values(":v", s("new"))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew)
        .send()
        .await
        .unwrap();

    let attrs = resp.attributes().expect("ALL_NEW should return attributes");
    assert_eq!(attrs.get("extra").unwrap(), &s("new"));
    // Original attributes should also be present.
    for (k, v) in &item {
        assert_eq!(attrs.get(k).unwrap(), v, "Attribute {k} mismatch");
    }
}

#[tokio::test]
async fn update_item_return_values_all_old() {
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
    let resp = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET extra = :v")
        .expression_attribute_values(":v", s("new"))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
        .send()
        .await
        .unwrap();

    let attrs = resp.attributes().expect("ALL_OLD should return attributes");
    for (k, v) in &item {
        assert_eq!(attrs.get(k).unwrap(), v, "Attribute {k} mismatch");
    }
    assert!(
        attrs.get("extra").is_none(),
        "Old item should not have 'extra'"
    );
}

#[tokio::test]
async fn update_item_return_values_all_old_on_new_item() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);
    let key = get_key(table, &item);

    let resp = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET extra = :v")
        .expression_attribute_values(":v", s("new"))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
        .send()
        .await
        .unwrap();

    assert!(
        resp.attributes().is_none(),
        "ALL_OLD should not return key attributes for a newly created item"
    );
}
