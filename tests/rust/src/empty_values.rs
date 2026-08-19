// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Empty value tests: empty strings, empty binary, empty sets, empty maps/lists.
//! Mirrors Python `test_data_types.py::TestEmptyValues` and `TestNestedStructures`.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, Put,
    ScalarAttributeType, TransactWriteItem, Update, WriteRequest,
};
use std::collections::HashMap;

async fn create_simple_table(c: &aws_sdk_dynamodb::Client) -> String {
    let name = format!("test_empty_{}", ts());
    c.create_table()
        .table_name(&name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(c, &name).await;
    name
}

#[tokio::test]
async fn put_item_empty_string_non_key() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("empty".into(), s(""));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("empty").unwrap(), &s(""));
}

#[tokio::test]
async fn put_item_empty_binary_non_key() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert(
        "empty".into(),
        AttributeValue::B(aws_smithy_types::Blob::new(Vec::<u8>::new())),
    );
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    if let AttributeValue::B(blob) = resp.item().unwrap().get("empty").unwrap() {
        assert!(blob.as_ref().is_empty());
    } else {
        panic!("Expected binary type");
    }
}

#[tokio::test]
async fn get_item_returns_empty_string() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("a".into(), s(""));
    item.insert("b".into(), s("notempty"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let got = resp.item().unwrap();
    assert_eq!(got.get("a").unwrap(), &s(""));
    assert_eq!(got.get("b").unwrap(), &s("notempty"));
}

#[tokio::test]
async fn batch_write_with_empty_string() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), s(""));
    c.batch_write_item()
        .request_items(
            &name,
            vec![WriteRequest::builder()
                .put_request(
                    aws_sdk_dynamodb::types::PutRequest::builder()
                        .set_item(Some(item))
                        .build()
                        .unwrap(),
                )
                .build()],
        )
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("val").unwrap(), &s(""));
}

#[tokio::test]
async fn update_item_set_empty_string() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), s("notempty"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    c.update_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .update_expression("SET val = :e")
        .expression_attribute_values(":e", s(""))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("val").unwrap(), &s(""));
}

#[tokio::test]
async fn scan_returns_items_with_empty_strings() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item1 = HashMap::new();
    item1.insert("pk".into(), s("k1"));
    item1.insert("val".into(), s(""));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item1))
        .send()
        .await
        .unwrap();
    let mut item2 = HashMap::new();
    item2.insert("pk".into(), s("k2"));
    item2.insert("val".into(), s("notempty"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item2))
        .send()
        .await
        .unwrap();

    let resp = c.scan().table_name(&name).send().await.unwrap();
    let items: HashMap<_, _> = resp
        .items()
        .iter()
        .map(|i| {
            (
                i.get("pk").unwrap().as_s().unwrap().clone(),
                i.get("val").unwrap().as_s().unwrap().clone(),
            )
        })
        .collect();
    assert_eq!(items.get("k1").unwrap(), "");
    assert_eq!(items.get("k2").unwrap(), "notempty");
}

#[tokio::test]
async fn transact_write_with_empty_string() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), s(""));
    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&name)
                        .set_item(Some(item))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("val").unwrap(), &s(""));
}

/// Regression: a TransactWriteItems `Update` that sets non-key attributes to an
/// empty string and empty binary must be accepted — empty values are valid on
/// non-key attributes on real DynamoDB. Guards the transactional update path
/// against faulting (previously a server-side error on an empty-value SET).
#[tokio::test]
async fn transact_update_with_empty_string_and_binary() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), s("notempty"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .update(
                    Update::builder()
                        .table_name(&name)
                        .key("pk", s("k1"))
                        .update_expression("SET s_attr = :s, b_attr = :b")
                        .expression_attribute_values(":s", s(""))
                        .expression_attribute_values(
                            ":b",
                            AttributeValue::B(aws_smithy_types::Blob::new(Vec::<u8>::new())),
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let got = resp.item().unwrap();
    assert_eq!(got.get("s_attr").unwrap(), &s(""));
    if let AttributeValue::B(blob) = got.get("b_attr").unwrap() {
        assert!(blob.as_ref().is_empty());
    } else {
        panic!("Expected binary type for b_attr");
    }
}

#[tokio::test]
async fn nested_empty_list() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut inner = HashMap::new();
    inner.insert("items".into(), list_val(vec![]));
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("data".into(), map_val(inner));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    if let AttributeValue::M(m) = resp.item().unwrap().get("data").unwrap() {
        if let AttributeValue::L(l) = m.get("items").unwrap() {
            assert!(l.is_empty());
        } else {
            panic!("Expected list");
        }
    } else {
        panic!("Expected map");
    }
}

#[tokio::test]
async fn nested_empty_map() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut inner = HashMap::new();
    inner.insert("meta".into(), map_val(HashMap::new()));
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("data".into(), map_val(inner));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    if let AttributeValue::M(m) = resp.item().unwrap().get("data").unwrap() {
        if let AttributeValue::M(inner) = m.get("meta").unwrap() {
            assert!(inner.is_empty());
        } else {
            panic!("Expected map");
        }
    } else {
        panic!("Expected map");
    }
}

#[tokio::test]
async fn nested_mixed_types_in_list() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut nested_map = HashMap::new();
    nested_map.insert("nested".into(), s("val"));
    let items_list = list_val(vec![
        s("text"),
        n(42),
        bool_val(true),
        null_val(),
        map_val(nested_map),
    ]);
    let mut outer = HashMap::new();
    outer.insert("items".into(), items_list);
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("data".into(), map_val(outer));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    if let AttributeValue::M(m) = resp.item().unwrap().get("data").unwrap() {
        if let AttributeValue::L(l) = m.get("items").unwrap() {
            assert_eq!(l.len(), 5);
            assert_eq!(l[0], s("text"));
            if let AttributeValue::M(inner) = &l[4] {
                assert_eq!(inner.get("nested").unwrap(), &s("val"));
            } else {
                panic!("Expected map at l[4]");
            }
        } else {
            panic!("Expected list");
        }
    } else {
        panic!("Expected map");
    }
}

#[tokio::test]
async fn deeply_nested_map() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut l3 = HashMap::new();
    l3.insert("value".into(), s("deep"));
    let mut l2 = HashMap::new();
    l2.insert("level2".into(), map_val(l3));
    let mut l1 = HashMap::new();
    l1.insert("level1".into(), map_val(l2));
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("data".into(), map_val(l1));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    if let AttributeValue::M(d) = resp.item().unwrap().get("data").unwrap() {
        if let AttributeValue::M(l1) = d.get("level1").unwrap() {
            if let AttributeValue::M(l2) = l1.get("level2").unwrap() {
                assert_eq!(l2.get("value").unwrap(), &s("deep"));
            } else {
                panic!("Expected map at level2");
            }
        } else {
            panic!("Expected map at level1");
        }
    } else {
        panic!("Expected map at data");
    }
}
