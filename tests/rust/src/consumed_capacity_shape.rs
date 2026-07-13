// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! ConsumedCapacity response shape. Real DynamoDB returns only `CapacityUnits`
//! (top-level and in the nested `Table` breakdown) for these single-item and
//! batch operations at both TOTAL and INDEXES granularity — it does NOT emit
//! the granular `ReadCapacityUnits` / `WriteCapacityUnits` sub-fields.
//! Verified against real DynamoDB (us-east-1).

use crate::test_base::*;
use aws_sdk_dynamodb::types::{KeysAndAttributes, ReturnConsumedCapacity, WriteRequest};
use std::collections::HashMap;

fn key(v: &str) -> HashMap<String, aws_sdk_dynamodb::types::AttributeValue> {
    let mut k = HashMap::new();
    k.insert(HASH_KEY_S.into(), s(v));
    k
}

#[tokio::test]
async fn get_item_consumed_capacity_has_no_granular_fields() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let id = format!("cc_{}", ts());
    c.put_item()
        .table_name(table)
        .set_item(Some(key(&id)))
        .send()
        .await
        .unwrap();

    for gran in [
        ReturnConsumedCapacity::Total,
        ReturnConsumedCapacity::Indexes,
    ] {
        let resp = c
            .get_item()
            .table_name(table)
            .set_key(Some(key(&id)))
            .return_consumed_capacity(gran.clone())
            .send()
            .await
            .unwrap();
        let cc = resp.consumed_capacity().expect("ConsumedCapacity");
        assert!(cc.capacity_units().is_some(), "CapacityUnits present");
        assert!(
            cc.read_capacity_units().is_none() && cc.write_capacity_units().is_none(),
            "top-level RCU/WCU must be absent ({gran:?})"
        );
        if let Some(tbl) = cc.table() {
            assert!(
                tbl.read_capacity_units().is_none() && tbl.write_capacity_units().is_none(),
                "nested Table RCU/WCU must be absent ({gran:?})"
            );
        }
    }
}

#[tokio::test]
async fn put_item_consumed_capacity_has_no_granular_fields() {
    let c = client();
    let t = tables().await;
    let resp = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(key(&format!("cc_{}", ts()))))
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();
    let cc = resp.consumed_capacity().expect("ConsumedCapacity");
    assert!(cc.capacity_units().is_some());
    assert!(
        cc.write_capacity_units().is_none() && cc.read_capacity_units().is_none(),
        "PutItem TOTAL must emit only CapacityUnits"
    );
}

#[tokio::test]
async fn batch_get_consumed_capacity_has_no_granular_fields() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let id = format!("ccb_{}", ts());
    c.put_item()
        .table_name(table)
        .set_item(Some(key(&id)))
        .send()
        .await
        .unwrap();
    let resp = c
        .batch_get_item()
        .request_items(
            table,
            KeysAndAttributes::builder().keys(key(&id)).build().unwrap(),
        )
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();
    let cc = resp.consumed_capacity().first().expect("ConsumedCapacity");
    assert!(cc.capacity_units().is_some());
    assert!(
        cc.read_capacity_units().is_none() && cc.write_capacity_units().is_none(),
        "BatchGetItem per-table CC must emit only CapacityUnits"
    );
}

#[tokio::test]
async fn batch_write_consumed_capacity_has_no_granular_fields() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = key(&format!("ccw_{}", ts()));
    item.insert("v".into(), s("x"));
    let resp = c
        .batch_write_item()
        .request_items(
            table,
            vec![WriteRequest::builder()
                .put_request(
                    aws_sdk_dynamodb::types::PutRequest::builder()
                        .set_item(Some(item))
                        .build()
                        .unwrap(),
                )
                .build()],
        )
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();
    let cc = resp.consumed_capacity().first().expect("ConsumedCapacity");
    assert!(cc.capacity_units().is_some());
    assert!(
        cc.write_capacity_units().is_none() && cc.read_capacity_units().is_none(),
        "BatchWriteItem per-table CC must emit only CapacityUnits"
    );
}
