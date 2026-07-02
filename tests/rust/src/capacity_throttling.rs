// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Capacity throttling tests — mirrors Java `CapacityThrottlingTests`.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ProvisionedThroughput,
    ReturnConsumedCapacity, ScalarAttributeType,
};

async fn create_on_demand_table(name: &str) {
    let c = client();
    c.create_table()
        .table_name(name)
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
    wait_for_active(c, name).await;
}

async fn create_provisioned_table(name: &str, rcu: i64, wcu: i64) {
    let c = client();
    c.create_table()
        .table_name(name)
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
        .billing_mode(BillingMode::Provisioned)
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(rcu)
                .write_capacity_units(wcu)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    wait_for_active(c, name).await;
}

#[tokio::test]
async fn get_item_consumed_capacity_total() {
    let c = client();
    let table = format!("CapGet_{}", ts());
    create_on_demand_table(&table).await;

    c.put_item()
        .table_name(&table)
        .item("pk", s("cap_test_1"))
        .item("data", s("some data value"))
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(&table)
        .key("pk", s("cap_test_1"))
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();

    let cap = resp.consumed_capacity().unwrap();
    assert_eq!(cap.table_name().unwrap(), table.as_str());
    assert!(cap.capacity_units().unwrap() > 0.0);

}

#[tokio::test]
async fn put_item_consumed_capacity_total() {
    let c = client();
    let table = format!("CapPut_{}", ts());
    create_on_demand_table(&table).await;

    let resp = c
        .put_item()
        .table_name(&table)
        .item("pk", s("cap_test_2"))
        .item("data", s("write data"))
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();

    let cap = resp.consumed_capacity().unwrap();
    assert_eq!(cap.table_name().unwrap(), table.as_str());
    assert!(cap.capacity_units().unwrap() > 0.0);

}

#[tokio::test]
async fn scan_consumed_capacity_total() {
    let c = client();
    let table = format!("CapScan_{}", ts());
    create_on_demand_table(&table).await;

    for i in 0..3 {
        c.put_item()
            .table_name(&table)
            .item("pk", s(&format!("scan_cap_{i}")))
            .send()
            .await
            .unwrap();
    }

    let resp = c
        .scan()
        .table_name(&table)
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();

    let cap = resp.consumed_capacity().unwrap();
    assert!(cap.capacity_units().unwrap() > 0.0);

}

#[tokio::test]
async fn no_consumed_capacity_by_default() {
    let c = client();
    let table = format!("CapNone_{}", ts());
    create_on_demand_table(&table).await;

    c.put_item()
        .table_name(&table)
        .item("pk", s("no_cap_test"))
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(&table)
        .key("pk", s("no_cap_test"))
        .send()
        .await
        .unwrap();

    assert!(resp.consumed_capacity().is_none());

}

#[tokio::test]
async fn per_partition_write_throttling() {
    let c = client();
    let table = format!("ThrottlePartW_{}", ts());
    // Use a provisioned table with 1 WCU so the per-table bucket (1 token)
    // is exhausted quickly, making per-partition throttling observable.
    create_provisioned_table(&table, 100, 1).await;

    let big_value = "x".repeat(10_000); // ~10 KB = 10 WCU per write
    let mut throttled = 0u32;
    let mut succeeded = 0u32;

    for i in 0..50 {
        match c
            .put_item()
            .table_name(&table)
            .item("pk", s("hot_partition"))
            .item("data", s(&big_value))
            .item("seq", n(i))
            .send()
            .await
        {
            Ok(_) => succeeded += 1,
            Err(e) => {
                let code = err_code(&e).unwrap_or("");
                if code.contains("ProvisionedThroughputExceeded")
                    || code.contains("ThrottlingException")
                {
                    throttled += 1;
                }
            }
        }
    }

    assert!(
        throttled > 0,
        "Expected some writes throttled. Succeeded: {succeeded}, Throttled: {throttled}"
    );

}

#[tokio::test]
async fn provisioned_table_write_throttling() {
    // Real DynamoDB has burst capacity that absorbs 50 rapid writes at 1 WCU.
    if is_real_dynamodb() {
        return;
    }
    let c = client();
    let table = format!("ThrottleProvW_{}", ts());
    create_provisioned_table(&table, 5, 1).await;

    let mut throttled = 0u32;
    let mut succeeded = 0u32;

    for i in 0..50 {
        match c
            .put_item()
            .table_name(&table)
            .item("pk", s(&format!("prov_key_{i}")))
            .item("data", s(&format!("value_{i}")))
            .send()
            .await
        {
            Ok(_) => succeeded += 1,
            Err(e) => {
                let code = err_code(&e).unwrap_or("");
                if code.contains("ProvisionedThroughputExceeded")
                    || code.contains("ThrottlingException")
                {
                    throttled += 1;
                }
            }
        }
    }

    assert!(
        throttled > 0,
        "Expected some writes throttled (1 WCU). Succeeded: {succeeded}, Throttled: {throttled}"
    );

}

#[tokio::test]
async fn provisioned_table_read_throttling() {
    // Real DynamoDB has burst capacity that absorbs 50 rapid reads at 1 RCU.
    if is_real_dynamodb() {
        return;
    }
    let c = client();
    let table = format!("ThrottleProvR_{}", ts());
    create_provisioned_table(&table, 1, 5).await;

    c.put_item()
        .table_name(&table)
        .item("pk", s("read_throttle_key"))
        .item("data", s("value"))
        .send()
        .await
        .unwrap();

    let mut throttled = 0u32;
    let mut succeeded = 0u32;

    for _ in 0..50 {
        match c
            .get_item()
            .table_name(&table)
            .key("pk", s("read_throttle_key"))
            .consistent_read(true)
            .send()
            .await
        {
            Ok(_) => succeeded += 1,
            Err(e) => {
                let code = err_code(&e).unwrap_or("");
                if code.contains("ProvisionedThroughputExceeded")
                    || code.contains("ThrottlingException")
                {
                    throttled += 1;
                }
            }
        }
    }

    assert!(
        throttled > 0,
        "Expected some reads throttled (1 RCU). Succeeded: {succeeded}, Throttled: {throttled}"
    );

}
