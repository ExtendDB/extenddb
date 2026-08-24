// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TTL tests: UpdateTimeToLive, DescribeTimeToLive.
//! Mirrors Python `test_ttl.py` and external Java TTL scenarios.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    TimeToLiveSpecification,
};

async fn create_ttl_table(c: &aws_sdk_dynamodb::Client) -> String {
    let name = format!("test_ttl_{}", ts());
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
async fn describe_ttl_disabled() {
    let c = client();
    let name = create_ttl_table(c).await;
    let resp = c
        .describe_time_to_live()
        .table_name(&name)
        .send()
        .await
        .unwrap();
    let ttl = resp.time_to_live_description().unwrap();
    let status = format!("{:?}", ttl.time_to_live_status().unwrap());
    assert!(
        status.contains("Disabled") || status.contains("Disabling"),
        "Expected DISABLED or DISABLING, got: {status}"
    );
}

#[tokio::test]
async fn enable_ttl() {
    let c = client();
    let name = create_ttl_table(c).await;
    c.update_time_to_live()
        .table_name(&name)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(true)
                .attribute_name("expires_at")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = c
        .describe_time_to_live()
        .table_name(&name)
        .send()
        .await
        .unwrap();
    let ttl = resp.time_to_live_description().unwrap();
    let status = format!("{:?}", ttl.time_to_live_status().unwrap());
    assert!(
        status.contains("Enabled") || status.contains("Enabling"),
        "Expected ENABLED or ENABLING, got: {status}"
    );
    assert_eq!(ttl.attribute_name().unwrap(), "expires_at");
}

#[tokio::test]
async fn enable_ttl_with_dotted_attribute_name() {
    let c = client();
    let name = create_ttl_table(c).await;
    c.update_time_to_live()
        .table_name(&name)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(true)
                .attribute_name("expires.at")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = c
        .describe_time_to_live()
        .table_name(&name)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.time_to_live_description()
            .unwrap()
            .attribute_name()
            .unwrap(),
        "expires.at"
    );
}

#[tokio::test]
async fn disable_ttl() {
    let c = client();
    let name = create_ttl_table(c).await;
    c.update_time_to_live()
        .table_name(&name)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(true)
                .attribute_name("ttl")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Real DynamoDB enforces a cooldown between TTL modifications.
    if is_real_dynamodb() {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }

    c.update_time_to_live()
        .table_name(&name)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(false)
                .attribute_name("ttl")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = c
        .describe_time_to_live()
        .table_name(&name)
        .send()
        .await
        .unwrap();
    let ttl = resp.time_to_live_description().unwrap();
    let status = format!("{:?}", ttl.time_to_live_status().unwrap());
    assert!(
        status.contains("Disabled") || status.contains("Disabling"),
        "Expected DISABLED or DISABLING, got: {status}"
    );
}

#[tokio::test]
async fn enable_ttl_nonexistent_table() {
    let c = client();
    let err = c
        .update_time_to_live()
        .table_name("nonexistent-table-xyz")
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(true)
                .attribute_name("ttl")
                .build()
                .unwrap(),
        )
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn describe_ttl_nonexistent_table() {
    let c = client();
    let err = c
        .describe_time_to_live()
        .table_name("nonexistent-table-xyz")
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn describe_ttl_enabled_attribute_name() {
    let c = client();
    let name = create_ttl_table(c).await;
    c.update_time_to_live()
        .table_name(&name)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(true)
                .attribute_name("my_ttl")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = c
        .describe_time_to_live()
        .table_name(&name)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.time_to_live_description()
            .unwrap()
            .attribute_name()
            .unwrap(),
        "my_ttl"
    );
}
