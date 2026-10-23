// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tagging tests: TagResource, UntagResource, ListTagsOfResource.
//! Mirrors Python `test_tagging.py` and external Java tagging scenarios.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType, Tag,
};
use std::collections::HashMap;

async fn create_tagged_table(c: &aws_sdk_dynamodb::Client) -> (String, String) {
    let name = format!("test_tag_{}", ts());
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
    let desc = c.describe_table().table_name(&name).send().await.unwrap();
    let arn = desc.table().unwrap().table_arn().unwrap().to_string();
    (name, arn)
}

#[tokio::test]
async fn tag_and_list() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .tags(
            Tag::builder()
                .key("team")
                .value("platform")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let tags: HashMap<_, _> = resp
        .tags()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    assert_eq!(tags.get("env").unwrap(), "test");
    assert_eq!(tags.get("team").unwrap(), "platform");
}

#[tokio::test]
async fn tag_overwrite() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("dev").build().unwrap())
        .send()
        .await
        .unwrap();
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("prod").build().unwrap())
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let tags: HashMap<_, _> = resp
        .tags()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    assert_eq!(tags.get("env").unwrap(), "prod");
}

#[tokio::test]
async fn untag() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .tags(Tag::builder().key("team").value("x").build().unwrap())
        .send()
        .await
        .unwrap();
    c.untag_resource()
        .resource_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let keys: Vec<_> = resp.tags().iter().map(|t| t.key().to_string()).collect();
    assert!(!keys.contains(&"env".to_string()));
    assert!(keys.contains(&"team".to_string()));
}

#[tokio::test]
async fn untag_nonexistent_key() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    // Should succeed silently
    c.untag_resource()
        .resource_arn(&arn)
        .tag_keys("no-such-key")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn list_tags_empty() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(resp.tags().is_empty());
}

#[tokio::test]
async fn tag_cross_account_resource_is_denied() {
    let c = client();
    let fake_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/nonexistent-xyz";
    let err = c
        .tag_resource()
        .resource_arn(fake_arn)
        .tags(Tag::builder().key("k").value("v").build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err_code(&err),
        Some("AccessDeniedException"),
        "cross-account ARN must be denied without disclosing existence"
    );
}

#[tokio::test]
async fn list_tags_cross_account_resource_is_denied() {
    let c = client();
    let fake_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/nonexistent-xyz";
    let err = c
        .list_tags_of_resource()
        .resource_arn(fake_arn)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err_code(&err),
        Some("AccessDeniedException"),
        "cross-account ARN must be denied without disclosing existence"
    );
}

#[tokio::test]
async fn tag_multiple_then_untag_all() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("a").value("1").build().unwrap())
        .tags(Tag::builder().key("b").value("2").build().unwrap())
        .tags(Tag::builder().key("c").value("3").build().unwrap())
        .send()
        .await
        .unwrap();

    c.untag_resource()
        .resource_arn(&arn)
        .tag_keys("a")
        .tag_keys("b")
        .tag_keys("c")
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(resp.tags().is_empty());
}

#[tokio::test]
async fn untag_cross_account_resource_is_denied() {
    let c = client();
    let fake_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/nonexistent-xyz";
    let err = c
        .untag_resource()
        .resource_arn(fake_arn)
        .tag_keys("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err_code(&err),
        Some("AccessDeniedException"),
        "cross-account ARN must be denied without disclosing existence"
    );
}

#[tokio::test]
async fn tag_add_incremental() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("a").value("1").build().unwrap())
        .send()
        .await
        .unwrap();
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("b").value("2").build().unwrap())
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let tags: HashMap<_, _> = resp
        .tags()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    assert_eq!(tags.len(), 2);
    assert_eq!(tags.get("a").unwrap(), "1");
    assert_eq!(tags.get("b").unwrap(), "2");
}

#[tokio::test]
async fn tag_empty_value() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("k").value("").build().unwrap())
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let tags: HashMap<_, _> = resp
        .tags()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    assert_eq!(tags.get("k").unwrap(), "");
}

// ---------------------------------------------------------------------------
// Resource ARN validation.
//
// Error classes below match DynamoDB, verified against the service. The ARNs
// are derived from a real table's ARN so the account and region match the
// caller's without hardcoding either.
// ---------------------------------------------------------------------------

/// Replace the table name in a table ARN.
fn with_table_name(arn: &str, table_name: &str) -> String {
    format!("{}/{table_name}", arn.rsplit_once('/').unwrap().0)
}

#[tokio::test]
async fn tag_nonexistent_table_in_own_account_reports_not_found() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    let missing = with_table_name(&arn, "nonexistent-xyz-99");

    let err = c
        .tag_resource()
        .resource_arn(&missing)
        .tags(Tag::builder().key("k").value("v").build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert_eq!(err_code(&err), Some("ResourceNotFoundException"));
}

#[tokio::test]
async fn tag_resource_arn_in_another_region_is_a_validation_error() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    // Rewrite only the region component, keeping the caller's own account.
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    let other_region = if parts[3] == "us-west-2" {
        "us-east-1"
    } else {
        "us-west-2"
    };
    let wrong_region = format!(
        "{}:{}:{}:{}:{}:{}",
        parts[0], parts[1], parts[2], other_region, parts[4], parts[5]
    );

    let err = c
        .tag_resource()
        .resource_arn(&wrong_region)
        .tags(Tag::builder().key("k").value("v").build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "an ARN for a different region must be rejected as invalid"
    );
}

#[tokio::test]
async fn tag_resource_arn_without_arn_prefix_is_a_validation_error() {
    let c = client();
    let err = c
        .tag_resource()
        .resource_arn("not-an-arn")
        .tags(Tag::builder().key("k").value("v").build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert_eq!(err_code(&err), Some("ValidationException"));
}

#[tokio::test]
async fn tag_non_table_dynamodb_arn_is_a_validation_error() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    // Same account and region, but an index resource rather than a table.
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    let index_arn = format!(
        "{}:{}:{}:{}:{}:index/foo",
        parts[0], parts[1], parts[2], parts[3], parts[4]
    );

    let err = c
        .tag_resource()
        .resource_arn(&index_arn)
        .tags(Tag::builder().key("k").value("v").build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert_eq!(err_code(&err), Some("ValidationException"));
}

#[tokio::test]
async fn tag_arn_for_another_service_is_denied() {
    let c = client();
    let err = c
        .tag_resource()
        .resource_arn("arn:aws:s3:::mybucket")
        .tags(Tag::builder().key("k").value("v").build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err_code(&err),
        Some("AccessDeniedException"),
        "an ARN for another service is denied, not reported as malformed"
    );
}
