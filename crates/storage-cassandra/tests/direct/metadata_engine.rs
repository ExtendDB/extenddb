// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for MetadataEngine tag operations.

use extenddb_core::types::Tag;
use extenddb_storage::MetadataEngine;

use crate::helpers::setup_engine;

#[tokio::test]
async fn test_tag_and_list_tags() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let arn = format!(
        "arn:aws:dynamodb:us-east-1:123456789012:table/test-{}",
        uuid::Uuid::new_v4().simple()
    );

    let tags = vec![
        Tag {
            key: "env".to_string(),
            value: "test".to_string(),
        },
        Tag {
            key: "owner".to_string(),
            value: "alice".to_string(),
        },
    ];

    engine
        .tag_resource(&arn, &tags)
        .await
        .expect("tag_resource should succeed");

    let result = engine
        .list_tags(&arn)
        .await
        .expect("list_tags should succeed");
    assert_eq!(result.len(), 2);
    // Cassandra returns in clustering key order
    assert_eq!(result[0].key, "env");
    assert_eq!(result[0].value, "test");
    assert_eq!(result[1].key, "owner");
    assert_eq!(result[1].value, "alice");
}

#[tokio::test]
async fn test_tag_resource_upserts() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let arn = format!(
        "arn:aws:dynamodb:us-east-1:123456789012:table/test-{}",
        uuid::Uuid::new_v4().simple()
    );

    engine
        .tag_resource(
            &arn,
            &[Tag {
                key: "env".to_string(),
                value: "staging".to_string(),
            }],
        )
        .await
        .expect("first tag_resource should succeed");

    // Overwrite with new value
    engine
        .tag_resource(
            &arn,
            &[Tag {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
        )
        .await
        .expect("second tag_resource should succeed");

    let result = engine
        .list_tags(&arn)
        .await
        .expect("list_tags should succeed");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, "prod");
}

#[tokio::test]
async fn test_untag_resource() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let arn = format!(
        "arn:aws:dynamodb:us-east-1:123456789012:table/test-{}",
        uuid::Uuid::new_v4().simple()
    );

    let tags = vec![
        Tag {
            key: "a".to_string(),
            value: "1".to_string(),
        },
        Tag {
            key: "b".to_string(),
            value: "2".to_string(),
        },
        Tag {
            key: "c".to_string(),
            value: "3".to_string(),
        },
    ];
    engine
        .tag_resource(&arn, &tags)
        .await
        .expect("tag_resource should succeed");

    engine
        .untag_resource(&arn, &["a".to_string(), "c".to_string()])
        .await
        .expect("untag_resource should succeed");

    let result = engine
        .list_tags(&arn)
        .await
        .expect("list_tags should succeed");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].key, "b");
    assert_eq!(result[0].value, "2");
}

#[tokio::test]
async fn test_list_tags_empty() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let arn = format!(
        "arn:aws:dynamodb:us-east-1:123456789012:table/test-{}",
        uuid::Uuid::new_v4().simple()
    );

    let result = engine
        .list_tags(&arn)
        .await
        .expect("list_tags should succeed");
    assert!(result.is_empty());
}
