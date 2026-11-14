// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for `MetadataEngine` operations that are not
//! TTL-specific.
//!
//! The plug-in repo's four tag tests were consolidated in-tree into the single
//! phased `test_resource_tag_lifecycle` before this suite was ported, so this
//! module carries the consolidated form rather than both.

use extenddb_core::types::Tag;
use extenddb_storage::MetadataEngine;

use crate::helpers::setup_engine;

fn tag(key: &str, value: &str) -> Tag {
    Tag {
        key: key.to_owned(),
        value: value.to_owned(),
    }
}

/// One phased pass over the tag lifecycle. Each phase asserts a distinct
/// behaviour — empty listing, exact multi-tag persistence and ordering,
/// same-key overwrite, and selective removal — but they share one engine
/// connection, because `setup_engine` reruns catalog migrations and dominated
/// the cost of testing these as four separate cases.
#[tokio::test]
async fn test_resource_tag_lifecycle() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let arn = format!(
        "arn:aws:dynamodb:us-east-1:123456789012:table/test-{}",
        uuid::Uuid::new_v4().simple()
    );

    // An untouched resource has no tags.
    assert!(
        engine
            .list_tags(&arn)
            .await
            .expect("list_tags on an untagged resource")
            .is_empty()
    );

    // Tags persist with their exact keys and values, in clustering-key order.
    engine
        .tag_resource(&arn, &[tag("env", "staging"), tag("owner", "alice")])
        .await
        .expect("tag_resource");
    let tags = engine.list_tags(&arn).await.expect("list_tags");
    assert_eq!(
        tags.iter()
            .map(|t| (t.key.as_str(), t.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("env", "staging"), ("owner", "alice")]
    );

    // Re-tagging an existing key replaces its value and leaves the other alone.
    engine
        .tag_resource(&arn, &[tag("env", "prod")])
        .await
        .expect("tag_resource overwrite");
    let tags = engine
        .list_tags(&arn)
        .await
        .expect("list_tags after upsert");
    assert_eq!(
        tags.iter()
            .map(|t| (t.key.as_str(), t.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("env", "prod"), ("owner", "alice")],
        "an upsert must replace only the key it names"
    );

    // Untagging removes exactly the named keys.
    engine
        .tag_resource(&arn, &[tag("team", "storage")])
        .await
        .expect("tag_resource third key");
    engine
        .untag_resource(&arn, &["env".to_owned(), "team".to_owned()])
        .await
        .expect("untag_resource");
    let tags = engine.list_tags(&arn).await.expect("list_tags after untag");
    assert_eq!(
        tags.iter()
            .map(|t| (t.key.as_str(), t.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("owner", "alice")],
        "untag must remove only the keys it names"
    );
}
