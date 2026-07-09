// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! CreateTable input-validation parity:
//! - INCLUDE projection requires NonKeyAttributes.
//! - A disabled stream must not carry a StreamViewType.
//! - KEYS_ONLY / ALL projections must not carry NonKeyAttributes.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType, StreamSpecification, StreamViewType,
};

fn attr(name: &str, ty: ScalarAttributeType) -> AttributeDefinition {
    AttributeDefinition::builder()
        .attribute_name(name)
        .attribute_type(ty)
        .build()
        .unwrap()
}

fn key(name: &str, kt: KeyType) -> KeySchemaElement {
    KeySchemaElement::builder()
        .attribute_name(name)
        .key_type(kt)
        .build()
        .unwrap()
}

fn gsi_with_projection(proj: Projection) -> GlobalSecondaryIndex {
    GlobalSecondaryIndex::builder()
        .index_name("idx1")
        .key_schema(key("gk", KeyType::Hash))
        .projection(proj)
        .build()
        .unwrap()
}

#[tokio::test]
async fn create_table_include_projection_without_nonkey_rejected() {
    let c = client();
    let name = format!("CtInclude_{}", ts());
    let gsi = gsi_with_projection(
        Projection::builder()
            .projection_type(ProjectionType::Include)
            .build(),
    );
    let err = c
        .create_table()
        .table_name(&name)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(key(HASH_KEY_S, KeyType::Hash))
        .attribute_definitions(attr(HASH_KEY_S, ScalarAttributeType::S))
        .attribute_definitions(attr("gk", ScalarAttributeType::S))
        .global_secondary_indexes(gsi)
        .send()
        .await
        .expect_err("INCLUDE without NonKeyAttributes must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
    assert!(
        err_msg(&err).contains("NonKeyAttributes"),
        "got: {}",
        err_msg(&err)
    );
}

#[tokio::test]
async fn create_table_keys_only_with_nonkey_rejected() {
    let c = client();
    let name = format!("CtKeysOnly_{}", ts());
    let gsi = gsi_with_projection(
        Projection::builder()
            .projection_type(ProjectionType::KeysOnly)
            .non_key_attributes("extra")
            .build(),
    );
    let err = c
        .create_table()
        .table_name(&name)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(key(HASH_KEY_S, KeyType::Hash))
        .attribute_definitions(attr(HASH_KEY_S, ScalarAttributeType::S))
        .attribute_definitions(attr("gk", ScalarAttributeType::S))
        .global_secondary_indexes(gsi)
        .send()
        .await
        .expect_err("KEYS_ONLY with NonKeyAttributes must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
}

#[tokio::test]
async fn create_table_disabled_stream_with_view_type_rejected() {
    let c = client();
    let name = format!("CtStream_{}", ts());
    let err = c
        .create_table()
        .table_name(&name)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(key(HASH_KEY_S, KeyType::Hash))
        .attribute_definitions(attr(HASH_KEY_S, ScalarAttributeType::S))
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(false)
                .stream_view_type(StreamViewType::NewImage)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect_err("disabled stream with a view type must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
}
