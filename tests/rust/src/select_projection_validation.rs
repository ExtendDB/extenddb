// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `Select` / `ProjectionExpression` validation parity for Query and Scan,
//! including the Scan SPECIFIC_ATTRIBUTES message-prefix fix.

use crate::test_base::*;
use aws_sdk_dynamodb::types::Select;

#[tokio::test]
async fn query_projection_with_select_all_attributes_rejected() {
    let c = client();
    let t = tables().await;
    let err = c
        .query()
        .table_name(&t.simple_key_string)
        .key_condition_expression("#h = :h")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":h", s("anything"))
        .projection_expression("str")
        .select(Select::AllAttributes)
        .send()
        .await
        .expect_err("ProjectionExpression + ALL_ATTRIBUTES must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
    assert!(
        err_msg(&err).contains("Cannot specify the ProjectionExpression when choosing to get"),
        "got: {}",
        err_msg(&err)
    );
}

#[tokio::test]
async fn query_all_projected_attributes_without_index_rejected() {
    let c = client();
    let t = tables().await;
    let err = c
        .query()
        .table_name(&t.simple_key_string)
        .key_condition_expression("#h = :h")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":h", s("anything"))
        .select(Select::AllProjectedAttributes)
        .send()
        .await
        .expect_err("ALL_PROJECTED_ATTRIBUTES without an index must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
    assert!(
        err_msg(&err)
            .contains("ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"),
        "got: {}",
        err_msg(&err)
    );
}

#[tokio::test]
async fn scan_specific_attributes_without_projection_rejected_clean_prefix() {
    let c = client();
    let t = tables().await;
    let err = c
        .scan()
        .table_name(&t.simple_key_string)
        .select(Select::SpecificAttributes)
        .send()
        .await
        .expect_err("SPECIFIC_ATTRIBUTES without a projection must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
    // Regression: the message must NOT carry the stray accumulating-validator
    // prefix "1 validation error detected:".
    assert!(
        !err_msg(&err).contains("validation error detected"),
        "Scan SPECIFIC_ATTRIBUTES message should not carry the count prefix, got: {}",
        err_msg(&err)
    );
}
