// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the state-dependent vector index rules on `UpdateTable`.
//!
//! These three rules need the table's current state (its index count, its key
//! schema, whether it holds vector indexes), so they live in the backend rather
//! than in request-only core validation, and they are tested at the wire because
//! the error CLASS is part of the contract: the count limit is a
//! `LimitExceededException` here where `CreateTable` reports the same limit as a
//! `ValidationException`. All messages were measured against the live service on
//! 2026-08-13 (us-east-1) and are asserted in full so wording cannot drift.

use crate::vector_index_search::{wait_for_active, wait_for_vector_index_active};
use crate::vector_index_unsupported::{call, table_name, vectors_supported};

async fn skip_unless_supported() -> bool {
    !vectors_supported().await
}

fn assert_error(status: u16, body: &str, class: &str, expected_message: &str) {
    assert_eq!(status, 400, "expected HTTP 400, body: {body}");
    let json: serde_json::Value = serde_json::from_str(body).expect("body is JSON");
    let type_field = json
        .get("__type")
        .and_then(|t| t.as_str())
        .unwrap_or_else(|| panic!("no __type in body: {body}"));
    assert!(
        type_field.ends_with(class),
        "expected {class}, got {type_field} (body: {body})"
    );
    let message = json
        .get("message")
        .or_else(|| json.get("Message"))
        .and_then(|m| m.as_str())
        .unwrap_or_else(|| panic!("no message in body: {body}"));
    assert_eq!(message, expected_message);
}

/// Create a plain PAY_PER_REQUEST table with `n` vector indexes named
/// `vidx0..vidxN`, each on its own attribute, and wait for all to be usable.
async fn create_table_with_indexes(name: &str, n: usize) {
    let indexes: Vec<String> = (0..n)
        .map(|i| {
            format!(
                r#"{{
            "IndexName": "vidx{i}",
            "Dimensions": 4,
            "DistanceFunction": "COSINE",
            "VectorAttribute": {{"AttributeName": "emb{i}"}},
            "Projection": {{"ProjectionType": "ALL"}}
        }}"#
            )
        })
        .collect();
    let vector_indexes = if n == 0 {
        String::new()
    } else {
        format!(r#", "VectorIndexes": [{}]"#, indexes.join(","))
    };
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST"{vector_indexes}
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable failed: {text}");
    wait_for_active(name).await;
    for i in 0..n {
        wait_for_vector_index_active(name, &format!("vidx{i}")).await;
    }
}

async fn delete_table(name: &str) {
    let (_s, _t) = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

fn create_update_body(table: &str, index_name: &str, attr: &str) -> String {
    format!(
        r#"{{
        "TableName": "{table}",
        "VectorIndexUpdates": [{{"Create": {{
            "IndexName": "{index_name}",
            "VectorAttribute": {{"AttributeName": "{attr}"}},
            "Dimensions": 4,
            "DistanceFunction": "COSINE",
            "Projection": {{"ProjectionType": "ALL"}}
        }}}}]
    }}"#
    )
}

/// Adding a sixth index via UpdateTable is a `LimitExceededException`, not the
/// `ValidationException` CreateTable reports for the same limit, and the wording
/// differs too. Measured 2026-08-13: five accepted, six refused, on both paths.
#[tokio::test]
async fn sixth_vector_index_via_update_table_is_limit_exceeded() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vupd-limit");
    create_table_with_indexes(&name, 5).await;

    let (status, text) = call(
        "UpdateTable",
        &create_update_body(&name, "vidxSixth", "emb9"),
    )
    .await;
    assert_error(
        status,
        &text,
        "LimitExceededException",
        "Subscriber limit exceeded: Number of vector secondary indexes exceeds per-table limit of 5",
    );

    delete_table(&name).await;
}

/// Using the table's partition key as the vector attribute on UpdateTable
/// reports the redefinition message embedding both schemas, with the vector
/// reported as type L and its dimension count. Distinct from CreateTable's
/// conflicting-definition message, because the key is not re-declared in the
/// update request. Measured 2026-08-13 with the SearchSchema confound removed.
#[tokio::test]
async fn partition_key_as_vector_attribute_reports_redefinition() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vupd-keyattr");
    create_table_with_indexes(&name, 0).await;

    let (status, text) = call("UpdateTable", &create_update_body(&name, "vidxPk", "pk")).await;
    assert_error(
        status,
        &text,
        "ValidationException",
        "One or more parameter values were invalid: Attributes cannot be redefined. Please check \
         that your attribute has the same type as previously defined. Existing schema: \
         Schema:[SchemaElement: key{pk:S:HASH}] New schema: \
         VectorIndexSchema:[VectorAttribute: key{pk:L:4}]",
    );

    delete_table(&name).await;
}

/// A table holding vector indexes cannot leave PAY_PER_REQUEST. Same message as
/// the CreateTable-side rejection; measured 2026-08-13 by switching a live
/// vector table to PROVISIONED. The control shows the same switch succeeds once
/// the index is gone, so the test discriminates on the index's presence.
#[tokio::test]
async fn switching_vector_table_to_provisioned_is_rejected() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vupd-billing");
    create_table_with_indexes(&name, 1).await;

    let switch = format!(
        r#"{{
        "TableName": "{name}",
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": {{"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}}
    }}"#
    );
    let (status, text) = call("UpdateTable", &switch).await;
    assert_error(
        status,
        &text,
        "ValidationException",
        "One or more parameter values were invalid: Vector indexes are only supported for \
         PAY_PER_REQUEST tables",
    );

    // Control: drop the index and the same switch is accepted, proving the
    // refusal discriminates on the vector index rather than something else.
    let drop = format!(
        r#"{{
        "TableName": "{name}",
        "VectorIndexUpdates": [{{"Delete": {{"IndexName": "vidx0"}}}}]
    }}"#
    );
    let (status, text) = call("UpdateTable", &drop).await;
    assert_eq!(status, 200, "index delete failed: {text}");
    let (status, text) = call("UpdateTable", &switch).await;
    assert_eq!(
        status, 200,
        "switch after index removal should succeed: {text}"
    );

    delete_table(&name).await;
}

/// Deleting one index and creating another in the same request is refused,
/// in EITHER action order: the online-index machinery accepts one action per
/// call, so a swap must be issued as two UpdateTable calls. Measured
/// 2026-08-24 (us-east-1): a Delete+Create pair against a live table answers
/// `LimitExceededException` with the subscriber-limit sentence, the same
/// refusal GSIs give. An earlier version of this test asserted the swap must
/// PASS, reasoning from the net-effect count rule; that reasoning applied to
/// the per-table count limit, never to the one-action rule, and was not
/// measured. The net-effect counting in storage remains correct for the
/// hypothetical of a request that reaches it, but no multi-action request
/// does any more.
#[tokio::test]
async fn swap_in_one_call_is_refused() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vupd-swap");
    create_table_with_indexes(&name, 5).await;

    let swap = format!(
        r#"{{
        "TableName": "{name}",
        "VectorIndexUpdates": [
            {{"Delete": {{"IndexName": "vidx0"}}}},
            {{"Create": {{
                "IndexName": "vidxNew",
                "VectorAttribute": {{"AttributeName": "embNew"}},
                "Dimensions": 4,
                "DistanceFunction": "COSINE",
                "Projection": {{"ProjectionType": "ALL"}}
            }}}}
        ]
    }}"#
    );
    let (status, text) = call("UpdateTable", &swap).await;
    assert_error(
        status,
        &text,
        "LimitExceededException",
        "Subscriber limit exceeded: Only 1 online index can be created or deleted \
         simultaneously per table",
    );

    // Create listed BEFORE the delete: same two actions, same refusal.
    let swap_create_first = format!(
        r#"{{
        "TableName": "{name}",
        "VectorIndexUpdates": [
            {{"Create": {{
                "IndexName": "vidxNewer",
                "VectorAttribute": {{"AttributeName": "embNewer"}},
                "Dimensions": 4,
                "DistanceFunction": "COSINE",
                "Projection": {{"ProjectionType": "ALL"}}
            }}}},
            {{"Delete": {{"IndexName": "vidx1"}}}}
        ]
    }}"#
    );
    let (status, text) = call("UpdateTable", &swap_create_first).await;
    assert_error(
        status,
        &text,
        "LimitExceededException",
        "Subscriber limit exceeded: Only 1 online index can be created or deleted \
         simultaneously per table",
    );

    // The swap expressed as two sequential calls goes through: the delete
    // frees the slot, then the create is a single action against a table of
    // four.
    let drop_one = format!(
        r#"{{
        "TableName": "{name}",
        "VectorIndexUpdates": [{{"Delete": {{"IndexName": "vidx0"}}}}]
    }}"#
    );
    let (status, text) = call("UpdateTable", &drop_one).await;
    assert_eq!(status, 200, "single delete failed: {text}");
    let (status, text) = call(
        "UpdateTable",
        &create_update_body(&name, "vidxNew", "embNew"),
    )
    .await;
    assert_eq!(status, 200, "single create after delete failed: {text}");

    delete_table(&name).await;
}

/// Adding a vector index to a table that is already PROVISIONED is refused, and
/// the refusal is evaluated on the request's NET billing mode.
///
/// Measured 2026-08-19 (probe P12) against a live PROVISIONED table. Two facts,
/// both asserted here:
///
///  * `UpdateTable` with a vector-index Create and no `BillingMode` member is
///    refused with the same whole string `CreateTable` returns, so one constant
///    serves both paths and both directions of the rule.
///  * The same request plus `BillingMode: PAY_PER_REQUEST` is accepted. The check
///    reads the request's net state, not the table's stored mode, which is the
///    same net-effect evaluation the index-count limit uses.
///
/// The accepted case is the discriminating one: a guard written against the
/// stored mode passes the refusal half and wrongly refuses this half.
#[tokio::test]
async fn adding_a_vector_index_to_a_provisioned_table_is_rejected() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vupd-prov-add");
    let create = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": {{"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}}
    }}"#
    );
    let (status, text) = call("CreateTable", &create).await;
    assert_eq!(status, 200, "provisioned setup table failed: {text}");
    wait_for_active(&name).await;

    let (status, text) = call("UpdateTable", &create_update_body(&name, "vidx", "emb")).await;
    assert_error(
        status,
        &text,
        "ValidationException",
        "One or more parameter values were invalid: Vector indexes are only supported for \
         PAY_PER_REQUEST tables",
    );

    // The net-state case: switch to on-demand and create the index in one call.
    let combined = format!(
        r#"{{
        "TableName": "{name}",
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexUpdates": [{{"Create": {{
            "IndexName": "vidx",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "Dimensions": 4,
            "DistanceFunction": "COSINE",
            "Projection": {{"ProjectionType": "ALL"}}
        }}}}]
    }}"#
    );
    let (status, text) = call("UpdateTable", &combined).await;
    assert_eq!(
        status, 200,
        "a switch to PAY_PER_REQUEST and a create in one request must be accepted: {text}"
    );

    // Wait the index out before deleting, so the table is not left mid-build.
    wait_for_vector_index_active(&name, "vidx").await;
    delete_table(&name).await;
}
