// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ConsumedCapacity.VectorIndexes` on write operations.
//!
//! The service meters writes replicated into a vector index as
//! `VectorWriteRequestBytes = max(dimensions * 4 + projected_non_vector_bytes,
//! 1024)`, charged whenever the PROJECTED index entry changes, and doubled
//! when the search-schema HASH value moves the entry between partitions
//! (measured against real DynamoDB 2026-08-13; the unit tests in
//! `extenddb_core::vector_capacity` pin those live figures). These tests prove
//! the WIRING: that write responses carry the map, with the model's figure,
//! only at INDEXES granularity, and omit it when nothing was charged.

use crate::vector_index_unsupported::{call, expect_vectors, table_name, vectors_supported};
use serde_json::Value;

async fn skip_unless_supported() -> bool {
    let supported = vectors_supported().await;
    assert!(
        !(!supported && expect_vectors() == Some(true)),
        "EXTENDDB_EXPECT_VECTORS=1 but the backend does not support vector indexes"
    );
    !supported
}

/// Create a table with one 512-dimension vector index (optionally
/// tenant-scoped) and wait for ACTIVE.
async fn create_table(name: &str, scoped: bool) {
    let search_schema = if scoped {
        r#""SearchSchema": [{"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}],"#
    } else {
        ""
    };
    let attr_defs = if scoped {
        r#"{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "tenant", "AttributeType": "S"}"#
    } else {
        r#"{"AttributeName": "pk", "AttributeType": "S"}"#
    };
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{attr_defs}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{{
            "IndexName": "vidx",
            "Dimensions": 512,
            "DistanceFunction": "COSINE",
            "VectorAttribute": {{"AttributeName": "emb"}},
            {search_schema}
            "Projection": {{"ProjectionType": "ALL"}}
        }}]
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable failed: {text}");
    crate::vector_index_search::wait_for_active(name).await;
}

fn vector_json(dims: usize) -> String {
    let parts: Vec<String> = (0..dims).map(|i| format!(r#"{{"N": "{i}"}}"#)).collect();
    format!("[{}]", parts.join(", "))
}

async fn put(table: &str, pk: &str, tenant: Option<&str>, rcc: &str) -> (u16, Value) {
    let tenant_attr = tenant
        .map(|t| format!(r#", "tenant": {{"S": "{t}"}}"#))
        .unwrap_or_default();
    let body = format!(
        r#"{{
        "TableName": "{table}",
        "Item": {{"pk": {{"S": "{pk}"}}, "emb": {{"L": {}}}{tenant_attr}}},
        "ReturnConsumedCapacity": "{rcc}"
    }}"#,
        vector_json(512)
    );
    let (status, text) = call("PutItem", &body).await;
    let json: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    (status, json)
}

fn vector_write_bytes(response: &Value) -> Option<f64> {
    response
        .pointer("/ConsumedCapacity/VectorIndexes/vidx/VectorWriteRequestBytes")
        .and_then(Value::as_f64)
}

/// A vector insert at INDEXES granularity reports the model figure:
/// 512 dims * 4 bytes + pk name (2) + pk value (1) + vector attr name (3)
/// = 2054, above the 1024 floor.
#[tokio::test]
async fn put_reports_vector_write_bytes_at_indexes_granularity() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vwr_put");
    create_table(&name, false).await;

    let (status, json) = put(&name, "a", None, "INDEXES").await;
    assert_eq!(status, 200, "PutItem failed: {json}");
    assert_eq!(
        vector_write_bytes(&json),
        Some(2054.0),
        "expected 512*4 + 6 projected bytes: {json}"
    );
}

/// TOTAL granularity carries no `VectorIndexes` map (measured: the service
/// reports it for INDEXES only).
#[tokio::test]
async fn total_granularity_omits_the_vector_indexes_map() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vwr_total");
    create_table(&name, false).await;

    let (status, json) = put(&name, "a", None, "TOTAL").await;
    assert_eq!(status, 200, "PutItem failed: {json}");
    assert!(
        json.pointer("/ConsumedCapacity/VectorIndexes").is_none(),
        "TOTAL must not carry VectorIndexes: {json}"
    );
}

/// A byte-identical re-put does not change the projected entry, so nothing is
/// charged and the map is omitted rather than zero-filled.
#[tokio::test]
async fn unchanged_projected_entry_is_not_charged() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vwr_same");
    create_table(&name, false).await;

    let (status, _) = put(&name, "a", None, "NONE").await;
    assert_eq!(status, 200);
    let (status, json) = put(&name, "a", None, "INDEXES").await;
    assert_eq!(status, 200, "PutItem failed: {json}");
    assert!(
        json.pointer("/ConsumedCapacity/VectorIndexes").is_none(),
        "identical re-put must not charge vector write capacity: {json}"
    );
}

/// An item without the vector attribute is never in the index; no charge.
#[tokio::test]
async fn item_without_vector_attribute_is_not_charged() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vwr_novec");
    create_table(&name, false).await;

    let body = format!(
        r#"{{
        "TableName": "{name}",
        "Item": {{"pk": {{"S": "plain"}}}},
        "ReturnConsumedCapacity": "INDEXES"
    }}"#
    );
    let (status, text) = call("PutItem", &body).await;
    assert_eq!(status, 200, "PutItem failed: {text}");
    let json: Value = serde_json::from_str(&text).expect("JSON");
    assert!(
        json.pointer("/ConsumedCapacity/VectorIndexes").is_none(),
        "non-vector item must not charge vector write capacity: {json}"
    );
}

/// Deleting a vector item replicates the removal and charges the deleted
/// image's figure.
#[tokio::test]
async fn delete_of_a_vector_item_is_charged() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vwr_del");
    create_table(&name, false).await;
    let (status, _) = put(&name, "a", None, "NONE").await;
    assert_eq!(status, 200);

    let body = format!(
        r#"{{
        "TableName": "{name}",
        "Key": {{"pk": {{"S": "a"}}}},
        "ReturnConsumedCapacity": "INDEXES"
    }}"#
    );
    let (status, text) = call("DeleteItem", &body).await;
    assert_eq!(status, 200, "DeleteItem failed: {text}");
    let json: Value = serde_json::from_str(&text).expect("JSON");
    assert_eq!(
        vector_write_bytes(&json),
        Some(2054.0),
        "delete charges the removed image: {json}"
    );
}

/// Changing the search-schema HASH value moves the entry between partitions,
/// which the service bills as a delete plus an insert: exactly double.
#[tokio::test]
async fn search_schema_partition_move_doubles_the_charge() {
    if skip_unless_supported().await {
        return;
    }
    let name = table_name("vwr_move");
    create_table(&name, true).await;

    // Insert under tenant t1: single charge, image is pk(3) + tenant name(6)
    // + tenant value(2) + emb name(3) = 14 projected bytes -> 2062.
    let (status, json) = put(&name, "a", Some("t1"), "INDEXES").await;
    assert_eq!(status, 200, "insert failed: {json}");
    assert_eq!(
        vector_write_bytes(&json),
        Some(2062.0),
        "scoped insert charges once: {json}"
    );

    // Move to tenant t2: same image size, doubled.
    let (status, json) = put(&name, "a", Some("t2"), "INDEXES").await;
    assert_eq!(status, 200, "move failed: {json}");
    assert_eq!(
        vector_write_bytes(&json),
        Some(4124.0),
        "partition move must be billed as delete + insert: {json}"
    );
}
