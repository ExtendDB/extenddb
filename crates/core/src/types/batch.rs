// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Batch operation types for `BatchGetItem` and `BatchWriteItem`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::Item;
use super::capacity::{
    ConsumedCapacity, ItemCollectionMetrics, ReturnConsumedCapacity, ReturnItemCollectionMetrics,
};

/// Per-table key set and options for `BatchGetItem`.
#[derive(Debug, Clone, Deserialize)]
pub struct KeysAndAttributes {
    #[serde(rename = "Keys")]
    pub keys: Vec<Item>,
    #[serde(rename = "ConsistentRead")]
    pub consistent_read: Option<bool>,
    #[serde(rename = "ProjectionExpression")]
    pub projection_expression: Option<String>,
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_names"
    )]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// Legacy projection API. Superseded by `ProjectionExpression`.
    /// Cannot be used together with `ProjectionExpression`.
    #[serde(rename = "AttributesToGet")]
    pub attributes_to_get: Option<Vec<String>>,
}

/// `BatchGetItem` request body.
#[derive(Debug, Clone, Deserialize)]
pub struct BatchGetItemInput {
    #[serde(rename = "RequestItems")]
    pub request_items: HashMap<String, KeysAndAttributes>,
    /// Controls whether consumed capacity information is returned.
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: ReturnConsumedCapacity,
}

/// `BatchGetItem` response body.
#[derive(Debug, Clone, Serialize)]
pub struct BatchGetItemOutput {
    #[serde(rename = "Responses")]
    pub responses: HashMap<String, Vec<Item>>,
    #[serde(rename = "UnprocessedKeys", skip_serializing_if = "HashMap::is_empty")]
    pub unprocessed_keys: HashMap<String, KeysAndAttributes>,
    /// Per-table consumed capacity (present when requested).
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<ConsumedCapacity>>,
}

/// Serialization support for `KeysAndAttributes` in `UnprocessedKeys`.
impl Serialize for KeysAndAttributes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("Keys", &self.keys)?;
        if let Some(cr) = &self.consistent_read {
            map.serialize_entry("ConsistentRead", cr)?;
        }
        if let Some(pe) = &self.projection_expression {
            map.serialize_entry("ProjectionExpression", pe)?;
        }
        if let Some(ean) = &self.expression_attribute_names {
            map.serialize_entry("ExpressionAttributeNames", ean)?;
        }
        if let Some(atg) = &self.attributes_to_get {
            map.serialize_entry("AttributesToGet", atg)?;
        }
        map.end()
    }
}

/// A single write request within `BatchWriteItem`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WriteRequest {
    #[serde(rename = "PutRequest", skip_serializing_if = "Option::is_none")]
    pub put_request: Option<PutRequest>,
    #[serde(rename = "DeleteRequest", skip_serializing_if = "Option::is_none")]
    pub delete_request: Option<DeleteRequest>,
}

/// A put operation within a `WriteRequest`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PutRequest {
    #[serde(rename = "Item")]
    pub item: Item,
}

/// A delete operation within a `WriteRequest`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeleteRequest {
    #[serde(rename = "Key")]
    pub key: Item,
}

/// `BatchWriteItem` request body.
#[derive(Debug, Clone, Deserialize)]
pub struct BatchWriteItemInput {
    #[serde(rename = "RequestItems")]
    pub request_items: HashMap<String, Vec<WriteRequest>>,
    /// Controls whether consumed capacity information is returned.
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Controls whether item collection metrics are returned.
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    pub return_item_collection_metrics: ReturnItemCollectionMetrics,
}

/// `BatchWriteItem` response body.
#[derive(Debug, Clone, Serialize)]
pub struct BatchWriteItemOutput {
    #[serde(rename = "UnprocessedItems", skip_serializing_if = "HashMap::is_empty")]
    pub unprocessed_items: HashMap<String, Vec<WriteRequest>>,
    /// Per-table consumed capacity (present when requested).
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<ConsumedCapacity>>,
    /// Per-table item collection metrics (present when requested).
    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<HashMap<String, Vec<ItemCollectionMetrics>>>,
}
