// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query and Scan request/response types for Virtual `DynamoDB`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::capacity::{ConsumedCapacity, ReturnConsumedCapacity};
use super::item::ConditionalOperator;
use super::{AttributeValue, Item};

/// Legacy condition used in `KeyConditions`, `QueryFilter`, and `ScanFilter`.
#[derive(Debug, Clone, Deserialize)]
pub struct Condition {
    #[serde(rename = "ComparisonOperator")]
    pub comparison_operator: String,
    #[serde(rename = "AttributeValueList", default)]
    pub attribute_value_list: Vec<AttributeValue>,
}

/// `Select` parameter — controls which attributes are returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Select {
    #[default]
    AllAttributes,
    AllProjectedAttributes,
    Count,
    SpecificAttributes,
}

impl<'de> Deserialize<'de> for Select {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "ALL_ATTRIBUTES" => Ok(Self::AllAttributes),
            "ALL_PROJECTED_ATTRIBUTES" => Ok(Self::AllProjectedAttributes),
            "COUNT" => Ok(Self::Count),
            "SPECIFIC_ATTRIBUTES" => Ok(Self::SpecificAttributes),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'select' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"
            ))),
        }
    }
}

/// `Query` request body.
#[derive(Debug, Clone, Deserialize)]
pub struct QueryInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "IndexName")]
    pub index_name: Option<String>,
    #[serde(rename = "KeyConditionExpression")]
    pub key_condition_expression: Option<String>,
    #[serde(rename = "FilterExpression")]
    pub filter_expression: Option<String>,
    #[serde(rename = "ProjectionExpression")]
    pub projection_expression: Option<String>,
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_names"
    )]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_values"
    )]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ScanIndexForward", default = "default_true")]
    pub scan_index_forward: bool,
    #[serde(rename = "Limit")]
    pub limit: Option<i64>,
    #[serde(rename = "ExclusiveStartKey")]
    pub exclusive_start_key: Option<Item>,
    #[serde(rename = "Select")]
    pub select: Option<Select>,
    #[serde(rename = "ConsistentRead")]
    pub consistent_read: Option<bool>,
    /// Controls whether consumed capacity information is returned.
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Legacy `KeyConditions` — desugared to `KeyConditionExpression`.
    #[serde(rename = "KeyConditions")]
    pub key_conditions: Option<HashMap<String, Condition>>,
    /// Legacy `QueryFilter` — desugared to `FilterExpression`.
    #[serde(rename = "QueryFilter")]
    pub query_filter: Option<HashMap<String, Condition>>,
    /// Legacy `AttributesToGet` — desugared to `ProjectionExpression`.
    #[serde(rename = "AttributesToGet")]
    pub attributes_to_get: Option<Vec<String>>,
    /// Logical operator for combining multiple legacy filter conditions.
    #[serde(rename = "ConditionalOperator")]
    pub conditional_operator: Option<ConditionalOperator>,
}

fn default_true() -> bool {
    true
}

/// `Query` response body.
#[derive(Debug, Clone, Serialize)]
pub struct QueryOutput {
    #[serde(rename = "Items", skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<Item>>,
    #[serde(rename = "Count")]
    pub count: i64,
    #[serde(rename = "ScannedCount")]
    pub scanned_count: i64,
    #[serde(rename = "LastEvaluatedKey", skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<Item>,
    /// Consumed capacity (present when requested).
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<ConsumedCapacity>,
}

/// `Scan` request body.
#[derive(Debug, Clone, Deserialize)]
pub struct ScanInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "IndexName")]
    pub index_name: Option<String>,
    #[serde(rename = "FilterExpression")]
    pub filter_expression: Option<String>,
    #[serde(rename = "ProjectionExpression")]
    pub projection_expression: Option<String>,
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_names"
    )]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_values"
    )]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "Limit")]
    pub limit: Option<i64>,
    #[serde(rename = "ExclusiveStartKey")]
    pub exclusive_start_key: Option<Item>,
    #[serde(rename = "Select")]
    pub select: Option<Select>,
    #[serde(rename = "Segment")]
    pub segment: Option<i64>,
    #[serde(rename = "TotalSegments")]
    pub total_segments: Option<i64>,
    #[serde(rename = "ConsistentRead")]
    pub consistent_read: Option<bool>,
    /// Controls whether consumed capacity information is returned.
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Legacy `ScanFilter` — desugared to `FilterExpression`.
    #[serde(rename = "ScanFilter")]
    pub scan_filter: Option<HashMap<String, Condition>>,
    /// Legacy `AttributesToGet` — desugared to `ProjectionExpression`.
    #[serde(rename = "AttributesToGet")]
    pub attributes_to_get: Option<Vec<String>>,
    /// Logical operator for combining multiple legacy filter conditions.
    #[serde(rename = "ConditionalOperator")]
    pub conditional_operator: Option<ConditionalOperator>,
}

/// `Scan` response body.
#[derive(Debug, Clone, Serialize)]
pub struct ScanOutput {
    #[serde(rename = "Items", skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<Item>>,
    #[serde(rename = "Count")]
    pub count: i64,
    #[serde(rename = "ScannedCount")]
    pub scanned_count: i64,
    #[serde(rename = "LastEvaluatedKey", skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<Item>,
    /// Consumed capacity (present when requested).
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<ConsumedCapacity>,
}
