// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Item types and size calculation for Virtual `DynamoDB` data operations.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use super::AttributeValue;
use super::capacity::{
    ConsumedCapacity, ItemCollectionMetrics, ReturnConsumedCapacity, ReturnItemCollectionMetrics,
};

/// A Virtual `DynamoDB` item — a map of attribute names to values.
pub type Item = BTreeMap<String, AttributeValue>;

/// `ReturnValues` parameter for write operations.
///
/// REQ-DATA-001: `PutItem` supports `NONE` and `ALL_OLD`.
/// REQ-DATA-004: `UpdateItem` supports all five variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReturnValues {
    #[default]
    None,
    AllOld,
    AllNew,
    UpdatedOld,
    UpdatedNew,
}

impl<'de> Deserialize<'de> for ReturnValues {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "NONE" => Ok(Self::None),
            "ALL_OLD" => Ok(Self::AllOld),
            "ALL_NEW" => Ok(Self::AllNew),
            "UPDATED_OLD" => Ok(Self::UpdatedOld),
            "UPDATED_NEW" => Ok(Self::UpdatedNew),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'returnValues' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATED_NEW]"
            ))),
        }
    }
}

/// Legacy `Expected` attribute condition.
///
/// Supports the pre-expression `Expected` parameter on `PutItem`, `DeleteItem`,
/// and `UpdateItem`. Desugared to a `ConditionExpression` at the engine layer.
#[derive(Debug, Clone, Deserialize)]
pub struct ExpectedAttributeValue {
    /// Legacy shorthand: if `Value` is set (without `ComparisonOperator`),
    /// it means `EQ` comparison.
    #[serde(rename = "Value")]
    pub value: Option<AttributeValue>,
    /// `true` = `attribute_exists`, `false` = `attribute_not_exists`.
    #[serde(rename = "Exists")]
    pub exists: Option<bool>,
    /// Comparison operator for the condition.
    #[serde(rename = "ComparisonOperator")]
    pub comparison_operator: Option<String>,
    /// Values for the comparison operator.
    #[serde(rename = "AttributeValueList")]
    pub attribute_value_list: Option<Vec<AttributeValue>>,
}

/// Legacy `AttributeUpdates` value update action.
///
/// Supports the pre-expression `AttributeUpdates` parameter on `UpdateItem`.
/// Desugared to an `UpdateExpression` at the engine layer.
#[derive(Debug, Clone, Deserialize)]
pub struct AttributeValueUpdate {
    /// The new value for the attribute.
    #[serde(rename = "Value")]
    pub value: Option<AttributeValue>,
    /// The action to perform: `PUT` (default), `DELETE`, or `ADD`.
    #[serde(rename = "Action", default = "default_update_action")]
    pub action: String,
}

fn default_update_action() -> String {
    "PUT".to_owned()
}

/// Logical operator for combining multiple `Expected` conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConditionalOperator {
    #[default]
    And,
    Or,
}

impl<'de> Deserialize<'de> for ConditionalOperator {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "AND" => Ok(Self::And),
            "OR" => Ok(Self::Or),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'conditionalOperator' \
                 failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"
            ))),
        }
    }
}

/// `PutItem` request body.
///
/// REQ-DATA-001: Supports `ConditionExpression`, `ReturnValues` (`NONE`, `ALL_OLD`).
#[derive(Debug, Clone, Deserialize)]
pub struct PutItemInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Item")]
    pub item: Item,
    #[serde(rename = "ReturnValues", default)]
    pub return_values: ReturnValues,
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: Option<String>,
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
    pub expression_attribute_values: Option<HashMap<String, super::AttributeValue>>,
    #[serde(rename = "Expected")]
    pub expected: Option<HashMap<String, ExpectedAttributeValue>>,
    #[serde(rename = "ConditionalOperator")]
    pub conditional_operator: Option<ConditionalOperator>,
    /// Controls whether the existing item is returned in the error when a condition fails.
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: super::ReturnValuesOnConditionCheckFailure,
    /// Controls whether consumed capacity information is returned.
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Controls whether item collection metrics are returned.
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    pub return_item_collection_metrics: ReturnItemCollectionMetrics,
}

/// `PutItem` response body.
#[derive(Debug, Clone, Serialize)]
pub struct PutItemOutput {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<Item>,
    /// Consumed capacity (present when requested).
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<ConsumedCapacity>,
    /// Item collection metrics (present when requested on a table with LSI).
    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<ItemCollectionMetrics>,
}

/// `GetItem` request body.
///
/// REQ-DATA-002: Supports `ConsistentRead`, `ProjectionExpression`, `ExpressionAttributeNames`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetItemInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: Item,
    // TODO(fidelity): Route ConsistentRead to read replica when replica support is added.
    // Single-node mode is strictly consistent, so ignoring this field is correct for now.
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
    /// Legacy `AttributesToGet` — desugared to `ProjectionExpression`.
    #[serde(rename = "AttributesToGet")]
    pub attributes_to_get: Option<Vec<String>>,
    /// Controls whether consumed capacity information is returned.
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: ReturnConsumedCapacity,
}

/// `GetItem` response body.
#[derive(Debug, Clone, Serialize)]
pub struct GetItemOutput {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<Item>,
    /// Consumed capacity (present when requested).
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<ConsumedCapacity>,
}

/// `DeleteItem` request body.
///
/// REQ-DATA-004: Supports `ConditionExpression`, `ReturnValues` (`NONE`, `ALL_OLD`).
#[derive(Debug, Clone, Deserialize)]
pub struct DeleteItemInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: Item,
    #[serde(rename = "ReturnValues", default)]
    pub return_values: ReturnValues,
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: Option<String>,
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
    pub expression_attribute_values: Option<HashMap<String, super::AttributeValue>>,
    #[serde(rename = "Expected")]
    pub expected: Option<HashMap<String, ExpectedAttributeValue>>,
    #[serde(rename = "ConditionalOperator")]
    pub conditional_operator: Option<ConditionalOperator>,
    /// Controls whether the existing item is returned in the error when a condition fails.
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: super::ReturnValuesOnConditionCheckFailure,
    /// Controls whether consumed capacity information is returned.
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Controls whether item collection metrics are returned.
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    pub return_item_collection_metrics: ReturnItemCollectionMetrics,
}

/// `DeleteItem` response body.
#[derive(Debug, Clone, Serialize)]
pub struct DeleteItemOutput {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<Item>,
    /// Consumed capacity (present when requested).
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<ConsumedCapacity>,
    /// Item collection metrics (present when requested on a table with LSI).
    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<ItemCollectionMetrics>,
}

/// `UpdateItem` request body.
///
/// REQ-DATA-003: Supports SET, REMOVE, ADD, DELETE update actions.
/// REQ-DATA-004: Supports all five `ReturnValues` variants.
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateItemInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: Item,
    #[serde(rename = "UpdateExpression")]
    pub update_expression: Option<String>,
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: Option<String>,
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
    pub expression_attribute_values: Option<HashMap<String, super::AttributeValue>>,
    #[serde(rename = "ReturnValues", default)]
    pub return_values: ReturnValues,
    #[serde(rename = "Expected")]
    pub expected: Option<HashMap<String, ExpectedAttributeValue>>,
    #[serde(rename = "ConditionalOperator")]
    pub conditional_operator: Option<ConditionalOperator>,
    /// Legacy `AttributeUpdates` parameter — desugared to `UpdateExpression`.
    #[serde(rename = "AttributeUpdates")]
    pub attribute_updates: Option<HashMap<String, AttributeValueUpdate>>,
    /// Controls whether the existing item is returned in the error when a condition fails.
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: super::ReturnValuesOnConditionCheckFailure,
    /// Controls whether consumed capacity information is returned.
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Controls whether item collection metrics are returned.
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    pub return_item_collection_metrics: ReturnItemCollectionMetrics,
}

/// `UpdateItem` response body.
#[derive(Debug, Clone, Serialize)]
pub struct UpdateItemOutput {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<Item>,
    /// Consumed capacity (present when requested).
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<ConsumedCapacity>,
    /// Item collection metrics (present when requested on a table with LSI).
    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<ItemCollectionMetrics>,
}

/// Extract key attributes from a full item to build a key-only item.
///
/// Used to construct `LastEvaluatedKey` from the last scanned/queried item.
#[must_use]
pub fn extract_key(item: &Item, key_schema: &[super::KeySchemaElement]) -> Item {
    let mut key = std::collections::BTreeMap::new();
    for ks in key_schema {
        if let Some(val) = item.get(&ks.attribute_name) {
            key.insert(ks.attribute_name.clone(), val.clone());
        }
    }
    key
}

/// Calculate the size of a Virtual `DynamoDB` item in bytes.
///
/// `DynamoDB` item size = sum of (attribute name UTF-8 length + attribute value size)
/// for all attributes. This is used for the 400 KB item size limit check.
///
/// Reference: <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html>
#[must_use]
pub fn item_size_bytes(item: &Item) -> usize {
    item.iter()
        .map(|(name, value)| name.len() + attribute_value_size(value))
        .sum()
}

/// Calculate the wire-format size of a single `AttributeValue`.
///
/// `DynamoDB` sizing rules:
/// - S: UTF-8 byte length
/// - N: string representation length (up to 38 significant digits + sign + decimal)
/// - B: raw byte length
/// - BOOL: 1 byte
/// - NULL: 1 byte
/// - L: 3 bytes overhead + sum of (element size + 1) per element
/// - M: 3 bytes overhead + sum of (name length + value size + 1) per entry
/// - SS/NS/BS: sum of element sizes
#[must_use]
pub fn attribute_value_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(s) => s.len(),
        AttributeValue::N(n) => dynamodb_number_size(n),
        AttributeValue::B(b) => b.len(),
        AttributeValue::Bool(_) | AttributeValue::Null => 1,
        // Every list element and map entry adds 1 byte of overhead beyond the
        // 3-byte container overhead, matching Amazon DynamoDB's item sizing.
        AttributeValue::L(list) => {
            3 + list
                .iter()
                .map(|v| attribute_value_size(v) + 1)
                .sum::<usize>()
        }
        AttributeValue::M(map) => {
            3 + map
                .iter()
                .map(|(k, v)| k.len() + attribute_value_size(v) + 1)
                .sum::<usize>()
        }
        AttributeValue::SS(set) => set.iter().map(String::len).sum(),
        AttributeValue::NS(set) => set.iter().map(|n| dynamodb_number_size(n)).sum(),
        AttributeValue::BS(set) => set.iter().map(Vec::len).sum(),
    }
}

/// Calculate the `DynamoDB` size of a number in bytes.
///
/// `DynamoDB` number sizing: approximately 1 byte per 2 significant digits + 1 byte.
/// Zero is 1 byte. Negative numbers add 1 byte. Max 21 bytes.
fn dynamodb_number_size(n: &str) -> usize {
    let s = n.trim_start_matches('-');
    let is_zero = s.chars().all(|c| c == '0' || c == '.');
    if is_zero {
        return 1;
    }

    let significant = if let Some(dot_pos) = s.find('.') {
        let (int_part, frac_part) = s.split_at(dot_pos);
        let frac = &frac_part[1..];
        let int_trimmed = int_part.trim_start_matches('0');
        let frac_trimmed = frac.trim_end_matches('0');
        if int_trimmed.is_empty() {
            let frac_sig = frac.trim_start_matches('0');
            frac_sig.trim_end_matches('0').len()
        } else {
            format!("{int_trimmed}{frac_trimmed}").len()
        }
    } else {
        let trimmed = s.trim_start_matches('0').trim_end_matches('0');
        if trimmed.is_empty() { 1 } else { trimmed.len() }
    };

    let mut size = significant.div_ceil(2) + 1;
    if n.starts_with('-') {
        size += 1;
    }
    size.min(21)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_owned())
    }

    #[test]
    fn scalar_sizes_unchanged() {
        assert_eq!(attribute_value_size(&s("hello")), 5);
        assert_eq!(attribute_value_size(&AttributeValue::Bool(true)), 1);
        assert_eq!(attribute_value_size(&AttributeValue::Null), 1);
    }

    #[test]
    fn empty_list_and_map_are_container_overhead_only() {
        assert_eq!(attribute_value_size(&AttributeValue::L(vec![])), 3);
        assert_eq!(attribute_value_size(&AttributeValue::M(BTreeMap::new())), 3);
    }

    #[test]
    fn list_adds_one_byte_per_element() {
        // Each {"S": "aa"} element is 2 value bytes + 1 byte overhead = 3.
        let list = AttributeValue::L(vec![s("aa"); 500]);
        assert_eq!(attribute_value_size(&list), 3 + 500 * 3);
    }

    #[test]
    fn map_adds_one_byte_per_entry() {
        // Entry sizes: name length + value size + 1 byte per entry.
        let mut m = BTreeMap::new();
        m.insert("a".to_owned(), s("xy"));
        m.insert("bb".to_owned(), s("z"));
        assert_eq!(
            attribute_value_size(&AttributeValue::M(m)),
            3 + (1 + 2 + 1) + (2 + 1 + 1)
        );
    }

    #[test]
    fn nested_container_overhead_compounds() {
        // Outer list holding one empty list: 3 + (3 + 1) = 7.
        let nested = AttributeValue::L(vec![AttributeValue::L(vec![])]);
        assert_eq!(attribute_value_size(&nested), 7);
    }

    #[test]
    fn item_size_counts_names_and_per_element_overhead() {
        // pk(2) + "b"(1) + data(4) + [3 + 500*3] = 1510.
        let mut item = Item::new();
        item.insert("pk".to_owned(), s("b"));
        item.insert("data".to_owned(), AttributeValue::L(vec![s("aa"); 500]));
        assert_eq!(item_size_bytes(&item), 2 + 1 + 4 + (3 + 500 * 3));
    }
}
