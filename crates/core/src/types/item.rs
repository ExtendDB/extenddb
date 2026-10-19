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
/// Measured against the service (us-east-1, 2026-10-19) by accept/refuse pairs
/// either side of the 409,600-byte item limit:
///
/// ```text
/// 1 + ceil(integer significant digits / 2) + ceil(fraction significant digits / 2)
///   + 1 if negative
/// ```
///
/// The two halves are rounded up separately, so digits that straddle the
/// decimal point cost one byte more than the same digits without a point:
/// `1.5` is 3 bytes where `15` is 2, and `3.14159` is 5 where `123456` is 4.
/// Leading zeros of the integer half and trailing zeros of the fraction half
/// are not significant; trailing zeros of the integer half are significant
/// when a fraction follows them (`100` is 2, `100.5` is 4) and a zero between
/// two significant digits always is (`1010` is 3). The exponent is not
/// counted (`1E125` is 2). Zero is 1 byte however it is spelled, and is never
/// negative (`-0` is 1).
fn dynamodb_number_size(n: &str) -> usize {
    let (negative, body) = match n.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, n.strip_prefix('+').unwrap_or(n)),
    };
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(pos) => (&body[..pos], body[pos + 1..].parse::<i64>().unwrap_or(0)),
        None => (body, 0),
    };
    let (int_text, frac_text) = match mantissa.find('.') {
        Some(pos) => (&mantissa[..pos], &mantissa[pos + 1..]),
        None => (mantissa, ""),
    };
    let digits: String = int_text.chars().chain(frac_text.chars()).collect();
    if !digits.bytes().any(|b| (b'1'..=b'9').contains(&b)) {
        return 1;
    }
    // Where the decimal point sits in `digits` once the exponent is applied.
    let point = i64::try_from(int_text.len())
        .unwrap_or(i64::MAX)
        .saturating_add(exponent);
    let len = i64::try_from(digits.len()).unwrap_or(i64::MAX);
    let (int_part, frac_part): (String, String) = if point <= 0 {
        (
            String::new(),
            "0".repeat(usize::try_from(-point).unwrap_or(0)) + &digits,
        )
    } else if point >= len {
        (
            digits.clone() + &"0".repeat(usize::try_from(point - len).unwrap_or(0)),
            String::new(),
        )
    } else {
        let split = usize::try_from(point).unwrap_or(0);
        (digits[..split].to_owned(), digits[split..].to_owned())
    };
    let int_part = int_part.trim_start_matches('0');
    let frac_part = frac_part.trim_end_matches('0');
    // Trailing zeros of the integer half fall away only when no fraction follows.
    let int_significant = if frac_part.is_empty() {
        int_part.trim_end_matches('0').len()
    } else {
        int_part.len()
    };
    // Leading zeros of the fraction half fall away only when no integer precedes.
    let frac_significant = if int_part.is_empty() {
        frac_part.trim_start_matches('0').len()
    } else {
        frac_part.len()
    };
    let size =
        1 + int_significant.div_ceil(2) + frac_significant.div_ceil(2) + usize::from(negative);
    size.min(21)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_owned())
    }

    #[test]
    fn number_size_matches_the_service_for_measured_literals() {
        // Each figure measured against the service (us-east-1, 2026-10-19) by an
        // accept/refuse pair either side of the 409,600-byte item limit.
        for (literal, bytes) in [
            ("0", 1),
            ("-0", 1),
            ("1", 2),
            ("15", 2),
            ("100", 2),
            ("1E125", 2),
            ("0.15", 2),
            ("1.5", 3),
            ("1.2", 3),
            ("1.200", 3),
            ("1010", 3),
            ("-42", 3),
            ("1.234", 4),
            ("100.5", 4),
            ("123456", 4),
            ("3.14159", 5),
        ] {
            assert_eq!(dynamodb_number_size(literal), bytes, "literal {literal}");
        }
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
