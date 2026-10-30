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
    /// Any JSON number; see [`saturated_int32`].
    #[serde(
        rename = "Limit",
        default,
        deserialize_with = "deserialize_int32_field"
    )]
    pub limit: Option<i64>,
    #[serde(rename = "ExclusiveStartKey")]
    pub exclusive_start_key: Option<Item>,
    #[serde(rename = "Select")]
    pub select: Option<Select>,
    /// Any JSON number; see [`saturated_int32`].
    #[serde(
        rename = "Segment",
        default,
        deserialize_with = "deserialize_int32_field"
    )]
    pub segment: Option<i64>,
    /// Any JSON number; see [`saturated_int32`].
    #[serde(
        rename = "TotalSegments",
        default,
        deserialize_with = "deserialize_int32_field"
    )]
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

/// The value the service validates and uses for a `Scan` `Limit`, `Segment`
/// or `TotalSegments` written as `number`: a fraction is truncated toward zero
/// and a value outside the int32 range is saturated to the nearer bound.
///
/// Measured 2026-10-25 (us-east-1): `Limit: 1.5` and `Limit: 1.0` return one
/// item, `Limit: 1e2`, `Limit: 2147483648` and `Limit: 9223372036854775808`
/// are accepted, `Segment: 1.5` of `TotalSegments: 2` returns the items of
/// segment 1, `Limit: 0.0` is reported as `0`, `Limit: -2147483649` as
/// `-2147483648` and `TotalSegments: 2147483648` as `2147483647`.
#[must_use]
pub fn saturated_int32(number: &serde_json::Number) -> i32 {
    if let Some(value) = number.as_i64() {
        return i32::try_from(value).unwrap_or(if value < 0 { i32::MIN } else { i32::MAX });
    }
    if number.is_u64() {
        return i32::MAX;
    }
    // A float: `as` truncates toward zero and saturates at the int32 bounds.
    #[allow(clippy::cast_possible_truncation)]
    number.as_f64().map_or(0, |value| value as i32)
}

/// Deserialize an optional `Scan` range field from any JSON number, as
/// [`saturated_int32`] reads it. A JSON value of another type is refused, as
/// it was when the field deserialized as an integer.
fn deserialize_int32_field<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<serde_json::Number>::deserialize(deserializer)
        .map(|number| number.map(|n| i64::from(saturated_int32(&n))))
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ScanInput, saturated_int32};

    fn number(text: &str) -> serde_json::Number {
        serde_json::from_str(text).expect("a JSON number")
    }

    #[test]
    fn range_fields_are_truncated_and_saturated_to_int32() {
        for (text, expected) in [
            ("1", 1),
            ("-2147483648", i32::MIN),
            ("2147483647", i32::MAX),
            ("-2147483649", i32::MIN),
            ("2147483648", i32::MAX),
            ("9223372036854775808", i32::MAX),
            ("-9223372036854775809", i32::MIN),
            ("1.5", 1),
            ("1.0", 1),
            ("0.0", 0),
            ("1e2", 100),
            ("1e40", i32::MAX),
            ("-1e40", i32::MIN),
        ] {
            assert_eq!(saturated_int32(&number(text)), expected, "{text}");
        }
    }

    #[test]
    fn scan_input_reads_any_number_in_the_range_fields() {
        let input: ScanInput = serde_json::from_value(json!({
            "TableName": "t", "Limit": 1.5, "Segment": 1.5, "TotalSegments": 2_147_483_648_i64,
        }))
        .expect("accepted");
        assert_eq!(
            (input.limit, input.segment, input.total_segments),
            (Some(1), Some(1), Some(i64::from(i32::MAX)))
        );
        let input: ScanInput = serde_json::from_str(
            r#"{"TableName": "t", "Limit": 9223372036854775808, "Segment": null}"#,
        )
        .expect("accepted");
        assert_eq!(
            (input.limit, input.segment),
            (Some(i64::from(i32::MAX)), None)
        );
        let input: ScanInput = serde_json::from_value(json!({"TableName": "t"})).expect("accepted");
        assert_eq!(input.total_segments, None);
        // Another JSON type is still refused.
        for body in [
            json!({"TableName": "t", "Limit": "1"}),
            json!({"TableName": "t", "Segment": true}),
            json!({"TableName": "t", "TotalSegments": [1]}),
        ] {
            assert!(
                serde_json::from_value::<ScanInput>(body.clone()).is_err(),
                "{body}"
            );
        }
    }
}
