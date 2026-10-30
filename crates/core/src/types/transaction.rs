// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Types for `TransactGetItems` and `TransactWriteItems` operations.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::capacity::{
    ConsumedCapacity, ItemCollectionMetrics, ReturnConsumedCapacity, ReturnItemCollectionMetrics,
    ReturnValuesOnConditionCheckFailure,
};
use super::{AttributeValue, Item};

/// Input for `TransactGetItems`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactGetItemsInput {
    /// Ordered array of up to 100 `TransactGetItem` objects.
    #[serde(rename = "TransactItems")]
    pub transact_items: Vec<TransactGetItem>,
    /// Controls whether consumed capacity information is returned.
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: ReturnConsumedCapacity,
}

/// A single get request within a `TransactGetItems` call.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactGetItem {
    /// The get operation to perform.
    #[serde(rename = "Get")]
    pub get: TransactGet,
}

/// Specifies an item to retrieve in a transaction.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactGet {
    /// Primary key of the item to retrieve.
    #[serde(rename = "Key")]
    pub key: Item,
    /// Table name.
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "ProjectionExpression")]
    pub projection_expression: Option<String>,
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_names"
    )]
    pub expression_attribute_names: Option<HashMap<String, String>>,
}

/// Output for `TransactGetItems`.
#[derive(Debug, Clone, Serialize)]
pub struct TransactGetItemsOutput {
    /// Ordered array of item responses.
    #[serde(rename = "Responses")]
    pub responses: Vec<ItemResponse>,
    /// Per-table consumed capacity (present when requested).
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<ConsumedCapacity>>,
}

/// A single item response within `TransactGetItems` output.
///
/// Real `DynamoDB` returns `{}` (no `Item` key) for missing items. When an item
/// exists, it returns `{"Item": {...}}`. The AWS SDK API model marks `Item` as
/// optional, and SDKs deserialize an absent `Item` key as `None`/`null`.
#[derive(Debug, Clone, Serialize)]
pub struct ItemResponse {
    /// The retrieved item, absent when the item does not exist.
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<Item>,
}

/// Input for `TransactWriteItems`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactWriteItemsInput {
    /// Ordered array of up to 100 write operations.
    #[serde(rename = "TransactItems")]
    pub transact_items: Vec<TransactWriteItem>,
    /// Idempotency token (valid for 10 minutes).
    #[serde(rename = "ClientRequestToken")]
    pub client_request_token: Option<String>,
    /// Controls whether consumed capacity information is returned.
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Controls whether item collection metrics are returned.
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    pub return_item_collection_metrics: ReturnItemCollectionMetrics,
}

/// A single write operation within a `TransactWriteItems` call.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactWriteItem {
    /// Condition check (no mutation).
    #[serde(rename = "ConditionCheck")]
    pub condition_check: Option<TransactConditionCheck>,
    /// Put operation.
    #[serde(rename = "Put")]
    pub put: Option<TransactPut>,
    /// Delete operation.
    #[serde(rename = "Delete")]
    pub delete: Option<TransactDelete>,
    /// Update operation.
    #[serde(rename = "Update")]
    pub update: Option<TransactUpdate>,
}

/// A condition check within a transaction.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactConditionCheck {
    /// Primary key of the item to check.
    #[serde(rename = "Key")]
    pub key: Item,
    /// Table name.
    #[serde(rename = "TableName")]
    pub table_name: String,
    /// Condition expression (required).
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: String,
    /// Attribute name substitutions.
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_names"
    )]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// Attribute value substitutions.
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_values"
    )]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Controls whether the existing item is returned in the cancellation reason.
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: ReturnValuesOnConditionCheckFailure,
}

/// A put operation within a transaction.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactPut {
    /// The item to write.
    #[serde(rename = "Item")]
    pub item: Item,
    /// Table name.
    #[serde(rename = "TableName")]
    pub table_name: String,
    /// Optional condition expression.
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: Option<String>,
    /// Attribute name substitutions.
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_names"
    )]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// Attribute value substitutions.
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_values"
    )]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Controls whether the existing item is returned in the cancellation reason.
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: ReturnValuesOnConditionCheckFailure,
}

/// A delete operation within a transaction.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactDelete {
    /// Primary key of the item to delete.
    #[serde(rename = "Key")]
    pub key: Item,
    /// Table name.
    #[serde(rename = "TableName")]
    pub table_name: String,
    /// Optional condition expression.
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: Option<String>,
    /// Attribute name substitutions.
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_names"
    )]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// Attribute value substitutions.
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_values"
    )]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Controls whether the existing item is returned in the cancellation reason.
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: ReturnValuesOnConditionCheckFailure,
}

/// An update operation within a transaction.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactUpdate {
    /// Primary key of the item to update.
    #[serde(rename = "Key")]
    pub key: Item,
    /// Table name.
    #[serde(rename = "TableName")]
    pub table_name: String,
    /// Update expression (required).
    #[serde(rename = "UpdateExpression")]
    pub update_expression: String,
    /// Optional condition expression.
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: Option<String>,
    /// Attribute name substitutions.
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_names"
    )]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// Attribute value substitutions.
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "crate::serde_helpers::deserialize_expression_values"
    )]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Controls whether the existing item is returned in the cancellation reason.
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: ReturnValuesOnConditionCheckFailure,
}

/// Output for `TransactWriteItems`.
#[derive(Debug, Clone, Serialize)]
pub struct TransactWriteItemsOutput {
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

/// Per-item cancellation reason in a `TransactionCanceledException`.
#[derive(Debug, Clone, Serialize)]
pub struct CancellationReason {
    /// Status code: `"None"`, `"ConditionalCheckFailed"`, `"ValidationError"`, etc.
    #[serde(rename = "Code")]
    pub code: String,
    /// Human-readable message, omitted for items with no error.
    #[serde(rename = "Message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The existing item when `ReturnValuesOnConditionCheckFailure` is `ALL_OLD`.
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<Item>,
}

impl CancellationReason {
    /// Create a reason indicating no error for this item position.
    #[must_use]
    pub fn none() -> Self {
        Self {
            code: "None".to_owned(),
            message: None,
            item: None,
        }
    }

    /// Create a reason for a failed condition check, optionally including the old item.
    ///
    /// Pass `Some(item)` when `ReturnValuesOnConditionCheckFailure` is `ALL_OLD`
    /// and the item exists; pass `None` otherwise.
    #[must_use]
    pub fn condition_check_failed_with_item(item: Option<Item>) -> Self {
        Self {
            code: "ConditionalCheckFailed".to_owned(),
            message: Some("The conditional request failed".to_owned()),
            item,
        }
    }

    /// Create a reason for an item that another transaction holds.
    #[must_use]
    pub fn transaction_conflict() -> Self {
        Self {
            code: "TransactionConflict".to_owned(),
            message: Some("Transaction is ongoing for the item".to_owned()),
            item: None,
        }
    }

    /// Create a reason for a validation error.
    #[must_use]
    pub fn validation_error(msg: impl Into<String>) -> Self {
        Self {
            code: "ValidationError".to_owned(),
            message: Some(msg.into()),
            item: None,
        }
    }

    /// Internal reason for a wrong-typed key found while preparing a
    /// transaction member: a table key element that does not match the
    /// schema, or a secondary-index key (on a written item or as a literal
    /// operand) that does not match its attribute definition. Never reaches a
    /// client: [`Self::resolve_key_faults`] rewrites it to `ValidationError`.
    #[must_use]
    pub fn key_type_mismatch(msg: impl Into<String>) -> Self {
        Self {
            code: KEY_TYPE_MISMATCH_CODE.to_owned(),
            message: Some(msg.into()),
            item: None,
        }
    }

    /// Internal reason for a secondary-index key left empty by a transaction
    /// member. Never reaches a client: [`Self::resolve_key_faults`] turns it
    /// into a top-level `ValidationException` message.
    #[must_use]
    pub fn index_key_empty_abort(msg: impl Into<String>) -> Self {
        Self {
            code: INDEX_KEY_EMPTY_ABORT_CODE.to_owned(),
            message: Some(msg.into()),
            item: None,
        }
    }

    /// Whether this reason is one of the internal key faults that
    /// [`Self::resolve_key_faults`] gives precedence to. A backend that runs
    /// members out of request order can stop once the earliest such member
    /// in request order is known: a wrong-typed key there outranks every later
    /// member, and an empty index key there outranks every later member that
    /// is not itself a wrong-typed key.
    #[must_use]
    pub fn is_key_fault(&self) -> bool {
        self.code == KEY_TYPE_MISMATCH_CODE || self.code == INDEX_KEY_EMPTY_ABORT_CODE
    }

    /// Apply the precedence `DynamoDB` gives key faults across the members of
    /// a `TransactWriteItems` request, measured on the service. A wrong-typed
    /// key (table or secondary index) on any member cancels the transaction
    /// with the first such member's `ValidationError` in position and `None`
    /// in every other position, whatever those members' own faults were.
    /// Otherwise an empty secondary-index key on any member aborts the whole
    /// request with a top-level `ValidationException`, outranking failed
    /// conditions and unapplicable statements on the other members. Otherwise
    /// the reasons are returned as they are.
    ///
    /// # Errors
    ///
    /// Returns `Err(message)` when the request must be answered with a
    /// top-level `ValidationException` carrying `message`.
    pub fn resolve_key_faults(reasons: Vec<Self>) -> Result<Vec<Self>, String> {
        let first_type_mismatch = reasons
            .iter()
            .position(|r| r.code == KEY_TYPE_MISMATCH_CODE);
        let mut reasons = reasons;
        for (i, reason) in reasons.iter_mut().enumerate() {
            match first_type_mismatch {
                Some(first) if i == first => reason.code = "ValidationError".to_owned(),
                Some(_) => *reason = Self::none(),
                None if reason.code == INDEX_KEY_EMPTY_ABORT_CODE => {
                    if let Some(message) = reason.message.take() {
                        return Err(message);
                    }
                    *reason = Self::none();
                }
                None => {}
            }
        }
        Ok(reasons)
    }
}

/// Internal cancellation code for a wrong-typed key. See
/// [`CancellationReason::key_type_mismatch`].
const KEY_TYPE_MISMATCH_CODE: &str = "ExtendDbKeyTypeMismatch";

/// Internal cancellation code for an empty secondary-index key. See
/// [`CancellationReason::index_key_empty_abort`].
const INDEX_KEY_EMPTY_ABORT_CODE: &str = "ExtendDbIndexKeyEmptyAbort";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_response_missing_serializes_as_empty_object() {
        let resp = ItemResponse { item: None };
        let json = serde_json::to_value(&resp).unwrap();
        // Real DynamoDB returns {} (no Item key) for missing items.
        assert_eq!(json, serde_json::json!({}));
    }

    #[test]
    fn item_response_present_serializes_item() {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S("val".to_owned()));
        let resp = ItemResponse { item: Some(item) };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["Item"]["pk"]["S"], "val");
    }

    fn codes(reasons: &[CancellationReason]) -> Vec<&str> {
        reasons.iter().map(|r| r.code.as_str()).collect()
    }

    #[test]
    fn no_key_fault_leaves_the_reasons_untouched() {
        let reasons = vec![
            CancellationReason::condition_check_failed_with_item(None),
            CancellationReason::validation_error("cannot apply"),
            CancellationReason::none(),
        ];
        let out = CancellationReason::resolve_key_faults(reasons).unwrap();
        assert_eq!(
            codes(&out),
            ["ConditionalCheckFailed", "ValidationError", "None"]
        );
    }

    #[test]
    fn an_empty_index_key_becomes_a_top_level_message_over_other_member_faults() {
        let reasons = vec![
            CancellationReason::condition_check_failed_with_item(None),
            CancellationReason::index_key_empty_abort("empty key"),
            CancellationReason::validation_error("cannot apply"),
        ];
        assert_eq!(
            CancellationReason::resolve_key_faults(reasons).unwrap_err(),
            "empty key"
        );
    }

    #[test]
    fn a_type_mismatch_masks_every_other_member_including_an_empty_key() {
        let reasons = vec![
            CancellationReason::condition_check_failed_with_item(None),
            CancellationReason::index_key_empty_abort("empty key"),
            CancellationReason::key_type_mismatch("type"),
            CancellationReason::validation_error("cannot apply"),
        ];
        let out = CancellationReason::resolve_key_faults(reasons).unwrap();
        assert_eq!(codes(&out), ["None", "None", "ValidationError", "None"]);
        assert_eq!(out[2].message.as_deref(), Some("type"));
    }

    #[test]
    fn only_the_first_type_mismatch_by_position_is_reported() {
        let reasons = vec![
            CancellationReason::none(),
            CancellationReason::key_type_mismatch("second"),
            CancellationReason::key_type_mismatch("third"),
        ];
        let out = CancellationReason::resolve_key_faults(reasons).unwrap();
        assert_eq!(codes(&out), ["None", "ValidationError", "None"]);
        assert_eq!(out[1].message.as_deref(), Some("second"));
    }
}
