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

    /// Internal reason for a wrong-typed key supplied in the request of a
    /// transaction member: a table key element that does not match the
    /// schema, or a secondary-index key (on a Put item or as an Update's
    /// literal operand) that does not match its attribute definition. Never
    /// reaches a client: [`Self::resolve_key_faults`] rewrites it to
    /// `ValidationError`.
    ///
    /// A wrong type an Update writes from `if_not_exists` or from an
    /// attribute reference is not this reason. With the path present the
    /// value comes from the stored item; with the path absent the fallback
    /// is a literal from the request, and the service ranks it with the
    /// values taken from the item. The service reports both as a plain
    /// positional `ValidationError` and leaves the other members' outcomes
    /// in place, so the storage layers carry them as
    /// [`Self::validation_error`].
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

    /// Apply the precedence `DynamoDB` gives key faults across the members of
    /// a `TransactWriteItems` request, measured on the service: the first
    /// internal key fault in request order decides. A wrong-typed key there
    /// (see [`Self::key_type_mismatch`]) cancels the transaction with that
    /// member's `ValidationError` and `None` in every other position,
    /// whatever those members' own faults were. An empty secondary-index key
    /// there (see [`Self::index_key_empty_abort`]) answers the whole request
    /// with a top-level `ValidationException` carrying its message. With no
    /// internal key fault the reasons are returned as they are: a wrong type
    /// an Update writes from the stored item arrives as a plain
    /// `ValidationError` and the other members keep their own outcomes beside
    /// it (measured: `[ValidationError, ConditionalCheckFailed]`).
    ///
    /// The storage layers resolve the faults supplied in the request (table
    /// keys, a Put item's index keys, an Update's literal types) over every
    /// member before any member is applied. They then run the members before
    /// the first Update member with an empty index-key literal and put that
    /// literal's reason at its position, so the reasons carry only empty
    /// values, the first of which decides, beside plain reasons: an empty
    /// value an earlier member wrote from the item answers ahead of the
    /// literal (measured 2026-11-01).
    ///
    /// # Errors
    ///
    /// Returns `Err(message)` when the request must be answered with a
    /// top-level `ValidationException` carrying `message`.
    pub fn resolve_key_faults(mut reasons: Vec<Self>) -> Result<Vec<Self>, String> {
        let Some(first) = reasons
            .iter()
            .position(|r| r.code == KEY_TYPE_MISMATCH_CODE || r.code == INDEX_KEY_EMPTY_ABORT_CODE)
        else {
            return Ok(reasons);
        };
        if reasons[first].code == INDEX_KEY_EMPTY_ABORT_CODE {
            return Err(reasons.swap_remove(first).message.unwrap_or_default());
        }
        for (i, reason) in reasons.iter_mut().enumerate() {
            if i == first {
                reason.code = "ValidationError".to_owned();
            } else {
                *reason = Self::none();
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

    // The kinds of fault a transaction member can carry into the resolver:
    // two internal key faults (a wrong type supplied in the request, an empty
    // index key) and the plain reasons beside them (a wrong type an Update
    // writes from the stored item, a failed condition). Each test below puts
    // one pair in both member orders; the expected answers are the measured
    // ones (2026-10-30 rows A2, B1, C1, D1, E1, F1, F2 and their reversals;
    // 2026-10-31 section 1, a Put item's empty key against a wrong type in
    // the request, which the first in request order decides).
    const COMPUTED: &str =
        "The update expression attempted to update the secondary index key to unsupported type";

    fn type_fault() -> CancellationReason {
        CancellationReason::key_type_mismatch("type")
    }

    fn empty_fault() -> CancellationReason {
        CancellationReason::index_key_empty_abort("empty key")
    }

    fn computed_fault() -> CancellationReason {
        CancellationReason::validation_error(COMPUTED)
    }

    fn failed_condition() -> CancellationReason {
        CancellationReason::condition_check_failed_with_item(None)
    }

    /// `[first, second]` and `[second, first]`, resolved.
    fn both_orders(
        first: fn() -> CancellationReason,
        second: fn() -> CancellationReason,
    ) -> [Result<Vec<CancellationReason>, String>; 2] {
        [
            CancellationReason::resolve_key_faults(vec![first(), second()]),
            CancellationReason::resolve_key_faults(vec![second(), first()]),
        ]
    }

    /// Assert a cancellation with `ValidationError` carrying the type fault's
    /// message at `at` and `None`, with no message, everywhere else.
    fn assert_type_fault_at(out: &Result<Vec<CancellationReason>, String>, at: usize) {
        let out = out.as_ref().expect("a cancellation");
        for (i, reason) in out.iter().enumerate() {
            if i == at {
                assert_eq!(reason.code, "ValidationError");
                assert_eq!(reason.message.as_deref(), Some("type"));
            } else {
                assert_eq!(reason.code, "None", "position {i}");
                assert_eq!(reason.message, None, "position {i}");
            }
        }
    }

    #[test]
    fn a_wrong_type_and_an_empty_key_answer_whichever_comes_first() {
        let [type_first, empty_first] = both_orders(type_fault, empty_fault);
        assert_type_fault_at(&type_first, 0);
        assert_eq!(empty_first.unwrap_err(), "empty key");
    }

    #[test]
    fn a_wrong_type_in_the_request_masks_a_computed_wrong_type_in_either_order() {
        let [type_first, computed_first] = both_orders(type_fault, computed_fault);
        assert_type_fault_at(&type_first, 0);
        assert_type_fault_at(&computed_first, 1);
    }

    #[test]
    fn a_wrong_type_in_the_request_masks_a_failed_condition_in_either_order() {
        let [type_first, condition_first] = both_orders(type_fault, failed_condition);
        assert_type_fault_at(&type_first, 0);
        assert_type_fault_at(&condition_first, 1);
    }

    #[test]
    fn an_empty_key_outranks_a_computed_wrong_type_in_either_order() {
        for out in both_orders(empty_fault, computed_fault) {
            assert_eq!(out.unwrap_err(), "empty key");
        }
    }

    #[test]
    fn an_empty_key_outranks_a_failed_condition_in_either_order() {
        for out in both_orders(empty_fault, failed_condition) {
            assert_eq!(out.unwrap_err(), "empty key");
        }
    }

    #[test]
    fn a_computed_wrong_type_and_a_failed_condition_keep_their_own_outcomes() {
        let [computed_first, condition_first] = both_orders(computed_fault, failed_condition);
        let computed_first = computed_first.unwrap();
        assert_eq!(
            codes(&computed_first),
            ["ValidationError", "ConditionalCheckFailed"]
        );
        assert_eq!(computed_first[0].message.as_deref(), Some(COMPUTED));
        let condition_first = condition_first.unwrap();
        assert_eq!(
            codes(&condition_first),
            ["ConditionalCheckFailed", "ValidationError"]
        );
        assert_eq!(condition_first[1].message.as_deref(), Some(COMPUTED));
    }

    #[test]
    fn only_the_first_of_two_wrong_types_is_reported() {
        for (first, second) in [("second", "third"), ("third", "second")] {
            let reasons = vec![
                CancellationReason::none(),
                CancellationReason::key_type_mismatch(first),
                CancellationReason::key_type_mismatch(second),
            ];
            let out = CancellationReason::resolve_key_faults(reasons).unwrap();
            assert_eq!(codes(&out), ["None", "ValidationError", "None"]);
            assert_eq!(out[1].message.as_deref(), Some(first));
            assert_eq!(out[2].message, None);
        }
    }

    #[test]
    fn the_first_of_two_empty_keys_answers() {
        for (first, second) in [
            ("put wording", "update wording"),
            ("update wording", "put wording"),
        ] {
            let reasons = vec![
                failed_condition(),
                CancellationReason::index_key_empty_abort(first),
                CancellationReason::index_key_empty_abort(second),
            ];
            assert_eq!(
                CancellationReason::resolve_key_faults(reasons).unwrap_err(),
                first
            );
        }
    }

    #[test]
    fn the_first_internal_fault_decides_among_plain_reasons() {
        let empty_before_type = vec![
            failed_condition(),
            empty_fault(),
            type_fault(),
            CancellationReason::validation_error("cannot apply"),
        ];
        assert_eq!(
            CancellationReason::resolve_key_faults(empty_before_type).unwrap_err(),
            "empty key"
        );
        let type_before_empty = vec![
            failed_condition(),
            type_fault(),
            empty_fault(),
            CancellationReason::validation_error("cannot apply"),
        ];
        assert_type_fault_at(
            &CancellationReason::resolve_key_faults(type_before_empty),
            1,
        );
    }
}
