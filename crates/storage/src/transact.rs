// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transaction operation types for `DataEngine::transact_get_items` and
//! `DataEngine::transact_write_items`.

use extenddb_core::expression::{Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{
    AttributeDefinition, CancellationReason, Item, ReturnValuesOnConditionCheckFailure,
    TableKeyInfo,
};
use extenddb_core::validation::{self, IndexKeyRef, SecondaryIndexEmptyContext};

use crate::StreamCapture;
use crate::error::StorageError;

/// Idempotency scope for a `TransactWriteItems` request.
///
/// A `ClientRequestToken` is unique per account in Amazon DynamoDB, not
/// globally, so the token is stored and looked up under `(account_id, token)`.
/// `fingerprint` distinguishes a genuine replay from a reused token carrying a
/// different request, within that same account scope.
pub struct IdempotencyKey<'a> {
    /// Account that owns the token. Scopes the dedup keyspace.
    pub account_id: &'a str,
    /// The client-supplied `ClientRequestToken`.
    pub token: &'a str,
    /// Collision-resistant fingerprint of the request payload.
    pub fingerprint: &'a str,
}

/// A single get operation within a transactional read.
pub struct TransactGetOp<'a> {
    /// Table metadata.
    pub key_info: &'a TableKeyInfo,
    /// Primary key to fetch.
    pub key: &'a Item,
}

/// A single write operation within a transactional write.
pub enum TransactWriteOp<'a> {
    /// Put an item (unconditional or with condition).
    Put {
        /// Table metadata.
        key_info: &'a TableKeyInfo,
        /// The full item to write.
        item: &'a Item,
        /// Optional condition expression.
        condition: Option<&'a Expr>,
        /// Expression maps for condition evaluation.
        maps: &'a ExpressionMaps,
        /// Whether to return the old item on condition failure.
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        /// Stream capture parameters (inserted in the same transaction).
        stream: Option<StreamCapture>,
    },
    /// Delete an item by key (unconditional or with condition).
    Delete {
        /// Table metadata.
        key_info: &'a TableKeyInfo,
        /// Primary key to delete.
        key: &'a Item,
        /// Optional condition expression.
        condition: Option<&'a Expr>,
        /// Expression maps for condition evaluation.
        maps: &'a ExpressionMaps,
        /// Whether to return the old item on condition failure.
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        /// Stream capture parameters (inserted in the same transaction).
        stream: Option<StreamCapture>,
    },
    /// Update an item by key with update actions.
    Update {
        /// Table metadata.
        key_info: &'a TableKeyInfo,
        /// Primary key of the item.
        key: &'a Item,
        /// Parsed update actions.
        actions: &'a [UpdateAction],
        /// Optional condition expression.
        condition: Option<&'a Expr>,
        /// Expression maps for condition and update evaluation.
        maps: &'a ExpressionMaps,
        /// Whether to return the old item on condition failure.
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        /// Stream capture parameters (inserted in the same transaction).
        stream: Option<StreamCapture>,
    },
    /// Check a condition without mutating.
    ConditionCheck {
        /// Table metadata.
        key_info: &'a TableKeyInfo,
        /// Primary key of the item to check.
        key: &'a Item,
        /// Condition expression (required).
        condition: &'a Expr,
        /// Expression maps for condition evaluation.
        maps: &'a ExpressionMaps,
        /// Whether to return the old item on condition failure.
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
    },
}

/// The key checks a transaction member can fail from the request alone,
/// before any row is read or locked: the table key's shape and types, and the
/// secondary-index keys the member supplies itself (a Put item's attributes,
/// an Update statement's literal operands). Faults carry the internal reasons
/// that [`CancellationReason::resolve_key_faults`] ranks: a wrong type as
/// [`CancellationReason::key_type_mismatch`], a Put item's empty index key as
/// [`CancellationReason::index_key_empty_abort`] with the Put wording.
///
/// `index_keys` and `index_attr_defs` are the table's secondary indexes and
/// the attribute definitions the backend checks their keys against. An
/// Update's empty literal is the second check,
/// [`transact_op_empty_literal_checks`]; the values an Update writes from the
/// stored item are checked by the member's arm once the statement is applied.
///
/// # Errors
///
/// Returns the member's first fault as its internal cancellation reason.
pub fn transact_op_key_checks(
    op: &TransactWriteOp<'_>,
    index_keys: &[IndexKeyRef<'_>],
    index_attr_defs: &[AttributeDefinition],
) -> Result<(), CancellationReason> {
    let type_fault = |e: extenddb_core::error::DynamoDbError| {
        CancellationReason::key_type_mismatch(e.to_string())
    };
    match op {
        TransactWriteOp::Put { key_info, item, .. } => {
            validation::validate_item_keys(
                item,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(type_fault)?;
            validation::validate_index_key_types(item, index_keys, index_attr_defs)
                .map_err(type_fault)?;
            validation::validate_index_key_not_empty(
                item,
                index_keys,
                SecondaryIndexEmptyContext::Item,
            )
            .map_err(|e| CancellationReason::index_key_empty_abort(e.to_string()))
        }
        TransactWriteOp::Update {
            key_info,
            key,
            actions,
            maps,
            ..
        } => {
            validation::validate_batch_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(type_fault)?;
            validation::validate_index_key_literal_operands(
                actions,
                index_keys,
                index_attr_defs,
                maps,
            )
            .map_err(type_fault)
        }
        TransactWriteOp::Delete { key_info, key, .. }
        | TransactWriteOp::ConditionCheck { key_info, key, .. } => {
            validation::validate_batch_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(type_fault)
        }
    }
}

/// The second request-level check of a transaction member, run only when no
/// member failed [`transact_op_key_checks`]: an Update member's empty
/// index-key literal (`SET k = :v`, `:v` an empty string or binary), carried
/// as [`CancellationReason::index_key_empty_abort`] with the update wording.
/// The service reports it on the statement, below every fault of the first
/// check and ahead of the member's condition, of an action that cannot apply
/// and of anything computed from the item, on every member. An empty value
/// written from the item or from the fallback of `if_not_exists` is not
/// checked here; the member's arm finds it on the written item.
///
/// # Errors
///
/// Returns the member's empty literal as its internal cancellation reason.
pub fn transact_op_empty_literal_checks(
    op: &TransactWriteOp<'_>,
    index_keys: &[IndexKeyRef<'_>],
) -> Result<(), CancellationReason> {
    match op {
        TransactWriteOp::Update { actions, maps, .. } => {
            validation::validate_index_key_empty_literals(actions, index_keys, maps)
                .map_err(|e| CancellationReason::index_key_empty_abort(e.to_string()))
        }
        TransactWriteOp::Put { .. }
        | TransactWriteOp::Delete { .. }
        | TransactWriteOp::ConditionCheck { .. } => Ok(()),
    }
}

/// The request-level answer of one check over every member: `member_checks`
/// yields each member's result in request order, from
/// [`transact_op_key_checks`] for the first pass and from
/// [`transact_op_empty_literal_checks`] for the second. When every member
/// passes the request goes on; otherwise
/// [`CancellationReason::resolve_key_faults`] decides it from those reasons
/// alone, before any member is applied.
///
/// # Errors
///
/// Returns `StorageError::TransactionCanceled` or `StorageError::Validation`
/// as the resolver decides when any member failed.
pub fn transact_key_check_pass(
    member_checks: impl IntoIterator<Item = Result<(), CancellationReason>>,
) -> Result<(), StorageError> {
    let mut any_fault = false;
    let reasons: Vec<CancellationReason> = member_checks
        .into_iter()
        .map(|check| match check {
            Ok(()) => CancellationReason::none(),
            Err(reason) => {
                any_fault = true;
                reason
            }
        })
        .collect();
    if !any_fault {
        return Ok(());
    }
    Err(match CancellationReason::resolve_key_faults(reasons) {
        Ok(reasons) => StorageError::TransactionCanceled(reasons),
        Err(message) => StorageError::Validation(message),
    })
}
