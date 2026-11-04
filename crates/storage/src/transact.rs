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
#[derive(Clone, Copy)]
pub struct IdempotencyKey<'a> {
    /// Account that owns the token. Scopes the dedup keyspace.
    pub account_id: &'a str,
    /// The client-supplied `ClientRequestToken`.
    pub token: &'a str,
    /// Collision-resistant fingerprint of the request payload.
    pub fingerprint: &'a str,
}

/// What a request does with a `ClientRequestToken` already stored with the
/// fingerprint `stored`, see [`StoredToken::of`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredToken {
    /// The same request committed with this token: answer it as a replay.
    Replay,
    /// The same request was cancelled with this token after the
    /// request-level key checks: run it again.
    Rerun,
    /// A different request holds the token.
    Mismatch,
}

impl StoredToken {
    /// Classify the fingerprint `stored` with a token against the
    /// fingerprint of the request that carries the token now.
    ///
    /// The service keeps a token used when its request is cancelled after the
    /// request-level key checks (an empty index-key literal, an empty or
    /// wrong-typed value written from the item, a failed condition): a
    /// different request with the token then answers
    /// `IdempotentParameterMismatchException` and the same request answers
    /// what it answered before (measured 2026-11-01). The backends store
    /// such a token with [`cancelled_token_fingerprint`] and run the same
    /// request again when it returns.
    #[must_use]
    pub fn of(stored: &str, fingerprint: &str) -> Self {
        if stored == fingerprint {
            Self::Replay
        } else if stored.strip_prefix(CANCELLED_TOKEN_PREFIX) == Some(fingerprint) {
            Self::Rerun
        } else {
            Self::Mismatch
        }
    }
}

/// Prefix of the fingerprint stored for a token whose request was cancelled.
/// A request fingerprint is hex, so no request fingerprint carries it.
const CANCELLED_TOKEN_PREFIX: &str = "cancelled:";

/// The fingerprint a backend stores for a token whose request with
/// `fingerprint` was cancelled after the request-level key checks, so the
/// token stays used; see [`StoredToken::of`]. A request cancelled by those
/// checks leaves its token unused.
#[must_use]
pub fn cancelled_token_fingerprint(fingerprint: &str) -> String {
    format!("{CANCELLED_TOKEN_PREFIX}{fingerprint}")
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
/// The backends run this check over every member before the
/// `ClientRequestToken` is checked, as the service does (measured
/// 2026-11-01): a fault here is answered even when the token belongs to a
/// different request, and leaves the token unused.
///
/// `index_keys` and `index_attr_defs` are the table's secondary indexes and
/// the attribute definitions the backend checks their keys against. An
/// Update's empty literal is [`transact_op_empty_literal_checks`]; the values
/// an Update writes from the stored item are checked by the member's arm once
/// the statement is applied.
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

/// An Update member's empty index-key literal (`SET k = :v`, `:v` an empty
/// string or binary), carried as [`CancellationReason::index_key_empty_abort`]
/// with the update wording. The service reports it on the member's
/// statement, below every fault of [`transact_op_key_checks`] and ahead of
/// the member's condition, of an action that cannot apply and of anything the
/// statement writes from the item. An empty value written from the item or
/// from the fallback of `if_not_exists` is not checked here; the member's arm
/// finds it on the written item. See [`transact_first_empty_literal`] for how
/// it ranks against the other members.
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

/// The first member, in request order, that fails
/// [`transact_op_empty_literal_checks`], with its position and internal
/// reason; `member_checks` yields each member's result in request order.
///
/// Across members the service reports the first empty index-key value in
/// request order, whether a member supplies it as a literal or writes it from
/// the item (measured 2026-11-01). So with an empty literal at position `p`
/// the backends run only the members before `p`, as they run every member
/// otherwise, put this reason at `p` and resolve the reasons with
/// [`CancellationReason::resolve_key_faults`]: an empty value an earlier
/// member writes from the item answers ahead of the literal, and no member
/// after `p` is read or locked. A member whose own condition fails writes
/// nothing, so its empty `if_not_exists` fallback is never reported.
pub fn transact_first_empty_literal(
    member_checks: impl IntoIterator<Item = Result<(), CancellationReason>>,
) -> Option<(usize, CancellationReason)> {
    member_checks
        .into_iter()
        .enumerate()
        .find_map(|(i, check)| check.err().map(|reason| (i, reason)))
}

/// The request-level answer of [`transact_op_key_checks`] over every member:
/// `member_checks` yields each member's result in request order. When every
/// member passes the request goes on; otherwise
/// [`CancellationReason::resolve_key_faults`] decides it from those reasons
/// alone, before the token is checked and before any member is applied.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_token_is_a_replay_a_rerun_or_a_mismatch() {
        let fp = "0123456789abcdef";
        assert_eq!(StoredToken::of(fp, fp), StoredToken::Replay);
        assert_eq!(
            StoredToken::of(&cancelled_token_fingerprint(fp), fp),
            StoredToken::Rerun
        );
        assert_eq!(
            StoredToken::of("fedcba9876543210", fp),
            StoredToken::Mismatch
        );
        assert_eq!(
            StoredToken::of(&cancelled_token_fingerprint("fedcba9876543210"), fp),
            StoredToken::Mismatch
        );
    }

    #[test]
    fn the_first_empty_literal_in_request_order_is_returned() {
        let empty = |m: &str| Err(CancellationReason::index_key_empty_abort(m));
        assert!(transact_first_empty_literal([Ok(()), Ok(())]).is_none());
        let (p, reason) =
            transact_first_empty_literal([Ok(()), empty("second"), empty("third")]).unwrap();
        assert_eq!(p, 1);
        assert_eq!(reason.message.as_deref(), Some("second"));
    }
}
