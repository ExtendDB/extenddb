// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Payload validation and single-item write helpers shared by
//! `handle_put_item` and the bulk put runner.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{Expr, ExpressionMaps, validate_unused_attributes};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeValue, ConditionalOperator, ExpectedAttributeValue, Item, TableKeyInfo,
    item_size_bytes,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BulkRowMarker, StorageEngine, StreamCapture};

use crate::expression_helpers::resolve_condition;
use crate::stream_capture;

/// Which check of [`validate_put_payload`] fired for a rejected item.
///
/// `handle_put_item` discards it; the bulk put error report decides by it
/// whether the record embeds the submitted image. Matching on the kind
/// instead of the message text means a wording change in the validation
/// layer fails a kind assertion rather than silently flipping the rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PutPayloadFault {
    /// A key attribute is absent from the item.
    MissingKey,
    /// A key attribute is present with the wrong type.
    KeyTypeMismatch,
    /// A secondary index key attribute has the wrong type or is an empty
    /// string. Reported by the bulk admission step only; `handle_put_item`
    /// meets these in storage.
    IndexKey,
    /// A primary-key attribute holds an empty string or an empty binary
    /// value. Told apart from the other key rules because the bulk record
    /// for it carries the submitted image (measured on the service),
    /// where the base-key type mismatch does not.
    KeyEmptyValue,
    /// The item is over the item size limit.
    Oversized,
    /// An attribute value nests deeper than the item nesting limit.
    NestingDepth,
    /// Any other item check.
    Other,
}

/// A rejected put payload: the check that fired and the error `PutItem`
/// answers for the same item.
#[derive(Debug)]
pub(crate) struct PutPayloadError {
    pub kind: PutPayloadFault,
    pub error: DynamoDbError,
}

impl PutPayloadError {
    fn other(error: DynamoDbError) -> Self {
        Self {
            kind: PutPayloadFault::Other,
            error,
        }
    }
}

/// The fault a primary-key value check reports: the empty-value rule is
/// named so the bulk record can attach the submitted image to it, which the
/// byte-size rule does not get. Both rules share one validator, so the rule
/// is read from the message it produced; the bulk row test for the
/// empty-string key asserts that message, so a reword of the
/// validator fails there rather than silently dropping the image.
fn key_fault(error: DynamoDbError) -> PutPayloadError {
    if error.to_string().contains("cannot contain an empty") {
        PutPayloadError {
            kind: PutPayloadFault::KeyEmptyValue,
            error,
        }
    } else {
        PutPayloadError::other(error)
    }
}

/// Resolve the condition fields of a put: `resolve_condition`, the
/// unused-attribute check (skipped when a non-empty `Expected` supplies the
/// condition, whose placeholders are merged rather than referenced), then
/// the nesting-depth check on the expression attribute values. One sequence
/// for `handle_put_item` and the bulk put runner, so a row's condition
/// faults carry the messages `PutItem` answers.
///
/// The nesting-depth limit applies to every `ExpressionAttributeValue` on
/// the single-item write paths, condition-only placeholders included:
/// measured on the service (us-east-1), a 32-level value referenced solely by a
/// `ConditionExpression` is rejected on `PutItem`, `DeleteItem`, and
/// `UpdateItem` alike, while 31 levels is accepted. `TransactWriteItems`
/// deliberately does NOT get this check: the same 32-level condition-only
/// value is accepted inside a transaction on every sub-op shape (also
/// measured).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for expression syntax,
/// unused-attribute, and nesting-depth faults.
pub(crate) fn resolve_put_condition(
    expression: Option<&str>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    expected: Option<&HashMap<String, ExpectedAttributeValue>>,
    conditional_operator: Option<ConditionalOperator>,
    limits: &LimitsConfig,
) -> Result<(Option<Expr>, ExpressionMaps), DynamoDbError> {
    let (condition, maps) = resolve_condition(
        expression,
        names,
        values,
        expected,
        conditional_operator,
        limits,
    )?;
    if expected.is_none_or(HashMap::is_empty) {
        let exprs: Vec<&Expr> = condition.iter().collect();
        validate_unused_attributes(
            &maps.names,
            &maps.values,
            &exprs,
            &[],
            &HashSet::new(),
            &HashSet::new(),
        )?;
    }
    if let Some(values) = values {
        extenddb_core::validation::validate_attribute_values_nesting_depth(values.values())?;
    }
    Ok((condition, maps))
}

/// Validate one item for a put: attribute name sizes, keys, numbers,
/// nesting depth, item size, and key sizes, in that order, then the vector
/// write check, reporting which check fired. One sequence for
/// `handle_put_item` and the bulk put runner, so a row's item faults carry
/// the messages `PutItem` answers. The name sizes come first because
/// `PutItem` refuses a long name ahead of its table lookup, with the
/// standalone request's wording; `handle_put_item` has run that check
/// before it calls this.
///
/// # Errors
///
/// Returns the fired [`PutPayloadFault`] and the error `PutItem` answers
/// for the same item.
pub(crate) fn validate_put_payload(
    item: &Item,
    key_info: &TableKeyInfo,
    limits: &LimitsConfig,
) -> Result<(), PutPayloadError> {
    use extenddb_core::validation::{self, AttributeNameRequestForm, ItemKeyFault};
    validation::validate_attribute_name_sizes(item, limits, AttributeNameRequestForm::SingleItem)
        .map_err(PutPayloadError::other)?;
    validation::validate_item_keys_reporting(
        item,
        &key_info.key_schema,
        &key_info.attribute_definitions,
    )
    .map_err(|(fault, error)| PutPayloadError {
        kind: match fault {
            ItemKeyFault::MissingKey => PutPayloadFault::MissingKey,
            ItemKeyFault::TypeMismatch => PutPayloadFault::KeyTypeMismatch,
        },
        error,
    })?;
    validation::validate_item_numbers(item).map_err(PutPayloadError::other)?;
    validation::validate_item_nesting_depth(item).map_err(|error| PutPayloadError {
        kind: PutPayloadFault::NestingDepth,
        error,
    })?;
    if item_size_bytes(item) > limits.max_item_size_bytes {
        return Err(PutPayloadError {
            kind: PutPayloadFault::Oversized,
            error: DynamoDbError::ValidationException(
                "Item size has exceeded the maximum allowed size".to_owned(),
            ),
        });
    }
    validation::validate_key_sizes(item, &key_info.key_schema, limits).map_err(key_fault)?;
    validation::validate_vector_write(
        item,
        &key_info.vector_indexes,
        &key_info.attribute_definitions,
    )
    .map_err(PutPayloadError::other)?;
    Ok(())
}

/// The stream capture a write to this table records, when the table's
/// stream is enabled: the view type from the key info, no caller identity,
/// the server's region.
pub(crate) fn stream_capture_for(
    key_info: &TableKeyInfo,
    region: &Arc<str>,
) -> Option<StreamCapture> {
    stream_capture::stream_view_type(key_info).map(|vt| StreamCapture {
        view_type: vt,
        user_identity: None,
        region: Arc::clone(region),
    })
}

/// Apply one put to storage the way `handle_put_item` does: the stream
/// capture is built from the table's stream specification and the region,
/// with no user identity, and inserted in the same transaction as the data
/// write; the bulk row marker is passed through to the row-outcome ledger.
/// The replaced item is fetched when the caller asks for it or a stream
/// record needs it.
///
/// # Errors
///
/// Returns the storage error unmapped; each caller owns its own mapping
/// (`handle_put_item` maps to `DynamoDbError`, the bulk runner matches the
/// variants directly).
#[allow(clippy::too_many_arguments)] // the write's inputs, per the shared core contract
pub(crate) async fn apply_put(
    storage: &dyn StorageEngine,
    region: &Arc<str>,
    key_info: &TableKeyInfo,
    item: Item,
    condition: Option<&Expr>,
    maps: &ExpressionMaps,
    return_old: bool,
    marker: Option<&BulkRowMarker>,
) -> Result<Option<Item>, StorageError> {
    let stream = stream_capture_for(key_info, region);
    let fetch_old = return_old || stream.is_some();
    storage
        .put_item(
            key_info,
            item,
            fetch_old,
            condition,
            maps,
            stream.as_ref(),
            marker,
        )
        .await
}

#[cfg(test)]
mod tests;
