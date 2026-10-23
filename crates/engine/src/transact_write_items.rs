// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TransactWriteItems` operation handler.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::build_expression_maps;
use crate::request_limits;
use crate::serialize_output;
use crate::stream_capture;
use crate::transact_write_helpers::{
    PreparedOp, compute_fingerprint, validate_client_request_token, validate_no_key_updates,
};
use crate::{DispatchMetrics, DispatchResult};
use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{Expr, ExpressionMaps, PathElement, UpdateAction};
use extenddb_core::types::{
    CancellationReason, Item, TransactWriteItem, TransactWriteItemsInput, TransactWriteItemsOutput,
};
use extenddb_core::validation::{
    validate_attribute_name_sizes, validate_attribute_values_nesting_depth,
    validate_item_nesting_depth, validate_item_size, validate_key_not_empty,
};

/// Handle a `TransactWriteItems` request.
///
/// Executes up to 100 write operations atomically. All operations succeed
/// or all are rolled back. Supports `Put`, `Delete`, `Update`, and `ConditionCheck`.
///
/// # Errors
///
/// Returns `TransactionCanceledException` if any condition fails.
/// Returns `ValidationException` for input validation failures.
/// Returns `IdempotentParameterMismatchException` for token conflicts.
pub async fn handle_transact_write_items(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    let input: TransactWriteItemsInput =
        serde_json::from_value(body.clone()).map_err(crate::deserialize_error)?;

    // Compute fingerprint keyed by the client request token for collision
    // resistance. Must happen after parsing so the token is available.
    let fingerprint = input
        .client_request_token
        .as_deref()
        .map(|t| compute_fingerprint(&body, t))
        .unwrap_or_default();

    // At least one and at most 100 items. The server runs the same check
    // before authorization.
    request_limits::check_transact_count(input.transact_items.len(), "TransactWriteItem")?;

    // Validate each item has exactly one operation
    for (i, twi) in input.transact_items.iter().enumerate() {
        let count = [
            twi.condition_check.is_some(),
            twi.put.is_some(),
            twi.delete.is_some(),
            twi.update.is_some(),
        ]
        .iter()
        .filter(|&&b| b)
        .count();
        if count != 1 {
            return Err(DynamoDbError::ValidationException(format!(
                "TransactItems[{i}] must contain exactly one of ConditionCheck, Put, Delete, or Update"
            )));
        }
    }

    // Validation runs in the order the service applies it, each phase over
    // every item before the next phase starts:
    //   1. per item, in order: expression grammar, the semantic walk, unused
    //      names and values, then the item's size (top-level ValidationException),
    //   2. table lookup (ResourceNotFoundException),
    //   3. the 4 MB request total (top-level ValidationException),
    //   4. key schema and key size (TransactionCanceledException with a
    //      ValidationError reason on the first offending item and None on
    //      every other; conditions are not evaluated when any item fails here),
    //   5. duplicate targets (top-level ValidationException).
    // Measured against DynamoDB for #108; a request with two defects reports
    // the earlier phase's error whichever item carries it.
    let parsed: Vec<ParsedExpressions> = input
        .transact_items
        .iter()
        .map(|twi| parse_write_expressions(twi, &ctx.limits))
        .collect::<Result<_, _>>()?;

    let mut prepared: Vec<PreparedOp> = Vec::with_capacity(input.transact_items.len());
    for (twi, exprs) in input.transact_items.iter().zip(parsed) {
        prepared.push(prepare_write_op(twi, exprs, ctx).await?);
    }

    // The 4 MB request total comes after every table is looked up and before
    // the key schema and duplicate checks (measured: eleven 390 KB items plus
    // a mistyped key, a duplicate, or a false condition all report the size).
    let total_size: usize = prepared
        .iter()
        .map(super::transact_write_helpers::PreparedOp::item_size)
        .sum();
    if total_size > 4 * 1024 * 1024 {
        return Err(DynamoDbError::ValidationException(
            "Transaction item size has exceeded the 4 MB limit".to_owned(),
        ));
    }

    // Key schema preflight: a key whose type does not match the schema, or a
    // key over the size limit, cancels the transaction with a ValidationError
    // reason on the FIRST offending item and None on every other item, even
    // when several keys fail (measured: items 1 and 3 mistyped report
    // [ValidationError, None, None]). (An EMPTY key value, by contrast, is a
    // top-level ValidationException and is handled in prepare_write_op.)
    // Running this before duplicate detection and before any condition is
    // evaluated matches the service: the other items report None, not
    // ConditionalCheckFailed.
    if let Some((index, reason)) = prepared
        .iter()
        .enumerate()
        .find_map(|(i, op)| op.key_schema_reason(&ctx.limits).map(|r| (i, r)))
    {
        let reasons: Vec<extenddb_core::types::CancellationReason> = (0..prepared.len())
            .map(|i| {
                if i == index {
                    reason.clone()
                } else {
                    extenddb_core::types::CancellationReason::none()
                }
            })
            .collect();
        return Err(storage_err_to_dynamo(
            extenddb_storage::error::StorageError::TransactionCanceled(reasons),
        ));
    }

    let mut seen_targets: HashSet<String> = HashSet::with_capacity(prepared.len());
    for op in &prepared {
        if !seen_targets.insert(op.canonical_target()) {
            return Err(DynamoDbError::ValidationException(
                "Transaction request cannot include multiple operations on one item".to_owned(),
            ));
        }
    }

    // Build storage operations
    // Vector-valued and search-schema validation failures surface as per-item
    // cancellation reasons rather than a top-level ValidationException.
    if let Some(reasons) = collect_vector_cancellation_reasons(&prepared) {
        let codes: Vec<String> = reasons.iter().map(|r| r.code.clone()).collect();
        return Err(DynamoDbError::TransactionCanceledException {
            message: format!(
                "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
                codes.join(", ")
            ),
            cancellation_reasons: reasons,
        });
    }

    let ops: Vec<extenddb_storage::TransactWriteOp<'_>> =
        prepared.iter().map(|p| p.to_storage_op()).collect();

    // Idempotency token validation (REQ-TRANSACT-003).
    // The token is passed to the storage layer and checked atomically within
    // the write transaction, guaranteeing that token storage and data writes
    // commit together.
    if let Some(ref token) = input.client_request_token {
        validate_client_request_token(token)?;
    }

    let idempotency =
        input
            .client_request_token
            .as_deref()
            .map(|t| extenddb_storage::IdempotencyKey {
                account_id: ctx.account_id.as_ref(),
                token: t,
                fingerprint: fingerprint.as_str(),
            });

    match ctx.storage.transact_write_items(&ops, idempotency).await {
        Ok(()) => {}
        Err(extenddb_storage::error::StorageError::IdempotentReplay) => {
            // A same-token replay re-reads the stored result rather than
            // writing, and its capacity says so: a transactional READ
            // recomputed against each item's size (2 x ceil(size/4KB)), not
            // the stored write magnitude relabelled. Measured: a ~1.5KB put
            // reports 4 WCU on the first call and 2 RCU on the replay, which
            // only agree below 1KB.
            let consumed_capacity = match input.return_consumed_capacity {
                extenddb_core::types::ReturnConsumedCapacity::None => None,
                rcc => {
                    let indexes = rcc == extenddb_core::types::ReturnConsumedCapacity::Indexes;
                    let mut per_table_rcu = capacity_helpers::PerTableCapacity::default();
                    for op in &prepared {
                        let rcu =
                            capacity_helpers::read_capacity_units(op.write_bytes(), true) * 2.0;
                        per_table_rcu.add(op.table_name(), rcu);
                    }
                    Some(
                        per_table_rcu
                            .iter()
                            .map(|(t, cu)| {
                                extenddb_core::types::ConsumedCapacity::transact_replay_read(
                                    t, cu, indexes,
                                )
                            })
                            .collect(),
                    )
                }
            };
            let output = TransactWriteItemsOutput {
                consumed_capacity,
                item_collection_metrics: None,
            };
            return Ok(DispatchResult::body_only(serialize_output(&output)?));
        }
        Err(extenddb_storage::error::StorageError::IdempotentMismatch) => {
            return Err(DynamoDbError::IdempotentParameterMismatchException(
                "The request uses the same client token as a previous, \
                 but different, request."
                    .to_owned(),
            ));
        }
        Err(e) => return Err(storage_err_to_dynamo(e)),
    }

    // Stream records are now captured atomically within the storage transaction.

    // Per-item WCU: round each item individually, then sum (M-1).
    let mut per_table_wcu = capacity_helpers::PerTableCapacity::default();
    let wcu: f64 = prepared
        .iter()
        .map(|op| {
            let item_wcu = capacity_helpers::write_capacity_units(op.write_bytes()) * 2.0; // transactions cost 2x
            per_table_wcu.add(op.table_name(), item_wcu);
            item_wcu
        })
        .sum();

    // NOTE: per-index (INDEXES) consumed-capacity breakdown for transactions is
    // deferred to the storage-layer capacity-reporting follow-up: the engine has
    // no resulting/old item for Update/Delete sub-ops, but DynamoDB reports a
    // per-index breakdown for those ops on GSI tables. Base-table aggregate
    // only for now.
    let consumed_capacity = capacity_helpers::transact_write_capacity(
        input.return_consumed_capacity,
        per_table_wcu.iter(),
    );

    // Collect ItemCollectionMetrics per table for write operations. When SIZE
    // was requested the member is present even with nothing to report (no
    // table has a local secondary index): the service returns `{}` there
    // (measured 2026-10-14), and omits the member only when it was not asked for.
    let mut all_icm: HashMap<String, Vec<extenddb_core::types::ItemCollectionMetrics>> =
        HashMap::new();
    for p in &prepared {
        if let Some(m) = p.item_collection_metric(input.return_item_collection_metrics) {
            all_icm
                .entry(p.table_name().to_owned())
                .or_default()
                .push(m);
        }
    }
    let item_collection_metrics = match input.return_item_collection_metrics {
        extenddb_core::types::ReturnItemCollectionMetrics::None => None,
        extenddb_core::types::ReturnItemCollectionMetrics::Size => Some(all_icm),
    };

    let output = TransactWriteItemsOutput {
        consumed_capacity,
        item_collection_metrics,
    };
    let body = serialize_output(&output)?;
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            write_capacity_units: wcu,
            ..Default::default()
        },
    })
}

/// Validate the vector-valued and search-schema attributes of each prepared
/// write operation. Returns per-item cancellation reasons when any operation
/// fails, or `None` when all pass.
fn collect_vector_cancellation_reasons(prepared: &[PreparedOp]) -> Option<Vec<CancellationReason>> {
    let mut reasons = Vec::with_capacity(prepared.len());
    let mut any_error = false;
    for op in prepared {
        let result = match op {
            PreparedOp::Put { key_info, item, .. } => {
                extenddb_core::validation::validate_vector_write(
                    item,
                    &key_info.vector_indexes,
                    &key_info.attribute_definitions,
                )
            }
            PreparedOp::Update {
                key_info,
                actions,
                maps,
                ..
            } => {
                let assigned = assigned_vector_attributes(actions, maps);
                extenddb_core::validation::validate_vector_write(
                    &assigned,
                    &key_info.vector_indexes,
                    &key_info.attribute_definitions,
                )
            }
            _ => Ok(()),
        };
        match result {
            Ok(()) => reasons.push(CancellationReason::none()),
            Err(DynamoDbError::ValidationException(message)) => {
                any_error = true;
                reasons.push(CancellationReason::validation_error(message));
            }
            Err(other) => {
                any_error = true;
                reasons.push(CancellationReason::validation_error(other.to_string()));
            }
        }
    }
    any_error.then_some(reasons)
}

/// Collect direct `SET attr = :value` assignments into a partial item for
/// vector validation.
///
/// This covers the bare-placeholder form ONLY, and deliberately so: it exists
/// to produce the correct per-item `CancellationReason` vector for the common
/// case, which the storage layer cannot reproduce because it aborts at the
/// first failing operation. It is NOT the authoritative vector check and must
/// never be treated as one. Any RHS that is not a bare placeholder
/// (`list_append`, `if_not_exists`, an attribute copy, or a form added later)
/// is invisible here and is caught instead by
/// `expression::apply_update_validated` on the evaluated image inside the
/// transaction, which every backend applies.
fn assigned_vector_attributes(actions: &[UpdateAction], maps: &ExpressionMaps) -> Item {
    let mut assigned = Item::new();
    for action in actions {
        if let UpdateAction::Set {
            path,
            value: Expr::Placeholder(placeholder),
        } = action
            && path.len() == 1
            && let PathElement::Attribute(name) = &path[0]
        {
            let resolved = name
                .strip_prefix('#')
                .and_then(|reference| maps.names.get(reference).map(String::as_str))
                .unwrap_or(name.as_str());
            if let Some(value) = maps.values.get(placeholder) {
                assigned.insert(resolved.to_owned(), value.clone());
            }
        }
    }
    assigned
}

/// A `TransactWriteItem`'s expressions, parsed and checked without touching
/// storage: grammar, operand types, every `#name` and `:value` reference
/// defined, and no unused names or values.
struct ParsedExpressions {
    maps: ExpressionMaps,
    condition: Option<Expr>,
    actions: Option<Vec<UpdateAction>>,
}

/// Phase 1 of `TransactWriteItems` validation: the expressions of one item.
///
/// Runs for every item before any table is looked up, so a malformed
/// expression on item three is reported ahead of a missing table on item one.
fn parse_write_expressions(
    twi: &TransactWriteItem,
    limits: &extenddb_core::limits::LimitsConfig,
) -> Result<ParsedExpressions, DynamoDbError> {
    use extenddb_core::expression::ExpressionKind;

    let (names, values, condition_expression, update_expression) = if let Some(put) = &twi.put {
        (
            put.expression_attribute_names.as_ref(),
            put.expression_attribute_values.as_ref(),
            put.condition_expression.as_deref(),
            None,
        )
    } else if let Some(del) = &twi.delete {
        (
            del.expression_attribute_names.as_ref(),
            del.expression_attribute_values.as_ref(),
            del.condition_expression.as_deref(),
            None,
        )
    } else if let Some(upd) = &twi.update {
        (
            upd.expression_attribute_names.as_ref(),
            upd.expression_attribute_values.as_ref(),
            upd.condition_expression.as_deref(),
            Some(upd.update_expression.as_str()),
        )
    } else if let Some(cc) = &twi.condition_check {
        (
            cc.expression_attribute_names.as_ref(),
            cc.expression_attribute_values.as_ref(),
            Some(cc.condition_expression.as_str()),
            None,
        )
    } else {
        // Unreachable: the one-operation check above already rejected this item.
        return Err(DynamoDbError::ValidationException(
            "TransactWriteItem must contain exactly one operation".to_owned(),
        ));
    };

    // Inside a transaction the service checks an Update item's
    // UpdateExpression (syntax, the ordered semantic walk, overlapping paths)
    // before its ConditionExpression, the reverse of UpdateItem; then the
    // unused names and values; then the item's size (measured 2026-10-14:
    // an oversize Put with an unused value reports the value, and an
    // oversize item one reports its size ahead of the next item's defects).
    let maps = build_expression_maps(names, values);
    let actions = match update_expression {
        Some(expr) => Some(crate::expression_helpers::parse_update_expr_checked(
            expr, &maps, limits,
        )?),
        None => None,
    };
    let condition = match condition_expression {
        Some(expr) if !expr.is_empty() => Some(crate::expression_helpers::parse_condition_checked(
            expr,
            &maps,
            limits,
            true,
            ExpressionKind::Condition,
        )?),
        _ => None,
    };
    // Transactions accept names/values with no expression (unlike single-item
    // APIs); only check for unused refs when some expression is present.
    if condition.is_some() || actions.is_some() {
        let exprs: Vec<&Expr> = condition.iter().collect();
        let action_refs: Vec<&UpdateAction> = actions.iter().flatten().collect();
        extenddb_core::expression::validate_unused_attributes(
            &maps.names,
            &maps.values,
            &exprs,
            &action_refs,
            &HashSet::new(),
            &HashSet::new(),
        )?;
    }
    if let Some(put) = &twi.put {
        validate_item_size(&put.item, limits.max_item_size_bytes)?;
    }
    Ok(ParsedExpressions {
        maps,
        condition,
        actions,
    })
}

/// Phase 2 of `TransactWriteItems` validation: look the table up and validate
/// the item or key against it, returning a `PreparedOp`.
async fn prepare_write_op(
    twi: &TransactWriteItem,
    exprs: ParsedExpressions,
    ctx: &OperationContext,
) -> Result<PreparedOp, DynamoDbError> {
    let ParsedExpressions {
        maps,
        condition,
        actions,
    } = exprs;

    if let Some(put) = &twi.put {
        let key_info = ctx
            .table_key_info_uncached(&put.table_name)
            .await
            .map_err(storage_err_to_dynamo)?;
        validate_item_nesting_depth(&put.item)?;
        validate_attribute_name_sizes(&put.item, &ctx.limits)?;
        validate_key_not_empty(&put.item, &key_info.key_schema)?;
        let stream =
            stream_capture::stream_view_type(&key_info).map(|vt| extenddb_storage::StreamCapture {
                view_type: vt,
                user_identity: None,
                region: ctx.region.clone(),
            });
        return Ok(PreparedOp::Put {
            key_info,
            item: put.item.clone(),
            condition,
            maps,
            return_values_on_ccf: put.return_values_on_condition_check_failure,
            stream,
        });
    }

    if let Some(del) = &twi.delete {
        let key_info = ctx
            .table_key_info_uncached(&del.table_name)
            .await
            .map_err(storage_err_to_dynamo)?;
        // Empty or oversize key values are up-front input validation in
        // DynamoDB (a top-level ValidationException), unlike a key type
        // mismatch which surfaces as a per-item cancellation reason.
        validate_key_not_empty(&del.key, &key_info.key_schema)?;
        let stream =
            stream_capture::stream_view_type(&key_info).map(|vt| extenddb_storage::StreamCapture {
                view_type: vt,
                user_identity: None,
                region: ctx.region.clone(),
            });
        return Ok(PreparedOp::Delete {
            key_info,
            key: del.key.clone(),
            condition,
            maps,
            return_values_on_ccf: del.return_values_on_condition_check_failure,
            stream,
        });
    }

    if let Some(upd) = &twi.update {
        let key_info = ctx
            .table_key_info_uncached(&upd.table_name)
            .await
            .map_err(storage_err_to_dynamo)?;
        // Empty or oversize key values are up-front input validation in
        // DynamoDB (a top-level ValidationException), unlike a key type
        // mismatch which surfaces as a per-item cancellation reason.
        validate_key_not_empty(&upd.key, &key_info.key_schema)?;
        let actions = actions.unwrap_or_default();
        validate_no_key_updates(&actions, &key_info, &maps)?;

        // Validate nesting depth of EAV values that get stored via SET actions.
        {
            let mut placeholders: Vec<String> = Vec::new();
            for action in &actions {
                if let extenddb_core::expression::UpdateAction::Set { value, .. } = action {
                    extenddb_core::expression::collect_value_placeholders(value, &mut placeholders);
                }
            }
            let stored: Vec<&extenddb_core::types::AttributeValue> = placeholders
                .iter()
                .filter_map(|name| maps.values.get(name))
                .collect();
            validate_attribute_values_nesting_depth(stored)?;
        }
        let stream =
            stream_capture::stream_view_type(&key_info).map(|vt| extenddb_storage::StreamCapture {
                view_type: vt,
                user_identity: None,
                region: ctx.region.clone(),
            });
        return Ok(PreparedOp::Update {
            key_info,
            key: upd.key.clone(),
            actions,
            condition,
            maps,
            return_values_on_ccf: upd.return_values_on_condition_check_failure,
            stream,
        });
    }

    if let Some(cc) = &twi.condition_check {
        let key_info = ctx
            .table_key_info_uncached(&cc.table_name)
            .await
            .map_err(storage_err_to_dynamo)?;
        // Empty or oversize key values are up-front input validation in
        // DynamoDB (a top-level ValidationException), unlike a key type
        // mismatch which surfaces as a per-item cancellation reason.
        validate_key_not_empty(&cc.key, &key_info.key_schema)?;
        let Some(condition) = condition else {
            // Unreachable: parse_write_expressions parsed the required expression.
            return Err(DynamoDbError::ValidationException(
                "ConditionCheck requires a ConditionExpression".to_owned(),
            ));
        };
        return Ok(PreparedOp::ConditionCheck {
            key_info,
            key: cc.key.clone(),
            condition,
            maps,
            return_values_on_ccf: cc.return_values_on_condition_check_failure,
        });
    }

    // Should be unreachable due to earlier validation
    Err(DynamoDbError::ValidationException(
        "TransactWriteItem must contain exactly one operation".to_owned(),
    ))
}
