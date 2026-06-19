// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DataEngine` implementation for the DynamoDB-at-home backend.
//!
//! Every item-level operation is forwarded to a real DynamoDB endpoint via the
//! AWS SDK.  The `stream: Option<&StreamCapture>` parameter is intentionally
//! ignored throughout — DynamoDB generates its own stream records natively;
//! ExtendDB does not synthesise them here.

use futures::future::BoxFuture;

use aws_sdk_dynamodb::types::{
    ConditionCheck, Delete, Get, Put, ReturnValue, TransactGetItem, TransactWriteItem, Update,
};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_core::expression::{ExpressionMaps, KeyCondition, UpdateAction, Expr};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, ItemPairResult, QueryResult, StreamCapture, TransactGetOp, TransactWriteOp};

use crate::DynamoEngine;
use crate::encoding::{item_from_sdk, item_to_sdk};
use crate::errors::from_sdk_error;
use crate::expression::Renderer;

// ── Helper — maps a sdk BuildError to StorageError::Internal ─────────────────

fn sdk_build_err<E: std::fmt::Display>(e: E) -> StorageError {
    StorageError::Internal(e.to_string())
}

// ── DataEngine ────────────────────────────────────────────────────────────────

impl DataEngine for DynamoEngine {
    // ── put_item ──────────────────────────────────────────────────────────────

    fn put_item(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let physical = self.namer.physical(&key_info.account_id, &key_info.table_name);
        let sdk_item = item_to_sdk(&item);

        // Clone the condition and maps so they can cross the async boundary.
        let condition = condition.cloned();
        let maps = maps.clone();

        Box::pin(async move {
            let mut req = self
                .client
                .put_item()
                .table_name(physical)
                .set_item(Some(sdk_item));

            if let Some(cond) = &condition {
                let mut r = Renderer::new();
                let expr = r.render_condition(cond, &maps)?;
                req = req.condition_expression(expr);
                if !r.names().is_empty() {
                    req = req.set_expression_attribute_names(Some(r.names().clone()));
                }
                if !r.values().is_empty() {
                    req = req.set_expression_attribute_values(Some(r.values().clone()));
                }
            }

            if return_old {
                req = req.return_values(ReturnValue::AllOld);
            }

            let out = req.send().await.map_err(from_sdk_error)?;

            if return_old {
                Ok(out.attributes().map(|m| item_from_sdk(m.clone())))
            } else {
                Ok(None)
            }
        })
    }

    // ── get_item ──────────────────────────────────────────────────────────────

    fn get_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let physical = self.namer.physical(&key_info.account_id, &key_info.table_name);
        let sdk_key = item_to_sdk(key);

        Box::pin(async move {
            let out = self
                .client
                .get_item()
                .table_name(physical)
                .set_key(Some(sdk_key))
                .send()
                .await
                .map_err(from_sdk_error)?;

            Ok(out.item().map(|m| item_from_sdk(m.clone())))
        })
    }

    // ── delete_item ───────────────────────────────────────────────────────────

    fn delete_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let physical = self.namer.physical(&key_info.account_id, &key_info.table_name);
        let sdk_key = item_to_sdk(key);
        let condition = condition.cloned();
        let maps = maps.clone();

        Box::pin(async move {
            let mut req = self
                .client
                .delete_item()
                .table_name(physical)
                .set_key(Some(sdk_key));

            if let Some(cond) = &condition {
                let mut r = Renderer::new();
                let expr = r.render_condition(cond, &maps)?;
                req = req.condition_expression(expr);
                if !r.names().is_empty() {
                    req = req.set_expression_attribute_names(Some(r.names().clone()));
                }
                if !r.values().is_empty() {
                    req = req.set_expression_attribute_values(Some(r.values().clone()));
                }
            }

            if return_old {
                req = req.return_values(ReturnValue::AllOld);
            }

            let out = req.send().await.map_err(from_sdk_error)?;

            if return_old {
                Ok(out.attributes().map(|m| item_from_sdk(m.clone())))
            } else {
                Ok(None)
            }
        })
    }

    // ── update_item ───────────────────────────────────────────────────────────
    //
    // CONCERN: DynamoDB's UpdateItem can return either ALL_OLD or ALL_NEW in a
    // single call — not both simultaneously.  When both `return_old` and
    // `return_new` are true, we prefer ALL_NEW (new slot populated, old = None).
    // The caller should not request both unless it can tolerate the missing old.

    #[allow(clippy::too_many_arguments)]
    fn update_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, ItemPairResult> {
        let physical = self.namer.physical(&key_info.account_id, &key_info.table_name);
        let sdk_key = item_to_sdk(key);
        let actions = actions.to_vec();
        let condition = condition.cloned();
        let maps = maps.clone();

        Box::pin(async move {
            // Use a single Renderer so update and condition tokens don't collide.
            let mut r = Renderer::new();
            let update_expr = r.render_update(&actions, &maps)?;

            let mut req = self
                .client
                .update_item()
                .table_name(physical)
                .set_key(Some(sdk_key))
                .update_expression(update_expr);

            if let Some(cond) = &condition {
                let cond_expr = r.render_condition(cond, &maps)?;
                req = req.condition_expression(cond_expr);
            }

            if !r.names().is_empty() {
                req = req.set_expression_attribute_names(Some(r.names().clone()));
            }
            if !r.values().is_empty() {
                req = req.set_expression_attribute_values(Some(r.values().clone()));
            }

            // DynamoDB supports only one ReturnValues mode per call.
            // Prefer ALL_NEW when both are requested (see CONCERN above).
            let (rv, want_new, want_old) = if return_new {
                (ReturnValue::AllNew, true, false)
            } else if return_old {
                (ReturnValue::AllOld, false, true)
            } else {
                (ReturnValue::None, false, false)
            };

            req = req.return_values(rv);

            let out = req.send().await.map_err(from_sdk_error)?;

            let item = out.attributes().map(|m| item_from_sdk(m.clone()));

            if want_new {
                Ok((None, item))
            } else if want_old {
                Ok((item, None))
            } else {
                Ok((None, None))
            }
        })
    }

    // ── query ─────────────────────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn query(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        let physical = self.namer.physical(&key_info.account_id, &key_info.table_name);
        let key_condition = key_condition.clone();
        let maps = maps.clone();
        let esk = exclusive_start_key.cloned();
        let index_name = index_name.map(|s| s.to_owned());

        Box::pin(async move {
            let mut r = Renderer::new();
            let kc_expr = r.render_key_condition(&key_condition, &maps)?;

            let mut req = self
                .client
                .query()
                .table_name(physical)
                .key_condition_expression(kc_expr)
                .scan_index_forward(forward);

            if !r.names().is_empty() {
                req = req.set_expression_attribute_names(Some(r.names().clone()));
            }
            if !r.values().is_empty() {
                req = req.set_expression_attribute_values(Some(r.values().clone()));
            }

            if let Some(l) = limit {
                req = req.limit(i32::try_from(l).unwrap_or(i32::MAX));
            }

            if let Some(k) = esk {
                req = req.set_exclusive_start_key(Some(item_to_sdk(&k)));
            }

            if let Some(n) = index_name {
                req = req.index_name(n);
            }

            let out = req.send().await.map_err(from_sdk_error)?;

            let items: Vec<Item> = out
                .items()
                .iter()
                .map(|m| item_from_sdk(m.clone()))
                .collect();

            let lek = out
                .last_evaluated_key()
                .map(|m| item_from_sdk(m.clone()));

            Ok((items, lek))
        })
    }

    // ── scan ──────────────────────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn scan(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        let physical = self.namer.physical(&key_info.account_id, &key_info.table_name);
        let esk = exclusive_start_key.cloned();
        let index_name = index_name.map(|s| s.to_owned());

        Box::pin(async move {
            let mut req = self.client.scan().table_name(physical);

            if let Some(l) = limit {
                req = req.limit(i32::try_from(l).unwrap_or(i32::MAX));
            }

            if let Some(k) = esk {
                req = req.set_exclusive_start_key(Some(item_to_sdk(&k)));
            }

            if let Some(seg) = segment {
                req = req.segment(i32::try_from(seg).unwrap_or(0));
            }

            if let Some(ts) = total_segments {
                req = req.total_segments(i32::try_from(ts).unwrap_or(1));
            }

            if let Some(n) = index_name {
                req = req.index_name(n);
            }

            let out = req.send().await.map_err(from_sdk_error)?;

            let items: Vec<Item> = out
                .items()
                .iter()
                .map(|m| item_from_sdk(m.clone()))
                .collect();

            let lek = out
                .last_evaluated_key()
                .map(|m| item_from_sdk(m.clone()));

            Ok((items, lek))
        })
    }

    // ── transact_get_items ────────────────────────────────────────────────────

    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        // Capture all data before crossing the async boundary.
        let items: Result<Vec<TransactGetItem>, StorageError> = ops
            .iter()
            .map(|op| {
                let physical = self.namer.physical(&op.key_info.account_id, &op.key_info.table_name);
                let sdk_key = item_to_sdk(op.key);
                let get = Get::builder()
                    .table_name(physical)
                    .set_key(Some(sdk_key))
                    .build()
                    .map_err(sdk_build_err)?;
                // TransactGetItem::builder().build() is infallible
                let tgi = TransactGetItem::builder().get(get).build();
                Ok(tgi)
            })
            .collect();

        Box::pin(async move {
            let tgi_vec = items?;

            let out = self
                .client
                .transact_get_items()
                .set_transact_items(Some(tgi_vec))
                .send()
                .await
                .map_err(from_sdk_error)?;

            let results: Vec<Option<Item>> = out
                .responses()
                .iter()
                .map(|resp| resp.item().map(|m| item_from_sdk(m.clone())))
                .collect();

            Ok(results)
        })
    }

    // ── transact_write_items ──────────────────────────────────────────────────
    //
    // Token interpretation: `token` is `(tok, fp)` where `tok` is the client
    // request token (idempotency key) forwarded to DynamoDB's
    // `client_request_token`, and `fp` is the fingerprint (request hash) used
    // only by Postgres for mismatch detection.  DynamoDB manages idempotency
    // natively (~10-minute window), so we forward `tok` and ignore `fp`.
    //
    // This mirrors the postgres interpretation in
    // `crates/storage-postgres/src/data/transactions.rs`:
    //   `if let Some((tok, fp)) = token { check_idempotency_token_in_tx(&mut tx, tok, fp) }`
    // where `tok` is the token string passed to the DB and `fp` is its fingerprint.

    fn transact_write_items(
        &self,
        ops: &[TransactWriteOp<'_>],
        token: Option<(&str, &str)>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let client_token = token.map(|(tok, _fp)| tok.to_owned());

        let twi_vec: Result<Vec<TransactWriteItem>, StorageError> = ops
            .iter()
            .map(|op| build_transact_write_item(op, &self.namer))
            .collect();

        Box::pin(async move {
            let twi_vec = twi_vec?;

            let mut req = self
                .client
                .transact_write_items()
                .set_transact_items(Some(twi_vec));

            if let Some(t) = client_token {
                req = req.client_request_token(t);
            }

            req.send().await.map_err(from_sdk_error)?;
            Ok(())
        })
    }

    // ── cleanup_expired_idempotency_tokens ────────────────────────────────────
    //
    // DynamoDB manages its own ~10-minute idempotency window natively.
    // There are no ExtendDB-managed idempotency rows to clean up in this backend.

    fn cleanup_expired_idempotency_tokens(
        &self,
        _max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async { Ok(0) })
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Build a single `TransactWriteItem` from a `TransactWriteOp`.
///
/// Each op gets its own `Renderer` so expression attribute name/value tokens
/// are scoped to the individual SDK item (separate maps per item in the batch).
fn build_transact_write_item(
    op: &TransactWriteOp<'_>,
    namer: &crate::naming::Namer,
) -> Result<TransactWriteItem, StorageError> {
    match op {
        TransactWriteOp::Put {
            key_info,
            item,
            condition,
            maps,
            ..
        } => {
            let physical = namer.physical(&key_info.account_id, &key_info.table_name);
            let sdk_item = item_to_sdk(item);
            let mut put_b = Put::builder()
                .table_name(physical)
                .set_item(Some(sdk_item));

            if let Some(cond) = condition {
                let mut r = Renderer::new();
                let expr = r.render_condition(cond, maps)?;
                put_b = put_b.condition_expression(expr);
                if !r.names().is_empty() {
                    put_b = put_b.set_expression_attribute_names(Some(r.names().clone()));
                }
                if !r.values().is_empty() {
                    put_b = put_b.set_expression_attribute_values(Some(r.values().clone()));
                }
            }

            let put = put_b.build().map_err(sdk_build_err)?;
            // TransactWriteItem::builder().build() is infallible
            let twi = TransactWriteItem::builder().put(put).build();
            Ok(twi)
        }

        TransactWriteOp::Delete {
            key_info,
            key,
            condition,
            maps,
            ..
        } => {
            let physical = namer.physical(&key_info.account_id, &key_info.table_name);
            let sdk_key = item_to_sdk(key);
            let mut del_b = Delete::builder()
                .table_name(physical)
                .set_key(Some(sdk_key));

            if let Some(cond) = condition {
                let mut r = Renderer::new();
                let expr = r.render_condition(cond, maps)?;
                del_b = del_b.condition_expression(expr);
                if !r.names().is_empty() {
                    del_b = del_b.set_expression_attribute_names(Some(r.names().clone()));
                }
                if !r.values().is_empty() {
                    del_b = del_b.set_expression_attribute_values(Some(r.values().clone()));
                }
            }

            let del = del_b.build().map_err(sdk_build_err)?;
            let twi = TransactWriteItem::builder().delete(del).build();
            Ok(twi)
        }

        TransactWriteOp::Update {
            key_info,
            key,
            actions,
            condition,
            maps,
            ..
        } => {
            let physical = namer.physical(&key_info.account_id, &key_info.table_name);
            let sdk_key = item_to_sdk(key);
            let mut r = Renderer::new();
            let update_expr = r.render_update(actions, maps)?;

            let mut upd_b = Update::builder()
                .table_name(physical)
                .set_key(Some(sdk_key))
                .update_expression(update_expr);

            if let Some(cond) = condition {
                let cond_expr = r.render_condition(cond, maps)?;
                upd_b = upd_b.condition_expression(cond_expr);
            }

            if !r.names().is_empty() {
                upd_b = upd_b.set_expression_attribute_names(Some(r.names().clone()));
            }
            if !r.values().is_empty() {
                upd_b = upd_b.set_expression_attribute_values(Some(r.values().clone()));
            }

            let upd = upd_b.build().map_err(sdk_build_err)?;
            let twi = TransactWriteItem::builder().update(upd).build();
            Ok(twi)
        }

        TransactWriteOp::ConditionCheck {
            key_info,
            key,
            condition,
            maps,
            ..
        } => {
            let physical = namer.physical(&key_info.account_id, &key_info.table_name);
            let sdk_key = item_to_sdk(key);
            let mut r = Renderer::new();
            let cond_expr = r.render_condition(condition, maps)?;

            let mut cc_b = ConditionCheck::builder()
                .table_name(physical)
                .set_key(Some(sdk_key))
                .condition_expression(cond_expr);

            if !r.names().is_empty() {
                cc_b = cc_b.set_expression_attribute_names(Some(r.names().clone()));
            }
            if !r.values().is_empty() {
                cc_b = cc_b.set_expression_attribute_values(Some(r.values().clone()));
            }

            let cc = cc_b.build().map_err(sdk_build_err)?;
            let twi = TransactWriteItem::builder().condition_check(cc).build();
            Ok(twi)
        }
    }
}
