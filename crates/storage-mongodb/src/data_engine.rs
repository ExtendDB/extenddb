// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DataEngine` trait implementation for `MongoEngine`.

use bson::{Document, doc};
use futures::future::BoxFuture;
use mongodb::options::{FindOneAndDeleteOptions, FindOneAndReplaceOptions, ReturnDocument};

use extenddb_core::expression::{
    self, Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition, UpdateAction,
};
use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, KeyType, ReturnValuesOnConditionCheckFailure,
    ScalarAttributeType, StreamEventName, StreamRecord, StreamRecordData, TableKeyInfo,
    extract_key, item_size_bytes,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    composite_pk_to_text, encode_netstring_composite, pk_to_text, sk_info,
};
use extenddb_storage::{
    DataEngine, IdempotencyKey, ItemPairResult, QueryResult, StreamCapture, StreamEngine,
    TransactGetOp, TransactWriteOp,
};

use crate::MongoEngine;
use crate::condition::condition_to_filter;
use crate::data::{
    composite_id, data_collection_name, document_to_item, index_document, index_entry_filter,
    item_to_document, pk_filter, sk_field_name, sk_suffix,
};
use crate::pushdown::{Pushable, is_pushable};

use extenddb_core::types::{AttributeDefinition, Projection, ProjectionType};

impl DataEngine for MongoEngine {
    fn put_item(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let item = item.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            self.put_item_impl(
                &key_info,
                item,
                return_old,
                condition.as_ref(),
                &maps,
                stream.as_ref(),
            )
            .await
        })
    }

    fn get_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        Box::pin(async move { self.get_item_impl(&key_info, &key).await })
    }

    fn delete_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            self.delete_item_impl(
                &key_info,
                &key,
                return_old,
                condition.as_ref(),
                &maps,
                stream.as_ref(),
            )
            .await
        })
    }

    fn update_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, ItemPairResult> {
        let key_info = key_info.clone();
        let key = key.clone();
        let actions = actions.to_vec();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            self.update_item_impl(
                &key_info,
                &key,
                &actions,
                return_old,
                return_new,
                condition.as_ref(),
                &maps,
                stream.as_ref(),
            )
            .await
        })
    }

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
        let key_info = key_info.clone();
        let key_condition = key_condition.clone();
        let maps = maps.clone();
        let exclusive_start_key = exclusive_start_key.cloned();
        let index_name = index_name.map(std::string::ToString::to_string);
        Box::pin(async move {
            self.query_impl(
                &key_info,
                &key_condition,
                &maps,
                forward,
                limit,
                exclusive_start_key.as_ref(),
                index_name.as_deref(),
            )
            .await
        })
    }

    fn scan(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        let key_info = key_info.clone();
        let exclusive_start_key = exclusive_start_key.cloned();
        let index_name = index_name.map(std::string::ToString::to_string);
        Box::pin(async move {
            self.scan_impl(
                &key_info,
                limit,
                exclusive_start_key.as_ref(),
                segment,
                total_segments,
                index_name.as_deref(),
            )
            .await
        })
    }

    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        let ops_data: Vec<_> = ops
            .iter()
            .map(|op| (op.key_info.clone(), op.key.clone()))
            .collect();
        Box::pin(async move { self.transact_get_items_impl(&ops_data).await })
    }

    fn transact_write_items(
        &self,
        ops: &[TransactWriteOp<'_>],
        idempotency: Option<IdempotencyKey<'_>>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let ops_owned: Vec<_> = ops.iter().map(clone_transact_write_op).collect();
        let idempotency_owned = idempotency.map(|k| {
            (
                k.account_id.to_owned(),
                k.token.to_owned(),
                k.fingerprint.to_owned(),
            )
        });
        Box::pin(async move {
            let idem_ref = idempotency_owned.as_ref().map(|(a, t, f)| IdempotencyKey {
                account_id: a.as_str(),
                token: t.as_str(),
                fingerprint: f.as_str(),
            });
            self.transact_write_items_impl(&ops_owned, idem_ref).await
        })
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            let coll = self.data_db.collection::<Document>("idempotency_tokens");
            let cutoff = time::OffsetDateTime::now_utc()
                - std::time::Duration::from_secs(max_age_seconds as u64);
            let cutoff_bson = mongodb::bson::DateTime::from_millis(cutoff.unix_timestamp() * 1000);
            let result = coll
                .delete_many(doc! { "created_at": { "$lt": cutoff_bson } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(result.deleted_count)
        })
    }
}

impl MongoEngine {
    async fn put_item_impl(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let new_doc =
            item_to_document(&item, &key_info.key_schema, &key_info.attribute_definitions)?;
        let key_filter = pk_filter(&item, &key_info.key_schema, &key_info.attribute_definitions)?;

        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tx_options = mongodb::options::TransactionOptions::builder()
            .read_concern(mongodb::options::ReadConcern::snapshot())
            .write_concern(
                mongodb::options::WriteConcern::builder()
                    .w(mongodb::options::Acknowledgment::Majority)
                    .build(),
            )
            .build();

        session
            .start_transaction()
            .with_options(tx_options)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let old_item: Option<Item>;
        let return_val: Option<Item>;

        if let Some(cond) = condition {
            let existing_doc = coll
                .find_one(key_filter.clone())
                .session(&mut session)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if let Some(ref existing) = existing_doc {
                let existing_item = document_to_item(existing)?;
                let passed = expression::evaluate_condition(cond, &existing_item, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if !passed {
                    let _ = session.abort_transaction().await;
                    return Err(StorageError::ConditionFailed(Some(existing_item)));
                }
                let opts = FindOneAndReplaceOptions::builder()
                    .return_document(ReturnDocument::Before)
                    .build();
                let old_doc = coll
                    .find_one_and_replace(key_filter, new_doc)
                    .with_options(opts)
                    .session(&mut session)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                old_item = old_doc.as_ref().map(document_to_item).transpose()?;
                return_val = if return_old { old_item.clone() } else { None };
            } else {
                let empty = std::collections::BTreeMap::new();
                let passed = expression::evaluate_condition(cond, &empty, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if !passed {
                    let _ = session.abort_transaction().await;
                    return Err(StorageError::ConditionFailed(None));
                }
                let result = coll.insert_one(new_doc).session(&mut session).await;
                if let Err(e) = result {
                    if e.to_string().contains("E11000") {
                        let _ = session.abort_transaction().await;
                        let winner = coll
                            .find_one(key_filter)
                            .await
                            .map_err(|e2| StorageError::Internal(e2.to_string()))?
                            .map(|d| document_to_item(&d))
                            .transpose()?;
                        return Err(StorageError::ConditionFailed(winner));
                    }
                    let _ = session.abort_transaction().await;
                    return Err(StorageError::Internal(e.to_string()));
                }
                old_item = None;
                return_val = None;
            }
        } else {
            let opts = FindOneAndReplaceOptions::builder()
                .upsert(true)
                .return_document(ReturnDocument::Before)
                .build();
            let old_doc = coll
                .find_one_and_replace(key_filter, new_doc)
                .with_options(opts)
                .session(&mut session)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            old_item = old_doc.as_ref().map(document_to_item).transpose()?;
            return_val = if return_old { old_item.clone() } else { None };
        }

        // Sync GSI collections within the transaction
        self.sync_indexes_in_session(key_info, old_item.as_ref(), Some(&item), &mut session)
            .await?;

        // Write stream record within the transaction
        if let Some(capture) = stream {
            self.write_stream_inline_in_session(
                key_info,
                capture,
                old_item.as_ref(),
                Some(&item),
                &mut session,
            )
            .await?;
        }

        session
            .commit_transaction()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(return_val)
    }

    async fn get_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let filter = pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)?;
        let doc = coll
            .find_one(filter)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        doc.as_ref().map(document_to_item).transpose()
    }

    async fn delete_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        // Pushdown fast path: conditional delete on a no-stream / no-GSI
        // table with a pushable condition. Collapses read-then-check-then-
        // write inside a session to a single `find_one_and_delete` with
        // the merged filter. See `crates/storage-mongodb/src/pushdown.rs`.
        if let Some(cond) = condition
            && stream.is_none()
            && self.gsi_cache_get_fresh(&key_info.table_id) == Some(false)
            && matches!(is_pushable(cond, maps), Pushable::Yes)
        {
            return self
                .delete_item_pushdown(key_info, key, return_old, cond, maps)
                .await;
        }

        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let key_filter = pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)?;

        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tx_options = mongodb::options::TransactionOptions::builder()
            .read_concern(mongodb::options::ReadConcern::snapshot())
            .write_concern(
                mongodb::options::WriteConcern::builder()
                    .w(mongodb::options::Acknowledgment::Majority)
                    .build(),
            )
            .build();

        session
            .start_transaction()
            .with_options(tx_options)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let deleted_item: Option<Item>;

        if let Some(cond) = condition {
            let existing_doc = coll
                .find_one(key_filter.clone())
                .session(&mut session)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if let Some(ref existing) = existing_doc {
                let existing_item = document_to_item(existing)?;
                let passed = expression::evaluate_condition(cond, &existing_item, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if !passed {
                    let _ = session.abort_transaction().await;
                    return Err(StorageError::ConditionFailed(Some(existing_item)));
                }
                coll.delete_one(key_filter)
                    .session(&mut session)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                deleted_item = Some(existing_item);
            } else {
                let empty = std::collections::BTreeMap::new();
                let passed = expression::evaluate_condition(cond, &empty, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if !passed {
                    let _ = session.abort_transaction().await;
                    return Err(StorageError::ConditionFailed(None));
                }
                deleted_item = None;
            }
        } else {
            let old_doc = coll
                .find_one_and_delete(key_filter)
                .session(&mut session)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            deleted_item = old_doc.as_ref().map(document_to_item).transpose()?;
        }

        // Sync GSI collections within the transaction
        if deleted_item.is_some() {
            self.sync_indexes_in_session(key_info, deleted_item.as_ref(), None, &mut session)
                .await?;
        }

        // Write stream record within the transaction
        if let Some(capture) = stream {
            self.write_stream_inline_in_session(
                key_info,
                capture,
                deleted_item.as_ref(),
                None,
                &mut session,
            )
            .await?;
        }

        session
            .commit_transaction()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(if return_old { deleted_item } else { None })
    }

    #[allow(clippy::too_many_arguments)]
    async fn update_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<(Option<Item>, Option<Item>), StorageError> {
        // Pushdown fast path (A5): conditional update on a no-stream /
        // no-GSI table with a pushable condition. Skips the session/
        // transaction overhead. See `crates/storage-mongodb/src/pushdown.rs`.
        if let Some(cond) = condition
            && stream.is_none()
            && self.gsi_cache_get_fresh(&key_info.table_id) == Some(false)
            && matches!(is_pushable(cond, maps), Pushable::Yes)
        {
            match self
                .update_item_pushdown(key_info, key, actions, return_old, return_new, cond, maps)
                .await
            {
                Ok(pair) => return Ok(pair),
                Err(StorageError::Internal(msg)) if msg.contains("raced by concurrent writer") => {
                    // Fall through to session-scoped path which
                    // has a proper retry loop.
                }
                Err(other) => return Err(other),
            }
        }

        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let key_filter = pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)?;

        // Fast path: use native MongoDB atomic operators when possible.
        // This avoids transactions and retries for simple unconditional updates.
        if condition.is_none()
            && !return_old
            && stream.is_none()
            && let Some(mongo_update) = self.try_build_native_update(actions, maps)
        {
            let opts = mongodb::options::FindOneAndUpdateOptions::builder()
                .upsert(true)
                .return_document(ReturnDocument::After)
                .build();
            let result_doc = coll
                .find_one_and_update(key_filter, mongo_update)
                .with_options(opts)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let new_item = if return_new {
                result_doc.as_ref().map(document_to_item).transpose()?
            } else {
                None
            };

            // Sync GSI (non-transactional but data write is atomic)
            if let Some(ref doc) = result_doc {
                let item = document_to_item(doc)?;
                self.sync_indexes(key_info, None, Some(&item)).await?;
            }

            return Ok((None, new_item));
        }

        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tx_options = mongodb::options::TransactionOptions::builder()
            .read_concern(mongodb::options::ReadConcern::snapshot())
            .write_concern(
                mongodb::options::WriteConcern::builder()
                    .w(mongodb::options::Acknowledgment::Majority)
                    .build(),
            )
            .build();

        for _attempt in 0..50 {
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let existing_doc = coll
                .find_one(key_filter.clone())
                .session(&mut session)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let current_version = existing_doc
                .as_ref()
                .and_then(|d| d.get_i64("_v").ok())
                .unwrap_or(0);

            let existing_item = if let Some(doc) = existing_doc.as_ref() {
                document_to_item(doc)?
            } else {
                key.clone()
            };

            if let Some(cond) = condition {
                let eval_item = if existing_doc.is_some() {
                    &existing_item
                } else {
                    &std::collections::BTreeMap::new()
                };
                let passed = expression::evaluate_condition(cond, eval_item, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if !passed {
                    let _ = session.abort_transaction().await;
                    return Err(StorageError::ConditionFailed(if existing_doc.is_some() {
                        Some(existing_item.clone())
                    } else {
                        None
                    }));
                }
            }

            let need_old = return_old || stream.is_some();
            // Only surface a pre-image when the item actually existed.
            // When existing_doc is None, `existing_item` is a fabricated
            // key-only stub used to seed apply_update — feeding it to
            // stream/ReturnValues would emit MODIFY with a phantom
            // OldImage instead of INSERT (§5.4 in RFC-0003).
            let old_item = if need_old && existing_doc.is_some() {
                Some(existing_item.clone())
            } else {
                None
            };

            let mut new_item = existing_item;
            expression::apply_update(actions, &mut new_item, maps)
                .map_err(|e| StorageError::Validation(e.to_string()))?;

            let mut new_doc = item_to_document(
                &new_item,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )?;
            let new_version = current_version + 1;
            new_doc.insert("_v", new_version);

            if existing_doc.is_some() {
                let mut versioned_filter = key_filter.clone();
                if current_version == 0 {
                    versioned_filter.insert("_v", doc! { "$not": { "$gt": 0_i64 } });
                } else {
                    versioned_filter.insert("_v", current_version);
                }
                let result = coll
                    .replace_one(versioned_filter, new_doc)
                    .session(&mut session)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if result.matched_count == 0 {
                    let _ = session.abort_transaction().await;
                    let base_us = 50u64.saturating_mul(1u64 << _attempt.min(8));
                    let jitter = rand::random_range(0..=base_us);
                    tokio::time::sleep(std::time::Duration::from_micros(jitter)).await;
                    continue;
                }
            } else {
                let opts = mongodb::options::ReplaceOptions::builder()
                    .upsert(true)
                    .build();
                coll.replace_one(key_filter.clone(), new_doc)
                    .with_options(opts)
                    .session(&mut session)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }

            // Sync GSI collections within the transaction
            self.sync_indexes_in_session(
                key_info,
                old_item.as_ref(),
                Some(&new_item),
                &mut session,
            )
            .await?;

            // Write stream record within the transaction
            if let Some(capture) = stream {
                self.write_stream_inline_in_session(
                    key_info,
                    capture,
                    old_item.as_ref(),
                    Some(&new_item),
                    &mut session,
                )
                .await?;
            }

            session
                .commit_transaction()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let old_item_result = if return_old { old_item } else { None };
            let new_item_result = if return_new { Some(new_item) } else { None };
            return Ok((old_item_result, new_item_result));
        }

        Err(StorageError::Internal(
            "UpdateItem: too many version conflicts, giving up".to_owned(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_impl(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        use futures::TryStreamExt;

        // Determine collection and effective key schema for the query target
        let (coll_name, effective_key_schema) = if let Some(idx_name) = index_name {
            let idx_info = self
                .index_info_by_table_id_impl(&key_info.table_id, idx_name)
                .await?;
            (
                data_collection_name(&idx_info.index_id),
                idx_info.key_schema.clone(),
            )
        } else {
            (
                data_collection_name(&key_info.table_id),
                key_info.key_schema.clone(),
            )
        };
        let coll = self.data_db.collection::<Document>(&coll_name);

        // Build the query filter — handle multi-part HASH keys
        let pk_text = if key_condition.extra_pk_conditions.is_empty() {
            let pk_value = resolve_key_expr(&key_condition.pk_value, maps)?;
            pk_to_text(&pk_value)
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .into_owned()
        } else {
            let mut parts = Vec::with_capacity(1 + key_condition.extra_pk_conditions.len());
            let first_val = resolve_key_expr(&key_condition.pk_value, maps)?;
            parts.push(
                pk_to_text(&first_val)
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                    .into_owned(),
            );
            for (_, value) in &key_condition.extra_pk_conditions {
                let val = resolve_key_expr(value, maps)?;
                parts.push(
                    pk_to_text(&val)
                        .map_err(|e| StorageError::Internal(e.to_string()))?
                        .into_owned(),
                );
            }
            encode_netstring_composite(&parts)
        };

        let mut filter = doc! { "pk": &pk_text };

        // Determine sort key field using effective key schema
        let sk_field = sk_field_name(&effective_key_schema, &key_info.attribute_definitions);

        // Apply sort key condition
        if let Some(ref sk_cond) = key_condition.sk_condition
            && let Some(sk_f) = sk_field
        {
            let sk_filter = build_sk_filter(sk_cond, sk_f, maps)?;
            for (k, v) in sk_filter {
                filter.insert(k, v);
            }
        }

        // Apply exclusive_start_key pagination.
        //
        // For a base-table query the cursor is a single sort-key comparison:
        // base-table items are uniquely keyed by (pk, sk), so `sk > cursor`
        // is unambiguous.
        //
        // For an index query the cursor is a compound tuple over
        // (index_sk?, base_pk, base_sk?). Index-key values are non-unique —
        // duplicates fall through to the base-key tie-breaker. Express as
        // a lexicographic `$or` of the shape
        //     (a > A) OR (a == A AND b > B) OR (a == A AND b == B AND c > C)
        // (with `<` when reverse). RFC-0003 §2.6.
        let is_index = index_name.is_some();
        if let Some(start_key) = exclusive_start_key {
            let cmp_gt = if forward { "$gt" } else { "$lt" };
            if is_index {
                let idx_sk_pair =
                    match sk_info(&effective_key_schema, &key_info.attribute_definitions) {
                        Some((sk_name, sk_type)) => start_key
                            .get(sk_name)
                            .map(|v| sk_to_bson(v, sk_type))
                            .transpose()?
                            .map(|b| (sk_field.expect("sk_field present when sk_info is Some"), b)),
                        None => None,
                    };
                let base_pk_bson: Option<bson::Bson> = {
                    // Build the base_pk text the same way the write path
                    // does — composite_pk_to_text on the base key schema.
                    // If the start_key is malformed we skip pagination
                    // (result is a query that may return duplicates).
                    let text = composite_pk_to_text(start_key, &key_info.base_key_schema).ok();
                    text.map(bson::Bson::String)
                };
                let base_sk_pair =
                    match sk_info(&key_info.base_key_schema, &key_info.attribute_definitions) {
                        Some((sk_name, sk_type)) => start_key
                            .get(sk_name)
                            .map(|v| sk_to_bson(v, sk_type))
                            .transpose()?
                            .map(|b| (format!("base_sk_{}", sk_suffix(sk_type)), b)),
                        None => None,
                    };

                let mut or_clauses: Vec<Document> = Vec::new();
                if let Some((sk_f, sk_bson)) = idx_sk_pair.clone() {
                    or_clauses.push(doc! { sk_f: { cmp_gt: sk_bson } });
                }
                if let Some(bp) = base_pk_bson.clone() {
                    let mut clause = Document::new();
                    if let Some((sk_f, sk_bson)) = idx_sk_pair.clone() {
                        clause.insert(sk_f, sk_bson);
                    }
                    clause.insert("base_pk", doc! { cmp_gt: bp });
                    or_clauses.push(clause);
                }
                if let (Some(bp), Some((base_sk_f, base_sk_bson))) =
                    (base_pk_bson, base_sk_pair.clone())
                {
                    let mut clause = Document::new();
                    if let Some((sk_f, sk_bson)) = idx_sk_pair {
                        clause.insert(sk_f, sk_bson);
                    }
                    clause.insert("base_pk", bp);
                    clause.insert(base_sk_f, doc! { cmp_gt: base_sk_bson });
                    or_clauses.push(clause);
                }

                if !or_clauses.is_empty() {
                    // Merge with any existing $or (unlikely — sk_condition
                    // uses ranged operators, not $or) by wrapping in $and.
                    if filter.contains_key("$or") {
                        let existing = filter.remove("$or").unwrap();
                        filter.insert(
                            "$and",
                            bson::bson!([{ "$or": existing }, { "$or": or_clauses }]),
                        );
                    } else {
                        filter.insert("$or", or_clauses);
                    }
                }
            } else if let (Some(sk_f), Some((sk_name, sk_type))) = (
                sk_field,
                sk_info(&effective_key_schema, &key_info.attribute_definitions),
            ) && let Some(sk_val) = start_key.get(sk_name)
            {
                let sk_bson = sk_to_bson(sk_val, sk_type)?;
                filter.insert(sk_f, doc! { cmp_gt: sk_bson });
            }
        }

        // Build sort direction. For indexes the sort tuple is
        // (index_sk?, base_pk, base_sk?) so pagination lands deterministic
        // within a group of items sharing index keys.
        let sort_direction = if forward { 1 } else { -1 };
        let sort_doc = if is_index {
            let mut sd = Document::new();
            if let Some(sk_f) = sk_field {
                sd.insert(sk_f, sort_direction);
            }
            sd.insert("base_pk", sort_direction);
            if let Some((_, sk_type)) =
                sk_info(&key_info.base_key_schema, &key_info.attribute_definitions)
            {
                sd.insert(format!("base_sk_{}", sk_suffix(sk_type)), sort_direction);
            }
            sd
        } else if let Some(sk_f) = sk_field {
            doc! { sk_f: sort_direction }
        } else {
            doc! { "pk": sort_direction }
        };

        // Apply limit (fetch one extra for pagination)
        let fetch_limit = limit.map(|l| l + 1);

        let collation_opt = if sk_field == Some("sk_s") {
            Some(
                mongodb::options::Collation::builder()
                    .locale("simple".to_string())
                    .build(),
            )
        } else {
            None
        };

        let opts = mongodb::options::FindOptions::builder()
            .sort(sort_doc)
            .limit(fetch_limit)
            .collation(collation_opt)
            .build();

        let cursor = coll
            .find(filter)
            .with_options(opts)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let docs: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut items: Vec<Item> = docs
            .iter()
            .map(document_to_item)
            .collect::<Result<Vec<_>, _>>()?;

        // Post-fetch filtering for binary begins_with (BSON Binary comparison
        // sorts by length first, making $gte/$lt unreliable for prefix matching).
        if let Some(SortKeyCondition::BeginsWith { prefix, .. }) = &key_condition.sk_condition {
            let prefix_av = resolve_key_expr(prefix, maps)?;
            if let AttributeValue::B(ref prefix_bytes) = prefix_av
                && let Some((sk_name, _)) =
                    sk_info(&effective_key_schema, &key_info.attribute_definitions)
            {
                items.retain(|item| {
                    item.get(sk_name)
                        .and_then(|v| {
                            if let AttributeValue::B(b) = v {
                                Some(b.starts_with(prefix_bytes))
                            } else {
                                None
                            }
                        })
                        .unwrap_or(false)
                });
            }
        }

        // Handle pagination. For an index query, LEK carries both the
        // index-key components and the base-key components so the next
        // page's ExclusiveStartKey can resolve the compound cursor.
        // RFC-0003 §7.2.
        let last_evaluated_key = if let Some(l) = limit {
            #[allow(clippy::cast_sign_loss)]
            let l_usize = l as usize;
            if items.len() > l_usize {
                items.truncate(l_usize);
                items.last().map(|item| {
                    if is_index {
                        let mut key = extract_key(item, &effective_key_schema);
                        let base_key = extract_key(item, &key_info.base_key_schema);
                        for (k, v) in base_key {
                            key.entry(k).or_insert(v);
                        }
                        key
                    } else {
                        extract_key(item, &key_info.key_schema)
                    }
                })
            } else {
                None
            }
        } else {
            None
        };

        Ok((items, last_evaluated_key))
    }

    async fn scan_impl(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        use futures::TryStreamExt;

        // The effective key schema for the collection under scan: index
        // schema for index scans (where the collection's _id encodes index
        // keys), base schema for base-table scans.
        let (coll_name, effective_key_schema) = if let Some(idx_name) = index_name {
            let idx_info = self
                .index_info_by_table_id_impl(&key_info.table_id, idx_name)
                .await?;
            (
                data_collection_name(&idx_info.index_id),
                idx_info.key_schema.clone(),
            )
        } else {
            (
                data_collection_name(&key_info.table_id),
                key_info.key_schema.clone(),
            )
        };
        let coll = self.data_db.collection::<Document>(&coll_name);

        let is_index = index_name.is_some();
        let mut filter = Document::new();

        // Apply exclusive_start_key for pagination. Base tables use _id
        // ordering (netstring-encoded); index scans use a compound cursor
        // over (pk, sk?, base_pk, base_sk?) so items with duplicate index
        // keys don't confuse pagination. RFC-0003 §7.2, §2.6.
        if let Some(start_key) = exclusive_start_key {
            if is_index {
                let idx_pk_bson: Option<bson::Bson> =
                    composite_pk_to_text(start_key, &effective_key_schema)
                        .ok()
                        .map(bson::Bson::String);
                let idx_sk_pair =
                    match sk_info(&effective_key_schema, &key_info.attribute_definitions) {
                        Some((sk_name, sk_type)) => start_key
                            .get(sk_name)
                            .map(|v| sk_to_bson(v, sk_type))
                            .transpose()?
                            .map(|b| (format!("sk_{}", sk_suffix(sk_type)), b)),
                        None => None,
                    };
                let base_pk_bson: Option<bson::Bson> =
                    composite_pk_to_text(start_key, &key_info.base_key_schema)
                        .ok()
                        .map(bson::Bson::String);
                let base_sk_pair =
                    match sk_info(&key_info.base_key_schema, &key_info.attribute_definitions) {
                        Some((sk_name, sk_type)) => start_key
                            .get(sk_name)
                            .map(|v| sk_to_bson(v, sk_type))
                            .transpose()?
                            .map(|b| (format!("base_sk_{}", sk_suffix(sk_type)), b)),
                        None => None,
                    };

                let mut or_clauses: Vec<Document> = Vec::new();
                if let Some(ip) = idx_pk_bson.clone() {
                    or_clauses.push(doc! { "pk": { "$gt": ip } });
                }
                if let (Some(ip), Some((sk_f, sk_bson))) =
                    (idx_pk_bson.clone(), idx_sk_pair.clone())
                {
                    or_clauses.push(doc! {
                        "pk": ip,
                        sk_f: { "$gt": sk_bson },
                    });
                }
                if let (Some(ip), Some(bp)) = (idx_pk_bson.clone(), base_pk_bson.clone()) {
                    let mut clause = doc! { "pk": ip };
                    if let Some((sk_f, sk_bson)) = idx_sk_pair.clone() {
                        clause.insert(sk_f, sk_bson);
                    }
                    clause.insert("base_pk", doc! { "$gt": bp });
                    or_clauses.push(clause);
                }
                if let (Some(ip), Some(bp), Some((base_sk_f, base_sk_bson))) =
                    (idx_pk_bson, base_pk_bson, base_sk_pair)
                {
                    let mut clause = doc! { "pk": ip };
                    if let Some((sk_f, sk_bson)) = idx_sk_pair {
                        clause.insert(sk_f, sk_bson);
                    }
                    clause.insert("base_pk", bp);
                    clause.insert(base_sk_f, doc! { "$gt": base_sk_bson });
                    or_clauses.push(clause);
                }

                if !or_clauses.is_empty() {
                    filter.insert("$or", or_clauses);
                }
            } else {
                // Base-table scan: unique (pk, sk) means _id > cursor.
                let start_pk = composite_pk_to_text(start_key, &effective_key_schema)?;
                if let Some((sk_name, _)) =
                    sk_info(&effective_key_schema, &key_info.attribute_definitions)
                {
                    if let Some(sk_val) = start_key.get(sk_name) {
                        let sk_text = match sk_val {
                            AttributeValue::S(s) => s.clone(),
                            AttributeValue::N(n) => n.clone(),
                            AttributeValue::B(b) => {
                                use base64::Engine;
                                base64::engine::general_purpose::STANDARD.encode(b)
                            }
                            _ => return Err(StorageError::Internal("invalid sk type".to_string())),
                        };
                        let start_id = composite_id(&start_pk, &sk_text);
                        filter.insert("_id", doc! { "$gt": start_id });
                    }
                } else {
                    filter.insert("_id", doc! { "$gt": &start_pk });
                }
            }
        }

        // Parallel scan segment filtering
        // segment/total_segments use CRC32 hash of pk modulo total_segments
        let apply_segment_filter = segment.is_some() && total_segments.is_some();

        // Apply limit
        let fetch_limit = limit.map(|l| {
            let extra = l + 1;
            if apply_segment_filter {
                extra * total_segments.unwrap_or(1)
            } else {
                extra
            }
        });

        // Sort key. Index scans sort by (pk, sk?, base_pk, base_sk?) so
        // pagination is well-defined across items sharing index keys.
        // Base-table scans sort by _id (unique).
        let sort_doc = if is_index {
            let mut sd = doc! { "pk": 1 };
            if let Some((_, sk_type)) =
                sk_info(&effective_key_schema, &key_info.attribute_definitions)
            {
                sd.insert(format!("sk_{}", sk_suffix(sk_type)), 1);
            }
            sd.insert("base_pk", 1);
            if let Some((_, sk_type)) =
                sk_info(&key_info.base_key_schema, &key_info.attribute_definitions)
            {
                sd.insert(format!("base_sk_{}", sk_suffix(sk_type)), 1);
            }
            sd
        } else {
            doc! { "_id": 1 }
        };

        let opts = mongodb::options::FindOptions::builder()
            .sort(sort_doc)
            .limit(fetch_limit)
            .build();

        let cursor = coll
            .find(filter)
            .with_options(opts)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let docs: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut items: Vec<Item> = Vec::new();
        for doc in &docs {
            let item = document_to_item(doc)?;

            // Apply segment filter if needed
            if let (Some(seg), Some(total)) = (segment, total_segments) {
                let pk_text = composite_pk_to_text(&item, &key_info.key_schema)?;
                let hash = crc32fast::hash(pk_text.as_bytes());
                #[allow(clippy::cast_sign_loss)]
                let total_u = total as u32;
                #[allow(clippy::cast_sign_loss)]
                let seg_u = seg as u32;
                if hash % total_u != seg_u {
                    continue;
                }
            }

            items.push(item);

            // Check if we have enough items
            if let Some(l) = limit {
                #[allow(clippy::cast_sign_loss)]
                if items.len() > l as usize {
                    break;
                }
            }
        }

        // Handle pagination. For index scans, LEK includes both the
        // index-key and base-key components. RFC-0003 §7.2.
        let last_evaluated_key = if let Some(l) = limit {
            #[allow(clippy::cast_sign_loss)]
            let l_usize = l as usize;
            if items.len() > l_usize {
                items.truncate(l_usize);
                items.last().map(|item| {
                    if is_index {
                        let mut key = extract_key(item, &effective_key_schema);
                        let base_key = extract_key(item, &key_info.base_key_schema);
                        for (k, v) in base_key {
                            key.entry(k).or_insert(v);
                        }
                        key
                    } else {
                        extract_key(item, &key_info.key_schema)
                    }
                })
            } else {
                None
            }
        } else {
            None
        };

        Ok((items, last_evaluated_key))
    }

    // ── Native MongoDB Update (fast path) ─────────────────────────────

    fn try_build_native_update(
        &self,
        actions: &[UpdateAction],
        maps: &ExpressionMaps,
    ) -> Option<Document> {
        let mut inc_doc = Document::new();
        let mut set_doc = Document::new();
        let mut unset_doc = Document::new();

        for action in actions {
            match action {
                UpdateAction::Add { path, value } => {
                    if path.len() != 1 {
                        return None;
                    }
                    let attr_name = match &path[0] {
                        PathElement::Attribute(name) => name,
                        _ => return None,
                    };
                    let val = match value {
                        Expr::Placeholder(name) => maps.resolve_value(name).ok()?,
                        _ => return None,
                    };
                    match val {
                        AttributeValue::N(n) => {
                            let field = format!("item_data.{attr_name}.N");
                            // Store numeric increment as string (matching our storage format)
                            // Use $inc on a helper field and reconcile, OR use a different approach.
                            // Actually: item_data stores N as string. We can't $inc a string.
                            // We need a numeric shadow field for $inc to work.
                            // For now, only optimize if we can parse as i64.
                            if let Ok(i) = n.parse::<i64>() {
                                // Use $inc on a numeric shadow field, then $set the string representation.
                                // Actually this won't work atomically in one update...
                                // The simplest correct approach: use $inc on item_data.attr.N
                                // BUT item_data.attr.N is stored as a string, not a number.
                                // MongoDB $inc doesn't work on strings.
                                // FALLBACK: we cannot use the native fast path for numeric ADD
                                // unless we change the storage format. Give up.
                                let _ = (field, i);
                                return None;
                            }
                            return None;
                        }
                        AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_) => {
                            // Set ADD — could use $addToSet but storage format is complex
                            return None;
                        }
                        _ => return None,
                    }
                }
                UpdateAction::Set { path, value } => {
                    if path.len() != 1 {
                        return None;
                    }
                    let attr_name = match &path[0] {
                        PathElement::Attribute(name) => name,
                        _ => return None,
                    };
                    let val = match value {
                        Expr::Placeholder(name) => maps.resolve_value(name).ok()?,
                        _ => return None, // complex expressions (if_not_exists, list_append, arithmetic)
                    };
                    let field = format!("item_data.{attr_name}");
                    let val_json = serde_json::to_value(val).ok()?;
                    let val_bson = bson::to_bson(&val_json).ok()?;
                    set_doc.insert(field, val_bson);
                }
                UpdateAction::Remove { path } => {
                    if path.len() != 1 {
                        return None;
                    }
                    let attr_name = match &path[0] {
                        PathElement::Attribute(name) => name,
                        _ => return None,
                    };
                    let field = format!("item_data.{attr_name}");
                    unset_doc.insert(field, 1);
                }
                UpdateAction::Delete { .. } => {
                    return None;
                }
            }
        }

        let mut update = Document::new();
        if !inc_doc.is_empty() {
            update.insert("$inc", inc_doc);
        }
        if !set_doc.is_empty() {
            update.insert("$set", set_doc);
        }
        if !unset_doc.is_empty() {
            update.insert("$unset", unset_doc);
        }

        if update.is_empty() {
            return None;
        }

        Some(update)
    }

    // ── GSI Sync ──────────────────────────────────────────────────────

    async fn sync_indexes(
        &self,
        key_info: &TableKeyInfo,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
    ) -> Result<(), StorageError> {
        use futures::TryStreamExt;

        // Fast path: skip catalog query if we know this table has no GSIs.
        // The cache entry is valid for GSI_CACHE_TTL, giving eventual
        // convergence when a GSI is added on another ExtendDB instance.
        if let Some(false) = self.gsi_cache_get_fresh(&key_info.table_id) {
            return Ok(());
        }

        let indexes_coll = self.catalog_db.collection::<Document>("indexes");
        let mut cursor = indexes_coll
            .find(doc! { "_id.table_id": &key_info.table_id })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut found_any = false;
        while let Some(idx_doc) = cursor
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            found_any = true;
            let index_id = match idx_doc.get_str("index_id") {
                Ok(id) => id.to_string(),
                Err(_) => continue,
            };
            let idx_key_schema: Vec<KeySchemaElement> = match idx_doc.get("key_schema") {
                Some(ks) => bson::from_bson(ks.clone()).unwrap_or_default(),
                None => continue,
            };
            let projection: Projection = match idx_doc.get("projection") {
                Some(p) => bson::from_bson(p.clone()).unwrap_or(Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                }),
                None => Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                },
            };

            let idx_coll_name = data_collection_name(&index_id);
            let idx_coll = self.data_db.collection::<Document>(&idx_coll_name);

            // Delete old index entry. The filter must match on both the
            // index-key AND the base-key components, because GSIs allow
            // duplicate index-key values across base items. See D-C1 /
            // RFC-0003 §2.1.
            if let Some(old) = old_item
                && item_has_index_keys(old, &idx_key_schema)
            {
                let projected_old =
                    project_item(old, &idx_key_schema, &key_info.key_schema, &projection);
                let old_filter = index_entry_filter(
                    &projected_old,
                    &idx_key_schema,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )?;
                let _ = idx_coll.delete_one(old_filter).await;
            }

            // Insert new index entry
            if let Some(new) = new_item
                && item_has_index_keys(new, &idx_key_schema)
            {
                let projected =
                    project_item(new, &idx_key_schema, &key_info.key_schema, &projection);
                let idx_doc = index_document(
                    &projected,
                    &idx_key_schema,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )?;
                let filter = index_entry_filter(
                    &projected,
                    &idx_key_schema,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )?;
                let opts = mongodb::options::ReplaceOptions::builder()
                    .upsert(true)
                    .build();
                idx_coll
                    .replace_one(filter, idx_doc)
                    .with_options(opts)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        self.gsi_cache_set(&key_info.table_id, found_any);
        Ok(())
    }

    async fn sync_indexes_in_session(
        &self,
        key_info: &TableKeyInfo,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
        session: &mut mongodb::ClientSession,
    ) -> Result<(), StorageError> {
        use futures::TryStreamExt;

        if let Some(false) = self.gsi_cache_get_fresh(&key_info.table_id) {
            return Ok(());
        }

        let indexes_coll = self.catalog_db.collection::<Document>("indexes");
        let mut cursor = indexes_coll
            .find(doc! { "_id.table_id": &key_info.table_id })
            .session(&mut *session)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut found_any = false;
        while let Some(idx_doc) = cursor
            .next(session)
            .await
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            found_any = true;
            let index_id = match idx_doc.get_str("index_id") {
                Ok(id) => id.to_string(),
                Err(_) => continue,
            };
            let idx_key_schema: Vec<KeySchemaElement> = match idx_doc.get("key_schema") {
                Some(ks) => bson::from_bson(ks.clone()).unwrap_or_default(),
                None => continue,
            };
            let projection: Projection = match idx_doc.get("projection") {
                Some(p) => bson::from_bson(p.clone()).unwrap_or(Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                }),
                None => Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                },
            };

            let idx_coll_name = data_collection_name(&index_id);
            let idx_coll = self.data_db.collection::<Document>(&idx_coll_name);

            if let Some(old) = old_item
                && item_has_index_keys(old, &idx_key_schema)
            {
                let projected_old =
                    project_item(old, &idx_key_schema, &key_info.key_schema, &projection);
                let old_filter = index_entry_filter(
                    &projected_old,
                    &idx_key_schema,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )?;
                let _ = idx_coll.delete_one(old_filter).session(&mut *session).await;
            }

            if let Some(new) = new_item
                && item_has_index_keys(new, &idx_key_schema)
            {
                let projected =
                    project_item(new, &idx_key_schema, &key_info.key_schema, &projection);
                let idx_doc = index_document(
                    &projected,
                    &idx_key_schema,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )?;
                let filter = index_entry_filter(
                    &projected,
                    &idx_key_schema,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )?;
                let opts = mongodb::options::ReplaceOptions::builder()
                    .upsert(true)
                    .build();
                idx_coll
                    .replace_one(filter, idx_doc)
                    .with_options(opts)
                    .session(&mut *session)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        self.gsi_cache_set(&key_info.table_id, found_any);
        Ok(())
    }

    async fn write_stream_inline_in_session(
        &self,
        key_info: &TableKeyInfo,
        capture: &StreamCapture,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
        session: &mut mongodb::ClientSession,
    ) -> Result<(), StorageError> {
        use extenddb_core::types::StreamViewType;

        let source_item = new_item.or(old_item);
        let Some(source) = source_item else {
            return Ok(());
        };

        let event = match (old_item, new_item) {
            (None, Some(_)) => StreamEventName::Insert,
            (Some(_), Some(_)) => StreamEventName::Modify,
            (Some(_), None) => StreamEventName::Remove,
            (None, None) => return Ok(()),
        };

        let keys: std::collections::BTreeMap<String, AttributeValue> = key_info
            .key_schema
            .iter()
            .filter_map(|ks| {
                source
                    .get(&ks.attribute_name)
                    .map(|v| (ks.attribute_name.clone(), v.clone()))
            })
            .collect();

        let new_image = match capture.view_type {
            StreamViewType::NewImage | StreamViewType::NewAndOldImages => new_item.cloned(),
            _ => None,
        };
        let old_image = match capture.view_type {
            StreamViewType::OldImage | StreamViewType::NewAndOldImages => old_item.cloned(),
            _ => None,
        };

        let size = source_item.map_or(0, |i| i64::try_from(item_size_bytes(i)).unwrap_or(i64::MAX));

        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_str = source
            .get(pk_name)
            .map(|v| match v {
                AttributeValue::S(s) => s.clone(),
                AttributeValue::N(n) => n.clone(),
                AttributeValue::B(b) => {
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b)
                }
                _ => String::new(),
            })
            .unwrap_or_default();

        // Both shard resolution and sequence-number assignment run inside
        // the same session as the data write. This is what makes stream
        // ordering safe under contention — see
        // stream_engine::next_sequence_number_in_session for the full
        // rationale.
        let shard_id = self
            .assign_shard_in_session(
                &key_info.account_id,
                &key_info.table_name,
                &pk_str,
                &mut *session,
            )
            .await?;
        let seq = self
            .next_sequence_number_in_session(&shard_id, &mut *session)
            .await?;

        let record = StreamRecord {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_name: event,
            event_version: "1.1".to_owned(),
            event_source: "aws:dynamodb".to_owned(),
            aws_region: capture.region.to_string(),
            dynamodb: StreamRecordData {
                approximate_creation_date_time: i64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                )
                .unwrap_or(i64::MAX),
                keys,
                new_image,
                old_image,
                sequence_number: seq,
                size_bytes: size,
                stream_view_type: capture.view_type,
            },
            user_identity: capture.user_identity.clone(),
        };

        let record_json =
            serde_json::to_value(&record).map_err(|e| StorageError::Internal(e.to_string()))?;
        let record_bson =
            bson::to_bson(&record_json).map_err(|e| StorageError::Internal(e.to_string()))?;

        // key_info already carries table_id — no need to re-read the catalog
        // just to resolve it, and re-reading inside the session against the
        // tables collection would join to the counter/records write set
        // needlessly.
        let table_id = &key_info.table_id;

        let records_coll = self.data_db.collection::<Document>("stream_records");
        records_coll
            .insert_one(doc! {
                "sequence_number": &record.dynamodb.sequence_number,
                "shard_id": &shard_id,
                "table_id": table_id,
                "event_name": crate::stream_engine::event_name_ddb_str(record.event_name),
                "record_data": record_bson,
                "created_at": mongodb::bson::DateTime::now(),
            })
            .session(&mut *session)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    // ── Transactions ──────────────────────────────────────────────────

    async fn transact_get_items_impl(
        &self,
        ops: &[(TableKeyInfo, Item)],
    ) -> Result<Vec<Option<Item>>, StorageError> {
        use extenddb_core::types::CancellationReason;
        use extenddb_core::validation;

        // Validate key types before starting transaction
        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        let mut any_failed = false;
        for (key_info, key) in ops {
            match validation::validate_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            ) {
                Ok(()) => reasons.push(CancellationReason::none()),
                Err(e) => {
                    any_failed = true;
                    reasons.push(CancellationReason::validation_error(e.to_string()));
                }
            }
        }
        if any_failed {
            return Err(StorageError::TransactionCanceled(reasons));
        }

        // Use a MongoDB session with snapshot read concern for consistent reads
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tx_options = mongodb::options::TransactionOptions::builder()
            .read_concern(mongodb::options::ReadConcern::snapshot())
            .build();

        session
            .start_transaction()
            .with_options(tx_options)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut results = Vec::with_capacity(ops.len());
        for (key_info, key) in ops {
            let coll_name = data_collection_name(&key_info.table_id);
            let coll = self.data_db.collection::<Document>(&coll_name);
            let filter = pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)?;
            let doc = coll
                .find_one(filter)
                .session(&mut session)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let item = doc.as_ref().map(document_to_item).transpose()?;
            results.push(item);
        }

        session
            .commit_transaction()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(results)
    }

    async fn transact_write_items_impl(
        &self,
        ops: &[OwnedTransactWriteOp],
        idempotency: Option<IdempotencyKey<'_>>,
    ) -> Result<(), StorageError> {
        use extenddb_core::types::CancellationReason;
        use extenddb_core::validation;

        // Start a MongoDB multi-document transaction
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tx_options = mongodb::options::TransactionOptions::builder()
            .read_concern(mongodb::options::ReadConcern::snapshot())
            .write_concern(
                mongodb::options::WriteConcern::builder()
                    .w(mongodb::options::Acknowledgment::Majority)
                    .build(),
            )
            .build();

        session
            .start_transaction()
            .with_options(tx_options)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Check idempotency token, scoped to the caller's account so that
        // identical tokens from different accounts never collide.
        if let Some(key) = idempotency {
            let idem_coll = self.data_db.collection::<Document>("idempotency_tokens");
            let existing = idem_coll
                .find_one(doc! { "account_id": key.account_id, "token": key.token })
                .session(&mut session)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if let Some(existing_doc) = existing {
                let stored_fp = existing_doc.get_str("fingerprint").unwrap_or_default();
                if stored_fp == key.fingerprint {
                    session
                        .abort_transaction()
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    return Err(StorageError::IdempotentReplay);
                }
                session
                    .abort_transaction()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                return Err(StorageError::IdempotentMismatch);
            }

            // Store the token. A unique index on (account_id, token)
            // catches the case where a concurrent request under snapshot
            // isolation didn't see our pre-check but raced us to the
            // insert. On E11000, abort our txn and resolve the winner
            // by re-reading outside the session — same replay/mismatch
            // logic as the pre-check path.
            let insert_res = idem_coll
                .insert_one(doc! {
                    "account_id": key.account_id,
                    "token": key.token,
                    "fingerprint": key.fingerprint,
                    "created_at": mongodb::bson::DateTime::now(),
                })
                .session(&mut session)
                .await;
            if let Err(e) = insert_res {
                let is_dup = matches!(
                    *e.kind,
                    mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                        mongodb::error::WriteError { code: 11000, .. }
                    ))
                );
                if is_dup {
                    let _ = session.abort_transaction().await;
                    let winner = idem_coll
                        .find_one(doc! { "account_id": key.account_id, "token": key.token })
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    return Err(
                        match winner.as_ref().and_then(|d| d.get_str("fingerprint").ok()) {
                            Some(fp) if fp == key.fingerprint => StorageError::IdempotentReplay,
                            _ => StorageError::IdempotentMismatch,
                        },
                    );
                }
                return Err(StorageError::Internal(e.to_string()));
            }
        }

        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        let mut any_failed = false;

        for op in ops {
            let reason = self
                .execute_transact_write_op_in_session(op, &mut session)
                .await;
            match reason {
                Ok(()) => reasons.push(CancellationReason::none()),
                Err(TransactOpError::Cancel(r)) => {
                    any_failed = true;
                    reasons.push(r);
                }
                Err(TransactOpError::Storage(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }

        if any_failed {
            let _ = session.abort_transaction().await;
            return Err(StorageError::TransactionCanceled(reasons));
        }

        session
            .commit_transaction()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn execute_transact_write_op_in_session(
        &self,
        op: &OwnedTransactWriteOp,
        session: &mut mongodb::ClientSession,
    ) -> Result<(), TransactOpError> {
        use extenddb_core::types::CancellationReason;
        use extenddb_core::validation;

        match op {
            OwnedTransactWriteOp::Put {
                key_info,
                item,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => {
                validation::validate_item_keys(
                    item,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                let coll_name = data_collection_name(&key_info.table_id);
                let coll = self.data_db.collection::<Document>(&coll_name);
                let key_filter =
                    pk_filter(item, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                // Always fetch the pre-image. Needed to (a) evaluate any
                // condition against it, (b) let sync_indexes_in_session delete
                // stale index entries when this write changes or removes a
                // GSI key attribute, and (c) supply OldImage to any attached
                // stream capture.
                let existing_doc = coll
                    .find_one(key_filter.clone())
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                let existing_item = if let Some(doc) = existing_doc.as_ref() {
                    Some(document_to_item(doc).map_err(TransactOpError::Storage)?)
                } else {
                    None
                };

                if let Some(cond) = condition {
                    let for_eval = existing_item.clone().unwrap_or_default();
                    let passed =
                        expression::evaluate_condition(cond, &for_eval, maps).map_err(|e| {
                            TransactOpError::Cancel(CancellationReason::validation_error(
                                e.to_string(),
                            ))
                        })?;
                    if !passed {
                        return Err(TransactOpError::Cancel(
                            CancellationReason::condition_check_failed_with_item(ccf_return_item(
                                *return_values_on_ccf,
                                existing_item.as_ref(),
                            )),
                        ));
                    }
                }

                let new_doc =
                    item_to_document(item, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                let opts = mongodb::options::ReplaceOptions::builder()
                    .upsert(true)
                    .build();
                coll.replace_one(key_filter, new_doc)
                    .with_options(opts)
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                // Propagate to secondary indexes and the stream within the
                // same transaction session — otherwise a transactional write
                // to a streams-enabled or GSI-bearing table would commit
                // the base row while silently dropping its dependent side
                // effects.
                self.sync_indexes_in_session(
                    key_info,
                    existing_item.as_ref(),
                    Some(item),
                    &mut *session,
                )
                .await
                .map_err(TransactOpError::Storage)?;
                if let Some(capture) = stream {
                    self.write_stream_inline_in_session(
                        key_info,
                        capture,
                        existing_item.as_ref(),
                        Some(item),
                        &mut *session,
                    )
                    .await
                    .map_err(TransactOpError::Storage)?;
                }

                Ok(())
            }
            OwnedTransactWriteOp::Delete {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => {
                validation::validate_key_only(
                    key,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                let coll_name = data_collection_name(&key_info.table_id);
                let coll = self.data_db.collection::<Document>(&coll_name);
                let key_filter =
                    pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                // Always fetch the pre-image. Needed for condition evaluation,
                // stale-index deletion in sync_indexes_in_session, and OldImage
                // capture for any attached stream.
                let existing_doc = coll
                    .find_one(key_filter.clone())
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                let existing_item = if let Some(doc) = existing_doc.as_ref() {
                    Some(document_to_item(doc).map_err(TransactOpError::Storage)?)
                } else {
                    None
                };

                if let Some(cond) = condition {
                    let for_eval = existing_item.clone().unwrap_or_default();
                    let passed =
                        expression::evaluate_condition(cond, &for_eval, maps).map_err(|e| {
                            TransactOpError::Cancel(CancellationReason::validation_error(
                                e.to_string(),
                            ))
                        })?;
                    if !passed {
                        return Err(TransactOpError::Cancel(
                            CancellationReason::condition_check_failed_with_item(ccf_return_item(
                                *return_values_on_ccf,
                                existing_item.as_ref(),
                            )),
                        ));
                    }
                }

                coll.delete_one(key_filter)
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                // Propagate to secondary indexes and the stream within the
                // same transaction session.
                self.sync_indexes_in_session(key_info, existing_item.as_ref(), None, &mut *session)
                    .await
                    .map_err(TransactOpError::Storage)?;
                if let Some(capture) = stream {
                    // DDB semantics: a delete on a non-existent key is a
                    // no-op, and no stream record is emitted. Guard on
                    // existing_item.is_some() to match.
                    if existing_item.is_some() {
                        self.write_stream_inline_in_session(
                            key_info,
                            capture,
                            existing_item.as_ref(),
                            None,
                            &mut *session,
                        )
                        .await
                        .map_err(TransactOpError::Storage)?;
                    }
                }

                Ok(())
            }
            OwnedTransactWriteOp::Update {
                key_info,
                key,
                actions,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => {
                validation::validate_key_only(
                    key,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                let coll_name = data_collection_name(&key_info.table_id);
                let coll = self.data_db.collection::<Document>(&coll_name);
                let key_filter =
                    pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                let existing_doc = coll
                    .find_one(key_filter.clone())
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                let existing_item = if let Some(doc) = existing_doc.as_ref() {
                    Some(document_to_item(doc).map_err(TransactOpError::Storage)?)
                } else {
                    None
                };
                let is_creating = existing_item.is_none();

                let mut item = existing_item.clone().unwrap_or_else(|| key.clone());

                if let Some(cond) = condition {
                    let empty = std::collections::BTreeMap::new();
                    let condition_item = if existing_item.is_some() {
                        &item
                    } else {
                        &empty
                    };
                    let passed = expression::evaluate_condition(cond, condition_item, maps)
                        .map_err(|e| {
                            TransactOpError::Cancel(CancellationReason::validation_error(
                                e.to_string(),
                            ))
                        })?;
                    if !passed {
                        return Err(TransactOpError::Cancel(
                            CancellationReason::condition_check_failed_with_item(ccf_return_item(
                                *return_values_on_ccf,
                                existing_item.as_ref(),
                            )),
                        ));
                    }
                }

                expression::apply_update(actions, &mut item, maps).map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                let new_doc =
                    item_to_document(&item, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                let opts = mongodb::options::ReplaceOptions::builder()
                    .upsert(true)
                    .build();
                coll.replace_one(key_filter, new_doc)
                    .with_options(opts)
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                // Propagate to secondary indexes and the stream within the
                // same transaction session. When the update creates the
                // item (previously did not exist), pass None as the old
                // image so the stream layer produces an INSERT record, not
                // a MODIFY with a synthesized key-only OldImage.
                self.sync_indexes_in_session(
                    key_info,
                    existing_item.as_ref(),
                    Some(&item),
                    &mut *session,
                )
                .await
                .map_err(TransactOpError::Storage)?;
                if let Some(capture) = stream {
                    let old_for_stream = if is_creating {
                        None
                    } else {
                        existing_item.as_ref()
                    };
                    self.write_stream_inline_in_session(
                        key_info,
                        capture,
                        old_for_stream,
                        Some(&item),
                        &mut *session,
                    )
                    .await
                    .map_err(TransactOpError::Storage)?;
                }

                Ok(())
            }
            OwnedTransactWriteOp::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf,
            } => {
                validation::validate_key_only(
                    key,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                let coll_name = data_collection_name(&key_info.table_id);
                let coll = self.data_db.collection::<Document>(&coll_name);
                let key_filter =
                    pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                let existing_doc = coll
                    .find_one(key_filter)
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                let existing_item = if let Some(doc) = existing_doc.as_ref() {
                    Some(document_to_item(doc).map_err(TransactOpError::Storage)?)
                } else {
                    None
                };

                let for_eval = existing_item.clone().unwrap_or_default();
                let passed =
                    expression::evaluate_condition(condition, &for_eval, maps).map_err(|e| {
                        TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                    })?;
                if !passed {
                    return Err(TransactOpError::Cancel(
                        CancellationReason::condition_check_failed_with_item(ccf_return_item(
                            *return_values_on_ccf,
                            existing_item.as_ref(),
                        )),
                    ));
                }

                Ok(())
            }
        }
    }

    // ── Pushdown fast path (A5) ──────────────────────────────────────
    //
    // Callers must pre-check the guard conditions:
    //   - condition is Some(cond)
    //   - stream.is_none()
    //   - gsi_cache_get_fresh(table_id) == Some(false)
    //   - is_pushable(cond, maps) == Pushable::Yes
    //
    // Under those guards, the write's atomicity is provided by MongoDB's
    // single-document find_one_and_* operators — no session needed, no
    // GSI sync, no stream record. The compiled filter merges with the
    // key filter so the operator matches only when both apply. On null
    // return, we follow up with a `find_one` against the key alone to
    // distinguish "key doesn't exist" from "condition failed".

    async fn delete_item_pushdown(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: &Expr,
        maps: &ExpressionMaps,
    ) -> Result<Option<Item>, StorageError> {
        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let key_filter = pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)?;
        let cond_filter = condition_to_filter(condition, maps)?;

        // Merge key filter and condition filter under an $and so the
        // delete only fires when both match.
        let merged = doc! { "$and": [key_filter.clone(), cond_filter] };

        let old_doc = coll
            .find_one_and_delete(merged)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if let Some(doc) = old_doc {
            let old_item = document_to_item(&doc)?;
            return Ok(if return_old { Some(old_item) } else { None });
        }

        // Null return: either the key doesn't exist or the condition
        // failed. Disambiguate with a follow-up find_one on the key.
        let existing = coll
            .find_one(key_filter)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        match existing {
            Some(doc) => {
                let existing_item = document_to_item(&doc)?;
                Err(StorageError::ConditionFailed(Some(existing_item)))
            }
            None => {
                // Key genuinely doesn't exist. Evaluate the condition
                // against an empty item to match DDB semantics — some
                // conditions (attribute_not_exists) evaluate to true
                // even when the item is missing, in which case the
                // delete is a no-op success rather than a condition
                // failure.
                let empty = std::collections::BTreeMap::new();
                let passed = expression::evaluate_condition(condition, &empty, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if passed {
                    Ok(None)
                } else {
                    Err(StorageError::ConditionFailed(None))
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn update_item_pushdown(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: &Expr,
        maps: &ExpressionMaps,
    ) -> Result<(Option<Item>, Option<Item>), StorageError> {
        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let key_filter = pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)?;
        let cond_filter = condition_to_filter(condition, maps)?;
        let merged = doc! { "$and": [key_filter.clone(), cond_filter] };

        // Load the item first so we can apply the update in Rust and
        // then replace it. This is a two-round-trip pushdown rather than
        // a single-RT one because DDB update expressions have richer
        // semantics than MongoDB's atomic update operators can express
        // in general (e.g. list_append, if_not_exists, arithmetic on
        // decimal strings). The win over the session-scoped fallback is
        // that we skip the session start/commit round trips.
        let existing_doc = coll
            .find_one(merged.clone())
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some(existing) = existing_doc else {
            // No document matched the key+condition filter. Disambiguate.
            let by_key = coll
                .find_one(key_filter.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return match by_key {
                Some(doc) => {
                    let existing_item = document_to_item(&doc)?;
                    Err(StorageError::ConditionFailed(Some(existing_item)))
                }
                None => {
                    // Key didn't exist. Evaluate condition against
                    // empty item (for attribute_not_exists-style
                    // guards that permit upsert).
                    let empty = std::collections::BTreeMap::new();
                    let passed = expression::evaluate_condition(condition, &empty, maps)
                        .map_err(|e| StorageError::Validation(e.to_string()))?;
                    if !passed {
                        return Err(StorageError::ConditionFailed(None));
                    }
                    // Condition allows the upsert. Build the new item
                    // from `key` + apply update actions.
                    let mut new_item = key.clone();
                    expression::apply_update(actions, &mut new_item, maps)
                        .map_err(|e| StorageError::Validation(e.to_string()))?;
                    let new_doc = item_to_document(
                        &new_item,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                    )?;
                    let opts = mongodb::options::ReplaceOptions::builder()
                        .upsert(true)
                        .build();
                    coll.replace_one(key_filter, new_doc)
                        .with_options(opts)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    Ok((None, if return_new { Some(new_item) } else { None }))
                }
            };
        };

        let existing_item = document_to_item(&existing)?;
        let mut new_item = existing_item.clone();
        expression::apply_update(actions, &mut new_item, maps)
            .map_err(|e| StorageError::Validation(e.to_string()))?;

        let new_doc = item_to_document(
            &new_item,
            &key_info.key_schema,
            &key_info.attribute_definitions,
        )?;

        // Bump the OCC version. The session-scoped path uses a versioned
        // filter to catch concurrent modifications; the pushdown path
        // does the same by merging the current version into the replace
        // filter. If a concurrent writer bumps _v between our find_one
        // and our replace_one, the replace matches nothing and we fall
        // back to a retry.
        let current_version = existing.get_i64("_v").unwrap_or(0);
        let mut new_doc_versioned = new_doc;
        new_doc_versioned.insert("_v", current_version + 1);

        let mut versioned_filter = key_filter.clone();
        if current_version == 0 {
            versioned_filter.insert("_v", doc! { "$not": { "$gt": 0_i64 } });
        } else {
            versioned_filter.insert("_v", current_version);
        }

        let result = coll
            .replace_one(versioned_filter, new_doc_versioned)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.matched_count == 0 {
            // Concurrent update raced us. Fall back to the session-scoped
            // path which has a retry loop. This is rare — return an
            // Internal error the trait implementer can catch and retry.
            return Err(StorageError::Internal(
                "pushdown update raced by concurrent writer; retry via session-scoped path"
                    .to_owned(),
            ));
        }

        let old_out = if return_old {
            Some(existing_item)
        } else {
            None
        };
        let new_out = if return_new { Some(new_item) } else { None };
        Ok((old_out, new_out))
    }
}

// ── Transaction helper types ──────────────────────────────────────────

enum TransactOpError {
    Cancel(extenddb_core::types::CancellationReason),
    Storage(StorageError),
}

/// Choose the `Item` value to include in a `CancellationReason` when a
/// condition check fails inside `TransactWriteItems`.
///
/// Per DynamoDB's contract, the pre-existing item is returned only when the
/// caller requested `ReturnValuesOnConditionCheckFailure = ALL_OLD` AND the
/// item existed at the time of the check. In all other cases the field is
/// omitted (returned as `None`).
fn ccf_return_item(
    rv: ReturnValuesOnConditionCheckFailure,
    existing: Option<&Item>,
) -> Option<Item> {
    match rv {
        ReturnValuesOnConditionCheckFailure::AllOld => existing.cloned(),
        ReturnValuesOnConditionCheckFailure::None => None,
    }
}

/// Owned version of `TransactWriteOp` to allow moving into async blocks.
enum OwnedTransactWriteOp {
    Put {
        key_info: TableKeyInfo,
        item: Item,
        condition: Option<Expr>,
        maps: ExpressionMaps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        stream: Option<StreamCapture>,
    },
    Delete {
        key_info: TableKeyInfo,
        key: Item,
        condition: Option<Expr>,
        maps: ExpressionMaps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        stream: Option<StreamCapture>,
    },
    Update {
        key_info: TableKeyInfo,
        key: Item,
        actions: Vec<UpdateAction>,
        condition: Option<Expr>,
        maps: ExpressionMaps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
        stream: Option<StreamCapture>,
    },
    ConditionCheck {
        key_info: TableKeyInfo,
        key: Item,
        condition: Expr,
        maps: ExpressionMaps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
    },
}

fn clone_transact_write_op(op: &TransactWriteOp<'_>) -> OwnedTransactWriteOp {
    match op {
        TransactWriteOp::Put {
            key_info,
            item,
            condition,
            maps,
            return_values_on_ccf,
            stream,
        } => OwnedTransactWriteOp::Put {
            key_info: (*key_info).clone(),
            item: (*item).clone(),
            condition: condition.cloned(),
            maps: (*maps).clone(),
            return_values_on_ccf: *return_values_on_ccf,
            stream: stream.clone(),
        },
        TransactWriteOp::Delete {
            key_info,
            key,
            condition,
            maps,
            return_values_on_ccf,
            stream,
        } => OwnedTransactWriteOp::Delete {
            key_info: (*key_info).clone(),
            key: (*key).clone(),
            condition: condition.cloned(),
            maps: (*maps).clone(),
            return_values_on_ccf: *return_values_on_ccf,
            stream: stream.clone(),
        },
        TransactWriteOp::Update {
            key_info,
            key,
            actions,
            condition,
            maps,
            return_values_on_ccf,
            stream,
        } => OwnedTransactWriteOp::Update {
            key_info: (*key_info).clone(),
            key: (*key).clone(),
            actions: actions.to_vec(),
            condition: condition.cloned(),
            maps: (*maps).clone(),
            return_values_on_ccf: *return_values_on_ccf,
            stream: stream.clone(),
        },
        TransactWriteOp::ConditionCheck {
            key_info,
            key,
            condition,
            maps,
            return_values_on_ccf,
        } => OwnedTransactWriteOp::ConditionCheck {
            key_info: (*key_info).clone(),
            key: (*key).clone(),
            condition: (*condition).clone(),
            maps: (*maps).clone(),
            return_values_on_ccf: *return_values_on_ccf,
        },
    }
}

/// Resolve a key expression (Placeholder) to an `AttributeValue`.
fn resolve_key_expr(expr: &Expr, maps: &ExpressionMaps) -> Result<AttributeValue, StorageError> {
    match expr {
        Expr::Placeholder(name) => maps
            .resolve_value(name)
            .cloned()
            .map_err(|e| StorageError::Validation(e.to_string())),
        _ => Err(StorageError::Internal(
            "expected placeholder in key condition".to_owned(),
        )),
    }
}

/// Build a `MongoDB` filter for a sort key condition.
fn build_sk_filter(
    sk_cond: &SortKeyCondition,
    sk_field: &str,
    maps: &ExpressionMaps,
) -> Result<Document, StorageError> {
    match sk_cond {
        SortKeyCondition::Compare { op, value, .. } => {
            let av = resolve_key_expr(value, maps)?;
            let sk_type = infer_sk_type_from_field(sk_field);
            let bson_val = sk_to_bson(&av, sk_type)?;

            let filter = match op {
                extenddb_core::expression::CompareOp::Eq => doc! { sk_field: bson_val },
                extenddb_core::expression::CompareOp::Lt => doc! { sk_field: { "$lt": bson_val } },
                extenddb_core::expression::CompareOp::Le => doc! { sk_field: { "$lte": bson_val } },
                extenddb_core::expression::CompareOp::Gt => doc! { sk_field: { "$gt": bson_val } },
                extenddb_core::expression::CompareOp::Ge => doc! { sk_field: { "$gte": bson_val } },
                extenddb_core::expression::CompareOp::Ne => doc! { sk_field: { "$ne": bson_val } },
            };
            Ok(filter)
        }
        SortKeyCondition::Between { low, high, .. } => {
            let sk_type = infer_sk_type_from_field(sk_field);
            let low_av = resolve_key_expr(low, maps)?;
            let high_av = resolve_key_expr(high, maps)?;
            if sk_between_low_gt_high(&low_av, &high_av) {
                return Err(StorageError::Validation(
                    "Invalid KeyConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound".to_owned(),
                ));
            }
            let low_bson = sk_to_bson(&low_av, sk_type)?;
            let high_bson = sk_to_bson(&high_av, sk_type)?;
            Ok(doc! { sk_field: { "$gte": low_bson, "$lte": high_bson } })
        }
        SortKeyCondition::BeginsWith { prefix, .. } => {
            let prefix_av = resolve_key_expr(prefix, maps)?;
            match prefix_av {
                AttributeValue::S(ref p) => {
                    // For begins_with on string sort keys: sk_s >= prefix AND sk_s < prefix + max_char
                    let upper = increment_string(p);
                    Ok(doc! { sk_field: { "$gte": p.as_str(), "$lt": &upper } })
                }
                AttributeValue::B(ref _b) => {
                    // BSON Binary comparison sorts by length first, then by content.
                    // This means $gte/$lt range queries don't work for prefix matching
                    // when the prefix is shorter than the stored values. Return an empty
                    // filter here and let the caller do post-fetch prefix filtering.
                    Ok(Document::new())
                }
                _ => Err(StorageError::Validation(
                    "begins_with requires string or binary sort key".to_string(),
                )),
            }
        }
    }
}

/// Convert an `AttributeValue` sort key to the appropriate BSON type.
/// Return true when a sort-key BETWEEN's low bound is strictly greater than its high bound.
///
/// DynamoDB rejects this at the wire layer with a ValidationException; the storage
/// backend must reject it too, since the engine layer only validates BETWEEN for
/// filter/condition expressions, not for KeyConditionExpression's sort-key path.
///
/// The comparison is done in the source AttributeValue domain so it happens before
/// any Decimal128/f64 conversion that could mask ordering. Strings are compared
/// lexicographically (matching DynamoDB), numbers via f64 (adequate for ordering —
/// values exceeding Decimal128 range are rejected downstream in `sk_to_bson`), and
/// binary bytewise.
fn sk_between_low_gt_high(low: &AttributeValue, high: &AttributeValue) -> bool {
    match (low, high) {
        (AttributeValue::S(l), AttributeValue::S(h)) => l > h,
        (AttributeValue::N(l), AttributeValue::N(h)) => {
            match (l.parse::<f64>(), h.parse::<f64>()) {
                (Ok(lf), Ok(hf)) => lf > hf,
                _ => false, // downstream sk_to_bson will surface the parse error
            }
        }
        (AttributeValue::B(l), AttributeValue::B(h)) => l > h,
        _ => false, // type mismatch — downstream sk_to_bson will surface it
    }
}

fn sk_to_bson(
    av: &AttributeValue,
    sk_type: ScalarAttributeType,
) -> Result<bson::Bson, StorageError> {
    match (sk_type, av) {
        (ScalarAttributeType::S, AttributeValue::S(s)) => Ok(bson::Bson::String(s.clone())),
        (ScalarAttributeType::N, AttributeValue::N(n)) => n
            .parse::<bson::Decimal128>()
            .map(bson::Bson::Decimal128)
            .map_err(|_| {
                StorageError::Validation(format!(
                    "Numeric sort key value '{n}' exceeds supported precision (Decimal128, 34 significant digits)"
                ))
            }),
        (ScalarAttributeType::B, AttributeValue::B(b)) => Ok(bson::Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: b.clone(),
        })),
        _ => Err(StorageError::Internal("sort key type mismatch".to_string())),
    }
}

/// Infer the `ScalarAttributeType` from the sort key field name.
fn infer_sk_type_from_field(field: &str) -> ScalarAttributeType {
    if field.ends_with("_n") {
        ScalarAttributeType::N
    } else if field.ends_with("_b") {
        ScalarAttributeType::B
    } else {
        ScalarAttributeType::S
    }
}

/// Increment a string to get the exclusive upper bound for `begins_with`.
fn increment_string(s: &str) -> String {
    // Append the maximum Unicode code point
    let mut result = s.to_string();
    result.push(char::MAX);
    result
}

/// Increment bytes to get the exclusive upper bound for `begins_with` on binary.
fn increment_bytes(b: &[u8]) -> Vec<u8> {
    let mut result = b.to_vec();
    // Increment the last byte, with carry
    let mut i = result.len();
    while i > 0 {
        i -= 1;
        if result[i] < 255 {
            result[i] += 1;
            return result;
        }
        result[i] = 0;
    }
    // All bytes were 0xFF; prepend a 0x01 byte (makes it longer)
    result.insert(0, 1);
    result
}

fn item_has_index_keys(item: &Item, idx_key_schema: &[KeySchemaElement]) -> bool {
    idx_key_schema
        .iter()
        .all(|ks| item.contains_key(&ks.attribute_name))
}

fn project_item(
    item: &Item,
    idx_key_schema: &[KeySchemaElement],
    base_key_schema: &[KeySchemaElement],
    projection: &Projection,
) -> Item {
    match projection.projection_type {
        ProjectionType::All => item.clone(),
        ProjectionType::KeysOnly => {
            let mut projected = Item::new();
            for ks in idx_key_schema.iter().chain(base_key_schema.iter()) {
                if let Some(v) = item.get(&ks.attribute_name) {
                    projected.insert(ks.attribute_name.clone(), v.clone());
                }
            }
            projected
        }
        ProjectionType::Include => {
            let mut projected = Item::new();
            // Always include key attributes
            for ks in idx_key_schema.iter().chain(base_key_schema.iter()) {
                if let Some(v) = item.get(&ks.attribute_name) {
                    projected.insert(ks.attribute_name.clone(), v.clone());
                }
            }
            // Include non-key attributes from projection
            if let Some(ref attrs) = projection.non_key_attributes {
                for attr in attrs {
                    if let Some(v) = item.get(attr) {
                        projected.insert(attr.clone(), v.clone());
                    }
                }
            }
            projected
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn between_low_gt_high_string() {
        assert!(sk_between_low_gt_high(
            &AttributeValue::S("z".into()),
            &AttributeValue::S("a".into())
        ));
        assert!(!sk_between_low_gt_high(
            &AttributeValue::S("a".into()),
            &AttributeValue::S("z".into())
        ));
        assert!(!sk_between_low_gt_high(
            &AttributeValue::S("m".into()),
            &AttributeValue::S("m".into())
        ));
    }

    #[test]
    fn between_low_gt_high_number() {
        assert!(sk_between_low_gt_high(
            &AttributeValue::N("100".into()),
            &AttributeValue::N("50".into())
        ));
        assert!(!sk_between_low_gt_high(
            &AttributeValue::N("50".into()),
            &AttributeValue::N("100".into())
        ));
        assert!(!sk_between_low_gt_high(
            &AttributeValue::N("42".into()),
            &AttributeValue::N("42".into())
        ));
    }

    #[test]
    fn ccf_return_item_all_old_with_existing() {
        let mut item = Item::new();
        item.insert("a".to_string(), AttributeValue::S("1".to_string()));
        let returned = ccf_return_item(ReturnValuesOnConditionCheckFailure::AllOld, Some(&item));
        assert_eq!(returned, Some(item));
    }

    #[test]
    fn ccf_return_item_all_old_without_existing() {
        let returned = ccf_return_item(ReturnValuesOnConditionCheckFailure::AllOld, None);
        assert_eq!(returned, None);
    }

    #[test]
    fn ccf_return_item_none_never_returns() {
        let mut item = Item::new();
        item.insert("a".to_string(), AttributeValue::S("1".to_string()));
        let returned = ccf_return_item(ReturnValuesOnConditionCheckFailure::None, Some(&item));
        assert_eq!(returned, None);
    }

    #[test]
    fn between_low_gt_high_binary() {
        assert!(sk_between_low_gt_high(
            &AttributeValue::B(vec![0xff]),
            &AttributeValue::B(vec![0x00])
        ));
        assert!(!sk_between_low_gt_high(
            &AttributeValue::B(vec![0x00]),
            &AttributeValue::B(vec![0xff])
        ));
    }
}
