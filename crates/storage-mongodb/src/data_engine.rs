// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DataEngine` trait implementation for `MongoEngine`.

use bson::{Document, doc};
use futures::future::BoxFuture;
use mongodb::options::{FindOneAndReplaceOptions, ReturnDocument};

use extenddb_core::expression::{
    self, Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition, UpdateAction,
    resolve_name_ref,
};
use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, ReturnValuesOnConditionCheckFailure,
    ScalarAttributeType, StreamEventName, StreamRecord, StreamRecordData, TableKeyInfo,
    extract_key, item_size_bytes,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    composite_pk_to_text, encode_netstring_composite, pk_to_text, sk_info,
};
use extenddb_storage::{
    DataEngine, IdempotencyKey, ItemPairResult, QueryResult, StreamCapture, TransactGetOp,
    TransactWriteOp,
};

use crate::MongoEngine;
use crate::condition::condition_to_filter;
use crate::data::{
    binary_sk_to_hex, composite_id, data_collection_name, document_to_item, index_document,
    index_entry_filter, item_to_document, pk_filter, sk_field_name, sk_suffix,
};
use crate::pushdown::{Pushable, is_pushable};

use extenddb_core::types::{Projection, ProjectionType};

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

/// Distinguishes live GSI creation, where concurrent writes are possible,
/// from restore backfills, where the target table is still unavailable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GsiBackfillMode {
    Live,
    Restore,
}

/// Invariants shared by every item in one GSI backfill batch.
pub(crate) struct GsiBackfillContext<'a> {
    pub(crate) key_info: &'a TableKeyInfo,
    pub(crate) index_id: &'a str,
    pub(crate) idx_key_schema: &'a [KeySchemaElement],
    pub(crate) projection: &'a Projection,
    pub(crate) mode: GsiBackfillMode,
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
        // Up-front index-key validation — must run before any write
        // work so the caller sees a top-level ValidationException on
        // wrong-type or empty index-key attributes (D-M10, RFC-0003
        // §2.3).
        self.validate_index_keys_for_item(key_info, &item).await?;

        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let key_filter = pk_filter(&item, &key_info.key_schema, &key_info.attribute_definitions)?;

        // Sessionless fast path for unconditional PutItem on a plain
        // table (no cond, no stream, no GSI). DDB's contract is
        // last-writer-wins with no client-visible conflict error
        // (RFC-0003 §4.1). Wrapping this in a snapshot transaction
        // would convert same-key contention into WriteConflict
        // aborts that eventually surface as `Internal` — a wire-
        // visible error DDB never emits. Rely on WiredTiger's
        // single-document atomicity instead. Two concurrent writes
        // serialize at the storage engine level; one wins the last-
        // writer-wins race and the other's version is overwritten.
        // No txn, no retry loop, no possible 500 from contention.
        if condition.is_none()
            && stream.is_none()
            && self.gsi_cache_get_fresh(&key_info.table_id) == Some(false)
        {
            let new_doc =
                item_to_document(&item, &key_info.key_schema, &key_info.attribute_definitions)?;
            let opts = FindOneAndReplaceOptions::builder()
                .upsert(true)
                .return_document(ReturnDocument::Before)
                .build();
            let old_doc = coll
                .find_one_and_replace(key_filter, new_doc)
                .with_options(opts)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let old_item = old_doc.as_ref().map(document_to_item).transpose()?;
            return Ok(if return_old { old_item } else { None });
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

        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            let new_doc =
                item_to_document(&item, &key_info.key_schema, &key_info.attribute_definitions)?;
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let attempt_res: Result<Option<Item>, TxErr> = async {
                let old_item: Option<Item>;

                if let Some(cond) = condition {
                    let existing_doc = coll
                        .find_one(key_filter.clone())
                        .session(&mut session)
                        .await
                        .map_err(TxErr::from)?;

                    if let Some(ref existing) = existing_doc {
                        let existing_item = document_to_item(existing)?;
                        let passed = expression::evaluate_condition(cond, &existing_item, maps)
                            .map_err(|e| TxErr::Fatal(StorageError::Validation(e.to_string())))?;
                        if !passed {
                            return Err(TxErr::Fatal(StorageError::ConditionFailed(Some(
                                existing_item,
                            ))));
                        }
                        let opts = FindOneAndReplaceOptions::builder()
                            .return_document(ReturnDocument::Before)
                            .build();
                        let old_doc = coll
                            .find_one_and_replace(key_filter.clone(), new_doc)
                            .with_options(opts)
                            .session(&mut session)
                            .await
                            .map_err(TxErr::from)?;
                        old_item = old_doc.as_ref().map(document_to_item).transpose()?;
                    } else {
                        let empty = std::collections::BTreeMap::new();
                        let passed = expression::evaluate_condition(cond, &empty, maps)
                            .map_err(|e| TxErr::Fatal(StorageError::Validation(e.to_string())))?;
                        if !passed {
                            return Err(TxErr::Fatal(StorageError::ConditionFailed(None)));
                        }
                        // Conditional insert: a concurrent inserter
                        // manifests either as E11000 (unique-index
                        // race) or as WriteConflict (snapshot-isolation
                        // race). Both are the runtime signature of a
                        // failed condition. Map dup-key to CCF with
                        // the winner's image; let WriteConflict fall
                        // through TxErr::Transient and retry — the
                        // retry will re-read and see the winner.
                        if let Err(e) = coll.insert_one(new_doc).session(&mut session).await {
                            if is_duplicate_key(&e) {
                                let _ = session.abort_transaction().await;
                                let winner = coll
                                    .find_one(key_filter.clone())
                                    .await
                                    .map_err(|e2| {
                                        TxErr::Fatal(StorageError::Internal(e2.to_string()))
                                    })?
                                    .map(|d| document_to_item(&d))
                                    .transpose()?;
                                return Err(TxErr::Fatal(StorageError::ConditionFailed(winner)));
                            }
                            return Err(TxErr::from(e));
                        }
                        old_item = None;
                    }
                } else {
                    let opts = FindOneAndReplaceOptions::builder()
                        .upsert(true)
                        .return_document(ReturnDocument::Before)
                        .build();
                    let old_doc = coll
                        .find_one_and_replace(key_filter.clone(), new_doc)
                        .with_options(opts)
                        .session(&mut session)
                        .await
                        .map_err(TxErr::from)?;
                    old_item = old_doc.as_ref().map(document_to_item).transpose()?;
                }

                self.sync_indexes_in_session(
                    key_info,
                    old_item.as_ref(),
                    Some(&item),
                    &mut session,
                )
                .await?;

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

                Ok(if return_old { old_item } else { None })
            }
            .await;

            match attempt_res {
                Ok(return_val) => match session.commit_transaction().await {
                    Ok(()) => return Ok(return_val),
                    Err(e) if is_transient_write_conflict(&e) => {
                        backoff_sleep(attempt).await;
                        continue;
                    }
                    Err(e) => return Err(StorageError::Internal(e.to_string())),
                },
                Err(TxErr::Transient) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                    continue;
                }
                Err(TxErr::Fatal(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }

        // Retry ceiling exhausted. RFC-0003 §4.3 requires
        // `TransactionConflictException` when a single-item write can't
        // serialize against concurrent activity — never a bare 500.
        Err(StorageError::TransactionConflict(
            "PutItem: too many concurrent write conflicts, giving up".to_owned(),
        ))
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

        // Sessionless fast path for unconditional DeleteItem on a plain
        // table (no cond, no stream, no GSI). Same rationale as
        // put_item_impl — DDB never surfaces contention on unconditional
        // single-item deletes. RFC-0003 §4.1.
        if condition.is_none()
            && stream.is_none()
            && self.gsi_cache_get_fresh(&key_info.table_id) == Some(false)
        {
            let old_doc = coll
                .find_one_and_delete(key_filter)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let deleted_item = old_doc.as_ref().map(document_to_item).transpose()?;
            return Ok(if return_old { deleted_item } else { None });
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

        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let attempt_res: Result<Option<Item>, TxErr> = async {
                let deleted_item: Option<Item>;

                if let Some(cond) = condition {
                    let existing_doc = coll
                        .find_one(key_filter.clone())
                        .session(&mut session)
                        .await
                        .map_err(TxErr::from)?;

                    if let Some(ref existing) = existing_doc {
                        let existing_item = document_to_item(existing)?;
                        let passed = expression::evaluate_condition(cond, &existing_item, maps)
                            .map_err(|e| TxErr::Fatal(StorageError::Validation(e.to_string())))?;
                        if !passed {
                            return Err(TxErr::Fatal(StorageError::ConditionFailed(Some(
                                existing_item,
                            ))));
                        }
                        coll.delete_one(key_filter.clone())
                            .session(&mut session)
                            .await
                            .map_err(TxErr::from)?;
                        deleted_item = Some(existing_item);
                    } else {
                        let empty = std::collections::BTreeMap::new();
                        let passed = expression::evaluate_condition(cond, &empty, maps)
                            .map_err(|e| TxErr::Fatal(StorageError::Validation(e.to_string())))?;
                        if !passed {
                            return Err(TxErr::Fatal(StorageError::ConditionFailed(None)));
                        }
                        deleted_item = None;
                    }
                } else {
                    let old_doc = coll
                        .find_one_and_delete(key_filter.clone())
                        .session(&mut session)
                        .await
                        .map_err(TxErr::from)?;
                    deleted_item = old_doc.as_ref().map(document_to_item).transpose()?;
                }

                if deleted_item.is_some() {
                    self.sync_indexes_in_session(
                        key_info,
                        deleted_item.as_ref(),
                        None,
                        &mut session,
                    )
                    .await?;
                }

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

                Ok(if return_old { deleted_item } else { None })
            }
            .await;

            match attempt_res {
                Ok(return_val) => match session.commit_transaction().await {
                    Ok(()) => return Ok(return_val),
                    Err(e) if is_transient_write_conflict(&e) => {
                        backoff_sleep(attempt).await;
                        continue;
                    }
                    Err(e) => return Err(StorageError::Internal(e.to_string())),
                },
                Err(TxErr::Transient) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                    continue;
                }
                Err(TxErr::Fatal(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }

        Err(StorageError::TransactionConflict(
            "DeleteItem: too many concurrent write conflicts, giving up".to_owned(),
        ))
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
        // This avoids transactions and retries for simple unconditional
        // updates. Gated on the table having no GSIs — the fast path
        // does not read the pre-image, so it has no way to compute the
        // GSI-key delta and would leave stale index entries when an
        // indexed attribute is $set to a new value or $unset (RFC-0003
        // §2.2). The GSI cache lets us avoid a catalog query in the
        // common (no-GSI) case; when the cache is stale-or-unknown
        // we fall through to the slow path which is authoritative.
        if condition.is_none()
            && !return_old
            && stream.is_none()
            && self.gsi_cache_get_fresh(&key_info.table_id) == Some(false)
            && let Some(mongo_update) = self.try_build_native_update(actions, maps)
        {
            let (fast_filter, took_fast) = match mongo_update {
                NativeUpdate::Doc(d) => {
                    let opts = mongodb::options::FindOneAndUpdateOptions::builder()
                        .upsert(true)
                        .return_document(ReturnDocument::After)
                        .build();
                    let result = coll
                        .find_one_and_update(key_filter.clone(), d)
                        .with_options(opts)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    (Some(result), true)
                }
                NativeUpdate::Pipeline {
                    type_guard,
                    pipeline,
                } => {
                    // Compose the key filter with the type guard. If
                    // the doc exists but fails the guard, findAndModify
                    // returns None and we fall through to the slow
                    // path (which raises ValidationException).
                    // Upsert is disabled here because a missing-doc
                    // "no match" is indistinguishable from a
                    // type-mismatch "no match"; the slow path handles
                    // both correctly.
                    let combined_filter = if let Some(guard) = type_guard {
                        doc! { "$and": [key_filter.clone(), guard] }
                    } else {
                        key_filter.clone()
                    };
                    let opts = mongodb::options::FindOneAndUpdateOptions::builder()
                        .upsert(false)
                        .return_document(ReturnDocument::After)
                        .build();
                    let result = coll
                        .find_one_and_update(combined_filter, pipeline)
                        .with_options(opts)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    if result.is_none() {
                        // Either the doc doesn't exist yet (need to
                        // upsert with proper type handling) or the
                        // guard rejected it. Fall through.
                        (None, false)
                    } else {
                        (Some(result), true)
                    }
                }
            };

            if took_fast {
                let result_doc = fast_filter.and_then(|d| d);
                let new_item = if return_new {
                    result_doc.as_ref().map(document_to_item).transpose()?
                } else {
                    None
                };
                return Ok((None, new_item));
            }
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

        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            // Sentinel returned by the attempt body to signal "the
            // OCC version guard didn't match; retry from a fresh
            // read." Distinct from TxErr::Transient because it isn't
            // a mongo-side conflict — the whole snapshot succeeded,
            // we just lost the CAS race.
            struct StaleVersion;
            #[allow(clippy::large_enum_variant)]
            enum AttemptOk {
                Committed(Option<Item>, Option<Item>),
                Stale,
            }

            let attempt_res: Result<AttemptOk, TxErr> = async {
                let existing_doc = coll
                    .find_one(key_filter.clone())
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?;

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
                        .map_err(|e| TxErr::Fatal(StorageError::Validation(e.to_string())))?;
                    if !passed {
                        return Err(TxErr::Fatal(StorageError::ConditionFailed(
                            if existing_doc.is_some() {
                                Some(existing_item.clone())
                            } else {
                                None
                            },
                        )));
                    }
                }

                let pre_image = existing_doc.as_ref().map(|_| existing_item.clone());
                let old_item_for_stream = if return_old || stream.is_some() {
                    pre_image.clone()
                } else {
                    None
                };

                let mut new_item = existing_item;
                expression::apply_update_validated(
                    actions,
                    &mut new_item,
                    maps,
                    &key_info.vector_indexes,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| TxErr::Fatal(StorageError::Validation(e.to_string())))?;

                // Reject wrong-type or empty index-key attributes on
                // the resulting item — D-M10, RFC-0003 §2.3. Same
                // shape as put_item's up-front check, but the
                // post-update item is what actually gets written.
                self.validate_index_keys_for_item(key_info, &new_item)
                    .await
                    .map_err(TxErr::Fatal)?;

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
                        .map_err(TxErr::from)?;

                    if result.matched_count == 0 {
                        // OCC CAS lost: someone else bumped _v after
                        // our find_one. Not a mongo conflict — the
                        // snapshot txn succeeded, we just have a
                        // stale read. Signal retry.
                        let _ = StaleVersion;
                        return Ok(AttemptOk::Stale);
                    }
                } else {
                    let opts = mongodb::options::ReplaceOptions::builder()
                        .upsert(true)
                        .build();
                    coll.replace_one(key_filter.clone(), new_doc)
                        .with_options(opts)
                        .session(&mut session)
                        .await
                        .map_err(TxErr::from)?;
                }

                self.sync_indexes_in_session(
                    key_info,
                    pre_image.as_ref(),
                    Some(&new_item),
                    &mut session,
                )
                .await?;

                if let Some(capture) = stream {
                    self.write_stream_inline_in_session(
                        key_info,
                        capture,
                        old_item_for_stream.as_ref(),
                        Some(&new_item),
                        &mut session,
                    )
                    .await?;
                }

                let old_item_result = if return_old {
                    old_item_for_stream
                } else {
                    None
                };
                let new_item_result = if return_new { Some(new_item) } else { None };
                Ok(AttemptOk::Committed(old_item_result, new_item_result))
            }
            .await;

            match attempt_res {
                Ok(AttemptOk::Committed(old, new)) => match session.commit_transaction().await {
                    Ok(()) => return Ok((old, new)),
                    Err(e) if is_transient_write_conflict(&e) => {
                        backoff_sleep(attempt).await;
                        continue;
                    }
                    Err(e) => return Err(StorageError::Internal(e.to_string())),
                },
                Ok(AttemptOk::Stale) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                    continue;
                }
                Err(TxErr::Transient) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                    continue;
                }
                Err(TxErr::Fatal(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }

        Err(StorageError::TransactionConflict(
            "UpdateItem: too many concurrent write conflicts, giving up".to_owned(),
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
                // Merge the resume bound into any existing sort-key
                // predicate rather than replacing it. Naive
                // `filter.insert(sk_f, {$gt: cursor})` drops the
                // caller's original range/prefix/eq bound and returns
                // items outside it on page 2+ (RFC-0003 §7.2).
                let cursor_bound = doc! { cmp_gt: sk_bson };
                match filter.remove(sk_f) {
                    None => {
                        filter.insert(sk_f, cursor_bound);
                    }
                    Some(bson::Bson::Document(mut existing)) => {
                        // Existing predicate already uses operators
                        // ($gte/$lte/$lt/...); merge ours into the
                        // same operator map. If the caller and the
                        // cursor share an operator (both $gt on a
                        // forward page whose caller filtered $gt),
                        // overwriting with the cursor is correct —
                        // the cursor's bound is always strictly
                        // beyond the caller's for that direction.
                        for (k, v) in cursor_bound {
                            existing.insert(k, v);
                        }
                        filter.insert(sk_f, existing);
                    }
                    Some(scalar) => {
                        // Caller's predicate was an equality
                        // (`sk = X`). Combine with the resume bound
                        // under $and — a scalar sk_f binding can't
                        // hold a $gt sibling.
                        let clauses = bson::bson!([
                            { sk_f: scalar },
                            { sk_f: cursor_bound },
                        ]);
                        if let Some(existing_and) = filter.remove("$and") {
                            let mut combined = match existing_and {
                                bson::Bson::Array(a) => a,
                                other => vec![other],
                            };
                            if let bson::Bson::Array(new) = clauses {
                                combined.extend(new);
                            }
                            filter.insert("$and", combined);
                        } else {
                            filter.insert("$and", clauses);
                        }
                    }
                }
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

        // Binary begins_with used to require a post-fetch pass because
        // BSON Binary comparison is length-first and $gte/$lt-on-Binary
        // dropped matches whenever the prefix was shorter than the stored
        // value. Since D-M5 stores binary sort keys as hex strings, the
        // $gte/$lt filter emitted by build_sk_filter is now authoritative
        // and no post-fetch filtering is needed. RFC-0003 §1.4.

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

        // Lazy cursor iteration. The segment filter (CRC32 hash of pk
        // mod total_segments) is applied per-item after fetching, so
        // any hard server-side limit interacts badly with skew: with
        // a modest hot-key concentration, a whole `(limit+1) *
        // total_segments` window can land in one segment and leave
        // the others empty, terminating the scan early with the
        // remaining items silently dropped. RFC-0003 §7.3.
        //
        // Instead, stream the cursor and stop when either
        //   (a) we have `limit + 1` in-segment items (so we know we
        //       need a LEK for the next page), or
        //   (b) the cursor is exhausted.
        // mongo batches under the hood (~101 docs per network trip),
        // so this is efficient without a hard limit — we consume at
        // most one extra network batch beyond what we return.
        let opts = mongodb::options::FindOptions::builder()
            .sort(sort_doc)
            .build();

        let mut cursor = coll
            .find(filter)
            .with_options(opts)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut items: Vec<Item> = Vec::new();
        let target = limit.map(|l| {
            #[allow(clippy::cast_sign_loss)]
            let l = l as usize;
            l + 1
        });

        while let Some(doc) = cursor
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            let item = document_to_item(&doc)?;

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

            if let Some(t) = target
                && items.len() >= t
            {
                break;
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

    /// Try to express the update as a native MongoDB atomic update.
    ///
    /// Returns:
    /// - `Some(NativeUpdate::Doc(...))` for a plain operator update
    ///   (`$set`/`$unset`/`$inc`), served by
    ///   `find_one_and_update(filter, doc)`.
    /// - `Some(NativeUpdate::Pipeline(...))` for numeric `ADD`, which
    ///   requires an aggregation-pipeline update (`$set` with computed
    ///   expressions) to convert the string-stored `.N` value to a
    ///   `Decimal128`, add the delta, and convert back — all
    ///   server-side.
    /// - `None` on anything else — set-typed `ADD`, `DELETE`,
    ///   `list_append`, `if_not_exists`, arithmetic, multi-component
    ///   paths. Those fall through to the session-scoped
    ///   read-modify-write path.
    ///
    /// The pipeline form is what makes RFC-0003 §4.4 achievable
    /// without a numeric shadow field: 50+ concurrent
    /// `UpdateItem ADD counter :one` calls all apply cumulatively
    /// because mongo serializes doc-scoped write locks around the
    /// pipeline's read + compute + write, no OCC retry needed.
    fn try_build_native_update(
        &self,
        actions: &[UpdateAction],
        maps: &ExpressionMaps,
    ) -> Option<NativeUpdate> {
        let mut set_doc = Document::new();
        let mut unset_doc = Document::new();
        let mut num_adds: Vec<(String, String)> = Vec::new();

        for action in actions {
            match action {
                UpdateAction::Add { path, value } => {
                    if path.len() != 1 {
                        return None;
                    }
                    let raw_name = match &path[0] {
                        PathElement::Attribute(name) => name,
                        _ => return None,
                    };
                    let attr_name = resolve_name_ref(raw_name, maps).ok()?.into_owned();
                    let val = match value {
                        Expr::Placeholder(name) => maps.resolve_value(name).ok()?,
                        _ => return None,
                    };
                    match val {
                        AttributeValue::N(n) => {
                            // Validate the delta parses as Decimal128
                            // up-front so a bad number fails fast
                            // rather than mid-pipeline on mongo.
                            if n.parse::<bson::Decimal128>().is_err() {
                                return None;
                            }
                            num_adds.push((attr_name, n.clone()));
                        }
                        AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_) => {
                            // Set ADD — $addToSet would work in theory
                            // but our .SS/.NS/.BS storage keeps the
                            // values inside item_data.<attr>.SS as an
                            // array. Not urgent enough to expand yet.
                            return None;
                        }
                        _ => return None,
                    }
                }
                UpdateAction::Delete { .. } => return None,
                UpdateAction::Set { path, value } => {
                    if path.len() != 1 {
                        return None;
                    }
                    let raw_name = match &path[0] {
                        PathElement::Attribute(name) => name,
                        _ => return None,
                    };
                    let attr_name = resolve_name_ref(raw_name, maps).ok()?;
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
                    let raw_name = match &path[0] {
                        PathElement::Attribute(name) => name,
                        _ => return None,
                    };
                    let attr_name = resolve_name_ref(raw_name, maps).ok()?;
                    let field = format!("item_data.{attr_name}");
                    unset_doc.insert(field, 1);
                }
            }
        }

        if set_doc.is_empty() && unset_doc.is_empty() && num_adds.is_empty() {
            return None;
        }

        if !num_adds.is_empty() {
            // Aggregation-pipeline stage. `$set` accepts computed
            // expressions here (unlike an operator update's `$set`).
            // Each numeric ADD is `<field> = toString(toDecimal(field
            // or 0) + delta)`; `$unset` is expressed as `<field> =
            // "$$REMOVE"`; SET actions are literal assignments. `_v`
            // is bumped in the same stage.
            let mut stage: Document = Document::new();
            for (k, v) in &set_doc {
                stage.insert(k, v.clone());
            }
            for k in unset_doc.keys() {
                stage.insert(k, "$$REMOVE");
            }
            // Guard: the fast path never reads the pre-image, so we
            // can't detect an existing non-numeric attribute (e.g.
            // ADD to a string). Require every ADD target to be
            // absent or already hold `.N` — else return no match and
            // let the caller fall back to the slow path, which reads
            // the pre-image and returns a proper ValidationException.
            let mut guard_clauses: Vec<Document> = Vec::with_capacity(num_adds.len());
            for (attr, delta_s) in &num_adds {
                let field = format!("item_data.{attr}.N");
                let field_ref = format!("${field}");
                let attr_path = format!("item_data.{attr}");
                let delta_dec = delta_s
                    .parse::<bson::Decimal128>()
                    .expect("validated above");
                stage.insert(
                    &field,
                    doc! {
                        "$toString": {
                            "$add": [
                                { "$toDecimal": { "$ifNull": [ &field_ref, "0" ] } },
                                { "$toDecimal": bson::Bson::Decimal128(delta_dec) },
                            ]
                        }
                    },
                );
                guard_clauses.push(doc! {
                    "$or": [
                        { &attr_path: { "$exists": false } },
                        { &field: { "$exists": true } },
                    ]
                });
            }
            stage.insert(
                "_v",
                doc! { "$add": [ { "$ifNull": [ "$_v", 0_i64 ] }, 1_i64 ] },
            );
            let type_guard = if guard_clauses.is_empty() {
                None
            } else if guard_clauses.len() == 1 {
                Some(guard_clauses.into_iter().next().unwrap())
            } else {
                Some(doc! { "$and": guard_clauses })
            };
            return Some(NativeUpdate::Pipeline {
                type_guard,
                pipeline: vec![doc! { "$set": stage }],
            });
        }

        let mut update = Document::new();
        if !set_doc.is_empty() {
            update.insert("$set", set_doc);
        }
        if !unset_doc.is_empty() {
            update.insert("$unset", unset_doc);
        }

        // Bump `_v` on every native fast-path write. Without this a
        // fast-path commit leaves `_v` at its previous value, and a
        // slow-path update running concurrently against that same
        // stale value can pass its versioned-filter guard and
        // overwrite the fast-path write (lost update, RFC-0003 §4.4).
        let mut inc_doc = Document::new();
        inc_doc.insert("_v", 1_i64);
        update.insert("$inc", inc_doc);

        Some(NativeUpdate::Doc(update))
    }

    // ── GSI Sync ──────────────────────────────────────────────────────

    /// Fetch (index_name, key_schema) for every index on the table.
    /// Used by up-front input validation so PutItem / UpdateItem
    /// rejects wrong-type or empty index-key attributes with a
    /// top-level ValidationException before doing any write work
    /// (D-M10, matches postgres put_item.rs).
    async fn fetch_index_key_schemas(
        &self,
        table_id: &str,
    ) -> Result<Vec<(String, Vec<KeySchemaElement>)>, StorageError> {
        use futures::TryStreamExt;

        if let Some(false) = self.gsi_cache_get_fresh(table_id) {
            return Ok(Vec::new());
        }

        let indexes_coll = self.catalog_db.collection::<Document>("indexes");
        let mut cursor = indexes_coll
            .find(doc! { "_id.table_id": table_id })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut out = Vec::new();
        while let Some(idx_doc) = cursor
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            let index_name = match idx_doc
                .get_document("_id")
                .and_then(|d| d.get_str("index_name"))
            {
                Ok(n) => n.to_string(),
                Err(_) => continue,
            };
            let key_schema: Vec<KeySchemaElement> = match idx_doc.get("key_schema") {
                Some(ks) => bson::from_bson(ks.clone()).unwrap_or_default(),
                None => continue,
            };
            out.push((index_name, key_schema));
        }
        Ok(out)
    }

    /// Reject an item whose secondary-index key attributes have the
    /// wrong scalar type or are empty. Called by put/update before
    /// the transaction is opened — matches postgres semantics of
    /// surfacing this as a top-level ValidationException rather than
    /// letting sync_indexes silently drop the malformed index doc
    /// (`data/mod.rs::index_document` skips typed sk fields on type
    /// mismatch, leaving the row un-locatable for subsequent
    /// deletes). RFC-0003 §2.3.
    async fn validate_index_keys_for_item(
        &self,
        key_info: &TableKeyInfo,
        item: &Item,
    ) -> Result<(), StorageError> {
        let idx_pairs = self.fetch_index_key_schemas(&key_info.table_id).await?;
        if idx_pairs.is_empty() {
            return Ok(());
        }
        let refs: Vec<extenddb_core::validation::IndexKeyRef<'_>> = idx_pairs
            .iter()
            .map(|(name, ks)| extenddb_core::validation::IndexKeyRef {
                index_name: name.as_str(),
                key_schema: ks.as_slice(),
            })
            .collect();
        extenddb_core::validation::validate_index_keys(item, &refs, &key_info.attribute_definitions)
            .map_err(|e| StorageError::Validation(e.to_string()))
    }

    async fn sync_indexes_in_session(
        &self,
        key_info: &TableKeyInfo,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
        session: &mut mongodb::ClientSession,
    ) -> Result<(), StorageError> {
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
                // Propagate the error rather than swallowing it — RFC-0003
                // §2.2 requires deleting the old entry when a write changes
                // or removes a GSI key attribute, and RFC-0003 §9.1 forbids
                // silent side-effect drops. A transient error here would
                // leave the stale index row live under the old GSI-key
                // value even though the base item no longer has it, and
                // subsequent queries would return the stale projection
                // forever.
                idx_coll
                    .delete_one(old_filter)
                    .session(&mut *session)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
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

        // Outcome of one attempt at running the whole idempotency check
        // + op fan-out + commit. `Retry` means MongoDB aborted the txn
        // as a transient conflict; the caller should re-run from the top.
        enum AttemptOutcome {
            Committed,
            CanceledReasons(Vec<CancellationReason>),
            Retry,
        }

        // Rehydrate the `IdempotencyKey` per attempt from owned strings.
        // The input struct holds `&str`s, so it cannot be moved across
        // loop iterations. This keeps the retry loop lifetime-clean
        // without asking upstream to change the trait signature.
        let idem_owned = idempotency.map(|k| {
            (
                k.account_id.to_owned(),
                k.token.to_owned(),
                k.fingerprint.to_owned(),
            )
        });

        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            let idempotency = idem_owned.as_ref().map(|(a, t, f)| IdempotencyKey {
                account_id: a.as_str(),
                token: t.as_str(),
                fingerprint: f.as_str(),
            });
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let outcome: Result<AttemptOutcome, StorageError> = async {
                // Check idempotency token, scoped to the caller's account
                // so that identical tokens from different accounts never
                // collide.
                if let Some(key) = idempotency {
                    let idem_coll = self.data_db.collection::<Document>("idempotency_tokens");
                    let existing = match idem_coll
                        .find_one(doc! { "account_id": key.account_id, "token": key.token })
                        .session(&mut session)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) if is_transient_write_conflict(&e) => {
                            return Ok(AttemptOutcome::Retry);
                        }
                        Err(e) => return Err(StorageError::Internal(e.to_string())),
                    };

                    // Filter out rows older than the DDB spec's 10-minute
                    // dedup window. Even with the TTL index set to 540s,
                    // MongoDB's TTL monitor runs on a ~60s cadence so a
                    // just-expired row can linger briefly. Treating a stale
                    // row as "not present" makes the read strictly correct
                    // regardless of monitor timing. See D-m4.
                    let existing = existing.filter(|doc| {
                        doc.get_datetime("created_at").is_ok_and(|dt| {
                            let age_ms = mongodb::bson::DateTime::now()
                                .timestamp_millis()
                                .saturating_sub(dt.timestamp_millis());
                            age_ms < 600_000
                        })
                    });

                    if let Some(existing_doc) = existing {
                        let stored_fp = existing_doc.get_str("fingerprint").unwrap_or_default();
                        return Err(if stored_fp == key.fingerprint {
                            StorageError::IdempotentReplay
                        } else {
                            StorageError::IdempotentMismatch
                        });
                    }

                    // Store the token. A unique index on (account_id, token)
                    // catches the case where a concurrent request under
                    // snapshot isolation didn't see our pre-check but raced
                    // us to the insert. On E11000, resolve the winner by
                    // re-reading outside the session — same replay/mismatch
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
                        if is_duplicate_key(&e) {
                            let winner = idem_coll
                                .find_one(doc! {
                                    "account_id": key.account_id,
                                    "token": key.token,
                                })
                                .await
                                .map_err(|e| StorageError::Internal(e.to_string()))?;
                            // Same 10-min age filter as the pre-check —
                            // don't let a barely-expired row masquerade
                            // as a live token. D-m4.
                            let winner = winner.filter(|d| {
                                d.get_datetime("created_at").is_ok_and(|dt| {
                                    let age_ms = mongodb::bson::DateTime::now()
                                        .timestamp_millis()
                                        .saturating_sub(dt.timestamp_millis());
                                    age_ms < 600_000
                                })
                            });
                            // If the winner aged out between our insert
                            // and this follow-up read, the row will be
                            // TTL'd shortly and the request is not a
                            // real dup — retry so the next attempt
                            // inserts fresh.
                            if winner.is_none() {
                                return Ok(AttemptOutcome::Retry);
                            }
                            return Err(
                                match winner.as_ref().and_then(|d| d.get_str("fingerprint").ok()) {
                                    Some(fp) if fp == key.fingerprint => {
                                        StorageError::IdempotentReplay
                                    }
                                    _ => StorageError::IdempotentMismatch,
                                },
                            );
                        }
                        if is_transient_write_conflict(&e) {
                            return Ok(AttemptOutcome::Retry);
                        }
                        return Err(StorageError::Internal(e.to_string()));
                    }
                }

                let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
                let mut any_failed = false;

                for op in ops {
                    match self
                        .execute_transact_write_op_in_session(op, &mut session)
                        .await
                    {
                        Ok(()) => reasons.push(CancellationReason::none()),
                        Err(TransactOpError::Cancel(r)) => {
                            any_failed = true;
                            reasons.push(r);
                        }
                        Err(TransactOpError::Transient) => {
                            return Ok(AttemptOutcome::Retry);
                        }
                        Err(TransactOpError::Storage(e)) => return Err(e),
                    }
                }

                if any_failed {
                    return Ok(AttemptOutcome::CanceledReasons(reasons));
                }
                Ok(AttemptOutcome::Committed)
            }
            .await;

            match outcome {
                Ok(AttemptOutcome::Committed) => match session.commit_transaction().await {
                    Ok(()) => return Ok(()),
                    Err(e) if is_transient_write_conflict(&e) => {
                        backoff_sleep(attempt).await;
                        continue;
                    }
                    Err(e) => return Err(StorageError::Internal(e.to_string())),
                },
                Ok(AttemptOutcome::CanceledReasons(reasons)) => {
                    let _ = session.abort_transaction().await;
                    return Err(StorageError::TransactionCanceled(reasons));
                }
                Ok(AttemptOutcome::Retry) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                    continue;
                }
                Err(e) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }

        // Exhausted retries under sustained contention. Surface as a
        // canceled transaction with a synthetic per-op TransactionConflict
        // reason so wire consumers see the DDB-canonical error string
        // instead of a bare HTTP 500. The engine maps StorageError::
        // TransactionCanceled to TransactionCanceledException; the
        // reason codes are echoed back in the message.
        let reasons = ops
            .iter()
            .map(|_| CancellationReason {
                code: "TransactionConflict".to_owned(),
                message: Some("Transaction is ongoing for the item".to_owned()),
                item: None,
            })
            .collect();
        Err(StorageError::TransactionCanceled(reasons))
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

                // Index-key type/empty faults inside a transaction
                // surface as per-item cancellation reasons (matches
                // postgres data/transactions.rs). D-M10.
                let idx_pairs = self
                    .fetch_index_key_schemas(&key_info.table_id)
                    .await
                    .map_err(TransactOpError::Storage)?;
                if !idx_pairs.is_empty() {
                    let idx_refs: Vec<extenddb_core::validation::IndexKeyRef<'_>> = idx_pairs
                        .iter()
                        .map(|(n, ks)| extenddb_core::validation::IndexKeyRef {
                            index_name: n.as_str(),
                            key_schema: ks.as_slice(),
                        })
                        .collect();
                    extenddb_core::validation::validate_index_keys(
                        item,
                        &idx_refs,
                        &key_info.attribute_definitions,
                    )
                    .map_err(|e| {
                        TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                    })?;
                }

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
                    .map_err(TransactOpError::from)?;

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
                    .map_err(TransactOpError::from)?;

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
                    .map_err(TransactOpError::from)?;

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
                    .map_err(TransactOpError::from)?;

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
                    .map_err(TransactOpError::from)?;

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

                expression::apply_update_validated(
                    actions,
                    &mut item,
                    maps,
                    &key_info.vector_indexes,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                // Validate index-key types/emptiness on the post-update
                // item; a violation here surfaces as a per-item
                // cancellation reason. D-M10, RFC-0003 §2.3.
                let idx_pairs = self
                    .fetch_index_key_schemas(&key_info.table_id)
                    .await
                    .map_err(TransactOpError::Storage)?;
                if !idx_pairs.is_empty() {
                    let idx_refs: Vec<extenddb_core::validation::IndexKeyRef<'_>> = idx_pairs
                        .iter()
                        .map(|(n, ks)| extenddb_core::validation::IndexKeyRef {
                            index_name: n.as_str(),
                            key_schema: ks.as_slice(),
                        })
                        .collect();
                    extenddb_core::validation::validate_index_keys(
                        &item,
                        &idx_refs,
                        &key_info.attribute_definitions,
                    )
                    .map_err(|e| {
                        TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                    })?;
                }

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
                    .map_err(TransactOpError::from)?;

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
                    .map_err(TransactOpError::from)?;

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
                    expression::apply_update_validated(
                        actions,
                        &mut new_item,
                        maps,
                        &key_info.vector_indexes,
                        &key_info.attribute_definitions,
                    )
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
        expression::apply_update_validated(
            actions,
            &mut new_item,
            maps,
            &key_info.vector_indexes,
            &key_info.attribute_definitions,
        )
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

    // ── GSI Backfill ──────────────────────────────────────────────
    //
    // Called by the gsi_backfill_worker in ttl_worker.rs. Reads one
    // batch of base-table items past the given cursor and upserts
    // matching index rows. Returns the new cursor and whether more
    // items remain to scan. The worker persists the cursor between
    // batches so a mid-backfill server restart resumes from where it
    // left off — see the CREATING → ACTIVE state machine in
    // update_table_impl / spawn_workers.

    pub(crate) async fn backfill_gsi_batch(
        &self,
        context: &GsiBackfillContext<'_>,
        cursor: Option<&bson::Bson>,
        batch_size: i64,
    ) -> Result<GsiBackfillProgress, StorageError> {
        use futures::TryStreamExt;

        let base_coll_name = data_collection_name(&context.key_info.table_id);
        let base_coll = self.data_db.collection::<Document>(&base_coll_name);
        let idx_coll_name = data_collection_name(context.index_id);
        let idx_coll = self.data_db.collection::<Document>(&idx_coll_name);

        let mut filter = Document::new();
        if let Some(c) = cursor {
            filter.insert("_id", doc! { "$gt": c.clone() });
        }

        let opts = mongodb::options::FindOptions::builder()
            .sort(doc! { "_id": 1 })
            .limit(batch_size)
            .build();

        // The read is intentionally outside the write transactions. The
        // cursor batch is only a bounded work list; each live item gets its
        // own short transaction below, so a backfill cannot hold write locks
        // across hundreds of base documents.
        let mut base_cursor = base_coll
            .find(filter)
            .with_options(opts)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut docs = Vec::new();
        while let Some(result) = base_cursor
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            docs.push(result);
        }

        let scanned = docs.len();
        let last_id = docs.last().and_then(|d| d.get("_id").cloned());

        // The test gate is outside any transaction. It lets an API test commit
        // a DeleteItem after this batch has been read but before the per-item
        // claim transaction begins, without consuming transaction lifetime.
        if context.mode == GsiBackfillMode::Live {
            self.wait_for_gsi_backfill_test_gate().await?;
        }

        for doc in &docs {
            let item = document_to_item(doc)?;
            if !item_has_index_keys(&item, context.idx_key_schema) {
                continue;
            }
            self.backfill_gsi_item(&base_coll, &idx_coll, context, doc, &item)
                .await?;
        }

        let progress = GsiBackfillProgress {
            scanned,
            last_id,
            done: (scanned as i64) < batch_size,
        };

        // A short-read (fewer docs than the batch size) means we've
        // reached the end of the base collection. Upstream flips the
        // index to ACTIVE when that happens.
        Ok(progress)
    }

    /// Backfill one item. Live backfills claim the exact base snapshot and
    /// write its index row in one short transaction. Restore backfills skip
    /// the claim because the target table remains unavailable until the copy
    /// and all index rows are complete.
    async fn backfill_gsi_item(
        &self,
        base_coll: &mongodb::Collection<Document>,
        idx_coll: &mongodb::Collection<Document>,
        context: &GsiBackfillContext<'_>,
        doc: &Document,
        item: &Item,
    ) -> Result<(), StorageError> {
        let base_id = doc
            .get("_id")
            .cloned()
            .ok_or_else(|| StorageError::Internal("base document missing _id".to_owned()))?;
        let item_data = doc
            .get("item_data")
            .cloned()
            .ok_or_else(|| StorageError::Internal("base document missing item_data".to_owned()))?;

        let projected = project_item(
            item,
            context.idx_key_schema,
            &context.key_info.key_schema,
            context.projection,
        );
        let idx_doc = index_document(
            &projected,
            context.idx_key_schema,
            &context.key_info.key_schema,
            &context.key_info.attribute_definitions,
        )?;
        let index_filter = index_entry_filter(
            &projected,
            context.idx_key_schema,
            &context.key_info.key_schema,
            &context.key_info.attribute_definitions,
        )?;
        let replace_opts = mongodb::options::ReplaceOptions::builder()
            .upsert(true)
            .build();

        if context.mode == GsiBackfillMode::Restore {
            idx_coll
                .replace_one(index_filter, idx_doc)
                .with_options(replace_opts)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return Ok(());
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

        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let guard_token = uuid::Uuid::new_v4().to_string();
            let attempt_result: Result<bool, TxErr> = async {
                let guard_result = base_coll
                    .update_one(
                        doc! { "_id": &base_id, "item_data": item_data.clone() },
                        doc! { "$set": { "_backfill_guard": &guard_token } },
                    )
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?;
                if guard_result.matched_count == 0 {
                    return Ok(false);
                }

                idx_coll
                    .replace_one(index_filter.clone(), idx_doc.clone())
                    .with_options(replace_opts.clone())
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?;

                let cleanup = base_coll
                    .update_one(
                        doc! { "_id": &base_id, "_backfill_guard": &guard_token },
                        doc! { "$unset": { "_backfill_guard": "" } },
                    )
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?;
                if cleanup.matched_count != 1 {
                    return Err(TxErr::Fatal(StorageError::Internal(
                        "GSI backfill guard cleanup matched no base document".to_owned(),
                    )));
                }

                Ok(true)
            }
            .await;

            match attempt_result {
                Ok(true) => match session.commit_transaction().await {
                    Ok(()) => return Ok(()),
                    Err(e) if is_transient_write_conflict(&e) => {
                        backoff_sleep(attempt).await;
                    }
                    Err(e) => return Err(StorageError::Internal(e.to_string())),
                },
                Ok(false) => {
                    let _ = session.abort_transaction().await;
                    return Ok(());
                }
                Err(TxErr::Transient) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                }
                Err(TxErr::Fatal(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }

        Err(StorageError::TransactionConflict(
            "GSI backfill: too many concurrent write conflicts, giving up".to_owned(),
        ))
    }

    /// Pause once when the test gate is armed, after a backfill batch has been
    /// read but before it claims or writes any base/index rows. The gate is
    /// controlled through the authenticated management settings API and is
    /// inert unless a test explicitly sets it to `armed`.
    async fn wait_for_gsi_backfill_test_gate(&self) -> Result<(), StorageError> {
        #[cfg(not(feature = "test-hooks"))]
        {
            Ok(())
        }

        #[cfg(feature = "test-hooks")]
        {
            use std::time::{Duration, Instant};

            let settings = self.catalog_db.collection::<Document>("settings");
            let key = extenddb_core::settings_keys::GSI_BACKFILL_TEST_GATE;
            let armed = settings
                .find_one(doc! { "_id": key, "value": "armed" })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            if armed.is_none() {
                return Ok(());
            }

            let claimed = settings
                .update_one(
                    doc! { "_id": key, "value": "armed" },
                    doc! { "$set": { "value": "paused" } },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            if claimed.matched_count == 0 {
                return Ok(());
            }

            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let state = settings
                    .find_one(doc! { "_id": key })
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                    .and_then(|d| d.get_str("value").ok().map(str::to_owned));
                if state.as_deref() == Some("release") {
                    settings
                        .update_one(
                            doc! { "_id": key, "value": "release" },
                            doc! { "$set": { "value": "idle" } },
                        )
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    let _ = settings
                        .update_one(
                            doc! { "_id": key, "value": "paused" },
                            doc! { "$set": { "value": "idle" } },
                        )
                        .await;
                    return Err(StorageError::Internal(
                        "timed out waiting for GSI backfill test gate release".to_owned(),
                    ));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Progress from one `backfill_gsi_batch` invocation.
pub(crate) struct GsiBackfillProgress {
    /// Number of base-collection documents read in this batch (before
    /// filtering out those missing index-key attributes).
    pub scanned: usize,
    /// The `_id` of the last document scanned; the next batch resumes
    /// with `_id > last_id`. `None` when the batch was empty.
    pub last_id: Option<bson::Bson>,
    /// Whether the base collection has been fully scanned.
    pub done: bool,
}

// ── Contention / retry helpers ──────────────────────────────────────────

/// Maximum number of times to retry a write that MongoDB aborted as a
/// transient conflict. Small enough that we don't lock a partition on
/// sustained hot-key contention; large enough to absorb ordinary
/// snapshot-isolation aborts. Matches the OCC retry ceiling elsewhere
/// in this file.
const TRANSIENT_RETRY_ATTEMPTS: u32 = 50;

/// Return type of `try_build_native_update`. Distinguishes an
/// operator-document update (`{$set, $unset, $inc}`) from an
/// aggregation-pipeline update (needed for numeric ADD, which
/// converts a string-stored `.N` value to Decimal128, applies the
/// delta, and converts back).
///
/// `Pipeline` carries an optional `type_guard` filter — for numeric
/// ADD we require the target attribute to be absent or already an
/// `.N` so we don't clobber a string with a number. When the guard
/// rejects the match, `find_one_and_update` returns `None`, and the
/// caller falls back to the slow (session-scoped) path which reads
/// the pre-image and surfaces a proper `ValidationException`.
enum NativeUpdate {
    Doc(Document),
    Pipeline {
        type_guard: Option<Document>,
        pipeline: Vec<Document>,
    },
}

/// Error signal used inside per-attempt transaction bodies. Lets the
/// body use `?` for control flow while distinguishing "retry this
/// whole transaction" from "return this error to the caller."
enum TxErr {
    Transient,
    Fatal(StorageError),
}

impl From<mongodb::error::Error> for TxErr {
    fn from(e: mongodb::error::Error) -> Self {
        if is_transient_write_conflict(&e) {
            TxErr::Transient
        } else {
            TxErr::Fatal(StorageError::Internal(e.to_string()))
        }
    }
}

impl From<StorageError> for TxErr {
    fn from(e: StorageError) -> Self {
        TxErr::Fatal(e)
    }
}

/// Detect the family of errors MongoDB uses to signal "your write lost
/// to another concurrent writer under snapshot isolation; retry."
///
/// The transient-transaction label is set on any error that a
/// `withTransaction` client would automatically retry. In addition to
/// abstract labels the raw `WriteConflict` (code 112) still shows up
/// when a same-document collision surfaces on the write itself rather
/// than at commit — check that too. RFC-0003 §4.1 / §4.3.
fn is_transient_write_conflict(e: &mongodb::error::Error) -> bool {
    if e.contains_label(mongodb::error::TRANSIENT_TRANSACTION_ERROR)
        || e.contains_label(mongodb::error::UNKNOWN_TRANSACTION_COMMIT_RESULT)
    {
        return true;
    }
    matches!(*e.kind, mongodb::error::ErrorKind::Command(ref c) if c.code == 112)
        || matches!(
            *e.kind,
            mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                mongodb::error::WriteError { code: 112, .. }
            ))
        )
}

/// Detect a duplicate-key error (E11000, code 11000). Used at
/// conditional-insert sites — a duplicate is the manifestation of a
/// conditional-put race, so it must be surfaced as
/// `ConditionalCheckFailedException` rather than a 500.
fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    matches!(
        *e.kind,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
            mongodb::error::WriteError { code: 11000, .. }
        ))
    )
}

/// Exponential-backoff sleep with random jitter. Used inside the OCC
/// / WriteConflict retry loops so competing writers don't lock-step
/// re-retry into the same conflict window.
async fn backoff_sleep(attempt: u32) {
    let base_us = 50u64.saturating_mul(1u64 << attempt.min(8));
    let jitter = rand::random_range(0..=base_us);
    tokio::time::sleep(std::time::Duration::from_micros(jitter)).await;
}

// ── Transaction helper types ──────────────────────────────────────────

enum TransactOpError {
    Cancel(extenddb_core::types::CancellationReason),
    Storage(StorageError),
    /// MongoDB aborted the transaction as a transient conflict —
    /// the whole transact_write_items txn should be retried from
    /// the top.
    Transient,
}

impl From<mongodb::error::Error> for TransactOpError {
    fn from(e: mongodb::error::Error) -> Self {
        if is_transient_write_conflict(&e) {
            TransactOpError::Transient
        } else {
            TransactOpError::Storage(StorageError::Internal(e.to_string()))
        }
    }
}

impl From<StorageError> for TransactOpError {
    fn from(e: StorageError) -> Self {
        TransactOpError::Storage(e)
    }
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
                    // `sk BEGINS_WITH P` matches every X where P is a
                    // prefix of X. Emit that as `sk >= P AND sk < P'`,
                    // where P' is the least string strictly greater
                    // than any P-starting string.
                    //
                    // `next_string_prefix` finds P' by incrementing
                    // the last non-`char::MAX` code point. If P is
                    // entirely `char::MAX`, no such P' exists — return
                    // just the lower-bound filter and let mongo match
                    // every string ≥ P (which is what DDB does).
                    match next_string_prefix(p) {
                        Some(upper) => Ok(doc! {
                            sk_field: { "$gte": p.as_str(), "$lt": upper }
                        }),
                        None => Ok(doc! { sk_field: { "$gte": p.as_str() } }),
                    }
                }
                AttributeValue::B(ref b) => {
                    // Binary sort keys are stored as lowercase hex strings
                    // (D-M5), so `sk BEGINS_WITH B` is a string-prefix range
                    // over the hex encoding, exactly like the S case above:
                    // `sk_b >= hex(B) AND sk_b < next_string_prefix(hex(B))`.
                    //
                    // The exclusive upper bound must be the next prefix in
                    // hex-STRING space (increment the last hex character), not
                    // hex(increment_bytes(B)). Incrementing the raw bytes then
                    // re-encoding widens the range and admits unrelated keys —
                    // e.g. begins_with(0x2F,0xFF) -> ["2fff", hex(0x30,0x00) =
                    // "3000"), which wrongly matches the stored key 0x30
                    // ("30"). next_string_prefix("2fff") = "2ffg" excludes it.
                    // When the prefix is empty, there is no upper bound and we
                    // match every key >= "" (all of them), matching DDB.
                    let lo = binary_sk_to_hex(b);
                    match next_string_prefix(&lo) {
                        Some(upper) => Ok(doc! { sk_field: { "$gte": lo, "$lt": upper } }),
                        None => Ok(doc! { sk_field: { "$gte": lo } }),
                    }
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
/// lexicographically (matching DynamoDB) and binary bytewise.
///
/// Numbers are compared via `f64`. f64→nearest rounding is monotonic, so this can
/// never make a valid `low <= high` range look inverted (no false ValidationException):
/// if `low <= high` then `low as f64 <= high as f64`. The only imprecision is the
/// reverse — a genuinely inverted range whose bounds differ only beyond f64's ~15–17
/// significant digits (DynamoDB numbers carry up to 38) rounds to equal and slips
/// past this guard. In that pathological case the `$gte low > $lte high` query simply
/// returns an empty result instead of the ValidationException DynamoDB would raise.
/// This bounded divergence is documented in `docs/differences-from-dynamodb.md`.
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
        // Binary sort keys are stored as hex-encoded strings; see
        // `binary_sk_to_hex` and the D-M5 rationale. Query/BETWEEN
        // filters must project to the same encoding.
        (ScalarAttributeType::B, AttributeValue::B(b)) => {
            Ok(bson::Bson::String(binary_sk_to_hex(b)))
        }
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

/// Compute the least string strictly greater than every string
/// beginning with `s`, used as the exclusive upper bound for
/// `sk BEGINS_WITH s`.
///
/// Strategy: find the rightmost char in `s` that isn't `char::MAX`,
/// increment it, and truncate everything to its right. If every char
/// is `char::MAX` (an unlikely-but-real edge case), no upper bound
/// exists — return `None` so the caller can drop the `$lt` clause.
///
/// The previous implementation appended `char::MAX` to `s` and used
/// `$lt`, which excluded any stored string equal to `s + char::MAX`
/// (or extending past it) — those still begin with `s` and DDB
/// matches them. D-m12.
fn next_string_prefix(s: &str) -> Option<String> {
    let chars: Vec<char> = s.chars().collect();
    // Walk from the right, find the first char we can bump.
    for i in (0..chars.len()).rev() {
        if chars[i] < char::MAX {
            let mut out = String::with_capacity(s.len());
            for c in &chars[..i] {
                out.push(*c);
            }
            // char::from_u32 handles the surrogate gap by skipping
            // to the next valid scalar. u32 → char via char::from_u32
            // returns None on the surrogate range D800..=DFFF, so
            // walk past it.
            let mut next = u32::from(chars[i]) + 1;
            let bumped = loop {
                if let Some(c) = char::from_u32(next) {
                    break c;
                }
                next += 1;
            };
            out.push(bumped);
            return Some(out);
        }
    }
    None
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
    fn next_string_prefix_ascii() {
        // Basic ASCII: "abc" -> "abd" as the exclusive upper bound.
        assert_eq!(next_string_prefix("abc").as_deref(), Some("abd"));

        // Trailing char::MAX skips back to a bumpable char.
        // E.g. "abZ\u{10FFFF}" -> "ab["
        let s: String = ['a', 'b', 'Z', char::MAX].iter().collect();
        let expected: String = ['a', 'b', '['].iter().collect();
        assert_eq!(next_string_prefix(&s).as_deref(), Some(expected.as_str()));

        // All-char::MAX -> None (no bound; caller drops $lt clause).
        let s: String = std::iter::repeat_n(char::MAX, 3).collect();
        assert!(next_string_prefix(&s).is_none());

        // Empty string is also unbounded (no chars to bump).
        assert!(next_string_prefix("").is_none());
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

    fn binary_begins_with_bounds(prefix: Vec<u8>) -> (String, Option<String>) {
        let mut values = std::collections::HashMap::new();
        values.insert(":p".to_string(), AttributeValue::B(prefix));
        let maps = ExpressionMaps::new(std::collections::HashMap::new(), values);
        let cond = SortKeyCondition::BeginsWith {
            path: vec![PathElement::Attribute("sk".to_string())],
            prefix: Expr::Placeholder(":p".to_string()),
        };
        let doc = build_sk_filter(&cond, "sk_b", &maps).unwrap();
        let inner = doc.get_document("sk_b").unwrap();
        let lo = inner.get_str("$gte").unwrap().to_string();
        let hi = inner.get_str("$lt").ok().map(str::to_string);
        (lo, hi)
    }

    #[test]
    fn binary_begins_with_uses_hex_space_prefix() {
        // Upper bound is the next prefix in hex-STRING space, not
        // hex(increment_bytes(prefix)).

        // begins_with(0x2F,0xFF): lo="2fff", hi must be "2ffg" (not "3000").
        // The old code produced "3000", which wrongly admitted stored key
        // 0x30 ("30") since "2fff" <= "30" < "3000". With "2ffg", "30" is
        // excluded because "30" > "2ffg".
        let (lo, hi) = binary_begins_with_bounds(vec![0x2f, 0xff]);
        assert_eq!(lo, "2fff");
        assert_eq!(hi.as_deref(), Some("2ffg"));
        assert!("30" >= hi.as_deref().unwrap(), "0x30 must be excluded");

        // begins_with(0xFF): lo="ff", hi must be "fg". The old code produced
        // "00" (0xFF+1 wrapped then prepended 0x01 -> "01ff"? either way an
        // empty/incorrect range), dropping every match.
        let (lo, hi) = binary_begins_with_bounds(vec![0xff]);
        assert_eq!(lo, "ff");
        assert_eq!(hi.as_deref(), Some("fg"));
        assert!("ffab" < hi.as_deref().unwrap(), "0xFFAB must be included");
    }
}
