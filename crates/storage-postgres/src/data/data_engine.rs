// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Thin `DataEngine` trait implementation that delegates to `impl PostgresEngine`
//! methods in sibling modules.

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::pk_to_text;
use extenddb_storage::{
    DataEngine, IdempotencyKey, ReadOptions, StreamCapture, TransactGetOp, TransactWriteOp,
};
use futures::future::BoxFuture;

use crate::PostgresEngine;

impl DataEngine for PostgresEngine {
    /// Declares vector support only where the server can actually serve it.
    ///
    /// `Some(self)` compiles only because `PostgresEngine` implements
    /// `VectorSearchEngine`, so this cannot claim a capability that does not
    /// exist in the build. The runtime half is the probe: vector storage needs
    /// pgvector, which is a property of the PostgreSQL server rather than of this
    /// binary, so the same build serves vector indexes against a server that has
    /// the extension and refuses them, byte-identically to before, against one
    /// that does not.
    ///
    /// The answer is the startup probe's, cached, so a request pays nothing for
    /// it. Installing pgvector on a running server therefore needs a restart to
    /// be noticed, which the admin guide says.
    fn as_vector_search(&self) -> Option<&dyn extenddb_storage::VectorSearchEngine> {
        self.vector_capable.then_some(self)
    }

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
            .map_err(|e| table_gone(e, &key_info.table_name))
        })
    }

    /// `_opts.consistent_read` is accepted and not acted on. Item reads and
    /// writes share `data_pool`, so every read is served by the copy that took
    /// the write. Routing `false` to a streaming replica of the data database
    /// needs a second data pool and a setting for it; `true` stays on
    /// `data_pool`.
    fn get_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        _opts: ReadOptions,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        Box::pin(async move {
            self.get_item_impl(&key_info, &key)
                .await
                .map_err(|e| table_gone(e, &key_info.table_name))
        })
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
            .map_err(|e| table_gone(e, &key_info.table_name))
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
    ) -> BoxFuture<'_, Result<(Option<Item>, Option<Item>), StorageError>> {
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
            .map_err(|e| table_gone(e, &key_info.table_name))
        })
    }

    /// `_opts`: see `get_item`.
    fn query(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
        _opts: ReadOptions,
    ) -> BoxFuture<'_, Result<(Vec<Item>, Option<Item>), StorageError>> {
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
            .map_err(|e| read_target_gone(e, &key_info.table_name, index_name.as_deref()))
        })
    }

    /// `_opts`: see `get_item`.
    fn scan(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
        _opts: ReadOptions,
    ) -> BoxFuture<'_, Result<(Vec<Item>, Option<Item>), StorageError>> {
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
            .map_err(|e| read_target_gone(e, &key_info.table_name, index_name.as_deref()))
        })
    }

    fn scan_key_in_segment(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        segment: i64,
        total_segments: i64,
        _index_name: Option<&str>,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        // Same hashtext-based assignment the scan predicate uses
        // ((hashtext(pk)::bigint & 2147483647) % total = seg), evaluated for
        // the candidate key. `key_info` is scan-shaped, so `key_schema[0]` is
        // the pk of whichever table (data or index) the scan targets, matching
        // the `pk` column the predicate hashes. Bigint bitmask instead of
        // abs() per CB-20 / SP-SCN-002 (hashtext() == i32::MIN).
        let pk_name = key_info.key_schema[0].attribute_name.clone();
        let key = key.clone();
        Box::pin(async move {
            let Some(pk_value) = key.get(&pk_name) else {
                // Schema validation upstream already rejects this shape.
                return Ok(false);
            };
            let pk_text = pk_to_text(pk_value)?;
            sqlx::query_scalar("SELECT (hashtext($1)::bigint & 2147483647) % $2 = $3")
                .bind(pk_text.as_ref())
                .bind(total_segments)
                .bind(segment)
                .fetch_one(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))
        })
    }

    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        // Clone ops to owned data to satisfy lifetime requirements
        let owned_ops: Vec<_> = ops
            .iter()
            .map(|op| (op.key_info.clone(), op.key.clone()))
            .collect();
        Box::pin(async move {
            // Reconstruct borrowed ops from owned data
            let borrowed_ops: Vec<TransactGetOp> = owned_ops
                .iter()
                .map(|(key_info, key)| TransactGetOp { key_info, key })
                .collect();
            let tables: Vec<(&str, &str)> = owned_ops
                .iter()
                .map(|(k, _)| (k.table_id.as_str(), k.table_name.as_str()))
                .collect();
            self.transact_get_items_impl(&borrowed_ops)
                .await
                .map_err(|e| table_gone_among(e, &tables))
        })
    }

    fn transact_write_items(
        &self,
        ops: &[TransactWriteOp<'_>],
        idempotency: Option<IdempotencyKey<'_>>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        // Clone ops to owned data - unavoidable due to lifetime constraints
        let owned_ops: Vec<_> = ops
            .iter()
            .map(|op| match op {
                TransactWriteOp::Put {
                    key_info,
                    item,
                    condition,
                    maps,
                    return_values_on_ccf,
                    stream,
                } => (
                    0u8,
                    (*key_info).clone(),
                    (*item).clone(),
                    None,
                    Vec::new(),
                    condition.cloned(),
                    (*maps).clone(),
                    *return_values_on_ccf,
                    stream.clone(),
                ),
                TransactWriteOp::Delete {
                    key_info,
                    key,
                    condition,
                    maps,
                    return_values_on_ccf,
                    stream,
                } => (
                    1u8,
                    (*key_info).clone(),
                    (*key).clone(),
                    None,
                    Vec::new(),
                    condition.cloned(),
                    (*maps).clone(),
                    *return_values_on_ccf,
                    stream.clone(),
                ),
                TransactWriteOp::Update {
                    key_info,
                    key,
                    actions,
                    condition,
                    maps,
                    return_values_on_ccf,
                    stream,
                } => (
                    2u8,
                    (*key_info).clone(),
                    (*key).clone(),
                    None,
                    actions.to_vec(),
                    condition.cloned(),
                    (*maps).clone(),
                    *return_values_on_ccf,
                    stream.clone(),
                ),
                TransactWriteOp::ConditionCheck {
                    key_info,
                    key,
                    condition,
                    maps,
                    return_values_on_ccf,
                } => (
                    3u8,
                    (*key_info).clone(),
                    (*key).clone(),
                    Some((*condition).clone()),
                    Vec::new(),
                    None,
                    (*maps).clone(),
                    *return_values_on_ccf,
                    None,
                ),
            })
            .collect();
        let idempotency = idempotency.map(|k| {
            (
                k.account_id.to_string(),
                k.token.to_string(),
                k.fingerprint.to_string(),
            )
        });

        Box::pin(async move {
            // Reconstruct borrowed ops from owned data
            let borrowed_ops: Vec<TransactWriteOp> = owned_ops
                .iter()
                .map(
                    |(
                        tag,
                        key_info,
                        item_or_key,
                        cond_check,
                        actions,
                        condition,
                        maps,
                        rv,
                        stream,
                    )| {
                        match tag {
                            0 => TransactWriteOp::Put {
                                key_info,
                                item: item_or_key,
                                condition: condition.as_ref(),
                                maps,
                                return_values_on_ccf: *rv,
                                stream: stream.clone(),
                            },
                            1 => TransactWriteOp::Delete {
                                key_info,
                                key: item_or_key,
                                condition: condition.as_ref(),
                                maps,
                                return_values_on_ccf: *rv,
                                stream: stream.clone(),
                            },
                            2 => TransactWriteOp::Update {
                                key_info,
                                key: item_or_key,
                                actions,
                                condition: condition.as_ref(),
                                maps,
                                return_values_on_ccf: *rv,
                                stream: stream.clone(),
                            },
                            3 => TransactWriteOp::ConditionCheck {
                                key_info,
                                key: item_or_key,
                                condition: cond_check.as_ref().unwrap(),
                                maps,
                                return_values_on_ccf: *rv,
                            },
                            _ => unreachable!(),
                        }
                    },
                )
                .collect();
            let tables: Vec<(&str, &str)> = borrowed_ops
                .iter()
                .map(|op| match op {
                    TransactWriteOp::Put { key_info, .. }
                    | TransactWriteOp::Delete { key_info, .. }
                    | TransactWriteOp::Update { key_info, .. }
                    | TransactWriteOp::ConditionCheck { key_info, .. } => {
                        (key_info.table_id.as_str(), key_info.table_name.as_str())
                    }
                })
                .collect();
            self.transact_write_items_impl(
                &borrowed_ops,
                idempotency
                    .as_ref()
                    .map(|(account_id, token, fingerprint)| IdempotencyKey {
                        account_id,
                        token,
                        fingerprint,
                    }),
            )
            .await
            .map_err(|e| table_gone_among(e, &tables))
        })
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            self.cleanup_expired_idempotency_tokens_impl(max_age_seconds)
                .await
        })
    }
}

/// A data table that is gone mid-request is a table that is gone.
///
/// A request that resolved the table just before a DeleteTable committed reaches
/// the data database after the drop, and PostgreSQL answers 42P01. The service
/// answers ResourceNotFoundException for a table being deleted, which is what
/// `TableNotFound` becomes on the wire; passing the 42P01 through was a 500.
fn table_gone(e: StorageError, table_name: &str) -> StorageError {
    if data_table_missing(&e) {
        StorageError::TableNotFound(table_name.to_owned())
    } else {
        e
    }
}

/// [`table_gone`] for a transaction over several tables: the table named is the
/// one whose data table the error names, or the first one if the error names an
/// index table (dropped with its table) or is not specific.
fn table_gone_among(e: StorageError, tables: &[(&str, &str)]) -> StorageError {
    if !data_table_missing(&e) {
        return e;
    }
    let StorageError::Internal(msg) = &e else {
        return e;
    };
    let name = tables
        .iter()
        .find(|(id, _)| msg.contains(&format!("_ddb_{id}\"")))
        .or(tables.first())
        .map(|(_, name)| (*name).to_owned());
    match name {
        Some(name) => StorageError::TableNotFound(name),
        None => e,
    }
}

/// [`table_gone`] for a Query or Scan, which may name an index: a 42P01 there can
/// be the index's table, dropped by a GSI delete that committed after the index
/// was resolved, and the service answers that as an index it does not have.
fn read_target_gone(e: StorageError, table_name: &str, index_name: Option<&str>) -> StorageError {
    match index_name {
        Some(index) if data_table_missing(&e) => StorageError::IndexNotFound(index.to_owned()),
        _ => table_gone(e, table_name),
    }
}

/// Whether an error is PostgreSQL's undefined_table for one of this backend's
/// data tables. Many write paths render the error with `to_string()`, which keeps
/// the message and drops the SQLSTATE, so the message is matched as well: it
/// names the relation, and every data table's name starts `_ddb_`.
fn data_table_missing(e: &StorageError) -> bool {
    match e {
        StorageError::Internal(msg) => {
            crate::gsi_queue::is_undefined_table(e)
                || (msg.contains("relation \"_ddb_") && msg.contains("does not exist"))
        }
        _ => false,
    }
}

#[cfg(test)]
mod table_gone_tests {
    use super::*;

    #[test]
    fn a_missing_data_table_is_a_missing_table_with_or_without_the_sqlstate() {
        for msg in [
            "SQLSTATE 42P01: error returned from database: relation \"_ddb_abc\" does not exist",
            "error returned from database: relation \"_ddb_abc\" does not exist",
        ] {
            let e = table_gone(StorageError::Internal(msg.to_owned()), "t");
            assert!(
                matches!(e, StorageError::TableNotFound(ref n) if n == "t"),
                "{msg}"
            );
        }
    }

    #[test]
    fn other_errors_pass_through() {
        let e = table_gone(StorageError::Internal("deadlock detected".to_owned()), "t");
        assert!(matches!(e, StorageError::Internal(_)));
        let e = table_gone(
            StorageError::Internal("relation \"settings\" does not exist".to_owned()),
            "t",
        );
        assert!(matches!(e, StorageError::Internal(_)), "not a data table");
    }

    #[test]
    fn a_transaction_names_the_table_whose_data_table_is_gone() {
        let tables = [("id-a", "a"), ("id-b", "b")];
        let e = table_gone_among(
            StorageError::Internal("relation \"_ddb_id-b\" does not exist".to_owned()),
            &tables,
        );
        assert!(matches!(e, StorageError::TableNotFound(ref n) if n == "b"));
        let e = table_gone_among(
            StorageError::Internal("relation \"_ddb_some-index\" does not exist".to_owned()),
            &tables,
        );
        assert!(matches!(e, StorageError::TableNotFound(ref n) if n == "a"));
        let e = table_gone_among(StorageError::Internal("deadlock".to_owned()), &tables);
        assert!(matches!(e, StorageError::Internal(_)));
    }

    #[test]
    fn a_missing_index_table_on_a_read_of_an_index_is_a_missing_index() {
        let msg = "relation \"_ddb_idx\" does not exist".to_owned();
        let e = read_target_gone(StorageError::Internal(msg), "t", Some("gsi1"));
        assert!(matches!(e, StorageError::IndexNotFound(ref n) if n == "gsi1"));
    }
}
