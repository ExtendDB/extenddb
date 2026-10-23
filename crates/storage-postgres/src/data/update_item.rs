// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_item` implementation for the `PostgreSQL` backend.

use extenddb_core::expression::{self, Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_core::validation;
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_sk, pk_to_text, sk_column, sk_info};

use super::index::{enqueue_async_indexes, fetch_write_path_indexes, sync_indexes};
use super::query::check_condition;
use super::tx_helpers::write_stream_record_in_tx;
use super::{data_table_name, json_to_item};
use crate::PostgresEngine;

impl PostgresEngine {
    /// Implementation of `DataEngine::update_item`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn update_item_impl(
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
        let ddb_table = data_table_name(&key_info.table_id);

        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_value = key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        let pk_text = pk_to_text(pk_value)?;

        // UpdateItem always needs a transaction (read-modify-write)
        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Both index families in one catalog visit (D-4: sync + async split for the
        // secondary indexes).
        //
        // Vector indexes come from this fresh read rather than from the cached key
        // info, and the answer decides two things: whether this write needs a
        // transaction at all, and what maintenance runs inside it. A cached empty
        // set would send a write down the no-maintenance fast path and silently
        // leave an index missing a row.
        //
        // What this does and does not remove. The defect being designed out is the
        // cached membership gate, and that is gone: an index takes effect the moment
        // its catalog row commits. What remains is a window between this read and
        // the data transaction's commit, in which an index created concurrently is
        // missed. That window cannot be closed here, because the catalog and the
        // data tables are different databases and no transaction spans them. It is
        // also exactly the window the secondary indexes have, for the same reason
        // and with the same read: parity with a GSI is the bar, and the backfill
        // that publishes a new index is what covers writes older than it.
        let (indexes, vector_metas) =
            fetch_write_path_indexes(&key_info.table_id, &self.pool).await?;
        // Read whenever anything can propagate, secondary or vector. Gating this on
        // the secondary set alone made a vector-only table ignore the configured
        // delay and apply its vector index inline, while a TransactWriteItems on the
        // same table read the delay unconditionally and enqueued: six write sites,
        // two answers, on a setting the differences doc says covers both index kinds.
        let sys_delay = if indexes.is_empty() && vector_metas.is_empty() {
            0
        } else {
            self.index_propagation_delay().await
        };

        // Sort-key binding, computed once: the key is immutable across attempts.
        let sk_parts = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            Some((parse_sk(sk_value, sk_type)?, sk_column(sk_type)))
        } else {
            None
        };

        // Read-modify-write, retried when a concurrent writer creates the row first.
        //
        // The locking read below serialises writers to an item that EXISTS. It cannot
        // lock a row that does not exist yet, so two writers can both decide to
        // insert. The loser's `ON CONFLICT DO NOTHING` affects no rows, and at that
        // moment the winner may still be uncommitted, so a plain re-read can see
        // nothing at all. Re-reading `FOR UPDATE` is what makes this terminate: it
        // blocks until the winner commits, returning its row, or aborts, returning
        // none so the insert can be retried.
        //
        // Returning ConditionalCheckFailedException here instead was wrong. DynamoDB
        // serialises writes to one item, and an UpdateItem carrying no condition is
        // an upsert, so every writer must succeed and each update expression applies
        // on top of whatever the previous writer committed. Measured against the
        // service: four writers setting four different attributes on one new key
        // yield an item holding all four, so the loser's expression must be re-applied
        // to the winner's item rather than overwriting it.
        //
        // A supplied condition is re-evaluated against the winner for the same
        // reason: after losing the race `attribute_exists` is now true, and failing
        // it unconditionally was wrong in the opposite direction.
        const MAX_CREATE_RACE_ATTEMPTS: u32 = 5;
        let mut attempt: u32 = 0;
        let (old_item, new_item, item, pre_mutation_item) = loop {
            attempt += 1;

            // Current row, locked when present. First pass: the initial read. Later
            // passes: the post-conflict re-read that waits on the race winner.
            let old_json: Option<serde_json::Value> = match &sk_parts {
                Some((sk, sk_col)) => {
                    let select_sql = format!(
                        "SELECT item_data FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2 FOR UPDATE"
                    );
                    let row: Option<(serde_json::Value,)> =
                        bind_sk_fetch_optional!(&select_sql, pk_text.as_ref(), sk, &mut *tx)?;
                    row.map(|(v,)| v)
                }
                None => {
                    let select_sql =
                        format!("SELECT item_data FROM {ddb_table} WHERE pk = $1 FOR UPDATE");
                    let row: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                        .bind(pk_text.as_ref())
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    row.map(|(v,)| v)
                }
            };

            // Build the working item: existing or new with key attributes only (upsert)
            let mut item = if let Some(json) = old_json.clone() {
                json_to_item(json)?
            } else {
                key.clone()
            };

            // Save pre-mutation item for index sync and stream capture.
            let pre_mutation_item =
                if (!indexes.is_empty() || stream.is_some()) && old_json.is_some() {
                    Some(item.clone())
                } else {
                    None
                };

            let old_item = if return_old && old_json.is_some() {
                Some(item.clone())
            } else {
                None
            };

            // Evaluate condition against the existing item (empty if non-existent).
            // DynamoDB treats a non-existent item as having no attributes at all.
            let empty = std::collections::BTreeMap::new();
            let condition_item = if old_json.is_some() { &item } else { &empty };
            match check_condition(condition, condition_item, maps) {
                Ok(()) => {}
                Err(StorageError::ConditionFailed(_)) => {
                    if old_json.is_some() {
                        return Err(StorageError::ConditionFailed(Some(item)));
                    }
                    return Err(StorageError::ConditionFailed(None));
                }
                Err(e) => return Err(e),
            }

            // Apply update actions and validate the resulting image against the
            // table's vector indexes: vector validity is a property of the stored
            // value, and expression-form checks cannot cover if_not_exists /
            // list_append / copy-from-attribute forms.
            expression::apply_update_validated(
                actions,
                &mut item,
                maps,
                &key_info.vector_indexes,
                &key_info.attribute_definitions,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;

            // Validate post-update item size (400 KB limit)
            validation::validate_item_size(&item, self.max_item_size_bytes)
                .map_err(|e| StorageError::Validation(e.to_string()))?;

            let new_item = if return_new { Some(item.clone()) } else { None };

            let item_json =
                serde_json::to_value(&item).map_err(|e| StorageError::Internal(e.to_string()))?;

            if old_json.is_some() {
                // Row existed and is locked by the read above, so update in place.
                match &sk_parts {
                    Some((sk, sk_col)) => {
                        let update_sql = format!(
                            "UPDATE {ddb_table} SET item_data = $3 WHERE pk = $1 AND {sk_col} = $2"
                        );
                        bind_sk_execute!(&update_sql, pk_text.as_ref(), sk, &item_json, &mut *tx)?;
                    }
                    None => {
                        let update_sql =
                            format!("UPDATE {ddb_table} SET item_data = $2 WHERE pk = $1");
                        sqlx::query(&update_sql)
                            .bind(pk_text.as_ref())
                            .bind(&item_json)
                            .execute(&mut *tx)
                            .await
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                    }
                }
                break (old_item, new_item, item, pre_mutation_item);
            }

            // Row absent, so insert atomically; someone may beat us to it.
            let inserted = match &sk_parts {
                Some((sk, sk_col)) => {
                    let insert_sql = format!(
                        "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES ($1, $2, $3) \
                         ON CONFLICT (pk, {sk_col}) DO NOTHING"
                    );
                    bind_sk_execute!(&insert_sql, pk_text.as_ref(), sk, &item_json, &mut *tx)?
                        .rows_affected()
                        == 1
                }
                None => {
                    let insert_sql = format!(
                        "INSERT INTO {ddb_table} (pk, item_data) VALUES ($1, $2) \
                         ON CONFLICT (pk) DO NOTHING"
                    );
                    sqlx::query(&insert_sql)
                        .bind(pk_text.as_ref())
                        .bind(&item_json)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?
                        .rows_affected()
                        == 1
                }
            };
            if inserted {
                break (old_item, new_item, item, pre_mutation_item);
            }

            // Lost the create race. Loop round: the locking read at the top waits for
            // the winner, then this item's update expression is applied on top of it.
            if attempt >= MAX_CREATE_RACE_ATTEMPTS {
                return Err(StorageError::Internal(format!(
                    "UpdateItem could not create the item after {attempt} attempts: a \
                     concurrent writer repeatedly created and rolled back the row"
                )));
            }
        };

        // Sync GSI/LSI update within transaction (D-4).
        if !indexes.is_empty() {
            sync_indexes(
                &mut tx,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                &indexes,
                pre_mutation_item.as_ref(),
                Some(&item),
                sys_delay,
            )
            .await?;
        }

        // Write stream record atomically within the transaction.
        if let Some(capture) = stream {
            write_stream_record_in_tx(
                &mut tx,
                key_info,
                capture,
                pre_mutation_item.as_ref(),
                Some(&item),
            )
            .await?;
        }
        // Persist async GSI work inside the same transaction — one row per
        // async index, each honoring its own propagation delay.
        let mut async_enqueued = enqueue_async_indexes(
            &mut tx,
            key_info,
            &indexes,
            pre_mutation_item.as_ref(),
            Some(&item),
            sys_delay,
        )
        .await?;

        // The pre-mutation image is already in hand here, and it is what lets the
        // vector row move partition or disappear when the update removes the
        // vector attribute.
        async_enqueued += crate::data::vector_index::maintain_vector_indexes(
            &mut tx,
            &vector_metas,
            &key_info.table_id,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            pre_mutation_item.as_ref(),
            Some(&item),
            sys_delay,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if async_enqueued > 0
            && let Some(ref q) = self.gsi_queue
        {
            q.notify_workers();
        }

        Ok((old_item, new_item))
    }
}
