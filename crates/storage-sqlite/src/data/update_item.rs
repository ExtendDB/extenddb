// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_item` for the SQLite backend.
//!
//! UpdateItem is an upsert: when the item is absent a new one is created from
//! the key plus the applied actions. The condition is evaluated against the
//! pre-update image (or an empty item), then update actions are applied and the
//! result is validated and written — all under the engine write lock (D1).

use extenddb_core::expression::{self, Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_core::validation;
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;

use super::index::{enqueue_async_indexes, fetch_indexes_for_table, sync_indexes};
use super::query::check_condition;
use super::transactions::index_key_refs;
use super::tx_helpers::{fetch_item_in_tx, upsert_item_in_tx, write_stream_record_in_tx};
use crate::store::SqliteEngine;

impl SqliteEngine {
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
        // Read the propagation delay BEFORE taking the write lock. It is a
        // runtime setting, not an invariant of this write, so it does not need
        // to be read under the lock, and the lock serialises every write in the
        // process: work done inside it is the backend's throughput bottleneck.
        let system_delay = self.index_propagation_delay().await;
        let _writer = self.write_lock.lock().await;
        // Read the index set after acquiring the write lock so a concurrently
        // added GSI (UpdateTable holds the same lock) is not missed.
        let indexes = fetch_indexes_for_table(&key_info.table_id, &self.pool).await?;
        // Literal operands assigned to a secondary-index key, checked on the
        // statement ahead of the condition as the service checks them. The
        // engine ran this check against its cached key info, which does not list
        // a GSI until the UpdateTable that adds it returns; a write that read the
        // cache before then and takes the write lock after the build sees the GSI
        // only in this read, and a literal of the wrong type for it gets the same
        // per-index wording.
        if !indexes.is_empty() {
            validation::validate_index_key_literal_operands(
                actions,
                &index_key_refs(&indexes),
                super::index::fresh_attr_defs(&indexes, &key_info.attribute_definitions),
                maps,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        }

        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let old = fetch_item_in_tx(&mut tx, key_info, key).await?;

        if condition.is_some() {
            let empty = Item::new();
            let target = old.as_ref().unwrap_or(&empty);
            if let Err(e) = check_condition(condition, target, maps) {
                return match e {
                    StorageError::ConditionFailed(_) => Err(StorageError::ConditionFailed(old)),
                    other => Err(other),
                };
            }
        }

        // Start from the existing image, or from the key for a fresh upsert.
        let mut item = old.clone().unwrap_or_else(|| key.clone());
        expression::apply_update_validated(
            actions,
            &mut item,
            maps,
            &key_info.vector_indexes,
            &key_info.attribute_definitions,
        )
        .map_err(|e| StorageError::Validation(e.to_string()))?;
        // Size the update the way the service does: what the statement writes
        // plus a fixed cost per clause, against the item limit.
        validation::validate_update_item_size(&item, actions, maps, self.max_item_size_bytes)
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        // Secondary-index key validation on the post-update item, matching the
        // TransactWriteItems update path: a wrong-typed index key attribute or
        // an index key set to an empty value is a ValidationException up front,
        // rather than silently producing a malformed / unmatchable index row.
        if !indexes.is_empty() {
            let idx_refs = index_key_refs(&indexes);
            // Literal operands were checked ahead of the condition, above; a
            // value computed from the existing item is only known now and
            // carries the service's computed-value wording.
            validation::validate_index_key_types_after_update(
                &item,
                &idx_refs,
                super::index::fresh_attr_defs(&indexes, &key_info.attribute_definitions),
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
            validation::validate_index_key_not_empty(
                &item,
                &idx_refs,
                validation::SecondaryIndexEmptyContext::UpdateExpression,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        }

        upsert_item_in_tx(&mut tx, key_info, &item).await?;

        if !indexes.is_empty() {
            sync_indexes(
                &mut tx,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                &indexes,
                old.as_ref(),
                Some(&item),
                system_delay,
            )
            .await?;
        }
        let enqueued_gsi = enqueue_async_indexes(
            &mut tx,
            key_info,
            &indexes,
            old.as_ref(),
            Some(&item),
            system_delay,
        )
        .await?
            > 0;
        // Vector indexes: applied in this transaction when the propagation delay is
        // 0, otherwise enqueued alongside the async GSI work. Gated on the cached
        // key info so a table without them costs no extra query, and kept outside
        // the `indexes` guard because a table may have a vector index and no GSI.
        let enqueued_vector = if key_info.vector_indexes.is_empty() {
            false
        } else {
            crate::data::vector_index::maintain_vector_indexes(
                &mut tx,
                &key_info.table_id,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                old.as_ref(),
                Some(&item),
                system_delay,
            )
            .await?
                > 0
        };
        let enqueued = enqueued_gsi || enqueued_vector;

        if let Some(capture) = stream {
            write_stream_record_in_tx(&mut tx, key_info, capture, old.as_ref(), Some(&item))
                .await?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if enqueued {
            self.gsi_notify.notify_waiters();
        }

        let old_ret = if return_old { old } else { None };
        let new_ret = if return_new { Some(item) } else { None };
        Ok((old_ret, new_ret))
    }
}

#[cfg(test)]
mod tests {
    use crate::SqliteEngine;
    use extenddb_core::expression::{ExpressionMaps, parse_update, tokenize};
    use extenddb_core::types::{AttributeValue, CreateTableInput, Item, UpdateTableInput};
    use extenddb_storage::error::StorageError;
    use extenddb_storage::{DataEngine, TableEngine};
    use serde_json::json;

    /// UpdateItem carrying the key info cached before an UpdateTable added a GSI,
    /// as a write does that read the cache before the UpdateTable returned and
    /// took the write lock after the build. A literal of the wrong type for the
    /// new index key gets the service's per-index wording, the answer once the
    /// cache lists the index, not the wording for a value computed from the item.
    #[tokio::test]
    async fn a_wrong_typed_literal_for_a_gsi_the_cached_key_info_lacks_names_the_index() {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");
        let account = "000000000000";
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(account)
            .execute(&engine.pool)
            .await
            .expect("account");
        let input: CreateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        engine
            .create_table_impl(account, input, false)
            .await
            .expect("create table");
        sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_name = 't'")
            .execute(&engine.pool)
            .await
            .expect("activate the table");
        let cached = engine.table_key_info(account, "t").await.expect("key info");
        let update: UpdateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "AttributeDefinitions": [{"AttributeName": "g", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "gidx",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))
        .expect("update input");
        engine
            .update_table_impl(account, update)
            .await
            .expect("add the GSI");

        let key: Item = serde_json::from_value(json!({"pk": {"S": "a"}})).expect("key");
        let actions = parse_update(&tokenize("SET g = :v").expect("tokenize")).expect("parse");
        let maps = ExpressionMaps::new(
            std::collections::HashMap::new(),
            std::collections::HashMap::from([("v".to_owned(), AttributeValue::N("9".to_owned()))]),
        );
        let result = engine
            .update_item(&cached, &key, &actions, false, false, None, &maps, None)
            .await;
        let Err(StorageError::Validation(msg)) = result else {
            panic!("a wrong-typed index key was not a ValidationException: {result:?}");
        };
        assert_eq!(
            msg,
            "One or more parameter values were invalid: Type mismatch for Index Key g \
             Expected: S Actual: N IndexName: gidx"
        );
        let stored = engine.get_item(&cached, &key).await.expect("get the item");
        assert!(stored.is_none(), "the refused update wrote nothing");
    }
}
