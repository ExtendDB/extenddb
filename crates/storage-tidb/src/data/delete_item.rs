// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_item` implementation for the `TiDB` backend.

use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, StreamEventName, TableKeyInfo};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk, sk_column, sk_info};

use super::query::check_condition;
use super::tx_helpers::{
    StreamSequenceAllocator, delete_item_without_old_item_in_tx,
    finalize_stream_records_best_effort, stream_capture_needs_old_item,
    write_stream_record_for_event_in_tx, write_stream_record_in_tx,
};
use super::{data_table_name, json_to_item, physical_pk_bytes};
use crate::TidbEngine;

impl TidbEngine {
    /// Implementation of `DataEngine::delete_item`.
    pub(crate) async fn delete_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);
        let pk = physical_pk_bytes(key, &key_info.key_schema)?;

        let needs_tx = condition.is_some() || return_old || stream.is_some();

        if let Some(capture) = delete_stream_capture_without_old_item(return_old, condition, stream)
        {
            let mut tx = self
                .data_pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let removed = delete_item_without_old_item_in_tx(&mut tx, key_info, key).await?;
            commit_delete_stream_without_old_item(
                &self.data_pool,
                tx,
                key_info,
                capture,
                key,
                removed,
            )
            .await?;
            return Ok(None);
        }

        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            if needs_tx {
                let select_sql = format!(
                    "SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ? FOR UPDATE"
                );
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");

                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let old: Option<(serde_json::Value,)> =
                    bind_sk_fetch_optional!(&select_sql, pk.as_slice(), &sk, &mut *tx)?;

                if let Some((ref old_json,)) = old {
                    let old_item: Item = json_to_item(old_json.clone())?;
                    match check_condition(condition, &old_item, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(Some(old_item)));
                        }
                        Err(e) => return Err(e),
                    }
                } else {
                    // No existing item — condition checks against empty item
                    let empty = std::collections::BTreeMap::new();
                    match check_condition(condition, &empty, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(None));
                        }
                        Err(e) => return Err(e),
                    }
                    // Nothing to delete
                    return Ok(None);
                }

                // Delete the row
                match &sk {
                    SortKeyValue::S(s) => {
                        sqlx::query(&delete_sql)
                            .bind(pk.as_slice())
                            .bind(s.as_bytes().to_vec())
                            .execute(&mut *tx)
                            .await
                    }
                    SortKeyValue::N(n) => {
                        sqlx::query(&delete_sql)
                            .bind(pk.as_slice())
                            .bind(n)
                            .execute(&mut *tx)
                            .await
                    }
                    SortKeyValue::B(b) => {
                        sqlx::query(&delete_sql)
                            .bind(pk.as_slice())
                            .bind(b)
                            .execute(&mut *tx)
                            .await
                    }
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;

                // Write stream record atomically within the transaction.
                let mut sequence_allocator = StreamSequenceAllocator::default();
                if let Some(capture) = stream {
                    let old_for_stream = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    write_stream_record_in_tx(
                        &mut tx,
                        &mut sequence_allocator,
                        key_info,
                        capture,
                        old_for_stream.as_ref(),
                        None,
                    )
                    .await?;
                }
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                finalize_stream_records_best_effort(
                    &self.data_pool,
                    "delete_item",
                    sequence_allocator.pending_records(),
                )
                .await;

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");
                match &sk {
                    SortKeyValue::S(s) => {
                        sqlx::query(&delete_sql)
                            .bind(pk.as_slice())
                            .bind(s.as_bytes().to_vec())
                            .execute(&self.data_pool)
                            .await
                    }
                    SortKeyValue::N(n) => {
                        sqlx::query(&delete_sql)
                            .bind(pk.as_slice())
                            .bind(n)
                            .execute(&self.data_pool)
                            .await
                    }
                    SortKeyValue::B(b) => {
                        sqlx::query(&delete_sql)
                            .bind(pk.as_slice())
                            .bind(b)
                            .execute(&self.data_pool)
                            .await
                    }
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok(None)
            }
        } else {
            // PK-only table
            if needs_tx {
                let select_sql =
                    format!("SELECT item_data FROM {ddb_table} WHERE pk = ? FOR UPDATE");
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = ?");

                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let old: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                    .bind(pk.as_slice())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if let Some((ref old_json,)) = old {
                    let old_item: Item = json_to_item(old_json.clone())?;
                    match check_condition(condition, &old_item, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(Some(old_item)));
                        }
                        Err(e) => return Err(e),
                    }
                } else {
                    let empty = std::collections::BTreeMap::new();
                    match check_condition(condition, &empty, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(None));
                        }
                        Err(e) => return Err(e),
                    }
                    return Ok(None);
                }

                sqlx::query(&delete_sql)
                    .bind(pk.as_slice())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                // Write stream record atomically within the transaction.
                let mut sequence_allocator = StreamSequenceAllocator::default();
                if let Some(capture) = stream {
                    let old_for_stream = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    write_stream_record_in_tx(
                        &mut tx,
                        &mut sequence_allocator,
                        key_info,
                        capture,
                        old_for_stream.as_ref(),
                        None,
                    )
                    .await?;
                }
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                finalize_stream_records_best_effort(
                    &self.data_pool,
                    "delete_item",
                    sequence_allocator.pending_records(),
                )
                .await;

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = ?");
                sqlx::query(&delete_sql)
                    .bind(pk.as_slice())
                    .execute(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok(None)
            }
        }
    }
}

fn delete_stream_capture_without_old_item<'a>(
    return_old: bool,
    condition: Option<&Expr>,
    stream: Option<&'a StreamCapture>,
) -> Option<&'a StreamCapture> {
    match stream {
        Some(capture)
            if !return_old && condition.is_none() && !stream_capture_needs_old_item(capture) =>
        {
            Some(capture)
        }
        _ => None,
    }
}

async fn commit_delete_stream_without_old_item(
    pool: &sqlx::MySqlPool,
    mut tx: sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    capture: &StreamCapture,
    key: &Item,
    removed: bool,
) -> Result<(), StorageError> {
    let mut sequence_allocator = StreamSequenceAllocator::default();
    if removed {
        write_stream_record_for_event_in_tx(
            &mut tx,
            &mut sequence_allocator,
            key_info,
            capture,
            StreamEventName::Remove,
            key,
            None,
            None,
        )
        .await?;
    }
    tx.commit()
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    finalize_stream_records_best_effort(pool, "delete_item", sequence_allocator.pending_records())
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use extenddb_core::expression::Expr;
    use extenddb_core::types::StreamViewType;
    use extenddb_storage::StreamCapture;

    use super::delete_stream_capture_without_old_item;

    fn capture(view_type: StreamViewType) -> StreamCapture {
        StreamCapture {
            view_type,
            user_identity: None,
            region: Arc::from("us-east-1"),
        }
    }

    fn condition() -> Expr {
        Expr::Function {
            name: "attribute_exists".to_owned(),
            args: vec![Expr::Path(vec![
                extenddb_core::expression::PathElement::Attribute("pk".to_owned()),
            ])],
        }
    }

    #[test]
    fn stream_delete_can_skip_old_item_for_key_or_new_image_views() {
        let keys_only = capture(StreamViewType::KeysOnly);
        let new_image = capture(StreamViewType::NewImage);

        assert!(delete_stream_capture_without_old_item(false, None, Some(&keys_only)).is_some());
        assert!(delete_stream_capture_without_old_item(false, None, Some(&new_image)).is_some());
    }

    #[test]
    fn stream_delete_keeps_old_item_read_when_result_needs_old_state() {
        let condition = condition();
        let keys_only = capture(StreamViewType::KeysOnly);
        let old_image = capture(StreamViewType::OldImage);
        let both_images = capture(StreamViewType::NewAndOldImages);

        assert!(
            delete_stream_capture_without_old_item(false, Some(&condition), Some(&keys_only))
                .is_none()
        );
        assert!(delete_stream_capture_without_old_item(true, None, Some(&keys_only)).is_none());
        assert!(delete_stream_capture_without_old_item(false, None, Some(&old_image)).is_none());
        assert!(delete_stream_capture_without_old_item(false, None, Some(&both_images)).is_none());
    }
}
