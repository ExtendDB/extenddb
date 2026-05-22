// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transaction helper functions for the SQLite backend.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extenddb_core::types::{
    AttributeValue, Item, StreamEventName, StreamRecord, StreamRecordData, StreamViewType,
    TableKeyInfo, item_size_bytes,
};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk, pk_to_text, sk_column, sk_info};

use super::{bigdecimal_to_f64, data_table_name, json_to_item};

/// Fetch a single item within an existing transaction (no locking — SQLite serializes writes).
pub(super) async fn fetch_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<Option<Item>, StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    let pk_name = &key_info.key_schema[0].attribute_name;
    let pk_value = key
        .get(pk_name)
        .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
    let pk_text = pk_to_text(pk_value)?;

    let json_opt = if let Some((sk_name, sk_type)) =
        sk_info(&key_info.key_schema, &key_info.attribute_definitions)
    {
        let sk_value = key
            .get(sk_name)
            .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
        let sk = parse_sk(sk_value, sk_type)?;
        let sk_col = sk_column(sk_type);
        let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");
        let row: Option<(serde_json::Value,)> =
            bind_sk_fetch_optional!(&sql, pk_text.as_ref(), &sk, &mut **tx)?;
        row.map(|(v,)| v)
    } else {
        let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ?");
        let row: Option<(serde_json::Value,)> = sqlx::query_as(&sql)
            .bind(pk_text.as_ref())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        row.map(|(v,)| v)
    };

    json_opt.map(json_to_item).transpose()
}

/// Fetch a single item within a transaction for write (SQLite serializes at DB level).
pub(super) async fn fetch_item_for_update(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<Option<Item>, StorageError> {
    // SQLite doesn't support FOR UPDATE; writer serialization handles conflicts.
    fetch_item_in_tx(tx, key_info, key).await
}

/// Upsert an item within a transaction.
pub(crate) async fn upsert_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    item: &Item,
) -> Result<(), StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    let pk_name = &key_info.key_schema[0].attribute_name;
    let pk_value = item
        .get(pk_name)
        .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
    let pk_text = pk_to_text(pk_value)?;
    let item_json =
        serde_json::to_value(item).map_err(|e| StorageError::Internal(e.to_string()))?;

    if let Some((sk_name, sk_type)) = sk_info(&key_info.key_schema, &key_info.attribute_definitions)
    {
        let sk_value = item
            .get(sk_name)
            .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
        let sk = parse_sk(sk_value, sk_type)?;
        let sk_col = sk_column(sk_type);
        let sql = format!(
            "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES (?, ?, ?) \
             ON CONFLICT (pk, {sk_col}) DO UPDATE SET item_data = EXCLUDED.item_data"
        );
        bind_sk_execute!(&sql, pk_text.as_ref(), &sk, &item_json, &mut **tx)?;
    } else {
        let sql = format!(
            "INSERT INTO {ddb_table} (pk, item_data) VALUES (?, ?) \
             ON CONFLICT (pk) DO UPDATE SET item_data = EXCLUDED.item_data"
        );
        sqlx::query(&sql)
            .bind(pk_text.as_ref())
            .bind(&item_json)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }
    Ok(())
}

/// Delete an item by key within a transaction.
pub(super) async fn delete_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<(), StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    let pk_name = &key_info.key_schema[0].attribute_name;
    let pk_value = key
        .get(pk_name)
        .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
    let pk_text = pk_to_text(pk_value)?;

    if let Some((sk_name, sk_type)) = sk_info(&key_info.key_schema, &key_info.attribute_definitions)
    {
        let sk_value = key
            .get(sk_name)
            .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
        let sk = parse_sk(sk_value, sk_type)?;
        let sk_col = sk_column(sk_type);
        let sql = format!("DELETE FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");
        match &sk {
            SortKeyValue::S(s) => {
                sqlx::query(&sql)
                    .bind(pk_text.as_ref())
                    .bind(s)
                    .execute(&mut **tx)
                    .await
            }
            SortKeyValue::N(n) => {
                sqlx::query(&sql)
                    .bind(pk_text.as_ref())
                    .bind(bigdecimal_to_f64(n))
                    .execute(&mut **tx)
                    .await
            }
            SortKeyValue::B(b) => {
                sqlx::query(&sql)
                    .bind(pk_text.as_ref())
                    .bind(b)
                    .execute(&mut **tx)
                    .await
            }
        }
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    } else {
        let sql = format!("DELETE FROM {ddb_table} WHERE pk = ?");
        sqlx::query(&sql)
            .bind(pk_text.as_ref())
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }
    Ok(())
}

/// Generate a monotonic sequence number for stream records using the seq_counters table.
async fn next_stream_seq(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<i64, StorageError> {
    sqlx::query("UPDATE seq_counters SET value = value + 1 WHERE name = 'stream'")
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let val: i64 = sqlx::query_scalar("SELECT value FROM seq_counters WHERE name = 'stream'")
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(val)
}

/// Write a stream record within an existing transaction.
pub(super) async fn write_stream_record_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    capture: &StreamCapture,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<(), StorageError> {
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
            AttributeValue::B(b) => BASE64.encode(b),
            _ => String::new(),
        })
        .unwrap_or_default();

    let shards: Vec<(String,)> = sqlx::query_as(
        "SELECT shard_id FROM stream_shards WHERE table_id = ? ORDER BY shard_id",
    )
    .bind(&key_info.table_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    if shards.is_empty() {
        return Ok(());
    }

    let hash = crc32fast::hash(pk_str.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    let idx = (hash as usize) % shards.len();
    let shard_id = &shards[idx].0;

    let seq_val = next_stream_seq(tx).await?;
    let seq = format!("{seq_val:021}");

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
        serde_json::to_string(&record).map_err(|e| StorageError::Internal(e.to_string()))?;

    sqlx::query(
        "INSERT INTO stream_records (sequence_number, shard_id, table_id, event_name, record_data) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&record.dynamodb.sequence_number)
    .bind(shard_id)
    .bind(&key_info.table_id)
    .bind(format!("{:?}", record.event_name))
    .bind(&record_json)
    .execute(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(())
}

/// Check an idempotency token within an existing transaction.
pub(super) async fn check_idempotency_token_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    token: &str,
    fingerprint: &str,
) -> Result<(), StorageError> {
    // Check for existing valid token (within 10 minutes).
    let existing: Option<(String,)> = sqlx::query_as(
        "SELECT fingerprint FROM idempotency_tokens \
         WHERE token = ? AND created_at > datetime('now', '-10 minutes')",
    )
    .bind(token)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    if let Some((stored_fp,)) = existing {
        if stored_fp == fingerprint {
            return Err(StorageError::IdempotentReplay);
        } else {
            return Err(StorageError::IdempotentMismatch);
        }
    }

    // No valid existing token — insert (or replace expired token).
    sqlx::query(
        "INSERT INTO idempotency_tokens (token, fingerprint) VALUES (?, ?) \
         ON CONFLICT (token) DO UPDATE SET fingerprint = EXCLUDED.fingerprint, \
         created_at = datetime('now')",
    )
    .bind(token)
    .bind(fingerprint)
    .execute(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(())
}
