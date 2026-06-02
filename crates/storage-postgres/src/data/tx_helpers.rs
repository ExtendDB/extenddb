// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transaction helper functions: item fetch/upsert/delete within a transaction,
//! stream record writing, and idempotency token checking.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extenddb_core::types::{
    AttributeValue, Item, StreamEventName, StreamRecord, StreamRecordData, StreamViewType,
    TableKeyInfo, item_size_bytes,
};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk, pk_to_text, sk_column, sk_info};

use super::{data_table_name, json_to_item};

/// Fetch a single item within an existing transaction.
pub(super) async fn fetch_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
        let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2");
        let row: Option<(serde_json::Value,)> =
            bind_sk_fetch_optional!(&sql, pk_text.as_ref(), &sk, &mut **tx)?;
        row.map(|(v,)| v)
    } else {
        let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1");
        let row: Option<(serde_json::Value,)> = sqlx::query_as(&sql)
            .bind(pk_text.as_ref())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        row.map(|(v,)| v)
    };

    json_opt.map(json_to_item).transpose()
}

/// Fetch a single item with `FOR UPDATE` lock within a transaction.
pub(super) async fn fetch_item_for_update(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
        let sql =
            format!("SELECT item_data FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2 FOR UPDATE");
        let row: Option<(serde_json::Value,)> =
            bind_sk_fetch_optional!(&sql, pk_text.as_ref(), &sk, &mut **tx)?;
        row.map(|(v,)| v)
    } else {
        let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1 FOR UPDATE");
        let row: Option<(serde_json::Value,)> = sqlx::query_as(&sql)
            .bind(pk_text.as_ref())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        row.map(|(v,)| v)
    };

    json_opt.map(json_to_item).transpose()
}

/// Upsert an item within a transaction.
pub(super) async fn upsert_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
            "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES ($1, $2, $3) \
             ON CONFLICT (pk, {sk_col}) DO UPDATE SET item_data = EXCLUDED.item_data"
        );
        bind_sk_execute!(&sql, pk_text.as_ref(), &sk, &item_json, &mut **tx)?;
    } else {
        let sql = format!(
            "INSERT INTO {ddb_table} (pk, item_data) VALUES ($1, $2) \
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
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
        let sql = format!("DELETE FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2");
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
                    .bind(n)
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
        let sql = format!("DELETE FROM {ddb_table} WHERE pk = $1");
        sqlx::query(&sql)
            .bind(pk_text.as_ref())
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }
    Ok(())
}

/// Write a stream record within an existing transaction.
///
/// Builds the stream record from the old/new items and the `StreamCapture`
/// parameters, assigns a shard, generates a sequence number, and inserts
/// the record — all within the caller's transaction.
///
/// The event type is determined from the old/new items:
/// - old=None, new=Some → Insert
/// - old=Some, new=Some → Modify
/// - old=Some, new=None → Remove
///
/// For Delete operations where the item didn't exist, no stream record is written.
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_stream_record_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
    capture: &StreamCapture,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<(), StorageError> {
    // No stream record if nothing changed (e.g., delete of non-existent item).
    let source_item = new_item.or(old_item);
    let Some(source) = source_item else {
        return Ok(());
    };

    // Determine the correct event type from old/new state.
    let event = match (old_item, new_item) {
        (None, Some(_)) => StreamEventName::Insert,
        (Some(_), Some(_)) => StreamEventName::Modify,
        (Some(_), None) => StreamEventName::Remove,
        // Unreachable: early return above handles (None, None).
        (None, None) => return Ok(()),
    };

    // Extract key attributes.
    let keys: std::collections::BTreeMap<String, AttributeValue> = key_info
        .key_schema
        .iter()
        .filter_map(|ks| {
            source
                .get(&ks.attribute_name)
                .map(|v| (ks.attribute_name.clone(), v.clone()))
        })
        .collect();

    // Build images based on view type.
    let new_image = match capture.view_type {
        StreamViewType::NewImage | StreamViewType::NewAndOldImages => new_item.cloned(),
        _ => None,
    };
    let old_image = match capture.view_type {
        StreamViewType::OldImage | StreamViewType::NewAndOldImages => old_item.cloned(),
        _ => None,
    };

    let size = source_item.map_or(0, |i| i64::try_from(item_size_bytes(i)).unwrap_or(i64::MAX));

    // Assign shard within the transaction.
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
        "SELECT shard_id FROM stream_shards \
         WHERE table_id = $1 \
         ORDER BY shard_id",
    )
    .bind(&key_info.table_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    if shards.is_empty() {
        // No shards — streams may not be fully set up yet. Skip silently.
        return Ok(());
    }

    let hash = crc32fast::hash(pk_str.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    let idx = (hash as usize) % shards.len();
    let shard_id = &shards[idx].0;

    // Generate monotonic sequence number within the transaction (CB-21).
    let (seq_val,): (i64,) = sqlx::query_as("SELECT nextval('stream_seq')")
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
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
        serde_json::to_value(&record).map_err(|e| StorageError::Internal(e.to_string()))?;

    sqlx::query(
        "INSERT INTO stream_records (sequence_number, shard_id, table_id, event_name, record_data) \
         VALUES ($1, $2, $3, $4, $5)",
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
///
/// Returns `Ok(())` for new tokens (inserted), `Err(IdempotentReplay)` for
/// matching replays, `Err(IdempotentMismatch)` for fingerprint conflicts.
pub(super) async fn check_idempotency_token_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    storage_key: &str,
    fingerprint: &str,
) -> Result<(), StorageError> {
    let row: Option<(String, bool)> = sqlx::query_as(
        r"WITH ins AS (
            INSERT INTO idempotency_tokens (token, fingerprint)
            VALUES ($1, $2)
            ON CONFLICT (token) DO UPDATE
                SET fingerprint = $2, created_at = NOW()
                WHERE idempotency_tokens.created_at <= NOW() - INTERVAL '10 minutes'
            RETURNING fingerprint, TRUE AS inserted
          )
          SELECT fingerprint, inserted FROM ins
          UNION ALL
          SELECT fingerprint, FALSE AS inserted
          FROM idempotency_tokens
          WHERE token = $1
            AND created_at > NOW() - INTERVAL '10 minutes'
            AND NOT EXISTS (SELECT 1 FROM ins)
          LIMIT 1",
    )
    .bind(storage_key)
    .bind(fingerprint)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    match row {
        Some((_, true)) | None => Ok(()),
        Some((stored, false)) if stored == fingerprint => Err(StorageError::IdempotentReplay),
        Some((_, false)) => Err(StorageError::IdempotentMismatch),
    }
}
