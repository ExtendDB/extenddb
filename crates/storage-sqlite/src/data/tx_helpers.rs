// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transaction helpers shared by the write path and transactions module:
//! in-transaction item fetch/upsert/delete, monotonic stream sequencing,
//! atomic stream-record capture, and idempotency-token checks.
//!
//! There is no `SELECT ... FOR UPDATE`: the engine's `write_lock` serializes
//! all writers (design decision D1), so an in-transaction read followed by a
//! write is already atomic with respect to other writers.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extenddb_core::types::{
    AttributeValue, Item, StreamEventName, StreamRecord, StreamRecordData, StreamViewType,
    TableKeyInfo, item_size_bytes,
};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{pk_to_text, sk_column, sk_info};

use super::{
    bind_sk_execute, bind_sk_fetch_optional, bind_sk_only_execute, data_table_name, json_to_item,
};
use crate::sqlite_util::format_timestamp;

/// Fetch a single item within a transaction by primary key.
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
        let sk = extenddb_storage::util::parse_sk(sk_value, sk_type)?;
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

/// Fetch for write. SQLite has no `FOR UPDATE`; the engine write lock provides
/// the serialization, so this is the same as `fetch_item_in_tx`.
pub(super) async fn fetch_item_for_update(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<Option<Item>, StorageError> {
    fetch_item_in_tx(tx, key_info, key).await
}

/// Insert or replace an item within a transaction.
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
        serde_json::to_string(item).map_err(|e| StorageError::Internal(e.to_string()))?;

    if let Some((sk_name, sk_type)) = sk_info(&key_info.key_schema, &key_info.attribute_definitions)
    {
        let sk_value = item
            .get(sk_name)
            .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
        let sk = extenddb_storage::util::parse_sk(sk_value, sk_type)?;
        let sk_col = sk_column(sk_type);
        let sql = format!(
            "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES (?, ?, ?) \
             ON CONFLICT (pk, {sk_col}) DO UPDATE SET item_data = excluded.item_data"
        );
        bind_sk_execute!(&sql, pk_text.as_ref(), &sk, &item_json, &mut **tx)?;
    } else {
        let sql = format!(
            "INSERT INTO {ddb_table} (pk, item_data) VALUES (?, ?) \
             ON CONFLICT (pk) DO UPDATE SET item_data = excluded.item_data"
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
        let sk = extenddb_storage::util::parse_sk(sk_value, sk_type)?;
        let sk_col = sk_column(sk_type);
        let sql = format!("DELETE FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");
        bind_sk_only_execute!(&sql, pk_text.as_ref(), &sk, &mut **tx)?;
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

/// Allocate the next monotonic stream sequence number within a transaction.
async fn next_stream_seq(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<i64, StorageError> {
    sqlx::query("UPDATE seq_counters SET value = value + 1 WHERE name = 'stream'")
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let value: i64 = sqlx::query_scalar("SELECT value FROM seq_counters WHERE name = 'stream'")
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(value)
}

/// Write a stream record in the same transaction as the data write, so capture
/// is atomic with the mutation (a hard requirement for correct streams).
pub(super) async fn write_stream_record_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    capture: &StreamCapture,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<(), StorageError> {
    let event_name = match (old_item, new_item) {
        (None, Some(_)) => StreamEventName::Insert,
        (Some(_), Some(_)) => StreamEventName::Modify,
        (Some(_), None) => StreamEventName::Remove,
        (None, None) => return Ok(()),
    };
    let source = new_item.or(old_item).expect("one image present");

    // Keys are the primary-key attributes from whichever image is present.
    let keys: Item = key_info
        .key_schema
        .iter()
        .filter_map(|ks| {
            source
                .get(&ks.attribute_name)
                .map(|v| (ks.attribute_name.clone(), v.clone()))
        })
        .collect();

    let new_image = matches!(
        capture.view_type,
        StreamViewType::NewImage | StreamViewType::NewAndOldImages
    )
    .then(|| new_item.cloned())
    .flatten();
    let old_image = matches!(
        capture.view_type,
        StreamViewType::OldImage | StreamViewType::NewAndOldImages
    )
    .then(|| old_item.cloned())
    .flatten();

    let size_bytes = i64::try_from(item_size_bytes(source)).unwrap_or(i64::MAX);

    // Hash the partition key to a shard.
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

    let shards: Vec<(String,)> =
        sqlx::query_as("SELECT shard_id FROM stream_shards WHERE table_id = ? ORDER BY shard_id")
            .bind(&key_info.table_id)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    if shards.is_empty() {
        return Ok(()); // Streams not enabled for this table.
    }
    let idx = (crc32fast::hash(pk_str.as_bytes()) as usize) % shards.len();
    let shard_id = shards[idx].0.clone();

    let seq = format!("{:021}", next_stream_seq(tx).await?);
    let approximate_creation_date_time = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
    .unwrap_or(i64::MAX);

    let record = StreamRecord {
        event_id: uuid::Uuid::new_v4().to_string(),
        event_name,
        event_version: "1.1".to_owned(),
        event_source: "aws:dynamodb".to_owned(),
        aws_region: capture.region.to_string(),
        dynamodb: StreamRecordData {
            approximate_creation_date_time,
            keys,
            new_image,
            old_image,
            sequence_number: seq.clone(),
            size_bytes,
            stream_view_type: capture.view_type,
        },
        user_identity: capture.user_identity.clone(),
    };
    let record_json =
        serde_json::to_string(&record).map_err(|e| StorageError::Internal(e.to_string()))?;

    sqlx::query(
        "INSERT INTO stream_records (shard_id, sequence_number, table_id, event_name, record_data) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&shard_id)
    .bind(&seq)
    .bind(&key_info.table_id)
    .bind(format!("{:?}", record.event_name))
    .bind(&record_json)
    .execute(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

/// Check (and record) an idempotency token within a transaction. Tokens are
/// valid for 10 minutes; the cutoff is computed in Rust as RFC 3339 so it
/// compares correctly against the stored RFC 3339 `created_at`.
pub(super) async fn check_idempotency_token_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    account_id: &str,
    token: &str,
    fingerprint: &str,
) -> Result<(), StorageError> {
    let cutoff = format_timestamp(time::OffsetDateTime::now_utc() - time::Duration::minutes(10));
    let existing: Option<(String,)> = sqlx::query_as(
        "SELECT fingerprint FROM idempotency_tokens \
         WHERE account_id = ? AND token = ? AND created_at > ?",
    )
    .bind(account_id)
    .bind(token)
    .bind(&cutoff)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    if let Some((stored_fp,)) = existing {
        return if stored_fp == fingerprint {
            Err(StorageError::IdempotentReplay)
        } else {
            Err(StorageError::IdempotentMismatch)
        };
    }

    let now = format_timestamp(time::OffsetDateTime::now_utc());
    sqlx::query(
        "INSERT INTO idempotency_tokens (account_id, token, fingerprint, created_at) \
         VALUES (?, ?, ?, ?) \
         ON CONFLICT (account_id, token) DO UPDATE SET fingerprint = excluded.fingerprint, \
         created_at = excluded.created_at",
    )
    .bind(account_id)
    .bind(token)
    .bind(fingerprint)
    .bind(&now)
    .execute(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod idempotency_tests {
    use super::check_idempotency_token_in_tx;
    use extenddb_storage::error::StorageError;
    use sqlx::sqlite::SqlitePool;

    async fn pool_with_schema() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE idempotency_tokens (\
                 account_id  TEXT NOT NULL,\
                 token       TEXT NOT NULL,\
                 fingerprint TEXT NOT NULL,\
                 created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),\
                 PRIMARY KEY (account_id, token)\
             )",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    async fn check(
        pool: &SqlitePool,
        account: &str,
        token: &str,
        fp: &str,
    ) -> Result<(), StorageError> {
        let mut tx = pool.begin().await.unwrap();
        let r = check_idempotency_token_in_tx(&mut tx, account, token, fp).await;
        tx.commit().await.unwrap();
        r
    }

    #[tokio::test]
    async fn idempotency_is_scoped_per_account() {
        let pool = pool_with_schema().await;

        // First use of (acctA, tok) records it.
        check(&pool, "acctA", "tok", "fpX").await.unwrap();

        // Same account + token + fingerprint = idempotent replay.
        assert!(matches!(
            check(&pool, "acctA", "tok", "fpX").await,
            Err(StorageError::IdempotentReplay)
        ));

        // Same account + token, different fingerprint = mismatch.
        assert!(matches!(
            check(&pool, "acctA", "tok", "fpY").await,
            Err(StorageError::IdempotentMismatch)
        ));

        // Regression: a DIFFERENT account reusing the same token value must be
        // independent — not a replay and not a mismatch.
        check(&pool, "acctB", "tok", "fpX").await.unwrap();
    }
}
