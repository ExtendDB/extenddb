// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transaction helper functions: item fetch/upsert/delete within a transaction,
//! stream record writing, and idempotency token checking.

use extenddb_core::types::{
    AttributeValue, Item, StreamEventName, StreamRecord, StreamRecordData, StreamViewType,
    TableKeyInfo, item_size_bytes,
};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk, sk_column, sk_info};

use super::{data_table_name, json_to_item, physical_pk_bytes};
use crate::stream_engine::stream_shard_id_for_partition_key;
use crate::tidb_util::{current_tidb_transaction_tso, current_tidb_tso};

const STREAM_SEQUENCE_TSO_WIDTH: usize = 21;
const STREAM_SEQUENCE_ORDINAL_WIDTH: usize = 6;
const STREAM_SEQUENCE_MAX_ORDINAL: u32 = 999_999;
const STREAM_COMMIT_SEQUENCE_SQL: &str = "CONCAT(\
    LPAD(CAST(JSON_UNQUOTE(JSON_EXTRACT(\
        TIDB_MVCC_INFO(TIDB_ENCODE_RECORD_KEY(DATABASE(), 'stream_records', shard_id, sequence_number)), \
        '$[0].mvcc.info.writes[0].commit_ts'\
    )) AS CHAR), 21, '0'), \
    RIGHT(sequence_number, 6)\
)";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PendingStreamRecord {
    pub shard_id: String,
    pub storage_sequence_number: String,
}

#[derive(Default)]
pub(super) struct StreamSequenceAllocator {
    transaction_tso: Option<u64>,
    next_ordinal: u32,
    pending_records: Vec<PendingStreamRecord>,
}

impl StreamSequenceAllocator {
    async fn next_in_tx(
        &mut self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    ) -> Result<String, StorageError> {
        let transaction_tso = match self.transaction_tso {
            Some(tso) => tso,
            None => {
                let tso = current_transaction_tso(tx).await?;
                self.transaction_tso = Some(tso);
                tso
            }
        };

        let ordinal = self.next_ordinal;
        if ordinal > STREAM_SEQUENCE_MAX_ORDINAL {
            return Err(StorageError::Internal(
                "too many TiDB stream records in one transaction".to_owned(),
            ));
        }
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| StorageError::Internal("stream sequence ordinal overflow".to_owned()))?;

        Ok(format_tso_sequence_number(transaction_tso, ordinal))
    }

    fn push_pending(&mut self, shard_id: String, storage_sequence_number: String) {
        self.pending_records.push(PendingStreamRecord {
            shard_id,
            storage_sequence_number,
        });
    }

    pub(super) fn pending_records(&self) -> &[PendingStreamRecord] {
        &self.pending_records
    }
}

/// Fetch a single item with `FOR UPDATE` lock within a transaction.
pub(super) async fn fetch_item_for_update(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<Option<Item>, StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    let pk = physical_pk_bytes(key, &key_info.key_schema)?;

    let json_opt = if let Some((sk_name, sk_type)) =
        sk_info(&key_info.key_schema, &key_info.attribute_definitions)
    {
        let sk_value = key
            .get(sk_name)
            .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
        let sk = parse_sk(sk_value, sk_type)?;
        let sk_col = sk_column(sk_type);
        let sql =
            format!("SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ? FOR UPDATE");
        let row: Option<(serde_json::Value,)> =
            bind_sk_fetch_optional!(&sql, pk.as_slice(), &sk, &mut **tx)?;
        row.map(|(v,)| v)
    } else {
        let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ? FOR UPDATE");
        let row: Option<(serde_json::Value,)> = sqlx::query_as(&sql)
            .bind(pk.as_slice())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        row.map(|(v,)| v)
    };

    json_opt.map(json_to_item).transpose()
}

/// Upsert an item within a transaction.
pub(super) async fn upsert_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    item: &Item,
) -> Result<(), StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    let pk = physical_pk_bytes(item, &key_info.key_schema)?;
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
             ON DUPLICATE KEY UPDATE item_data = VALUES(item_data)"
        );
        bind_sk_execute!(&sql, pk.as_slice(), &sk, &item_json, &mut **tx)?;
    } else {
        let sql = format!(
            "INSERT INTO {ddb_table} (pk, item_data) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE item_data = VALUES(item_data)"
        );
        sqlx::query(&sql)
            .bind(pk.as_slice())
            .bind(&item_json)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }
    Ok(())
}

/// Put an item without materializing the old row and return the stream event.
///
/// This is valid only when callers do not need conditions, old return values,
/// or old stream images. TiDB's native upsert owns the concurrency and affected
/// rows classify whether the stream event is an `INSERT` or `MODIFY` without
/// materializing the old item.
pub(super) async fn put_item_without_old_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    item: &Item,
) -> Result<StreamEventName, StorageError> {
    let pk = physical_pk_bytes(item, &key_info.key_schema)?;
    let item_json =
        serde_json::to_value(item).map_err(|e| StorageError::Internal(e.to_string()))?;
    put_prepared_item_without_old_item_in_tx(tx, key_info, item, &pk, &item_json).await
}

pub(super) async fn put_prepared_item_without_old_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    item: &Item,
    pk: &[u8],
    item_json: &serde_json::Value,
) -> Result<StreamEventName, StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    if let Some((sk_name, sk_type)) = sk_info(&key_info.key_schema, &key_info.attribute_definitions)
    {
        let sk_value = item
            .get(sk_name)
            .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
        let sk = parse_sk(sk_value, sk_type)?;
        let sk_col = sk_column(sk_type);
        let upsert_sql = format!(
            "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES (?, ?, ?) \
             ON DUPLICATE KEY UPDATE item_data = VALUES(item_data)"
        );
        let result = bind_sk_execute!(&upsert_sql, pk, &sk, item_json, &mut **tx)?;
        Ok(stream_event_from_upsert_rows_affected(
            result.rows_affected(),
        ))
    } else {
        let upsert_sql = format!(
            "INSERT INTO {ddb_table} (pk, item_data) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE item_data = VALUES(item_data)"
        );
        let result = sqlx::query(&upsert_sql)
            .bind(pk)
            .bind(item_json)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(stream_event_from_upsert_rows_affected(
            result.rows_affected(),
        ))
    }
}

fn stream_event_from_upsert_rows_affected(rows_affected: u64) -> StreamEventName {
    if rows_affected == 1 {
        StreamEventName::Insert
    } else {
        StreamEventName::Modify
    }
}

/// Delete an item by key within a transaction.
pub(super) async fn delete_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<(), StorageError> {
    delete_item_without_old_item_in_tx(tx, key_info, key)
        .await
        .map(|_| ())
}

/// Delete an item without materializing the old row.
///
/// Returns `true` only when TiDB actually removed a row, which is enough to
/// decide whether a stream `REMOVE` record is needed for views that do not
/// expose old images.
pub(super) async fn delete_item_without_old_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<bool, StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    let pk = physical_pk_bytes(key, &key_info.key_schema)?;

    let result = if let Some((sk_name, sk_type)) =
        sk_info(&key_info.key_schema, &key_info.attribute_definitions)
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
                    .bind(pk.as_slice())
                    .bind(s.as_bytes().to_vec())
                    .execute(&mut **tx)
                    .await
            }
            SortKeyValue::N(n) => {
                sqlx::query(&sql)
                    .bind(pk.as_slice())
                    .bind(n)
                    .execute(&mut **tx)
                    .await
            }
            SortKeyValue::B(b) => {
                sqlx::query(&sql)
                    .bind(pk.as_slice())
                    .bind(b)
                    .execute(&mut **tx)
                    .await
            }
        }
        .map_err(|e| StorageError::Internal(e.to_string()))?
    } else {
        let sql = format!("DELETE FROM {ddb_table} WHERE pk = ?");
        sqlx::query(&sql)
            .bind(pk.as_slice())
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
    };
    Ok(result.rows_affected() > 0)
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
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    sequence_allocator: &mut StreamSequenceAllocator,
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

    write_stream_record_for_event_in_tx(
        tx,
        sequence_allocator,
        key_info,
        capture,
        event,
        source,
        old_item,
        new_item,
    )
    .await
}

pub(super) fn stream_capture_needs_old_item(capture: &StreamCapture) -> bool {
    matches!(
        capture.view_type,
        StreamViewType::OldImage | StreamViewType::NewAndOldImages
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_stream_record_for_event_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    sequence_allocator: &mut StreamSequenceAllocator,
    key_info: &TableKeyInfo,
    capture: &StreamCapture,
    event: StreamEventName,
    source: &Item,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<(), StorageError> {
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

    let size = i64::try_from(item_size_bytes(source)).unwrap_or(i64::MAX);

    // Assign shard within the transaction.
    let pk = physical_pk_bytes(source, &key_info.key_schema)?;
    let shard_id = stream_shard_id_for_partition_key(&key_info.table_id, &pk);

    // Use transaction TSO only as the clustered storage key while the row is
    // committed atomically with the item write. After commit, TiDB MVCC
    // commit_ts becomes the user-visible stream sequence base.
    let seq = sequence_allocator.next_in_tx(tx).await?;

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
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&record.dynamodb.sequence_number)
    .bind(&shard_id)
    .bind(&key_info.table_id)
    .bind(format!("{:?}", record.event_name))
    .bind(&record_json)
    .execute(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    sequence_allocator.push_pending(shard_id, record.dynamodb.sequence_number);

    Ok(())
}

async fn current_transaction_tso(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
) -> Result<u64, StorageError> {
    let tso = current_tidb_transaction_tso(tx).await?;
    u64::try_from(tso)
        .map_err(|_| StorageError::Internal(format!("TiDB returned negative TSO: {tso}")))
}

pub(crate) async fn next_stream_sequence(
    pool: &sqlx::MySqlPool,
    _shard_id: &str,
) -> Result<String, StorageError> {
    let tso = current_tidb_tso(pool).await?;
    let tso = u64::try_from(tso)
        .map_err(|_| StorageError::Internal(format!("TiDB returned negative TSO: {tso}")))?;
    Ok(format_tso_sequence_number(tso, 0))
}

pub(crate) fn format_tso_sequence_number(tso: u64, ordinal: u32) -> String {
    format!("{tso:0STREAM_SEQUENCE_TSO_WIDTH$}{ordinal:0STREAM_SEQUENCE_ORDINAL_WIDTH$}")
}

fn push_stream_record_pk_tuple_predicate<'a>(
    query: &mut sqlx::QueryBuilder<'a, sqlx::MySql>,
    records: &'a [PendingStreamRecord],
) {
    query.push("(shard_id, sequence_number) IN (");
    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            query.push(", ");
        }
        query.push("(");
        query.push_bind(&record.shard_id);
        query.push(", ");
        query.push_bind(&record.storage_sequence_number);
        query.push(")");
    }
    query.push(")");
}

pub(crate) async fn finalize_stream_records(
    pool: &sqlx::MySqlPool,
    records: &[PendingStreamRecord],
) -> Result<u64, StorageError> {
    if records.is_empty() {
        return Ok(0);
    }

    let mut query = sqlx::QueryBuilder::<sqlx::MySql>::new(
        "UPDATE stream_records \
         SET commit_sequence_number = ",
    );
    query.push(STREAM_COMMIT_SEQUENCE_SQL);
    query.push(", record_data = JSON_SET(record_data, '$.dynamodb.SequenceNumber', ");
    query.push(STREAM_COMMIT_SEQUENCE_SQL);
    query.push(") WHERE commit_sequence_number IS NULL AND ");
    query.push(STREAM_COMMIT_SEQUENCE_SQL);
    query.push(" IS NOT NULL AND ");
    push_stream_record_pk_tuple_predicate(&mut query, records);

    let result = query
        .build()
        .execute(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(result.rows_affected())
}

pub(crate) async fn finalize_stream_records_best_effort(
    pool: &sqlx::MySqlPool,
    operation: &'static str,
    records: &[PendingStreamRecord],
) {
    if records.is_empty() {
        return;
    }

    match finalize_stream_records(pool, records).await {
        Ok(finalized) if finalized == records.len() as u64 => {}
        Ok(finalized) => {
            tracing::warn!(
                operation,
                finalized,
                expected = records.len(),
                "TiDB stream commit-sequence finalization left pending records for repair"
            );
        }
        Err(error) => {
            tracing::warn!(
                operation,
                %error,
                "TiDB stream commit-sequence finalization failed; stream reads will repair"
            );
        }
    }
}

pub(crate) async fn finalize_pending_stream_records_for_shard(
    pool: &sqlx::MySqlPool,
    shard_id: &str,
    limit: i64,
) -> Result<u64, StorageError> {
    let mut total_finalized = 0_u64;
    loop {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT shard_id, sequence_number \
             FROM stream_records \
             WHERE shard_id = ? AND commit_sequence_number IS NULL \
             ORDER BY sequence_number LIMIT ?",
        )
        .bind(shard_id)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if rows.is_empty() {
            return Ok(total_finalized);
        }

        let row_count = rows.len();
        let pending = rows
            .into_iter()
            .map(|(shard_id, storage_sequence_number)| PendingStreamRecord {
                shard_id,
                storage_sequence_number,
            })
            .collect::<Vec<_>>();

        let finalized = finalize_stream_records(pool, &pending).await?;
        if finalized == 0 {
            let remaining: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM stream_records \
                 WHERE shard_id = ? AND commit_sequence_number IS NULL",
            )
            .bind(shard_id)
            .fetch_one(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if remaining > 0 {
                return Err(StorageError::Internal(format!(
                    "unable to finalize {remaining} TiDB stream records from MVCC commit metadata"
                )));
            }
            return Ok(total_finalized);
        }

        total_finalized += finalized;
        if row_count < usize::try_from(limit).unwrap_or(usize::MAX) {
            return Ok(total_finalized);
        }
    }
}

/// Check an idempotency token within an existing transaction.
///
/// Returns `Ok(())` for newly claimed tokens, `Err(IdempotentReplay)` for
/// matching in-window replays, and `Err(IdempotentMismatch)` for fingerprint
/// conflicts. TiDB native TTL owns bulk retention; this path only atomically
/// recycles a same-token row if the 10-minute DynamoDB idempotency window has
/// already elapsed but the background TTL job has not removed it yet.
pub(super) async fn check_idempotency_token_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    storage_key: &str,
    fingerprint: &str,
) -> Result<(), StorageError> {
    let claim_id = uuid::Uuid::new_v4().to_string();
    sqlx::query(idempotency_token_claim_sql())
        .bind(storage_key)
        .bind(fingerprint)
        .bind(&claim_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT fingerprint, claim_id FROM idempotency_tokens \
         WHERE token = ? \
         FOR UPDATE",
    )
    .bind(storage_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    match row {
        Some((_, stored_claim_id)) if stored_claim_id == claim_id => Ok(()),
        Some((stored, _)) if stored == fingerprint => Err(StorageError::IdempotentReplay),
        Some((_, _)) => Err(StorageError::IdempotentMismatch),
        None => Err(StorageError::Internal(
            "idempotency token disappeared during transaction".to_owned(),
        )),
    }
}

fn idempotency_token_claim_sql() -> &'static str {
    "INSERT INTO idempotency_tokens (token, fingerprint, claim_id) VALUES (?, ?, ?) \
     ON DUPLICATE KEY UPDATE \
        fingerprint = IF(created_at <= DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL 600 SECOND), VALUES(fingerprint), fingerprint), \
        claim_id = IF(created_at <= DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL 600 SECOND), VALUES(claim_id), claim_id), \
        created_at = IF(created_at <= DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL 600 SECOND), CURRENT_TIMESTAMP(6), created_at)"
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use extenddb_core::types::{StreamEventName, StreamViewType};
    use extenddb_storage::StreamCapture;

    use super::{
        PendingStreamRecord, format_tso_sequence_number, idempotency_token_claim_sql,
        push_stream_record_pk_tuple_predicate, stream_capture_needs_old_item,
        stream_event_from_upsert_rows_affected,
    };

    #[test]
    fn tidb_tso_sequence_numbers_sort_lexicographically() {
        let first = format_tso_sequence_number(42, 0);
        let second = format_tso_sequence_number(43, 0);

        assert_eq!(first, "000000000000000000042000000");
        assert!(first < second);
        assert_eq!(second.len(), 27);
    }

    #[test]
    fn tidb_tso_sequence_ordinals_order_records_inside_one_transaction() {
        let first = format_tso_sequence_number(42, 0);
        let second = format_tso_sequence_number(42, 1);

        assert_eq!(second, "000000000000000000042000001");
        assert!(first < second);
    }

    #[test]
    fn idempotency_token_claim_uses_single_native_upsert_without_cleanup_delete() {
        let sql = idempotency_token_claim_sql();

        assert!(sql.starts_with("INSERT INTO idempotency_tokens"));
        assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
        assert!(sql.contains("claim_id"));
        assert!(!sql.contains("DELETE FROM idempotency_tokens"));
    }

    #[test]
    fn stream_capture_needs_old_item_only_for_old_image_views() {
        fn capture(view_type: StreamViewType) -> StreamCapture {
            StreamCapture {
                view_type,
                user_identity: None,
                region: Arc::from("us-east-1"),
            }
        }

        assert!(!stream_capture_needs_old_item(&capture(
            StreamViewType::KeysOnly
        )));
        assert!(!stream_capture_needs_old_item(&capture(
            StreamViewType::NewImage
        )));
        assert!(stream_capture_needs_old_item(&capture(
            StreamViewType::OldImage
        )));
        assert!(stream_capture_needs_old_item(&capture(
            StreamViewType::NewAndOldImages
        )));
    }

    #[test]
    fn upsert_affected_rows_classify_stream_event_without_old_item() {
        assert_eq!(
            stream_event_from_upsert_rows_affected(1),
            StreamEventName::Insert
        );
        assert_eq!(
            stream_event_from_upsert_rows_affected(2),
            StreamEventName::Modify
        );
        assert_eq!(
            stream_event_from_upsert_rows_affected(0),
            StreamEventName::Modify
        );
    }

    #[test]
    fn stream_finalization_uses_native_primary_key_tuple_predicate() {
        let records = [
            PendingStreamRecord {
                shard_id: "shard-a".to_owned(),
                storage_sequence_number: "0001".to_owned(),
            },
            PendingStreamRecord {
                shard_id: "shard-b".to_owned(),
                storage_sequence_number: "0002".to_owned(),
            },
        ];
        let mut query = sqlx::QueryBuilder::<sqlx::MySql>::new("");

        push_stream_record_pk_tuple_predicate(&mut query, &records);

        let sql = sqlx::Execute::sql(&query.build()).to_owned();
        assert_eq!(sql, "(shard_id, sequence_number) IN ((?, ?), (?, ?))");
        assert!(!sql.contains(" OR "));
    }
}
