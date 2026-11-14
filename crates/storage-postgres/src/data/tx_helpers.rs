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
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk, pk_to_text, sk_column, sk_info};
use extenddb_storage::{StoredToken, StreamCapture, cancelled_token_fingerprint};

use super::index::db_error;
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
            .map_err(db_error)?;
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
            .map_err(db_error)?;
    }
    Ok(())
}

/// Insert an item within a transaction only when its row does not exist yet,
/// returning whether the insert won.
///
/// `fetch_item_for_update` cannot lock a row that does not exist, so two
/// transactions writing the same NEW item can both read "absent" and both
/// pass their condition checks. This helper is the atomic arbiter for that
/// create race: `ON CONFLICT DO NOTHING` blocks on a concurrent uncommitted
/// insert; if that writer commits it reports zero rows affected (we lost), and
/// if that writer aborts our insert proceeds and wins. A loser's caller then
/// re-reads the now-committed (and lockable) state and re-evaluates its
/// condition instead of silently overwriting the winner.
pub(super) async fn insert_item_if_absent_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
    item: &Item,
) -> Result<bool, StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    let pk_name = &key_info.key_schema[0].attribute_name;
    let pk_value = item
        .get(pk_name)
        .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
    let pk_text = pk_to_text(pk_value)?;
    let item_json =
        serde_json::to_value(item).map_err(|e| StorageError::Internal(e.to_string()))?;

    let rows_affected = if let Some((sk_name, sk_type)) =
        sk_info(&key_info.key_schema, &key_info.attribute_definitions)
    {
        let sk_value = item
            .get(sk_name)
            .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
        let sk = parse_sk(sk_value, sk_type)?;
        let sk_col = sk_column(sk_type);
        let sql = format!(
            "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES ($1, $2, $3) \
             ON CONFLICT (pk, {sk_col}) DO NOTHING"
        );
        bind_sk_execute!(&sql, pk_text.as_ref(), &sk, &item_json, &mut **tx)?.rows_affected()
    } else {
        let sql = format!(
            "INSERT INTO {ddb_table} (pk, item_data) VALUES ($1, $2) \
             ON CONFLICT (pk) DO NOTHING"
        );
        sqlx::query(&sql)
            .bind(pk_text.as_ref())
            .bind(&item_json)
            .execute(&mut **tx)
            .await
            .map_err(db_error)?
            .rows_affected()
    };
    Ok(rows_affected == 1)
}

/// Delete an item by key within a transaction. Returns the rows deleted.
pub(super) async fn delete_item_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<u64, StorageError> {
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
        let deleted = match &sk {
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
        .map_err(db_error)?;
        Ok(deleted.rows_affected())
    } else {
        let sql = format!("DELETE FROM {ddb_table} WHERE pk = $1");
        let deleted = sqlx::query(&sql)
            .bind(pk_text.as_ref())
            .execute(&mut **tx)
            .await
            .map_err(db_error)?;
        Ok(deleted.rows_affected())
    }
}

/// Write a stream record within an existing transaction.
///
/// Builds the stream record from the old/new items and the `StreamCapture`
/// parameters, assigns a shard ([`stream_record_shard_in_tx`]), and stores the
/// record in `stream_pending` ([`write_stream_record_to_shard_in_tx`]), all
/// within the caller's transaction. The record gets its sequence number
/// after the transaction commits (`number_shard_batch` in `stream_engine.rs`).
///
/// The event type is determined from the old/new items:
/// - old=None, new=Some → Insert
/// - old=Some, new=Some → Modify
/// - old=Some, new=None → Remove
///
/// For Delete operations where the item didn't exist, no stream record is written.
pub(super) async fn write_stream_record_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
    capture: &StreamCapture,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<(), StorageError> {
    let Some(shard_id) = stream_record_shard_in_tx(tx, key_info, old_item, new_item).await? else {
        return Ok(());
    };
    write_stream_record_to_shard_in_tx(tx, key_info, capture, old_item, new_item, &shard_id).await
}

/// The open shard that receives the stream record of a write. `None` when
/// the write records nothing: no item changed (a delete of an item that did
/// not exist), or the table has no open shard. This reads the shard rows in
/// the caller's transaction without a lock.
/// [`write_stream_record_to_shard_in_tx`] locks the chosen shard and checks
/// that it is still open.
pub(super) async fn stream_record_shard_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<Option<String>, StorageError> {
    // No stream record if nothing changed (e.g., delete of non-existent item).
    let Some(source) = new_item.or(old_item) else {
        return Ok(None);
    };

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

    // Only the open shards: they belong to the table's enabled stream. A
    // disabled stream's shards are closed and take no new records, so a write
    // that read the table's key info before a disable records nothing.
    //
    // The open set is read without a lock, and only the shard this record
    // goes to is locked, FOR KEY SHARE, re-checking that it is still open
    // (`write_stream_record_to_shard_in_tx`). Locking every open row
    // FOR SHARE made concurrent writers to one table extend the same
    // MultiXacts on all four rows for a record that goes into one.
    let mut shards: Vec<(String,)> = sqlx::query_as(
        "SELECT shard_id FROM stream_shards \
         WHERE table_id = $1 AND ending_sequence_number IS NULL \
         ORDER BY shard_id",
    )
    .bind(&key_info.table_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(db_error)?;

    if shards.is_empty() {
        // No shards — streams may not be fully set up yet. Skip silently.
        return Ok(None);
    }

    let hash = crc32fast::hash(pk_str.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    let idx = (hash as usize) % shards.len();
    Ok(Some(shards.swap_remove(idx).0))
}

/// Store the stream record of a write in `stream_pending`, in shard
/// `shard_id` ([`stream_record_shard_in_tx`]), within the caller's
/// transaction. Records nothing when no item changed, or when a disable
/// closed the shard after [`stream_record_shard_in_tx`] chose it.
///
/// This locks the shard's row FOR KEY SHARE and checks again that the shard
/// is open. A disable locks the open rows FOR UPDATE before it takes the
/// ending numbers (`close_stream_shards`), and that mode conflicts with this
/// lock. The write therefore commits before the close takes the ending
/// number, or it waits here, finds the row closed and records nothing. A
/// transaction that stores several records stores them in shard id order,
/// the order in which the disable locks the rows
/// (`transact_write_items_impl`).
pub(super) async fn write_stream_record_to_shard_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
    capture: &StreamCapture,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    shard_id: &str,
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

    let still_open: Option<(String,)> = sqlx::query_as(
        "SELECT shard_id FROM stream_shards \
         WHERE shard_id = $1 AND ending_sequence_number IS NULL FOR KEY SHARE",
    )
    .bind(shard_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db_error)?;
    if still_open.is_none() {
        // Closed between the read and the lock: the stream is disabled.
        return Ok(());
    }

    // The record's one clock read: clock_timestamp() is the database clock
    // when this statement runs, after the write's locking reads, while the
    // column default (NOW()) is the transaction start, which in a long
    // transaction sits seconds before the record. created_at and
    // ApproximateCreationDateTime both derive from this reading, and the
    // retention sweep compares created_at with the database's NOW(), so the
    // retention window is measured on the database clock alone.
    let (created_at,): (time::OffsetDateTime,) = sqlx::query_as("SELECT clock_timestamp()")
        .fetch_one(&mut **tx)
        .await
        .map_err(db_error)?;
    let approximate_creation_date_time = created_at.unix_timestamp();

    let record = StreamRecord {
        event_id: uuid::Uuid::new_v4().to_string(),
        event_name: event,
        event_version: "1.1".to_owned(),
        event_source: "aws:dynamodb".to_owned(),
        aws_region: capture.region.to_string(),
        dynamodb: StreamRecordData {
            approximate_creation_date_time,
            keys,
            new_image,
            old_image,
            // Set when the record is numbered, after this write commits.
            sequence_number: String::new(),
            size_bytes: size,
            stream_view_type: capture.view_type,
        },
        user_identity: capture.user_identity.clone(),
    };

    // The record's JSON text, sent once: the database parses it into the
    // jsonb column, and its length in bytes is stored beside it. A numbering
    // batch adds up that length (`NUMBER_BATCH_BYTES` in stream_engine.rs).
    // It is the record's size before TOAST compresses it, which is what the
    // numbering copies and rewrites; `pg_column_size` would give the
    // compressed size.
    let record_text =
        serde_json::to_string(&record).map_err(|e| StorageError::Internal(e.to_string()))?;
    let record_bytes = i64::try_from(record_text.len()).unwrap_or(i64::MAX);

    // The record waits in stream_pending, with no sequence number, until this
    // transaction commits. A reader of the shard, or the numbering worker,
    // then moves it to stream_records under the next number
    // (`number_shard_batch` in stream_engine.rs). A number drawn here with
    // nextval would be handed out at once, in call order, and this write
    // could commit after a write that drew a higher number. A reader that had
    // already read the higher number would then never return this record
    // (#808).
    sqlx::query(
        "INSERT INTO stream_pending \
         (shard_id, table_id, event_name, record_data, record_bytes, created_at) \
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind(shard_id)
    .bind(&key_info.table_id)
    .bind(format!("{:?}", record.event_name))
    .bind(&record_text)
    .bind(record_bytes)
    .bind(created_at)
    .execute(&mut **tx)
    .await
    .map_err(db_error)?;

    Ok(())
}

/// Check an idempotency token within an existing transaction.
///
/// The token is scoped to `account_id`: the same token value from different
/// accounts is stored and matched independently. Returns `Ok(())` for new
/// tokens (inserted, or a stored row taken for this run),
/// `Err(IdempotentReplay)` for matching replays, `Err(IdempotentMismatch)`
/// for fingerprint conflicts.
///
/// Two sends of one request with one token can reach this check at the same
/// time (#851). The second send's insert waits on the first send's token row
/// and, once the first send commits, locks the committed row. The select of
/// the same statement reads with the snapshot taken when the statement
/// started, before that commit, so it returns no row, or the `cancelled:` row
/// that the committed row replaced. The locked row is neither: the insert's
/// `WHERE` is false on it, so it is less than 10 minutes old and is not this
/// request's cancelled row. In those two cases the statement runs once more.
/// At READ COMMITTED the second statement takes a new snapshot, which sees
/// the locked row. This transaction holds the lock until it ends, so the row
/// cannot change between the two statements and the second one does not wait
/// on it. A committed row of this request then answers as a replay, the
/// answer the first send got.
///
/// The second statement reads the locked row when the transaction runs at
/// READ COMMITTED and `NOW()` is the same in both statements.
/// `transact_write_items_impl` opens the transaction with `begin()`, which
/// takes the default isolation level of the role or the database, so the
/// check also tests the second result. If it still returns no row, or this
/// request's `cancelled:` row, the check answers `Err(TransactionInProgress)`
/// and does not run the request.
pub(super) async fn check_idempotency_token_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: &str,
    token: &str,
    fingerprint: &str,
) -> Result<(), StorageError> {
    let mut row = run_token_statement(tx, account_id, token, fingerprint).await?;
    if stale_read(row.as_ref(), fingerprint) {
        row = run_token_statement(tx, account_id, token, fingerprint).await?;
        if stale_read(row.as_ref(), fingerprint) {
            return Err(StorageError::TransactionInProgress);
        }
    }

    match row {
        Some((_, true)) | None => Ok(()),
        Some((stored, false)) => match StoredToken::of(&stored, fingerprint) {
            StoredToken::Replay => Err(StorageError::IdempotentReplay),
            StoredToken::Rerun | StoredToken::Mismatch => Err(StorageError::IdempotentMismatch),
        },
    }
}

/// A result of [`run_token_statement`] that a snapshot from before another
/// send's commit gives: no row, or this request's `cancelled:` row that the
/// statement did not take.
fn stale_read(row: Option<&(String, bool)>, fingerprint: &str) -> bool {
    match row {
        None => true,
        Some((stored, false)) => StoredToken::of(stored, fingerprint) == StoredToken::Rerun,
        Some((_, true)) => false,
    }
}

/// One statement of [`check_idempotency_token_in_tx`]: insert the token row,
/// or take a stored row that is older than 10 minutes or that this request
/// left cancelled (see `StoredToken::of`), and return it with `true`.
/// Otherwise return the stored row with `false`, read with the statement's
/// snapshot, or no row when that snapshot holds none less than 10 minutes old.
async fn run_token_statement(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: &str,
    token: &str,
    fingerprint: &str,
) -> Result<Option<(String, bool)>, StorageError> {
    sqlx::query_as(
        r"WITH ins AS (
            INSERT INTO idempotency_tokens (account_id, token, fingerprint)
            VALUES ($1, $2, $3)
            ON CONFLICT (account_id, token) DO UPDATE
                SET fingerprint = $3, created_at = NOW()
                WHERE idempotency_tokens.created_at <= NOW() - INTERVAL '10 minutes'
                   OR idempotency_tokens.fingerprint = $4
            RETURNING fingerprint, TRUE AS inserted
          )
          SELECT fingerprint, inserted FROM ins
          UNION ALL
          SELECT fingerprint, FALSE AS inserted
          FROM idempotency_tokens
          WHERE account_id = $1
            AND token = $2
            AND created_at > NOW() - INTERVAL '10 minutes'
            AND NOT EXISTS (SELECT 1 FROM ins)
          LIMIT 1",
    )
    .bind(account_id)
    .bind(token)
    .bind(fingerprint)
    .bind(cancelled_token_fingerprint(fingerprint))
    .fetch_optional(&mut **tx)
    .await
    .map_err(db_error)
}

/// Keep a token used after its request was cancelled past the request-level
/// key checks, once the request's transaction has rolled back: store it with
/// [`cancelled_token_fingerprint`] and the time now, so the token stays used
/// for 10 minutes from the last cancellation (measured 2026-11-05). A row
/// another request or another send stored less than 10 minutes ago is kept.
pub(super) async fn record_cancelled_token(
    pool: &sqlx::PgPool,
    account_id: &str,
    token: &str,
    fingerprint: &str,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO idempotency_tokens (account_id, token, fingerprint)
          VALUES ($1, $2, $3)
          ON CONFLICT (account_id, token) DO UPDATE
              SET fingerprint = $3, created_at = NOW()
              WHERE idempotency_tokens.created_at <= NOW() - INTERVAL '10 minutes'
                 OR idempotency_tokens.fingerprint = $3",
    )
    .bind(account_id)
    .bind(token)
    .bind(cancelled_token_fingerprint(fingerprint))
    .execute(pool)
    .await
    .map_err(db_error)?;
    Ok(())
}
