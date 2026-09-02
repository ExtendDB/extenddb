// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra TTL expiration queue helpers.
//!
//! DynamoDB TTL cannot use Cassandra native row TTL because native expiry
//! bypasses secondary-index cleanup and DynamoDB Streams. Valid timestamps are
//! indexed into day-bucketed, 64-way sharded partitions and are deleted through
//! the normal item mutation path.

use cdrs_tokio::query::BatchQueryBuilder;
use extenddb_core::types::{AttributeValue, Item, TableKeyInfo};
use extenddb_storage::error::StorageError;

use crate::CassandraEngine;

pub(crate) const TTL_SHARDS: i32 = 64;
pub(crate) const TTL_BUCKET_SECONDS: i64 = 86_400;
const TTL_QUEUE_TABLE: &str = "ttl_expirations";
const TTL_BUCKET_TABLE: &str = "ttl_expiration_buckets";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TtlConfig {
    pub attribute: String,
    pub generation: uuid::Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TtlEntry {
    pub bucket: i64,
    pub expires_at: i64,
    pub shard: i32,
    pub key_hash: String,
    pub key_data: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TtlWorkState {
    Pending,
    Claimed,
    EffectsApplied,
}

impl TtlWorkState {
    /// Parse a persisted state string.
    ///
    /// An unrecognised value is an error rather than a silent downgrade to
    /// `PENDING`: a row written by a newer state machine must not be re-claimed
    /// and re-executed by an older one.
    pub(crate) fn parse(value: Option<&str>) -> Result<Self, StorageError> {
        match value {
            Some("CLAIMED") => Ok(Self::Claimed),
            Some("EFFECTS_APPLIED") => Ok(Self::EffectsApplied),
            Some("PENDING") | None => Ok(Self::Pending),
            Some(other) => Err(StorageError::Internal(format!(
                "Unknown TTL work state {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TtlStreamPlan {
    pub event_id: String,
    pub sequence_number: String,
    pub created_at_ms: i64,
    pub region: String,
    pub view_type: extenddb_core::types::StreamViewType,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TtlWorkData {
    pub old_item: Item,
    pub delete_timestamp_ms: i64,
    pub stream: Option<TtlStreamPlan>,
}

#[derive(Debug, Clone)]
pub(crate) struct TtlWorkRow {
    pub entry: TtlEntry,
    pub state: TtlWorkState,
    pub work_id: Option<uuid::Uuid>,
    pub work_data: Option<TtlWorkData>,
}

pub(crate) fn ttl_epoch_seconds(item: &Item, attribute: &str) -> Option<i64> {
    match item.get(attribute) {
        Some(AttributeValue::N(value)) => value.parse::<i64>().ok().filter(|value| *value > 0),

        _ => None,
    }
}

pub(crate) fn entry_for_item(
    key_info: &TableKeyInfo,
    item: &Item,
    ttl_attribute: &str,
) -> Result<Option<TtlEntry>, StorageError> {
    let Some(expires_at) = ttl_epoch_seconds(item, ttl_attribute) else {
        return Ok(None);
    };

    let mut key = Item::new();
    for element in &key_info.key_schema {
        let value = item.get(&element.attribute_name).ok_or_else(|| {
            StorageError::Internal(format!(
                "TTL item missing key attribute {}",
                element.attribute_name
            ))
        })?;
        key.insert(element.attribute_name.clone(), value.clone());
    }

    let key_data = serde_json::to_string(&key)
        .map_err(|error| StorageError::Internal(format!("Serialize TTL key: {error}")))?;
    let hash = crc32fast::hash(key_data.as_bytes());

    Ok(Some(TtlEntry {
        bucket: expires_at / TTL_BUCKET_SECONDS,
        expires_at,
        shard: (hash % TTL_SHARDS as u32) as i32,
        key_hash: format!("{hash:08x}"),
        key_data,
    }))
}

fn same_queue_key(left: &TtlEntry, right: &TtlEntry) -> bool {
    left.bucket == right.bucket
        && left.expires_at == right.expires_at
        && left.shard == right.shard
        && left.key_hash == right.key_hash
        && left.key_data == right.key_data
}

pub(crate) fn add_ttl_queue_mutations(
    batch: &mut BatchQueryBuilder,
    account_keyspace: &str,
    key_info: &TableKeyInfo,
    ttl_attribute: &str,
    generation: uuid::Uuid,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<(), StorageError> {
    let old_entry = old_item
        .map(|item| entry_for_item(key_info, item, ttl_attribute))
        .transpose()?
        .flatten();
    let new_entry = new_item
        .map(|item| entry_for_item(key_info, item, ttl_attribute))
        .transpose()?
        .flatten();
    let unchanged = matches!(
        (&old_entry, &new_entry),
        (Some(old), Some(new)) if same_queue_key(old, new)
    );

    if let Some(old) = old_entry.filter(|_| !unchanged) {
        let cql = format!(
            "DELETE FROM {account_keyspace}.{TTL_QUEUE_TABLE} \
             WHERE table_id = ? AND generation = ? AND bucket = ? AND shard = ? \
             AND expires_at = ? AND key_hash = ? AND key_data = ?"
        );
        let values = cdrs_tokio::query_values!(
            key_info.table_id.as_str(),
            cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
            old.bucket,
            old.shard,
            old.expires_at,
            old.key_hash.as_str(),
            old.key_data.as_str()
        );
        let previous = std::mem::replace(batch, BatchQueryBuilder::new());
        *batch = previous.add_query(cql, values);
    }

    if let Some(new) = new_entry {
        let bucket_cql = format!(
            "INSERT INTO {account_keyspace}.{TTL_BUCKET_TABLE} \
             (table_id, generation, bucket, shard) VALUES (?, ?, ?, ?)"
        );
        let previous = std::mem::replace(batch, BatchQueryBuilder::new());
        *batch = previous.add_query(
            bucket_cql,
            cdrs_tokio::query_values!(
                key_info.table_id.as_str(),
                cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
                new.bucket,
                new.shard
            ),
        );

        let entry_cql = format!(
            "INSERT INTO {account_keyspace}.{TTL_QUEUE_TABLE} \
             (table_id, generation, bucket, shard, expires_at, key_hash, key_data, \
              state, work_id, work_data) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', null, null)"
        );
        let previous = std::mem::replace(batch, BatchQueryBuilder::new());
        *batch = previous.add_query(
            entry_cql,
            cdrs_tokio::query_values!(
                key_info.table_id.as_str(),
                cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
                new.bucket,
                new.shard,
                new.expires_at,
                new.key_hash.as_str(),
                new.key_data.as_str()
            ),
        );
    }

    Ok(())
}

pub(crate) fn add_ttl_reconciliation_mutation(
    batch: &mut BatchQueryBuilder,
    account_keyspace: &str,
    key_info: &TableKeyInfo,
    item: &Item,
) -> Result<(), StorageError> {
    let mut key = Item::new();
    for element in &key_info.key_schema {
        let value = item.get(&element.attribute_name).ok_or_else(|| {
            StorageError::Internal(format!(
                "TTL reconciliation item missing key attribute {}",
                element.attribute_name
            ))
        })?;
        key.insert(element.attribute_name.clone(), value.clone());
    }
    let key_data = serde_json::to_string(&key).map_err(|error| {
        StorageError::Internal(format!("Serialize TTL reconciliation key: {error}"))
    })?;
    let partition = (crc32fast::hash(key_data.as_bytes()) % TTL_SHARDS as u32) as i32;
    let cql = format!(
        "INSERT INTO {account_keyspace}.ttl_reconcile_pending \
         (worker_partition, id, table_id, account_id, table_name, key_data) \
         VALUES (?, now(), ?, ?, ?, ?)"
    );
    let previous = std::mem::replace(batch, BatchQueryBuilder::new());
    *batch = previous.add_query(
        cql,
        cdrs_tokio::query_values!(
            partition,
            key_info.table_id.as_str(),
            key_info.account_id.as_str(),
            key_info.table_name.as_str(),
            key_data.as_str()
        ),
    );
    Ok(())
}

pub(crate) async fn insert_ttl_entry(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
    entry: &TtlEntry,
) -> Result<(), StorageError> {
    let generation_bytes = cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec());
    let bucket_cql = format!(
        "INSERT INTO {account_keyspace}.{TTL_BUCKET_TABLE} \
         (table_id, generation, bucket, shard) VALUES (?, ?, ?, ?)"
    );
    engine
        .session
        .query_with_values(
            &bucket_cql,
            cdrs_tokio::query_values!(
                table_id,
                generation_bytes.clone(),
                entry.bucket,
                entry.shard
            ),
        )
        .await
        .map_err(|error| StorageError::Internal(format!("Insert TTL bucket: {error}")))?;

    let entry_cql = format!(
        "INSERT INTO {account_keyspace}.{TTL_QUEUE_TABLE} \
         (table_id, generation, bucket, shard, expires_at, key_hash, key_data, state) \
         VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING') IF NOT EXISTS"
    );
    let result = crate::cassandra_util::query_lwt(
        &engine.session,
        &entry_cql,
        cdrs_tokio::query_values!(
            table_id,
            generation_bytes,
            entry.bucket,
            entry.shard,
            entry.expires_at,
            entry.key_hash.as_str(),
            entry.key_data.as_str()
        ),
    )
    .await?;
    let rows = result
        .response_body()
        .map_err(|error| StorageError::Internal(format!("Parse TTL reconcile LWT: {error}")))?
        .into_rows()
        .unwrap_or_default();
    let Some(row) = rows.first() else {
        return Err(StorageError::Internal(
            "TTL reconcile LWT returned no result".to_owned(),
        ));
    };
    use cdrs_tokio::types::IntoRustByName;
    let applied: bool = row
        .get_r_by_name("[applied]")
        .map_err(|error| StorageError::Internal(format!("Parse TTL reconcile result: {error}")))?;
    if applied {
        return Ok(());
    }
    let state: Option<String> = row.get_by_name("state").ok().flatten();
    if state.as_deref() == Some("PENDING") {
        return Ok(());
    }
    Err(StorageError::Internal(
        "TTL reconciliation deferred by in-flight expiration work".to_owned(),
    ))
}

pub(crate) async fn claim_ttl_work(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
    entry: &TtlEntry,
    work_id: uuid::Uuid,
    work_data: &TtlWorkData,
) -> Result<bool, StorageError> {
    let cql = format!(
        "UPDATE {account_keyspace}.{TTL_QUEUE_TABLE} SET state = 'CLAIMED', \
         work_id = ?, work_data = ? WHERE table_id = ? AND generation = ? \
         AND bucket = ? AND shard = ? AND expires_at = ? AND key_hash = ? \
         AND key_data = ? IF state = 'PENDING'"
    );
    let work_json = serde_json::to_string(work_data)
        .map_err(|error| StorageError::Internal(format!("Serialize TTL work: {error}")))?;
    let result = crate::cassandra_util::query_lwt(
        &engine.session,
        &cql,
        cdrs_tokio::query_values!(
            cdrs_tokio::types::value::Bytes::new(work_id.as_bytes().to_vec()),
            work_json.as_str(),
            table_id,
            cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
            entry.bucket,
            entry.shard,
            entry.expires_at,
            entry.key_hash.as_str(),
            entry.key_data.as_str()
        ),
    )
    .await?;
    work_lwt_applied(&result)
}

pub(crate) async fn mark_ttl_effects_applied(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
    row: &TtlWorkRow,
) -> Result<bool, StorageError> {
    let Some(work_id) = row.work_id else {
        return Ok(false);
    };
    let cql = format!(
        "UPDATE {account_keyspace}.{TTL_QUEUE_TABLE} SET state = 'EFFECTS_APPLIED' \
         WHERE table_id = ? AND generation = ? AND bucket = ? AND shard = ? \
         AND expires_at = ? AND key_hash = ? AND key_data = ? \
         IF state = 'CLAIMED' AND work_id = ?"
    );
    let result = crate::cassandra_util::query_lwt(
        &engine.session,
        &cql,
        cdrs_tokio::query_values!(
            table_id,
            cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
            row.entry.bucket,
            row.entry.shard,
            row.entry.expires_at,
            row.entry.key_hash.as_str(),
            row.entry.key_data.as_str(),
            cdrs_tokio::types::value::Bytes::new(work_id.as_bytes().to_vec())
        ),
    )
    .await?;
    work_lwt_applied(&result)
}

pub(crate) async fn complete_ttl_work(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
    row: &TtlWorkRow,
) -> Result<bool, StorageError> {
    let Some(work_id) = row.work_id else {
        return Ok(false);
    };
    let cql = format!(
        "DELETE FROM {account_keyspace}.{TTL_QUEUE_TABLE} WHERE table_id = ? \
         AND generation = ? AND bucket = ? AND shard = ? AND expires_at = ? \
         AND key_hash = ? AND key_data = ? IF state = 'EFFECTS_APPLIED' AND work_id = ?"
    );
    let result = crate::cassandra_util::query_lwt(
        &engine.session,
        &cql,
        cdrs_tokio::query_values!(
            table_id,
            cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
            row.entry.bucket,
            row.entry.shard,
            row.entry.expires_at,
            row.entry.key_hash.as_str(),
            row.entry.key_data.as_str(),
            cdrs_tokio::types::value::Bytes::new(work_id.as_bytes().to_vec())
        ),
    )
    .await?;
    work_lwt_applied(&result)
}

pub(crate) async fn retire_pending_ttl_work(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
    entry: &TtlEntry,
) -> Result<bool, StorageError> {
    let cql = format!(
        "DELETE FROM {account_keyspace}.{TTL_QUEUE_TABLE} WHERE table_id = ? \
         AND generation = ? AND bucket = ? AND shard = ? AND expires_at = ? \
         AND key_hash = ? AND key_data = ? IF state = 'PENDING'"
    );
    let result = crate::cassandra_util::query_lwt(
        &engine.session,
        &cql,
        cdrs_tokio::query_values!(
            table_id,
            cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
            entry.bucket,
            entry.shard,
            entry.expires_at,
            entry.key_hash.as_str(),
            entry.key_data.as_str()
        ),
    )
    .await?;
    work_lwt_applied(&result)
}

pub(crate) async fn abort_claimed_ttl_work(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
    row: &TtlWorkRow,
) -> Result<bool, StorageError> {
    let Some(work_id) = row.work_id else {
        return Ok(false);
    };
    let cql = format!(
        "DELETE FROM {account_keyspace}.{TTL_QUEUE_TABLE} WHERE table_id = ? \
     AND generation = ? AND bucket = ? AND shard = ? AND expires_at = ? \
     AND key_hash = ? AND key_data = ? IF state = 'CLAIMED' AND work_id = ?"
    );
    let result = crate::cassandra_util::query_lwt(
        &engine.session,
        &cql,
        cdrs_tokio::query_values!(
            table_id,
            cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
            row.entry.bucket,
            row.entry.shard,
            row.entry.expires_at,
            row.entry.key_hash.as_str(),
            row.entry.key_data.as_str(),
            cdrs_tokio::types::value::Bytes::new(work_id.as_bytes().to_vec())
        ),
    )
    .await?;
    work_lwt_applied(&result)
}

fn work_lwt_applied(result: &cdrs_tokio::frame::Envelope) -> Result<bool, StorageError> {
    use cdrs_tokio::types::IntoRustByName;

    let rows = result
        .response_body()
        .map_err(|error| StorageError::Internal(format!("Parse TTL work LWT: {error}")))?
        .into_rows()
        .unwrap_or_default();
    let Some(row) = rows.first() else {
        return Err(StorageError::Internal(
            "TTL work LWT returned no result".to_owned(),
        ));
    };
    row.get_r_by_name("[applied]")
        .map_err(|error| StorageError::Internal(format!("Parse TTL work result: {error}")))
}

/// Remove a `(bucket, shard)` registration from the bucket registry.
///
/// `guard_timestamp` must be a microsecond timestamp captured *before* the
/// caller observed the partition to be empty. Passing it makes the delete lose
/// to any queue insert that commits afterwards: every insert re-registers the
/// bucket in the same logged batch as the entry, and that insert carries a
/// later coordinator timestamp, so an entry can never be left behind with its
/// registration deleted. `None` skips the guard and is only correct when the
/// generation is being retired and no further inserts can target it.
async fn retire_bucket_registration(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
    bucket: i64,
    shard: i32,
    guard_timestamp: Option<i64>,
) -> Result<(), StorageError> {
    let using = guard_timestamp
        .map(|timestamp| format!(" USING TIMESTAMP {timestamp}"))
        .unwrap_or_default();
    let cql = format!(
        "DELETE FROM {account_keyspace}.{TTL_BUCKET_TABLE}{using} \
         WHERE table_id = ? AND generation = ? AND bucket = ? AND shard = ?"
    );
    engine
        .session
        .query_with_values(
            &cql,
            cdrs_tokio::query_values!(
                table_id,
                cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
                bucket,
                shard
            ),
        )
        .await
        .map_err(|error| {
            StorageError::Internal(format!("Retire TTL bucket registration: {error}"))
        })?;
    Ok(())
}

/// Upper bound on registry partitions a single sweep cycle will visit.
///
/// The registry holds one row per `(day bucket, shard)` ever written, so
/// without a cap the per-cycle query fan-out grows linearly with the age of
/// the table. Partitions are rotated between cycles, so capping bounds the
/// cost of a cycle without starving any partition.
const TTL_MAX_PARTITIONS_PER_CYCLE: usize = 512;

pub(crate) async fn load_due_ttl_work(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
    now: i64,
    limit: usize,
) -> Result<Vec<TtlWorkRow>, StorageError> {
    use cdrs_tokio::types::IntoRustByName;

    if limit == 0 {
        return Ok(Vec::new());
    }
    // Captured before the emptiness observations below, so it can safely guard
    // the retirement of any partition this cycle finds drained.
    let retire_guard = chrono::Utc::now().timestamp_micros();
    let generation_bytes = cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec());
    let current_bucket = now / TTL_BUCKET_SECONDS;
    let bucket_query = format!(
        "SELECT bucket, shard FROM {account_keyspace}.{TTL_BUCKET_TABLE} \
         WHERE table_id = ? AND generation = ? AND bucket <= ?"
    );
    let bucket_rows = crate::cassandra_util::query_rows(
        &engine.session,
        &bucket_query,
        cdrs_tokio::query_values!(table_id, generation_bytes.clone(), current_bucket),
        "load_due_ttl_buckets",
    )
    .await?;
    let mut partitions: Vec<(i64, i32)> = Vec::with_capacity(bucket_rows.len());
    for row in bucket_rows {
        partitions.push((
            crate::cassandra_util::get_column(&row, "bucket", "load_due_ttl_buckets")?,
            crate::cassandra_util::get_column(&row, "shard", "load_due_ttl_buckets")?,
        ));
    }
    if !partitions.is_empty() {
        let partition_count = partitions.len();
        partitions.rotate_left(((now / 60) as usize) % partition_count);
        partitions.truncate(TTL_MAX_PARTITIONS_PER_CYCLE);
    }

    let mut work = Vec::with_capacity(limit);
    for (index, (bucket, shard)) in partitions.iter().enumerate() {
        let remaining = limit - work.len();
        if remaining == 0 {
            break;
        }
        let partitions_left = partitions.len() - index;
        let partition_limit = remaining.div_ceil(partitions_left).max(1);
        let query = format!(
            "SELECT expires_at, key_hash, key_data, state, work_id, work_data \
             FROM {account_keyspace}.{TTL_QUEUE_TABLE} WHERE table_id = ? \
             AND generation = ? AND bucket = ? AND shard = ? AND expires_at <= ? \
             LIMIT {partition_limit}"
        );
        let rows = crate::cassandra_util::query_rows(
            &engine.session,
            &query,
            cdrs_tokio::query_values!(table_id, generation_bytes.clone(), *bucket, *shard, now),
            "load_due_ttl_work",
        )
        .await?;
        // A fully past day bucket that yields nothing is drained: every entry
        // it could ever hold is already due, so an empty read means the
        // partition is empty rather than not-yet-due. Retire the registration
        // so the fan-out above stops growing with the age of the table.
        //
        // The scan above runs at the driver's default consistency, where a
        // replica that has not caught up reads as empty. Retiring on that read
        // would orphan any entry it missed — the entry's own insert is older
        // than the guard timestamp, so the guard would not save it, and the item
        // would then never expire. Confirm at LOCAL_QUORUM first.
        if rows.is_empty() && *bucket < current_bucket {
            let confirm = crate::cassandra_util::query_rows_quorum(
                &engine.session,
                &format!(
                    "SELECT key_hash FROM {account_keyspace}.{TTL_QUEUE_TABLE} \
                     WHERE table_id = ? AND generation = ? AND bucket = ? AND shard = ? LIMIT 1"
                ),
                cdrs_tokio::query_values!(table_id, generation_bytes.clone(), *bucket, *shard),
                "confirm_drained_ttl_bucket",
            )
            .await?;
            if confirm.is_empty() {
                retire_bucket_registration(
                    engine,
                    account_keyspace,
                    table_id,
                    generation,
                    *bucket,
                    *shard,
                    Some(retire_guard),
                )
                .await?;
            }
            continue;
        }
        for row in rows {
            let state: Option<String> = row.get_by_name("state").ok().flatten();
            let work_id: Option<uuid::Uuid> = row.get_by_name("work_id").ok().flatten();
            let work_data: Option<String> = row.get_by_name("work_data").ok().flatten();
            work.push(TtlWorkRow {
                entry: TtlEntry {
                    bucket: *bucket,
                    shard: *shard,
                    expires_at: crate::cassandra_util::get_column(
                        &row,
                        "expires_at",
                        "load_due_ttl_work",
                    )?,
                    key_hash: crate::cassandra_util::get_column(
                        &row,
                        "key_hash",
                        "load_due_ttl_work",
                    )?,
                    key_data: crate::cassandra_util::get_column(
                        &row,
                        "key_data",
                        "load_due_ttl_work",
                    )?,
                },
                state: TtlWorkState::parse(state.as_deref())?,
                work_id,
                work_data: work_data
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(|error| {
                        StorageError::Internal(format!("Parse TTL work data: {error}"))
                    })?,
            });
        }
    }
    Ok(work)
}

pub(crate) async fn delete_ttl_entry(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,

    entry: &TtlEntry,
) -> Result<(), StorageError> {
    let cql = format!(
        "DELETE FROM {account_keyspace}.{TTL_QUEUE_TABLE} \
         WHERE table_id = ? AND generation = ? AND bucket = ? AND shard = ? \
         AND expires_at = ? AND key_hash = ? AND key_data = ?"
    );
    engine
        .session
        .query_with_values(
            &cql,
            cdrs_tokio::query_values!(
                table_id,
                cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
                entry.bucket,
                entry.shard,
                entry.expires_at,
                entry.key_hash.as_str(),
                entry.key_data.as_str()
            ),
        )
        .await
        .map_err(|error| StorageError::Internal(format!("Delete TTL entry: {error}")))?;
    Ok(())
}

/// Load every queue row for a generation regardless of due time, in any state.
///
/// Used to drain a retired generation: unlike [`load_due_ttl_work`] this does
/// not filter by `expires_at`, because cleanup has to account for work that was
/// claimed before the generation was retired.
pub(crate) async fn load_generation_work(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
    limit: usize,
) -> Result<Vec<TtlWorkRow>, StorageError> {
    use cdrs_tokio::types::IntoRustByName;

    let generation_bytes = cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec());
    let mut work = Vec::new();
    for (bucket, shard) in
        generation_partitions(engine, account_keyspace, table_id, generation).await?
    {
        if work.len() >= limit {
            break;
        }
        // Quorum: work this read misses would be abandoned holding a base-row
        // claim, and the generation would be reported drained when it is not.
        let rows = crate::cassandra_util::query_rows_quorum(
            &engine.session,
            &format!(
                "SELECT expires_at, key_hash, key_data, state, work_id, work_data \
                 FROM {account_keyspace}.{TTL_QUEUE_TABLE} WHERE table_id = ? \
                 AND generation = ? AND bucket = ? AND shard = ?"
            ),
            cdrs_tokio::query_values!(table_id, generation_bytes.clone(), bucket, shard),
            "load_generation_work",
        )
        .await?;
        for row in rows {
            let state: Option<String> = row.get_by_name("state").ok().flatten();
            let work_id: Option<uuid::Uuid> = row.get_by_name("work_id").ok().flatten();
            let work_data: Option<String> = row.get_by_name("work_data").ok().flatten();
            work.push(TtlWorkRow {
                entry: TtlEntry {
                    bucket,
                    shard,
                    expires_at: crate::cassandra_util::get_column(
                        &row,
                        "expires_at",
                        "load_generation_work",
                    )?,
                    key_hash: crate::cassandra_util::get_column(
                        &row,
                        "key_hash",
                        "load_generation_work",
                    )?,
                    key_data: crate::cassandra_util::get_column(
                        &row,
                        "key_data",
                        "load_generation_work",
                    )?,
                },
                state: TtlWorkState::parse(state.as_deref())?,
                work_id,
                work_data: work_data
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(|error| {
                        StorageError::Internal(format!("Parse TTL work data: {error}"))
                    })?,
            });
        }
    }
    Ok(work)
}

/// List the `(bucket, shard)` partitions registered for a generation.
async fn generation_partitions(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
) -> Result<Vec<(i64, i32)>, StorageError> {
    let rows = crate::cassandra_util::query_rows(
        &engine.session,
        &format!(
            "SELECT bucket, shard FROM {account_keyspace}.{TTL_BUCKET_TABLE} \
             WHERE table_id = ? AND generation = ?"
        ),
        cdrs_tokio::query_values!(
            table_id,
            cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec())
        ),
        "generation_partitions",
    )
    .await?;
    let mut partitions = Vec::with_capacity(rows.len());
    for row in rows {
        partitions.push((
            crate::cassandra_util::get_column(&row, "bucket", "generation_partitions")?,
            crate::cassandra_util::get_column(&row, "shard", "generation_partitions")?,
        ));
    }
    Ok(partitions)
}

/// Remove a retired generation's queue rows.
///
/// Only `PENDING` rows are removed. A row in `CLAIMED` or `EFFECTS_APPLIED`
/// owns durable state — a base-row claim, and possibly already-applied index
/// and stream effects — so deleting it would strand that state with nothing
/// left to drive recovery. Those rows are left in place and reported by the
/// `false` return, which keeps `ttl_cleanup_generation` set so the worker
/// drains them and retries.
pub(crate) async fn clear_ttl_generation(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
    generation: uuid::Uuid,
) -> Result<bool, StorageError> {
    let generation_bytes = cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec());
    let mut fully_drained = true;
    for (bucket, shard) in
        generation_partitions(engine, account_keyspace, table_id, generation).await?
    {
        // Quorum: `partition_drained` below decides whether to delete the bucket
        // registration and whether the generation can be reported fully drained.
        let rows = crate::cassandra_util::query_rows_quorum(
            &engine.session,
            &format!(
                "SELECT expires_at, key_hash, key_data, state \
                 FROM {account_keyspace}.{TTL_QUEUE_TABLE} WHERE table_id = ? \
                 AND generation = ? AND bucket = ? AND shard = ?"
            ),
            cdrs_tokio::query_values!(table_id, generation_bytes.clone(), bucket, shard),
            "clear_ttl_generation",
        )
        .await?;
        let mut partition_drained = true;
        for row in rows {
            use cdrs_tokio::types::IntoRustByName;
            let state: Option<String> = row.get_by_name("state").ok().flatten();
            if TtlWorkState::parse(state.as_deref())? != TtlWorkState::Pending {
                partition_drained = false;
                fully_drained = false;
                continue;
            }
            let entry = TtlEntry {
                bucket,
                shard,
                expires_at: crate::cassandra_util::get_column(
                    &row,
                    "expires_at",
                    "clear_ttl_generation",
                )?,
                key_hash: crate::cassandra_util::get_column(
                    &row,
                    "key_hash",
                    "clear_ttl_generation",
                )?,
                key_data: crate::cassandra_util::get_column(
                    &row,
                    "key_data",
                    "clear_ttl_generation",
                )?,
            };
            if !retire_pending_ttl_work(engine, account_keyspace, table_id, generation, &entry)
                .await?
            {
                // Claimed between the read and the delete; leave it for the
                // drain pass.
                partition_drained = false;
                fully_drained = false;
            }
        }
        if partition_drained {
            retire_bucket_registration(
                engine,
                account_keyspace,
                table_id,
                generation,
                bucket,
                shard,
                None,
            )
            .await?;
        }
    }
    Ok(fully_drained)
}

pub(crate) async fn clear_ttl_entries(
    engine: &CassandraEngine,
    account_keyspace: &str,
    table_id: &str,
) -> Result<(), StorageError> {
    let bucket_query = format!(
        "SELECT generation, bucket, shard FROM {account_keyspace}.{TTL_BUCKET_TABLE} \
         WHERE table_id = ?"
    );
    let rows = crate::cassandra_util::query_rows(
        &engine.session,
        &bucket_query,
        cdrs_tokio::query_values!(table_id),
        "clear_ttl_entries",
    )
    .await?;
    for row in rows {
        let generation: uuid::Uuid =
            crate::cassandra_util::get_column(&row, "generation", "clear_ttl_entries")?;
        let bucket: i64 = crate::cassandra_util::get_column(&row, "bucket", "clear_ttl_entries")?;
        let shard: i32 = crate::cassandra_util::get_column(&row, "shard", "clear_ttl_entries")?;
        let delete = format!(
            "DELETE FROM {account_keyspace}.{TTL_QUEUE_TABLE} \
             WHERE table_id = ? AND generation = ? AND bucket = ? AND shard = ?"
        );
        engine
            .session
            .query_with_values(
                &delete,
                cdrs_tokio::query_values!(
                    table_id,
                    cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
                    bucket,
                    shard
                ),
            )
            .await
            .map_err(|error| StorageError::Internal(format!("Clear TTL partition: {error}")))?;
    }
    let delete_buckets =
        format!("DELETE FROM {account_keyspace}.{TTL_BUCKET_TABLE} WHERE table_id = ?");
    engine
        .session
        .query_with_values(&delete_buckets, cdrs_tokio::query_values!(table_id))
        .await
        .map_err(|error| StorageError::Internal(format!("Clear TTL buckets: {error}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    fn key_info() -> TableKeyInfo {
        TableKeyInfo {
            table_name: "table".to_owned(),
            account_id: "123456789012".to_owned(),
            table_id: "table-id".to_owned(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "id".to_owned(),
                key_type: KeyType::Hash,
            }],
            base_key_schema: vec![KeySchemaElement {
                attribute_name: "id".to_owned(),
                key_type: KeyType::Hash,
            }],
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "id".to_owned(),
                attribute_type: ScalarAttributeType::S,
            }],
            has_lsi: false,
            global_secondary_indexes: Vec::new(),
            local_secondary_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            stream_specification: None,
        }
    }

    #[test]
    fn ttl_epoch_accepts_only_positive_integral_numbers() {
        let mut item = Item::new();
        for (value, expected) in [("123", Some(123)), ("0", None), ("-1", None), ("1.5", None)] {
            item.insert("ttl".to_owned(), AttributeValue::N(value.to_owned()));
            assert_eq!(ttl_epoch_seconds(&item, "ttl"), expected);
        }
        item.insert("ttl".to_owned(), AttributeValue::S("123".to_owned()));
        assert_eq!(ttl_epoch_seconds(&item, "ttl"), None);
    }

    #[test]
    fn ttl_entry_is_stable_and_bounded_to_a_shard() {
        let mut item = Item::new();
        item.insert("id".to_owned(), AttributeValue::S("a".to_owned()));
        item.insert("ttl".to_owned(), AttributeValue::N("123".to_owned()));
        let first = entry_for_item(&key_info(), &item, "ttl").unwrap().unwrap();
        let second = entry_for_item(&key_info(), &item, "ttl").unwrap().unwrap();
        assert_eq!(first, second);
        assert!((0..TTL_SHARDS).contains(&first.shard));
        assert_eq!(first.bucket, first.expires_at / TTL_BUCKET_SECONDS);
    }

    #[test]
    fn unchanged_ttl_does_not_delete_and_reinsert_same_queue_key() {
        let key_info = key_info();
        let mut item = Item::new();
        item.insert("id".to_owned(), AttributeValue::S("a".to_owned()));
        item.insert("ttl".to_owned(), AttributeValue::N("123".to_owned()));
        let mut batch = BatchQueryBuilder::new();
        add_ttl_queue_mutations(
            &mut batch,
            "account_keyspace",
            &key_info,
            "ttl",
            uuid::Uuid::new_v4(),
            Some(&item),
            Some(&item),
        )
        .unwrap();
        let built = batch.build().unwrap();
        assert_eq!(built.request.queries.len(), 2);
    }

    /// Every TTL claim and the exact base-row delete condition on
    /// `item_data = ?`, where the expected value is produced by re-serialising
    /// an item that was parsed out of the stored string. That only works while
    /// re-serialising a stored form reproduces it byte for byte, so an
    /// accidental change to `AttributeValue`'s serde representation would
    /// silently stop TTL deleting anything and start failing writes with
    /// `TransactionConflict`. This asserts the invariant directly rather than
    /// leaving it to a live Cassandra test to notice.
    ///
    /// The stored form is whatever the write path produced, so the property
    /// under test is that serialisation is canonical: a value that has been
    /// serialised once is unchanged by every later round trip. Note that this
    /// means the first serialisation *does* normalise — `N: "1.500"` is stored
    /// as `1.5` — which is why the claim compares against the stored form and
    /// never against a client-supplied string.
    #[test]
    fn stored_item_json_round_trips_byte_for_byte() {
        let submitted = [
            r#"{"id":{"S":"a"}}"#,
            r#"{"id":{"S":""}}"#,
            r#"{"id":{"S":"a"},"n":{"N":"0"}}"#,
            r#"{"id":{"S":"a"},"n":{"N":"-1"}}"#,
            r#"{"id":{"S":"a"},"n":{"N":"1.500"}}"#,
            r#"{"id":{"S":"a"},"n":{"N":"1e3"}}"#,
            r#"{"id":{"S":"a"},"n":{"N":"123456789012345678901234567890"}}"#,
            r#"{"b":{"B":"aGVsbG8="},"id":{"S":"a"}}"#,
            r#"{"bool":{"BOOL":true},"id":{"S":"a"},"null":{"NULL":true}}"#,
            r#"{"id":{"S":"a"},"l":{"L":[{"S":"x"},{"N":"1"}]}}"#,
            r#"{"id":{"S":"a"},"m":{"M":{"a":{"S":"x"},"b":{"N":"2"}}}}"#,
            r#"{"id":{"S":"a"},"ss":{"SS":["a","b"]}}"#,
            r#"{"id":{"S":"a"},"unicode":{"S":"héllo → 世界"}}"#,
        ];
        for original in submitted {
            let parsed: Item = serde_json::from_str(original)
                .unwrap_or_else(|error| panic!("parse {original}: {error}"));
            // What the write path persists into `item_data`.
            let stored = serde_json::to_string(&parsed).expect("serialize item");
            // What a claim or exact delete reconstructs from it.
            let reloaded: Item = serde_json::from_str(&stored)
                .unwrap_or_else(|error| panic!("parse stored {stored}: {error}"));
            let expected = serde_json::to_string(&reloaded).expect("serialize reloaded item");
            assert_eq!(
                expected, stored,
                "item_data round trip is not byte-stable for {original}"
            );
        }
    }
}
