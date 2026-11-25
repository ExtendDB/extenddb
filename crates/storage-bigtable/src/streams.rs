// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DynamoDB` Streams support for the Cloud Bigtable backend.
//!
//! Stream shard lineage, deduplication markers, and records live in `__extenddb_catalog__`:
//! - `stream_shard:<shard_id>` — one `ShardMeta` row per labelled stream shard
//!   (`shardId-{table_id}-{label_millis:013}{view_digit:1}{index:02}`), carrying
//!   contiguous `next_seq_num` (`1, 2, ...`) and `last_sequence_number`.
//! - `stream_record:<shard_id>:<seq_26>` — one `StreamRecord` row per contiguous
//!   shard log slot (`slot_sequence_number(shard_id, slot)`), claimed via atomic
//!   `Catalog::put_if_absent` so `seq_2 > seq_1` can never become visible on a
//!   shard ahead of `seq_1`.
//! - `stream_dedup:<shard_id>:<dedup_key>` — exact-once publication marker ensuring
//!   concurrent or retried `roll_forward` / `sweeper` passes never emit duplicate
//!   stream records.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::Duration;

use extenddb_core::types::{
    Item, KeySchemaElement, StreamEventName, StreamRecord, StreamRecordData, StreamSpecification,
    StreamViewType, extract_key, item_size_bytes,
};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage::ShardRow;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::catalog::keys;
use crate::catalog::{Catalog, CatalogJsonAction};

/// Fixed width of Bigtable stream sequence numbers in decimal digits.
pub const SEQUENCE_NUMBER_WIDTH: usize = 26;

/// Placeholder sequence number stored during Phase 1 before Phase 2 assigns a
/// contiguous per-shard log slot.
pub const UNASSIGNED_SEQUENCE_NUMBER: &str = "00000000000000000000000000";

const EVENT_SOURCE: &str = "aws:dynamodb";
const EVENT_VERSION: &str = "1.1";

/// Top-level JSON field stored alongside persisted `stream_record:*` rows so
/// `AT_TIMESTAMP` seeks and retention cleanup compare at millisecond precision
/// while wire `StreamRecord` deserialization ignores the extra field.
const CREATED_AT_MS_FIELD: &str = "created_at_ms";

/// Default number of `stream_record:<shard_id>:*` rows fetched per Bigtable
/// range page during `first_sequence_number_at_or_after`.
const AT_TIMESTAMP_SCAN_PAGE_SIZE: i64 = 512;

/// Read the UTC wall clock once, returning `(epoch_secs, epoch_millis)` from
/// the same clock read so `ApproximateCreationDateTime` and `created_at_ms`
/// always agree (`epoch_millis.div_euclid(1_000) == epoch_secs`).
#[must_use]
pub fn current_stream_clock() -> (i64, i64) {
    let now = time::OffsetDateTime::now_utc();
    let secs = now.unix_timestamp();
    let ms = secs
        .saturating_mul(1_000)
        .saturating_add(i64::from(now.millisecond()));
    (secs, ms)
}

/// Decode a persisted `stream_record:<shard_id>:<seq_26>` JSON value into its
/// [`StreamRecord`] and effective creation time in Unix epoch milliseconds.
///
/// Legacy rows written before `created_at_ms` was stored fall back to
/// `ApproximateCreationDateTime * 1_000`.
#[must_use]
pub fn decode_stored_record(value: serde_json::Value) -> Option<(StreamRecord, i64)> {
    let stored_ms = value
        .get(CREATED_AT_MS_FIELD)
        .and_then(serde_json::Value::as_i64);
    let record: StreamRecord = serde_json::from_value(value).ok()?;
    let effective_ms = stored_ms.unwrap_or_else(|| {
        record
            .dynamodb
            .approximate_creation_date_time
            .saturating_mul(1_000)
    });
    Some((record, effective_ms))
}

const fn default_next_seq_num() -> u64 {
    1
}

/// Persisted shard metadata row stored at `stream_shard:<shard_id>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMeta {
    pub table_id: String,
    pub shard_id: String,
    #[serde(default)]
    pub parent_shard_id: Option<String>,
    pub starting_sequence_number: String,
    #[serde(default)]
    pub ending_sequence_number: Option<String>,
    #[serde(default = "default_next_seq_num")]
    pub next_seq_num: u64,
    #[serde(default)]
    pub last_sequence_number: Option<String>,
}

impl ShardMeta {
    /// Construct a brand-new open shard row whose `starting_sequence_number`
    /// anchors at slot `1` of this shard's deterministic sequence space.
    #[must_use]
    pub fn new_open(table_id: String, shard_id: String, parent_shard_id: Option<String>) -> Self {
        let starting_sequence_number = slot_sequence_number(&shard_id, 1);
        Self {
            table_id,
            shard_id,
            parent_shard_id,
            starting_sequence_number,
            ending_sequence_number: None,
            next_seq_num: 1,
            last_sequence_number: None,
        }
    }

    #[must_use]
    pub fn to_lineage_row(&self) -> ShardRow {
        ShardRow {
            table_id: self.table_id.clone(),
            shard_id: self.shard_id.clone(),
            parent_shard_id: self.parent_shard_id.clone(),
            starting_sequence_number: self.starting_sequence_number.clone(),
            ending_sequence_number: self.ending_sequence_number.clone(),
        }
    }
}

/// Extract `(label_millis, shard_idx)` from a labelled `shard_id`
/// (`shardId-{table_id}-{label_millis:013}{view_digit:1}{index:02}`), falling
/// back deterministically for arbitrary synthetic shard identifiers.
#[must_use]
pub fn shard_label_millis_and_index(shard_id: &str) -> (u64, u8) {
    if let Some((_, suffix)) = shard_id.rsplit_once('-')
        && suffix.len() == 16
        && suffix.bytes().all(|b| b.is_ascii_digit())
    {
        let label_ms = suffix[..13].parse::<u64>().unwrap_or(1_000_000_000_000);
        let idx = suffix[14..16].parse::<u8>().unwrap_or(0);
        return (label_ms.max(1), idx);
    }
    let fallback_idx = u8::try_from(crc32fast::hash(shard_id.as_bytes()) % 100).unwrap_or(0);
    (1_000_000_000_000, fallback_idx)
}

/// Deterministic 26-digit sequence number for `(shard_id, slot)` (`slot >= 1`):
/// `{label_millis:013}{slot:011}{shard_idx:02}`.
#[must_use]
pub fn slot_sequence_number(shard_id: &str, slot: u64) -> String {
    let (label_ms, shard_idx) = shard_label_millis_and_index(shard_id);
    let clamped_slot = slot.clamp(1, 99_999_999_999);
    format!("{label_ms:013}{clamped_slot:011}{shard_idx:02}")
}

/// Parse the 11-digit contiguous `slot` index out of a 26-digit `slot_sequence_number`.
#[must_use]
pub fn parse_slot_from_sequence(seq: &str) -> Option<u64> {
    if seq.len() == SEQUENCE_NUMBER_WIDTH && seq.bytes().all(|b| b.is_ascii_digit()) {
        seq[13..24].parse::<u64>().ok()
    } else {
        None
    }
}

/// Build the stream ARN for `(region, account_id, table_name, label)`.
#[must_use]
pub fn build_stream_arn(region: &str, account_id: &str, table_name: &str, label: &str) -> String {
    extenddb_storage::util::stream_arn(region, account_id, table_name, label)
}

fn node_decimal_suffix() -> u8 {
    static SUFFIX: OnceLock<u8> = OnceLock::new();
    *SUFFIX.get_or_init(|| rand::random_range(0u8..100))
}

/// Generate a fresh, strictly monotonic 26-digit decimal sequence number
/// (`{ms:013}{counter:011}{node:02}`) for `StreamEngine::next_sequence_number`.
#[must_use]
pub fn next_sequence_number() -> String {
    static SEQ_STATE: Mutex<(u64, u64)> = Mutex::new((0, 0));

    let now_ms =
        u64::try_from((time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).max(1))
            .unwrap_or(1);
    let (ms, counter) = {
        let mut guard = SEQ_STATE.lock().unwrap_or_else(PoisonError::into_inner);
        if now_ms > guard.0 {
            guard.0 = now_ms;
            guard.1 = 1;
        } else if guard.1 < 99_999_999_999 {
            guard.1 += 1;
        } else {
            guard.0 = guard.0.saturating_add(1);
            guard.1 = 1;
        }
        (guard.0, guard.1)
    };
    let suffix = node_decimal_suffix();
    format!("{ms:013}{counter:011}{suffix:02}")
}

/// Weak-reference registry of per-shard async mutexes so concurrent in-process
/// stream writers on the same shard serialize cleanly without CAS contention.
fn shard_local_mutex(shard_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    static SHARD_LOCKS: OnceLock<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>> =
        OnceLock::new();
    let map = SHARD_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(existing) = guard.get(shard_id).and_then(Weak::upgrade) {
        return existing;
    }
    if guard.len() > 2_048 {
        guard.retain(|_, weak| weak.strong_count() > 0);
    }
    let m = Arc::new(tokio::sync::Mutex::new(()));
    guard.insert(shard_id.to_owned(), Arc::downgrade(&m));
    m
}

fn view_includes_new(v: StreamViewType) -> bool {
    matches!(
        v,
        StreamViewType::NewImage | StreamViewType::NewAndOldImages
    )
}

fn view_includes_old(v: StreamViewType) -> bool {
    matches!(
        v,
        StreamViewType::OldImage | StreamViewType::NewAndOldImages
    )
}

/// Build a `StreamRecord` and its millisecond creation timestamp from a single
/// UTC clock read, honoring optional `StreamCapture` metadata (`view_type`,
/// `region`, and TTL `user_identity`) and computing `SizeBytes` via
/// `extenddb_core::types::item_size_bytes`.
#[must_use]
pub fn build_record_with_timestamp(
    spec: Option<&StreamSpecification>,
    capture: Option<&StreamCapture>,
    key_schema: &[KeySchemaElement],
    old: Option<&Item>,
    new: Option<&Item>,
    seq: &str,
    default_region: &str,
) -> Option<(StreamRecord, i64)> {
    let view = match (capture, spec) {
        (Some(cap), _) => cap.view_type,
        (None, Some(s)) if s.stream_enabled => s.stream_view_type?,
        _ => return None,
    };
    let event_name = match (old, new) {
        (None, Some(_)) => StreamEventName::Insert,
        (Some(o), Some(n)) => {
            if o == n {
                return None;
            }
            StreamEventName::Modify
        }
        (Some(_), None) => StreamEventName::Remove,
        (None, None) => return None,
    };
    let representative = new.or(old)?;
    let keys = extract_key(representative, key_schema);
    let new_image = if view_includes_new(view) {
        new.cloned()
    } else {
        None
    };
    let old_image = if view_includes_old(view) {
        old.cloned()
    } else {
        None
    };
    let size_bytes = i64::try_from(item_size_bytes(representative)).unwrap_or(i64::MAX);
    let (creation_ts, created_at_ms) = current_stream_clock();
    let region = capture.map_or_else(
        || default_region.to_owned(),
        |c| c.region.as_ref().to_owned(),
    );
    let user_identity = capture.and_then(|c| c.user_identity.clone());

    Some((
        StreamRecord {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_name,
            event_version: EVENT_VERSION.to_owned(),
            event_source: EVENT_SOURCE.to_owned(),
            aws_region: region,
            dynamodb: StreamRecordData {
                approximate_creation_date_time: creation_ts,
                keys,
                new_image,
                old_image,
                sequence_number: seq.to_owned(),
                size_bytes,
                stream_view_type: view,
            },
            user_identity,
        },
        created_at_ms,
    ))
}

/// Build a `StreamRecord` describing one item mutation, honoring optional
/// `StreamCapture` metadata (`view_type`, `region`, and TTL `user_identity`)
/// and computing `SizeBytes` via `extenddb_core::types::item_size_bytes`.
#[must_use]
pub fn build_record(
    spec: Option<&StreamSpecification>,
    capture: Option<&StreamCapture>,
    key_schema: &[KeySchemaElement],
    old: Option<&Item>,
    new: Option<&Item>,
    seq: &str,
    default_region: &str,
) -> Option<StreamRecord> {
    build_record_with_timestamp(spec, capture, key_schema, old, new, seq, default_region)
        .map(|(record, _)| record)
}

/// Persist a shard metadata row into `__extenddb_catalog__`.
///
/// # Errors
///
/// Returns [`StorageError`] if JSON serialization or the catalog write fails.
pub async fn put_shard(cat: &Catalog<'_>, shard: &ShardMeta) -> Result<(), StorageError> {
    let body = serde_json::to_value(shard)
        .map_err(|e| StorageError::Internal(format!("encode shard meta: {e}")))?;
    cat.put(&keys::stream_shard(&shard.shard_id), &body).await
}

/// Insert a shard metadata row only if it does not already exist.
///
/// # Errors
///
/// Returns [`StorageError`] if JSON serialization or the catalog CAS fails.
pub async fn put_shard_if_absent(
    cat: &Catalog<'_>,
    shard: &ShardMeta,
) -> Result<bool, StorageError> {
    let body = serde_json::to_value(shard)
        .map_err(|e| StorageError::Internal(format!("encode shard meta: {e}")))?;
    cat.put_if_absent(&keys::stream_shard(&shard.shard_id), &body)
        .await
}

/// Load all shard metadata rows belonging to `table_id`.
///
/// # Errors
///
/// Returns [`StorageError`] if the catalog prefix scan fails.
pub async fn load_table_shards(
    cat: &Catalog<'_>,
    table_id: &str,
) -> Result<Vec<ShardMeta>, StorageError> {
    let rows = cat
        .scan_prefix(&keys::stream_shard_table_prefix(table_id))
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for (_, v) in rows {
        if let Ok(meta) = serde_json::from_value::<ShardMeta>(v)
            && meta.table_id == table_id
        {
            out.push(meta);
        }
    }
    Ok(out)
}

/// Atomically verify `ShardMeta` has not been sealed strictly below `committed_seq`
/// and advance `next_seq_num` / `last_sequence_number` via CAS.
///
/// Returns `Ok(false)` when `ending_sequence_number` is set strictly below
/// `committed_seq` (or the shard metadata row was removed), allowing
/// `write_record_ordered_at` to delete any unexposed post-close slot record and
/// return `Ok(None)`, and preventing `latest_sequence_for_shard` or retention
/// pruning from advancing `ShardMeta.last_sequence_number` past
/// `ending_sequence_number`.
async fn advance_shard_cursor_if_open(
    cat: &Catalog<'_>,
    shard_id: &str,
    min_next_slot: u64,
    committed_seq: &str,
) -> Result<bool, StorageError> {
    let rk = keys::stream_shard(shard_id);
    cat.mutate_json_cas(&rk, std::convert::identity, |existing| {
        let Some(val) = existing else {
            return Ok((CatalogJsonAction::NoOp, false));
        };
        let mut meta: ShardMeta = serde_json::from_value(val)
            .map_err(|e| StorageError::Internal(format!("decode shard meta {shard_id}: {e}")))?;
        if meta
            .ending_sequence_number
            .as_deref()
            .is_some_and(|end_seq| committed_seq > end_seq)
        {
            return Ok((CatalogJsonAction::NoOp, false));
        }
        let mut changed = false;
        if meta.next_seq_num < min_next_slot {
            meta.next_seq_num = min_next_slot;
            changed = true;
        }
        if meta
            .last_sequence_number
            .as_deref()
            .is_none_or(|prev| committed_seq > prev)
        {
            meta.last_sequence_number = Some(committed_seq.to_owned());
            changed = true;
        }
        if !changed {
            return Ok((CatalogJsonAction::NoOp, true));
        }
        let encoded = serde_json::to_value(&meta)
            .map_err(|e| StorageError::Internal(format!("encode shard meta: {e}")))?;
        Ok((CatalogJsonAction::Put(encoded), true))
    })
    .await
}

/// Ensure `ShardMeta.next_seq_num` is strictly past a `stream_record:<shard_id>:<seq>`
/// row BEFORE 24-hour retention pruning deletes the record, without advancing
/// a closed shard past its `ending_sequence_number`.
///
/// # Errors
///
/// Returns [`StorageError`] if advancing the shard cursor fails.
pub async fn ensure_shard_past_record(
    cat: &Catalog<'_>,
    record_row_key: &str,
) -> Result<(), StorageError> {
    let Some(rest) = record_row_key.strip_prefix("stream_record:") else {
        return Ok(());
    };
    let Some((shard_id, seq)) = rest.rsplit_once(':') else {
        return Ok(());
    };
    if let Some(slot) = parse_slot_from_sequence(seq) {
        let _ = advance_shard_cursor_if_open(cat, shard_id, slot.saturating_add(1), seq).await?;
    }
    Ok(())
}

/// Close each open shard in `shard_ids` atomically via `Catalog::mutate_json_cas`,
/// serializing against in-process writers via `shard_local_mutex` and preserving
/// concurrent `next_seq_num` / `last_sequence_number` advances.
///
/// # Errors
///
/// Returns [`StorageError`] if reading the shard tail or persisting the closed `ShardMeta` fails.
pub async fn close_shards_by_id(
    cat: &Catalog<'_>,
    shard_ids: &[String],
) -> Result<(), StorageError> {
    for sid in shard_ids {
        let local_lock = shard_local_mutex(sid);
        let _guard = local_lock.lock().await;
        let fallback_latest = latest_sequence_for_shard(cat, sid).await?;
        let rk = keys::stream_shard(sid);
        cat.mutate_json_cas(&rk, std::convert::identity, |existing| {
            let Some(val) = existing else {
                return Ok((CatalogJsonAction::NoOp, ()));
            };
            let mut meta: ShardMeta = serde_json::from_value(val)
                .map_err(|e| StorageError::Internal(format!("decode shard meta {sid}: {e}")))?;
            if meta.ending_sequence_number.is_some() {
                return Ok((CatalogJsonAction::NoOp, ()));
            }
            let computed_latest = if meta.next_seq_num > 1 {
                Some(slot_sequence_number(sid, meta.next_seq_num - 1))
            } else {
                None
            };
            let end_seq = [
                meta.last_sequence_number.as_deref(),
                computed_latest.as_deref(),
                fallback_latest.as_deref(),
            ]
            .into_iter()
            .flatten()
            .max()
            .map_or_else(|| meta.starting_sequence_number.clone(), str::to_owned);
            meta.ending_sequence_number = Some(end_seq);
            let encoded = serde_json::to_value(&meta)
                .map_err(|e| StorageError::Internal(format!("encode shard meta: {e}")))?;
            Ok((CatalogJsonAction::Put(encoded), ()))
        })
        .await?;
    }
    Ok(())
}

async fn read_dedup_seq(cat: &Catalog<'_>, rk: &str) -> Result<Option<String>, StorageError> {
    let Some(val) = cat.get(rk).await? else {
        return Ok(None);
    };
    Ok(val
        .get("sequence_number")
        .or_else(|| val.get("seq"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned))
}

async fn persist_dedup_marker(cat: &Catalog<'_>, rk: &str, seq: &str, created_at: i64) {
    let _ = cat
        .put_if_absent(
            rk,
            &json!({
                "sequence_number": seq,
                "created_at": created_at,
            }),
        )
        .await;
}

/// Append a stream record into the next contiguous per-shard log slot with
/// exact-once cross-node deduplication and an optional explicit millisecond
/// creation timestamp.
///
/// - Never falls back to a clock-derived sequence number: `ShardMeta` is always
///   persisted BEFORE `latest_stream_label` becomes visible on `table_meta`.
/// - Returns `Ok(None)` without writing when `ShardMeta.ending_sequence_number`
///   is already set, so a closed shard never receives records past its ending
///   sequence number.
/// - When `created_at_ms` is `None` (direct `StreamEngine::write_stream_record`
///   callers), derives milliseconds from
///   `record.dynamodb.approximate_creation_date_time * 1_000` without taking a
///   second clock read.
///
/// # Errors
///
/// Returns [`StorageError`] if catalog reads/CAS writes fail or contention exceeds the retry cap.
// Justification: dedup check, contiguous slot claiming, post-close sealing check, and cursor CAS advance form one atomic append loop.
#[allow(clippy::too_many_lines)]
pub async fn write_record_ordered_at(
    cat: &Catalog<'_>,
    shard_id: &str,
    record: &StreamRecord,
    created_at_ms: Option<i64>,
    dedup_key: Option<&str>,
) -> Result<Option<String>, StorageError> {
    let local_lock = shard_local_mutex(shard_id);
    let _guard = local_lock.lock().await;

    let effective_created_at_ms = created_at_ms.unwrap_or_else(|| {
        record
            .dynamodb
            .approximate_creation_date_time
            .saturating_mul(1_000)
    });
    let event_dedup_rk =
        (!record.event_id.is_empty()).then(|| keys::stream_dedup(shard_id, &record.event_id));
    let extra_dedup_rk = dedup_key
        .filter(|dk| !dk.is_empty() && Some(*dk) != Some(record.event_id.as_str()))
        .map(|dk| keys::stream_dedup(shard_id, dk));
    let shard_rk = keys::stream_shard(shard_id);

    for attempt in 0..256u32 {
        // 1. Read `ShardMeta` FIRST.
        let Some(shard_val) = cat.get(&shard_rk).await? else {
            return Err(StorageError::Internal(format!(
                "stream shard '{shard_id}' metadata row is missing"
            )));
        };
        let meta: ShardMeta = serde_json::from_value(shard_val)
            .map_err(|e| StorageError::Internal(format!("decode shard meta {shard_id}: {e}")))?;

        // 2. Check deduplication markers SECOND (after reading `ShardMeta`).
        for drk in [event_dedup_rk.as_deref(), extra_dedup_rk.as_deref()]
            .into_iter()
            .flatten()
        {
            if let Some(existing_seq) = read_dedup_seq(cat, drk).await? {
                if let Some(slot) = parse_slot_from_sequence(&existing_seq)
                    && !advance_shard_cursor_if_open(
                        cat,
                        shard_id,
                        slot.saturating_add(1),
                        &existing_seq,
                    )
                    .await?
                {
                    let _ = cat
                        .delete(&keys::stream_record(shard_id, &existing_seq))
                        .await;
                    for clean_drk in [event_dedup_rk.as_deref(), extra_dedup_rk.as_deref()]
                        .into_iter()
                        .flatten()
                    {
                        let _ = cat.delete(clean_drk).await;
                    }
                    return Ok(None);
                }
                return Ok(Some(existing_seq));
            }
        }

        // 3. Refuse appending to a closed shard (`ending_sequence_number` set).
        if meta.ending_sequence_number.is_some() {
            return Ok(None);
        }

        let cand_slot = meta.next_seq_num.max(1);
        let cand_seq = slot_sequence_number(shard_id, cand_slot);
        let rec_rk = keys::stream_record(shard_id, &cand_seq);
        let mut candidate = record.clone();
        candidate.dynamodb.sequence_number.clone_from(&cand_seq);
        let mut body = serde_json::to_value(&candidate)
            .map_err(|e| StorageError::Internal(format!("encode stream record: {e}")))?;
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                CREATED_AT_MS_FIELD.to_owned(),
                serde_json::Value::from(effective_created_at_ms),
            );
        }

        if cat.put_if_absent(&rec_rk, &body).await? {
            // Persist dedup markers BEFORE advancing `ShardMeta.next_seq_num` so a
            // crash mid-append never loses deduplication on recovery, then atomically
            // verify via CAS on `ShardMeta` that the shard was not sealed below `cand_seq`.
            for drk in [event_dedup_rk.as_deref(), extra_dedup_rk.as_deref()]
                .into_iter()
                .flatten()
            {
                persist_dedup_marker(
                    cat,
                    drk,
                    &cand_seq,
                    candidate.dynamodb.approximate_creation_date_time,
                )
                .await;
            }
            if !advance_shard_cursor_if_open(cat, shard_id, cand_slot.saturating_add(1), &cand_seq)
                .await?
            {
                let _ = cat.delete(&rec_rk).await;
                for drk in [event_dedup_rk.as_deref(), extra_dedup_rk.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    let _ = cat.delete(drk).await;
                }
                return Ok(None);
            }
            return Ok(Some(cand_seq));
        }

        // Another writer claimed `cand_slot` (or an idempotent `put_if_absent` retry
        // observed our own slot). Persist that winner's dedup marker BEFORE advancing
        // `ShardMeta.next_seq_num`, then verify via CAS that the shard remains open.
        let winner_opt = cat
            .get(&rec_rk)
            .await?
            .and_then(|v| serde_json::from_value::<StreamRecord>(v).ok())
            .filter(|w| !w.event_id.is_empty());
        if let Some(ref winner_rec) = winner_opt {
            let winner_drk = keys::stream_dedup(shard_id, &winner_rec.event_id);
            persist_dedup_marker(
                cat,
                &winner_drk,
                &cand_seq,
                winner_rec.dynamodb.approximate_creation_date_time,
            )
            .await;
            if winner_rec.event_id == record.event_id
                && let Some(ref extra_drk) = extra_dedup_rk
            {
                persist_dedup_marker(
                    cat,
                    extra_drk,
                    &cand_seq,
                    winner_rec.dynamodb.approximate_creation_date_time,
                )
                .await;
            }
        }

        if !advance_shard_cursor_if_open(cat, shard_id, cand_slot.saturating_add(1), &cand_seq)
            .await?
        {
            let _ = cat.delete(&rec_rk).await;
            if let Some(ref winner_rec) = winner_opt {
                let _ = cat
                    .delete(&keys::stream_dedup(shard_id, &winner_rec.event_id))
                    .await;
                if winner_rec.event_id == record.event_id {
                    for drk in [event_dedup_rk.as_deref(), extra_dedup_rk.as_deref()]
                        .into_iter()
                        .flatten()
                    {
                        let _ = cat.delete(drk).await;
                    }
                }
            }
            return Ok(None);
        }

        if let Some(winner_rec) = winner_opt
            && winner_rec.event_id == record.event_id
        {
            return Ok(Some(cand_seq));
        }

        if attempt > 8 {
            let jitter_ms = rand::random_range(1..=4u64);
            tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
        }
    }

    Err(StorageError::Transient(format!(
        "stream shard '{shard_id}' append contention limit exceeded"
    )))
}

/// Append a stream record into the next contiguous per-shard log slot with
/// exact-once cross-node deduplication, deriving `created_at_ms` from
/// `record.dynamodb.approximate_creation_date_time * 1_000`.
///
/// # Errors
///
/// Returns [`StorageError`] if catalog reads/CAS writes fail or contention exceeds the retry cap.
pub async fn write_record_ordered(
    cat: &Catalog<'_>,
    shard_id: &str,
    record: &StreamRecord,
    dedup_key: Option<&str>,
) -> Result<Option<String>, StorageError> {
    write_record_ordered_at(cat, shard_id, record, None, dedup_key).await
}

/// Append a stream record idempotently via contiguous shard slot assignment.
///
/// # Errors
///
/// Returns [`StorageError`] if writing the ordered stream record fails.
pub async fn write_record(
    cat: &Catalog<'_>,
    shard_id: &str,
    record: &StreamRecord,
) -> Result<(), StorageError> {
    write_record_ordered(cat, shard_id, record, None)
        .await
        .map(|_| ())
}

/// Return the highest 26-digit sequence number recorded in `shard_id`, or `None`,
/// using `ShardMeta` plus bounded tail-slot point reads rather than scanning
/// the shard's 24-hour record history.
///
/// # Errors
///
/// Returns [`StorageError`] if reading or decoding `ShardMeta` or tail records fails.
pub async fn latest_sequence_for_shard(
    cat: &Catalog<'_>,
    shard_id: &str,
) -> Result<Option<String>, StorageError> {
    let Some(val) = cat.get(&keys::stream_shard(shard_id)).await? else {
        return Ok(None);
    };
    let meta: ShardMeta = serde_json::from_value(val)
        .map_err(|e| StorageError::Internal(format!("decode shard meta {shard_id}: {e}")))?;

    let mut best: Option<String> = meta.last_sequence_number;
    if best.is_none() && meta.next_seq_num > 1 {
        best = Some(slot_sequence_number(shard_id, meta.next_seq_num - 1));
    }
    if let (Some(b), Some(end_seq)) = (best.as_deref(), meta.ending_sequence_number.as_deref())
        && b > end_seq
    {
        best = Some(end_seq.to_owned());
    }

    // Probe at `next_seq_num` in case a writer persisted `stream_record` right
    // before crashing prior to advancing `ShardMeta.next_seq_num`. Never probe
    // or advance past a closed shard's `ending_sequence_number`.
    let mut probe_slot = meta.next_seq_num.max(1);
    for _ in 0..64 {
        let probe_seq = slot_sequence_number(shard_id, probe_slot);
        if meta
            .ending_sequence_number
            .as_deref()
            .is_some_and(|end_seq| probe_seq.as_str() > end_seq)
        {
            break;
        }
        let rk = keys::stream_record(shard_id, &probe_seq);
        if cat.get(&rk).await?.is_some() {
            if !advance_shard_cursor_if_open(
                cat,
                shard_id,
                probe_slot.saturating_add(1),
                &probe_seq,
            )
            .await?
            {
                break;
            }
            best = Some(probe_seq);
            probe_slot = probe_slot.saturating_add(1);
        } else {
            break;
        }
    }

    Ok(best)
}

/// Return the lowest 26-digit sequence number recorded in `shard_id` whose
/// stored creation time (`created_at_ms`, or `ApproximateCreationDateTime * 1_000`
/// for legacy rows) is at or after `at_ms` (Unix epoch milliseconds), or `None`
/// when the shard is absent or holds no qualifying record.
///
/// Pages `stream_record:<shard_id>:*` in ascending sequence-number order,
/// bounded by `ShardMeta.ending_sequence_number` when the shard is closed, so
/// the first matching record is the minimum qualifying sequence number even
/// under non-monotonic clocks.
///
/// # Errors
///
/// Returns [`StorageError`] if reading or decoding `ShardMeta` or scanning
/// stream records fails.
pub async fn first_sequence_number_at_or_after(
    cat: &Catalog<'_>,
    shard_id: &str,
    at_ms: i64,
) -> Result<Option<String>, StorageError> {
    first_sequence_number_at_or_after_paged(cat, shard_id, at_ms, AT_TIMESTAMP_SCAN_PAGE_SIZE).await
}

/// Paged implementation of [`first_sequence_number_at_or_after`] with an
/// explicit Bigtable `rows_limit` per scan page.
///
/// # Errors
///
/// Returns [`StorageError`] if reading or decoding `ShardMeta` or scanning
/// stream records fails.
pub async fn first_sequence_number_at_or_after_paged(
    cat: &Catalog<'_>,
    shard_id: &str,
    at_ms: i64,
    page_size: i64,
) -> Result<Option<String>, StorageError> {
    let Some(val) = cat.get(&keys::stream_shard(shard_id)).await? else {
        return Ok(None);
    };
    let meta: ShardMeta = serde_json::from_value(val)
        .map_err(|e| StorageError::Internal(format!("decode shard meta {shard_id}: {e}")))?;

    let prefix = keys::stream_record_shard_prefix(shard_id);
    let end_open = match meta.ending_sequence_number.as_deref() {
        Some(end_seq) => format!("{prefix}{end_seq}\x00"),
        None => format!("stream_record:{shard_id};"),
    };
    let page_limit = page_size.max(1);
    let mut start_key = prefix;
    let mut inclusive = true;

    loop {
        let rows = cat
            .scan_str_range(&start_key, inclusive, &end_open, page_limit)
            .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let batch_len = rows.len();
        let mut last_key = None;
        for (rk, value) in rows {
            last_key = Some(rk);
            let Some((record, rec_ms)) = decode_stored_record(value) else {
                continue;
            };
            if meta
                .ending_sequence_number
                .as_deref()
                .is_some_and(|end_seq| record.dynamodb.sequence_number.as_str() > end_seq)
            {
                return Ok(None);
            }
            if rec_ms >= at_ms {
                return Ok(Some(record.dynamodb.sequence_number));
            }
        }
        if i64::try_from(batch_len).unwrap_or(i64::MAX) < page_limit {
            return Ok(None);
        }
        if let Some(last) = last_key {
            start_key = last;
            inclusive = false;
        } else {
            return Ok(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{AttributeValue, KeyType, UserIdentity};

    #[test]
    fn sequence_numbers_are_fixed_width_monotonic_decimal_u128() {
        let mut prev = String::new();
        for _ in 0..200 {
            let seq = next_sequence_number();
            assert_eq!(seq.len(), SEQUENCE_NUMBER_WIDTH);
            assert!(
                seq.bytes().all(|b| b.is_ascii_digit()),
                "sequence number must be pure decimal digits: {seq}"
            );
            assert!(
                seq.parse::<u128>().is_ok(),
                "sequence number must parse as u128: {seq}"
            );
            assert!(seq > prev, "expected {seq} > {prev}");
            prev = seq;
        }
    }

    #[test]
    fn slot_sequence_numbers_are_contiguous_monotonic_and_roundtrip_slot() {
        let shard_id = "shardId-0123456789abcdef-1700000000123002";
        let mut prev = String::new();
        for slot in 1..=50u64 {
            let seq = slot_sequence_number(shard_id, slot);
            assert_eq!(seq.len(), SEQUENCE_NUMBER_WIDTH);
            assert!(seq.bytes().all(|b| b.is_ascii_digit()));
            assert!(!seq.starts_with('0'));
            assert!(seq.parse::<u128>().is_ok());
            assert_eq!(parse_slot_from_sequence(&seq), Some(slot));
            assert!(seq > prev);
            prev = seq;
        }
    }

    #[test]
    fn build_record_uses_dynamodb_item_size_bytes_and_propagates_ttl_identity() {
        let ks = vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }];
        let mut old = Item::new();
        old.insert("pk".into(), AttributeValue::S("k1".into()));
        old.insert("expires_at".into(), AttributeValue::N("100".into()));

        let capture = StreamCapture {
            view_type: StreamViewType::OldImage,
            user_identity: Some(UserIdentity {
                principal_id: "dynamodb.amazonaws.com".into(),
                identity_type: "Service".into(),
            }),
            region: Arc::from("us-west-2"),
        };
        let seq = next_sequence_number();
        let rec = build_record(
            None,
            Some(&capture),
            &ks,
            Some(&old),
            None,
            &seq,
            "us-east-1",
        )
        .unwrap();
        assert_eq!(rec.event_name, StreamEventName::Remove);
        assert_eq!(rec.aws_region, "us-west-2");
        assert!(rec.dynamodb.old_image.is_some());
        assert!(rec.dynamodb.new_image.is_none());
        assert_eq!(
            rec.dynamodb.size_bytes,
            i64::try_from(item_size_bytes(&old)).unwrap()
        );
        let uid = rec.user_identity.expect("user_identity");
        assert_eq!(uid.principal_id, "dynamodb.amazonaws.com");
        assert_eq!(uid.identity_type, "Service");
    }

    #[test]
    fn current_stream_clock_returns_consistent_secs_and_millis() {
        let (secs, ms) = current_stream_clock();
        assert!(secs > 0);
        assert_eq!(ms.div_euclid(1_000), secs);
        let sub_ms = ms.rem_euclid(1_000);
        assert!((0..1_000).contains(&sub_ms));
    }

    #[test]
    fn build_record_with_timestamp_shares_clock_read_with_approximate_creation_date_time() {
        let ks = vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }];
        let mut item = Item::new();
        item.insert("pk".into(), AttributeValue::S("k1".into()));
        let spec = StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewImage),
        };
        let seq = next_sequence_number();
        let (rec, created_ms) = build_record_with_timestamp(
            Some(&spec),
            None,
            &ks,
            None,
            Some(&item),
            &seq,
            "us-east-1",
        )
        .expect("record");
        assert_eq!(
            created_ms.div_euclid(1_000),
            rec.dynamodb.approximate_creation_date_time,
            "created_at_ms and ApproximateCreationDateTime must come from the same clock read"
        );
    }

    #[test]
    fn decode_stored_record_preserves_created_at_ms_falls_back_and_omits_wire_field() {
        let ks = vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }];
        let mut item = Item::new();
        item.insert("pk".into(), AttributeValue::S("k1".into()));
        let spec = StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewImage),
        };
        let mut rec = build_record(
            Some(&spec),
            None,
            &ks,
            None,
            Some(&item),
            "17000000000000000000000100",
            "us-east-1",
        )
        .expect("record");
        rec.dynamodb.approximate_creation_date_time = 1_700_000_042;

        // Legacy row JSON without `created_at_ms` falls back to whole seconds * 1_000.
        let legacy_val = serde_json::to_value(&rec).expect("encode legacy");
        let (decoded_legacy, legacy_ms) =
            decode_stored_record(legacy_val.clone()).expect("decode legacy");
        assert_eq!(decoded_legacy.event_id, rec.event_id);
        assert_eq!(legacy_ms, 1_700_000_042_000);

        // Modern row carrying `created_at_ms` preserves sub-second precision while
        // wire `StreamRecord` serialization never emits the internal field.
        let mut with_ms = legacy_val;
        with_ms.as_object_mut().expect("json object").insert(
            CREATED_AT_MS_FIELD.to_owned(),
            serde_json::Value::from(1_700_000_042_789i64),
        );
        let (decoded_modern, modern_ms) = decode_stored_record(with_ms).expect("decode modern");
        assert_eq!(decoded_modern.event_id, rec.event_id);
        assert_eq!(modern_ms, 1_700_000_042_789);
        let reencoded = serde_json::to_value(&decoded_modern).expect("reencode");
        assert!(
            reencoded.get(CREATED_AT_MS_FIELD).is_none(),
            "internal created_at_ms field must not leak into wire StreamRecord"
        );

        // Non-record JSON returns None cleanly.
        assert!(decode_stored_record(json!({"created_at_ms": 123})).is_none());
    }
}
