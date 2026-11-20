// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB Streams support for the Cloud Bigtable backend.
//!
//! Stream shard lineage, deduplication markers, and records live in `__extenddb_catalog__`:
//! - `stream_shard:<shard_id>` — one `ShardMeta` row per labelled stream shard
//!   (`shardId-{table_id}-{label_millis:013}{view_digit:1}{index:02}`), carrying
//!   contiguous `next_seq_num` (`1, 2, ...`) and `last_sequence_number`.
//! - `stream_record:<shard_id>:<seq_26>` — one `StreamRecord` row per contiguous
//!   shard log slot (`slot_sequence_number(shard_id, slot)`), claimed via atomic
//!   `Catalog::put_if_absent` so `seq_2 > seq_1` can never become visible on a
//!   shard ahead of `seq_1` (`RFC-0003` §5.2, `BUG-04`).
//! - `stream_dedup:<shard_id>:<dedup_key>` — exact-once publication marker ensuring
//!   concurrent or retried `roll_forward` / `sweeper` passes never emit duplicate
//!   stream records (`BUG-04`, `BUG-05`).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, StreamEventName, StreamRecord, StreamRecordData,
    StreamSpecification, StreamViewType,
};
use extenddb_storage::StreamCapture;
use extenddb_storage::stream_lineage::ShardRow;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::catalog::keys;
use crate::catalog::{Catalog, CatalogJsonAction};

/// Fixed width of Bigtable stream sequence numbers in decimal digits.
pub const SEQUENCE_NUMBER_WIDTH: usize = 26;

/// Placeholder sequence number stored during Phase 1 before Phase 2 assigns a
/// contiguous per-shard log slot (`BUG-04`).
pub const UNASSIGNED_SEQUENCE_NUMBER: &str = "00000000000000000000000000";

const EVENT_SOURCE: &str = "aws:dynamodb";
const EVENT_VERSION: &str = "1.1";

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
    let fallback_idx = (crc32fast::hash(shard_id.as_bytes()) % 100) as u8;
    (1_000_000_000_000, fallback_idx)
}

/// Deterministic 26-digit sequence number for `(shard_id, slot)` (`slot >= 1`):
/// `{label_millis:013}{slot:011}{shard_idx:02}` (`BUG-04`).
///
/// Because `label_millis` and `shard_idx` are fixed per `shard_id`, slot `k`
/// maps bijectively to one exact `stream_record:<shard_id>:<seq_26>` key, so
/// `Catalog::put_if_absent` fills slots `1, 2, 3, ...` contiguously without gaps.
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
/// (`{ms:013}{counter:011}{node:02}`).
#[must_use]
pub fn next_sequence_number() -> String {
    static SEQ_STATE: Mutex<(u64, u64)> = Mutex::new((0, 0));

    let now_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).max(1) as u64;
    let (ms, counter) = {
        let mut guard = SEQ_STATE.lock().unwrap_or_else(|e| e.into_inner());
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
/// stream writers on the same shard serialize cleanly without CAS contention (`BUG-04`).
fn shard_local_mutex(shard_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    static SHARD_LOCKS: OnceLock<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>> =
        OnceLock::new();
    let map = SHARD_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().unwrap_or_else(|e| e.into_inner());
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

fn extract_keys(item: &Item, key_schema: &[KeySchemaElement]) -> BTreeMap<String, AttributeValue> {
    let mut keys = BTreeMap::new();
    for ks in key_schema {
        if let Some(v) = item.get(&ks.attribute_name) {
            keys.insert(ks.attribute_name.clone(), v.clone());
        }
    }
    keys
}

/// Build a `StreamRecord` describing one item mutation, honoring optional
/// `StreamCapture` metadata (`view_type`, `region`, and TTL `user_identity`).
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
    let keys = extract_keys(representative, key_schema);
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
    let size_bytes = serde_json::to_string(&keys).map(|s| s.len()).unwrap_or(0)
        + new_image
            .as_ref()
            .and_then(|n| serde_json::to_string(n).ok().map(|s| s.len()))
            .unwrap_or(0);
    let creation_ts = time::OffsetDateTime::now_utc().unix_timestamp();
    let region = capture
        .map(|c| c.region.as_ref().to_owned())
        .unwrap_or_else(|| default_region.to_owned());
    let user_identity = capture.and_then(|c| c.user_identity.clone());

    Some(StreamRecord {
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
            size_bytes: size_bytes as i64,
            stream_view_type: view,
        },
        user_identity,
    })
}

/// Persist a shard metadata row into `__extenddb_catalog__`.
pub async fn put_shard(cat: &Catalog<'_>, shard: &ShardMeta) -> Result<(), String> {
    let body = serde_json::to_value(shard).map_err(|e| format!("encode shard meta: {e}"))?;
    cat.put(&keys::stream_shard(&shard.shard_id), &body).await
}

/// Insert a shard metadata row only if it does not already exist (`BUG-12`).
pub async fn put_shard_if_absent(cat: &Catalog<'_>, shard: &ShardMeta) -> Result<bool, String> {
    let body = serde_json::to_value(shard).map_err(|e| format!("encode shard meta: {e}"))?;
    cat.put_if_absent(&keys::stream_shard(&shard.shard_id), &body)
        .await
}

/// Load all shard metadata rows belonging to `table_id`.
pub async fn load_table_shards(
    cat: &Catalog<'_>,
    table_id: &str,
) -> Result<Vec<ShardMeta>, String> {
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

/// Advance `ShardMeta.next_seq_num` and `last_sequence_number` monotonically via CAS.
async fn advance_shard_cursor_at_least(
    cat: &Catalog<'_>,
    shard_id: &str,
    min_next_slot: u64,
    committed_seq: &str,
) -> Result<(), String> {
    let rk = keys::stream_shard(shard_id);
    cat.mutate_json_cas(&rk, std::convert::identity, |existing| {
        let Some(val) = existing else {
            return Ok((CatalogJsonAction::NoOp, ()));
        };
        let mut meta: ShardMeta = serde_json::from_value(val)
            .map_err(|e| format!("decode shard meta {shard_id}: {e}"))?;
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
            return Ok((CatalogJsonAction::NoOp, ()));
        }
        let encoded = serde_json::to_value(&meta).map_err(|e| format!("encode shard meta: {e}"))?;
        Ok((CatalogJsonAction::Put(encoded), ()))
    })
    .await
}

/// Ensure `ShardMeta.next_seq_num` is strictly past a `stream_record:<shard_id>:<seq>`
/// row BEFORE 24-hour retention pruning deletes the record, guaranteeing pruned
/// sequence slots can never be reused even after an un-healed crash.
pub async fn ensure_shard_past_record(
    cat: &Catalog<'_>,
    record_row_key: &str,
) -> Result<(), String> {
    let Some(rest) = record_row_key.strip_prefix("stream_record:") else {
        return Ok(());
    };
    let Some((shard_id, seq)) = rest.rsplit_once(':') else {
        return Ok(());
    };
    if let Some(slot) = parse_slot_from_sequence(seq) {
        advance_shard_cursor_at_least(cat, shard_id, slot.saturating_add(1), seq).await?;
    }
    Ok(())
}

/// Close each open shard in `shard_ids` atomically via `Catalog::mutate_json_cas`
/// (`BUG-12`, `BUG-13`), preserving concurrent `next_seq_num` / `last_sequence_number` advances.
pub async fn close_shards_by_id(cat: &Catalog<'_>, shard_ids: &[String]) -> Result<(), String> {
    for sid in shard_ids {
        let fallback_latest = latest_sequence_for_shard(cat, sid).await?;
        let rk = keys::stream_shard(sid);
        cat.mutate_json_cas(&rk, std::convert::identity, |existing| {
            let Some(val) = existing else {
                return Ok((CatalogJsonAction::NoOp, ()));
            };
            let mut meta: ShardMeta =
                serde_json::from_value(val).map_err(|e| format!("decode shard meta {sid}: {e}"))?;
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
            .map(str::to_owned)
            .unwrap_or_else(|| meta.starting_sequence_number.clone());
            meta.ending_sequence_number = Some(end_seq);
            let encoded =
                serde_json::to_value(&meta).map_err(|e| format!("encode shard meta: {e}"))?;
            Ok((CatalogJsonAction::Put(encoded), ()))
        })
        .await?;
    }
    Ok(())
}

async fn read_dedup_seq(cat: &Catalog<'_>, rk: &str) -> Result<Option<String>, String> {
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
/// exact-once cross-node deduplication (`BUG-04`, `BUG-05`, `RFC-0003` §5.2).
///
/// Protocol invariant preventing cross-node duplicate slots:
/// 1. In each loop iteration, read `ShardMeta` FIRST, and THEN check `stream_dedup`
///    SECOND before attempting `put_if_absent` on `cand_slot`.
/// 2. Whenever any writer (either the slot winner or a colliding helper) advances
///    `ShardMeta.next_seq_num` past `cand_slot`, it MUST persist the slot winner's
///    `stream_dedup:<shard_id>:<winner_event_id>` marker BEFORE advancing `ShardMeta`.
///    Consequently, any concurrent caller racing for the same event that reads
///    `ShardMeta.next_seq_num >= cand_slot + 1` is guaranteed to observe the
///    durable `stream_dedup` marker in step (1) and return idempotently without
///    allocating a duplicate slot!
pub async fn write_record_ordered(
    cat: &Catalog<'_>,
    shard_id: &str,
    record: &StreamRecord,
    dedup_key: Option<&str>,
) -> Result<String, String> {
    let local_lock = shard_local_mutex(shard_id);
    let _guard = local_lock.lock().await;

    let event_dedup_rk =
        (!record.event_id.is_empty()).then(|| keys::stream_dedup(shard_id, &record.event_id));
    let extra_dedup_rk = dedup_key
        .filter(|dk| !dk.is_empty() && Some(*dk) != Some(record.event_id.as_str()))
        .map(|dk| keys::stream_dedup(shard_id, dk));
    let shard_rk = keys::stream_shard(shard_id);

    for attempt in 0..256u32 {
        // 1. Read `ShardMeta.next_seq_num` FIRST.
        let shard_opt = cat.get(&shard_rk).await?;

        // 2. Check deduplication markers SECOND (after reading `ShardMeta`).
        for drk in [event_dedup_rk.as_deref(), extra_dedup_rk.as_deref()]
            .into_iter()
            .flatten()
        {
            if let Some(existing_seq) = read_dedup_seq(cat, drk).await? {
                if let Some(slot) = parse_slot_from_sequence(&existing_seq) {
                    let _ = advance_shard_cursor_at_least(
                        cat,
                        shard_id,
                        slot.saturating_add(1),
                        &existing_seq,
                    )
                    .await;
                }
                return Ok(existing_seq);
            }
        }

        let cand_slot = match shard_opt {
            Some(val) => serde_json::from_value::<ShardMeta>(val)
                .map(|m| m.next_seq_num.max(1))
                .unwrap_or(1),
            None => {
                let fallback_seq = if record.dynamodb.sequence_number.is_empty()
                    || record.dynamodb.sequence_number == UNASSIGNED_SEQUENCE_NUMBER
                {
                    next_sequence_number()
                } else {
                    record.dynamodb.sequence_number.clone()
                };
                let mut rec = record.clone();
                rec.dynamodb.sequence_number = fallback_seq.clone();
                let rk = keys::stream_record(shard_id, &fallback_seq);
                let body = serde_json::to_value(&rec).map_err(|e| format!("encode record: {e}"))?;
                cat.put(&rk, &body).await?;
                for drk in [event_dedup_rk.as_deref(), extra_dedup_rk.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    persist_dedup_marker(
                        cat,
                        drk,
                        &fallback_seq,
                        rec.dynamodb.approximate_creation_date_time,
                    )
                    .await;
                }
                return Ok(fallback_seq);
            }
        };

        let cand_seq = slot_sequence_number(shard_id, cand_slot);
        let rec_rk = keys::stream_record(shard_id, &cand_seq);
        let mut candidate = record.clone();
        candidate.dynamodb.sequence_number = cand_seq.clone();
        let body = serde_json::to_value(&candidate).map_err(|e| format!("encode record: {e}"))?;

        if cat.put_if_absent(&rec_rk, &body).await? {
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
            let _ = advance_shard_cursor_at_least(
                cat,
                shard_id,
                cand_slot.saturating_add(1),
                &cand_seq,
            )
            .await;
            return Ok(cand_seq);
        }

        // Another writer claimed `cand_slot`. Help persist that winner's `event_id`
        // dedup marker BEFORE advancing `ShardMeta.next_seq_num` to `cand_slot + 1`.
        if let Some(winner_val) = cat.get(&rec_rk).await?
            && let Ok(winner_rec) = serde_json::from_value::<StreamRecord>(winner_val)
            && !winner_rec.event_id.is_empty()
        {
            let winner_drk = keys::stream_dedup(shard_id, &winner_rec.event_id);
            persist_dedup_marker(
                cat,
                &winner_drk,
                &cand_seq,
                winner_rec.dynamodb.approximate_creation_date_time,
            )
            .await;
            if winner_rec.event_id == record.event_id {
                if let Some(ref extra_drk) = extra_dedup_rk {
                    persist_dedup_marker(
                        cat,
                        extra_drk,
                        &cand_seq,
                        winner_rec.dynamodb.approximate_creation_date_time,
                    )
                    .await;
                }
                let _ = advance_shard_cursor_at_least(
                    cat,
                    shard_id,
                    cand_slot.saturating_add(1),
                    &cand_seq,
                )
                .await;
                return Ok(cand_seq);
            }
        }

        let _ =
            advance_shard_cursor_at_least(cat, shard_id, cand_slot.saturating_add(1), &cand_seq)
                .await;

        if attempt > 8 {
            let jitter_ms = rand::random_range(1..=4u64);
            tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
        }
    }

    Err(format!(
        "stream shard '{shard_id}' append contention limit exceeded"
    ))
}

/// Append a stream record idempotently via contiguous shard slot assignment.
pub async fn write_record(
    cat: &Catalog<'_>,
    shard_id: &str,
    record: &StreamRecord,
) -> Result<(), String> {
    write_record_ordered(cat, shard_id, record, None)
        .await
        .map(|_| ())
}

/// Return the highest 26-digit sequence number recorded in `shard_id`, or `None`.
pub async fn latest_sequence_for_shard(
    cat: &Catalog<'_>,
    shard_id: &str,
) -> Result<Option<String>, String> {
    let mut best: Option<String> = None;
    if let Some(val) = cat.get(&keys::stream_shard(shard_id)).await?
        && let Ok(meta) = serde_json::from_value::<ShardMeta>(val)
    {
        if let Some(last) = meta.last_sequence_number {
            best = Some(last);
        } else if meta.next_seq_num > 1 {
            best = Some(slot_sequence_number(shard_id, meta.next_seq_num - 1));
        }
    }
    let rows = cat
        .scan_prefix(&keys::stream_record_shard_prefix(shard_id))
        .await?;
    for (rk, _) in rows {
        if let Some((_, seq)) = rk.rsplit_once(':')
            && best.as_deref().is_none_or(|prev| seq > prev)
        {
            best = Some(seq.to_owned());
        }
    }
    Ok(best)
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{KeyType, UserIdentity};

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
    fn build_record_propagates_ttl_service_user_identity_and_view_type() {
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
        let uid = rec.user_identity.expect("user_identity");
        assert_eq!(uid.principal_id, "dynamodb.amazonaws.com");
        assert_eq!(uid.identity_type, "Service");
    }
}
