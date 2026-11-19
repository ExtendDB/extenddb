// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB Streams support for the Cloud Bigtable backend.
//!
//! Stream shard lineage and records live in `__extenddb_catalog__`:
//! - `stream_shard:<shard_id>` — one `ShardMeta` row per labelled stream shard
//!   (`shardId-{table_id}-{label_millis:013}{view_digit:1}{index:02}`)
//! - `stream_record:<shard_id>:<seq_26>` — one `StreamRecord` row per event
//!
//! Sequence numbers are fixed-width 26-digit decimal strings
//! (`{epoch_ms:013}{counter:09}{node_suffix:04}`), strictly monotonic within a
//! process, lexicographically ordered by time, and representable as `u128` for
//! `extenddb-engine` shard-iterator arithmetic.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, StreamEventName, StreamRecord, StreamRecordData,
    StreamSpecification, StreamViewType,
};
use extenddb_storage::StreamCapture;
use extenddb_storage::stream_lineage::ShardRow;
use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::catalog::keys;

/// Fixed width of Bigtable stream sequence numbers in decimal digits.
pub const SEQUENCE_NUMBER_WIDTH: usize = 26;

const EVENT_SOURCE: &str = "aws:dynamodb";
const EVENT_VERSION: &str = "1.1";

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
}

impl ShardMeta {
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

/// Build the stream ARN for `(region, account_id, table_name, label)`.
#[must_use]
pub fn build_stream_arn(region: &str, account_id: &str, table_name: &str, label: &str) -> String {
    extenddb_storage::util::stream_arn(region, account_id, table_name, label)
}

fn node_decimal_suffix() -> u16 {
    static SUFFIX: OnceLock<u16> = OnceLock::new();
    *SUFFIX.get_or_init(|| rand::random_range(0u16..10_000))
}

/// Generate a fresh, strictly monotonic 26-digit decimal sequence number
/// (`{ms:013}{counter:09}{node:04}`).
#[must_use]
pub fn next_sequence_number() -> String {
    static SEQ_STATE: Mutex<(u64, u32)> = Mutex::new((0, 0));

    let now_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).max(1) as u64;
    let (ms, counter) = {
        let mut guard = SEQ_STATE.lock().unwrap_or_else(|e| e.into_inner());
        if now_ms > guard.0 {
            guard.0 = now_ms;
            guard.1 = 0;
        } else if guard.1 < 999_999_999 {
            guard.1 += 1;
        } else {
            guard.0 = guard.0.saturating_add(1);
            guard.1 = 0;
        }
        (guard.0, guard.1)
    };
    let suffix = node_decimal_suffix();
    format!("{ms:013}{counter:09}{suffix:04}")
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

/// Close each open shard in `shard_ids` by stamping `ending_sequence_number`.
pub async fn close_shards_by_id(cat: &Catalog<'_>, shard_ids: &[String]) -> Result<(), String> {
    for sid in shard_ids {
        let Some(val) = cat.get(&keys::stream_shard(sid)).await? else {
            continue;
        };
        let Ok(mut meta) = serde_json::from_value::<ShardMeta>(val) else {
            continue;
        };
        if meta.ending_sequence_number.is_some() {
            continue;
        }
        let end_seq = latest_sequence_for_shard(cat, sid)
            .await?
            .unwrap_or_else(|| meta.starting_sequence_number.clone());
        meta.ending_sequence_number = Some(end_seq);
        put_shard(cat, &meta).await?;
    }
    Ok(())
}

/// Persist a stream record idempotently into `__extenddb_catalog__`.
///
/// Keyed by `(shard_id, sequence_number)`. Re-applying the same committed
/// `StreamRecord` during 2PC sweeper recovery overwrites the exact same row
/// with identical content, producing zero duplicate records.
pub async fn write_record(
    cat: &Catalog<'_>,
    shard_id: &str,
    record: &StreamRecord,
) -> Result<(), String> {
    let key = keys::stream_record(shard_id, &record.dynamodb.sequence_number);
    let body = serde_json::to_value(record).map_err(|e| format!("encode record: {e}"))?;
    cat.put(&key, &body).await
}

/// Return the highest 26-digit sequence number recorded in `shard_id`, or `None`.
pub async fn latest_sequence_for_shard(
    cat: &Catalog<'_>,
    shard_id: &str,
) -> Result<Option<String>, String> {
    let rows = cat
        .scan_prefix(&keys::stream_record_shard_prefix(shard_id))
        .await?;
    let mut last: Option<String> = None;
    for (rk, _) in rows {
        if let Some((_, seq)) = rk.rsplit_once(':')
            && last.as_deref().is_none_or(|prev| seq > prev)
        {
            last = Some(seq.to_owned());
        }
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{KeyType, UserIdentity};
    use std::sync::Arc;

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
