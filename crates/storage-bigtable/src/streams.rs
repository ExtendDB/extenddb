//! DynamoDB Streams support for the BigTable backend.
//!
//! Stream state is persisted in the catalog magic table:
//! - `stream:<arn>` — stream metadata (view type, label, key schema)
//! - `stream_record:<arn>:<shard>:<seq>` — one record per row
//!
//! The backend uses a single shard per stream (`shardId-000`). Sequence
//! numbers are 20-digit zero-padded nanosecond timestamps plus an 8-digit
//! random hex suffix, so they sort lexicographically in monotonic-time order
//! and tolerate concurrent same-nanosecond writes.

use std::collections::BTreeMap;

use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, StreamEventName, StreamRecord, StreamRecordData,
    StreamSpecification, StreamViewType,
};
use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::catalog::keys;

pub const SINGLE_SHARD_ID: &str = "shardId-000";
const REGION: &str = "us-east-1";
const EVENT_SOURCE: &str = "aws:dynamodb";
const EVENT_VERSION: &str = "1.1";

/// Persisted stream metadata. Serialized as the value of `stream:<arn>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMeta {
    pub stream_arn: String,
    pub stream_label: String,
    pub stream_view_type: StreamViewType,
    pub table_name: String,
    pub table_arn: String,
    pub key_schema: Vec<KeySchemaElement>,
}

/// Build the stream ARN for a new stream. `ts_iso` is the stream label
/// (also used as the trailing ARN component, matching DDB's format).
pub fn build_stream_arn(account_id: &str, table_name: &str, ts_iso: &str) -> String {
    format!("arn:aws:dynamodb:{REGION}:{account_id}:table/{table_name}/stream/{ts_iso}")
}

/// Format a UTC timestamp as the ISO label DDB uses for stream ARNs.
pub fn stream_label_now() -> String {
    let now = time::OffsetDateTime::now_utc();
    now.format(&time::format_description::well_known::Iso8601::DEFAULT)
        .unwrap_or_else(|_| format!("{}", now.unix_timestamp_nanos()))
}

/// Generate a fresh, monotonic-by-time, lex-sortable sequence number.
pub fn next_sequence_number() -> String {
    let ns: i128 = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
    // Pad to 20 digits — fits up to year ~5138.
    let suffix: u32 = rand::random();
    format!("{ns:020}{suffix:08x}")
}

fn view_includes_new(v: StreamViewType) -> bool {
    matches!(v, StreamViewType::NewImage | StreamViewType::NewAndOldImages)
}

fn view_includes_old(v: StreamViewType) -> bool {
    matches!(v, StreamViewType::OldImage | StreamViewType::NewAndOldImages)
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

/// Build a StreamRecord describing one mutation.
pub fn build_record(
    spec: &StreamSpecification,
    key_schema: &[KeySchemaElement],
    old: Option<&Item>,
    new: Option<&Item>,
    seq: &str,
) -> Option<StreamRecord> {
    if !spec.stream_enabled {
        return None;
    }
    let view = spec.stream_view_type.clone()?;
    let event_name = match (old, new) {
        (None, Some(_)) => StreamEventName::Insert,
        (Some(_), Some(_)) => StreamEventName::Modify,
        (Some(_), None) => StreamEventName::Remove,
        (None, None) => return None,
    };
    let representative = new.or(old)?;
    let keys = extract_keys(representative, key_schema);
    let new_image = if view_includes_new(view.clone()) {
        new.map(|n| n.clone().into_iter().collect())
    } else {
        None
    };
    let old_image = if view_includes_old(view.clone()) {
        old.map(|o| o.clone().into_iter().collect())
    } else {
        None
    };
    // Size: rough JSON encoding length of the post-image keys + new image.
    let size_bytes = serde_json::to_string(&keys).map(|s| s.len()).unwrap_or(0)
        + new_image
            .as_ref()
            .and_then(|n| serde_json::to_string(n).ok().map(|s| s.len()))
            .unwrap_or(0);
    let creation_ts = time::OffsetDateTime::now_utc().unix_timestamp();
    Some(StreamRecord {
        event_id: uuid::Uuid::new_v4().to_string(),
        event_name,
        event_version: EVENT_VERSION.to_owned(),
        event_source: EVENT_SOURCE.to_owned(),
        aws_region: REGION.to_owned(),
        dynamodb: StreamRecordData {
            approximate_creation_date_time: creation_ts,
            keys,
            new_image,
            old_image,
            sequence_number: seq.to_owned(),
            size_bytes: size_bytes as i64,
            stream_view_type: view,
        },
        user_identity: None,
    })
}

/// Persist a stream record into the catalog.
pub async fn write_record(cat: &Catalog<'_>, arn: &str, record: &StreamRecord) -> Result<(), String> {
    let key = keys::stream_record(arn, SINGLE_SHARD_ID, &record.dynamodb.sequence_number);
    let body = serde_json::to_value(record).map_err(|e| format!("encode record: {e}"))?;
    cat.put(&key, &body).await
}

/// Convenience wrapper: assemble a record for one mutation and persist it.
/// Returns `Ok(())` (no-op) when streams aren't enabled.
pub async fn emit(
    cat: &Catalog<'_>,
    arn: Option<&str>,
    spec: Option<&StreamSpecification>,
    key_schema: &[KeySchemaElement],
    old: Option<&Item>,
    new: Option<&Item>,
) -> Result<(), String> {
    let (Some(arn), Some(spec)) = (arn, spec) else {
        return Ok(());
    };
    let seq = next_sequence_number();
    let Some(record) = build_record(spec, key_schema, old, new, &seq) else {
        return Ok(());
    };
    write_record(cat, arn, &record).await
}
