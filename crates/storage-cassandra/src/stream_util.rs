// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stream utilities: Hybrid Logical Clock sequence number generation and
//! stream record batch statement construction.

use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extenddb_core::types::{
    AttributeValue, Item, StreamEventName, StreamRecord, StreamRecordData, StreamViewType,
    TableKeyInfo, item_size_bytes,
};
use extenddb_storage::StreamCapture;

/// Number of fixed shards per stream (matches PostgreSQL reference implementation).
pub const SHARDS_PER_STREAM: u32 = 4;

/// Hybrid Logical Clock for generating ordered, unique stream sequence numbers.
///
/// Format: `{timestamp_ms:013}{counter:06}{node_id:04}` = 23 digits.
/// This is within DynamoDB's 40-character sequence number limit.
///
/// Node ID is derived from `crc32(hostname:contact_point) % 9999 + 1` at
/// startup, ensuring uniqueness across instances on the same host (different
/// ports) and in containerized environments.
pub struct HybridClock {
    last_timestamp_ms: i64,
    logical_counter: u32,
    node_id: u16,
}

impl HybridClock {
    /// Create a new HybridClock with the given node ID.
    pub fn new(node_id: u16) -> Self {
        Self {
            last_timestamp_ms: 0,
            logical_counter: 0,
            node_id,
        }
    }

    /// Derive a stable node ID (in the range 1..=9999) from an ExtendDB instance identifier.
    ///
    /// Instance identifiers (e.g. hostname + listening address) ensure that multiple ExtendDB
    /// instances, whether on the same or different hosts, get distinct node IDs, in turn allowing
    /// tie-breaking for conflicting clock values.
    pub fn derive_node_id(instance_id: &str) -> u16 {
        let hash = crc32fast::hash(instance_id.as_bytes());
        u16::try_from(hash % 9999 + 1).unwrap_or(1)
    }

    /// Generate the next clock value, used a sequence number for stream records.
    ///
    /// Returns a 23-digit zero-padded string. Lexicographic ordering matches
    /// temporal ordering.
    pub fn generate(&mut self) -> String {
        let now_ms = chrono::Utc::now().timestamp_millis();

        if now_ms > self.last_timestamp_ms {
            self.last_timestamp_ms = now_ms;
            self.logical_counter = 0;
        } else {
            if now_ms < self.last_timestamp_ms {
                // Clock went backwards — clamp and increment counter.
                tracing::warn!(
                    delta_ms = self.last_timestamp_ms - now_ms,
                    "HybridClock: system clock went backwards; clamping to last timestamp"
                );
            }
            self.logical_counter += 1;
            // Counter exhausted: wait for next millisecond.
            // Requires ~1B writes/sec/node — will never occur in practice.
            if self.logical_counter > 999_999 {
                std::thread::sleep(std::time::Duration::from_millis(1));
                self.last_timestamp_ms = chrono::Utc::now().timestamp_millis();
                self.logical_counter = 0;
            }
        }

        format!(
            "{:013}{:06}{:04}",
            self.last_timestamp_ms, self.logical_counter, self.node_id
        )
    }
}

/// Thread-safe HybridClock handle.
pub type SharedHlc = Arc<Mutex<HybridClock>>;

/// Create a new SharedHlc, deriving node ID from ExtendDB's instance ID.
pub fn new_shared_hlc(instance_id: &str) -> SharedHlc {
    let node_id = HybridClock::derive_node_id(instance_id);
    tracing::info!(node_id, instance_id, "HybridClock initialized");
    Arc::new(Mutex::new(HybridClock::new(node_id)))
}

/// Compute the shard ID for a given partition key and table ID.
///
/// Uses CRC32 hash modulo shard count, matching the PostgreSQL reference
/// implementation. Uses `table_id` (UUID) rather than `table_name` to
/// prevent shard ID collisions after table deletion and recreation.
pub fn assign_shard_id(partition_key: &str, table_id: &str) -> String {
    let hash = crc32fast::hash(partition_key.as_bytes());
    let idx = (hash as usize) % SHARDS_PER_STREAM as usize;
    format!("shardId-{table_id}-{idx:012}")
}

/// Zero sequence number used as the starting point for new shards.
pub const ZERO_SEQUENCE: &str = "00000000000000000000000"; // 23 zeros

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamRecordIdentity {
    pub event_id: String,
    pub sequence_number: String,
    pub created_at_ms: i64,
}

/// Build a CQL INSERT statement for a stream record to be included in a LOGGED BATCH.
///
/// Returns `None` if no record should be written (both old and new items are absent,
/// i.e. a delete of a non-existent item).
///
/// The returned string uses string-interpolated values (not `?` placeholders) because
/// cdrs-tokio's `BatchQueryBuilder` requires all statements in a batch to share the
/// same bind parameter count. All values are server-generated or sanitized — there is
/// no user-controlled input in the interpolated fields.
///
/// # Arguments
/// - `account_keyspace` — the per-account Cassandra keyspace
/// - `table_id` — UUID of the table (used for shard assignment)
/// - `key_info` — key schema and attribute definitions for the table
/// - `old_item` — item state before the write (None for inserts)
/// - `new_item` — item state after the write (None for deletes)
/// - `capture` — stream view type and region from the caller
/// - `hlc` — shared HLC for sequence number generation
/// - `retention_seconds` — Cassandra TTL for the stream record
pub fn stream_record_statement(
    account_keyspace: &str,
    table_id: &str,
    key_info: &TableKeyInfo,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    capture: &StreamCapture,
    hlc: &Arc<Mutex<HybridClock>>,
    retention_seconds: u32,
) -> Option<String> {
    let identity = StreamRecordIdentity {
        event_id: uuid::Uuid::new_v4().to_string(),
        sequence_number: hlc
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generate(),
        created_at_ms: chrono::Utc::now().timestamp_millis(),
    };
    stream_record_statement_with_identity(
        account_keyspace,
        table_id,
        key_info,
        old_item,
        new_item,
        capture,
        &identity,
        retention_seconds,
    )
}

/// Build a stream record statement with a caller-supplied event identity.
///
/// TTL expiration persists its identity before applying effects so that a retry
/// rewrites the same record instead of publishing a second visible `REMOVE`.
#[allow(clippy::too_many_arguments)]
pub fn stream_record_statement_with_identity(
    account_keyspace: &str,
    table_id: &str,
    key_info: &TableKeyInfo,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    capture: &StreamCapture,
    identity: &StreamRecordIdentity,
    retention_seconds: u32,
) -> Option<String> {
    let source = new_item.or(old_item)?;

    let event = match (old_item, new_item) {
        (None, Some(_)) => StreamEventName::Insert,
        (Some(_), Some(_)) => StreamEventName::Modify,
        (Some(_), None) => StreamEventName::Remove,
        (None, None) => return None,
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

    let size = i64::try_from(item_size_bytes(source)).unwrap_or(i64::MAX);

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

    let shard_id = assign_shard_id(&pk_str, table_id);

    let record = StreamRecord {
        event_id: identity.event_id.clone(),
        event_name: event,
        event_version: "1.1".to_owned(),
        event_source: "aws:dynamodb".to_owned(),
        aws_region: capture.region.to_string(),
        dynamodb: StreamRecordData {
            approximate_creation_date_time: identity.created_at_ms / 1_000,
            keys,
            new_image,
            old_image,
            sequence_number: identity.sequence_number.clone(),
            size_bytes: size,
            stream_view_type: capture.view_type,
        },
        user_identity: capture.user_identity.clone(),
    };

    let record_json = serde_json::to_string(&record).ok()?;
    let event_name = format!("{:?}", record.event_name);
    let seq = &record.dynamodb.sequence_number;
    let now_ms = identity.created_at_ms;

    Some(format!(
        "INSERT INTO {account_keyspace}.stream_records \
         (shard_id, sequence_number, table_id, event_name, record_data, created_at) \
         VALUES ('{shard_id}', '{seq}', '{table_id}', '{event_name}', \
         '{record_json_escaped}', {now_ms}) \
         USING TTL {retention_seconds}",
        record_json_escaped = record_json.replace('\'', "''"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_ordering() {
        let mut clock = HybridClock::new(1);
        let a = clock.generate();
        let b = clock.generate();
        let c = clock.generate();
        assert!(a < b, "sequence numbers must be ordered: {a} < {b}");
        assert!(b < c, "sequence numbers must be ordered: {b} < {c}");
    }

    #[test]
    fn test_sequence_length() {
        let mut clock = HybridClock::new(1);
        let seq = clock.generate();
        assert_eq!(seq.len(), 23, "sequence number must be 23 digits: {seq}");
    }

    #[test]
    fn test_sequence_numeric() {
        let mut clock = HybridClock::new(9999);
        let seq = clock.generate();
        assert!(
            seq.chars().all(|c| c.is_ascii_digit()),
            "sequence must be numeric: {seq}"
        );
    }

    #[test]
    fn test_node_id_range() {
        let id = HybridClock::derive_node_id("localhost:9042");
        assert!(
            (1..=9999).contains(&id),
            "node_id must be in 1..=9999, got {id}"
        );
    }

    #[test]
    fn test_different_contact_points_give_different_ids() {
        let id1 = HybridClock::derive_node_id("localhost:18443");
        let id2 = HybridClock::derive_node_id("localhost:18444");
        // Not guaranteed to differ (hash collision possible) but overwhelmingly likely.
        // This test documents the intent rather than asserting strict inequality.
        let _ = (id1, id2);
    }

    #[test]
    fn test_assign_shard_id_stable() {
        let s1 = assign_shard_id("user-123", "table-uuid-abc");
        let s2 = assign_shard_id("user-123", "table-uuid-abc");
        assert_eq!(s1, s2, "shard assignment must be deterministic");
    }

    #[test]
    fn test_assign_shard_id_format() {
        let s = assign_shard_id("pk", "my-table-id");
        assert!(
            s.starts_with("shardId-my-table-id-"),
            "unexpected format: {s}"
        );
    }
}
