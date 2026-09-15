// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! GSI/LSI index maintenance for the SQLite backend.
//!
//! Synchronous indexes (LSIs and zero-delay GSIs) are reconciled in the base
//! write transaction. Async GSIs are deferred via the `gsi_pending` queue —
//! **one self-describing row per index**: each row snapshots the base key
//! schema, attribute definitions, and its single target index definition, so
//! the worker applies with zero catalog reads. Each row carries its index's own
//! (jittered) propagation delay, and `ready_at` is kept monotonic within the
//! base key's `worker_partition` so jitter cannot reorder updates to one item.
//! Index key columns follow D2 — `N` keys are stored as the order-preserving
//! TEXT encoding, `S` as TEXT, `B` as BLOB — via [`sk_bound`].

use extenddb_core::types::{
    AttributeDefinition, Item, KeySchemaElement, Projection, ProjectionType, ScalarAttributeType,
    TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{composite_pk_to_text, parse_sk, sk_column, sk_column_n};
use serde::{Deserialize, Serialize};

use super::{BoundValue, all_sort_key_info, index_table_name, sk_bound};

/// Number of partitions a base key can hash to. Updates to one base item share
/// a partition; `ready_at` is clamped monotonic within a partition so the
/// single drain worker (which orders by `id`) applies them in order. More
/// partitions reduce false serialization between unrelated keys; there is no
/// concurrency cost because SQLite has a single writer.
const NUM_PARTITIONS: u64 = 16;

/// Stable partition for a base-table key (FNV-1a, mapped to a partition). A
/// local hash keeps the mapping fixed for a key across builds (`std`'s hasher
/// is not guaranteed stable).
fn partition_for(base_pk_text: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a offset basis
    for byte in base_pk_text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3); // FNV prime
    }
    (hash % NUM_PARTITIONS) as i64
}

/// Jittered propagation delay (ms), uniform in `[delay_ms / 2, delay_ms]`.
///
/// Simulates DynamoDB's eventual-consistency variability (a fixed delay is
/// perfectly predictable) while keeping the configured delay a meaningful lower
/// bound — jittering all the way to ~0 would make the delay almost meaningless.
/// `delay_ms <= 1` is returned unchanged. Callers only enqueue async indexes,
/// so `delay_ms >= 1`.
fn jitter_delay_ms(delay_ms: u64) -> u64 {
    if delay_ms <= 1 {
        delay_ms
    } else {
        use rand::Rng;
        rand::rng().random_range(delay_ms / 2 + 1..=delay_ms)
    }
}

/// A single target index definition, snapshotted at enqueue time. The
/// propagation delay is not stored — it is already encoded in the row's
/// `ready_at`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GsiIndexDef {
    pub(crate) index_id: String,
    pub(crate) key_schema: Vec<KeySchemaElement>,
    pub(crate) projection: Projection,
}

/// Self-describing apply context serialized into `gsi_pending.index_context`.
/// One per row → exactly one target index, so the worker needs no catalog read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GsiApplyContext {
    pub(crate) base_key_schema: Vec<KeySchemaElement>,
    pub(crate) attribute_definitions: Vec<AttributeDefinition>,
    pub(crate) index: GsiIndexDef,
}

/// What one `gsi_pending` row asks the worker to do: maintain a GSI, or maintain a
/// vector index. One queue serves both so that updates to a single base item stay
/// ordered across index kinds, which two queues drained independently could not
/// guarantee.
///
/// `untagged` is load-bearing, not stylistic. A GSI context serializes to exactly
/// the bytes it did before this enum existed, so rows already on disk from an
/// earlier version still deserialize, and rows written now are still readable by
/// one. That matters more than it looks: an unparseable `index_context` is treated
/// as a poison row and *dropped*, so a tagged representation would silently discard
/// every in-flight GSI update across an upgrade.
///
/// The variants are unambiguous by shape rather than by tag: a GSI context requires
/// `index` and a vector context requires `vector`, and no writer emits the other's
/// field, so for any context this code produces exactly one variant matches.
/// `pending_context_tests` pins both directions, including a verbatim legacy payload.
///
/// To be exact rather than reassuring: a hand-corrupted payload carrying BOTH fields
/// would match `Gsi`, because untagged tries variants in declaration order and
/// ignores unknown fields. That is unreachable from any serializer here, and a
/// genuinely malformed context is already dropped as a poison row, so it is a
/// property of the representation worth knowing rather than a case to defend.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum PendingApplyContext {
    Gsi(GsiApplyContext),
    Vector(extenddb_storage::vector_lifecycle::VectorApplyContext),
}

impl PendingApplyContext {
    /// The base table's key schema, which both kinds carry and the queue needs in
    /// order to place the row in its base key's partition.
    fn base_key_schema(&self) -> &[KeySchemaElement] {
        match self {
            Self::Gsi(c) => &c.base_key_schema,
            Self::Vector(c) => &c.base_key_schema,
        }
    }
}

/// Apply one claimed row to whichever index kind it describes.
///
/// Both arms tolerate a data table that no longer exists, so a base table or index
/// dropped while a row was in flight is applied-as-skip rather than logged and
/// dropped as unprocessable.
pub(crate) async fn apply_pending_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    context: &PendingApplyContext,
) -> Result<(), StorageError> {
    match context {
        PendingApplyContext::Gsi(c) => apply_claimed_row(tx, old_item, new_item, c).await,
        PendingApplyContext::Vector(c) => {
            super::vector_index::apply_vector_context(tx, old_item, new_item, c).await
        }
    }
}

/// Metadata for a single index, used on the write path and by the GSI worker.
pub(crate) struct IndexMeta {
    pub(super) index_id: String,
    pub(super) index_name: String,
    pub(super) index_type: String,
    pub(super) key_schema: Vec<KeySchemaElement>,
    pub(super) projection: Projection,
    /// Per-GSI propagation delay (ms). `None` = use system default; `Some(0)` =
    /// synchronous.
    pub(super) propagation_delay_ms: Option<i64>,
}

/// Fetch all index metadata for a table from the catalog.
pub(crate) async fn fetch_indexes_for_table(
    table_id: &str,
    pool: &sqlx::SqlitePool,
) -> Result<Vec<IndexMeta>, StorageError> {
    let rows: Vec<(String, String, String, String, String, Option<i64>)> = sqlx::query_as(
        "SELECT index_id, index_name, index_type, key_schema, projection, propagation_delay_ms \
         FROM indexes WHERE table_id = ?",
    )
    .bind(table_id)
    .fetch_all(pool)
    .await
    .map_err(crate::sqlite_util::map_sqlx_err)?;

    rows.into_iter()
        .map(|(index_id, index_name, index_type, ks, proj, delay)| {
            Ok(IndexMeta {
                index_id,
                index_name,
                index_type,
                key_schema: serde_json::from_str(&ks)
                    .map_err(|e| StorageError::Internal(e.to_string()))?,
                projection: serde_json::from_str(&proj)
                    .map_err(|e| StorageError::Internal(e.to_string()))?,
                propagation_delay_ms: delay,
            })
        })
        .collect()
}

/// Effective GSI propagation delay (ms): per-GSI override, else system default.
/// `Some(0)` forces synchronous; negative is treated as "use system default".
pub(crate) fn effective_delay(idx: &IndexMeta, system_default: u64) -> u64 {
    match idx.propagation_delay_ms {
        Some(0) => 0,
        Some(ms) if ms > 0 => ms as u64,
        _ => system_default,
    }
}

/// Enqueue async GSI propagation for a write: **one self-describing
/// `gsi_pending` row per async index**, each honoring its own effective delay.
///
/// Returns the number of rows enqueued so callers can skip waking the worker
/// when nothing was queued. Must run inside the base write transaction so the
/// pending rows commit atomically with the item mutation.
pub(crate) async fn enqueue_async_indexes(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    indexes: &[IndexMeta],
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    system_default_delay: u64,
) -> Result<usize, StorageError> {
    let mut enqueued = 0usize;
    for idx in indexes {
        if idx.index_type == "LSI" {
            continue; // LSIs are always synchronous.
        }
        let delay = effective_delay(idx, system_default_delay);
        if delay == 0 {
            continue; // Synchronous GSI — handled in-txn by sync_indexes.
        }
        let context = GsiApplyContext {
            base_key_schema: key_info.key_schema.clone(),
            attribute_definitions: key_info.attribute_definitions.clone(),
            index: GsiIndexDef {
                index_id: idx.index_id.clone(),
                key_schema: idx.key_schema.clone(),
                projection: idx.projection.clone(),
            },
        };
        enqueue_pending_row(
            tx,
            &key_info.table_id,
            old_item,
            new_item,
            delay,
            &PendingApplyContext::Gsi(context),
        )
        .await?;
        enqueued += 1;
    }
    Ok(enqueued)
}

/// Insert one self-describing `gsi_pending` row inside the base write transaction
/// (zero crash window). Shared by the GSI and vector write paths, which is what puts
/// both index kinds in one totally ordered queue.
///
/// `delay_ms` is the effective delay; a jitter in `[delay/2, delay]` is applied.
/// `ready_at` is clamped to `max(now + jitter, MAX(ready_at) in the base key's
/// partition)` so a later write that draws a smaller jitter can never become
/// eligible before an earlier one — preserving per-key FIFO when the worker drains
/// the partition in `id` order.
pub(crate) async fn enqueue_pending_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table_id: &str,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    delay_ms: u64,
    context: &PendingApplyContext,
) -> Result<(), StorageError> {
    let old_json = old_item
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let new_json = new_item
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let context_json =
        serde_json::to_string(context).map_err(|e| StorageError::Internal(e.to_string()))?;

    // Route all updates for one base item to a single partition (per-key FIFO).
    // The base key is immutable: `new_item` carries it for puts/updates,
    // `old_item` for deletes. Both context kinds describe the same base table, so
    // a GSI row and a vector row for one item hash to the same partition and stay
    // mutually ordered.
    let worker_partition = match new_item.or(old_item) {
        Some(item) => partition_for(&composite_pk_to_text(item, context.base_key_schema())?),
        None => 0,
    };

    // ready_at as RFC 3339 so it compares correctly (lexically) against the
    // worker's RFC 3339 `now` cutoff and against other rows' ready_at.
    let jittered = jitter_delay_ms(delay_ms);
    let candidate = crate::sqlite_util::format_timestamp(
        time::OffsetDateTime::now_utc()
            + time::Duration::milliseconds(i64::try_from(jittered).unwrap_or(i64::MAX)),
    );
    // Monotonic clamp within the partition (RFC 3339 strings sort lexically).
    let part_max: Option<String> =
        sqlx::query_scalar("SELECT MAX(ready_at) FROM gsi_pending WHERE worker_partition = ?")
            .bind(worker_partition)
            .fetch_one(&mut **tx)
            .await
            .map_err(crate::sqlite_util::map_sqlx_err)?;
    let ready_at = match part_max {
        Some(prev) if prev > candidate => prev,
        _ => candidate,
    };

    sqlx::query(
        "INSERT INTO gsi_pending \
         (table_id, worker_partition, old_item, new_item, index_context, ready_at) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(table_id)
    .bind(worker_partition)
    .bind(&old_json)
    .bind(&new_json)
    .bind(&context_json)
    .bind(&ready_at)
    .execute(&mut **tx)
    .await
    .map_err(crate::sqlite_util::map_sqlx_err)?;
    Ok(())
}

/// Project an item according to an index's projection configuration.
pub(crate) fn project_item_for_index(
    item: &Item,
    index_ks: &[KeySchemaElement],
    base_ks: &[KeySchemaElement],
    projection: &Projection,
) -> Item {
    match projection.projection_type {
        ProjectionType::All => item.clone(),
        ProjectionType::KeysOnly | ProjectionType::Include => {
            let mut projected = Item::new();
            for ks in base_ks.iter().chain(index_ks.iter()) {
                if let Some(v) = item.get(&ks.attribute_name) {
                    projected.insert(ks.attribute_name.clone(), v.clone());
                }
            }
            if projection.projection_type == ProjectionType::Include
                && let Some(attrs) = &projection.non_key_attributes
            {
                for attr in attrs {
                    if let Some(v) = item.get(attr) {
                        projected.insert(attr.clone(), v.clone());
                    }
                }
            }
            projected
        }
    }
}

/// Whether an item carries every key attribute an index requires.
pub(crate) fn item_has_index_keys(item: &Item, index_ks: &[KeySchemaElement]) -> bool {
    index_ks
        .iter()
        .all(|ks| item.contains_key(&ks.attribute_name))
}

/// Synchronously reconcile index tables for a base-item change — but only for
/// indexes that are synchronous: LSIs (always) and GSIs whose effective delay
/// is 0. GSIs with a non-zero delay are deferred via `gsi_pending` and applied
/// by the worker, so they are skipped here.
pub(crate) async fn sync_indexes(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    indexes: &[IndexMeta],
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    system_default_delay: u64,
) -> Result<(), StorageError> {
    let base_sks = all_sort_key_info(base_key_schema, attr_defs);
    for idx in indexes {
        if idx.index_type != "LSI" && effective_delay(idx, system_default_delay) != 0 {
            continue; // Async GSI — applied later by the propagation worker.
        }
        let idx_table = index_table_name(&idx.index_id);
        let idx_sks = all_sort_key_info(&idx.key_schema, attr_defs);

        if let Some(old) = old_item
            && item_has_index_keys(old, &idx.key_schema)
        {
            delete_index_row_multi(tx, &idx_table, old, base_key_schema, &base_sks).await?;
        }

        if let Some(new) = new_item
            && item_has_index_keys(new, &idx.key_schema)
        {
            let projected =
                project_item_for_index(new, &idx.key_schema, base_key_schema, &idx.projection);
            insert_index_row_multi(
                tx,
                &idx_table,
                new,
                &projected,
                &idx.key_schema,
                base_key_schema,
                &idx_sks,
                &base_sks,
            )
            .await?;
        }
    }
    Ok(())
}

/// Delete an index row identified by its base-table key columns.
pub(crate) async fn delete_index_row_multi(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    idx_table: &str,
    item: &Item,
    base_ks: &[KeySchemaElement],
    base_sks: &[(&str, ScalarAttributeType)],
) -> Result<(), StorageError> {
    let base_pk_text = composite_pk_to_text(item, base_ks)?;

    let mut where_parts = vec!["base_pk = ?".to_owned()];
    for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
        where_parts.push(format!("base_{} = ?", sk_column_n(i, sk_type)));
    }
    let sql = format!(
        "DELETE FROM {idx_table} WHERE {}",
        where_parts.join(" AND ")
    );

    let mut query = sqlx::query(&sql).bind(base_pk_text);
    for &(sk_name, sk_type) in base_sks {
        // Bind a value for every placeholder, mirroring insert_index_row_multi:
        // a missing sort-key attribute binds an empty string rather than
        // skipping the bind (which would desynchronise placeholders and binds).
        let bound = match item.get(sk_name) {
            Some(v) => sk_bound(&parse_sk(v, sk_type)?),
            None => BoundValue::Text(String::new()),
        };
        query = super::bind_bound!(query, bound);
    }
    query
        .execute(&mut **tx)
        .await
        .map_err(crate::sqlite_util::map_sqlx_err)?;
    Ok(())
}

/// Insert (or replace) an index row for a base item.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_index_row_multi(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    idx_table: &str,
    item: &Item,
    projected: &Item,
    index_ks: &[KeySchemaElement],
    base_ks: &[KeySchemaElement],
    idx_sks: &[(&str, ScalarAttributeType)],
    base_sks: &[(&str, ScalarAttributeType)],
) -> Result<(), StorageError> {
    let idx_pk_text = composite_pk_to_text(item, index_ks)?;
    let base_pk_text = composite_pk_to_text(item, base_ks)?;
    let item_json =
        serde_json::to_string(projected).map_err(|e| StorageError::Internal(e.to_string()))?;

    // Columns and the matching bound values, in column order.
    let mut cols = vec!["pk".to_owned()];
    let mut values: Vec<BoundValue> = vec![BoundValue::Text(idx_pk_text)];

    for (i, &(sk_name, sk_type)) in idx_sks.iter().enumerate() {
        cols.push(sk_column_n(i, sk_type));
        if let Some(v) = item.get(sk_name) {
            values.push(sk_bound(&parse_sk(v, sk_type)?));
        } else {
            values.push(BoundValue::Text(String::new()));
        }
    }

    cols.push("base_pk".to_owned());
    values.push(BoundValue::Text(base_pk_text));

    for (i, &(sk_name, sk_type)) in base_sks.iter().enumerate() {
        cols.push(format!("base_{}", sk_column_n(i, sk_type)));
        if let Some(v) = item.get(sk_name) {
            values.push(sk_bound(&parse_sk(v, sk_type)?));
        } else {
            values.push(BoundValue::Text(String::new()));
        }
    }

    cols.push("item_data".to_owned());
    values.push(BoundValue::Text(item_json));

    let placeholders = vec!["?"; cols.len()].join(", ");
    let sql = format!(
        "INSERT OR REPLACE INTO {idx_table} ({}) VALUES ({placeholders})",
        cols.join(", ")
    );

    let mut query = sqlx::query(&sql);
    for v in values {
        query = super::bind_bound!(query, v);
    }
    query
        .execute(&mut **tx)
        .await
        .map_err(crate::sqlite_util::map_sqlx_err)?;
    Ok(())
}

/// Helper: derive the sort-key column name used by an index data table for the
/// Nth index sort key. Exposed for query routing.
#[allow(dead_code)]
pub(crate) fn index_sk_column(index: usize, sk_type: ScalarAttributeType) -> String {
    if index == 0 {
        sk_column(sk_type).to_owned()
    } else {
        sk_column_n(index, sk_type)
    }
}

/// True if a storage error is a "missing table" error (the `_ddb_*` index table
/// or a `_vidx_*` vector table was dropped, e.g. the base table was deleted
/// while a `gsi_pending` row was in flight). Such rows are benignly skipped.
pub(crate) fn is_no_such_table(e: &StorageError) -> bool {
    matches!(e, StorageError::Internal(msg) if msg.contains("no such table"))
}

/// Apply one claimed `gsi_pending` row to its single target index, within the
/// worker's transaction, using the row's self-describing `index_context` — no
/// catalog read. A missing index data table (the base table was deleted while
/// the row was in flight) is skipped benignly.
pub(crate) async fn apply_claimed_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    context: &GsiApplyContext,
) -> Result<(), StorageError> {
    let base_sks = all_sort_key_info(&context.base_key_schema, &context.attribute_definitions);
    let idx = &context.index;
    let idx_table = index_table_name(&idx.index_id);
    let idx_sks = all_sort_key_info(&idx.key_schema, &context.attribute_definitions);

    if let Some(old) = old_item
        && item_has_index_keys(old, &idx.key_schema)
    {
        delete_index_row_multi(tx, &idx_table, old, &context.base_key_schema, &base_sks)
            .await
            .or_else(|e| if is_no_such_table(&e) { Ok(()) } else { Err(e) })?;
    }
    if let Some(new) = new_item
        && item_has_index_keys(new, &idx.key_schema)
    {
        let projected = project_item_for_index(
            new,
            &idx.key_schema,
            &context.base_key_schema,
            &idx.projection,
        );
        insert_index_row_multi(
            tx,
            &idx_table,
            new,
            &projected,
            &idx.key_schema,
            &context.base_key_schema,
            &idx_sks,
            &base_sks,
        )
        .await
        .or_else(|e| if is_no_such_table(&e) { Ok(()) } else { Err(e) })?;
    }
    Ok(())
}

#[cfg(test)]
mod pending_context_tests {
    use super::{GsiApplyContext, GsiIndexDef, PendingApplyContext};
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, Projection, ProjectionType,
        ScalarAttributeType,
    };
    use extenddb_storage::vector_lifecycle::{VectorApplyContext, VectorIndexMeta};

    /// A context written by a build that predates vector rows, verbatim. It must
    /// still deserialize.
    ///
    /// This is the test that protects an upgrade. A row whose `index_context` fails
    /// to parse is treated as poison and DROPPED, so if the representation had
    /// changed incompatibly, every GSI update in flight at the moment of the upgrade
    /// would have been discarded silently, with the item written and its index never
    /// catching up.
    const LEGACY_GSI_CONTEXT: &str = r#"{
        "base_key_schema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "attribute_definitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "index": {
            "index_id": "idx-1",
            "key_schema": [{"AttributeName": "gsipk", "KeyType": "HASH"}],
            "projection": {"ProjectionType": "ALL"}
        }
    }"#;

    fn base_ks() -> Vec<KeySchemaElement> {
        vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }]
    }

    fn base_ad() -> Vec<AttributeDefinition> {
        vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }]
    }

    fn gsi_context() -> GsiApplyContext {
        GsiApplyContext {
            base_key_schema: base_ks(),
            attribute_definitions: base_ad(),
            index: GsiIndexDef {
                index_id: "idx-1".to_owned(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "gsipk".to_owned(),
                    key_type: KeyType::Hash,
                }],
                projection: Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                },
            },
        }
    }

    fn vector_context() -> VectorApplyContext {
        VectorApplyContext {
            base_key_schema: base_ks(),
            attribute_definitions: base_ad(),
            table_id: "t-1".to_owned(),
            vector: VectorIndexMeta {
                index_id: "vidx-1".to_owned(),
                dimensions: 2,
                vector_attribute_name: "emb".to_owned(),
                projection: Projection {
                    projection_type: ProjectionType::KeysOnly,
                    non_key_attributes: None,
                },
                hash_attribute_name: Some("tenant".to_owned()),
                search_schema_attribute_names: vec!["tenant".to_owned()],
            },
        }
    }

    #[test]
    fn a_legacy_gsi_context_still_deserializes_as_a_gsi_row() {
        let parsed: PendingApplyContext =
            serde_json::from_str(LEGACY_GSI_CONTEXT).expect("legacy context must still parse");
        match parsed {
            PendingApplyContext::Gsi(c) => assert_eq!(c.index.index_id, "idx-1"),
            PendingApplyContext::Vector(_) => {
                panic!("a legacy GSI context must not be read as a vector row")
            }
        }
    }

    /// The bytes a GSI row writes today are the bytes it wrote before the enum
    /// existed, so a row written by this build is still readable by the previous
    /// one. `untagged` is what buys this, and this test is what keeps it.
    #[test]
    fn wrapping_a_gsi_context_does_not_change_its_serialized_form() {
        let bare = serde_json::to_string(&gsi_context()).expect("bare");
        let wrapped =
            serde_json::to_string(&PendingApplyContext::Gsi(gsi_context())).expect("wrapped");
        assert_eq!(
            bare, wrapped,
            "the queue's on-disk format must not change for GSI rows"
        );
    }

    /// A vector context round-trips and carries no GSI discriminant field.
    ///
    /// Note what this does NOT guard: it still passes if `untagged` is removed, so it
    /// is not the protection for on-disk compatibility. The two tests above are, and
    /// both fail without `untagged`. This one pins the shape contract that makes the
    /// discrimination possible in the first place.
    #[test]
    fn a_vector_context_round_trips_and_carries_no_gsi_discriminant() {
        let json = serde_json::to_string(&PendingApplyContext::Vector(vector_context()))
            .expect("serialize");
        assert!(
            !json.contains("\"index\":"),
            "a vector context must not carry the GSI discriminant field: {json}"
        );
        let parsed: PendingApplyContext = serde_json::from_str(&json).expect("deserialize");
        match parsed {
            PendingApplyContext::Vector(c) => {
                assert_eq!(c.vector.index_id, "vidx-1");
                assert_eq!(c.table_id, "t-1");
                assert_eq!(c.vector.dimensions, 2);
                assert_eq!(c.vector.hash_attribute_name.as_deref(), Some("tenant"));
                assert_eq!(c.vector.search_schema_attribute_names, ["tenant"]);
                assert_eq!(
                    c.vector.projection.projection_type,
                    ProjectionType::KeysOnly
                );
            }
            PendingApplyContext::Gsi(_) => panic!("a vector context must not be read as a GSI row"),
        }
    }

    /// A GSI row and a vector row for the SAME base item must land in the same queue
    /// partition, which is what makes them mutually ordered.
    ///
    /// This is the claim that reusing one queue buys ordering ACROSS index kinds, and
    /// it holds only because both contexts hash the same base key rather than
    /// anything index-specific. Asserted on the partition because the partition is the
    /// mechanism: two kinds hashing differently would land in separate partitions,
    /// each monotonic on its own, leaving the relative order of a GSI and a vector
    /// update to one item unconstrained.
    ///
    /// Driven through the real `enqueue_pending_row` rather than a test shim, so it is
    /// the production path being measured.
    #[tokio::test]
    async fn a_gsi_row_and_a_vector_row_for_one_item_share_a_partition() {
        let engine = crate::SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");

        let the_item: extenddb_core::types::Item =
            serde_json::from_str(r#"{"pk":{"S":"shared-key"},"emb":{"L":[{"N":"1"}]}}"#)
                .expect("item");
        let mut tx = engine.pool.begin_with("BEGIN IMMEDIATE").await.expect("tx");
        for context in [
            PendingApplyContext::Gsi(gsi_context()),
            PendingApplyContext::Vector(vector_context()),
        ] {
            super::enqueue_pending_row(&mut tx, "t-1", None, Some(&the_item), 60_000, &context)
                .await
                .expect("enqueue");
        }
        tx.commit().await.expect("commit");

        let partitions: Vec<(i64,)> =
            sqlx::query_as("SELECT DISTINCT worker_partition FROM gsi_pending")
                .fetch_all(&engine.pool)
                .await
                .expect("partitions");
        assert_eq!(
            partitions.len(),
            1,
            "a GSI row and a vector row for one base item must share a partition, or \
             their relative order is unconstrained"
        );
        let (depth,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM gsi_pending")
            .fetch_one(&engine.pool)
            .await
            .expect("depth");
        assert_eq!(
            depth, 2,
            "both kinds must have enqueued, so one partition is not an artefact of a \
             missing row"
        );
    }

    /// A catalog created before the rename must keep honouring its operator's value.
    ///
    /// This is the compatibility property the whole fallback exists for, and the cost
    /// of getting it wrong is not cosmetic: the server refuses to start on a
    /// catalog-version mismatch rather than migrating, so nothing ever rewrites the old
    /// row. Reading past it would silently reset a configured delay to the default, and
    /// since 0 means synchronous, the silent change would be from strict to eventually
    /// consistent, which is exactly the direction that turns a passing test suite into
    /// a flaky one somewhere else.
    #[tokio::test]
    async fn a_pre_rename_catalog_still_honours_its_configured_delay() {
        let engine = crate::SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");

        // Reshape the catalog to look as it did before the rename: the legacy key
        // only, carrying a deliberately non-default value.
        sqlx::query("DELETE FROM settings WHERE key = 'index_propagation_delay_ms'")
            .execute(&engine.pool)
            .await
            .expect("drop canonical row");
        sqlx::query("INSERT INTO settings (key, value) VALUES ('gsi_propagation_delay_ms', '0')")
            .execute(&engine.pool)
            .await
            .expect("seed legacy row");

        assert_eq!(
            engine.index_propagation_delay().await,
            0,
            "a value set under the pre-rename key must still be honoured, or an \
             operator's synchronous setting silently becomes asynchronous"
        );
    }

    /// With both rows present the canonical one wins, deterministically.
    ///
    /// Reachable if an operator sets the legacy key on a build that predates the
    /// canonicalising write path and then upgrades. Without the explicit ordering the
    /// winner would be whichever row SQLite happened to return first.
    #[tokio::test]
    async fn the_canonical_key_wins_when_both_are_present() {
        let engine = crate::SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");

        sqlx::query(
            "INSERT OR REPLACE INTO settings (key, value) \
             VALUES ('index_propagation_delay_ms', '7'), ('gsi_propagation_delay_ms', '999')",
        )
        .execute(&engine.pool)
        .await
        .expect("seed both rows");

        assert_eq!(
            engine.index_propagation_delay().await,
            7,
            "the canonical key must take precedence over the deprecated alias"
        );
    }
}
