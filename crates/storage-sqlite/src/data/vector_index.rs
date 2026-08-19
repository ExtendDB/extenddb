// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Vector index maintenance on the write path.
//!
//! Vector indexes are **eventually consistent**, the same model the service gives
//! them and the same model a GSI has here, so maintenance runs on the existing
//! `gsi_pending` queue rather than in the base write transaction. [`maintain_vector_indexes`]
//! is the single entry point and owns that choice: a propagation delay of 0 keeps
//! the work in the caller's transaction, any other delay enqueues it.
//!
//! Reusing the GSI queue is what makes the asynchronous path correct rather than
//! merely deferred, and none of these properties would come free from a queue of
//! its own:
//!
//! * **Crash safety.** The pending row is inserted in the base write transaction,
//!   so there is no window in which the item is committed and the index work is
//!   not yet durable. The worker claims and applies in one transaction, so a crash
//!   mid-apply rolls back and the row is retried. At-least-once delivery is safe
//!   because applying a row is idempotent: it deletes the base key's row and
//!   reinserts it from the snapshotted item.
//! * **Per-key ordering, across index kinds.** The row's partition is a hash of the
//!   *base* key, so a vector row and a GSI row for the same item share a partition,
//!   `ready_at` is clamped monotonic within it, and the worker drains in `id` order.
//!   Two writes to one item therefore reach both index kinds in write order even
//!   though the delay is jittered.
//! * **Snapshot semantics.** The row carries its own [`VectorApplyContext`], so the
//!   worker needs no catalog read and an index dropped, or redefined, between
//!   enqueue and apply cannot make a queued write unapplicable or retroactively
//!   change how it was indexed.
//!
//! A write to a table whose items do not carry the vector still enqueues, because
//! the removal is the point: an item that loses its vector attribute must leave the
//! index, and skipping the enqueue would leave the stale row in place forever.

use serde::{Deserialize, Serialize};

use extenddb_core::types::{AttributeDefinition, Item, KeySchemaElement, SearchSchemaElementType};
use extenddb_core::validation::{vector_components, vector_norm};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::pk_to_text;

use super::{BoundValue, all_sort_key_info, sk_bound, vector_table_name};
use crate::vector_search::partition_value;

/// A vector index as the write path needs it.
///
/// Serializable because the asynchronous path snapshots it verbatim into the
/// pending row's [`VectorApplyContext`]. The write path and the worker therefore
/// apply from the *same* description of the index, which is the property that stops
/// a queued write from being reinterpreted under a later definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VectorIndexMeta {
    pub index_id: String,
    pub dimensions: usize,
    pub vector_attribute_name: String,
    /// The index's projection, applied to the stored row exactly as the GSI path
    /// applies its own. Not applying it was an unexplained divergence from the
    /// sibling, and it made a search return attributes the index does not project.
    pub projection: extenddb_core::types::Projection,
    /// The single HASH element's attribute name, when the index declares one.
    /// `None` means the index is unscoped and every row shares one partition.
    pub hash_attribute_name: Option<String>,
    /// Every attribute named by the SearchSchema, HASH and INLINE_FILTER alike.
    ///
    /// These are projected regardless of `ProjectionType`, which is the documented
    /// rule for a vector index and is NOT GSI `KEYS_ONLY` semantics: `KEYS_ONLY`
    /// on a vector index projects the base primary key, the vector attribute and
    /// the inline filter attributes. Withholding them is not merely a reporting
    /// difference, it breaks search: the filter is evaluated against the stored
    /// payload, so a missing filter attribute makes every row fail the predicate
    /// and a filtered search match nothing.
    pub search_schema_attribute_names: Vec<String>,
}

/// Load the vector indexes of a table.
///
/// Read inside the write transaction rather than taken from the cached
/// `TableKeyInfo`, because the cache carries the search schema but not the index
/// id, and the id is what names the data table.
pub(crate) async fn fetch_vector_indexes_for_table(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table_id: &str,
) -> Result<Vec<VectorIndexMeta>, StorageError> {
    let rows: Vec<(String, i64, String, Option<String>, String)> = sqlx::query_as(
        "SELECT index_id, dimensions, vector_attribute, search_schema, projection \
         FROM vector_indexes WHERE table_id = ?",
    )
    .bind(table_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(crate::sqlite_util::map_sqlx_err)?;

    let mut out = Vec::with_capacity(rows.len());
    for (index_id, dimensions, vector_attribute, search_schema, projection) in rows {
        let attr: extenddb_core::types::VectorAttribute =
            serde_json::from_str(&vector_attribute)
                .map_err(|e| StorageError::Internal(format!("vector_attribute: {e}")))?;
        let (hash_attribute_name, search_schema_attribute_names) = match search_schema.as_deref() {
            Some(json) => {
                let elements: Vec<extenddb_core::types::SearchSchemaElement> =
                    serde_json::from_str(json)
                        .map_err(|e| StorageError::Internal(format!("search_schema: {e}")))?;
                let hash = elements
                    .iter()
                    .find(|e| e.element_type == SearchSchemaElementType::Hash)
                    .map(|e| e.attribute_name.clone());
                let all = elements
                    .into_iter()
                    .map(|e| e.attribute_name)
                    .collect::<Vec<_>>();
                (hash, all)
            }
            None => (None, Vec::new()),
        };
        let projection: extenddb_core::types::Projection = serde_json::from_str(&projection)
            .map_err(|e| StorageError::Internal(format!("vector projection: {e}")))?;
        out.push(VectorIndexMeta {
            index_id,
            dimensions: usize::try_from(dimensions).map_err(|_| {
                StorageError::Internal(format!("vector dimensions out of range: {dimensions}"))
            })?,
            vector_attribute_name: attr.attribute_name,
            hash_attribute_name,
            search_schema_attribute_names,
            projection,
        });
    }
    Ok(out)
}

/// Whether an item belongs in a vector index.
///
/// It must carry the vector attribute, and the HASH attribute when the index
/// declares one: without the latter the row could not be placed in a partition,
/// and putting it in the unscoped partition would make it visible to searches of
/// every other partition. Not an error, exactly as a GSI silently omits an item
/// missing its index key.
fn item_is_indexable(item: &Item, meta: &VectorIndexMeta) -> bool {
    if !item.contains_key(&meta.vector_attribute_name) {
        return false;
    }
    match &meta.hash_attribute_name {
        Some(name) => item.contains_key(name),
        None => true,
    }
}

/// The partition column value for an item under one index.
fn item_partition(item: &Item, meta: &VectorIndexMeta) -> Result<String, StorageError> {
    match &meta.hash_attribute_name {
        Some(name) => {
            let value = item.get(name).ok_or_else(|| {
                StorageError::Internal(
                    "indexable check passed but the hash attribute is absent".to_owned(),
                )
            })?;
            partition_value(Some((name.as_str(), value)))
        }
        None => partition_value(None),
    }
}

/// Base-key bind values for a row, in key-schema order.
fn base_key_binds(
    item: &Item,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<Vec<BoundValue>, StorageError> {
    let base_sks = all_sort_key_info(base_key_schema, attr_defs);
    let pk_attr = &base_key_schema[0].attribute_name;
    let pk = item.get(pk_attr).ok_or_else(|| {
        StorageError::Internal("item written without its partition key".to_owned())
    })?;
    let mut binds = vec![BoundValue::Text(pk_to_text(pk)?.into_owned())];
    for &(name, sk_type) in &base_sks {
        // Sort keys use the same storage representation as the GSI/LSI tables:
        // order-preserving text for numbers and a BLOB for binary. Encoding them
        // any other way would still be self-consistent here but would diverge
        // from every other index table for the same item.
        match item.get(name) {
            Some(value) => binds.push(sk_bound(&extenddb_storage::util::parse_sk(value, sk_type)?)),
            None => binds.push(BoundValue::Text(String::new())),
        }
    }
    Ok(binds)
}

/// Column names for the base key, in key-schema order.
fn base_key_columns(
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Vec<String> {
    let base_sks = all_sort_key_info(base_key_schema, attr_defs);
    let mut cols = vec!["base_pk".to_owned()];
    for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
        cols.push(format!(
            "base_{}",
            extenddb_storage::util::sk_column_n(i, sk_type)
        ));
    }
    cols
}

/// Everything the propagation worker needs to apply one vector index update,
/// serialized into `gsi_pending.index_context`.
///
/// `table_id` is carried here even though the queue row has a `table_id` column of
/// its own, because a vector data table is named from the table id *and* the index
/// id. Reading it from the context preserves the invariant that the context alone
/// is sufficient, rather than splitting one apply's inputs across a column and a
/// JSON blob. Both are written from the same variable in the same statement, so
/// they cannot disagree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VectorApplyContext {
    pub(crate) base_key_schema: Vec<KeySchemaElement>,
    pub(crate) attribute_definitions: Vec<AttributeDefinition>,
    pub(crate) table_id: String,
    /// Deliberately named `vector` rather than `index`: it is the field whose
    /// presence lets the untagged `PendingApplyContext` tell a vector row from a
    /// GSI row by shape alone. See that type for why the discriminant is a shape
    /// and not a tag.
    pub(crate) vector: VectorIndexMeta,
}

/// Maintain every vector index on a table for one item write.
///
/// The single entry point for the write path, and the one place that decides
/// between synchronous and asynchronous. `delay_ms` of 0 applies in the caller's
/// transaction; anything else enqueues one `gsi_pending` row per index. Returns the
/// number of rows enqueued, so the caller knows whether to wake the worker, and
/// returns 0 for the synchronous path because there is nothing to wake.
///
/// Keeping the branch here rather than at each call site matters: there are seven
/// write paths, and a single one that enqueued while also applying inline would
/// double-apply, while one that did neither would silently stop indexing.
///
/// `old_item` and `new_item` follow the same convention as `sync_indexes`: a put
/// supplies both when replacing, a delete supplies only the old.
pub(crate) async fn maintain_vector_indexes(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table_id: &str,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    delay_ms: u64,
) -> Result<usize, StorageError> {
    // Read inside the transaction rather than from the cached `TableKeyInfo`: the
    // cache carries the search schema but not the index id, and the id is what
    // names the data table.
    let metas = fetch_vector_indexes_for_table(tx, table_id).await?;
    if metas.is_empty() {
        return Ok(0);
    }
    let key_cols = base_key_columns(base_key_schema, attr_defs);

    if delay_ms == 0 {
        for meta in &metas {
            apply_vector_index(
                tx,
                table_id,
                meta,
                base_key_schema,
                attr_defs,
                &key_cols,
                old_item,
                new_item,
            )
            .await?;
        }
        return Ok(0);
    }

    let mut enqueued = 0usize;
    for meta in metas {
        // Enqueued even when the new item carries no vector: the removal is the
        // work in that case, and skipping it would leave a stale row indexed.
        let context = super::index::PendingApplyContext::Vector(VectorApplyContext {
            base_key_schema: base_key_schema.to_vec(),
            attribute_definitions: attr_defs.to_vec(),
            table_id: table_id.to_owned(),
            vector: meta,
        });
        super::index::enqueue_pending_row(tx, table_id, old_item, new_item, delay_ms, &context)
            .await?;
        enqueued += 1;
    }
    Ok(enqueued)
}

/// Apply one claimed vector pending row, from its self-describing context.
///
/// A missing data table is skipped rather than treated as a failure: the base table
/// or the index itself can be dropped while a row is in flight, which is a routine
/// race and not a defect.
///
/// This is log hygiene rather than data safety, and worth being exact about. The
/// batch already guards every row with a savepoint, so without this the row would be
/// rolled back and dropped, reaching the same end state by a noisier route. What the
/// tolerance changes is that an expected race stops emitting an ERROR line, which
/// otherwise trains operators to ignore the one signal that says a row was thrown
/// away. Matches the GSI sibling, so both arms of the dispatcher behave alike.
pub(crate) async fn apply_vector_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    context: &VectorApplyContext,
) -> Result<(), StorageError> {
    let key_cols = base_key_columns(&context.base_key_schema, &context.attribute_definitions);
    apply_vector_index(
        tx,
        &context.table_id,
        &context.vector,
        &context.base_key_schema,
        &context.attribute_definitions,
        &key_cols,
        old_item,
        new_item,
    )
    .await
    .or_else(|e| {
        if super::index::is_no_such_table(&e) {
            Ok(())
        } else {
            Err(e)
        }
    })
}

/// Apply an item write to a single vector index.
///
/// The delete-then-insert shape matters. An item can move between partitions when
/// its HASH attribute changes, and the row is keyed by the base item rather than by
/// the partition, so an insert alone would leave the old partition's row in place
/// and the item would be findable in two partitions at once.
///
/// The delete keys off `old_item.or(new_item)` because the base key is immutable, so
/// either carries it. That is what lets a put whose caller had no reason to read the
/// old item still displace the row it replaces.
#[allow(clippy::too_many_arguments)]
async fn apply_vector_index(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table_id: &str,
    meta: &VectorIndexMeta,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    key_cols: &[String],
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<(), StorageError> {
    let vec_table = vector_table_name(table_id, &meta.index_id);
    let where_clause = key_cols
        .iter()
        .map(|c| format!("{c} = ?"))
        .collect::<Vec<_>>()
        .join(" AND ");

    // Remove any existing row for this base item first, whatever partition it
    // was in.
    if let Some(source) = old_item.or(new_item) {
        let binds = base_key_binds(source, base_key_schema, attr_defs)?;
        let sql = format!("DELETE FROM {vec_table} WHERE {where_clause}");
        let mut q = sqlx::query(&sql);
        for b in binds {
            q = super::bind_bound!(q, b);
        }
        q.execute(&mut **tx)
            .await
            .map_err(crate::sqlite_util::map_sqlx_err)?;
    }

    let Some(new_item) = new_item else {
        return Ok(()); // A delete: removal above is the whole of the work.
    };
    insert_vector_row(
        tx,
        table_id,
        meta,
        new_item,
        base_key_schema,
        attr_defs,
        key_cols,
    )
    .await
}

/// Write one item's row into one vector index.
///
/// Shared by the write path and by backfill deliberately. These are the only two
/// producers of a vector row, and a second copy of this logic would be free to
/// drift: a backfilled row shaped differently from a live-written one would search
/// correctly right up until the difference mattered, with nothing to catch it.
///
/// A non-indexable item is a no-op rather than an error, which is what makes a
/// backfill over a table where only some items carry the vector work.
pub(crate) async fn insert_vector_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table_id: &str,
    meta: &VectorIndexMeta,
    item: &Item,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    key_cols: &[String],
) -> Result<(), StorageError> {
    if !item_is_indexable(item, meta) {
        return Ok(());
    }
    let vec_table = vector_table_name(table_id, &meta.index_id);

    let value = item.get(&meta.vector_attribute_name).ok_or_else(|| {
        StorageError::Internal("indexable check passed but the vector is absent".to_owned())
    })?;
    let components = vector_components(value).ok_or_else(|| {
        // Core validates the write before it reaches storage, so a malformed
        // vector here means validation was bypassed rather than that a caller
        // sent bad input.
        StorageError::Internal(
            "vector attribute reached storage without passing validation".to_owned(),
        )
    })?;
    if components.len() != meta.dimensions {
        return Err(StorageError::Internal(format!(
            "vector has {} components, index declares {}",
            components.len(),
            meta.dimensions
        )));
    }

    let mut blob = Vec::with_capacity(components.len() * 4);
    for x in &components {
        blob.extend_from_slice(&x.to_le_bytes());
    }
    let norm = vector_norm(&components);
    let part = item_partition(item, meta)?;
    // Projected exactly as the GSI sibling projects, so a search returns what
    // the index declares and no more.
    let mut projected =
        super::index::project_item_for_index(item, &[], base_key_schema, &meta.projection);
    // The SearchSchema attributes are always projected, whatever the
    // ProjectionType. See `search_schema_attribute_names` for why: the inline
    // filter is evaluated against this payload, so dropping the attribute would
    // silently turn every filtered search into a zero-result search.
    for name in &meta.search_schema_attribute_names {
        if !projected.contains_key(name)
            && let Some(v) = item.get(name)
        {
            projected.insert(name.clone(), v.clone());
        }
    }
    // The vector itself is not kept in the payload: it is already in the `vec`
    // column as `f32`, which is the width the service validates against, and the
    // search path rebuilds the attribute from those bits. Keeping a verbatim
    // decimal copy here duplicated 10 to 15 KB per row at 1024 dimensions and
    // would have returned the client's original precision where the service
    // returns the narrowed value.
    projected.remove(&meta.vector_attribute_name);
    let item_json = serde_json::to_string(&projected)
        .map_err(|e| StorageError::Internal(format!("serialize item: {e}")))?;

    // A plain INSERT, deliberately, where the GSI sibling uses INSERT OR REPLACE.
    // Every caller reaches this through `apply_vector_index`, which unconditionally
    // deletes the base key's row first, so no live row can exist here and a conflict
    // is impossible. Keeping it a plain INSERT means that if a future refactor ever
    // makes that delete conditional, this fails loudly with a primary key violation
    // rather than silently replacing a row and hiding the broken invariant.
    let cols = std::iter::once("part".to_owned())
        .chain(key_cols.iter().cloned())
        .chain(["vec".to_owned(), "nrm".to_owned(), "item_data".to_owned()])
        .collect::<Vec<_>>();
    let sql = format!(
        "INSERT INTO {vec_table} ({}) VALUES ({binds})",
        cols.join(", "),
        binds = super::bind_list(cols.len())
    );
    let key_binds = base_key_binds(item, base_key_schema, attr_defs)?;
    let mut q = sqlx::query(&sql).bind(part);
    for b in key_binds {
        q = super::bind_bound!(q, b);
    }
    q.bind(blob)
        .bind(f64::from(norm))
        .bind(item_json)
        .execute(&mut **tx)
        .await
        .map_err(crate::sqlite_util::map_sqlx_err)?;
    Ok(())
}

/// Populate a newly created vector index from the base table.
///
/// Batched by offset exactly as `backfill_gsi` is, and for the same reason: a table
/// large enough to be worth indexing is too large to hold in memory. Returns the
/// number of rows written, which is what distinguishes "backfilled nothing because
/// no item carries the vector" from "backfilled nothing because the scan is broken".
/// Everything a backfill needs that does not change between batches.
///
/// Bundled because the alternative was an eight-argument function threaded through two
/// drivers, where the only per-batch values are the page size and the cursor.
struct BackfillPlan<'a> {
    table_id: &'a str,
    meta: &'a VectorIndexMeta,
    base_key_schema: &'a [KeySchemaElement],
    attr_defs: &'a [AttributeDefinition],
    key_cols: Vec<String>,
}

impl<'a> BackfillPlan<'a> {
    fn new(
        table_id: &'a str,
        meta: &'a VectorIndexMeta,
        base_key_schema: &'a [KeySchemaElement],
        attr_defs: &'a [AttributeDefinition],
    ) -> Self {
        Self {
            table_id,
            meta,
            base_key_schema,
            attr_defs,
            key_cols: base_key_columns(base_key_schema, attr_defs),
        }
    }
}

/// Backfill one batch of existing rows into the vector index.
///
/// Returns `(written, fetched, last_rowid)`. `fetched` distinguishes a short read
/// (the end) from a full one, and `last_rowid` is the cursor to resume from.
///
/// Pagination is by KEY, not by `OFFSET`. Offset anchors on a position, so removing any
/// already-scanned row shifts every later position by one and the next batch skips a
/// row entirely. That row is then missing from the index permanently, and no queue
/// entry can repair it, because the skipped row was never written to: only the removed
/// one was. Reproduced before this change: one removal during a backfill left the row
/// at the batch boundary absent from the index.
///
/// The cursor is `rowid`, not `pk`, and the difference is a correctness matter rather
/// than a preference. Composite-key base tables have `PRIMARY KEY (pk, sk*)`, so `pk`
/// alone is not unique; a `pk > last_pk` cursor whose batch boundary fell inside one
/// partition's sort-key group excluded every remaining row sharing that `pk`,
/// permanently, on both the UpdateTable path and startup reconciliation (which share
/// this function). Reproduced: a five-row partition scanned with a batch of three
/// indexed four rows and skipped two. `rowid` is unique regardless of the key layout,
/// so one query shape serves both. It is a valid cursor because the base tables are
/// ordinary rowid tables and every write is `INSERT ... ON CONFLICT DO UPDATE`
/// (`tx_helpers.rs`), which updates in place and never reassigns a rowid; rows
/// inserted after a batch passed their rowid position are the concurrent writes the
/// queue hold already captures and replays after ACTIVE.
///
/// It was unreachable while the whole backfill ran in one transaction, and became
/// reachable the moment batches started committing independently.
/// One batch's outcome: rows indexed, rows skipped as poison, rows fetched
/// (for termination), and the cursor for the next batch.
struct BatchOutcome {
    written: usize,
    skipped: usize,
    fetched: i64,
    last_rowid: i64,
}

async fn backfill_vector_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    plan: &BackfillPlan<'_>,
    limit: i64,
    after_rowid: i64,
) -> Result<BatchOutcome, StorageError> {
    let base_table = super::data_table_name(plan.table_id);
    // `rowid > ?` with 0 as the initial cursor: every real rowid is positive, so
    // the first batch needs no separate query shape.
    let sql =
        format!("SELECT rowid, item_data FROM {base_table} WHERE rowid > ? ORDER BY rowid LIMIT ?");
    let rows: Vec<(i64, String)> = sqlx::query_as(&sql)
        .bind(after_rowid)
        .bind(limit)
        .fetch_all(&mut **tx)
        .await
        .map_err(crate::sqlite_util::map_sqlx_err)?;
    let fetched = i64::try_from(rows.len()).unwrap_or(limit);
    let last_rowid = rows.last().map_or(after_rowid, |(rid, _)| *rid);
    let mut written = 0usize;
    let mut skipped = 0usize;
    for (rowid, item_json) in rows {
        // Poison classification. The live write path treats a malformed vector
        // as an invariant violation and errors loudly, because core validation
        // ran before storage was reached. That reasoning is FALSE here: rows
        // written before the index existed never passed vector validation, so
        // a malformed or wrong-dimension vector in the base table is expected
        // input for a backfill, not a bug. Propagating it wedged the build in
        // an infinite recovery loop: the error left the index CREATING, the
        // watchdog re-ran the rebuild, and the same row failed again, forever,
        // while the CREATING hold also froze every queued index write for the
        // table. A row whose stored bytes cannot enter the index is skipped
        // and counted instead, exactly as a GSI omits an item whose key
        // attribute has the wrong type. Transient failures (the INSERT itself
        // erroring) still propagate: those are retryable and must not drop
        // rows.
        let Ok(item) = serde_json::from_str::<Item>(&item_json) else {
            tracing::warn!(
                rowid,
                index = %plan.meta.index_id,
                "backfill: stored item is unparseable; skipping row"
            );
            skipped += 1;
            continue;
        };
        if !item_is_indexable(&item, plan.meta) {
            continue;
        }
        let vector_ok = item
            .get(&plan.meta.vector_attribute_name)
            .and_then(vector_components)
            .is_some_and(|c| c.len() == plan.meta.dimensions);
        if !vector_ok {
            tracing::warn!(
                rowid,
                index = %plan.meta.index_id,
                "backfill: vector attribute malformed or wrong dimension; skipping row"
            );
            skipped += 1;
            continue;
        }
        insert_vector_row(
            tx,
            plan.table_id,
            plan.meta,
            &item,
            plan.base_key_schema,
            plan.attr_defs,
            &plan.key_cols,
        )
        .await?;
        written += 1;
    }
    Ok(BatchOutcome {
        written,
        skipped,
        fetched,
        last_rowid,
    })
}

/// A completed backfill: rows indexed and rows skipped as poison. `skipped`
/// is recorded on the catalog row so an ACTIVE index that deliberately omits
/// rows says so, rather than the omission being indistinguishable from a bug.
pub(crate) struct BackfillOutcome {
    pub(crate) written: usize,
    pub(crate) skipped: usize,
}

/// Backfill the index in independently committed batches, releasing SQLite's write
/// lock between them.
///
/// This is what lets the base table stay writable while an index builds, which is how
/// the service behaves: the table remains ACTIVE and accepts writes throughout, and
/// only the index reports CREATING. Holding one transaction for the whole backfill
/// would block every write until it finished.
///
/// Releasing the lock is also what creates the ordering hazard this design has to
/// answer. A write landing mid-backfill is enqueued, and if it were applied before the
/// backfill wrote its older snapshot of the same item, the index would converge on the
/// stale generation. The queue worker therefore refuses to claim any row for a table
/// whose vector index is still CREATING, so those writes accumulate and are applied
/// only after this returns and the index flips to ACTIVE.
///
/// A crash part-way leaves the index in CREATING with some rows written, which
/// `reconcile_incomplete_vector_indexes` repairs at startup by rebuilding it.
pub(crate) async fn backfill_vector_index_in_batches(
    pool: &sqlx::SqlitePool,
    write_lock: &tokio::sync::Mutex<()>,
    table_id: &str,
    meta: &VectorIndexMeta,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    batch_delay: std::time::Duration,
) -> Result<BackfillOutcome, StorageError> {
    const BATCH: i64 = 500;
    let plan = BackfillPlan::new(table_id, meta, base_key_schema, attr_defs);
    let mut cursor: i64 = 0;
    let mut written = 0usize;
    let mut skipped = 0usize;
    loop {
        let outcome = {
            let _writer = write_lock.lock().await;
            let mut tx = pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(crate::sqlite_util::map_sqlx_err)?;
            let result = backfill_vector_batch(&mut tx, &plan, BATCH, cursor).await?;
            tx.commit()
                .await
                .map_err(crate::sqlite_util::map_sqlx_err)?;
            result
        };
        written += outcome.written;
        skipped += outcome.skipped;
        if outcome.fetched < BATCH {
            break;
        }
        cursor = outcome.last_rowid;
        // Outside the lock, so a write can actually proceed during the pause. Zero in
        // production; a test sets it so a write is guaranteed to land mid-backfill.
        if !batch_delay.is_zero() {
            tokio::time::sleep(batch_delay).await;
        }
    }
    Ok(BackfillOutcome { written, skipped })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A composite-key table whose partition spans a batch boundary must still
    /// backfill every row.
    ///
    /// This is the regression test for a silent data-loss defect: the cursor was
    /// `pk > last_pk`, and on a table with `PRIMARY KEY (pk, sk_s)` a batch ending
    /// inside one partition's sort-key group excluded every remaining row sharing
    /// that `pk`. Five rows in one partition scanned with a batch of three indexed
    /// four and skipped two, permanently, on both the UpdateTable path and startup
    /// reconciliation, which share `backfill_vector_batch`. The rowid cursor cannot
    /// lose rows because rowid is unique whatever the key layout.
    ///
    /// Driven through `backfill_vector_batch` directly with a batch of 3 rather
    /// than through the drivers, because they hardcode a 500-row batch and seeding
    /// 501 rows would test the same lines slower.
    #[tokio::test]
    async fn a_composite_key_partition_straddling_a_batch_boundary_is_fully_backfilled() {
        use extenddb_core::types::{
            AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
        };
        let engine = crate::SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");

        let ks = vec![
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            },
        ];
        let ad = vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ];

        let table_id = "t-composite";
        let mut tx = engine.pool.begin_with("BEGIN IMMEDIATE").await.expect("tx");
        crate::SqliteEngine::create_data_table(&mut tx, table_id, &ks, &ad)
            .await
            .expect("base table");
        crate::SqliteEngine::create_vector_data_table(&mut tx, table_id, "vidx-1", &ks, &ad)
            .await
            .expect("vector table");

        // One partition with five sort keys plus a second partition, so the batch
        // of three ends INSIDE partition "a": exactly the boundary that lost rows.
        let base_table = super::super::data_table_name(table_id);
        for (pk, sk) in [
            ("a", "1"),
            ("a", "2"),
            ("a", "3"),
            ("a", "4"),
            ("a", "5"),
            ("b", "1"),
        ] {
            sqlx::query(&format!(
                "INSERT INTO {base_table} (pk, sk_s, item_data) VALUES (?, ?, ?)"
            ))
            .bind(pk)
            .bind(sk)
            .bind(format!(
                r#"{{"pk":{{"S":"{pk}"}},"sk":{{"S":"{sk}"}},"emb":{{"L":[{{"N":"1"}},{{"N":"0"}}]}}}}"#
            ))
            .execute(&mut *tx)
            .await
            .expect("seed");
        }

        let meta = VectorIndexMeta {
            index_id: "vidx-1".to_owned(),
            dimensions: 2,
            vector_attribute_name: "emb".to_owned(),
            projection: extenddb_core::types::Projection {
                projection_type: extenddb_core::types::ProjectionType::All,
                non_key_attributes: None,
            },
            hash_attribute_name: None,
            search_schema_attribute_names: Vec::new(),
        };
        let plan = BackfillPlan::new(table_id, &meta, &ks, &ad);

        let mut cursor: i64 = 0;
        let mut written = 0usize;
        loop {
            let outcome = backfill_vector_batch(&mut tx, &plan, 3, cursor)
                .await
                .expect("batch");
            written += outcome.written;
            if outcome.fetched < 3 {
                break;
            }
            cursor = outcome.last_rowid;
        }
        tx.commit().await.expect("commit");

        assert_eq!(
            written, 6,
            "every row must be indexed; the pk-only cursor wrote 4 and skipped 2"
        );
        let vec_table = super::super::vector_table_name(table_id, "vidx-1");
        let (rows,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {vec_table}"))
            .fetch_one(&engine.pool)
            .await
            .expect("count");
        assert_eq!(rows, 6, "the index must hold all six rows");
    }

    /// A row whose stored bytes cannot enter the index is skipped and counted,
    /// not propagated. Propagating it wedged the build permanently: the error
    /// left the index CREATING, the watchdog re-ran the rebuild, the same row
    /// failed again, and the CREATING hold froze every queued index write for
    /// the table. This is the review finding on this file: "non-conformant
    /// items keep failing and the index creation will be stuck in recovery
    /// loop forever".
    ///
    /// Discriminating by construction: the poison rows (a wrong-dimension
    /// vector, a non-list vector, and unparseable item bytes) sit BETWEEN good
    /// rows, so the pre-fix behaviour (error on first poison row, nothing
    /// after it indexed) cannot produce these counts.
    #[tokio::test]
    async fn poison_rows_are_skipped_and_counted_rather_than_wedging_the_backfill() {
        use extenddb_core::types::{
            AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
        };
        let engine = crate::SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");

        let ks = vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let ad = vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];

        let table_id = "t-poison";
        let mut tx = engine.pool.begin_with("BEGIN IMMEDIATE").await.expect("tx");
        crate::SqliteEngine::create_data_table(&mut tx, table_id, &ks, &ad)
            .await
            .expect("base table");
        crate::SqliteEngine::create_vector_data_table(&mut tx, table_id, "vidx-p", &ks, &ad)
            .await
            .expect("vector table");

        let base_table = super::super::data_table_name(table_id);
        // good, wrong-dimension, good, non-list vector, unparseable, good.
        let rows: [(&str, String); 6] = [
            (
                "g1",
                r#"{"pk":{"S":"g1"},"emb":{"L":[{"N":"1"},{"N":"0"}]}}"#.to_owned(),
            ),
            (
                "p1",
                r#"{"pk":{"S":"p1"},"emb":{"L":[{"N":"1"}]}}"#.to_owned(),
            ),
            (
                "g2",
                r#"{"pk":{"S":"g2"},"emb":{"L":[{"N":"0"},{"N":"1"}]}}"#.to_owned(),
            ),
            (
                "p2",
                r#"{"pk":{"S":"p2"},"emb":{"S":"not-a-vector"}}"#.to_owned(),
            ),
            ("p3", "{not json".to_owned()),
            (
                "g3",
                r#"{"pk":{"S":"g3"},"emb":{"L":[{"N":"1"},{"N":"1"}]}}"#.to_owned(),
            ),
        ];
        for (pk, item) in &rows {
            sqlx::query(&format!(
                "INSERT INTO {base_table} (pk, item_data) VALUES (?, ?)"
            ))
            .bind(pk)
            .bind(item)
            .execute(&mut *tx)
            .await
            .expect("seed");
        }

        let meta = VectorIndexMeta {
            index_id: "vidx-p".to_owned(),
            dimensions: 2,
            vector_attribute_name: "emb".to_owned(),
            projection: extenddb_core::types::Projection {
                projection_type: extenddb_core::types::ProjectionType::All,
                non_key_attributes: None,
            },
            hash_attribute_name: None,
            search_schema_attribute_names: Vec::new(),
        };
        let plan = BackfillPlan::new(table_id, &meta, &ks, &ad);

        let outcome = backfill_vector_batch(&mut tx, &plan, 100, 0)
            .await
            .expect("a batch containing poison rows must still complete");
        tx.commit().await.expect("commit");

        assert_eq!(outcome.written, 3, "the three good rows are indexed");
        assert_eq!(outcome.skipped, 3, "the three poison rows are counted");

        // The good rows AFTER the poison rows made it in, which is the part the
        // pre-fix behaviour cannot do: it stopped at p1.
        let vec_table = super::super::vector_table_name(table_id, "vidx-p");
        let (rows,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {vec_table}"))
            .fetch_one(&engine.pool)
            .await
            .expect("count");
        assert_eq!(rows, 3);
    }
}
