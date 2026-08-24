// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Vector index maintenance on the write path.
//!
//! One entry point, [`maintain_vector_indexes`], called from every site that
//! writes a base item. It reads the index set, decides per index whether the
//! change applies inline or goes on the propagation queue, and returns how many
//! rows it enqueued so the caller knows whether to wake a worker.
//!
//! Three properties are deliberate here, each one a defect the SQLite
//! implementation has and this one must not inherit:
//!
//! 1. **Membership is never read from the cached `TableKeyInfo`.** A cached empty
//!    set makes a write skip an index that another request has just created. The
//!    metadata is read per write from the catalog, in the same round trip that
//!    already reads the secondary indexes, so a new index takes effect the moment
//!    its catalog row commits. Note what \"same round trip\" can and cannot mean
//!    here: `vector_indexes` lives in the catalog database and the write runs on
//!    the data database, so no single transaction spans them. Freshness parity
//!    with a GSI is the achievable property, and it is the one that matters.
//! 2. **A malformed or wrong-dimension stored vector is non-indexable, not an
//!    error.** Such an item cannot have passed live validation, so it arrived
//!    before the index existed and was skipped by the backfill. Failing the write
//!    would make an unrelated update to that item impossible; the row is removed
//!    from the index and the write proceeds.
//! 3. **A write to a CREATING index always enqueues**, at any delay including
//!    zero. Applying inline would race the backfill's older snapshot of the same
//!    item, and the backfill's plain INSERT would then collide. Only an ACTIVE
//!    index at delay zero is applied inline.

use extenddb_core::types::{AttributeDefinition, Item, KeySchemaElement, ScalarAttributeType};
use extenddb_core::validation::vector_item::{vector_components, vector_norm};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    SortKeyValue, composite_pk_to_text, parse_sk, sk_column, sk_column_n,
};
use extenddb_storage::vector_lifecycle::{
    VectorApplyContext, VectorIndexMeta, item_is_indexable, item_partition, projected_payload,
};
use pgvector::Vector;

use super::{all_sort_key_info, vector_table_name};

/// Read the vector index metadata for a table from the catalog.
///
/// Every field the write path needs, including the index id that names the data
/// table, which is exactly what the cached `TableKeyInfo` cannot supply.
///
/// Returns the rows with their status, because the status decides inline versus
/// enqueue and only this read can see it.
pub(crate) async fn fetch_vector_indexes_for_table(
    catalog: &sqlx::PgPool,
    table_id: &str,
) -> Result<Vec<(VectorIndexMeta, String)>, StorageError> {
    let rows: Vec<(
        String,
        i32,
        serde_json::Value,
        Option<serde_json::Value>,
        serde_json::Value,
        String,
    )> = sqlx::query_as(
        "SELECT index_id, dimensions, vector_attribute, search_schema, projection, index_status \
         FROM vector_indexes WHERE table_id = $1 ORDER BY index_name",
    )
    .bind(table_id)
    .fetch_all(catalog)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    let mut out = Vec::with_capacity(rows.len());
    for (index_id, dimensions, vector_attribute, search_schema, projection, index_status) in rows {
        let attr: extenddb_core::types::VectorAttribute = serde_json::from_value(vector_attribute)
            .map_err(|e| StorageError::Internal(format!("vector_attribute: {e}")))?;
        let search_schema: Vec<extenddb_core::types::SearchSchemaElement> = match search_schema {
            Some(value) => serde_json::from_value(value)
                .map_err(|e| StorageError::Internal(format!("vector search_schema: {e}")))?,
            None => Vec::new(),
        };
        let projection: extenddb_core::types::Projection = serde_json::from_value(projection)
            .map_err(|e| StorageError::Internal(format!("vector projection: {e}")))?;
        let hash_attribute_name = search_schema
            .iter()
            .find(|e| e.element_type == extenddb_core::types::SearchSchemaElementType::Hash)
            .map(|e| e.attribute_name.clone());
        out.push((
            VectorIndexMeta {
                index_id,
                dimensions: usize::try_from(dimensions).map_err(|_| {
                    StorageError::Internal(format!("vector dimensions out of range: {dimensions}"))
                })?,
                vector_attribute_name: attr.attribute_name,
                projection,
                hash_attribute_name,
                search_schema_attribute_names: search_schema
                    .iter()
                    .map(|e| e.attribute_name.clone())
                    .collect(),
            },
            index_status,
        ));
    }
    Ok(out)
}

/// The base table's key columns for a vector data table, in insert order.
fn base_key_columns(base_sks: &[(&str, ScalarAttributeType)]) -> Vec<String> {
    let mut cols = vec!["base_pk".to_owned()];
    for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
        let col = if i == 0 {
            format!("base_{}", sk_column(sk_type))
        } else {
            format!("base_{}", sk_column_n(i, sk_type))
        };
        cols.push(col);
    }
    cols
}

/// Maintain every vector index on a table for one base-item change.
///
/// `old_item` and `new_item` are the before and after images: a put has both when
/// it replaces, a delete has only `old_item`, and either may be absent.
///
/// `metas` is the caller's own fresh read, passed in rather than fetched here: the
/// same read also decides whether the write needs a transaction at all, and doing
/// it twice would be two chances to disagree.
///
/// Returns the number of rows enqueued, so the caller can wake the queue only when
/// there is something to drain.
// The parameters mirror the write sites' own locals: the transaction, the catalog
// pool the metadata comes from, the table's identity and key shape, the two images,
// and the delay. A wrapper struct would have to be built at all six call sites from
// exactly these values.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn maintain_vector_indexes(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    metas: &[(VectorIndexMeta, String)],
    table_id: &str,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    delay_ms: u64,
) -> Result<usize, StorageError> {
    if metas.is_empty() {
        return Ok(0);
    }

    let mut enqueued = 0usize;
    for (meta, index_status) in metas {
        // A CREATING index never takes an inline write, whatever the delay. The
        // backfill is scanning the base table and holds an older snapshot of this
        // same item; writing the new one now would let the backfill overwrite it,
        // and its deliberately plain INSERT would collide with the row this put
        // there. The queue hold parks the row until the index is published.
        let inline = delay_ms == 0 && index_status == "ACTIVE";
        if inline {
            apply_vector_index(tx, meta, base_key_schema, attr_defs, old_item, new_item).await?;
            continue;
        }
        // Enqueued even when the new image carries no vector: the removal is the
        // work in that case, and skipping it would leave a stale row indexed.
        let context = crate::gsi_queue::PendingApplyContext::Vector(VectorApplyContext {
            base_key_schema: base_key_schema.to_vec(),
            attribute_definitions: attr_defs.to_vec(),
            table_id: table_id.to_owned(),
            vector: meta.clone(),
        });
        super::index::enqueue_pending_row(tx, table_id, old_item, new_item, delay_ms, &context)
            .await?;
        enqueued += 1;
    }
    Ok(enqueued)
}

/// Apply one base-item change to one vector index.
///
/// Delete then insert, unconditionally, because the partition column is part of
/// the row and a changed HASH value moves the row rather than updating it in
/// place. The delete also makes the insert below a plain one.
pub(crate) async fn apply_vector_index(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    meta: &VectorIndexMeta,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<(), StorageError> {
    let vec_table = vector_table_name(&meta.index_id);
    let base_sks = all_sort_key_info(base_key_schema, attr_defs);
    let key_cols = base_key_columns(&base_sks);

    if let Some(source) = old_item.or(new_item) {
        let where_clause = key_cols
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{c} = ${}", i + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("DELETE FROM {vec_table} WHERE {where_clause}");
        let mut query = sqlx::query(&sql).bind(composite_pk_to_text(source, base_key_schema)?);
        for &(sk_name, sk_type) in &base_sks {
            if let Some(value) = source.get(sk_name) {
                query = match parse_sk(value, sk_type)? {
                    SortKeyValue::S(s) => query.bind(s),
                    SortKeyValue::N(n) => query.bind(n),
                    SortKeyValue::B(b) => query.bind(b),
                };
            }
        }
        query
            .execute(&mut **tx)
            .await
            .map_err(crate::vector::map_vector_sql_error)?;
    }

    let Some(new_item) = new_item else {
        // A delete: the removal above is the whole of the work.
        return Ok(());
    };
    insert_vector_row(tx, meta, new_item, base_key_schema, attr_defs).await
}

/// Write one item's row into one vector index.
///
/// A non-indexable item is a no-op, which is what lets an index exist on a table
/// where only some items carry the vector.
///
/// Stored bytes that cannot enter the index are also non-indexable rather than an
/// error, and that is the difference from the SQLite implementation. Live writes
/// are validated by core before they reach storage, so a malformed or
/// wrong-dimension vector here belongs to an item written before the index
/// existed, which the backfill skipped and counted. Failing would make every
/// later update to that item fail too, including an update that has nothing to do
/// with the vector.
pub(crate) async fn insert_vector_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    meta: &VectorIndexMeta,
    item: &Item,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), StorageError> {
    if !item_is_indexable(item, meta) {
        return Ok(());
    }
    let Some(value) = item.get(&meta.vector_attribute_name) else {
        return Ok(());
    };
    let Some(components) = vector_components(value) else {
        tracing::warn!(
            index_id = %meta.index_id,
            attribute = %meta.vector_attribute_name,
            "stored vector attribute cannot be read as a vector; leaving the item unindexed"
        );
        return Ok(());
    };
    if components.len() != meta.dimensions {
        tracing::warn!(
            index_id = %meta.index_id,
            found = components.len(),
            declared = meta.dimensions,
            "stored vector has the wrong dimension count; leaving the item unindexed"
        );
        return Ok(());
    }

    let vec_table = vector_table_name(&meta.index_id);
    let base_sks = all_sort_key_info(base_key_schema, attr_defs);
    let key_cols = base_key_columns(&base_sks);
    let part = item_partition(item, meta)?;
    let norm = vector_norm(&components);
    // Projection, the always-projected SearchSchema attributes, and the stripped
    // vector attribute are the shared payload rules, so a live-written row and a
    // backfilled one cannot differ in shape.
    let projected = projected_payload(item, base_key_schema, meta);
    let item_json =
        serde_json::to_value(&projected).map_err(|e| StorageError::Internal(e.to_string()))?;

    // A plain INSERT, deliberately, where the GSI sibling upserts. Every caller
    // reaches this through `apply_vector_index`, which deletes the base key's row
    // first, so no live row can exist here. Keeping it plain means that if a
    // future change ever makes that delete conditional, this fails loudly on the
    // primary key rather than quietly replacing a row and hiding the break.
    let mut cols = vec!["part".to_owned()];
    cols.extend(key_cols.iter().cloned());
    cols.extend([
        "embedding".to_owned(),
        "nrm".to_owned(),
        "item_data".to_owned(),
    ]);
    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("${i}")).collect();
    let sql = format!(
        "INSERT INTO {vec_table} ({}) VALUES ({})",
        cols.join(", "),
        placeholders.join(", ")
    );

    // As bytes: the column is BYTEA because the unscoped sentinel carries a NUL,
    // which PostgreSQL rejects in a text column.
    let mut query = sqlx::query(&sql)
        .bind(part.into_bytes())
        .bind(composite_pk_to_text(item, base_key_schema)?);
    for &(sk_name, sk_type) in &base_sks {
        if let Some(value) = item.get(sk_name) {
            query = match parse_sk(value, sk_type)? {
                SortKeyValue::S(s) => query.bind(s),
                SortKeyValue::N(n) => query.bind(n),
                SortKeyValue::B(b) => query.bind(b),
            };
        }
    }
    query
        .bind(Vector::from(components))
        .bind(f64::from(norm))
        .bind(item_json)
        .execute(&mut **tx)
        .await
        .map_err(crate::vector::map_vector_sql_error)?;
    Ok(())
}

/// Apply one claimed queue row to its vector index, from the row's own context.
pub async fn apply_claimed_vector_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    context: &VectorApplyContext,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<(), StorageError> {
    apply_vector_index(
        tx,
        &context.vector,
        &context.base_key_schema,
        &context.attribute_definitions,
        old_item,
        new_item,
    )
    .await
}

/// The PostgreSQL backfill driver for one vector index.
///
/// Supplies the storage primitives the shared lifecycle drives. Everything about
/// ordering, poison handling and the failure contract lives in
/// `extenddb_storage::vector_lifecycle`; this is SQL and transactions.
pub(crate) struct PostgresVectorBuild {
    pub(crate) catalog: sqlx::PgPool,
    pub(crate) data: sqlx::PgPool,
    pub(crate) queue_notify: Option<std::sync::Arc<crate::gsi_queue::GsiQueue>>,
    pub(crate) table_id: String,
    pub(crate) index_id: String,
    pub(crate) base_key_schema: Vec<KeySchemaElement>,
    pub(crate) attribute_definitions: Vec<AttributeDefinition>,
    pub(crate) dimensions: u32,
    pub(crate) meta: Option<VectorIndexMeta>,
}

/// The backfill's position in the base table: the whole primary key.
///
/// A keyset cursor rather than an offset, and the FULL key rather than the
/// partition alone. Both choices are load-bearing. An offset shifts when a
/// concurrent delete removes an earlier row, which silently skips a row that was
/// never indexed; a partition-only cursor loses rows inside a composite-key
/// partition, because the next batch resumes past the whole partition. PostgreSQL
/// has no rowid to fall back on, which is what the shared contract predicted.
#[derive(Debug, Clone)]
pub(crate) struct BaseKeyCursor {
    pk: String,
    /// The sort key values, in key order. Owned scalars rather than the shared
    /// bind enum, which is neither `Clone` nor `Debug` and does not need to be.
    sort_keys: Vec<CursorKey>,
}

/// One sort key value in a backfill cursor.
#[derive(Debug, Clone)]
enum CursorKey {
    S(String),
    N(sqlx::types::BigDecimal),
    B(Vec<u8>),
}

impl std::fmt::Display for BaseKeyCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Only ever used for a log line naming the row a poison skip happened on.
        write!(f, "pk={}", self.pk)
    }
}

impl PostgresVectorBuild {
    /// Load the index definition from the catalog.
    ///
    /// Read rather than passed in because recovery has no request to read it from:
    /// the `UpdateTable` that created the index is long gone by the time a
    /// reconciler rebuilds it.
    pub(crate) async fn load_meta(&mut self) -> Result<(), StorageError> {
        let metas = fetch_vector_indexes_for_table(&self.catalog, &self.table_id).await?;
        let found = metas
            .into_iter()
            .find(|(meta, _)| meta.index_id == self.index_id)
            .map(|(meta, _)| meta);
        self.meta = Some(found.ok_or_else(|| {
            StorageError::Internal(
                "the vector index catalog row vanished before its build started".to_owned(),
            )
        })?);
        Ok(())
    }

    fn meta(&self) -> Result<&VectorIndexMeta, StorageError> {
        self.meta.as_ref().ok_or_else(|| {
            StorageError::Internal(
                "vector backfill started before the index definition was loaded".to_owned(),
            )
        })
    }
}

impl extenddb_storage::vector_lifecycle::VectorIndexBuild for PostgresVectorBuild {
    type Cursor = BaseKeyCursor;

    async fn backfill_batch(
        &mut self,
        cursor: Option<BaseKeyCursor>,
        limit: i64,
    ) -> Result<extenddb_storage::vector_lifecycle::BatchOutcome<BaseKeyCursor>, StorageError> {
        use extenddb_storage::vector_lifecycle::{BackfillRow, classify_backfill_row};

        let meta = self.meta()?.clone();
        let base_sks = all_sort_key_info(&self.base_key_schema, &self.attribute_definitions);
        let base_table = super::data_table_name(&self.table_id);

        // Order by the whole key so the keyset comparison below is total, and select
        // the key columns because they are the cursor.
        let mut key_cols = vec!["pk".to_owned()];
        for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
            key_cols.push(sk_column_n(i, sk_type));
        }
        let order = key_cols.join(", ");
        let selected = key_cols.join(", ");

        // Row-value comparison, which PostgreSQL evaluates lexicographically over
        // the tuple, so one predicate resumes the scan exactly where it stopped
        // whatever the key arity.
        let (where_clause, has_cursor) = match &cursor {
            Some(_) => {
                let placeholders: Vec<String> =
                    (1..=key_cols.len()).map(|i| format!("${i}")).collect();
                (
                    format!(" WHERE ({order}) > ({})", placeholders.join(", ")),
                    true,
                )
            }
            None => (String::new(), false),
        };
        let limit_param = if has_cursor { key_cols.len() + 1 } else { 1 };
        let sql = format!(
            "SELECT {selected}, item_data FROM {base_table}{where_clause} \
             ORDER BY {order} LIMIT ${limit_param}"
        );

        let mut tx = self
            .data
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut query = sqlx::query(&sql);
        if let Some(cursor) = &cursor {
            query = query.bind(cursor.pk.clone());
            for sk in &cursor.sort_keys {
                query = match sk {
                    CursorKey::S(s) => query.bind(s.clone()),
                    CursorKey::N(n) => query.bind(n.clone()),
                    CursorKey::B(b) => query.bind(b.clone()),
                };
            }
        }
        let rows = query
            .bind(limit)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let fetched = i64::try_from(rows.len()).unwrap_or(i64::MAX);
        let mut written = 0usize;
        let mut skipped = 0usize;
        let mut next_cursor = None;

        for row in &rows {
            use sqlx::Row as _;
            let pk: String = row
                .try_get("pk")
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let mut sort_keys = Vec::with_capacity(base_sks.len());
            for (i, &(_, sk_type)) in base_sks.iter().enumerate() {
                let col = sk_column_n(i, sk_type);
                let value = match sk_type {
                    ScalarAttributeType::S => row
                        .try_get::<Option<String>, _>(col.as_str())
                        .map(|v| CursorKey::S(v.unwrap_or_default())),
                    ScalarAttributeType::N => row
                        .try_get::<Option<sqlx::types::BigDecimal>, _>(col.as_str())
                        .map(|v| CursorKey::N(v.unwrap_or_default())),
                    ScalarAttributeType::B => row
                        .try_get::<Option<Vec<u8>>, _>(col.as_str())
                        .map(|v| CursorKey::B(v.unwrap_or_default())),
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;
                sort_keys.push(value);
            }
            let item_json: serde_json::Value = row
                .try_get("item_data")
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            next_cursor = Some(BaseKeyCursor {
                pk: pk.clone(),
                sort_keys,
            });

            let cursor_label = BaseKeyCursor {
                pk,
                sort_keys: Vec::new(),
            };
            // The classification is the shared rule, so a poison row means the same
            // thing on both backends and the count matches.
            match classify_backfill_row(&item_json.to_string(), &meta, &cursor_label) {
                BackfillRow::Index(item) => {
                    // The classifier parsed the row already, so the item comes from
                    // it rather than being deserialised a second time.
                    insert_vector_row(
                        &mut tx,
                        &meta,
                        &item,
                        &self.base_key_schema,
                        &self.attribute_definitions,
                    )
                    .await?;
                    written += 1;
                }
                BackfillRow::Poison => skipped += 1,
                BackfillRow::Omit => {}
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(extenddb_storage::vector_lifecycle::BatchOutcome {
            written,
            skipped,
            fetched,
            cursor: next_cursor,
        })
    }

    async fn set_backfilling(&mut self) -> Result<(), StorageError> {
        // The owner string is for the operator, not for the code: ownership itself is
        // the advisory lock, and no decision reads this column. What it answers at
        // three in the morning is "which process is building this index", which the
        // lock cannot be asked from another session.
        sqlx::query(
            "UPDATE vector_indexes SET backfilling = true, build_owner = $3, \
             build_heartbeat_at = NOW() WHERE table_id = $1 AND index_id = $2",
        )
        .bind(&self.table_id)
        .bind(&self.index_id)
        .bind(build_owner_label())
        .execute(&self.catalog)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn mark_active(&mut self, skipped: usize) -> Result<(), StorageError> {
        let skipped = i64::try_from(skipped).unwrap_or(i64::MAX);
        // One transition: ACTIVE, the member cleared to absent rather than false,
        // and the skip count recorded so an index that deliberately omits rows says
        // so. The build columns are cleared because ownership ends here.
        sqlx::query(
            "UPDATE vector_indexes SET index_status = 'ACTIVE', backfilling = NULL, \
             skipped_item_count = $3, build_owner = NULL, build_heartbeat_at = NULL \
             WHERE table_id = $1 AND index_id = $2",
        )
        .bind(&self.table_id)
        .bind(&self.index_id)
        .bind(skipped)
        .execute(&self.catalog)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        // The hold goes only after the flip has committed. Held slightly too long
        // is harmless, because the queue rows simply wait; released early would let
        // the worker apply a write against an index that is not yet published.
        release_hold(&self.data, &self.table_id, &self.index_id).await?;
        Ok(())
    }

    async fn reset_data_table(&mut self) -> Result<(), StorageError> {
        // Reload first: a rebuild has no request to read the definition from, and
        // the index may have been altered since the build that died.
        self.load_meta().await?;
        let mut tx = self
            .data
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        crate::PostgresEngine::drop_vector_data_table(&mut tx, &self.index_id).await?;
        crate::PostgresEngine::create_vector_data_table(
            &mut tx,
            &self.index_id,
            self.dimensions,
            &self.base_key_schema,
            &self.attribute_definitions,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    fn notify_active(&mut self) {
        if let Some(queue) = &self.queue_notify {
            queue.notify_workers();
        }
    }

    async fn heartbeat(&mut self) -> Result<(), StorageError> {
        // Renewed between batches so a peer process can tell a slow build from a
        // dead one. The advisory lock proves liveness while the session lives; this
        // column is what a sweep reads without needing to hold the lock.
        sqlx::query(
            "UPDATE vector_indexes SET build_heartbeat_at = NOW() \
             WHERE table_id = $1 AND index_id = $2",
        )
        .bind(&self.table_id)
        .bind(&self.index_id)
        .execute(&self.catalog)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }
}

/// Release holds left behind by a crash.
///
/// `live` is the set of index ids that are legitimately building. Anything else is
/// a leftover: a crash between taking a hold and committing the catalog row, or
/// between deleting an index and releasing its hold. A stale hold silently stops
/// the propagation queue claiming anything for its table.
///
/// Age-bounded, and the bound is doing real work rather than tidying up. Another
/// front-end may have taken a hold and not yet committed its catalog row, so it
/// would not appear in `live`; deleting that hold would let the queue apply writes
/// into the table its backfill is scanning, which is the one ordering rule this
/// table exists to enforce. A crash-orphaned hold is old by definition and an
/// in-flight one is young by definition, so the age separates them without needing
/// to know which process owns what.
async fn sweep_orphan_holds(
    engine: &crate::PostgresEngine,
    live: &[String],
) -> Result<(), StorageError> {
    let swept = sqlx::query(
        "DELETE FROM vector_index_holds \
         WHERE NOT (index_id = ANY($1)) AND created_at < NOW() - INTERVAL '1 minute'",
    )
    .bind(live)
    .execute(&engine.data_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    if swept.rows_affected() > 0 {
        tracing::info!(
            holds = swept.rows_affected(),
            "released vector index build holds left behind by a crash"
        );
    }
    Ok(())
}

/// Take the queue hold for a table whose vector index is about to build.
///
/// Inserted BEFORE the catalog's CREATING row commits, so no writer can enqueue
/// against an index the queue does not yet know to hold.
pub(crate) async fn take_hold(
    data: &sqlx::PgPool,
    table_id: &str,
    index_id: &str,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO vector_index_holds (table_id, index_id) VALUES ($1, $2) \
         ON CONFLICT (table_id, index_id) DO NOTHING",
    )
    .bind(table_id)
    .bind(index_id)
    .execute(data)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

/// Release the queue hold, after the index is published or its build is abandoned.
pub(crate) async fn release_hold(
    data: &sqlx::PgPool,
    table_id: &str,
    index_id: &str,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM vector_index_holds WHERE table_id = $1 AND index_id = $2")
        .bind(table_id)
        .bind(index_id)
        .execute(data)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

/// Who is building, for an operator reading the catalog.
///
/// Host and process id, which is what identifies a front-end in a deployment where
/// several share one database. Nothing branches on this value.
fn build_owner_label() -> String {
    // HOSTNAME is a shell variable rather than an exported one, so it is absent from
    // the environment a service manager gives a unit: the multi-front-end deployment
    // where this column is the only thing that answers "which host" is exactly where
    // reading it alone would degrade to "unknown". /etc/hostname is the fallback.
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|h| h.trim().to_owned())
                .filter(|h| !h.is_empty())
        })
        .unwrap_or_else(|| "unknown".to_owned());
    format!("{host}/{}", std::process::id())
}

/// Namespace for ExtendDB advisory locks, so a lock taken here cannot collide
/// with one taken by another feature that hashed a different string.
const ADVISORY_LOCK_NAMESPACE: i32 = 0x0045_4442;

/// A held build-ownership lock. Dropping it returns the connection to the pool,
/// which releases the session-scoped lock.
pub(crate) struct BuildOwner {
    _conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
}

/// Try to take ownership of one index's build.
///
/// Session-scoped rather than transaction-scoped, because the build spans many
/// transactions, and a session lock dies with its connection: a front-end that
/// crashes mid-build stops owning the build without anyone having to decide that
/// its claim has expired. The heartbeat column exists for the other half of the
/// question, which a lock cannot answer: whether an owner that still holds the
/// lock is making progress.
///
/// Returns `None` when another process owns the build, which is not an error: the
/// other process is doing the work.
pub(crate) async fn build_ownership(data: &sqlx::PgPool, index_id: &str) -> Option<BuildOwner> {
    let mut conn = match data.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::warn!("could not acquire a connection for vector build ownership: {e}");
            return None;
        }
    };
    // hashtext gives a stable i32 for the id, and the namespace keeps this key
    // space separate from the migration lock's.
    let taken: Result<bool, _> =
        sqlx::query_scalar("SELECT pg_try_advisory_lock($1, hashtext($2))")
            .bind(ADVISORY_LOCK_NAMESPACE)
            .bind(index_id)
            .fetch_one(&mut *conn)
            .await;
    match taken {
        Ok(true) => Some(BuildOwner { _conn: conn }),
        Ok(false) => None,
        Err(e) => {
            tracing::warn!("vector build ownership probe failed: {e}");
            None
        }
    }
}

/// Rebuild every vector index a crash left in `CREATING`.
///
/// Runs at startup. There is no failure state on the wire for an index to sit in,
/// so a build that died left its index `CREATING`, and the repair is to rebuild
/// rather than resume: rows already written would collide with the backfill's
/// deliberately plain INSERT.
///
/// Ownership is still taken per index, so several front-ends starting together do
/// not rebuild the same index concurrently, and an index whose build is genuinely
/// still running elsewhere is left to its owner.
///
/// Returns the number of indexes this process rebuilt.
pub async fn reconcile_incomplete_vector_indexes(
    engine: &crate::PostgresEngine,
) -> Result<usize, StorageError> {
    // At startup every CREATING index is stuck by definition: this process has just
    // begun, so no build of its own can be running, and any build from a previous
    // life died with it. Ownership still decides per index, because peers may be
    // starting at the same time.
    rebuild_stuck_vector_indexes(engine, None).await
}

/// Rebuild `CREATING` indexes whose build is not making progress.
///
/// `stale_after` bounds which ones count. `None` means every `CREATING` index,
/// which is the startup case. A running deployment passes a duration, so an index
/// whose heartbeat is recent is left to the process renewing it: that is the
/// question an advisory lock cannot answer, and the reason the heartbeat column
/// exists at all.
///
/// Without this, a build that dies after its first batch leaves its index
/// `CREATING` and its queue hold in place, so the table's whole index propagation
/// stops, and the only exit is a restart.
pub async fn rebuild_stuck_vector_indexes(
    engine: &crate::PostgresEngine,
    stale_after: Option<std::time::Duration>,
) -> Result<usize, StorageError> {
    // A null heartbeat counts as stale: it means the build never reached its first
    // batch, so nothing is renewing it.
    let stale_seconds = stale_after.map(|d| d.as_secs_f64());
    let rows: Vec<(String, String, i32, serde_json::Value, serde_json::Value)> = sqlx::query_as(
        "SELECT v.index_id, v.table_id, v.dimensions, t.key_schema, t.attribute_definitions \
         FROM vector_indexes v JOIN tables t ON t.table_id = v.table_id \
         WHERE v.index_status = 'CREATING' \
         AND ($1::float8 IS NULL \
              OR v.build_heartbeat_at IS NULL \
              OR v.build_heartbeat_at < NOW() - make_interval(secs => $1)) \
         ORDER BY v.index_name",
    )
    .bind(stale_seconds)
    .fetch_all(&engine.pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    // Swept against the UNFILTERED set of building indexes, not against the rows
    // selected above. Mid-run those rows are filtered by staleness, so they are not
    // the set that legitimately holds the queue and sweeping against them would
    // release a healthy build's hold.
    //
    // The sweep runs at runtime as well as at startup because three routes leave a
    // hold with no CREATING row behind it, which means the stuck-build sweep can
    // never see them: a failed catalog commit, a crash between taking the hold and
    // committing the catalog row, and a crash after a delete commits but before its
    // release. Without this they are permanent until a restart; with it they heal
    // within a minute. The age bound is what keeps a peer's just-taken hold safe.
    let building: Vec<String> =
        sqlx::query_scalar("SELECT index_id FROM vector_indexes WHERE index_status = 'CREATING'")
            .fetch_all(&engine.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    sweep_orphan_holds(engine, &building).await?;

    let mut rebuilt = 0usize;
    for (index_id, table_id, dimensions, ks_json, ad_json) in rows {
        let Some(_owner) = build_ownership(&engine.data_pool, &index_id).await else {
            tracing::info!(
                index_id = %index_id,
                "vector index build is owned by another process; not rebuilding it here"
            );
            continue;
        };
        let base_key_schema: Vec<KeySchemaElement> =
            serde_json::from_value(ks_json).map_err(|e| StorageError::Internal(e.to_string()))?;
        let attribute_definitions: Vec<AttributeDefinition> =
            serde_json::from_value(ad_json).map_err(|e| StorageError::Internal(e.to_string()))?;
        let dimensions = u32::try_from(dimensions)
            .map_err(|_| StorageError::Internal("vector dimensions out of range".to_owned()))?;

        // The hold is re-taken rather than assumed: a crash may have happened either
        // side of the original insert, and holding twice is harmless where not
        // holding at all is not.
        take_hold(&engine.data_pool, &table_id, &index_id).await?;

        let mut ops = PostgresVectorBuild {
            catalog: engine.pool.clone(),
            data: engine.data_pool.clone(),
            queue_notify: engine.gsi_queue.clone(),
            table_id: table_id.clone(),
            index_id: index_id.clone(),
            base_key_schema,
            attribute_definitions,
            dimensions,
            meta: None,
        };
        match extenddb_storage::vector_lifecycle::rebuild_index(
            &mut ops,
            extenddb_storage::vector_lifecycle::BACKFILL_BATCH,
        )
        .await
        {
            Ok(written) => {
                rebuilt += 1;
                tracing::info!(
                    index_id = %index_id,
                    vectors_indexed = written,
                    "rebuilt an incomplete vector index at startup"
                );
            }
            Err(e) => tracing::error!(
                index_id = %index_id,
                "failed to rebuild an incomplete vector index; leaving it CREATING: {e}"
            ),
        }
    }
    Ok(rebuilt)
}
