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
pub(crate) async fn apply_claimed_vector_row(
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
