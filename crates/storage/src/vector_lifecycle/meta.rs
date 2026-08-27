// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The vector index description shared by the write path, the backfill, and
//! the propagation queue.

use serde::{Deserialize, Serialize};

use extenddb_core::types::{AttributeDefinition, Item, KeySchemaElement};

use super::partition::partition_value;
use crate::error::StorageError;

/// A vector index as the write path needs it.
///
/// Serializable because the asynchronous path snapshots it verbatim into the
/// pending row's [`VectorApplyContext`]. The write path and the worker therefore
/// apply from the *same* description of the index, which is the property that stops
/// a queued write from being reinterpreted under a later definition.
///
/// Lives in shared code because both the snapshot bytes and the projection rules
/// hanging off it must be identical across backends: a queue row written by one
/// build of one backend must stay readable, and a row shaped differently between
/// backends would search correctly right up until the difference mattered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexMeta {
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

/// Everything the propagation worker needs to apply one vector index update,
/// serialized into the pending queue row's context column.
///
/// `table_id` is carried here even though the queue row has a `table_id` column of
/// its own, because a vector data table is named from the table id *and* the index
/// id. Reading it from the context preserves the invariant that the context alone
/// is sufficient, rather than splitting one apply's inputs across a column and a
/// JSON blob. Both are written from the same variable in the same statement, so
/// they cannot disagree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorApplyContext {
    pub base_key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub table_id: String,
    /// Deliberately named `vector` rather than `index`: it is the field whose
    /// presence lets an untagged pending-context enum tell a vector row from a
    /// GSI row by shape alone. Backends discriminate queue rows by this shape,
    /// so the field name is part of the on-disk format and must not change.
    pub vector: VectorIndexMeta,
}

/// Whether an item belongs in a vector index.
///
/// It must carry the vector attribute, and the HASH attribute when the index
/// declares one: without the latter the row could not be placed in a partition,
/// and putting it in the unscoped partition would make it visible to searches of
/// every other partition. Not an error, exactly as a GSI silently omits an item
/// missing its index key.
#[must_use]
pub fn item_is_indexable(item: &Item, meta: &VectorIndexMeta) -> bool {
    if !item.contains_key(&meta.vector_attribute_name) {
        return false;
    }
    match &meta.hash_attribute_name {
        Some(name) => item.contains_key(name),
        None => true,
    }
}

/// The partition column value for an item under one index.
///
/// # Errors
/// Fails when the indexable check was bypassed (the HASH attribute is absent) or
/// the attribute value cannot be encoded.
pub fn item_partition(item: &Item, meta: &VectorIndexMeta) -> Result<String, StorageError> {
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
