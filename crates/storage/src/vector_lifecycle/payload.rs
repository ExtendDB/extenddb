// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The stored payload of one vector index row.

use extenddb_core::types::{Item, KeySchemaElement, ProjectionType};

use super::meta::VectorIndexMeta;

/// Build the payload stored alongside one indexed vector.
///
/// Shared by the write path and by backfill deliberately, across backends. These
/// are the only producers of a vector row, and a second copy of this logic would
/// be free to drift: a backfilled row shaped differently from a live-written one
/// would search correctly right up until the difference mattered, with nothing to
/// catch it.
///
/// Three rules, in order:
///
/// 1. The item is projected per the index's projection, exactly as the GSI path
///    projects its own (a vector index has no key schema of its own, so
///    `KEYS_ONLY` and `INCLUDE` start from the base primary key).
/// 2. The SearchSchema attributes are always projected, whatever the
///    `ProjectionType`. See [`VectorIndexMeta::search_schema_attribute_names`]
///    for why: the inline filter is evaluated against this payload, so dropping
///    the attribute would silently turn every filtered search into a zero-result
///    search.
/// 3. The vector itself is not kept in the payload: it is already stored as
///    `f32`, which is the width the service validates against, and the search
///    path rebuilds the attribute from those bits. Keeping a verbatim decimal
///    copy here duplicated 10 to 15 KB per row at 1024 dimensions and would have
///    returned the client's original precision where the service returns the
///    narrowed value.
#[must_use]
pub fn projected_payload(
    item: &Item,
    base_key_schema: &[KeySchemaElement],
    meta: &VectorIndexMeta,
) -> Item {
    let mut projected = match meta.projection.projection_type {
        ProjectionType::All => item.clone(),
        ProjectionType::KeysOnly | ProjectionType::Include => {
            let mut projected = Item::new();
            for ks in base_key_schema {
                if let Some(v) = item.get(&ks.attribute_name) {
                    projected.insert(ks.attribute_name.clone(), v.clone());
                }
            }
            if meta.projection.projection_type == ProjectionType::Include
                && let Some(attrs) = &meta.projection.non_key_attributes
            {
                for attr in attrs {
                    if let Some(v) = item.get(attr) {
                        projected.insert(attr.clone(), v.clone());
                    }
                }
            }
            projected
        }
    };
    for name in &meta.search_schema_attribute_names {
        if !projected.contains_key(name)
            && let Some(v) = item.get(name)
        {
            projected.insert(name.clone(), v.clone());
        }
    }
    projected.remove(&meta.vector_attribute_name);
    projected
}
