//! Global Secondary Index support via shadow tables.
//!
//! Each GSI gets its own BigTable table named `<base_data_table>_g<8hex>`
//! (8 hex chars from a stable hash of the index name to fit BigTable's
//! 50-char table-id limit). Shadow row key encodes the GSI key with the base
//! key appended after a 0xFE separator (so multiple base rows sharing the
//! same GSI key don't collide). Shadow cells are projected per the GSI's
//! ProjectionType (`ALL` writes every attribute; `KEYS_ONLY` writes only
//! base+GSI keys; `INCLUDE` adds the configured `NonKeyAttributes`).

use std::collections::BTreeMap;

use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, KeyType, Projection, ProjectionType,
};
use extenddb_storage::error::StorageError;

use crate::data::encoding::row_key;

const BASE_SEP: u8 = 0xFE;

/// Derive the BigTable shadow-table id for a given (base_data_table, index_name).
pub fn shadow_table_id(base_data_table: &str, index_name: &str) -> String {
    // FNV-1a over the index name → 8 hex chars; deterministic and fits in
    // the BigTable table-name length budget.
    let mut h: u64 = 0xcbf29ce484222325;
    for b in index_name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{base_data_table}_g{:08x}", (h & 0xFFFF_FFFF) as u32)
}

fn tag_bytes_for(av: &AttributeValue) -> Result<(u8, Vec<u8>), StorageError> {
    row_key::sk_tag_and_bytes(av)
}

/// Build the shadow row key for a given item, knowing both the GSI key schema
/// and the base table's key schema. Returns None if the item lacks any of the
/// GSI key attributes (sparse-index semantics — that item simply doesn't get
/// a shadow entry).
pub fn shadow_row_key_for_item(
    item: &Item,
    gsi_key_schema: &[KeySchemaElement],
    base_key_schema: &[KeySchemaElement],
) -> Result<Option<Vec<u8>>, StorageError> {
    let mut out = Vec::with_capacity(64);

    // GSI partition key (HASH) — required for the index entry to exist.
    let gsi_pk = gsi_key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .ok_or_else(|| StorageError::Validation("GSI missing HASH key".into()))?;
    let Some(pk_val) = item.get(&gsi_pk.attribute_name) else {
        return Ok(None);
    };
    let (tag, bytes) = tag_bytes_for(pk_val)?;
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&bytes);

    // GSI sort key (optional)
    if let Some(gsi_sk) = gsi_key_schema.iter().find(|k| k.key_type == KeyType::Range) {
        let Some(sk_val) = item.get(&gsi_sk.attribute_name) else {
            return Ok(None);
        };
        let (tag, bytes) = tag_bytes_for(sk_val)?;
        out.push(tag);
        out.extend_from_slice(&bytes);
    }

    // Separator before the base-key suffix.
    out.push(BASE_SEP);

    // Base partition key (HASH) — required (it's a key attr).
    let base_pk = base_key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .ok_or_else(|| StorageError::Validation("base table missing HASH key".into()))?;
    let pk_val = item.get(&base_pk.attribute_name).ok_or_else(|| {
        StorageError::Validation(format!("item missing key attr {}", base_pk.attribute_name))
    })?;
    let (tag, bytes) = tag_bytes_for(pk_val)?;
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&bytes);

    // Base sort key (optional)
    if let Some(base_sk) = base_key_schema.iter().find(|k| k.key_type == KeyType::Range) {
        let sk_val = item.get(&base_sk.attribute_name).ok_or_else(|| {
            StorageError::Validation(format!("item missing key attr {}", base_sk.attribute_name))
        })?;
        let (tag, bytes) = tag_bytes_for(sk_val)?;
        out.push(tag);
        out.extend_from_slice(&bytes);
    }

    Ok(Some(out))
}

/// Build the prefix used to scan all shadow entries matching `gsi_pk` (and
/// optionally a `gsi_sk` operator). Used by Query against an index.
pub fn shadow_prefix_for_pk(pk: &AttributeValue) -> Result<Vec<u8>, StorageError> {
    let (tag, bytes) = tag_bytes_for(pk)?;
    let mut out = Vec::with_capacity(1 + 4 + bytes.len());
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&bytes);
    Ok(out)
}

/// Project a base item down to exactly the attributes a shadow row needs,
/// per the GSI's `Projection`:
/// - `ALL`        → pass-through (every attribute).
/// - `KEYS_ONLY`  → base-table key attrs + GSI key attrs.
/// - `INCLUDE`    → base+GSI keys plus the configured `NonKeyAttributes`.
pub fn project_for_shadow(
    item: &Item,
    projection: &Projection,
    base_key_schema: &[KeySchemaElement],
    gsi_key_schema: &[KeySchemaElement],
) -> Item {
    if matches!(projection.projection_type, ProjectionType::All) {
        return item.clone();
    }
    let mut out: Item = BTreeMap::new();
    let mut copy_named = |name: &str| {
        if let Some(v) = item.get(name) {
            out.insert(name.to_owned(), v.clone());
        }
    };
    for ks in base_key_schema {
        copy_named(&ks.attribute_name);
    }
    for ks in gsi_key_schema {
        copy_named(&ks.attribute_name);
    }
    if matches!(projection.projection_type, ProjectionType::Include) {
        if let Some(extras) = &projection.non_key_attributes {
            for name in extras {
                copy_named(name);
            }
        }
    }
    out
}

/// Decode shadow-row cells back into an Item map. The base-key columns are
/// already inside `item` (we projected ALL on write) so the caller gets the
/// item directly.
pub fn decode_shadow_cells(
    cells: Vec<bigtable_rs::bigtable::RowCell>,
) -> Result<Option<Item>, StorageError> {
    let mut item: Item = BTreeMap::new();
    for c in cells {
        if c.family_name == crate::data::item_ops::FAMILY_DATA {
            let attr = String::from_utf8(c.qualifier)
                .map_err(|e| StorageError::Internal(format!("decode qualifier: {e}")))?;
            item.insert(attr, crate::data::encoding::cell::decode(&c.value)?);
        }
    }
    if item.is_empty() {
        Ok(None)
    } else {
        Ok(Some(item))
    }
}
