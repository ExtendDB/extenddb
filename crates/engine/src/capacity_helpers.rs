// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helpers for building `ConsumedCapacity` responses and `ItemCollectionMetrics`.
//!
//! Computes real capacity units based on item sizes and consistency mode.
//! Read CU: `ceil(item_size / 4KB)`, halved for eventually consistent reads.
//! Write CU: `ceil(item_size / 1KB)`.

use std::sync::atomic::AtomicU64;

use extenddb_core::types::{
    ConsumedCapacity, Item, ItemCollectionMetrics, KeySchemaElement, Projection, ProjectionType,
    ReturnConsumedCapacity, ReturnItemCollectionMetrics, TableKeyInfo, item_size_bytes,
};

/// Global counter for requests that used approximate consumed capacity.
/// Incremented by engine handlers; read and reset by the background warning task.
pub static CAPACITY_REQUEST_COUNT: AtomicU64 = AtomicU64::new(0);

/// Build a `ConsumedCapacity` for a read operation with real CU, or `None` if not requested.
#[must_use]
pub fn read_capacity(
    rcc: ReturnConsumedCapacity,
    table_name: &str,
    cu: f64,
) -> Option<ConsumedCapacity> {
    match rcc {
        ReturnConsumedCapacity::None => None,
        rcc => {
            CAPACITY_REQUEST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let indexes = rcc == ReturnConsumedCapacity::Indexes;
            Some(ConsumedCapacity::read(table_name, cu, indexes))
        }
    }
}

/// Build a `ConsumedCapacity` for a write operation with real CU, or `None` if not requested.
#[must_use]
pub fn write_capacity(
    rcc: ReturnConsumedCapacity,
    table_name: &str,
    cu: f64,
) -> Option<ConsumedCapacity> {
    match rcc {
        ReturnConsumedCapacity::None => None,
        rcc => {
            CAPACITY_REQUEST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let indexes = rcc == ReturnConsumedCapacity::Indexes;
            Some(ConsumedCapacity::write(table_name, cu, indexes))
        }
    }
}

/// Build a write `ConsumedCapacity` with a per-index breakdown for `INDEXES`
/// mode, or the plain table-level capacity otherwise.
///
/// `base_cu` is the base-table write capacity. `old_item` and `new_item`
/// describe the index transition: inserts provide only the new item, deletes
/// only the old item, and replacements/updates provide both. Index metadata
/// comes from the cached `TableKeyInfo`.
#[must_use]
pub fn write_capacity_indexed(
    rcc: ReturnConsumedCapacity,
    table_name: &str,
    base_cu: f64,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    charge_unchanged_projection: bool,
    key_info: &TableKeyInfo,
) -> Option<ConsumedCapacity> {
    match rcc {
        ReturnConsumedCapacity::None => None,
        rcc => {
            CAPACITY_REQUEST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let (gsi, lsi) =
                index_write_units(old_item, new_item, charge_unchanged_projection, key_info);
            let breakdown = rcc == ReturnConsumedCapacity::Indexes;
            Some(ConsumedCapacity::write_indexed(
                table_name, base_cu, gsi, lsi, breakdown,
            ))
        }
    }
}

/// Compute per-GSI and per-LSI write capacity units for an item transition.
///
/// Returns `(gsi_units, lsi_units)` keyed by index name. Sparse-index inserts
/// and deletes are charged from the projection that exists. When both versions
/// project into an index, a changed key is a delete plus an insert. With an
/// unchanged key, updates skip identical projected entries while PutItem
/// replacements still charge the index write.
///
/// Index metadata is read from the cached `TableKeyInfo`, so no extra catalog
/// round-trip is needed.
#[must_use]
pub fn index_write_units(
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    charge_unchanged_projection: bool,
    key_info: &TableKeyInfo,
) -> (
    std::collections::HashMap<String, f64>,
    std::collections::HashMap<String, f64>,
) {
    let base_keys: Vec<&str> = key_info
        .base_key_schema
        .iter()
        .map(|k| k.attribute_name.as_str())
        .collect();

    let mut gsi = std::collections::HashMap::new();
    for index in &key_info.global_secondary_indexes {
        if let Some(cu) = one_index_write_units(
            old_item,
            new_item,
            charge_unchanged_projection,
            &index.key_schema,
            &base_keys,
            &index.projection,
        ) {
            gsi.insert(index.index_name.clone(), cu);
        }
    }

    let mut lsi = std::collections::HashMap::new();
    for index in &key_info.local_secondary_indexes {
        if let Some(cu) = one_index_write_units(
            old_item,
            new_item,
            charge_unchanged_projection,
            &index.key_schema,
            &base_keys,
            &index.projection,
        ) {
            lsi.insert(index.index_name.clone(), cu);
        }
    }

    (gsi, lsi)
}

/// Write units for one index transition, or `None` when neither item projects
/// into the sparse index.
fn one_index_write_units(
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    charge_unchanged_projection: bool,
    index_key_schema: &[KeySchemaElement],
    base_keys: &[&str],
    projection: &Projection,
) -> Option<f64> {
    let old_projection = old_item
        .and_then(|item| sparse_index_projection(item, index_key_schema, base_keys, projection));
    let new_projection = new_item
        .and_then(|item| sparse_index_projection(item, index_key_schema, base_keys, projection));

    match (old_projection, new_projection) {
        (None, None) => None,
        (Some(projected), None) | (None, Some(projected)) => {
            Some(write_capacity_units(item_size_bytes(&projected)))
        }
        (Some(old_projected), Some(new_projected)) => {
            let same_key = old_item.zip(new_item).is_some_and(|(old, new)| {
                index_key_schema
                    .iter()
                    .all(|key| old.get(&key.attribute_name) == new.get(&key.attribute_name))
            });
            if same_key && !charge_unchanged_projection && old_projected == new_projected {
                return None;
            }

            let old_cu = write_capacity_units(item_size_bytes(&old_projected));
            let new_cu = write_capacity_units(item_size_bytes(&new_projected));
            Some(if same_key {
                old_cu.max(new_cu)
            } else {
                old_cu + new_cu
            })
        }
    }
}

/// Project an item into one index, or return `None` when the item is not a
/// member of the sparse index.
fn sparse_index_projection(
    item: &Item,
    index_key_schema: &[KeySchemaElement],
    base_keys: &[&str],
    projection: &Projection,
) -> Option<Item> {
    if index_key_schema
        .iter()
        .any(|key| !item.contains_key(&key.attribute_name))
    {
        return None;
    }
    Some(project_index_item(
        item,
        index_key_schema,
        base_keys,
        projection,
    ))
}

/// Build the subset of `item` that an index materializes, per its projection.
fn project_index_item(
    item: &Item,
    index_key_schema: &[KeySchemaElement],
    base_keys: &[&str],
    projection: &Projection,
) -> Item {
    match projection.projection_type {
        // ALL projects the entire item.
        ProjectionType::All => item.clone(),
        // KEYS_ONLY and INCLUDE always project index keys + base table keys.
        ProjectionType::KeysOnly | ProjectionType::Include => {
            let mut out = Item::new();
            for ks in index_key_schema {
                if let Some(v) = item.get(&ks.attribute_name) {
                    out.insert(ks.attribute_name.clone(), v.clone());
                }
            }
            for k in base_keys {
                if let Some(v) = item.get(*k) {
                    out.insert((*k).to_owned(), v.clone());
                }
            }
            if projection.projection_type == ProjectionType::Include
                && let Some(non_key) = &projection.non_key_attributes
            {
                for a in non_key {
                    if let Some(v) = item.get(a) {
                        out.insert(a.clone(), v.clone());
                    }
                }
            }
            out
        }
    }
}

/// Build a `Vec<ConsumedCapacity>` for a batch/transaction read, or `None` if not requested.
/// One entry per distinct table name with real CU values.
#[must_use]
pub fn batch_read_capacity<'a>(
    rcc: ReturnConsumedCapacity,
    table_cus: impl Iterator<Item = (&'a str, f64)>,
) -> Option<Vec<ConsumedCapacity>> {
    match rcc {
        ReturnConsumedCapacity::None => None,
        rcc => {
            CAPACITY_REQUEST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let indexes = rcc == ReturnConsumedCapacity::Indexes;
            Some(
                table_cus
                    .map(|(t, cu)| ConsumedCapacity::read(t, cu, indexes))
                    .collect(),
            )
        }
    }
}

/// Build a `Vec<ConsumedCapacity>` for a batch/transaction write, or `None` if not requested.
/// One entry per distinct table name with real CU values (base-table aggregate only).
#[must_use]
pub fn batch_write_capacity<'a>(
    rcc: ReturnConsumedCapacity,
    table_cus: impl Iterator<Item = (&'a str, f64)>,
) -> Option<Vec<ConsumedCapacity>> {
    match rcc {
        ReturnConsumedCapacity::None => None,
        rcc => {
            CAPACITY_REQUEST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let indexes = rcc == ReturnConsumedCapacity::Indexes;
            Some(
                table_cus
                    .map(|(t, cu)| ConsumedCapacity::write(t, cu, indexes))
                    .collect(),
            )
        }
    }
}

/// Build a `Vec<ConsumedCapacity>` for `TransactGetItems`, or `None` if not requested.
///
/// Transactions differ from single-item/batch reads: real DynamoDB emits the
/// granular `ReadCapacityUnits` sub-field, so this uses `ConsumedCapacity::transact_read`.
#[must_use]
pub fn transact_read_capacity<'a>(
    rcc: ReturnConsumedCapacity,
    table_cus: impl Iterator<Item = (&'a str, f64)>,
) -> Option<Vec<ConsumedCapacity>> {
    match rcc {
        ReturnConsumedCapacity::None => None,
        rcc => {
            CAPACITY_REQUEST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let indexes = rcc == ReturnConsumedCapacity::Indexes;
            Some(
                table_cus
                    .map(|(t, cu)| ConsumedCapacity::transact_read(t, cu, indexes))
                    .collect(),
            )
        }
    }
}

/// Build a `Vec<ConsumedCapacity>` for `TransactWriteItems`, or `None` if not requested.
///
/// Transactions differ from single-item/batch writes: real DynamoDB emits the
/// granular `WriteCapacityUnits` sub-field, so this uses `ConsumedCapacity::transact_write`.
#[must_use]
pub fn transact_write_capacity<'a>(
    rcc: ReturnConsumedCapacity,
    table_cus: impl Iterator<Item = (&'a str, f64)>,
) -> Option<Vec<ConsumedCapacity>> {
    match rcc {
        ReturnConsumedCapacity::None => None,
        rcc => {
            CAPACITY_REQUEST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let indexes = rcc == ReturnConsumedCapacity::Indexes;
            Some(
                table_cus
                    .map(|(t, cu)| ConsumedCapacity::transact_write(t, cu, indexes))
                    .collect(),
            )
        }
    }
}

/// Compute read capacity units for a single item: `ceil(item_size / 4096)`.
///
/// `strongly_consistent`: if `true`, returns full RCU; if `false` (eventually
/// consistent, the `DynamoDB` default), returns half. Minimum 1.0 RCU for
/// strongly consistent, 0.5 RCU for eventually consistent (matches real
/// `DynamoDB`, which charges a minimum of 1 RCU even for missing items).
#[must_use]
#[allow(clippy::cast_precision_loss)] // max 400KB item / 4KB = 100, fits in f64 exactly
pub fn read_capacity_units(item_size_bytes: usize, strongly_consistent: bool) -> f64 {
    let kb4 = item_size_bytes.div_ceil(4096);
    let full = if kb4 == 0 { 1.0 } else { kb4 as f64 };
    if strongly_consistent {
        full
    } else {
        full * 0.5
    }
}

/// Compute write capacity units for a single item: `ceil(item_size / 1024)`.
/// Minimum 1 WCU even for small items.
#[must_use]
#[allow(clippy::cast_precision_loss)] // max 400KB item / 1KB = 400, fits in f64 exactly
pub fn write_capacity_units(item_size_bytes: usize) -> f64 {
    let kb1 = item_size_bytes.div_ceil(1024);
    if kb1 == 0 { 1.0 } else { kb1 as f64 }
}

/// Build `ItemCollectionMetrics` for a write operation, or `None` if not requested
/// or the table has no LSI (only tables with LSIs have item collections).
///
/// `key_schema` is used to extract the partition key name; `item_or_key` is the
/// item or key from which to extract the PK value.
#[must_use]
pub fn item_metrics(
    ricm: ReturnItemCollectionMetrics,
    key_schema: &[KeySchemaElement],
    item_or_key: &Item,
    has_lsi: bool,
) -> Option<ItemCollectionMetrics> {
    if ricm == ReturnItemCollectionMetrics::None || !has_lsi {
        return None;
    }
    // The partition key is the HASH key (first element by convention).
    let pk = key_schema
        .iter()
        .find(|ks| ks.key_type == extenddb_core::types::KeyType::Hash)?;
    let pk_value = item_or_key.get(&pk.attribute_name)?;
    Some(ItemCollectionMetrics::stub(&pk.attribute_name, pk_value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{AttributeValue, KeyType};

    fn key(name: &str) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type: KeyType::Hash,
        }
    }

    fn item(pk: &str, index_key: Option<&str>) -> Item {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
        if let Some(value) = index_key {
            item.insert("gsi_pk".to_owned(), AttributeValue::S(value.to_owned()));
        }
        item
    }

    fn keys_only() -> Projection {
        Projection {
            projection_type: ProjectionType::KeysOnly,
            non_key_attributes: None,
        }
    }

    #[test]
    fn charges_deleted_sparse_index_projection() {
        let old = item("item", Some("old"));
        let new = item("item", None);
        let units = one_index_write_units(
            Some(&old),
            Some(&new),
            false,
            &[key("gsi_pk")],
            &["pk"],
            &keys_only(),
        );
        assert_eq!(units, Some(1.0));
    }

    #[test]
    fn changing_index_key_charges_delete_and_insert() {
        let old = item("item", Some("old"));
        let new = item("item", Some("new"));
        let units = one_index_write_units(
            Some(&old),
            Some(&new),
            false,
            &[key("gsi_pk")],
            &["pk"],
            &keys_only(),
        );
        assert_eq!(units, Some(2.0));
    }

    #[test]
    fn update_skips_unchanged_projection_but_replacement_charges_it() {
        let mut old = item("item", Some("index"));
        old.insert("other".to_owned(), AttributeValue::S("old".to_owned()));
        let mut new = item("item", Some("index"));
        new.insert("other".to_owned(), AttributeValue::S("new".to_owned()));
        let key_schema = [key("gsi_pk")];
        let projection = keys_only();

        let update_units = one_index_write_units(
            Some(&old),
            Some(&new),
            false,
            &key_schema,
            &["pk"],
            &projection,
        );
        assert_eq!(update_units, None);

        let replacement_units = one_index_write_units(
            Some(&old),
            Some(&new),
            true,
            &key_schema,
            &["pk"],
            &projection,
        );
        assert_eq!(replacement_units, Some(1.0));
    }
}
