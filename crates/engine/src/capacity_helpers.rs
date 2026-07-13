// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helpers for building `ConsumedCapacity` responses and `ItemCollectionMetrics`.
//!
//! Computes real capacity units based on item sizes and consistency mode.
//! Read CU: `ceil(item_size / 4KB)`, halved for eventually consistent reads.
//! Write CU: `ceil(item_size / 1KB)`.

use std::sync::atomic::AtomicU64;

use extenddb_core::types::{
    ConsumedCapacity, Item, ItemCollectionMetrics, KeySchemaElement, ReturnConsumedCapacity,
    ReturnItemCollectionMetrics,
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
/// One entry per distinct table name with real CU values.
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
