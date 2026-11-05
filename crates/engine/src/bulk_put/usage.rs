// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The stored usage JSON of one bulk operation: one vocabulary of member
//! names, one accumulator, and one reader.
//!
//! The runner writes the value with every checkpoint and the handlers read
//! it back for Describe. Absent members read as zero by design, so a member
//! name spelled differently by a writer and a reader would surface only as
//! silent zeros in the wire usage; keeping every spelling in this module is
//! what rules that failure out.

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::types::{
    BulkOperationBytes, BulkOperationUsage, BulkOperationVectorBytes, IndexInfo, Item,
    TableKeyInfo, item_size_bytes,
};

use crate::capacity_helpers;

/// Member names of the stored usage JSON. The stored value is the wire
/// shape of one `BulkOperationUsage` entry, so these equal the serde
/// renames on that type.
const TABLE_NAME: &str = "TableName";
const BULK_PUT_BYTES: &str = "BulkPutBytes";
const TABLE: &str = "Table";

/// The charge for a row whose condition evaluated false.
const CONDITION_FAILED_BYTES: u64 = 1024;
const GLOBAL_SECONDARY_INDEXES: &str = "GlobalSecondaryIndexes";
const LOCAL_SECONDARY_INDEXES: &str = "LocalSecondaryIndexes";
const VECTOR_INDEXES: &str = "VectorIndexes";
const BULK_PUT_VECTOR_BYTES: &str = "BulkPutVectorBytes";

/// Running usage figures, restored from the record and persisted with
/// every checkpoint. All values are bytes.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct UsageAccum {
    table_bytes: u64,
    gsi: HashMap<String, u64>,
    lsi: HashMap<String, u64>,
    vector: HashMap<String, u64>,
}

impl UsageAccum {
    /// Restore from the stored usage JSON. The initial value is `{}`, which
    /// restores to zeroes.
    pub(crate) fn restore(value: &Value) -> Self {
        Self {
            table_bytes: table_member_u64(value),
            gsi: bytes_map(value, GLOBAL_SECONDARY_INDEXES, BULK_PUT_BYTES),
            lsi: bytes_map(value, LOCAL_SECONDARY_INDEXES, BULK_PUT_BYTES),
            vector: bytes_map(value, VECTOR_INDEXES, BULK_PUT_VECTOR_BYTES),
        }
    }

    /// Charge one row that reached the write: the item's logical size in
    /// bytes with a one kilobyte floor (measured on the service: 512 bytes
    /// charge 1,024; 1,500 charge 1,500; 2,049 charge 2,049).
    /// The write-unit rounding `capacity_helpers::write_capacity_units`
    /// applies to `ConsumedCapacity` does not apply here.
    pub(crate) fn add_base(&mut self, item_bytes: usize) {
        self.table_bytes = self.table_bytes.saturating_add(charged_bytes(item_bytes));
    }

    /// Charge one row whose condition evaluated false: the one kilobyte
    /// floor alone, whatever the item's size (measured on the service:
    /// 1,000 condition-failed rows of about 382,300 logical bytes
    /// charged 1,024,000 in total). No index is charged, since nothing was
    /// written.
    pub(crate) fn add_condition_failed(&mut self) {
        self.table_bytes = self.table_bytes.saturating_add(CONDITION_FAILED_BYTES);
    }

    /// Charge the secondary and vector indexes for one applied write. Each
    /// secondary index is charged the bytes of the item its projection
    /// stores, with the same one kilobyte floor as the table (measured on
    /// the service: a KEYS_ONLY projection of a 1,500 byte row charges
    /// 1,024, an ALL projection of it charges 1,500). An index the row does
    /// not project into gets no entry, so an index no row reached is absent
    /// from the usage rather than reported as zero.
    pub(crate) fn add_index_writes(
        &mut self,
        old: Option<&Item>,
        new: Option<&Item>,
        info: &TableKeyInfo,
    ) {
        let base_keys: Vec<&str> = info
            .base_key_schema
            .iter()
            .map(|k| k.attribute_name.as_str())
            .collect();
        for index in &info.global_secondary_indexes {
            if let Some(bytes) = index_write_bytes(old, new, index, &base_keys) {
                let entry = self.gsi.entry(index.index_name.clone()).or_insert(0);
                *entry = entry.saturating_add(bytes);
            }
        }
        for index in &info.local_secondary_indexes {
            if let Some(bytes) = index_write_bytes(old, new, index, &base_keys) {
                let entry = self.lsi.entry(index.index_name.clone()).or_insert(0);
                *entry = entry.saturating_add(bytes);
            }
        }
        for (name, bytes) in capacity_helpers::vector_write_charges(old, new, info) {
            let entry = self.vector.entry(name).or_insert(0);
            *entry = entry.saturating_add(whole_f64_to_u64(bytes));
        }
    }

    /// The top-level figure: the table's bytes plus every secondary index's
    /// (measured on the service: 102,400,000 for the
    /// table and 27,487,232 for one index report 129,887,232). Vector bytes
    /// are a different quantity and stay out of it.
    fn total_bytes(&self) -> u64 {
        self.gsi
            .values()
            .chain(self.lsi.values())
            .fold(self.table_bytes, |sum, bytes| sum.saturating_add(*bytes))
    }

    /// The stored and wire shape: one `BulkOperationUsage` object.
    ///
    /// The runner passes the table ARN; the service carries the bare table
    /// name in the usage entry's `TableName` member (measured on the
    /// service: every recorded usage entry carries the bare name), so the
    /// stored value is the bare name.
    pub(crate) fn to_value(&self, table_arn: &str) -> Value {
        let usage = BulkOperationUsage {
            table_name: bare_table_name(table_arn).to_owned(),
            bulk_put_bytes: self.total_bytes(),
            table: BulkOperationBytes {
                bulk_put_bytes: self.table_bytes,
            },
            global_secondary_indexes: self
                .gsi
                .iter()
                .map(|(name, bytes)| {
                    (
                        name.clone(),
                        BulkOperationBytes {
                            bulk_put_bytes: *bytes,
                        },
                    )
                })
                .collect(),
            local_secondary_indexes: self
                .lsi
                .iter()
                .map(|(name, bytes)| {
                    (
                        name.clone(),
                        BulkOperationBytes {
                            bulk_put_bytes: *bytes,
                        },
                    )
                })
                .collect(),
            vector_indexes: self
                .vector
                .iter()
                .map(|(name, bytes)| {
                    (
                        name.clone(),
                        BulkOperationVectorBytes {
                            bulk_put_vector_bytes: *bytes,
                        },
                    )
                })
                .collect(),
        };
        // A struct of string-keyed maps cannot fail to serialize; Null
        // appears only if that invariant broke.
        serde_json::to_value(&usage).unwrap_or(Value::Null)
    }
}

/// Rebuild the wire usage entry from the stored JSON. The runner stores the
/// full wire shape; absent members read as zero so a terminal record whose
/// run wrote nothing still renders a zeroed entry.
///
/// The service carries the bare table name in `TableName`. A record stored
/// before this fix holds the table ARN there, so a stored value that starts
/// with `arn:` renders as the bare name after the final `table/`; `table_name`
/// is the bare name the caller passes for a record whose `TableName` member is
/// absent.
pub(crate) fn usage_from_value(value: &Value, table_name: &str) -> BulkOperationUsage {
    let table_bytes = table_member_u64(value);
    BulkOperationUsage {
        table_name: value.get(TABLE_NAME).and_then(Value::as_str).map_or_else(
            || table_name.to_owned(),
            |stored| bare_table_name(stored).to_owned(),
        ),
        bulk_put_bytes: member_u64(value, BULK_PUT_BYTES).max(table_bytes),
        table: BulkOperationBytes {
            bulk_put_bytes: table_bytes,
        },
        global_secondary_indexes: bytes_map(value, GLOBAL_SECONDARY_INDEXES, BULK_PUT_BYTES)
            .into_iter()
            .map(|(name, bytes)| {
                (
                    name,
                    BulkOperationBytes {
                        bulk_put_bytes: bytes,
                    },
                )
            })
            .collect(),
        local_secondary_indexes: bytes_map(value, LOCAL_SECONDARY_INDEXES, BULK_PUT_BYTES)
            .into_iter()
            .map(|(name, bytes)| {
                (
                    name,
                    BulkOperationBytes {
                        bulk_put_bytes: bytes,
                    },
                )
            })
            .collect(),
        vector_indexes: bytes_map(value, VECTOR_INDEXES, BULK_PUT_VECTOR_BYTES)
            .into_iter()
            .map(|(name, bytes)| {
                (
                    name,
                    BulkOperationVectorBytes {
                        bulk_put_vector_bytes: bytes,
                    },
                )
            })
            .collect(),
    }
}

/// Read one unsigned member from a usage JSON object; absent is zero.
fn member_u64(value: &Value, member: &str) -> u64 {
    value.get(member).and_then(Value::as_u64).unwrap_or(0)
}

/// Read one `{name: {inner: n}}` map from a usage JSON object.
fn bytes_map(value: &Value, member: &str, inner: &str) -> HashMap<String, u64> {
    value
        .get(member)
        .and_then(Value::as_object)
        .map(|entries| {
            entries
                .iter()
                .map(|(name, entry)| (name.clone(), member_u64(entry, inner)))
                .collect()
        })
        .unwrap_or_default()
}

/// The table figure of a stored usage value: the `Table` member's bytes.
/// A value written before the top-level figure became the total carries
/// the table figure at the top level too, so a missing `Table` member
/// falls back to it.
fn table_member_u64(value: &Value) -> u64 {
    value.get(TABLE).map_or_else(
        || member_u64(value, BULK_PUT_BYTES),
        |table| member_u64(table, BULK_PUT_BYTES),
    )
}

/// The bare table name the service carries in a usage entry's `TableName`
/// member. The runner passes the table ARN; the service emits the bare
/// name, so a value spelled as an ARN renders as the name after the final
/// `:table/`, and a value already bare is returned unchanged.
fn bare_table_name(name_or_arn: &str) -> &str {
    if name_or_arn.starts_with("arn:") {
        name_or_arn
            .rsplit_once(":table/")
            .map_or(name_or_arn, |(_, name)| name)
    } else {
        name_or_arn
    }
}

/// The bytes one written item is charged: its logical size with a one
/// kilobyte floor.
fn charged_bytes(item_bytes: usize) -> u64 {
    u64::try_from(item_bytes).unwrap_or(u64::MAX).max(1024)
}

/// The bytes one index is charged for one write, or `None` when neither
/// image projects into the sparse index or the projection is unchanged
/// under the same index key. The decision mirrors
/// `capacity_helpers::index_write_units`; the charge is the projected
/// item's bytes with the one kilobyte floor instead of its write units.
fn index_write_bytes(
    old: Option<&Item>,
    new: Option<&Item>,
    index: &IndexInfo,
    base_keys: &[&str],
) -> Option<u64> {
    let project = |item: Option<&Item>| {
        item.and_then(|item| {
            capacity_helpers::sparse_index_projection(
                item,
                &index.key_schema,
                base_keys,
                &index.projection,
            )
        })
    };
    match (project(old), project(new)) {
        (None, None) => None,
        (Some(projected), None) | (None, Some(projected)) => {
            Some(charged_bytes(item_size_bytes(&projected)))
        }
        (Some(old_projected), Some(new_projected)) => {
            let same_key = old.zip(new).is_some_and(|(old, new)| {
                index
                    .key_schema
                    .iter()
                    .all(|key| old.get(&key.attribute_name) == new.get(&key.attribute_name))
            });
            if same_key && old_projected == new_projected {
                return None;
            }
            let old_bytes = charged_bytes(item_size_bytes(&old_projected));
            let new_bytes = charged_bytes(item_size_bytes(&new_projected));
            Some(if same_key {
                old_bytes.max(new_bytes)
            } else {
                old_bytes.saturating_add(new_bytes)
            })
        }
    }
}

/// Convert a `capacity_helpers` figure to an integer. The figures are
/// non-negative whole numbers far below 2^53, so the conversion is exact.
fn whole_f64_to_u64(value: f64) -> u64 {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let converted = value.max(0.0) as u64;
    converted
}

#[cfg(test)]
mod tests;
