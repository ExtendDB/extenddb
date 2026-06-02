// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Per-DynamoDB-table DDL and item CRUD for the `TiDB` backend.
//!
//! Each Virtual `DynamoDB` table maps to a `TiDB` table named `_ddb_<table_id>`.
//! Partition keys are stored as bytes. Sort keys use typed columns (`sk_s`, `sk_n`, `sk_b`)
//! for correct ordering. The full item is stored as JSON in `item_data`.

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::pk_to_bytes;

/// DynamoDB partition-key values are limited to 2048 bytes. TiDB's default
/// maximum index length is 3072 bytes, so storing the hash key as raw bytes
/// leaves exactly 1024 bytes for a sort key in native clustered and secondary
/// indexes.
pub(crate) const DYNAMODB_HASH_KEY_COLUMN_BYTES: usize = 2048;
pub(crate) const DYNAMODB_HASH_KEY_COLUMN_TYPE: &str = "VARBINARY(2048)";
pub(crate) const DYNAMODB_SORT_KEY_COLUMN_BYTES: usize = 1024;
pub(crate) const DYNAMODB_SORT_KEY_COLUMN_TYPE: &str = "VARBINARY(1024)";
pub(crate) const DATA_TABLE_SPLIT_REGIONS: u16 = 16;
pub(crate) const DATA_TABLE_PARTITIONS: u16 = 16;
pub(crate) const VARBINARY_SPLIT_LOWER: &str = "X''";
pub(crate) const DECIMAL_SPLIT_LOWER: &str =
    "-99999999999999999999999999999999999.999999999999999999999999999999";
pub(crate) const DECIMAL_SPLIT_UPPER: &str =
    "99999999999999999999999999999999999.999999999999999999999999999999";

pub(crate) fn varbinary_split_upper(bytes: usize) -> String {
    format!("X'{}'", "ff".repeat(bytes))
}

/// SQL table name for a Virtual `DynamoDB` table.
///
/// Uses `_ddb_` prefix to avoid collisions with catalog metadata tables.
/// Includes `account_id` for multi-account isolation.
/// Table names are validated at the engine layer (alphanumeric + `_.-`),
/// so this is safe for identifier construction.
pub(crate) fn data_table_name(table_id: &str) -> String {
    format!("`{}`", physical_data_table_name(table_id))
}

/// Raw TiDB table name for a Virtual `DynamoDB` table.
pub(crate) fn physical_data_table_name(table_id: &str) -> String {
    format!("_ddb_{table_id}")
}

/// Encode the DynamoDB partition-key tuple exactly as TiDB stores it in `pk`.
pub(crate) fn physical_pk_bytes(
    item: &Item,
    key_schema: &[KeySchemaElement],
) -> Result<Vec<u8>, StorageError> {
    let hash_values = key_schema
        .iter()
        .filter(|ks| ks.key_type == extenddb_core::types::KeyType::Hash)
        .map(|ks| {
            item.get(&ks.attribute_name).ok_or_else(|| {
                StorageError::Internal(format!(
                    "missing partition key attribute {}",
                    ks.attribute_name
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    physical_pk_bytes_from_values(&hash_values)
}

pub(crate) fn physical_pk_bytes_from_values(
    values: &[&AttributeValue],
) -> Result<Vec<u8>, StorageError> {
    let out = if values.len() == 1 {
        pk_to_bytes(values[0])?.into_owned()
    } else {
        let mut out = Vec::new();
        for value in values {
            let bytes = pk_to_bytes(value)?;
            out.extend_from_slice(bytes.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(&bytes);
            out.push(b',');
        }
        out
    };

    if out.len() > DYNAMODB_HASH_KEY_COLUMN_BYTES {
        return Err(StorageError::Validation(format!(
            "One or more parameter values are not valid. \
             The partition key size must be between 1 and {DYNAMODB_HASH_KEY_COLUMN_BYTES} bytes"
        )));
    }
    Ok(out)
}

pub(crate) fn validate_native_key_schema_shape(
    context: &str,
    key_schema: &[KeySchemaElement],
) -> Result<(), StorageError> {
    let range_count = key_schema
        .iter()
        .filter(|key| key.key_type == KeyType::Range)
        .count();
    if range_count > 1 {
        return Err(StorageError::Validation(format!(
            "One or more parameter values were invalid: TiDB backend supports at most one RANGE key for {context} because native clustered and secondary indexes must fit TiDB's 3072-byte key limit"
        )));
    }
    Ok(())
}

/// Look up all RANGE key attribute definitions from the key schema (preserving order).
pub(crate) fn all_sort_key_info<'a>(
    key_schema: &'a [KeySchemaElement],
    attr_defs: &'a [AttributeDefinition],
) -> Vec<(&'a str, ScalarAttributeType)> {
    key_schema
        .iter()
        .filter(|ks| ks.key_type == extenddb_core::types::KeyType::Range)
        .filter_map(|ks| {
            attr_defs
                .iter()
                .find(|ad| ad.attribute_name == ks.attribute_name)
                .map(|ad| (ks.attribute_name.as_str(), ad.attribute_type))
        })
        .collect()
}

/// Deserialize an `item_data` JSON value into an `Item`.
pub(crate) fn json_to_item(v: serde_json::Value) -> Result<Item, StorageError> {
    serde_json::from_value(v).map_err(|e| StorageError::Internal(e.to_string()))
}

pub(crate) fn repeat_tuple_placeholders(count: usize, width: usize) -> String {
    let tuple = if width == 1 {
        "?".to_owned()
    } else {
        format!(
            "({})",
            std::iter::repeat_n("?", width)
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    std::iter::repeat_n(tuple, count)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Bind a `SortKeyValue` to a positional parameter in a sqlx query and execute it.
///
/// Reduces the repeated match-on-variant-and-bind pattern across query helpers.
macro_rules! bind_sk_fetch_optional {
    ($sql:expr, $pk:expr, $sk:expr, $executor:expr) => {
        match $sk {
            extenddb_storage::util::SortKeyValue::S(s) => {
                sqlx::query_as($sql)
                    .bind($pk)
                    .bind(s.as_bytes().to_vec())
                    .fetch_optional($executor)
                    .await
            }
            extenddb_storage::util::SortKeyValue::N(n) => {
                sqlx::query_as($sql)
                    .bind($pk)
                    .bind(n)
                    .fetch_optional($executor)
                    .await
            }
            extenddb_storage::util::SortKeyValue::B(b) => {
                sqlx::query_as($sql)
                    .bind($pk)
                    .bind(b)
                    .fetch_optional($executor)
                    .await
            }
        }
        .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))
    };
}

macro_rules! bind_sk_execute {
    ($sql:expr, $pk:expr, $sk:expr, $item_json:expr, $executor:expr) => {
        match $sk {
            extenddb_storage::util::SortKeyValue::S(s) => {
                sqlx::query($sql)
                    .bind($pk)
                    .bind(s.as_bytes().to_vec())
                    .bind($item_json)
                    .execute($executor)
                    .await
            }
            extenddb_storage::util::SortKeyValue::N(n) => {
                sqlx::query($sql)
                    .bind($pk)
                    .bind(n)
                    .bind($item_json)
                    .execute($executor)
                    .await
            }
            extenddb_storage::util::SortKeyValue::B(b) => {
                sqlx::query($sql)
                    .bind($pk)
                    .bind(b)
                    .bind($item_json)
                    .execute($executor)
                    .await
            }
        }
        .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))
    };
}

macro_rules! bind_sk_execute_raw {
    ($sql:expr, $pk:expr, $sk:expr, $item_json:expr, $executor:expr) => {
        match $sk {
            extenddb_storage::util::SortKeyValue::S(s) => {
                sqlx::query($sql)
                    .bind($pk)
                    .bind(s.as_bytes().to_vec())
                    .bind($item_json)
                    .execute($executor)
                    .await
            }
            extenddb_storage::util::SortKeyValue::N(n) => {
                sqlx::query($sql)
                    .bind($pk)
                    .bind(n)
                    .bind($item_json)
                    .execute($executor)
                    .await
            }
            extenddb_storage::util::SortKeyValue::B(b) => {
                sqlx::query($sql)
                    .bind($pk)
                    .bind(b)
                    .bind($item_json)
                    .execute($executor)
                    .await
            }
        }
    };
}

macro_rules! bind_sk_update_execute {
    ($sql:expr, $item_json:expr, $pk:expr, $sk:expr, $executor:expr) => {
        match $sk {
            extenddb_storage::util::SortKeyValue::S(s) => {
                sqlx::query($sql)
                    .bind($item_json)
                    .bind($pk)
                    .bind(s.as_bytes().to_vec())
                    .execute($executor)
                    .await
            }
            extenddb_storage::util::SortKeyValue::N(n) => {
                sqlx::query($sql)
                    .bind($item_json)
                    .bind($pk)
                    .bind(n)
                    .execute($executor)
                    .await
            }
            extenddb_storage::util::SortKeyValue::B(b) => {
                sqlx::query($sql)
                    .bind($item_json)
                    .bind($pk)
                    .bind(b)
                    .execute($executor)
                    .await
            }
        }
        .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))
    };
}

// Submodules declared after macros so they can use bind_sk_fetch_optional/bind_sk_execute.
mod batch_write;
mod data_engine;
mod ddl;
mod delete_item;
mod index;
mod put_item;
mod query;
mod query_scan;
mod region_split;
mod statistics;
mod table_attributes;
mod transactions;
mod tx_helpers;
mod update_item;

pub(crate) use region_split::{
    USER_TABLE_FULL_KEYSPACE_SPLITS_MIGRATION, user_data_table_region_split_sqls,
};
pub(crate) use table_attributes::{
    deny_data_table_region_merges, user_data_table_region_merge_option_sqls,
};

pub(crate) use index::{native_index_key_tuple_columns, native_index_name};
pub(crate) use tx_helpers::{
    StreamRecordHandleMode, detect_stream_record_handle_mode,
    finalize_pending_stream_records_for_shard, next_stream_sequence,
};

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use extenddb_core::types::{AttributeValue, KeySchemaElement, KeyType};

    use super::{physical_pk_bytes, validate_native_key_schema_shape};

    #[test]
    fn physical_pk_bytes_uses_raw_binary_hash_keys() {
        let key_schema = vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let item = BTreeMap::from([(
            "pk".to_owned(),
            AttributeValue::B(vec![0, 1, 2, 253, 254, 255]),
        )]);

        assert_eq!(
            physical_pk_bytes(&item, &key_schema).expect("physical pk"),
            vec![0, 1, 2, 253, 254, 255]
        );
    }

    #[test]
    fn physical_pk_bytes_uses_the_full_hash_tuple() {
        let key_schema = vec![
            KeySchemaElement {
                attribute_name: "tenant".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "bucket".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "ts".to_owned(),
                key_type: KeyType::Range,
            },
        ];
        let item = BTreeMap::from([
            (
                "tenant".to_owned(),
                AttributeValue::S("customer-a".to_owned()),
            ),
            ("bucket".to_owned(), AttributeValue::N("42".to_owned())),
            ("ts".to_owned(), AttributeValue::N("7".to_owned())),
        ]);

        assert_eq!(
            physical_pk_bytes(&item, &key_schema).expect("physical pk"),
            b"10:customer-a,2:42,".to_vec()
        );
    }

    #[test]
    fn physical_pk_bytes_rejects_hash_tuples_wider_than_tidb_column() {
        let key_schema = vec![
            KeySchemaElement {
                attribute_name: "tenant".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "bucket".to_owned(),
                key_type: KeyType::Hash,
            },
        ];
        let item = BTreeMap::from([
            ("tenant".to_owned(), AttributeValue::S("x".repeat(1024))),
            ("bucket".to_owned(), AttributeValue::S("y".repeat(1024))),
        ]);

        let err = physical_pk_bytes(&item, &key_schema).unwrap_err();

        assert!(
            err.to_string()
                .contains("partition key size must be between 1 and 2048 bytes")
        );
    }

    #[test]
    fn native_key_schema_shape_rejects_multi_range_tidb_indexes() {
        let key_schema = vec![
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk1".to_owned(),
                key_type: KeyType::Range,
            },
            KeySchemaElement {
                attribute_name: "sk2".to_owned(),
                key_type: KeyType::Range,
            },
        ];

        let err = validate_native_key_schema_shape("index idx1", &key_schema).unwrap_err();

        assert!(err.to_string().contains("at most one RANGE key"));
        assert!(err.to_string().contains("3072-byte key limit"));
    }
}
