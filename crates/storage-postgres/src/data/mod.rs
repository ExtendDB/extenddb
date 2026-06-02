// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Per-DynamoDB-table DDL and item CRUD for the `PostgreSQL` backend.
//!
//! Each Virtual `DynamoDB` table maps to a `PostgreSQL` table named `_ddb_<table_id>`.
//! Partition keys are stored as TEXT. Sort keys use typed columns (`sk_s`, `sk_n`, `sk_b`)
//! for correct ordering. The full item is stored as JSONB in `item_data`.

use extenddb_core::types::{AttributeDefinition, Item, KeySchemaElement, ScalarAttributeType};
use extenddb_storage::error::StorageError;

/// SQL table name for a Virtual `DynamoDB` table.
///
/// Uses `_ddb_` prefix to avoid collisions with catalog metadata tables.
/// Includes `account_id` for multi-account isolation.
/// Table names are validated at the engine layer (alphanumeric + `_.-`),
/// so this is safe for identifier construction.
pub(crate) fn data_table_name(table_id: &str) -> String {
    format!("\"{}\"", physical_data_table_name(table_id))
}

/// SQL table name for a GSI/LSI data table.
pub(crate) fn index_table_name(index_id: &str) -> String {
    format!("\"{}\"", physical_index_table_name(index_id))
}

/// Raw PostgreSQL table name for a Virtual `DynamoDB` table.
pub(crate) fn physical_data_table_name(table_id: &str) -> String {
    format!("_ddb_{table_id}")
}

/// Raw PostgreSQL table name for a GSI/LSI data table.
pub(crate) fn physical_index_table_name(index_id: &str) -> String {
    format!("_ddb_{index_id}")
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

/// Deserialize an `item_data` JSONB value into an `Item`.
pub(crate) fn json_to_item(v: serde_json::Value) -> Result<Item, StorageError> {
    serde_json::from_value(v).map_err(|e| StorageError::Internal(e.to_string()))
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
                    .bind(s)
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
                    .bind(s)
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

// Submodules declared after macros so they can use bind_sk_fetch_optional/bind_sk_execute.
mod data_engine;
mod ddl;
mod delete_item;
mod index;
mod put_item;
mod query;
mod query_scan;
mod transactions;
mod tx_helpers;
mod update_item;

pub(crate) use index::{
    delete_index_row_multi, insert_index_row_multi, item_has_index_keys, project_item_for_index,
};
