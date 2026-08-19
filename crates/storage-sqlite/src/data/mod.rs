// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Per-DynamoDB-table data storage for the SQLite backend.
//!
//! Each virtual DynamoDB table maps to a SQLite table named `_ddb_<table_id>`;
//! each secondary index maps to `_ddb_<index_id>`. The full item is stored as
//! JSON `TEXT` in `item_data`; key columns exist only for lookup and ordering.
//!
//! # Key column types (design decision D2)
//!
//! - Partition key (`pk`): always `TEXT` via the shared `pk_to_text` (equality
//!   only) — identical to the PostgreSQL backend.
//! - Sort key, by attribute type:
//!   - `S` → `sk_s TEXT` (SQLite BINARY collation = DynamoDB UTF-8 byte order).
//!   - `N` → `sk_n TEXT` holding [`encode_orderable_number`], so lexicographic
//!     order equals numeric order with full 38-digit precision (never `REAL`).
//!   - `B` → `sk_b BLOB` (memcmp = DynamoDB unsigned byte order).
//!
//! The exact, full-precision value is always preserved in `item_data` JSON, so
//! reads lose nothing; the typed key columns are used only for
//! lookup/range/ordering.

use extenddb_core::types::{
    AttributeDefinition, Item, KeySchemaElement, KeyType, ScalarAttributeType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::SortKeyValue;

use crate::number_key::encode_orderable_number;

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
pub(crate) mod vector_index;

pub(crate) use index::{
    PendingApplyContext, apply_pending_context, insert_index_row_multi, project_item_for_index,
};
pub(crate) use tx_helpers::upsert_item_in_tx;

/// Quoted SQL identifier for a virtual DynamoDB table's data table.
pub(crate) fn data_table_name(table_id: &str) -> String {
    format!("\"_ddb_{table_id}\"")
}

/// Quoted SQL identifier for a GSI/LSI data table.
pub(crate) fn index_table_name(index_id: &str) -> String {
    format!("\"_ddb_{index_id}\"")
}

/// Quoted SQL identifier for a vector-index data table.
///
/// The name carries the base `table_id` as well as the `index_id` so every vector
/// table belonging to a DynamoDB table is discoverable from the table alone. That
/// is what lets `drop_data_table` clean them up: by the time it runs, the catalog
/// rows have already been cascade-deleted inside the same transaction, so the
/// index ids can no longer be read from `vector_indexes`.
///
/// One row per vector rather than a packed blob per partition. Measured
/// 2026-08-06: a packed blob reads 2 to 4x faster but makes every write
/// O(partition), since inserting one vector rewrites the whole blob (390 MB for
/// a 100k-vector partition at 1024 dimensions). Vector indexes are maintained on
/// every write to an indexed attribute, so that trade is not available.
pub(crate) fn vector_table_name(table_id: &str, index_id: &str) -> String {
    format!("\"_vidx_{table_id}_{index_id}\"")
}

/// `GLOB` pattern matching every vector data table of one DynamoDB table.
///
/// `GLOB` rather than `LIKE` because the pattern itself is full of underscores, and
/// in `LIKE` an underscore is a single-character wildcard: `_vidx_<id>_%` would match
/// far more than it appears to. It can only ever over-match, since a wildcard also
/// matches a literal underscore, and no other table could share the UUID, so `LIKE`
/// was safe in practice. It was not safe for the stated reason, though, and a comment
/// that justifies the wrong half is worse than none.
///
/// `GLOB` treats `_` literally and uses `*` for the wildcard, so the pattern means
/// what it reads. `table_id` is a server-generated UUID, which contains no `GLOB`
/// metacharacters either.
pub(crate) fn vector_table_glob_pattern(table_id: &str) -> String {
    format!("_vidx_{table_id}_*")
}

/// All RANGE key attributes in key-schema order, paired with their scalar type.
pub(crate) fn all_sort_key_info<'a>(
    key_schema: &'a [KeySchemaElement],
    attr_defs: &'a [AttributeDefinition],
) -> Vec<(&'a str, ScalarAttributeType)> {
    key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Range)
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

/// A value bound into a positional `sqlx` query, already mapped to its SQLite
/// storage representation.
///
/// The `Text` and `Blob` cases carry sort-key values per D2. `Int` carries a
/// query parameter that is a number in its own right rather than a key: before it
/// existed, the only way an integer reached SQL in this crate was interpolation.
#[derive(Debug, Clone)]
pub(crate) enum BoundValue {
    /// `TEXT` — used for `S` (raw string) and `N` (order-preserving encoding).
    Text(String),
    /// `BLOB` — used for `B`.
    Blob(Vec<u8>),
    /// `INTEGER` — a numeric parameter, not a key. Distinct from `N`, which is
    /// deliberately TEXT so that lexicographic order equals numeric order; this
    /// is for values whose ordering is irrelevant, such as a `LIMIT`.
    Int(i64),
}

/// Map a parsed sort-key value to its SQLite storage representation (D2):
/// `S` → TEXT, `N` → order-preserving TEXT, `B` → BLOB.
pub(crate) fn sk_bound(sk: &SortKeyValue) -> BoundValue {
    match sk {
        SortKeyValue::S(s) => BoundValue::Text(s.clone()),
        SortKeyValue::N(n) => BoundValue::Text(encode_orderable_number(n)),
        SortKeyValue::B(b) => BoundValue::Blob(b.clone()),
    }
}

/// Bind a [`BoundValue`] onto a positional `sqlx` query.
macro_rules! bind_bound {
    ($query:expr, $bound:expr) => {
        match $bound {
            crate::data::BoundValue::Text(s) => $query.bind(s),
            crate::data::BoundValue::Blob(b) => $query.bind(b),
            crate::data::BoundValue::Int(i) => $query.bind(i),
        }
    };
}

/// A positional placeholder list for `n` parameters: `?, ?, ?`.
///
/// Deduplicates two identical constructions on the index write paths.
///
/// # The call shape is load-bearing, so do not "simplify" it
///
/// Call sites pass this as a **named** `format!` argument:
///
/// ```ignore
/// format!("INSERT INTO {t} ({}) VALUES ({binds})", cols, binds = bind_list(n))
/// ```
///
/// Collapsing that to a local plus implicit capture, which reads as a tidy-up:
///
/// ```ignore
/// let binds = bind_list(n);
/// format!("INSERT INTO {t} ({}) VALUES ({binds})", cols)   // WRONG
/// ```
///
/// changes nothing about the SQL and breaks a static check. With implicit
/// capture the identifier exists only inside the format string and **no
/// expression appears in the macro arguments at all**, so a `syn`-based rule
/// cannot see that a `VALUES (...)` interpolation is a placeholder list rather
/// than a value. The named form puts the name and the call in one AST node.
pub(crate) fn bind_list(n: usize) -> String {
    vec!["?"; n].join(", ")
}

/// Bind `pk` then a sort-key value, then `fetch_optional` a single JSON column.
macro_rules! bind_sk_fetch_optional {
    ($sql:expr, $pk:expr, $sk:expr, $executor:expr) => {{
        let __q = sqlx::query_as::<_, (serde_json::Value,)>($sql).bind($pk);
        let __q = match crate::data::sk_bound($sk) {
            crate::data::BoundValue::Text(s) => __q.bind(s),
            crate::data::BoundValue::Blob(b) => __q.bind(b),
            crate::data::BoundValue::Int(i) => __q.bind(i),
        };
        __q.fetch_optional($executor)
            .await
            .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))
    }};
}

/// Bind `pk`, a sort-key value, then `item_json`, and `execute`.
macro_rules! bind_sk_execute {
    ($sql:expr, $pk:expr, $sk:expr, $item_json:expr, $executor:expr) => {{
        let __q = sqlx::query($sql).bind($pk);
        let __q = match crate::data::sk_bound($sk) {
            crate::data::BoundValue::Text(s) => __q.bind(s),
            crate::data::BoundValue::Blob(b) => __q.bind(b),
            crate::data::BoundValue::Int(i) => __q.bind(i),
        };
        __q.bind($item_json)
            .execute($executor)
            .await
            .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))
    }};
}

/// Bind `pk` then a sort-key value, and `execute` (no item payload).
macro_rules! bind_sk_only_execute {
    ($sql:expr, $pk:expr, $sk:expr, $executor:expr) => {{
        let __q = sqlx::query($sql).bind($pk);
        let __q = match crate::data::sk_bound($sk) {
            crate::data::BoundValue::Text(s) => __q.bind(s),
            crate::data::BoundValue::Blob(b) => __q.bind(b),
            crate::data::BoundValue::Int(i) => __q.bind(i),
        };
        __q.execute($executor)
            .await
            .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))
    }};
}

pub(crate) use {bind_bound, bind_sk_execute, bind_sk_fetch_optional, bind_sk_only_execute};
