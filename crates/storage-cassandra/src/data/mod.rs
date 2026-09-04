// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data table operations for Cassandra.

use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::Item;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::SortKeyValue;

mod condition;
mod data_engine;
pub mod ddl;
mod delete_item;
pub mod index;
mod put_get_item;
mod query;
mod query_helpers;
mod scan;
pub mod transaction_ledger;
mod transactions;
pub(crate) mod ttl;
mod update_item;

use condition::check_condition;
use extenddb_core::expression;

/// Resolve an expression (placeholder) to an `AttributeValue`.
pub(crate) fn resolve_expr_to_av(
    expr: &expression::Expr,
    maps: &ExpressionMaps,
) -> Result<extenddb_core::types::AttributeValue, StorageError> {
    match expr {
        expression::Expr::Placeholder(name) => maps
            .resolve_value(name)
            .cloned()
            .map_err(|e| StorageError::Validation(e.to_string())),
        _ => Err(StorageError::Internal(
            "expected placeholder in key condition".to_owned(),
        )),
    }
}

type CassandraSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

/// Deserialize an `item_data` text value into an `Item`.
pub(crate) fn json_to_item(text: String) -> Result<Item, StorageError> {
    serde_json::from_str(&text).map_err(|e| StorageError::Internal(e.to_string()))
}

/// Convert a DynamoDB numeric value (`BigDecimal`) into the cdrs-tokio
/// `Decimal` wire type.
///
/// DynamoDB's `N` type is an arbitrary-precision decimal. Cassandra's `decimal`
/// type is encoded as a 4-byte `scale` followed by the two's-complement
/// `unscaled` value (a varint), representing `unscaled * 10^(-scale)`.
/// `BigDecimal::as_bigint_and_exponent()` yields exactly that pair (the exponent
/// is the scale), so the mapping is lossless for arbitrary precision.
pub(crate) fn bigdecimal_to_cql_decimal(
    n: &bigdecimal::BigDecimal,
) -> cdrs_tokio::types::decimal::Decimal {
    let (unscaled, scale) = n.as_bigint_and_exponent();
    #[allow(clippy::cast_possible_truncation)]
    let scale_i32 = scale as i32;
    cdrs_tokio::types::decimal::Decimal::new(unscaled, scale_i32)
}

/// Bind a DynamoDB numeric value (`BigDecimal`) as a Cassandra `decimal` bound
/// parameter `Value`.
///
/// This replaces the previous workaround that bound `N` values as strings,
/// which the Cassandra `decimal` column rejected ("Expected 0 or at least 4
/// bytes") and which broke column-level numeric comparisons.
pub(crate) fn decimal_to_value(n: &bigdecimal::BigDecimal) -> cdrs_tokio::types::value::Value {
    cdrs_tokio::types::value::Value::from(bigdecimal_to_cql_decimal(n))
}

/// Execute a query with pk and sort key, returning the result.
///
/// Helper to reduce repetitive match-on-sk-type pattern.
pub(crate) async fn query_with_pk_sk(
    session: &CassandraSession,
    query: &str,
    pk: &str,
    sk: &SortKeyValue,
) -> Result<cdrs_tokio::frame::Envelope, StorageError> {
    match sk {
        SortKeyValue::S(s) => {
            session
                .query_with_values(query, cdrs_tokio::query_values!(pk, s.as_str()))
                .await
        }
        SortKeyValue::N(n) => {
            session
                .query_with_values(query, cdrs_tokio::query_values!(pk, decimal_to_value(n)))
                .await
        }
        SortKeyValue::B(b) => {
            session
                .query_with_values(query, cdrs_tokio::query_values!(pk, b.clone()))
                .await
        }
    }
    .map_err(|e| StorageError::Internal(format!("Query failed: {e}")))
}

/// Execute a query with pk, sort key, and item_data, returning the result.
///
/// Helper for INSERT/UPDATE operations.
pub(crate) async fn query_with_pk_sk_item(
    session: &CassandraSession,
    query: &str,
    pk: &str,
    sk: &SortKeyValue,
    item_text: &str,
) -> Result<cdrs_tokio::frame::Envelope, StorageError> {
    match sk {
        SortKeyValue::S(s) => {
            session
                .query_with_values(query, cdrs_tokio::query_values!(pk, s.as_str(), item_text))
                .await
        }
        SortKeyValue::N(n) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(pk, decimal_to_value(n), item_text),
                )
                .await
        }
        SortKeyValue::B(b) => {
            session
                .query_with_values(query, cdrs_tokio::query_values!(pk, b.clone(), item_text))
                .await
        }
    }
    .map_err(|e| StorageError::Internal(format!("Query failed: {e}")))
}

/// Execute a query with `(pk, sk, txn_id_bytes)` bound parameters.
///
/// Used for: rollback DELETE, commit DELETE.
pub(crate) async fn query_with_pk_sk_txnid(
    session: &CassandraSession,
    query: &str,
    pk: &str,
    sk: &SortKeyValue,
    txn_id: cdrs_tokio::types::value::Bytes,
) -> Result<cdrs_tokio::frame::Envelope, StorageError> {
    match sk {
        SortKeyValue::S(s) => {
            session
                .query_with_values(query, cdrs_tokio::query_values!(pk, s.as_str(), txn_id))
                .await
        }
        SortKeyValue::N(n) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(pk, decimal_to_value(n), txn_id),
                )
                .await
        }
        SortKeyValue::B(b) => {
            session
                .query_with_values(query, cdrs_tokio::query_values!(pk, b.clone(), txn_id))
                .await
        }
    }
    .map_err(|e| StorageError::Internal(format!("Query failed: {e}")))
}

/// Execute a query with `(txn_id_bytes, txn_timestamp, pk, sk)` bound parameters.
///
/// Used for: prepare UPDATE existing item.
pub(crate) async fn query_with_txnid_ts_pk_sk(
    session: &CassandraSession,
    query: &str,
    txn_id: cdrs_tokio::types::value::Bytes,
    txn_timestamp: i64,
    pk: &str,
    sk: &SortKeyValue,
) -> Result<cdrs_tokio::frame::Envelope, StorageError> {
    match sk {
        SortKeyValue::S(s) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(txn_id, txn_timestamp, pk, s.as_str()),
                )
                .await
        }
        SortKeyValue::N(n) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(txn_id, txn_timestamp, pk, decimal_to_value(n)),
                )
                .await
        }
        SortKeyValue::B(b) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(txn_id, txn_timestamp, pk, b.clone()),
                )
                .await
        }
    }
    .map_err(|e| StorageError::Internal(format!("Query failed: {e}")))
}

/// Execute a query with `(pk, sk, item_text, txn_id_bytes, txn_timestamp)` bound parameters.
///
/// Used for: prepare INSERT new item.
pub(crate) async fn query_with_pk_sk_item_txnid_ts(
    session: &CassandraSession,
    query: &str,
    pk: &str,
    sk: &SortKeyValue,
    item_text: &str,
    txn_id: cdrs_tokio::types::value::Bytes,
    txn_timestamp: i64,
) -> Result<cdrs_tokio::frame::Envelope, StorageError> {
    match sk {
        SortKeyValue::S(s) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(pk, s.as_str(), item_text, txn_id, txn_timestamp),
                )
                .await
        }
        SortKeyValue::N(n) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(
                        pk,
                        decimal_to_value(n),
                        item_text,
                        txn_id,
                        txn_timestamp
                    ),
                )
                .await
        }
        SortKeyValue::B(b) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(pk, b.clone(), item_text, txn_id, txn_timestamp),
                )
                .await
        }
    }
    .map_err(|e| StorageError::Internal(format!("Query failed: {e}")))
}

/// Execute a query with `(item_text, txn_timestamp, pk, sk, txn_id_bytes)` bound parameters.
///
/// Used for: commit PUT/UPDATE.
pub(crate) async fn query_with_item_ts_pk_sk_txnid(
    session: &CassandraSession,
    query: &str,
    item_text: &str,
    txn_timestamp: i64,
    pk: &str,
    sk: &SortKeyValue,
    txn_id: cdrs_tokio::types::value::Bytes,
) -> Result<cdrs_tokio::frame::Envelope, StorageError> {
    match sk {
        SortKeyValue::S(s) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(item_text, txn_timestamp, pk, s.as_str(), txn_id),
                )
                .await
        }
        SortKeyValue::N(n) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(
                        item_text,
                        txn_timestamp,
                        pk,
                        decimal_to_value(n),
                        txn_id
                    ),
                )
                .await
        }
        SortKeyValue::B(b) => {
            session
                .query_with_values(
                    query,
                    cdrs_tokio::query_values!(item_text, txn_timestamp, pk, b.clone(), txn_id),
                )
                .await
        }
    }
    .map_err(|e| StorageError::Internal(format!("Query failed: {e}")))
}

/// Execute a SELECT query by primary key (pk only, or pk + sort key).
///
/// Builds the appropriate query based on whether a sort key is present,
/// executes it, and returns the first row if any.
pub(crate) async fn select_by_pk(
    session: &CassandraSession,
    keyspace: &str,
    table: &str,
    columns: &str,
    pk: &str,
    sk: Option<&SortKeyValue>,
    sk_col: Option<&str>,
) -> Result<Option<cdrs_tokio::types::rows::Row>, StorageError> {
    let result = if let (Some(sk), Some(sk_col)) = (sk, sk_col) {
        let query =
            format!("SELECT {columns} FROM {keyspace}.{table} WHERE pk = ? AND {sk_col} = ?");
        query_with_pk_sk(session, &query, pk, sk).await
    } else {
        let query = format!("SELECT {columns} FROM {keyspace}.{table} WHERE pk = ?");
        session
            .query_with_values(&query, cdrs_tokio::query_values!(pk))
            .await
            .map_err(|e| StorageError::Internal(format!("Query failed: {e}")))
    }?;

    let rows = result
        .response_body()
        .map_err(|e| StorageError::Internal(e.to_string()))?
        .into_rows()
        .unwrap_or_default();

    Ok(rows.into_iter().next())
}
