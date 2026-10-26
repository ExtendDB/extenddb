// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data table operations for Cassandra.
//!
//! Every statement under this module states its consistency level through
//! [`read_params`](crate::cassandra_util::read_params),
//! [`write_params`](crate::cassandra_util::write_params) or
//! [`lwt_params`](crate::cassandra_util::lwt_params); the schema statements in
//! `ddl.rs` are the one exception. Two checks hold that. The lint below
//! rejects the `Session` methods and `cassandra_util` helpers named in
//! `clippy.toml`, each of which runs a statement at the driver default; it
//! cannot name a `Default` constructor (`StatementParams::default()`,
//! `StatementParamsBuilder::default()`, `BatchQueryBuilder::default()`) and it
//! does not see which constructor a call passes or the level a
//! `BatchQueryBuilder` sets. The test in
//! `level_pairing_tests.rs` reads the statement sites in this module's files
//! whose CQL and level it can resolve and checks that a `SELECT` carries a
//! read level, a plain write `write_params` and an `IF` statement
//! `lwt_params`, and that no batch builder other than
//! [`batch_builder`](crate::cassandra_util::batch_builder) appears here.
//! Allow the lint only on a schema statement.

#![warn(clippy::disallowed_methods)]

use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::frame::Envelope;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::statement::StatementParams;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::value::{Bytes, Value};
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

#[cfg(test)]
mod level_pairing_tests;

use condition::check_condition;
use extenddb_core::expression;
use index::sk_to_value;

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

/// Convert a DynamoDB numeric value (`BigDecimal`) into the `cdrs_tokio`
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

/// Bind a `DynamoDB` numeric value (`BigDecimal`) as a Cassandra `decimal` bound
/// parameter `Value`.
///
/// This replaces the previous workaround that bound `N` values as strings,
/// which the Cassandra `decimal` column rejected ("Expected 0 or at least 4
/// bytes") and which broke column-level numeric comparisons.
pub(crate) fn decimal_to_value(n: &bigdecimal::BigDecimal) -> cdrs_tokio::types::value::Value {
    cdrs_tokio::types::value::Value::from(bigdecimal_to_cql_decimal(n))
}

/// Execute `query` with positional `values` at the level `params` builds:
/// [`read_params`](crate::cassandra_util::read_params),
/// [`write_params`](crate::cassandra_util::write_params) or
/// [`lwt_params`](crate::cassandra_util::lwt_params).
async fn execute_bound(
    session: &CassandraSession,
    query: &str,
    values: Vec<Value>,
    params: impl FnOnce(QueryValues) -> StatementParams,
) -> Result<Envelope, StorageError> {
    let params = params(QueryValues::SimpleValues(values));
    crate::cassandra_util::execute_at(session, query, params, "Query failed").await
}

/// Execute a query with `(pk, sk)` bound parameters at the level `params`
/// states.
pub(crate) async fn query_with_pk_sk(
    session: &CassandraSession,
    query: &str,
    pk: &str,
    sk: &SortKeyValue,
    params: impl FnOnce(QueryValues) -> StatementParams,
) -> Result<Envelope, StorageError> {
    execute_bound(session, query, vec![pk.into(), sk_to_value(sk)], params).await
}

/// Execute a query with `(pk, sk, txn_id_bytes)` bound parameters.
///
/// Used for: rollback DELETE, commit DELETE.
pub(crate) async fn query_with_pk_sk_txnid(
    session: &CassandraSession,
    query: &str,
    pk: &str,
    sk: &SortKeyValue,
    txn_id: Bytes,
    params: impl FnOnce(QueryValues) -> StatementParams,
) -> Result<Envelope, StorageError> {
    let values = vec![pk.into(), sk_to_value(sk), txn_id.into()];
    execute_bound(session, query, values, params).await
}

/// Execute a query with `(txn_id_bytes, txn_timestamp, pk, sk)` bound parameters.
///
/// Used for: prepare UPDATE existing item.
pub(crate) async fn query_with_txnid_ts_pk_sk(
    session: &CassandraSession,
    query: &str,
    txn_id: Bytes,
    txn_timestamp: i64,
    pk: &str,
    sk: &SortKeyValue,
    params: impl FnOnce(QueryValues) -> StatementParams,
) -> Result<Envelope, StorageError> {
    let values = vec![
        txn_id.into(),
        txn_timestamp.into(),
        pk.into(),
        sk_to_value(sk),
    ];
    execute_bound(session, query, values, params).await
}

/// Execute a query with `(pk, sk, item_text, txn_id_bytes, txn_timestamp)` bound parameters.
///
/// Used for: prepare INSERT new item.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn query_with_pk_sk_item_txnid_ts(
    session: &CassandraSession,
    query: &str,
    pk: &str,
    sk: &SortKeyValue,
    item_text: &str,
    txn_id: Bytes,
    txn_timestamp: i64,
    params: impl FnOnce(QueryValues) -> StatementParams,
) -> Result<Envelope, StorageError> {
    let values = vec![
        pk.into(),
        sk_to_value(sk),
        item_text.into(),
        txn_id.into(),
        txn_timestamp.into(),
    ];
    execute_bound(session, query, values, params).await
}

/// Execute a query with `(item_text, txn_timestamp, pk, sk, txn_id_bytes)` bound parameters.
///
/// Used for: commit PUT/UPDATE.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn query_with_item_ts_pk_sk_txnid(
    session: &CassandraSession,
    query: &str,
    item_text: &str,
    txn_timestamp: i64,
    pk: &str,
    sk: &SortKeyValue,
    txn_id: Bytes,
    params: impl FnOnce(QueryValues) -> StatementParams,
) -> Result<Envelope, StorageError> {
    let values = vec![
        item_text.into(),
        txn_timestamp.into(),
        pk.into(),
        sk_to_value(sk),
        txn_id.into(),
    ];
    execute_bound(session, query, values, params).await
}

/// Execute a SELECT query by primary key (pk only, or pk + sort key) at
/// [`read_consistency`](crate::cassandra_util::read_consistency)`(consistent_read)`.
///
/// Builds the appropriate query based on whether a sort key is present,
/// executes it, and returns the first row if any.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn select_by_pk(
    session: &CassandraSession,
    keyspace: &str,
    table: &str,
    columns: &str,
    pk: &str,
    sk: Option<&SortKeyValue>,
    sk_col: Option<&str>,
    consistent_read: bool,
) -> Result<Option<cdrs_tokio::types::rows::Row>, StorageError> {
    let params = |values| crate::cassandra_util::read_params(consistent_read, values);
    let result = if let (Some(sk), Some(sk_col)) = (sk, sk_col) {
        let query =
            format!("SELECT {columns} FROM {keyspace}.{table} WHERE pk = ? AND {sk_col} = ?");
        query_with_pk_sk(session, &query, pk, sk, params).await
    } else {
        let query = format!("SELECT {columns} FROM {keyspace}.{table} WHERE pk = ?");
        execute_bound(session, &query, vec![pk.into()], params).await
    }?;

    let rows = result
        .response_body()
        .map_err(|e| StorageError::Internal(e.to_string()))?
        .into_rows()
        .unwrap_or_default();

    Ok(rows.into_iter().next())
}
