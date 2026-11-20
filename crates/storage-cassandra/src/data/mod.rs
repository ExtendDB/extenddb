// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data table operations for Cassandra.

use crate::cassandra_util::PolicySession as _;
use crate::cassandra_util::Stmt;
use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::Item;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::SortKeyValue;
use index::sk_to_value;

mod condition;
mod data_engine;
pub mod ddl;
mod delete_item;
pub mod index;
mod put_get_item;
mod query;
mod query_helpers;
mod scan;
mod sk_range;
mod transact_stream;
pub mod transaction_ledger;
mod transactions;
pub(crate) mod ttl;
mod update_item;
pub mod version;

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

/// Run `query` with `values` as a statement of `kind`, at the level the
/// consistency policy gives that kind ([`crate::cassandra_util::params`]).
async fn run(
    session: &CassandraSession,
    query: &str,
    kind: Stmt,
    values: cdrs_tokio::query::QueryValues,
) -> Result<cdrs_tokio::frame::Envelope, StorageError> {
    session
        .run(kind, query, values)
        .await
        .map_err(|e| StorageError::Internal(format!("Query failed: {e}")))
}

/// Execute a statement of `kind` with `(pk, sk)` bound parameters.
pub(crate) async fn query_with_pk_sk(
    session: &CassandraSession,
    query: &str,
    kind: Stmt,
    pk: &str,
    sk: &SortKeyValue,
) -> Result<cdrs_tokio::frame::Envelope, StorageError> {
    run(
        session,
        query,
        kind,
        cdrs_tokio::query_values!(pk, sk_to_value(sk)),
    )
    .await
}

/// Execute an LWT with `(pk, sk, txn_id_bytes)` bound parameters.
///
/// Used for: rollback DELETE, commit DELETE.
pub(crate) async fn query_with_pk_sk_txnid(
    session: &CassandraSession,
    query: &str,
    pk: &str,
    sk: &SortKeyValue,
    txn_id: cdrs_tokio::types::value::Bytes,
) -> Result<cdrs_tokio::frame::Envelope, StorageError> {
    run(
        session,
        query,
        Stmt::Lwt,
        cdrs_tokio::query_values!(pk, sk_to_value(sk), txn_id),
    )
    .await
}

/// Execute an LWT with `(txn_id_bytes, txn_timestamp, pk, sk)` bound parameters.
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
    run(
        session,
        query,
        Stmt::Lwt,
        cdrs_tokio::query_values!(txn_id, txn_timestamp, pk, sk_to_value(sk)),
    )
    .await
}

/// Execute an LWT with `(pk, sk, item_text, txn_id_bytes, txn_timestamp)`
/// bound parameters.
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
    run(
        session,
        query,
        Stmt::Lwt,
        cdrs_tokio::query_values!(pk, sk_to_value(sk), item_text, txn_id, txn_timestamp),
    )
    .await
}

/// Execute a SELECT query by primary key (pk only, or pk + sort key).
///
/// Builds the appropriate query based on whether a sort key is present,
/// executes it at `LOCAL_QUORUM`, and returns the first row if any.
pub(crate) async fn select_by_pk(
    session: &CassandraSession,
    keyspace: &str,
    table: &str,
    columns: &str,
    pk: &str,
    sk: Option<&SortKeyValue>,
    sk_col: Option<&str>,
) -> Result<Option<cdrs_tokio::types::rows::Row>, StorageError> {
    select_by_pk_at(
        session,
        keyspace,
        table,
        columns,
        pk,
        sk,
        sk_col,
        Stmt::Read,
    )
    .await
}

/// [`select_by_pk`] as a read of `kind`: [`Stmt::Read`] (`LOCAL_QUORUM`) or
/// [`Stmt::SerialRead`] (`LOCAL_SERIAL`). A read at `LOCAL_SERIAL` sees every
/// LWT that has committed, and completes one still in progress, before it
/// answers.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn select_by_pk_at(
    session: &CassandraSession,
    keyspace: &str,
    table: &str,
    columns: &str,
    pk: &str,
    sk: Option<&SortKeyValue>,
    sk_col: Option<&str>,
    kind: Stmt,
) -> Result<Option<cdrs_tokio::types::rows::Row>, StorageError> {
    let (query, values) = if let (Some(sk), Some(sk_col)) = (sk, sk_col) {
        (
            format!("SELECT {columns} FROM {keyspace}.{table} WHERE pk = ? AND {sk_col} = ?"),
            cdrs_tokio::query_values!(pk, sk_to_value(sk)),
        )
    } else {
        (
            format!("SELECT {columns} FROM {keyspace}.{table} WHERE pk = ?"),
            cdrs_tokio::query_values!(pk),
        )
    };
    let result = run(session, &query, kind, values).await?;

    let rows = result
        .response_body()
        .map_err(|e| StorageError::Internal(e.to_string()))?
        .into_rows()
        .unwrap_or_default();

    Ok(rows.into_iter().next())
}
