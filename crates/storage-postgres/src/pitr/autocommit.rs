// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The put and delete that need no transaction, each one autocommit
//! statement that also records the write's history row when the table's
//! recovery is enabled.

use extenddb_core::types::{ScalarAttributeType, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, sk_column};

use super::{DB_NOW_US, sk_text};
use crate::data::data_table_name;
use crate::data::index::db_error;

/// The history row of a write that is its statement's only data row, added
/// when the table has a `pitr_state` row. `$1` is the data pk, then
/// `$table`, `$sk` and `$image` name the statement's parameters. The commit
/// time is read per row of `w`, so after the data write took the row lock.
fn history_from(w: &str, table: &str, sk: &str, image: &str) -> String {
    format!(
        "INSERT INTO item_history (table_id, pk, sk, commit_time_us, item_data) \
         SELECT {table}, $1, {sk}, {DB_NOW_US}, {image} FROM {w} \
         WHERE EXISTS (SELECT 1 FROM pitr_state WHERE table_id = {table})"
    )
}

/// A put with no condition, index or stream, as one autocommit statement:
/// the upsert, and the history row when the table's recovery is enabled.
pub(crate) async fn autocommit_put(
    pool: &sqlx::PgPool,
    key_info: &TableKeyInfo,
    pk_text: &str,
    sk: Option<&SortKeyValue>,
    item_json: &serde_json::Value,
) -> Result<(), StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    let Some(sk) = sk else {
        let sql = format!(
            "WITH w AS (INSERT INTO {ddb_table} (pk, item_data) VALUES ($1, $2) \
             ON CONFLICT (pk) DO UPDATE SET item_data = EXCLUDED.item_data RETURNING 1) {}",
            history_from("w", "$3", "''", "$2")
        );
        sqlx::query(&sql)
            .bind(pk_text)
            .bind(item_json)
            .bind(&key_info.table_id)
            .execute(pool)
            .await
            .map_err(db_error)?;
        return Ok(());
    };
    let sk_col = sk_column(sk_type(sk));
    let sql = format!(
        "WITH w AS (INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES ($1, $2, $3) \
         ON CONFLICT (pk, {sk_col}) DO UPDATE SET item_data = EXCLUDED.item_data RETURNING 1) {}",
        history_from("w", "$4", "$5", "$3")
    );
    let query = bind_typed_sk(sqlx::query(&sql).bind(pk_text), sk);
    query
        .bind(item_json)
        .bind(&key_info.table_id)
        .bind(sk_text(sk))
        .execute(pool)
        .await
        .map_err(db_error)?;
    Ok(())
}

/// A delete with no condition, index or stream, as one autocommit
/// statement: the delete, and the history row when it removed an item and
/// the table's recovery is enabled.
pub(crate) async fn autocommit_delete(
    pool: &sqlx::PgPool,
    key_info: &TableKeyInfo,
    pk_text: &str,
    sk: Option<&SortKeyValue>,
) -> Result<(), StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    let Some(sk) = sk else {
        let sql = format!(
            "WITH w AS (DELETE FROM {ddb_table} WHERE pk = $1 RETURNING 1) {}",
            history_from("w", "$2", "''", "NULL")
        );
        sqlx::query(&sql)
            .bind(pk_text)
            .bind(&key_info.table_id)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        return Ok(());
    };
    let sk_col = sk_column(sk_type(sk));
    let sql = format!(
        "WITH w AS (DELETE FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2 RETURNING 1) {}",
        history_from("w", "$3", "$4", "NULL")
    );
    bind_typed_sk(sqlx::query(&sql).bind(pk_text), sk)
        .bind(&key_info.table_id)
        .bind(sk_text(sk))
        .execute(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

fn sk_type(sk: &SortKeyValue) -> ScalarAttributeType {
    match sk {
        SortKeyValue::S(_) => ScalarAttributeType::S,
        SortKeyValue::N(_) => ScalarAttributeType::N,
        SortKeyValue::B(_) => ScalarAttributeType::B,
    }
}

fn bind_typed_sk<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    sk: &'q SortKeyValue,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match sk {
        SortKeyValue::S(s) => query.bind(s),
        SortKeyValue::N(n) => query.bind(n),
        SortKeyValue::B(b) => query.bind(b),
    }
}
