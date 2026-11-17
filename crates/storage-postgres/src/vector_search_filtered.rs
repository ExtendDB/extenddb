// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Vector search with a [`VectorItemFilter`].
//!
//! The filter is evaluated in this process, on the decoded item, with the
//! condition semantics that FilterExpression uses. Translating it to SQL would
//! give a second implementation of those semantics, and the two would drift
//! on a missing attribute, a number comparison or a type mismatch.
//!
//! The statement is the unfiltered one with `LIMIT NULL`, which PostgreSQL
//! reads as no limit. Rows arrive in score order with the same tie break, and
//! the stream stops at the `top_k`-th matching row. So the cut applies to
//! matching rows only, as it does for the equality filters in SQL.

use futures::TryStreamExt;
use pgvector::Vector;
use sqlx::PgPool;
use sqlx::postgres::{PgArguments, Postgres};
use sqlx::query::QueryAs;

use extenddb_core::types::Item;
use extenddb_storage::VectorItemFilter;
use extenddb_storage::error::StorageError;

/// One row of the search statement: the ordered score, the stored vector and
/// the stored item.
pub(crate) type SearchRow = (f64, Vector, serde_json::Value);

/// Run `query` and return the first `top_k` rows whose item satisfies
/// `filter`, in the order the statement returns them.
///
/// `map_sql` turns a statement failure into the error the unfiltered path
/// reports for it.
///
/// # Errors
///
/// Returns the mapped statement error, [`StorageError::Internal`] for a stored
/// item that does not decode, or the error of [`VectorItemFilter::matches`].
pub(crate) async fn fetch_matching<'q>(
    query: QueryAs<'q, Postgres, SearchRow, PgArguments>,
    pool: &PgPool,
    filter: &dyn VectorItemFilter,
    top_k: i64,
    map_sql: impl Fn(sqlx::Error) -> StorageError,
) -> Result<Vec<SearchRow>, StorageError> {
    let wanted = usize::try_from(top_k.max(0)).unwrap_or(0);
    let mut kept = Vec::with_capacity(wanted.min(128));
    if wanted == 0 {
        return Ok(kept);
    }
    let mut rows = query.fetch(pool);
    while let Some(row) = rows.try_next().await.map_err(&map_sql)? {
        let item: Item = serde_json::from_value(row.2.clone())
            .map_err(|e| StorageError::Internal(format!("stored item: {e}")))?;
        if filter.matches(&item)? {
            kept.push(row);
            if kept.len() == wanted {
                break;
            }
        }
    }
    Ok(kept)
}
