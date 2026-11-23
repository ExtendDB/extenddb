// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Vector search with a [`VectorItemFilter`].
//!
//! The filter is evaluated in this process, on the decoded item, with the
//! condition semantics that FilterExpression uses. Translating it to SQL would
//! give a second implementation of those semantics, and the two would drift
//! on a missing attribute, a number comparison or a type mismatch.
//!
//! So the server returns candidates in score order, with the same tie break
//! as the unfiltered statement, and the search keeps the first `top_k` that
//! match. It reads the ordering in pages and stops at the `top_k`-th match:
//!
//! - A page is the ordering after the last row of the page before, compared
//!   on the whole sort key (`(score, base key) > (last score, last base
//!   key)`), with a `LIMIT`. It carries each row's score and address only, so
//!   it is a top-N sort of narrow rows that fits in the server's sort memory,
//!   which PostgreSQL may run in parallel, and it never sorts again the rows
//!   that earlier pages returned.
//! - The first page holds four times `top_k` rows, at least
//!   [`FIRST_PAGE_MIN`], and each next page eight times the one before, up
//!   to [`MAX_PAGE`]. A condition that most rows satisfy finishes after one
//!   page; one that few rows satisfy needs a number of pages that grows with
//!   the logarithm of the rows it reads.
//! - The items of a page are read by address, [`ITEM_BATCH`] at a time and in
//!   page order, so the search decodes items only up to the row where it
//!   stops.
//!
//! The kept rows' embeddings are read at the end by address. Every statement
//! runs in one transaction ([`ScanConnection`]), so all of them read one
//! snapshot, the addresses stay valid between them, and the result is the one
//! a single statement would give. Dropping the search future cancels the
//! statement in flight.

use std::collections::HashMap;

use futures::TryStreamExt;
use pgvector::Vector;
use sqlx::PgPool;
use sqlx::postgres::{PgArguments, Postgres};
use sqlx::query::QueryAs;
use sqlx::types::Json;

use extenddb_core::types::Item;
use extenddb_storage::VectorItemFilter;
use extenddb_storage::error::StorageError;

use crate::vector_search_scan::ScanConnection;

/// Rows of the first page, at least.
const FIRST_PAGE_MIN: i64 = 64;
/// Rows of a page, at most. Bounds what one statement sorts and sends.
const MAX_PAGE: i64 = 65_536;
/// Items read per statement. Bounds the items decoded past the row where the
/// search stops.
const ITEM_BATCH: usize = 1024;

/// Turns a statement failure into the error the search reports.
pub(crate) type MapSql<'a> = dyn Fn(sqlx::Error) -> StorageError + Send + Sync + 'a;

/// One kept row: the ordered score, the stored vector and the stored item.
pub(crate) type MatchedRow = (f64, Vector, Item);

/// The search statement without its embedding, and what it binds.
///
/// `score` is the scoring expression, `from` the `FROM ... WHERE ...` clause
/// and `key_columns` the base key columns that break score ties, in order.
/// They read the parameters in the unfiltered statement's numbering: `$1` the
/// query vector, `$2` the partition, `$3` the limit, `$4` the query norm, then
/// two per equality filter. A page after the first binds the last score and
/// the last row address after those.
pub(crate) struct ScanStatement {
    pub(crate) score: String,
    pub(crate) from: String,
    pub(crate) key_columns: Vec<String>,
    pub(crate) vec_table: String,
    pub(crate) vector: Vector,
    pub(crate) partition: Vec<u8>,
    pub(crate) norm: f64,
    /// Each equality filter's name and its value as JSON text.
    pub(crate) filters: Vec<(String, String)>,
}

impl ScanStatement {
    /// The first page (`after` false) or a page after a given row (`after`
    /// true), at most `$3` rows.
    fn page_sql(&self, after: bool) -> String {
        let keys = self.key_columns.join(", ");
        let order = self
            .key_columns
            .iter()
            .map(|col| format!("{col} ASC"))
            .collect::<Vec<_>>()
            .join(", ");
        let after = if after {
            // The last row's key is read by its address, which is valid in
            // this snapshot, so the key needs no bind of its own type.
            let last_score = 5 + 2 * self.filters.len();
            format!(
                " WHERE (score, {keys}) > (SELECT ${last_score}::float8, {keys} FROM {} \
                 WHERE ctid = ${}::text::tid)",
                self.vec_table,
                last_score + 1
            )
        } else {
            String::new()
        };
        format!(
            "SELECT score, rid FROM (SELECT {} AS score, ctid::text AS rid, {keys} {}) \
             AS candidates{after} ORDER BY score ASC, {order} LIMIT $3",
            self.score, self.from
        )
    }

    fn bind<'q>(
        &self,
        sql: &'q str,
        limit: i64,
        after: Option<(f64, String)>,
    ) -> QueryAs<'q, Postgres, (f64, String), PgArguments> {
        let mut query = sqlx::query_as::<_, (f64, String)>(sql)
            .bind(self.vector.clone())
            .bind(self.partition.clone())
            .bind(limit)
            .bind(self.norm);
        for (name, value) in &self.filters {
            query = query.bind(name.clone()).bind(value.clone());
        }
        if let Some((score, rid)) = after {
            query = query.bind(score).bind(rid);
        }
        query
    }
}

/// Keep each row of `page` whose item matches, in order, until `kept` holds
/// `wanted` rows. Returns whether it does.
///
/// The items are read by address, [`ITEM_BATCH`] at a time, so a search that
/// stops early in a page decodes only the items up to that point.
async fn offer_page(
    scan: &mut ScanConnection,
    statement: &ScanStatement,
    page: &[(f64, String)],
    filter: &dyn VectorItemFilter,
    wanted: usize,
    kept: &mut Vec<(f64, String, Item)>,
    map_sql: &MapSql<'_>,
) -> Result<bool, StorageError> {
    // In the order of `rids`: the ordinality is the position in the array.
    let sql = format!(
        "SELECT page.n, item_data FROM unnest($1::text[]) WITH ORDINALITY AS page(rid, n) \
         JOIN {} AS v ON v.ctid = page.rid::tid ORDER BY page.n",
        statement.vec_table
    );
    for batch in page.chunks(ITEM_BATCH) {
        let rids: Vec<&str> = batch.iter().map(|(_, rid)| rid.as_str()).collect();
        let mut items = sqlx::query_as::<_, (i64, Json<Item>)>(&sql)
            .bind(rids)
            .fetch(scan.connection());
        let mut position = 0usize;
        while let Some((n, Json(item))) = items.try_next().await.map_err(map_sql)? {
            let Some((score, rid)) = batch.get(position) else {
                return Err(vanished("past the end of the page"));
            };
            // The join drops a row whose address no longer resolves, which
            // one snapshot rules out; a gap in the positions would show it.
            if usize::try_from(n).ok() != Some(position + 1) {
                return Err(vanished(rid));
            }
            position += 1;
            if filter.matches(&item)? {
                kept.push((*score, rid.clone(), item));
                if kept.len() == wanted {
                    return Ok(true);
                }
            }
        }
        if let Some((_, rid)) = batch.get(position) {
            return Err(vanished(rid));
        }
    }
    Ok(false)
}

fn vanished(rid: &str) -> StorageError {
    StorageError::Internal(format!("vector row {rid} vanished inside one snapshot"))
}

/// Return the first `top_k` rows of `statement`'s ordering whose item
/// satisfies `filter`, in that order.
///
/// `map_sql` turns a statement failure into the error the unfiltered path
/// reports for it.
///
/// # Errors
///
/// Returns the mapped statement error, which covers a stored item that does
/// not decode, or the error of [`VectorItemFilter::matches`].
pub(crate) async fn fetch_matching(
    statement: &ScanStatement,
    pool: &PgPool,
    filter: &dyn VectorItemFilter,
    top_k: i64,
    map_sql: &MapSql<'_>,
) -> Result<Vec<MatchedRow>, StorageError> {
    let wanted = usize::try_from(top_k.max(0)).unwrap_or(0);
    if wanted == 0 {
        return Ok(Vec::new());
    }
    let mut scan = ScanConnection::begin(pool).await.map_err(map_sql)?;
    let mut kept = Vec::with_capacity(wanted.min(128));

    let (first, next) = (statement.page_sql(false), statement.page_sql(true));
    let mut page = top_k.saturating_mul(4).clamp(FIRST_PAGE_MIN, MAX_PAGE);
    let mut after = None;
    loop {
        let sql = if after.is_some() { &next } else { &first };
        let rows: Vec<(f64, String)> = statement
            .bind(sql, page, after.take())
            .fetch_all(scan.connection())
            .await
            .map_err(map_sql)?;
        let full = offer_page(
            &mut scan, statement, &rows, filter, wanted, &mut kept, map_sql,
        )
        .await?;
        // A page shorter than its limit ends the ordering.
        if full || i64::try_from(rows.len()).unwrap_or(i64::MAX) < page {
            break;
        }
        after = rows.last().cloned();
        page = page.saturating_mul(8).min(MAX_PAGE);
    }

    let embeddings = read_embeddings(&mut scan, statement, &kept, map_sql).await?;
    scan.commit().await.map_err(map_sql)?;

    kept.into_iter()
        .map(|(score, rid, item)| {
            let embedding = embeddings
                .get(&rid)
                .cloned()
                .ok_or_else(|| vanished(&rid))?;
            Ok((score, embedding, item))
        })
        .collect()
}

/// The stored vectors of the kept rows, by row address.
async fn read_embeddings(
    scan: &mut ScanConnection,
    statement: &ScanStatement,
    kept: &[(f64, String, Item)],
    map_sql: &MapSql<'_>,
) -> Result<HashMap<String, Vector>, StorageError> {
    if kept.is_empty() {
        return Ok(HashMap::new());
    }
    let rids: Vec<&str> = kept.iter().map(|(_, rid, _)| rid.as_str()).collect();
    let sql = format!(
        "SELECT ctid::text, embedding FROM {} WHERE ctid = ANY($1::text[]::tid[])",
        statement.vec_table
    );
    let rows: Vec<(String, Vector)> = sqlx::query_as(&sql)
        .bind(rids)
        .fetch_all(scan.connection())
        .await
        .map_err(map_sql)?;
    Ok(rows.into_iter().collect())
}
