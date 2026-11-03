// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Row-by-row rewrites of one key column for migration 6, in an order that
//! keeps the table's primary key valid after every row.
//!
//! `PostgreSQL` checks a primary key as each row changes, not at the end of the
//! statement. Two legacy keys that differ only by a U+0002 after a U+0001, such
//! as `a\x01` and `a\x01\x02` with the same other key columns, escape to two
//! different texts, `a\x01\x02` and `a\x01\x02\x02`, but the first one's new
//! text is the second one's legacy text. If the shorter one is rewritten first,
//! the table holds `a\x01\x02` twice until the longer one is rewritten, and the
//! update fails with SQLSTATE 23505. A one-statement `UPDATE` meets the two rows
//! in heap order, so whether it fails depends on where they are stored.
//!
//! [`rewrite_longest_first`] rewrites the rows in descending byte length of
//! their legacy text, which never produces such a duplicate:
//!
//! - Every rewrite makes the text longer. Every row it rewrites holds a U+0001,
//!   and the escape adds one byte for each; a rebuilt composite key also gives
//!   each part its new length, which never takes fewer digits than the old one.
//! - Suppose rewriting row `r` gives the text that row `s` holds, and the two
//!   rows agree on the other key columns. If `s` has not been rewritten yet, it
//!   holds its legacy text, and that text is the new text of `r`, so it is
//!   longer than the legacy text of `r` and `s` was rewritten earlier. If `s`
//!   has been rewritten, the two rows have the same new text, and since the
//!   escape is injective their legacy texts were equal.
//! - Each other key column holds the legacy texts of every row, or, when an
//!   earlier pass of the same transaction rewrote it, their escapes. Either
//!   way two rows agree on it now exactly when they agreed before the
//!   migration. Two rows that agreed on every key column before the migration
//!   would have broken the primary key, so no rewrite meets a duplicate.

use extenddb_storage::migrations::MigrationError;
use sqlx::PgConnection;

use super::db_error;

/// Rows read from the cursor per round trip, and the batch `escape_jsonb_rows`
/// reads.
pub(crate) const BATCH: usize = 500;

/// The cursor [`rewrite_longest_first`] declares and closes before returning.
const CURSOR: &str = "escape_0006_longest_first";

/// Rewrite `col` of the rows that hold U+0001 and that `rows` (a boolean SQL
/// expression over the row) selects, one row at a time, longest legacy text
/// first; `rewrite` gives a row's new text from its legacy text. Runs in the
/// caller's transaction and returns the number of rows changed. `what` names
/// the column in errors, with the DynamoDB table when the caller knows it.
///
/// One cursor is declared over the matching rows in that order and read
/// [`BATCH`] rows at a time, so this process holds one batch, and `PostgreSQL`
/// sorts the matching rows in `work_mem`, spilling to temporary files beyond
/// it. The cursor's rows are fixed when it is declared and each is returned
/// once, so no row is rewritten twice. Each row is locked as the cursor
/// returns it, as the single-statement path locks the rows it updates.
pub(crate) async fn rewrite_longest_first(
    conn: &mut PgConnection,
    quoted_table: &str,
    col: &str,
    rows: &str,
    what: &str,
    rewrite: impl Fn(&str) -> Result<String, MigrationError>,
) -> Result<u64, MigrationError> {
    let failed = |step: &str, e: &sqlx::Error| db_error(&format!("{step} {what}"), e);
    sqlx::query(&format!(
        "DECLARE {CURSOR} NO SCROLL CURSOR FOR \
         SELECT ctid::text, \"{col}\" FROM {quoted_table} \
         WHERE position(chr(1) IN \"{col}\") > 0 AND ({rows}) \
         ORDER BY octet_length(\"{col}\") DESC FOR UPDATE"
    ))
    .execute(&mut *conn)
    .await
    .map_err(|e| failed("find the rows to rewrite in", &e))?;
    let fetch = format!("FETCH {BATCH} FROM {CURSOR}");
    let update = format!("UPDATE {quoted_table} SET \"{col}\" = $1 WHERE ctid = $2::tid");
    let mut changed: u64 = 0;
    loop {
        let batch: Vec<(String, String)> = sqlx::query_as(&fetch)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| failed("read the rows to rewrite in", &e))?;
        for (ctid, legacy) in &batch {
            let done = sqlx::query(&update)
                .bind(rewrite(legacy)?)
                .bind(ctid)
                .execute(&mut *conn)
                .await
                .map_err(|e| failed("re-encode", &e))?;
            changed += done.rows_affected();
        }
        if batch.len() < BATCH {
            break;
        }
    }
    sqlx::query(&format!("CLOSE {CURSOR}"))
        .execute(&mut *conn)
        .await
        .map_err(|e| failed("close the cursor over", &e))?;
    Ok(changed)
}

/// A database error as text, with `PostgreSQL`'s DETAIL when the error has one;
/// for a unique violation the DETAIL names the key already present. sqlx's
/// `Display` writes the message alone. The DETAIL is quoted with its control
/// characters escaped, since the keys this migration rewrites hold them.
pub(crate) fn with_detail(e: &sqlx::Error) -> String {
    let detail = match e {
        sqlx::Error::Database(db) => db
            .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
            .and_then(sqlx::postgres::PgDatabaseError::detail),
        _ => None,
    };
    match detail {
        Some(detail) => format!("{e} (DETAIL: {detail:?})"),
        None => e.to_string(),
    }
}
