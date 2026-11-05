// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The input files of a bulk operation, one row per file in
//! `bulk_operation_input_files` (catalog migration 8). Start inserts
//! them, a checkpoint updates the rows of the files its cursor passes, and
//! every full read of the record attaches them. The operation row's own
//! `input_files` column keeps an empty array; the per-file figures lived
//! there once, and every checkpoint that passed a file rewrote the whole
//! list, so a load of N files wrote N lists of N entries.

use super::*;

/// Insert the files of a new operation in list order, `file_index` from 0,
/// in one statement.
pub(super) async fn insert_files(
    conn: &mut sqlx::PgConnection,
    account_id: &str,
    operation_id: &str,
    files: &[InputFile],
) -> Result<(), StorageError> {
    if files.is_empty() {
        return Ok(());
    }
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    let sizes: Vec<i64> = files.iter().map(|f| f.size).collect();
    let rows: Vec<Option<i64>> = files.iter().map(|f| f.rows).collect();
    let etags: Vec<Option<&str>> = files.iter().map(|f| f.etag.as_deref()).collect();
    let abandons: Vec<Option<String>> = files
        .iter()
        .map(|f| f.abandon.map(|a| a.to_json().to_string()))
        .collect();
    sqlx::query(
        "INSERT INTO bulk_operation_input_files \
             (account_id, operation_id, file_index, path, size, row_count, etag, abandon) \
         SELECT $1, $2, f.ord - 1, f.path, f.size, f.row_count, f.etag, f.abandon::jsonb \
           FROM UNNEST($3::text[], $4::bigint[], $5::bigint[], $6::text[], $7::text[]) \
                WITH ORDINALITY AS f(path, size, row_count, etag, abandon, ord)",
    )
    .bind(account_id)
    .bind(operation_id)
    .bind(&paths)
    .bind(&sizes)
    .bind(&rows)
    .bind(&etags)
    .bind(&abandons)
    .execute(conn)
    .await
    .map_err(internal)?;
    Ok(())
}

/// One stored file row: index, path, size, row count, tag, abandon.
type FileRow = (i64, String, i64, Option<i64>, Option<String>, Option<Value>);

/// The stored file rows of one operation, in list order.
async fn fetch_files(
    conn: &mut sqlx::PgConnection,
    account_id: &str,
    operation_id: &str,
) -> Result<Vec<FileRow>, StorageError> {
    sqlx::query_as(
        "SELECT file_index, path, size, row_count, etag, abandon \
         FROM bulk_operation_input_files \
         WHERE account_id = $1 AND operation_id = $2 ORDER BY file_index",
    )
    .bind(account_id)
    .bind(operation_id)
    .fetch_all(conn)
    .await
    .map_err(internal)
}

/// A stored abandon: absent is no abandon, and anything that is not an
/// abandon the store wrote, a JSON `null` included, is refused as
/// unreadable, as the SQLite store refuses it.
pub(super) fn abandon_from_stored(
    stored: Option<&Value>,
) -> Result<Option<FileAbandon>, StorageError> {
    let Some(value) = stored else {
        return Ok(None);
    };
    FileAbandon::from_json(value).map(Some).ok_or_else(|| {
        StorageError::Internal(
            "bulk_operation_input_files row has an unreadable abandon".to_owned(),
        )
    })
}

/// Decode the stored file rows into the record's file list. The rows must
/// number the files from 0 without a gap.
fn decode_files(rows: Vec<FileRow>) -> Result<Vec<InputFile>, StorageError> {
    rows.into_iter()
        .enumerate()
        .map(|(position, (index, path, size, rows, etag, abandon))| {
            if usize::try_from(index).ok() != Some(position) {
                return Err(StorageError::Internal(format!(
                    "bulk_operation_input_files has file {index} where file {position} belongs"
                )));
            }
            Ok(InputFile {
                path,
                size,
                rows,
                etag,
                abandon: abandon_from_stored(abandon.as_ref())?,
            })
        })
        .collect()
}

/// Attach the stored files to a record decoded from its operation row.
pub(super) async fn attach_files(
    conn: &mut sqlx::PgConnection,
    record: &mut BulkOperationRecord,
) -> Result<(), StorageError> {
    let rows = fetch_files(conn, &record.account_id, &record.operation_id).await?;
    record.input_files = decode_files(rows)?;
    Ok(())
}

/// Attach the stored files to a record the claim or the listing decoded,
/// or log at ERROR why they do not decode and answer `None`, the way
/// `unreadable::decode_or_log` treats the operation row. A read that fails
/// is an error, not an unreadable row.
pub(super) async fn with_files_or_log(
    conn: &mut sqlx::PgConnection,
    mut record: BulkOperationRecord,
    action: &str,
) -> Result<Option<BulkOperationRecord>, StorageError> {
    let rows = fetch_files(conn, &record.account_id, &record.operation_id).await?;
    match decode_files(rows) {
        Ok(files) => {
            record.input_files = files;
            Ok(Some(record))
        }
        Err(error) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                "bulk operation record does not decode, {action}: {error}"
            );
            Ok(None)
        }
    }
}

/// Delete the file rows of one operation, in the caller's transaction.
pub(super) async fn delete_files(
    conn: &mut sqlx::PgConnection,
    account_id: &str,
    operation_id: &str,
) -> Result<(), StorageError> {
    sqlx::query(
        "DELETE FROM bulk_operation_input_files WHERE account_id = $1 AND operation_id = $2",
    )
    .bind(account_id)
    .bind(operation_id)
    .execute(conn)
    .await
    .map_err(internal)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_null_abandon_is_refused_like_an_unreadable_one() {
        assert!(
            matches!(
                abandon_from_stored(Some(&Value::Null)),
                Err(StorageError::Internal(_))
            ),
            "a null abandon reads as unreadable, as on SQLite"
        );
        assert!(
            abandon_from_stored(None)
                .expect("no abandon is readable")
                .is_none()
        );
    }

    #[test]
    fn the_file_rows_must_number_the_files_without_a_gap() {
        let row = |index: i64| (index, format!("in/{index}.json"), 1, None, None, None);
        assert_eq!(
            decode_files(vec![row(0), row(1)]).expect("in order").len(),
            2
        );
        assert!(matches!(
            decode_files(vec![row(0), row(2)]),
            Err(StorageError::Internal(_))
        ));
    }
}
