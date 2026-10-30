// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_table` implementation for `SqliteEngine`.
//!
//! With `control_plane_delay_seconds < 1` the table and its data tables are
//! dropped immediately; otherwise the table is marked DELETING with a scheduled
//! transition that the control-plane worker completes.

use extenddb_core::types::{DeleteTableInput, TableDescription, TableStatus};
use extenddb_storage::error::StorageError;

use crate::sqlite_util::format_timestamp;
use crate::store::SqliteEngine;
use crate::table_helpers::{INDEX_COLUMNS, IndexRow, TABLE_COLUMNS, TableRow};

impl SqliteEngine {
    pub(crate) async fn delete_table_impl(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;

        // The status lookup runs inside the write transaction, so two
        // DeleteTable calls on one table are serialized: the second reads the
        // row the first committed (DELETING, or gone) rather than the ACTIVE
        // row both would otherwise read before either wrote. A DELETING row is
        // fetched too, so that a DeleteTable of a table already being deleted
        // can be answered the way the service answers it.
        let _writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row: Option<TableRow> = sqlx::query_as(&format!(
            "SELECT {TABLE_COLUMNS} FROM tables WHERE account_id = ? AND table_name = ? \
             AND table_status IN ('ACTIVE', 'CREATING', 'DELETING')"
        ))
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row = row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
        if row.table_status == "DELETING" {
            // The service answers a DeleteTable of a table that is already
            // being deleted as a table still in use (measured 2026-10-25 and
            // 2026-10-30), not with the table's description.
            return Err(StorageError::TableBeingDeleted(input.table_name.clone()));
        }
        if row.deletion_protection_enabled {
            return Err(StorageError::DeletionProtected(row.table_arn.clone()));
        }

        // A restore target (CREATING with no scheduled transition) is being
        // filled by RestoreTableFromBackup. The service refuses to delete a
        // table it is still creating, and deleting this one would make the
        // restore fail part-way; refuse until the restore finishes. An
        // abandoned target is removed by the restore sweep, not here.
        if row.table_status == "CREATING" {
            let restoring: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE table_id = ? \
                 AND table_status = 'CREATING' AND status_transition_at IS NULL)",
            )
            .bind(&row.table_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if restoring {
                return Err(StorageError::IndexesInUse(format!(
                    "Attempt to change a resource which is still in use: Table is being \
                     restored: {}",
                    row.table_name
                )));
            }
        }

        let index_rows: Vec<IndexRow> = sqlx::query_as(&format!(
            "SELECT {INDEX_COLUMNS} FROM indexes WHERE table_id = ?"
        ))
        .bind(&row.table_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let delay_secs: f64 = sqlx::query_scalar::<_, String>(
            "SELECT value FROM settings WHERE key = 'control_plane_delay_seconds'",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.25);

        let index_ids: Vec<String> = index_rows.iter().map(|r| r.index_id.clone()).collect();
        let table_id = row.table_id.clone();
        let table_arn = row.table_arn.clone();

        if delay_secs < 1.0 {
            // A vector index with `backfilling` set is an UpdateTable build in
            // flight (the with-table path never sets the column), and the
            // service refuses to delete the table underneath one; the refusal
            // clears when the index flips ACTIVE or its create is cancelled.
            // Checked INSIDE the immediate transaction rather than before it,
            // so a build whose catalog row commits while this call is waiting
            // on the write lock is still caught: the check and the cascade
            // delete are atomic against a concurrent UpdateTable.
            Self::refuse_if_index_build_in_flight(&mut tx, &row.table_id).await?;
            sqlx::query("DELETE FROM tags WHERE resource_arn = ?")
                .bind(&table_arn)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            // CASCADE removes indexes, stream shards/records.
            sqlx::query("DELETE FROM tables WHERE table_id = ?")
                .bind(&table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            for idx_id in &index_ids {
                Self::drop_index_data_table(&mut tx, idx_id).await?;
            }
            Self::drop_data_table(&mut tx, &table_id).await?;
            // Drop any still-pending GSI propagation rows for this table in the
            // same transaction that drops the tables, so the worker doesn't
            // waste a claim→deserialize→skip cycle on now-orphaned rows.
            sqlx::query("DELETE FROM gsi_pending WHERE table_id = ?")
                .bind(&table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        } else {
            let transition = format_timestamp(
                time::OffsetDateTime::now_utc() + time::Duration::seconds_f64(delay_secs),
            );
            // Same in-flight-build refusal as the immediate branch, and inside
            // the transaction that flips the table to DELETING for the same
            // reason: without it, a build committing between an earlier check
            // and this flip would have its table deleted underneath it.
            Self::refuse_if_index_build_in_flight(&mut tx, &row.table_id).await?;
            // Fenced on the table id read above, so the mark can only land on
            // the table this call described, never on a table created into the
            // same name afterwards.
            let marked = sqlx::query(
                "UPDATE tables SET table_status = 'DELETING', status_transition_at = ? \
                 WHERE account_id = ? AND table_name = ? AND table_id = ?",
            )
            .bind(&transition)
            .bind(account_id)
            .bind(&input.table_name)
            .bind(&table_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if marked.rows_affected() == 0 {
                return Err(StorageError::TableNotFound(input.table_name.clone()));
            }
            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            self.control_plane_notify.notify_one();
        }

        let desc = self.build_table_description_from_row(account_id, row, index_rows)?;
        Ok(TableDescription {
            table_status: TableStatus::Deleting,
            ..desc
        })
    }

    /// Refuse the operation while any UpdateTable-created vector index build is
    /// in flight on the table (`backfilling IS NOT NULL`; the with-table path
    /// never sets the column). Must be called inside a `BEGIN IMMEDIATE`
    /// transaction so the answer is atomic with the caller's own writes.
    async fn refuse_if_index_build_in_flight(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let in_flight: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM vector_indexes WHERE table_id = ? AND backfilling IS NOT NULL LIMIT 1",
        )
        .bind(table_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        if in_flight.is_some() {
            // The sentence AWS documents for this refusal, quoted in the vector
            // search tutorial's readiness callout and pinned by the ground-truth
            // runs of 2026-08-24 (us-east-1 and eu-west-2). Composed here rather
            // than in the engine map because the variant carries the whole
            // client-facing message: only the backend knows which in-use state
            // it refused for.
            return Err(StorageError::IndexesInUse(
                "Cannot delete table while indexes are being created, updated, or deleted."
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod deleting_window_tests {
    use crate::SqliteEngine;
    use extenddb_core::types::{DeleteTableInput, UpdateTableInput};
    use extenddb_storage::TableEngine;
    use extenddb_storage::error::StorageError;
    use serde_json::json;

    const ACCOUNT: &str = "000000000000";

    /// An engine with one ACTIVE table `t`, whose catalog row is then set to
    /// DELETING directly: the window a slow data drop would hold open.
    async fn engine_with_deleting_table() -> SqliteEngine {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");
        sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
            .execute(&engine.pool)
            .await
            .expect("delay");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(ACCOUNT)
            .execute(&engine.pool)
            .await
            .expect("account");
        let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        engine
            .create_table_impl(ACCOUNT, input, false)
            .await
            .expect("create table");
        sqlx::query("UPDATE tables SET table_status = 'DELETING' WHERE table_name = 't'")
            .execute(&engine.pool)
            .await
            .expect("mark DELETING");
        engine
    }

    /// A second DeleteTable during the window reports the table as being
    /// deleted, not as absent, and leaves the row in place.
    #[tokio::test]
    async fn a_second_delete_table_during_the_window_is_table_being_deleted() {
        let engine = engine_with_deleting_table().await;
        let err = engine
            .delete_table(
                ACCOUNT,
                DeleteTableInput {
                    table_name: "t".to_owned(),
                },
            )
            .await
            .expect_err("DeleteTable on a DELETING table");
        assert!(
            matches!(&err, StorageError::TableBeingDeleted(name) if name == "t"),
            "expected TableBeingDeleted(\"t\"), got {err:?}"
        );
        let status: String =
            sqlx::query_scalar("SELECT table_status FROM tables WHERE table_name = 't'")
                .fetch_one(&engine.pool)
                .await
                .expect("row still present");
        assert_eq!(status, "DELETING");
    }

    /// UpdateTable during the window reports the table as being deleted,
    /// not with the sentence for the other non-ACTIVE statuses.
    #[tokio::test]
    async fn update_table_during_the_window_is_table_being_deleted() {
        let engine = engine_with_deleting_table().await;
        let update: UpdateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "DeletionProtectionEnabled": true
        }))
        .expect("update");
        let err = engine
            .update_table(ACCOUNT, update)
            .await
            .expect_err("UpdateTable on a DELETING table");
        assert!(
            matches!(&err, StorageError::TableBeingDeleted(name) if name == "t"),
            "expected TableBeingDeleted(\"t\"), got {err:?}"
        );
    }

    /// A CREATING table keeps the existing answer: the new sentence is for
    /// DELETING only.
    #[tokio::test]
    async fn update_table_on_a_creating_table_keeps_table_not_active() {
        let engine = engine_with_deleting_table().await;
        sqlx::query("UPDATE tables SET table_status = 'CREATING' WHERE table_name = 't'")
            .execute(&engine.pool)
            .await
            .expect("mark CREATING");
        let update: UpdateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "DeletionProtectionEnabled": true
        }))
        .expect("update");
        let err = engine
            .update_table(ACCOUNT, update)
            .await
            .expect_err("UpdateTable on a CREATING table");
        assert!(
            matches!(&err, StorageError::TableNotActive(name) if name == "t"),
            "expected TableNotActive(\"t\"), got {err:?}"
        );
    }
}
