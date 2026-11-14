// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The data-plane side of a store-backed restore on Cassandra: the
//! [`RestoreEngine`] methods the engine's restore job calls.
//!
//! `bulk_load` writes one verified data file's items into the CREATING
//! target, each item's base row and every GSI and LSI row in one logged
//! batch ([`CassandraEngine::put_restored_item_at`], no lightweight
//! transaction: a CREATING restore target has no writer but its job),
//! `RESTORE_WRITE_CONCURRENCY` in flight, in batches of
//! at most [`RESTORE_BATCH_ITEMS`] items or [`RESTORE_BATCH_BYTES`] of
//! serialized item text (a batch closes at the first item that takes its
//! `item_data` text past the bound, so the batch term of a restore's memory
//! is bounded in bytes, not only in items). Before each batch's writes it
//! reads the ExtendDB host's clock for the batch's write timestamp (the
//! stamp, below) and then runs a lightweight transaction on the record (`confirm_restore_claim`:
//! the heartbeat refreshed `IF` the record is RUNNING under the claim's
//! generation) and ends the file as a lost claim when it is not applied, so
//! the check is linearized with a takeover's own transaction rather than a
//! quorum read beside it. The batch's writes into the account keyspace
//! carry no condition of their own (a Cassandra write cannot be conditioned
//! on another partition); the fence is the timestamp: every write of the
//! batch lands `USING TIMESTAMP` the reading taken before the check, so a
//! writer that stalls between the check and its writes and lands them after
//! a takeover lands them below every write and delete the successor made
//! (the successor's batches carry readings taken after the takeover, its
//! `finish_restore` and the deletes through the ACTIVE table are later
//! still), and Cassandra keeps the later cell or tombstone; the stalled
//! batch changes nothing the successor wrote or removed. The stamp is read
//! from the ExtendDB host's clock (`data::version::now_micros`, the clock
//! the item versions already use), while the writes it must lose to are
//! lightweight transactions and the deletes through the ACTIVE table,
//! whose write times the Cassandra coordinator sets from its own clock, so
//! the rule needs the ExtendDB hosts' clocks and the Cassandra nodes' clocks
//! to agree, as the version scheme already needs the hosts' to. After
//! each batch it records the file's progress
//! with a lightweight transaction conditioned on the restore generation and
//! on the record being RUNNING; `[applied] = false` there is the same lost
//! claim, and the load stops without touching the target again; an unknown
//! outcome of that transaction is decided by a LOCAL_SERIAL re-read of the
//! record (applied when it carries the values written, one retry when it
//! carries the earlier ones, the lost claim when it is not RUNNING under the
//! generation; `write_restore_progress`). The writes are idempotent upserts,
//! so a re-applied batch after a resume rewrites the same base and index rows
//! at a later timestamp (the base row's `version` is the timestamp, so a
//! later ordinary write's version is above it and its index rows resolve in
//! order, ADR-0027), and a key the backup
//! holds twice collapses into one row: a shortfall `count_restore_target`
//! finds against the manifest's total is the duplicate (the engine's count
//! check, 5.3 step 6). Progress is recorded after the batch's writes, so a
//! crash inside the first batch leaves items with no progress; the engine
//! resumes such a record with the first file re-applied (`reapplying`) when
//! the target holds no more than the backup's total, and fails it
//! `target_not_empty` above the total. An empty file records its progress
//! too (`items_applied = start_at`), so a resume does not re-read it.
//!
//! `finish_restore` is [`CassandraEngine::activate_restore_target_row`]'s
//! lightweight transaction (DONE, the table and its indexes ACTIVE, the
//! counts, conditioned on CREATING, RUNNING and the generation). The index
//! rows are created ACTIVE with the table, so Cassandra never returns
//! [`Finished::Pending`].

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::backup_catalog::{
    JobClaim, JobStatus, RESTORE_CASSANDRA_BEFORE_PROGRESS, RestoreTargetKey, WriteOutcome,
    restore_pause_point,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::restore_engine::{BulkLoadFile, Finished, RestoreEngine};
pub(crate) use extenddb_storage::restore_engine::{RESTORE_BATCH_BYTES, RESTORE_BATCH_ITEMS};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use crate::CassandraEngine;
use crate::backup_engine::RESTORE_WRITE_CONCURRENCY;
use crate::data::ddl::{data_table_name, index_table_name};

/// The error of a `bulk_load` whose progress write found the record no longer
/// this claim's: the reconciler took it over while the file was loading, and
/// whoever holds the record now owns the target. Nothing further is touched.
fn claim_lost(table_name: &str) -> StorageError {
    StorageError::Internal(format!(
        "restore of table {table_name}: the claim on the target was lost while a data file \
         loaded (the record is not RUNNING under this generation); the record's owner \
         finishes it"
    ))
}

impl CassandraEngine {
    /// The key shape of a CREATING restore target, read from its `tables`
    /// row by hand (the put path refuses a CREATING table through
    /// `fetch_table_key_info`, so the restore builds the info itself, as the
    /// legacy copy does). `TableNotFound` when `key` is not a CREATING
    /// restore target with this table id, as the other backends answer. The
    /// put path reads the target's indexes from the catalog by `table_id`,
    /// so the routing fields are left empty.
    async fn restore_target_key_info(
        &self,
        key: &RestoreTargetKey,
    ) -> Result<TableKeyInfo, StorageError> {
        let query = format!(
            "SELECT table_id, table_status, restore_source_backup_arn, key_schema, \
             attribute_definitions FROM {}.tables WHERE account_id = ? AND table_name = ?",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows_quorum::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(key.account_id.as_str(), key.table_name.as_str()),
            "restore target key info",
        )
        .await?;
        let row = rows.first().filter(|row| {
            let id: Option<String> = row.get_by_name("table_id").ok().flatten();
            let status: Option<String> = row.get_by_name("table_status").ok().flatten();
            let source: Option<String> =
                row.get_by_name("restore_source_backup_arn").ok().flatten();
            id.as_deref() == Some(&key.table_id)
                && status.as_deref() == Some("CREATING")
                && source.is_some()
        });
        let row = row.ok_or_else(|| {
            StorageError::TableNotFound(format!(
                "{} is not a CREATING restore target with table id {}",
                key.table_name, key.table_id
            ))
        })?;
        let ks_text: String =
            crate::cassandra_util::get_column(row, "key_schema", "restore target key info")?;
        let ad_text: String = crate::cassandra_util::get_column(
            row,
            "attribute_definitions",
            "restore target key info",
        )?;
        let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
            serde_json::from_str(&ks_text)
                .map_err(|e| StorageError::Internal(format!("key schema: {e}")))?;
        let attribute_definitions = serde_json::from_str(&ad_text)
            .map_err(|e| StorageError::Internal(format!("attribute definitions: {e}")))?;
        Ok(TableKeyInfo {
            table_name: key.table_name.clone(),
            account_id: key.account_id.clone(),
            table_id: key.table_id.clone(),
            base_key_schema: key_schema.clone(),
            key_schema,
            attribute_definitions,
            has_lsi: false,
            global_secondary_indexes: Vec::new(),
            local_secondary_indexes: Vec::new(),
            stream_specification: None,
            vector_indexes: Vec::new(),
        })
    }

    /// The claim's liveness before each batch's writes: a lightweight
    /// transaction on the record (`restore_heartbeat_at` refreshed, `IF` the
    /// table id, the generation and RUNNING), linearized on the target's
    /// partition with the takeover's own transaction, so the check and a
    /// `claim_restore_dead` order wholly one before the other where a
    /// quorum read could run beside one. `Err` (a lost claim) when the
    /// transaction is not applied: the reconciler took the record over, and
    /// whoever holds it owns the target, so nothing further is written. An
    /// unknown outcome is decided by a LOCAL_SERIAL re-read of the record.
    /// The writes themselves carry no condition on the record: a batch landed
    /// after a takeover is fenced by its write timestamp, read before this
    /// check (the module doc).
    async fn confirm_restore_claim(
        &self,
        key: &RestoreTargetKey,
        generation: u64,
    ) -> Result<(), StorageError> {
        let query = format!(
            "UPDATE {}.tables SET restore_heartbeat_at = toTimestamp(now()) \
             WHERE account_id = ? AND table_name = ? \
             IF table_id = ? AND restore_owner_generation = ? AND restore_status = 'RUNNING'",
            self.catalog_keyspace()
        );
        let outcome = crate::backup_catalog::conditional_write(
            &self.session,
            &query,
            cdrs_tokio::query::QueryValues::SimpleValues(vec![
                cdrs_tokio::types::value::Value::from(key.account_id.as_str()),
                cdrs_tokio::types::value::Value::from(key.table_name.as_str()),
                cdrs_tokio::types::value::Value::from(key.table_id.as_str()),
                cdrs_tokio::types::value::Value::from(crate::backup_catalog::generation_column(
                    generation,
                )),
            ]),
        )
        .await?;
        match outcome {
            WriteOutcome::Matched => return Ok(()),
            WriteOutcome::NotMatched => return Err(claim_lost(&key.table_name)),
            WriteOutcome::Unknown => {}
        }
        let query = format!(
            "SELECT table_id, restore_status, restore_owner_generation FROM {}.tables \
             WHERE account_id = ? AND table_name = ?",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(key.account_id.as_str(), key.table_name.as_str()),
            "restore claim",
        )
        .await?;
        let live = rows.first().is_some_and(|row| {
            let table_id: Option<String> = row.get_by_name("table_id").ok().flatten();
            let status: Option<String> = row.get_by_name("restore_status").ok().flatten();
            let owner: Option<i64> = row.get_by_name("restore_owner_generation").ok().flatten();
            table_id.as_deref() == Some(key.table_id.as_str())
                && status.as_deref() == Some(JobStatus::Running.as_str())
                && owner == Some(crate::backup_catalog::generation_column(generation))
        });
        if live {
            Ok(())
        } else {
            Err(claim_lost(&key.table_name))
        }
    }

    /// Write one batch of items into the target, each item's base and index
    /// rows in its own logged batch at one write timestamp,
    /// `RESTORE_WRITE_CONCURRENCY` in flight, after confirming the claim.
    /// The timestamp is read before the claim check (the module doc's
    /// fence). Drains `batch`.
    async fn flush_restore_batch(
        &self,
        key: &RestoreTargetKey,
        generation: u64,
        key_info: &TableKeyInfo,
        indexes: &[crate::data::index::IndexMeta],
        batch: &mut Vec<Item>,
    ) -> Result<u64, StorageError> {
        let stamp = crate::data::version::now_micros();
        self.confirm_restore_claim(key, generation).await?;
        crate::test_hooks::after_restore_claim_check(&key.table_id).await;
        let count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        let mut writes = futures::stream::iter(batch.drain(..))
            .map(|item| async move {
                self.put_restored_item_at(key_info, indexes, &item, stamp)
                    .await
            })
            .buffer_unordered(RESTORE_WRITE_CONCURRENCY);
        while let Some(result) = writes.next().await {
            result?;
        }
        Ok(count)
    }

    /// The progress lightweight transaction alone: `restore_file_index` and
    /// `restore_items_applied`, conditioned on the table id, the restore
    /// generation and the record being RUNNING. A test may make it answer
    /// `Unknown`, with or without the transaction run.
    async fn progress_write(
        &self,
        key: &RestoreTargetKey,
        generation: u64,
        file_index: u32,
        items_applied: u64,
    ) -> Result<WriteOutcome, StorageError> {
        let fault = crate::test_hooks::progress_write_fault(&key.account_id);
        if fault == Some(crate::test_hooks::ProgressWriteFault::Lost) {
            return Ok(WriteOutcome::Unknown);
        }
        let query = format!(
            "UPDATE {}.tables SET restore_file_index = ?, restore_items_applied = ? \
             WHERE account_id = ? AND table_name = ? \
             IF table_id = ? AND restore_owner_generation = ? AND restore_status = 'RUNNING'",
            self.catalog_keyspace()
        );
        let outcome = crate::backup_catalog::conditional_write(
            &self.session,
            &query,
            cdrs_tokio::query::QueryValues::SimpleValues(vec![
                cdrs_tokio::types::value::Value::from(
                    i32::try_from(file_index).unwrap_or(i32::MAX),
                ),
                cdrs_tokio::types::value::Value::from(
                    i64::try_from(items_applied).unwrap_or(i64::MAX),
                ),
                cdrs_tokio::types::value::Value::from(key.account_id.as_str()),
                cdrs_tokio::types::value::Value::from(key.table_name.as_str()),
                cdrs_tokio::types::value::Value::from(key.table_id.as_str()),
                cdrs_tokio::types::value::Value::from(crate::backup_catalog::generation_column(
                    generation,
                )),
            ]),
        )
        .await?;
        if fault == Some(crate::test_hooks::ProgressWriteFault::Landed) {
            return Ok(WriteOutcome::Unknown);
        }
        Ok(outcome)
    }

    /// What the record says after a progress write whose outcome is unknown,
    /// read at LOCAL_SERIAL so a transaction that did commit is seen: RUNNING
    /// under `generation` with this table id and carrying the values just
    /// written is `Applied`; RUNNING under `generation` with other values
    /// (the earlier progress: nothing but this job writes the record's
    /// progress while it owns it) is `Pending`; anything else is `Lost`.
    async fn progress_write_state(
        &self,
        key: &RestoreTargetKey,
        generation: u64,
        file_index: u32,
        items_applied: u64,
    ) -> Result<ProgressWriteState, StorageError> {
        let query = format!(
            "SELECT table_id, restore_status, restore_owner_generation, restore_file_index, \
             restore_items_applied FROM {}.tables WHERE account_id = ? AND table_name = ?",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(key.account_id.as_str(), key.table_name.as_str()),
            "restore progress",
        )
        .await?;
        let Some(row) = rows.first() else {
            return Ok(ProgressWriteState::Lost);
        };
        let table_id: Option<String> = row.get_by_name("table_id").ok().flatten();
        let status: Option<String> = row.get_by_name("restore_status").ok().flatten();
        let owner: Option<i64> = row.get_by_name("restore_owner_generation").ok().flatten();
        let owned = table_id.as_deref() == Some(key.table_id.as_str())
            && status.as_deref() == Some(JobStatus::Running.as_str())
            && owner == Some(crate::backup_catalog::generation_column(generation));
        if !owned {
            return Ok(ProgressWriteState::Lost);
        }
        let recorded_file: Option<i32> = row.get_by_name("restore_file_index").ok().flatten();
        let recorded_items: Option<i64> = row.get_by_name("restore_items_applied").ok().flatten();
        let written = (
            Some(i32::try_from(file_index).unwrap_or(i32::MAX)),
            Some(i64::try_from(items_applied).unwrap_or(i64::MAX)),
        );
        if (recorded_file, recorded_items) == written {
            Ok(ProgressWriteState::Applied)
        } else {
            Ok(ProgressWriteState::Pending)
        }
    }

    /// Record the file's progress (`restore_file_index`, the running total
    /// `restore_items_applied`) with a lightweight transaction conditioned on
    /// the restore generation and on the record being RUNNING. `[applied] =
    /// false` is a lost claim. An unknown outcome (a write timeout or failure,
    /// a lost connection: the Paxos round may still have committed) is
    /// decided by re-reading the record at LOCAL_SERIAL: RUNNING under this
    /// generation with the values just written is applied and the file goes
    /// on; RUNNING under this generation with the earlier values is one
    /// retry of the transaction, whose own unknown outcome is re-read the
    /// same way, and a second unknown outcome that the record does not carry
    /// fails the file (the error names it; the engine then fails the restore
    /// and removes the target); anything else is the lost claim.
    async fn write_restore_progress(
        &self,
        key: &RestoreTargetKey,
        generation: u64,
        file_index: u32,
        items_applied: u64,
    ) -> Result<(), StorageError> {
        restore_pause_point(RESTORE_CASSANDRA_BEFORE_PROGRESS).await;
        let mut retried = false;
        loop {
            match self
                .progress_write(key, generation, file_index, items_applied)
                .await?
            {
                WriteOutcome::Matched => return Ok(()),
                WriteOutcome::NotMatched => return Err(claim_lost(&key.table_name)),
                WriteOutcome::Unknown => {}
            }
            match self
                .progress_write_state(key, generation, file_index, items_applied)
                .await?
            {
                ProgressWriteState::Applied => return Ok(()),
                ProgressWriteState::Lost => return Err(claim_lost(&key.table_name)),
                ProgressWriteState::Pending if !retried => {
                    tracing::warn!(
                        "restore of table {}: the progress write for data file {file_index} \
                         had no known outcome and the record does not carry it; written once \
                         more",
                        key.table_name
                    );
                    retried = true;
                }
                ProgressWriteState::Pending => {
                    return Err(StorageError::Internal(format!(
                        "restore of table {}: the progress write for data file {file_index} \
                         had no known outcome twice and the record still carries the earlier \
                         progress; the restore fails and its target is removed",
                        key.table_name
                    )));
                }
            }
        }
    }
}

/// What the record says about a progress write whose outcome was unknown.
#[derive(Debug, PartialEq, Eq)]
enum ProgressWriteState {
    /// RUNNING under the claim's generation, the written values on it.
    Applied,
    /// RUNNING under the claim's generation, the earlier values on it.
    Pending,
    /// Not RUNNING under the claim's generation, or gone.
    Lost,
}

impl RestoreEngine for CassandraEngine {
    fn bulk_load<'a>(
        &'a self,
        target: &'a RestoreTargetKey,
        claim: &'a dyn JobClaim,
        file: BulkLoadFile,
        items: BoxStream<'a, Result<Item, StorageError>>,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            let generation = claim.generation();
            let key_info = self.restore_target_key_info(target).await?;
            // The target's indexes, once per file (a CREATING target's do not
            // change), every GSI synchronous as the put path's
            // `IndexWrites::AllSynchronous` makes them.
            let mut indexes = crate::data::index::fetch_indexes_for_table(
                &target.table_id,
                &self.session,
                &self.catalog_keyspace(),
            )
            .await?;
            for index in &mut indexes {
                index.propagation_delay_ms = Some(0);
            }
            let batch_size = crate::test_hooks::restore_batch_size(&target.account_id).max(1);
            let mut items = items;
            let mut batch: Vec<Item> = Vec::with_capacity(batch_size.min(1024));
            let mut batch_bytes = 0usize;
            let mut applied: u64 = 0;
            let mut wrote_progress = false;
            while let Some(item) = items.try_next().await? {
                // The item's `item_data` text, the unit the byte bound is
                // measured in (the put path serializes it again for the row).
                let text_len = serde_json::to_string(&item)
                    .map_err(|e| StorageError::Internal(format!("item: {e}")))?
                    .len();
                batch_bytes = batch_bytes.saturating_add(text_len);
                batch.push(item);
                if batch.len() >= batch_size || batch_bytes >= RESTORE_BATCH_BYTES {
                    applied += self
                        .flush_restore_batch(target, generation, &key_info, &indexes, &mut batch)
                        .await?;
                    batch_bytes = 0;
                    self.write_restore_progress(
                        target,
                        generation,
                        file.file_index,
                        file.start_at.saturating_add(applied),
                    )
                    .await?;
                    wrote_progress = true;
                }
            }
            if !batch.is_empty() {
                applied += self
                    .flush_restore_batch(target, generation, &key_info, &indexes, &mut batch)
                    .await?;
                self.write_restore_progress(
                    target,
                    generation,
                    file.file_index,
                    file.start_at.saturating_add(applied),
                )
                .await?;
                wrote_progress = true;
            }
            // An empty file still records that it is complete, so a resume
            // does not re-read it.
            if !wrote_progress {
                self.write_restore_progress(target, generation, file.file_index, file.start_at)
                    .await?;
            }
            Ok(applied)
        })
    }

    fn finish_restore<'a>(
        &'a self,
        target: &'a RestoreTargetKey,
        claim: &'a dyn JobClaim,
        item_count: u64,
        size_bytes: u64,
    ) -> BoxFuture<'a, Result<Finished, StorageError>> {
        Box::pin(async move {
            let activated = self
                .activate_restore_target_row(
                    &target.account_id,
                    &target.table_name,
                    &target.table_id,
                    claim.generation(),
                    i64::try_from(item_count).unwrap_or(i64::MAX),
                    i64::try_from(size_bytes).unwrap_or(i64::MAX),
                )
                .await?;
            if activated {
                Ok(Finished::Active)
            } else {
                Ok(Finished::NotMatched(format!(
                    "the restore record of {} is not RUNNING under generation {}, or the table \
                     is not a CREATING restore target",
                    target.table_name,
                    claim.generation()
                )))
            }
        })
    }

    fn count_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            // A full COUNT(*) over the target's data table: a single scan of
            // a CREATING table, run once per restore (the cost is stated in
            // the limits doc). The id names the data table; the account
            // keyspace scopes it to the target's account, and the catalog is
            // read first so a key that is not a CREATING restore target with
            // this id is `TableNotFound`, as on the other backends.
            self.restore_target_key_info(key).await?;
            let account_keyspace = self.account_keyspace(&key.account_id);
            let data_table = data_table_name(&key.table_id);
            let query = format!("SELECT COUNT(*) AS c FROM {account_keyspace}.{data_table}");
            let rows = crate::cassandra_util::query_rows_quorum::<StorageError>(
                &self.session,
                &query,
                cdrs_tokio::query_values!(),
                "count restore target",
            )
            .await?;
            let count: i64 = rows
                .first()
                .map(|row| crate::cassandra_util::get_column(row, "c", "count restore target"))
                .transpose()?
                .unwrap_or(0);
            Ok(u64::try_from(count).unwrap_or(0))
        })
    }

    fn restore_target_ready<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            // The catalog first, so a bare id reads no other account's table;
            // then the data table's row in `system_schema.tables` and one per
            // GSI and LSI the catalog names (the registration LWT lands
            // before the data DDL, which creates the data table and then
            // each index table in its own statement, so a crash between them
            // leaves a target missing an index table). Unquoted CQL
            // identifiers are stored lowercased.
            self.restore_target_key_info(key).await?;
            let account_keyspace = self.account_keyspace(&key.account_id).to_lowercase();
            let indexes = crate::data::index::fetch_indexes_for_table(
                &key.table_id,
                &self.session,
                &self.catalog_keyspace(),
            )
            .await?;
            let mut tables = vec![data_table_name(&key.table_id).to_lowercase()];
            tables.extend(
                indexes
                    .iter()
                    .map(|index| index_table_name(&index.index_id).to_lowercase()),
            );
            for table in tables {
                let rows = crate::cassandra_util::query_rows::<StorageError>(
                    &self.session,
                    "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND \
                     table_name = ?",
                    cdrs_tokio::query_values!(account_keyspace.clone(), table),
                    "restore target data table",
                )
                .await?;
                if rows.is_empty() {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }
}
