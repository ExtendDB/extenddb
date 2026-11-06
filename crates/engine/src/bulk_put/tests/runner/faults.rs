// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Wraps the real storage engine and injects controlled faults on the bulk
/// write path; everything else delegates to the wrapped engine.
///
/// - `fail_put_pk`: the first write, or the first batch, carrying an item
///   with this partition key fails with a transient storage error and writes
///   nothing, then later attempts pass.
/// - `fail_every_batch`: every batch fails with a transient storage error
///   and writes nothing, the way a statement timeout shorter than one batch
///   fails every attempt.
/// - `hold_first_batch`: the first batch signals the first `Notify` and
///   waits for the second before it reaches storage, the way a batch of a
///   process that stalled past its lease is still in flight when another
///   process takes the operation over.
/// - `pend_finish`: the first `finish` signals and pends forever; dropping
///   the runner there models a crash after the final checkpoint and before
///   the terminal transition.
/// - `commit_then_error_row`: the batch carrying the marked row at this
///   `(file_index, row_index)` commits, its items and ledger rows in one
///   transaction, and then answers a storage error, the way a data write
///   can commit and its acknowledgement be lost.
pub(crate) struct FaultingStorage {
    pub(crate) inner: Arc<dyn StorageEngine>,
    pub(crate) fail_put_pk: Option<String>,
    /// When set, the `fail_put_pk` write fails with an internal storage
    /// error carrying this message instead of a transient one.
    pub(crate) fail_put_pk_internal: Option<String>,
    pub(crate) fail_fired: AtomicBool,
    pub(crate) fail_every_batch: bool,
    pub(crate) hold_first_batch: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    pub(crate) hold_fired: AtomicBool,
    pub(crate) commit_then_error_row: Option<(i64, i64)>,
    pub(crate) commit_then_error_fired: AtomicBool,
    pub(crate) pend_finish: Option<Arc<tokio::sync::Notify>>,
    pub(crate) finish_fired: AtomicBool,
    /// When set, the first `checkpoint` signals and pends forever, holding
    /// its runner (and, under the worker, its slot) mid-run while other
    /// operations' checkpoints pass; the worker tests observe overlapping
    /// runs through it.
    pub(crate) pend_checkpoint: Option<Arc<tokio::sync::Notify>>,
    pub(crate) checkpoint_pend_fired: AtomicBool,
    /// When true, the first `checkpoint` fails with a transient storage
    /// error, so the run ends in a runner storage error and the operation
    /// stays non-terminal, leased until its lease expires.
    pub(crate) fail_checkpoint_once: bool,
    pub(crate) checkpoint_fail_fired: AtomicBool,
    /// When set, every `checkpoint` asks the store for this lease instead
    /// of the runner's, so the store's lease can run out while the runner's
    /// own deadline is still far off.
    pub(crate) checkpoint_lease: Option<Duration>,
    /// When set, `get_bulk_operation` rewrites the returned record's status
    /// to this string, modelling a stored status outside the known set.
    pub(crate) doctor_status: Option<String>,
    /// The `n`th `get_bulk_operation` call (1-based) answers a storage
    /// error once; later calls pass through.
    pub(crate) fail_get_on_call: Option<usize>,
    pub(crate) get_calls: std::sync::atomic::AtomicUsize,
    /// When true, the first `delete_bulk_row_outcomes` fails with a
    /// transient storage error, the way a retention pass can lose one
    /// pair's ledger delete.
    pub(crate) fail_ledger_delete_once: bool,
    pub(crate) ledger_delete_fail_fired: AtomicBool,
    /// When set, the first `delete_bulk_row_outcomes` completes, then
    /// signals and pends forever, holding its caller between the ledger
    /// delete and whatever follows; aborting the caller there models a
    /// crash after the delete committed.
    pub(crate) pend_after_ledger_delete: Option<Arc<tokio::sync::Notify>>,
    pub(crate) ledger_delete_pend_fired: AtomicBool,
    /// Completed `purge_operations` calls, so a test can pin that a
    /// retention pass ran to its end.
    pub(crate) purge_calls: Arc<AtomicU64>,
    /// The row count of every `put_items_batch` call, in order, so a test
    /// can pin the runner's flush policy.
    pub(crate) batch_sizes: Arc<std::sync::Mutex<Vec<usize>>>,
    /// The row count of every `bulk_rows_recorded` answer, in order, so a
    /// test can pin how many recorded rows a resumed run holds at once.
    pub(crate) recorded_reads: Arc<std::sync::Mutex<Vec<usize>>>,
}

impl FaultingStorage {
    pub(crate) fn new(inner: &Arc<dyn StorageEngine>) -> Self {
        Self {
            inner: Arc::clone(inner),
            fail_put_pk: None,
            fail_put_pk_internal: None,
            fail_fired: AtomicBool::new(false),
            fail_every_batch: false,
            hold_first_batch: None,
            hold_fired: AtomicBool::new(false),
            commit_then_error_row: None,
            commit_then_error_fired: AtomicBool::new(false),
            pend_finish: None,
            finish_fired: AtomicBool::new(false),
            pend_checkpoint: None,
            checkpoint_pend_fired: AtomicBool::new(false),
            fail_checkpoint_once: false,
            checkpoint_fail_fired: AtomicBool::new(false),
            checkpoint_lease: None,
            doctor_status: None,
            fail_get_on_call: None,
            get_calls: std::sync::atomic::AtomicUsize::new(0),
            fail_ledger_delete_once: false,
            ledger_delete_fail_fired: AtomicBool::new(false),
            pend_after_ledger_delete: None,
            ledger_delete_pend_fired: AtomicBool::new(false),
            purge_calls: Arc::new(AtomicU64::new(0)),
            batch_sizes: Arc::new(std::sync::Mutex::new(Vec::new())),
            recorded_reads: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

impl DataEngine for FaultingStorage {
    fn scan_key_in_segment(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        segment: i64,
        total_segments: i64,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        self.inner
            .scan_key_in_segment(key_info, key, segment, total_segments, index_name)
    }

    fn as_vector_search(&self) -> Option<&dyn extenddb_storage::VectorSearchEngine> {
        self.inner.as_vector_search()
    }

    fn put_item(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
        marker: Option<&BulkRowMarker>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        if let Some(pk) = &self.fail_put_pk
            && item.get("pk") == Some(&AttributeValue::S(pk.clone()))
            && !self.fail_fired.swap(true, Ordering::SeqCst)
        {
            let error = match &self.fail_put_pk_internal {
                Some(message) => StorageError::Internal(message.clone()),
                None => StorageError::Transient("injected write failure".to_owned()),
            };
            return Box::pin(async move { Err(error) });
        }
        self.inner
            .put_item(key_info, item, return_old, condition, maps, stream, marker)
    }

    fn bulk_row_outcome(
        &self,
        account_id: &str,
        operation_id: &str,
        file_index: i64,
        row_index: i64,
    ) -> BoxFuture<'_, Result<Option<RowOutcome>, StorageError>> {
        self.inner
            .bulk_row_outcome(account_id, operation_id, file_index, row_index)
    }

    /// Records the batch's row count, applies the injected batch faults,
    /// and hands the batch to the wrapped engine, which applies it in one
    /// transaction.
    fn put_items_batch(
        &self,
        key_info: &TableKeyInfo,
        rows: Vec<extenddb_storage::BatchRow>,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Vec<extenddb_storage::BatchRowResult>, StorageError>> {
        self.batch_sizes
            .lock()
            .expect("batch size recorder")
            .push(rows.len());
        let fails = self.fail_every_batch
            || self.fail_put_pk.as_ref().is_some_and(|pk| {
                rows.iter()
                    .any(|row| row.item.get("pk") == Some(&AttributeValue::S(pk.clone())))
                    && !self.fail_fired.swap(true, Ordering::SeqCst)
            });
        if fails {
            let error = match &self.fail_put_pk_internal {
                Some(message) if !self.fail_every_batch => StorageError::Internal(message.clone()),
                _ => StorageError::Transient("injected write failure".to_owned()),
            };
            return Box::pin(async move { Err(error) });
        }
        let commit_then_error = self.commit_then_error_row.is_some_and(|(file, row)| {
            rows.iter().any(|r| {
                r.marker
                    .as_ref()
                    .is_some_and(|m| m.file_index == file && m.row_index == row)
            }) && !self.commit_then_error_fired.swap(true, Ordering::SeqCst)
        });
        let hold = self
            .hold_first_batch
            .as_ref()
            .filter(|_| !self.hold_fired.swap(true, Ordering::SeqCst))
            .cloned();
        let key_info = key_info.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            if let Some((entered, release)) = hold {
                entered.notify_one();
                release.notified().await;
            }
            let results = self
                .inner
                .put_items_batch(&key_info, rows, stream.as_ref())
                .await?;
            if commit_then_error {
                return Err(StorageError::Internal(
                    "injected error after a committed batch".to_owned(),
                ));
            }
            Ok(results)
        })
    }

    fn bulk_write_takeover(
        &self,
        account_id: &str,
        operation_id: &str,
        owner: &str,
        cursor: (i64, i64),
    ) -> BoxFuture<'_, Result<u32, StorageError>> {
        self.inner
            .bulk_write_takeover(account_id, operation_id, owner, cursor)
    }

    fn bulk_rows_recorded(
        &self,
        account_id: &str,
        operation_id: &str,
        file_index: i64,
        from_row: i64,
        limit: i64,
    ) -> BoxFuture<'_, Result<Vec<extenddb_storage::RecordedRow>, StorageError>> {
        let reads = Arc::clone(&self.recorded_reads);
        let read =
            self.inner
                .bulk_rows_recorded(account_id, operation_id, file_index, from_row, limit);
        Box::pin(async move {
            let rows = read.await?;
            reads.lock().unwrap().push(rows.len());
            Ok(rows)
        })
    }

    fn delete_bulk_row_outcomes(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        if self.fail_ledger_delete_once
            && !self.ledger_delete_fail_fired.swap(true, Ordering::SeqCst)
        {
            return Box::pin(async {
                Err(StorageError::Transient(
                    "injected ledger delete failure".to_owned(),
                ))
            });
        }
        let pend = self
            .pend_after_ledger_delete
            .as_ref()
            .filter(|_| !self.ledger_delete_pend_fired.swap(true, Ordering::SeqCst))
            .map(Arc::clone);
        let fut = self
            .inner
            .delete_bulk_row_outcomes(account_id, operation_id);
        Box::pin(async move {
            let deleted = fut.await?;
            if let Some(notify) = pend {
                notify.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(deleted)
        })
    }

    fn get_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        self.inner.get_item(key_info, key)
    }

    fn delete_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        self.inner
            .delete_item(key_info, key, return_old, condition, maps, stream)
    }

    fn update_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, ItemPairResult> {
        self.inner.update_item(
            key_info, key, actions, return_old, return_new, condition, maps, stream,
        )
    }

    fn query(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        self.inner.query(
            key_info,
            key_condition,
            maps,
            forward,
            limit,
            exclusive_start_key,
            index_name,
        )
    }

    fn scan(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        self.inner.scan(
            key_info,
            limit,
            exclusive_start_key,
            segment,
            total_segments,
            index_name,
        )
    }

    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        self.inner.transact_get_items(ops)
    }

    fn transact_write_items(
        &self,
        ops: &[TransactWriteOp<'_>],
        idempotency: Option<IdempotencyKey<'_>>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.transact_write_items(ops, idempotency)
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        self.inner
            .cleanup_expired_idempotency_tokens(max_age_seconds)
    }
}
