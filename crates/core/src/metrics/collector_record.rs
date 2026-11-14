// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The named `record_*` methods of `MetricsCollector`, one per metric.
//!
//! Split from `collector.rs` to keep both files under the 500-line limit. A
//! metric of a request or a table takes the account it belongs to; a metric
//! of the whole deployment takes none.

use super::collector::MetricsCollector;
use super::types::MetricName;

impl MetricsCollector {
    /// Record a latency observation in microseconds.
    pub fn record_latency(
        &self,
        account_id: &str,
        table_name: Option<&str>,
        operation: &str,
        latency_us: f64,
    ) {
        self.record(
            MetricName::SuccessfulRequestLatency,
            latency_us,
            Some(account_id),
            table_name,
            None,
            Some(operation),
        );
    }

    /// Record a successful request's consumed read capacity.
    pub fn record_read_capacity(
        &self,
        account_id: &str,
        table_name: &str,
        operation: &str,
        units: f64,
    ) {
        self.record(
            MetricName::ConsumedReadCapacityUnits,
            units,
            Some(account_id),
            Some(table_name),
            None,
            Some(operation),
        );
    }

    /// Record a successful request's consumed write capacity.
    pub fn record_write_capacity(
        &self,
        account_id: &str,
        table_name: &str,
        operation: &str,
        units: f64,
    ) {
        self.record(
            MetricName::ConsumedWriteCapacityUnits,
            units,
            Some(account_id),
            Some(table_name),
            None,
            Some(operation),
        );
    }

    /// Record a user error (4xx).
    pub fn record_user_error(&self, account_id: &str, table_name: Option<&str>, operation: &str) {
        self.record(
            MetricName::UserErrors,
            1.0,
            Some(account_id),
            table_name,
            None,
            Some(operation),
        );
    }

    /// Record a system error (5xx).
    pub fn record_system_error(&self, account_id: &str, table_name: Option<&str>, operation: &str) {
        self.record(
            MetricName::SystemErrors,
            1.0,
            Some(account_id),
            table_name,
            None,
            Some(operation),
        );
    }

    /// Record a conditional check failure.
    pub fn record_conditional_check_failure(
        &self,
        account_id: &str,
        table_name: Option<&str>,
        operation: &str,
    ) {
        self.record(
            MetricName::ConditionalCheckFailedRequests,
            1.0,
            Some(account_id),
            table_name,
            None,
            Some(operation),
        );
    }

    /// Record a transaction conflict.
    pub fn record_transaction_conflict(&self, account_id: &str, operation: &str) {
        self.record(
            MetricName::TransactionConflict,
            1.0,
            Some(account_id),
            None,
            None,
            Some(operation),
        );
    }

    /// Record returned item count from a query/scan.
    #[allow(clippy::cast_precision_loss)] // count values are small enough for f64
    pub fn record_returned_items(
        &self,
        account_id: &str,
        table_name: &str,
        operation: &str,
        count: u64,
    ) {
        self.record(
            MetricName::ReturnedItemCount,
            count as f64,
            Some(account_id),
            Some(table_name),
            None,
            Some(operation),
        );
    }

    /// Record returned bytes from a query/scan.
    #[allow(clippy::cast_precision_loss)] // byte counts within 1 MB page limit
    pub fn record_returned_bytes(
        &self,
        account_id: &str,
        table_name: &str,
        operation: &str,
        bytes: u64,
    ) {
        self.record(
            MetricName::ReturnedBytes,
            bytes as f64,
            Some(account_id),
            Some(table_name),
            None,
            Some(operation),
        );
    }

    /// Record a TTL deletion.
    pub fn record_ttl_deletion(&self, account_id: &str, table_name: &str) {
        self.record(
            MetricName::TimeToLiveDeletedItemCount,
            1.0,
            Some(account_id),
            Some(table_name),
            None,
            None,
        );
    }

    /// Record a queue registration the TTL audit found missing and recreated.
    pub fn record_ttl_audit_repair(&self, account_id: &str, table_name: &str) {
        self.record(
            MetricName::TtlAuditRepairedEntryCount,
            1.0,
            Some(account_id),
            Some(table_name),
            None,
            None,
        );
    }

    /// Record a sweep row that failed and was left for its queue-state retry.
    ///
    /// Isolated failures are normal (contention, restarts); a sustained
    /// nonzero rate on one table means rows are chronically erroring and
    /// retrying without ever expiring, which a log line alone won't surface.
    pub fn record_ttl_sweep_row_error(&self, account_id: &str, table_name: &str) {
        self.record(
            MetricName::TtlSweepRowErrorCount,
            1.0,
            Some(account_id),
            Some(table_name),
            None,
            None,
        );
    }

    /// Record the number of unresolved TTL destroy markers seen in one pass.
    pub fn record_ttl_repair_markers(&self, count: f64) {
        self.record(
            MetricName::TtlRepairMarkerCount,
            count,
            None,
            None,
            None,
            None,
        );
    }

    /// Record TTL deletion staleness (seconds past expiry).
    pub fn record_ttl_staleness(&self, account_id: &str, table_name: &str, staleness_secs: f64) {
        self.record(
            MetricName::TtlDeletionStaleness,
            staleness_secs,
            Some(account_id),
            Some(table_name),
            None,
            None,
        );
    }

    /// P120c: Record an HTTP request (dimensions: operation).
    pub fn record_request_count(&self, account_id: &str, operation: &str) {
        self.record(
            MetricName::RequestCount,
            1.0,
            Some(account_id),
            None,
            None,
            Some(operation),
        );
    }

    /// P120c: Record a storage query execution (dimensions: source, category).
    pub fn record_storage_query(
        &self,
        source: super::types::QuerySource,
        category: super::types::QueryCategory,
        latency_us: f64,
    ) {
        let source_str = source.to_string();
        let category_str = category.to_string();
        // Count
        self.record(
            MetricName::StorageQueryCount,
            1.0,
            None,
            None,
            Some(&source_str),
            Some(&category_str),
        );
        // Latency
        self.record(
            MetricName::StorageQueryLatency,
            latency_us,
            None,
            None,
            Some(&source_str),
            Some(&category_str),
        );
    }

    /// P120d: Record pool connection gauge values.
    pub fn record_pool_state(&self, active: u32, idle: u32) {
        #[allow(clippy::cast_precision_loss)]
        {
            self.record(
                MetricName::PoolActiveConnections,
                f64::from(active),
                None,
                None,
                None,
                None,
            );
            self.record(
                MetricName::PoolIdleConnections,
                f64::from(idle),
                None,
                None,
                None,
                None,
            );
        }
    }

    /// P120d: Record pool acquire latency in microseconds.
    pub fn record_pool_acquire_latency(&self, latency_us: f64) {
        self.record(
            MetricName::PoolAcquireLatency,
            latency_us,
            None,
            None,
            None,
            None,
        );
    }

    /// P120e: Record a worker's last successful cycle timestamp.
    #[allow(clippy::cast_precision_loss)]
    pub fn record_worker_success(&self, source: super::types::QuerySource, latency_us: f64) {
        let worker_str = source.to_string();
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.record(
            MetricName::WorkerLastSuccess,
            now_secs as f64,
            None,
            None,
            None,
            Some(&worker_str),
        );
        self.record(
            MetricName::WorkerCycleLatency,
            latency_us,
            None,
            None,
            None,
            Some(&worker_str),
        );
    }

    /// P120e: Record a worker error.
    pub fn record_worker_error(&self, source: super::types::QuerySource) {
        let worker_str = source.to_string();
        self.record(
            MetricName::WorkerErrorCount,
            1.0,
            None,
            None,
            None,
            Some(&worker_str),
        );
    }

    /// Record the end of a store-backed backup job: one `BackupJobs` under
    /// the outcome and its duration in microseconds under the same
    /// dimension. The job measures the duration from the snapshot's open to
    /// the end of its last catalog or store call: the AVAILABLE write for a
    /// job that succeeded, the FAILED write and the prefix's deletion for
    /// one that failed.
    pub fn record_backup_job(&self, outcome: super::types::BackupJobOutcome, duration_us: f64) {
        let outcome = outcome.to_string();
        self.record(
            MetricName::BackupJobs,
            1.0,
            None,
            None,
            None,
            Some(&outcome),
        );
        self.record(
            MetricName::BackupJobDuration,
            duration_us,
            None,
            None,
            None,
            Some(&outcome),
        );
    }

    /// Record bytes a backup job wrote to the store; the job records them
    /// once it ended AVAILABLE, so a failed job's uploaded and then deleted
    /// bytes are not counted.
    #[allow(clippy::cast_precision_loss)] // byte counts of one backup
    pub fn record_backup_bytes_uploaded(&self, bytes: u64) {
        self.record(
            MetricName::BackupBytesUploaded,
            bytes as f64,
            None,
            None,
            None,
            None,
        );
    }

    /// Record a running backup job's snapshot age in seconds (gauge).
    pub fn record_backup_snapshot_age(&self, age_secs: f64) {
        self.record(
            MetricName::BackupSnapshotAge,
            age_secs,
            None,
            None,
            None,
            None,
        );
    }

    /// Record a failed store call of a backup job, a restore job or the
    /// backup reconciler; `op` names the call (a backup job's `put` and
    /// `delete_prefix`, a restore's `get`, the reconciler's `get`, `list`
    /// and `delete_prefix`).
    pub fn record_backup_store_error(&self, op: &str) {
        self.record(
            MetricName::BackupStoreErrors,
            1.0,
            None,
            None,
            None,
            Some(op),
        );
    }

    /// Record the end of a store-backed restore job: one `RestoreJobs` under
    /// the outcome and its duration in microseconds under the same
    /// dimension, as [`Self::record_backup_job`] records a backup's. The job
    /// measures the duration from its registration to the end of its last
    /// catalog or store call.
    pub fn record_restore_job(&self, outcome: super::types::BackupJobOutcome, duration_us: f64) {
        let outcome = outcome.to_string();
        self.record(
            MetricName::RestoreJobs,
            1.0,
            None,
            None,
            None,
            Some(&outcome),
        );
        self.record(
            MetricName::RestoreJobDuration,
            duration_us,
            None,
            None,
            None,
            Some(&outcome),
        );
    }

    /// Record bytes a restore job read from the store, as each download
    /// arrived.
    #[allow(clippy::cast_precision_loss)] // byte counts of one backup
    pub fn record_restore_bytes_downloaded(&self, bytes: u64) {
        self.record(
            MetricName::RestoreBytesDownloaded,
            bytes as f64,
            None,
            None,
            None,
            None,
        );
    }
}
