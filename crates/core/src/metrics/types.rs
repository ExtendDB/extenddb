// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Metric types and enumerations.

use serde::{Deserialize, Serialize};

/// `DynamoDB` `CloudWatch` metric names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MetricName {
    ConsumedReadCapacityUnits,
    ConsumedWriteCapacityUnits,
    SuccessfulRequestLatency,
    SystemErrors,
    UserErrors,
    /// Throttled request count. Recorded when token bucket rejects a request.
    ThrottledRequests,
    /// Read throttle events. Recorded when a read operation is throttled.
    ReadThrottleEvents,
    /// Write throttle events. Recorded when a write operation is throttled.
    WriteThrottleEvents,
    ConditionalCheckFailedRequests,
    TransactionConflict,
    ReturnedItemCount,
    ReturnedBytes,
    TimeToLiveDeletedItemCount,
    /// Seconds between TTL expiry and actual deletion (staleness).
    TtlDeletionStaleness,
    /// Queue registrations the periodic TTL audit found missing and recreated.
    /// Nonzero means some loss path fired; sustained nonzero means one is
    /// firing repeatedly and needs investigation.
    TtlAuditRepairedEntryCount,
    TtlSweepRowErrorCount,
    /// Unresolved TTL destroy markers observed by the repair worker. Sustained
    /// growth means destroys are repeatedly ending ambiguously (Cassandra
    /// health) or a repair path is stuck; the ADR requires operators to
    /// monitor this.
    TtlRepairMarkerCount,
    /// P120c: HTTP request count (dimensions: operation).
    RequestCount,
    /// P120c: Storage query count (dimensions: source, category).
    StorageQueryCount,
    /// P120c: Storage query latency in microseconds (dimensions: source, category).
    StorageQueryLatency,
    /// P120d: Current active connections in the pool (gauge).
    PoolActiveConnections,
    /// P120d: Current idle connections in the pool (gauge).
    PoolIdleConnections,
    /// P120d: Pool acquire latency in microseconds.
    PoolAcquireLatency,
    /// P120e: Unix timestamp of last successful worker cycle (gauge).
    WorkerLastSuccess,
    /// P120e: Worker cycle latency in microseconds.
    WorkerCycleLatency,
    /// P120e: Worker error count.
    WorkerErrorCount,
    /// Store-backed backup jobs ended, one per job; the operation dimension
    /// carries the outcome ([`BackupJobOutcome`]: `succeeded`, `failed` or
    /// `cancelled`). `/metrics` emits it under this name as JSON, as it does
    /// every metric (the endpoint is not Prometheus exposition format); a
    /// converter would give it a name like `backup_jobs_total{outcome}`, and
    /// nothing in the server renders that.
    BackupJobs,
    /// Duration of a store-backed backup job in microseconds, from the
    /// snapshot's open to the end of its last catalog or store call (the
    /// AVAILABLE write, or the FAILED write and the prefix's deletion);
    /// recorded for every outcome, with the outcome as the operation
    /// dimension.
    BackupJobDuration,
    /// Bytes a backup job that ended AVAILABLE wrote to the store: every
    /// data file and document as the store reported it stored; a failed
    /// job's uploaded and then deleted bytes are not counted.
    BackupBytesUploaded,
    /// Seconds between a running backup job's snapshot instant and now
    /// (gauge), recorded at each confirmed heartbeat.
    BackupSnapshotAge,
    /// Store operations a backup or restore job or the backup reconciler
    /// saw fail; the operation dimension names the store call (`put` by a
    /// backup job's uploads, `delete_prefix` by its failure path and the
    /// reconciler's deleting duties, `get` by a restore's reads of the
    /// manifest and its sidecar, in the request and in a resume, by a
    /// restore job's downloads and by the reconciler's manifest probe, and
    /// `list` by the reconciler's manifest probe and deleting duties).
    BackupStoreErrors,
    /// Store-backed restore jobs ended, one per job; the operation dimension
    /// carries the outcome ([`BackupJobOutcome`]: `succeeded` for a DONE
    /// record, `failed` for a FAILED one or a job that stopped without a
    /// status write of its own, `cancelled` for one the shutdown stopped,
    /// whose record stays RUNNING for a resume). Named as `BackupJobs` is
    /// (the `/metrics` name; a converter's would be
    /// `restore_jobs_total{outcome}`).
    RestoreJobs,
    /// Duration of a store-backed restore job in microseconds, from its
    /// registration to the end of its last catalog or store call, with the
    /// outcome as the operation dimension.
    RestoreJobDuration,
    /// Bytes of each data-file download attempt of a restore job as they
    /// arrived, verified or not, so an attempt the job retried counts its
    /// bytes too; the manifest and sidecar reads, and a filesystem store's
    /// second read of each file for the load, are not counted.
    RestoreBytesDownloaded,
}

/// How a store-backed backup or restore job ended, the operation dimension
/// of [`MetricName::BackupJobs`], [`MetricName::BackupJobDuration`],
/// [`MetricName::RestoreJobs`] and [`MetricName::RestoreJobDuration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackupJobOutcome {
    /// The manifest was written and the row is AVAILABLE.
    Succeeded,
    /// The row is FAILED, or the job stopped without a status write of its
    /// own (its claim lost).
    Failed,
    /// The server's shutdown stopped the job; a backup row is FAILED with
    /// reason `shutdown`, a restore record stays RUNNING for a resume.
    Cancelled,
}

impl std::fmt::Display for BackupJobOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        })
    }
}

impl std::fmt::Display for MetricName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConsumedReadCapacityUnits => f.write_str("ConsumedReadCapacityUnits"),
            Self::ConsumedWriteCapacityUnits => f.write_str("ConsumedWriteCapacityUnits"),
            Self::SuccessfulRequestLatency => f.write_str("SuccessfulRequestLatency"),
            Self::SystemErrors => f.write_str("SystemErrors"),
            Self::UserErrors => f.write_str("UserErrors"),
            Self::ThrottledRequests => f.write_str("ThrottledRequests"),
            Self::ReadThrottleEvents => f.write_str("ReadThrottleEvents"),
            Self::WriteThrottleEvents => f.write_str("WriteThrottleEvents"),
            Self::ConditionalCheckFailedRequests => f.write_str("ConditionalCheckFailedRequests"),
            Self::TransactionConflict => f.write_str("TransactionConflict"),
            Self::ReturnedItemCount => f.write_str("ReturnedItemCount"),
            Self::ReturnedBytes => f.write_str("ReturnedBytes"),
            Self::TimeToLiveDeletedItemCount => f.write_str("TimeToLiveDeletedItemCount"),
            Self::TtlDeletionStaleness => f.write_str("TtlDeletionStaleness"),
            Self::TtlAuditRepairedEntryCount => f.write_str("TtlAuditRepairedEntryCount"),
            Self::TtlSweepRowErrorCount => f.write_str("TtlSweepRowErrorCount"),
            Self::TtlRepairMarkerCount => f.write_str("TtlRepairMarkerCount"),
            Self::RequestCount => f.write_str("RequestCount"),
            Self::StorageQueryCount => f.write_str("StorageQueryCount"),
            Self::StorageQueryLatency => f.write_str("StorageQueryLatency"),
            Self::PoolActiveConnections => f.write_str("PoolActiveConnections"),
            Self::PoolIdleConnections => f.write_str("PoolIdleConnections"),
            Self::PoolAcquireLatency => f.write_str("PoolAcquireLatency"),
            Self::WorkerLastSuccess => f.write_str("WorkerLastSuccess"),
            Self::WorkerCycleLatency => f.write_str("WorkerCycleLatency"),
            Self::WorkerErrorCount => f.write_str("WorkerErrorCount"),
            Self::BackupJobs => f.write_str("BackupJobs"),
            Self::BackupJobDuration => f.write_str("BackupJobDuration"),
            Self::BackupBytesUploaded => f.write_str("BackupBytesUploaded"),
            Self::BackupSnapshotAge => f.write_str("BackupSnapshotAge"),
            Self::BackupStoreErrors => f.write_str("BackupStoreErrors"),
            Self::RestoreJobs => f.write_str("RestoreJobs"),
            Self::RestoreJobDuration => f.write_str("RestoreJobDuration"),
            Self::RestoreBytesDownloaded => f.write_str("RestoreBytesDownloaded"),
        }
    }
}

/// Dimension key for metric aggregation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Dimension {
    /// The account the metric belongs to. Absent on a metric of the whole
    /// deployment.
    AccountId(String),
    TableName(String),
    GlobalSecondaryIndexName(String),
    Operation(String),
}

/// Origin of a database query for attribution in observability metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum QuerySource {
    /// User-facing HTTP request handler.
    Request,
    /// Background: log level poller.
    PollLogLevel,
    /// Background: throttling enabled poller.
    PollThrottlingEnabled,
    /// Background: GSI delay poller.
    PollGsiDelay,
    /// Background: TTL sweeper.
    TtlSweeper,
    /// Background: stream record cleanup.
    StreamCleanup,
    /// Background: idempotency token cleanup.
    IdempotencyCleanup,
    /// Background: metrics flush.
    MetricsFlush,
    /// Background: metrics prune.
    MetricsPrune,
    /// Background: capacity warning.
    CapacityWarning,
    /// Background: table size refresh.
    TableSizeRefresh,
    /// Background: control plane transitions.
    ControlPlane,
    /// Background: login attempt cleanup.
    LoginAttemptCleanup,
    /// Background: the backup reconciler.
    BackupReconciler,
    /// A store-backed backup job (the worker metrics are recorded once per
    /// job, at its end).
    BackupJob,
    /// A store-backed restore job (the worker metrics are recorded once per
    /// job, at its end; a job the shutdown stopped records none).
    RestoreJob,
    /// Management CLI commands.
    Management,
}

impl std::fmt::Display for QuerySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request => f.write_str("Request"),
            Self::PollLogLevel => f.write_str("PollLogLevel"),
            Self::PollThrottlingEnabled => f.write_str("PollThrottlingEnabled"),
            Self::PollGsiDelay => f.write_str("PollGsiDelay"),
            Self::TtlSweeper => f.write_str("TtlSweeper"),
            Self::StreamCleanup => f.write_str("StreamCleanup"),
            Self::IdempotencyCleanup => f.write_str("IdempotencyCleanup"),
            Self::MetricsFlush => f.write_str("MetricsFlush"),
            Self::MetricsPrune => f.write_str("MetricsPrune"),
            Self::CapacityWarning => f.write_str("CapacityWarning"),
            Self::TableSizeRefresh => f.write_str("TableSizeRefresh"),
            Self::ControlPlane => f.write_str("ControlPlane"),
            Self::LoginAttemptCleanup => f.write_str("LoginAttemptCleanup"),
            Self::BackupReconciler => f.write_str("BackupReconciler"),
            Self::BackupJob => f.write_str("BackupJob"),
            Self::RestoreJob => f.write_str("RestoreJob"),
            Self::Management => f.write_str("Management"),
        }
    }
}

/// Category of a storage query for metric attribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum QueryCategory {
    /// User data reads/writes (items, indexes).
    Data,
    /// Catalog metadata (table definitions, indexes, streams).
    Catalog,
    /// Authentication and authorization (credentials, policies).
    Auth,
    /// Runtime settings.
    Settings,
    /// Management operations (admin, IAM).
    Management,
}

impl std::fmt::Display for QueryCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Data => f.write_str("Data"),
            Self::Catalog => f.write_str("Catalog"),
            Self::Auth => f.write_str("Auth"),
            Self::Settings => f.write_str("Settings"),
            Self::Management => f.write_str("Management"),
        }
    }
}

/// Time window for metric queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum TimeWindow {
    LastMinute,
    Last5Minutes,
    LastHour,
    LastDay,
    AllTime,
}

/// A point-in-time snapshot of a single metric's statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSnapshot {
    pub metric: MetricName,
    pub dimensions: Vec<Dimension>,
    pub window: TimeWindow,
    pub sum: f64,
    pub count: u64,
    pub min: f64,
    pub max: f64,
    /// Percentile values: p50, p90, p95, p99.
    /// Only populated for latency metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub percentiles: Option<Percentiles>,
}

/// Percentile statistics for latency metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Percentiles {
    /// 50th percentile (median).
    pub p50: f64,
    /// 90th percentile.
    pub p90: f64,
    /// 95th percentile.
    pub p95: f64,
    /// 99th percentile.
    pub p99: f64,
}

/// Query parameters for the metrics endpoint.
///
/// Supports two modes:
/// 1. **Window mode** (backward compatible): `?window=Last5Minutes`
/// 2. **Custom range mode**: `?start=<ISO8601>&end=<ISO8601>&granularity=1m`
///
/// Auto-granularity when not specified: ≤10m→1m, ≤2h→5m, ≤8h→15m, >8h→1h.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MetricsQuery {
    /// Only the metrics of this account. Absent: every account's metrics and
    /// the deployment's own.
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub table_name: Option<String>,
    #[serde(default)]
    pub metric: Option<MetricName>,
    #[serde(default)]
    pub window: Option<TimeWindow>,
    /// ISO 8601 start time for custom range queries.
    #[serde(default)]
    pub start: Option<String>,
    /// ISO 8601 end time for custom range queries.
    #[serde(default)]
    pub end: Option<String>,
    /// Granularity for time-series buckets (e.g., "1m", "5m", "15m", "1h").
    #[serde(default)]
    pub granularity: Option<String>,
}

/// A pre-aggregated metrics bucket ready for DB persistence.
///
/// Represents one minute of aggregated data for a single metric+dimension key.
#[derive(Debug, Clone)]
pub struct FlushBucket {
    /// Minute-aligned wall-clock timestamp.
    pub bucket: std::time::SystemTime,
    /// Which metric this bucket aggregates.
    pub metric: MetricName,
    /// Account dimension (empty string for a metric of the whole deployment).
    pub account_id: String,
    /// Table name dimension (empty string if not table-scoped).
    pub table_name: String,
    /// Index name dimension (empty string if not index-scoped).
    pub index_name: String,
    /// Operation name dimension (empty string if not operation-scoped).
    pub operation: String,
    /// Sum of all data point values in this bucket.
    pub sum: f64,
    /// Number of data points aggregated into this bucket.
    pub count: u64,
    /// Minimum data point value in this bucket.
    pub min: f64,
    /// Maximum data point value in this bucket.
    pub max: f64,
}

/// A time-series bucket in the metrics response.
///
/// Each bucket represents aggregated metrics over a granularity-sized window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsBucket {
    /// ISO 8601 timestamp for the start of this bucket.
    pub timestamp: String,
    /// Metric name.
    pub metric: MetricName,
    /// Dimensions for this bucket.
    pub dimensions: Vec<Dimension>,
    /// Sum of values in this bucket.
    pub sum: f64,
    /// Count of data points in this bucket.
    pub count: u64,
    /// Minimum value in this bucket.
    pub min: f64,
    /// Maximum value in this bucket.
    pub max: f64,
}

/// Full metrics response with aggregate + time-series data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    /// Aggregate metrics (backward compatible with existing API).
    pub metrics: Vec<MetricSnapshot>,
    /// Time-series buckets (new). Empty for in-memory-only queries.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<MetricsBucket>,
    /// Per-operation latency segment breakdowns for the console deep-dive.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<OperationSegments>,
    /// Data source: "memory" or "database".
    pub source: String,
}

/// Per-segment latency breakdown for a single request.
///
/// Segments are exhaustive and non-overlapping — they sum to `total`.
/// All values are in microseconds.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencySegments {
    /// Credential lookup + `SigV4` verification.
    pub auth_us: f64,
    /// IAM policy fetch + evaluation.
    pub authz_us: f64,
    /// Token bucket check + lazy `describe_table`.
    pub throttle_us: f64,
    /// Everything inside engine dispatch.
    pub dispatch_us: f64,
    /// JSON serialization + CRC32 + response construction.
    pub response_us: f64,
    /// Full handler time (= auth + authz + throttle + dispatch + response).
    pub total_us: f64,
}

/// Average latency segments for a single operation type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationSegments {
    /// Operation name (e.g. "`GetItem`", "`PutItem`").
    pub operation: String,
    /// Number of requests averaged.
    pub count: u64,
    /// Average latency per segment.
    pub avg: LatencySegments,
}

/// Parsed granularity in seconds.
///
/// # Errors
///
/// Returns `None` if the granularity string is not recognized.
#[must_use]
pub fn parse_granularity(s: &str) -> Option<u64> {
    match s {
        "1m" => Some(60),
        "5m" => Some(300),
        "15m" => Some(900),
        "1h" => Some(3600),
        _ => None,
    }
}

/// Auto-select granularity based on the time range duration.
///
/// ≤10m→1m, ≤2h→5m, ≤8h→15m, >8h→1h.
#[must_use]
pub fn auto_granularity(range_secs: u64) -> u64 {
    if range_secs <= 600 {
        60
    } else if range_secs <= 7200 {
        300
    } else if range_secs <= 28800 {
        900
    } else {
        3600
    }
}

/// Convert a `TimeWindow` to its duration in seconds.
#[must_use]
pub fn window_duration_secs(window: TimeWindow) -> Option<u64> {
    match window {
        TimeWindow::LastMinute => Some(60),
        TimeWindow::Last5Minutes => Some(300),
        TimeWindow::LastHour => Some(3600),
        TimeWindow::LastDay => Some(86400),
        TimeWindow::AllTime => None,
    }
}
