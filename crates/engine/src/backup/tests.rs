// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Engine backup rules, against a double of the catalog index and the legacy
//! reader that records what the engine asked of it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use extenddb_core::backup_format::BackupArn;
use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeDefinition, BackupDescription, BackupDetails, BackupSummary, BillingMode,
    BillingModeSummary, CreateTableInput, DeleteTableInput, DescribeTableInput, GsiDescription,
    GsiInput, IndexInfo, Item, KeySchemaElement, KeyType, ListTablesInput, ListTablesOutput,
    LsiInput, OnDemandThroughput, Projection, ProjectionType, ProvisionedThroughput,
    ProvisionedThroughputDescription, RestoreSummary, ScalarAttributeType, SourceTableDetails,
    StreamSpecification, StreamViewType, TableDescription, TableKeyInfo, TableStatus,
    TimeToLiveDescription, TimeToLiveStatus, UpdateTableInput,
};
use extenddb_storage::backup_catalog::{
    AbandonedLegacyBackup, BackupKey, BackupRow, BackupStatus, DeleteStart, Heartbeat, JobClaim,
    JobRow, JobStatus, RestoreJobRow, RestoreTargetKey, StaleClock, StatusFields, StoreIdentity,
    StoreKind, TimeBounds, WriteOutcome, new_generation,
};
use extenddb_storage::backup_definition::{BACKUP_DEFINITION_VERSION, BackupTableDefinition};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupCatalog, BackupRestoreSource, BoxedFuture, JobPermit, LegacyBackupReader, MetadataEngine,
    RestoreRequest, SnapshotEngine, TableEngine, TableSnapshot,
};
use futures::TryStreamExt;
use futures::stream::BoxStream;
use serde_json::{Value, json};

use super::*;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const CREATED: f64 = 1_791_976_431.438;

fn arn(account: &str) -> String {
    format!("arn:aws:dynamodb:{REGION}:{account}:table/Music/backup/01791976431438-73d8d5bc")
}

fn source_arn() -> String {
    format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music")
}

// --- double -----------------------------------------------------------------

#[derive(Default)]
struct Fake {
    sources: Mutex<HashMap<String, BackupRestoreSource>>,
    descriptions: Mutex<HashMap<String, BackupDescription>>,
    summaries: Mutex<Vec<BackupSummary>>,
    restore_error: Mutex<Option<StorageError>>,
    /// Returned (once) by `restore_source` instead of the stored source.
    source_error: Mutex<Option<StorageError>>,
    restores: Mutex<Vec<RestoreRequest>>,
    /// The permits the legacy restore and create were handed, held as the
    /// backend would hold them for the job's life.
    permits: Mutex<Vec<JobPermit>>,
    creates: Mutex<Vec<(String, String)>>,
    source_reads: Mutex<usize>,
    listed_for: Mutex<Vec<Option<String>>>,
    /// When set, `restore_source` waits for a permit before answering.
    source_gate: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// The catalog index: rows by ARN, shared with the claims handed out.
    rows: Arc<Mutex<HashMap<String, BackupRow>>>,
    /// `backup_in_use` answers: the CREATING target's name, by backup ARN.
    in_use: Mutex<HashMap<String, String>>,
    /// What `start_backup_delete` answers after the row check, by ARN.
    delete_starts: Mutex<HashMap<String, DeleteStart>>,
    delete_starts_seen: Mutex<Vec<BackupKey>>,
    row_reads: Mutex<Vec<String>>,
    dead_jobs: Mutex<Vec<RestoreJobRow>>,
    failed_jobs: Mutex<Vec<(RestoreTargetKey, String)>>,
    /// A heartbeat that lands on the record between the listing and the
    /// FAILED write: the `heartbeat_at` the record carries when the write
    /// arrives.
    heartbeat_after_listing: Mutex<HashMap<RestoreTargetKey, f64>>,
    removed_targets: Mutex<Vec<RestoreTargetKey>>,
    /// Whether `remove_restore_target` answers `true` (the target was a
    /// CREATING restore target with the key's id).
    target_still_creating: Mutex<bool>,
    /// Whether `fail_restore_job` answers `true` (the record was RUNNING);
    /// separate from `target_still_creating` so the FAILED write matching
    /// while the removal finds the target moved is a reachable state.
    record_still_running: Mutex<bool>,
    abandoned_backups: Mutex<Vec<AbandonedLegacyBackup>>,
    deleted: Mutex<Vec<String>>,
    maintenance_held: Mutex<bool>,
    /// The windows `check_maintenance_claim` was called with: one entry per
    /// reconciler cycle, so a test can see that a cycle ran.
    maintenance_checks: Mutex<Vec<Duration>>,
    /// When set, `check_maintenance_claim` fails with it (every call).
    maintenance_error: Mutex<Option<String>>,
    /// When set, the age of a live maintenance claim's last heartbeat, and
    /// `check_maintenance_claim(stale_after)` answers held while the age is
    /// under `stale_after`, the rule the backends apply; `maintenance_held`
    /// answers otherwise.
    maintenance_beat_age: Mutex<Option<Duration>>,
    /// How far the catalog's clock runs ahead of this process's (negative:
    /// behind), in seconds; `list_backup_rows_older_than` measures ages on
    /// the catalog's clock.
    catalog_clock_offset_secs: Mutex<f64>,
    /// When set, `check_maintenance_claim` takes this long to answer (a
    /// catalog query held behind the pool).
    maintenance_delay: Mutex<Option<Duration>>,
    /// The `TimeBounds` `list_account_backup_rows` received.
    bounds_seen: Mutex<Vec<TimeBounds>>,
    /// The account's tables, by name, as `describe_table` reports them; a
    /// name absent is `TableNotFound`.
    tables: Mutex<HashMap<String, TableDescription>>,
    /// The names `describe_table` was asked for.
    table_reads: Mutex<Vec<String>>,
    /// What `catalog_id` answers; `catalog-1` unless a test changes it.
    catalog_id: Mutex<Option<String>>,
    /// When set, `catalog_id` fails as a catalog that cannot be read.
    catalog_id_error: Mutex<bool>,
    /// Rows `set_backup_deleted` moved to DELETED, by ARN.
    set_deleted: Mutex<Vec<String>>,
    /// Rows `remove_backup_row` removed, by ARN.
    removed_rows: Mutex<Vec<String>>,
    /// Every `remove_backup_row` call, by ARN, refused ones included.
    remove_attempts: Mutex<Vec<String>>,
    /// When set, `list_backup_rows` fails with it.
    list_error: Mutex<Option<String>>,
    /// The stores `newest_available_backup_row` was asked for.
    newest_reads: Mutex<Vec<StoreIdentity>>,
    /// RUNNING restore records with a live heartbeat (a fresh one, say),
    /// which `list_running_restore_jobs` lists and `list_dead_restore_jobs`
    /// does not.
    running_jobs: Mutex<Vec<RestoreJobRow>>,
    /// The table snapshots `open_table_snapshot` serves, by table name; a
    /// name absent is `TableNotFound`.
    snapshots: Mutex<HashMap<String, SnapshotFixture>>,
    /// When set, `open_table_snapshot` fails with it (once).
    snapshot_error: Mutex<Option<StorageError>>,
    /// Snapshots opened, by table name.
    snapshots_opened: Mutex<Vec<String>>,
    /// What `describe_ttl` answers; disabled when unset.
    ttl: Mutex<Option<TimeToLiveDescription>>,
    /// The rows `claim_backup` received, as given.
    claims: Mutex<Vec<BackupRow>>,
    /// The ARNs `claim_dead_backup` hands over (the backend's liveness test
    /// modelled as a set): a CREATING row named here has a dead owner.
    dead: Mutex<std::collections::HashSet<String>>,
    /// The ARNs `claim_dead_backup` took over.
    takeovers: Mutex<Vec<String>>,
    /// Every `set_status` a claim received, in order.
    status_writes: Arc<Mutex<Vec<StatusWrite>>>,
    /// What a claim's `heartbeat` does.
    heartbeat_answer: Arc<Mutex<HeartbeatAnswer>>,
    /// Releases a beat parked under `HeartbeatAnswer::Park`.
    park_release: Arc<tokio::sync::Notify>,
    /// What the fake's claims declare about their beat
    /// (`JobClaim::heartbeat_parks_before_its_bound`): `true` models SQLite,
    /// `false` (the default) the other backends.
    parks_before_bound: Arc<std::sync::atomic::AtomicBool>,
    /// Heartbeats the claims received.
    heartbeats: Arc<std::sync::atomic::AtomicUsize>,
    /// When set, the next `set_status` reports `Unknown` (applied or not as
    /// the flag says) instead of its outcome; taken by that write unless
    /// `unknown_write_sticky` is set (a connection that fails on every
    /// write).
    unknown_write: Arc<Mutex<Option<UnknownWrite>>>,
    unknown_write_sticky: Arc<std::sync::atomic::AtomicBool>,
    /// When set, moved into `unknown_write` once a (non-sticky) unknown
    /// write has been taken: the write sent after an unknown outcome is
    /// itself unknown, in this way.
    unknown_write_then: Arc<Mutex<Option<UnknownWrite>>>,
    /// When set, every `set_status` fails as a claim connection that was
    /// reset.
    status_write_error: Arc<std::sync::atomic::AtomicBool>,
    /// When set, `claim_dead_backup` hands the claim over and then gives
    /// the row a newer generation: another reconciler took the row over in
    /// the gap before this one's write (item 31's two-reconciler race).
    takeover_lost_to_another: std::sync::atomic::AtomicBool,
    /// What `sweep_restore_registrations` answers.
    swept_registrations: Mutex<Vec<String>>,
}

/// A table snapshot the `Fake` serves.
struct SnapshotFixture {
    description: TableDescription,
    key_ordered: bool,
    items: Vec<Result<Item, StorageError>>,
    /// When set, the stream takes one permit per item before yielding it,
    /// so a test can hold the job inside the stream.
    gate: Option<Arc<tokio::sync::Semaphore>>,
}

/// One `set_status` a claim received.
#[derive(Debug, Clone)]
struct StatusWrite {
    arn: String,
    generation: u64,
    from: JobStatus,
    to: JobStatus,
    fields: StatusFields,
}

/// What a `Fake` claim's heartbeat does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum HeartbeatAnswer {
    /// Confirm while the row carries the claim's generation, whatever its
    /// status (the backends' `UPDATE ... WHERE owner_generation = ?` has no
    /// status clause, so a job's beats during its own `delete_prefix`
    /// refresh the FAILED row), refreshing `heartbeat_at`; `Lost` otherwise.
    #[default]
    Confirm,
    /// Confirm whatever the row carries (a connection whose session still
    /// holds its lock while the row was rewritten).
    AlwaysConfirm,
    /// `Lost`, whatever the row.
    Lost,
    /// Never return.
    Stall,
    /// An error.
    Error,
    /// Park outside the bound, as SQLite's beat does behind the engine
    /// write lock, until `Fake::park_release` is notified; then answer as
    /// `heartbeat_answer` reads at that moment.
    Park,
}

/// How the next `set_status` reports `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnknownWrite {
    /// The write landed and the connection failed after it.
    Applied,
    /// The write was lost with the connection.
    Lost,
}

/// A `Fake` claim on one row.
struct FakeClaim {
    rows: Arc<Mutex<HashMap<String, BackupRow>>>,
    arn: String,
    generation: u64,
    status_writes: Arc<Mutex<Vec<StatusWrite>>>,
    heartbeat_answer: Arc<Mutex<HeartbeatAnswer>>,
    park_release: Arc<tokio::sync::Notify>,
    parks_before_bound: Arc<std::sync::atomic::AtomicBool>,
    heartbeats: Arc<std::sync::atomic::AtomicUsize>,
    unknown_write: Arc<Mutex<Option<UnknownWrite>>>,
    unknown_write_sticky: Arc<std::sync::atomic::AtomicBool>,
    unknown_write_then: Arc<Mutex<Option<UnknownWrite>>>,
    status_write_error: Arc<std::sync::atomic::AtomicBool>,
}

impl Fake {
    fn claim_on(&self, arn: &str, generation: u64) -> Box<dyn JobClaim> {
        Box::new(FakeClaim {
            rows: Arc::clone(&self.rows),
            arn: arn.to_owned(),
            generation,
            status_writes: Arc::clone(&self.status_writes),
            heartbeat_answer: Arc::clone(&self.heartbeat_answer),
            park_release: Arc::clone(&self.park_release),
            parks_before_bound: Arc::clone(&self.parks_before_bound),
            heartbeats: Arc::clone(&self.heartbeats),
            unknown_write: Arc::clone(&self.unknown_write),
            unknown_write_sticky: Arc::clone(&self.unknown_write_sticky),
            unknown_write_then: Arc::clone(&self.unknown_write_then),
            status_write_error: Arc::clone(&self.status_write_error),
        })
    }
}

/// `JobStatus` as a backup row carries it.
fn row_status(status: JobStatus) -> Option<BackupStatus> {
    match status {
        JobStatus::Creating => Some(BackupStatus::Creating),
        JobStatus::Available => Some(BackupStatus::Available),
        JobStatus::Failed => Some(BackupStatus::Failed),
        JobStatus::Running | JobStatus::Done => None,
    }
}

/// `BackupStatus` as a job record reports it.
fn job_status(status: BackupStatus) -> JobStatus {
    match status {
        BackupStatus::Creating => JobStatus::Creating,
        BackupStatus::Available => JobStatus::Available,
        BackupStatus::Failed => JobStatus::Failed,
        BackupStatus::Deleting | BackupStatus::Deleted => JobStatus::Done,
    }
}

impl JobClaim for FakeClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxedFuture<'_, Result<Heartbeat, StorageError>> {
        self.heartbeats
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let answer = *self.heartbeat_answer.lock().unwrap();
        let confirmed = match self.rows.lock().unwrap().get_mut(&self.arn) {
            Some(row)
                if answer == HeartbeatAnswer::Confirm
                    && row.owner_generation == self.generation =>
            {
                row.heartbeat_at = seconds_since_epoch();
                true
            }
            _ => false,
        };
        Box::pin(async move {
            match answer {
                HeartbeatAnswer::Confirm if confirmed => Ok(Heartbeat::Confirmed),
                HeartbeatAnswer::AlwaysConfirm => Ok(Heartbeat::Confirmed),
                HeartbeatAnswer::Confirm | HeartbeatAnswer::Lost => Ok(Heartbeat::Lost),
                HeartbeatAnswer::Stall => futures::future::pending().await,
                HeartbeatAnswer::Error => Err(StorageError::Internal(
                    "the claim connection failed".to_owned(),
                )),
                HeartbeatAnswer::Park => futures::future::pending().await,
            }
        })
    }

    /// SQLite's shape: under `Park` the beat waits outside the bound (the
    /// engine write lock's queue) and the bound covers the answer alone.
    fn heartbeat_within(
        &self,
        bound: Duration,
    ) -> BoxedFuture<'_, Option<Result<Heartbeat, StorageError>>> {
        Box::pin(async move {
            if *self.heartbeat_answer.lock().unwrap() == HeartbeatAnswer::Park {
                self.park_release.notified().await;
            }
            tokio::time::timeout(bound, self.heartbeat()).await.ok()
        })
    }

    /// What the fake declares about its beat: `true` models SQLite (the
    /// park is the write lock's queue), `false` the other backends, where a
    /// beat still in flight past its bound is one the process was stopped
    /// in (`Park` then stands for the stop).
    fn heartbeat_parks_before_its_bound(&self) -> bool {
        self.parks_before_bound
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// The backends' write: conditioned on the claim's generation and on
    /// the row being in `from`; `completed` stamps `completed_at`.
    fn set_status<'a>(
        &'a self,
        from: JobStatus,
        to: JobStatus,
        fields: &'a StatusFields,
    ) -> BoxedFuture<'a, Result<WriteOutcome, StorageError>> {
        self.status_writes.lock().unwrap().push(StatusWrite {
            arn: self.arn.clone(),
            generation: self.generation,
            from,
            to,
            fields: fields.clone(),
        });
        if self
            .status_write_error
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Box::pin(async move {
                Err(StorageError::Internal(
                    "claim session: connection reset".to_owned(),
                ))
            });
        }
        let unknown = if self
            .unknown_write_sticky
            .load(std::sync::atomic::Ordering::Acquire)
        {
            *self.unknown_write.lock().unwrap()
        } else {
            let taken = self.unknown_write.lock().unwrap().take();
            if taken.is_some() {
                *self.unknown_write.lock().unwrap() =
                    self.unknown_write_then.lock().unwrap().take();
            }
            taken
        };
        let apply = unknown != Some(UnknownWrite::Lost);
        let mut outcome = WriteOutcome::NotMatched;
        if apply
            && let Some(row) = self.rows.lock().unwrap().get_mut(&self.arn)
            && row.owner_generation == self.generation
            && Some(row.status) == row_status(from)
            && let Some(to_status) = row_status(to)
        {
            row.status = to_status;
            if let Some(size) = fields.size_bytes {
                row.size_bytes = size;
            }
            if let Some(count) = fields.item_count {
                row.item_count = count;
            }
            if let Some(reason) = &fields.failure_reason {
                row.failure_reason = Some(reason.clone());
            }
            if let Some(details) = &fields.source_table_details_json {
                row.source_table_details_json = details.clone();
            }
            if fields.completed {
                row.completed_at = Some(seconds_since_epoch());
            }
            outcome = WriteOutcome::Matched;
        }
        if unknown.is_some() {
            outcome = WriteOutcome::Unknown;
        }
        Box::pin(async move { Ok(outcome) })
    }

    fn read(&self) -> BoxedFuture<'_, Result<JobRow, StorageError>> {
        let found = self.rows.lock().unwrap().get(&self.arn).map(|row| JobRow {
            status: job_status(row.status),
            owner_generation: row.owner_generation,
            heartbeat_at: row.heartbeat_at,
            failure_reason: row.failure_reason.clone(),
        });
        Box::pin(async move {
            found.ok_or_else(|| StorageError::Internal("the row is gone".to_owned()))
        })
    }
}

impl Fake {
    fn with_source(source: BackupRestoreSource) -> Self {
        let fake = Self::default();
        fake.sources
            .lock()
            .unwrap()
            .insert(source.backup_arn.clone(), source);
        fake
    }
    fn restores(&self) -> Vec<RestoreRequest> {
        self.restores.lock().unwrap().clone()
    }
    fn source_reads(&self) -> usize {
        *self.source_reads.lock().unwrap()
    }
}

fn unused<T: Send + 'static>() -> BoxedFuture<'static, Result<T, StorageError>> {
    Box::pin(async { Err(StorageError::Internal("not used by these tests".to_owned())) })
}

/// The runtime the tests run with: four slots, no configured store.
fn runtime() -> BackupRuntime {
    BackupRuntime::new(4)
}

fn store() -> StoreIdentity {
    StoreIdentity {
        store_kind: StoreKind::Filesystem,
        store_root: "/var/lib/extenddb/backups".to_owned(),
        store_id: "store-1".to_owned(),
    }
}

/// An in-memory `BackupStore` for the engine's tests: objects by key, a
/// switch that makes every call fail as a transport error, a switch that
/// makes every call wait forever (a store that has stopped answering), and
/// the keys each operation was called with.
#[derive(Default)]
struct MemStore {
    objects: Mutex<std::collections::BTreeMap<String, Vec<u8>>>,
    /// When set, every operation fails with this text as the cause under a
    /// "dispatch failure" link, the shape the SDK's transport errors have.
    down: Mutex<Option<String>>,
    /// When set, every operation's future never completes; `stalls` counts
    /// the calls that entered the stall. A stalled call returns once the
    /// store is un-stalled, as a parked filesystem call returns when its
    /// mount answers.
    stalled: std::sync::atomic::AtomicBool,
    stalls: std::sync::atomic::AtomicUsize,
    /// When set, every operation takes this long before it answers: a
    /// healthy root whose calls are slow.
    delay: Mutex<Option<Duration>>,
    deletes: Mutex<Vec<String>>,
    puts: Mutex<Vec<String>>,
    /// When set, `put` fails as a transport error after reading its body;
    /// every other call answers.
    fail_puts: std::sync::atomic::AtomicBool,
    /// When set, `put` fails as a transport error 200 ms after reading its
    /// body's first chunk, while the encoder waits for the file's next item
    /// (a connection reset mid-upload).
    fail_puts_after_first_chunk: std::sync::atomic::AtomicBool,
    /// When set, `put` reports one byte more than it stored (a store whose
    /// size accounting differs from the bytes written).
    misreport_put_size: std::sync::atomic::AtomicBool,
    /// When set, `get` of a key under this prefix fails as a transport
    /// error; every other call answers (the identity file's read among
    /// them, so the store stays healthy).
    fail_gets_under: Mutex<Option<String>>,
    /// Called with the key of every `put` that stored its object, so a test
    /// can act at a point of the job (after the sidecar's put, before the
    /// claim re-check).
    on_put: Mutex<Option<OnPut>>,
    /// Bytes `put` stored, by key, in the order the puts ended.
    put_sizes: Mutex<Vec<(String, u64)>>,
    /// When set, the bytes `put_if_absent` leaves under the key right after
    /// it stored its own: a server that ignores `If-None-Match` and a second
    /// writer that won the plain-put race.
    overwrite_after_conditional_put: Mutex<Option<Vec<u8>>>,
    /// When set, `put_if_absent` finds the key taken (`None`) while `get`
    /// and `head` find no object: something that is not an object (a FIFO,
    /// a socket) sits at the path.
    key_taken_by_a_non_object: std::sync::atomic::AtomicBool,
    /// When set, `validate` fails with this text as a store condition (the
    /// probe write failing with an I/O error) while every other call
    /// answers.
    validate_error: Mutex<Option<String>>,
    /// When set, a `delete_prefix` of a prefix (not of a manifest key) is
    /// followed by a put of this key under the prefix: a writer whose put
    /// overlapped the deletion.
    late_put_after_delete: Mutex<Option<String>>,
    /// When set, `delete_prefix` takes this long before it answers (a slow
    /// store's deletion).
    delete_delay: Mutex<Option<Duration>>,
    /// When set, `list` of this prefix fails as a transport error.
    fail_lists_of: Mutex<Option<String>>,
    /// When set, `delete_prefix` of this prefix fails as a transport error.
    fail_deletes_of: Mutex<Option<String>>,
}

/// What `MemStore::on_put` runs after a stored `put`, with the object's key.
type OnPut = Box<dyn Fn(&str) + Send + Sync>;

/// A transport error with the SDK's shape: a top link naming only the class
/// and the cause one link below it.
#[derive(Debug)]
struct DispatchFailure(ConnectError);

#[derive(Debug)]
struct ConnectError(String);

impl std::fmt::Display for DispatchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("dispatch failure")
    }
}

impl std::error::Error for DispatchFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConnectError {}

impl MemStore {
    /// A store that is down fails every call as the S3 store fails one it
    /// cannot send: a transport error whose cause is a link below the top.
    fn check(&self) -> Result<(), extenddb_backup_store::StoreError> {
        match self.down.lock().unwrap().as_ref() {
            Some(text) => Err(extenddb_backup_store::StoreError::Transport {
                source: Box::new(DispatchFailure(ConnectError(text.clone()))),
            }),
            None => Ok(()),
        }
    }
    /// Wait while the store is stalled, and the delay if one is set;
    /// otherwise return at once.
    async fn stall(&self) {
        let delay = *self.delay.lock().unwrap();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        if self.stalled.load(std::sync::atomic::Ordering::Acquire) {
            self.stalls
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            while self.stalled.load(std::sync::atomic::Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    fn set_down(&self, down: bool) {
        *self.down.lock().unwrap() = down.then(|| "connection refused".to_owned());
    }
    fn set_stalled(&self, stalled: bool) {
        self.stalled
            .store(stalled, std::sync::atomic::Ordering::Release);
    }
    fn set_delay(&self, delay: Option<Duration>) {
        *self.delay.lock().unwrap() = delay;
    }
    fn insert(&self, key: &str, body: &[u8]) {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_owned(), body.to_vec());
    }
    fn keys(&self) -> Vec<String> {
        self.objects.lock().unwrap().keys().cloned().collect()
    }
    fn matches(key: &str, prefix: &str) -> bool {
        let prefix = prefix.trim_end_matches('/');
        prefix.is_empty() || key == prefix || key.starts_with(&format!("{prefix}/"))
    }
}

async fn collect_body(
    body: extenddb_backup_store::ByteStream,
) -> Result<Vec<u8>, extenddb_backup_store::StoreError> {
    body.try_fold(Vec::new(), |mut acc, chunk| async move {
        acc.extend_from_slice(&chunk);
        Ok(acc)
    })
    .await
}

impl extenddb_backup_store::BackupStore for MemStore {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: extenddb_backup_store::ByteStream,
    ) -> BoxedFuture<'a, Result<extenddb_backup_store::ObjectMeta, extenddb_backup_store::StoreError>>
    {
        Box::pin(async move {
            self.stall().await;
            self.check()?;
            let mut body = body;
            if self
                .fail_puts_after_first_chunk
                .load(std::sync::atomic::Ordering::Acquire)
            {
                let _first = body.try_next().await?;
                tokio::time::sleep(Duration::from_millis(200)).await;
                return Err(extenddb_backup_store::StoreError::Transport {
                    source: Box::new(DispatchFailure(ConnectError(
                        "connection reset mid-body".to_owned(),
                    ))),
                });
            }
            let bytes = collect_body(body).await?;
            if self.fail_puts.load(std::sync::atomic::Ordering::Acquire) {
                return Err(extenddb_backup_store::StoreError::Transport {
                    source: Box::new(DispatchFailure(ConnectError("put refused".to_owned()))),
                });
            }
            self.puts.lock().unwrap().push(key.to_owned());
            let size = bytes.len() as u64;
            self.put_sizes.lock().unwrap().push((key.to_owned(), size));
            self.insert(key, &bytes);
            if let Some(on_put) = self.on_put.lock().unwrap().as_ref() {
                on_put(key);
            }
            let reported = if self
                .misreport_put_size
                .load(std::sync::atomic::Ordering::Acquire)
            {
                size + 1
            } else {
                size
            };
            Ok(extenddb_backup_store::ObjectMeta {
                key: key.to_owned(),
                size: reported,
                last_modified: None,
                etag: None,
            })
        })
    }

    fn put_if_absent<'a>(
        &'a self,
        key: &'a str,
        body: extenddb_backup_store::ByteStream,
    ) -> BoxedFuture<
        'a,
        Result<Option<extenddb_backup_store::ObjectMeta>, extenddb_backup_store::StoreError>,
    > {
        Box::pin(async move {
            self.stall().await;
            self.check()?;
            let bytes = collect_body(body).await?;
            if self
                .key_taken_by_a_non_object
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Ok(None);
            }
            let mut objects = self.objects.lock().unwrap();
            if objects.contains_key(key) {
                return Ok(None);
            }
            self.puts.lock().unwrap().push(key.to_owned());
            let size = bytes.len() as u64;
            objects.insert(key.to_owned(), bytes);
            if let Some(other) = self.overwrite_after_conditional_put.lock().unwrap().clone() {
                objects.insert(key.to_owned(), other);
            }
            Ok(Some(extenddb_backup_store::ObjectMeta {
                key: key.to_owned(),
                size,
                last_modified: None,
                etag: None,
            }))
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxedFuture<'a, Result<extenddb_backup_store::ByteStream, extenddb_backup_store::StoreError>>
    {
        Box::pin(async move {
            self.stall().await;
            self.check()?;
            if self
                .fail_gets_under
                .lock()
                .unwrap()
                .as_deref()
                .is_some_and(|prefix| key.starts_with(prefix))
            {
                return Err(extenddb_backup_store::StoreError::Transport {
                    source: Box::new(DispatchFailure(ConnectError("get refused".to_owned()))),
                });
            }
            let bytes = self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or(extenddb_backup_store::StoreError::NotFound)?;
            Ok(extenddb_backup_store::byte_stream_from(bytes))
        })
    }

    fn head<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxedFuture<
        'a,
        Result<Option<extenddb_backup_store::ObjectMeta>, extenddb_backup_store::StoreError>,
    > {
        Box::pin(async move {
            self.stall().await;
            self.check()?;
            Ok(self.objects.lock().unwrap().get(key).map(|bytes| {
                extenddb_backup_store::ObjectMeta {
                    key: key.to_owned(),
                    size: bytes.len() as u64,
                    last_modified: None,
                    etag: None,
                }
            }))
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
    ) -> BoxStream<'a, Result<extenddb_backup_store::ObjectMeta, extenddb_backup_store::StoreError>>
    {
        if let Err(e) = self.check() {
            return Box::pin(futures::stream::once(async move { Err(e) }));
        }
        if self.fail_lists_of.lock().unwrap().as_deref() == Some(prefix) {
            return Box::pin(futures::stream::once(async move {
                Err(extenddb_backup_store::StoreError::Transport {
                    source: Box::new(DispatchFailure(ConnectError("list refused".to_owned()))),
                })
            }));
        }
        let metas: Vec<_> = self
            .objects
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| Self::matches(k, prefix))
            .map(|(k, v)| {
                Ok(extenddb_backup_store::ObjectMeta {
                    key: k.clone(),
                    size: v.len() as u64,
                    last_modified: None,
                    etag: None,
                })
            })
            .collect();
        Box::pin(futures::stream::iter(metas))
    }

    fn delete_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> BoxedFuture<'a, Result<u64, extenddb_backup_store::StoreError>> {
        Box::pin(async move {
            self.stall().await;
            self.check()?;
            let delay = *self.delete_delay.lock().unwrap();
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            if self.fail_deletes_of.lock().unwrap().as_deref() == Some(prefix) {
                return Err(extenddb_backup_store::StoreError::Transport {
                    source: Box::new(DispatchFailure(ConnectError("delete refused".to_owned()))),
                });
            }
            self.deletes.lock().unwrap().push(prefix.to_owned());
            let mut objects = self.objects.lock().unwrap();
            let before = objects.len();
            objects.retain(|k, _| !Self::matches(k, prefix));
            let deleted = (before - objects.len()) as u64;
            if let Some(late) = self.late_put_after_delete.lock().unwrap().as_deref()
                && !prefix.ends_with(".json")
            {
                objects.insert(format!("{prefix}{late}"), b"late".to_vec());
            }
            Ok(deleted)
        })
    }

    fn validate(&self) -> BoxedFuture<'_, Result<(), extenddb_backup_store::StoreError>> {
        Box::pin(async move {
            self.stall().await;
            self.check()?;
            match self.validate_error.lock().unwrap().clone() {
                Some(text) => Err(extenddb_backup_store::StoreError::Other(text)),
                None => Ok(()),
            }
        })
    }
}

/// A runtime with `store` configured as `identity`, healthy.
fn runtime_on(mem: &Arc<MemStore>, identity: StoreIdentity) -> BackupRuntime {
    runtime().with_store(ConfiguredStore::new(
        identity.store_kind,
        identity.store_root.clone(),
        Arc::clone(mem) as Arc<dyn extenddb_backup_store::BackupStore>,
        None,
        Ok(identity),
    ))
}

/// A runtime whose configured store (`store()`'s kind and root) failed its
/// startup check with `problem`: the id is unknown.
fn runtime_unhealthy(mem: &Arc<MemStore>, problem: StoreProblem) -> BackupRuntime {
    runtime().with_store(ConfiguredStore::new(
        store().store_kind,
        store().store_root,
        Arc::clone(mem) as Arc<dyn extenddb_backup_store::BackupStore>,
        None,
        Err(problem),
    ))
}

/// The identity file of `store()` for catalog `catalog_id`, as bytes.
fn identity_file(store_id: &str, catalog_id: &str) -> Vec<u8> {
    StoreIdentityFile {
        format_version: 1,
        store_id: store_id.to_owned(),
        catalog_id: catalog_id.to_owned(),
        backend: "test".to_owned(),
        created_at_ms: 0,
    }
    .to_bytes()
}

/// A catalog row for `arn`, with the manifest fixture's source table (the
/// `Music` table of account 123456789012).
fn row(arn: &str, status: BackupStatus) -> BackupRow {
    let parsed = BackupArn::parse(arn).unwrap();
    let manifest: Value = serde_json::from_str(include_str!(
        "../../../core/testdata/extenddb-manifest/v1.json"
    ))
    .unwrap();
    BackupRow {
        backup_id: parsed.backup_id().to_owned(),
        account_id: parsed.account_id().to_owned(),
        table_name: parsed.table_name().to_owned(),
        backup_arn: arn.to_owned(),
        backup_name: "music-nightly".to_owned(),
        status,
        store: store(),
        size_bytes: 61828,
        item_count: 100_000,
        created_at: extenddb_storage::util::backup_creation_date_time(arn).unwrap(),
        completed_at: None,
        failure_reason: None,
        source_table_details_json: manifest["source_table"].clone(),
        owner_generation: 7,
        heartbeat_at: 1.0,
    }
}

impl BackupCatalog for Fake {
    fn catalog_id(&self) -> BoxedFuture<'_, Result<String, StorageError>> {
        if *self.catalog_id_error.lock().unwrap() {
            return Box::pin(async { Err(StorageError::Internal("pool exhausted".to_owned())) });
        }
        let id = self
            .catalog_id
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| "catalog-1".to_owned());
        Box::pin(async move { Ok(id) })
    }

    /// The backends' insert: the row as given, CREATING under a fresh
    /// generation with `heartbeat_at` now; `AlreadyExists` when the ARN has a
    /// row.
    fn claim_backup<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxedFuture<'a, Result<Box<dyn JobClaim>, StorageError>> {
        self.claims.lock().unwrap().push(row.clone());
        let mut rows = self.rows.lock().unwrap();
        if rows.contains_key(&row.backup_arn) {
            let arn = row.backup_arn.clone();
            return Box::pin(async move { Err(StorageError::AlreadyExists(arn)) });
        }
        let generation = new_generation();
        let mut inserted = row.clone();
        inserted.status = BackupStatus::Creating;
        inserted.owner_generation = generation;
        inserted.heartbeat_at = seconds_since_epoch();
        rows.insert(row.backup_arn.clone(), inserted);
        drop(rows);
        let claim = self.claim_on(&row.backup_arn, generation);
        Box::pin(async move { Ok(claim) })
    }

    /// The takeover: a CREATING row whose ARN is in the `dead` set gets a
    /// fresh generation, `heartbeat_at` untouched; anything else is `None`.
    fn claim_dead_backup<'a>(
        &'a self,
        key: &'a BackupKey,
        _stale_after: Duration,
    ) -> BoxedFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        let dead = self.dead.lock().unwrap().contains(&key.backup_arn);
        let mut rows = self.rows.lock().unwrap();
        let claim = match rows.get_mut(&key.backup_arn) {
            Some(row) if dead && row.status == BackupStatus::Creating => {
                row.owner_generation = new_generation();
                let generation = row.owner_generation;
                if self
                    .takeover_lost_to_another
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    row.owner_generation = new_generation();
                }
                drop(rows);
                self.takeovers.lock().unwrap().push(key.backup_arn.clone());
                Some(self.claim_on(&key.backup_arn, generation))
            }
            _ => None,
        };
        Box::pin(async move { Ok(claim) })
    }

    fn sweep_restore_registrations(&self) -> BoxedFuture<'_, Result<Vec<String>, StorageError>> {
        let swept = self.swept_registrations.lock().unwrap().clone();
        Box::pin(async move { Ok(swept) })
    }

    /// The backend's listing: rows of the store (kind, root and id), of the
    /// status when given, in (`created_at`, `backup_arn`) order.
    fn list_backup_rows<'a>(
        &'a self,
        status: Option<BackupStatus>,
        store: &'a StoreIdentity,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        if let Some(e) = self.list_error.lock().unwrap().clone() {
            return Box::pin(futures::stream::once(async move {
                Err(StorageError::Internal(e))
            }));
        }
        let mut rows: Vec<BackupRow> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.store == *store)
            .filter(|r| status.is_none_or(|s| r.status == s))
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            a.created_at
                .total_cmp(&b.created_at)
                .then_with(|| a.backup_arn.cmp(&b.backup_arn))
        });
        Box::pin(futures::stream::iter(rows.into_iter().map(Ok)))
    }

    /// The last AVAILABLE row of the store in the listing's order, read as
    /// one row; `list_error` fails it as it fails the listing.
    fn newest_available_backup_row<'a>(
        &'a self,
        store: &'a StoreIdentity,
    ) -> BoxedFuture<'a, Result<Option<BackupRow>, StorageError>> {
        self.newest_reads.lock().unwrap().push(store.clone());
        if let Some(e) = self.list_error.lock().unwrap().clone() {
            return Box::pin(async move { Err(StorageError::Internal(e)) });
        }
        let newest = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.store == *store && r.status == BackupStatus::Available)
            .max_by(|a, b| {
                a.created_at
                    .total_cmp(&b.created_at)
                    .then_with(|| a.backup_arn.cmp(&b.backup_arn))
            })
            .cloned();
        Box::pin(async move { Ok(newest) })
    }

    fn list_account_backup_rows<'a>(
        &'a self,
        account_id: &'a str,
        table_name: Option<&'a str>,
        bounds: &'a TimeBounds,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        self.bounds_seen.lock().unwrap().push(*bounds);
        let rows: Vec<BackupRow> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.account_id == account_id)
            .filter(|r| table_name.is_none_or(|t| r.table_name == t))
            .filter(|r| bounds.contains(r.created_at))
            .cloned()
            .collect();
        Box::pin(futures::stream::iter(rows.into_iter().map(Ok)))
    }

    /// The backend's rule: the row of (account, id), and only when its
    /// account, table and ARN equal the request's.
    fn read_backup_row<'a>(
        &'a self,
        arn: &'a BackupArn,
    ) -> BoxedFuture<'a, Result<Option<BackupRow>, StorageError>> {
        self.row_reads.lock().unwrap().push(arn.to_arn_string());
        let found = self
            .rows
            .lock()
            .unwrap()
            .values()
            .find(|r| r.account_id == arn.account_id() && r.backup_id == arn.backup_id())
            .filter(|r| r.table_name == arn.table_name() && r.backup_arn == arn.to_arn_string())
            .cloned();
        Box::pin(async move { Ok(found) })
    }

    fn start_backup_delete<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxedFuture<'a, Result<DeleteStart, StorageError>> {
        self.delete_starts_seen.lock().unwrap().push(key.clone());
        let answer = self
            .delete_starts
            .lock()
            .unwrap()
            .get(&key.backup_arn)
            .cloned();
        let mut rows = self.rows.lock().unwrap();
        let status = rows.get(&key.backup_arn).map(|r| r.status);
        // The natural answer moves an AVAILABLE row to DELETING, as the
        // backends do; a preset answer leaves the row as the test set it.
        if answer.is_none()
            && status == Some(BackupStatus::Available)
            && let Some(row) = rows.get_mut(&key.backup_arn)
        {
            row.status = BackupStatus::Deleting;
        }
        drop(rows);
        Box::pin(async move {
            match (answer, status) {
                (Some(answer), _) => Ok(answer),
                (None, Some(BackupStatus::Available)) => Ok(DeleteStart::Deleting),
                (None, Some(status)) => Ok(DeleteStart::Status(status)),
                (None, None) => Err(StorageError::Validation("Backup not found: x".to_owned())),
            }
        })
    }

    /// DELETING to DELETED, conditional, as the backends do.
    fn set_backup_deleted<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxedFuture<'a, Result<bool, StorageError>> {
        let moved = match self.rows.lock().unwrap().get_mut(&key.backup_arn) {
            Some(row) if row.status == BackupStatus::Deleting => {
                row.status = BackupStatus::Deleted;
                true
            }
            _ => false,
        };
        if moved {
            self.set_deleted
                .lock()
                .unwrap()
                .push(key.backup_arn.clone());
        }
        Box::pin(async move { Ok(moved) })
    }

    /// Remove a DELETED or FAILED tombstone, as the backends do.
    fn remove_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxedFuture<'a, Result<bool, StorageError>> {
        self.remove_attempts
            .lock()
            .unwrap()
            .push(key.backup_arn.clone());
        let mut rows = self.rows.lock().unwrap();
        let removed = match rows.get(&key.backup_arn) {
            Some(row) if matches!(row.status, BackupStatus::Deleted | BackupStatus::Failed) => {
                rows.remove(&key.backup_arn);
                true
            }
            _ => false,
        };
        if removed {
            self.removed_rows
                .lock()
                .unwrap()
                .push(key.backup_arn.clone());
        }
        Box::pin(async move { Ok(removed) })
    }

    /// Remove a CREATING row whose heartbeat is older than `stale_after`
    /// (on this process's clock, which stands for the catalog's here).
    fn remove_dead_creating_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxedFuture<'a, Result<bool, StorageError>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let mut rows = self.rows.lock().unwrap();
        let removed = match rows.get(&key.backup_arn) {
            Some(row)
                if row.status == BackupStatus::Creating
                    && row.heartbeat_at < now - stale_after.as_secs_f64() =>
            {
                rows.remove(&key.backup_arn);
                true
            }
            _ => false,
        };
        Box::pin(async move { Ok(removed) })
    }

    /// The backends' listing on the named clock, the catalog's being this
    /// process's moved by `catalog_clock_offset_secs`: rows of the store
    /// with `status` whose `heartbeat_at`, or `completed_at` when set, is
    /// older than `age`.
    fn list_backup_rows_older_than<'a>(
        &'a self,
        status: BackupStatus,
        store: &'a StoreIdentity,
        clock: StaleClock,
        age: Duration,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        let cutoff = seconds_since_epoch() + *self.catalog_clock_offset_secs.lock().unwrap()
            - age.as_secs_f64();
        let mut rows: Vec<BackupRow> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.store == *store && r.status == status)
            .filter(|r| match clock {
                StaleClock::Heartbeat => r.heartbeat_at < cutoff,
                StaleClock::Completed => r.completed_at.is_some_and(|c| c < cutoff),
            })
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            a.created_at
                .total_cmp(&b.created_at)
                .then_with(|| a.backup_arn.cmp(&b.backup_arn))
        });
        Box::pin(futures::stream::iter(rows.into_iter().map(Ok)))
    }

    fn backup_in_use<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxedFuture<'a, Result<Option<String>, StorageError>> {
        let found = self.in_use.lock().unwrap().get(&key.backup_arn).cloned();
        Box::pin(async move { Ok(found) })
    }

    fn remove_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxedFuture<'a, Result<bool, StorageError>> {
        self.removed_targets.lock().unwrap().push(key.clone());
        let answer = *self.target_still_creating.lock().unwrap();
        Box::pin(async move { Ok(answer) })
    }

    fn list_dead_restore_jobs(
        &self,
        _stale_after: Duration,
    ) -> BoxedFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        let jobs = self.dead_jobs.lock().unwrap().clone();
        Box::pin(async move { Ok(jobs) })
    }

    /// The RUNNING records of `dead_jobs` and every record of
    /// `running_jobs`, by status, whatever their heartbeat.
    fn list_running_restore_jobs(
        &self,
    ) -> BoxedFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        let mut jobs: Vec<RestoreJobRow> = self
            .dead_jobs
            .lock()
            .unwrap()
            .iter()
            .filter(|j| j.status == extenddb_storage::backup_catalog::JobStatus::Running)
            .cloned()
            .collect();
        jobs.extend(self.running_jobs.lock().unwrap().iter().cloned());
        Box::pin(async move { Ok(jobs) })
    }

    /// Matches as a backend does: only a record still RUNNING under the
    /// listed `owner_generation` and `heartbeat_at` (the record's current
    /// values are those of its `dead_jobs` entry, moved by
    /// `heartbeat_after_listing`).
    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        owner_generation: u64,
        heartbeat_at: f64,
        reason: &'a str,
    ) -> BoxedFuture<'a, Result<bool, StorageError>> {
        self.failed_jobs
            .lock()
            .unwrap()
            .push((key.clone(), reason.to_owned()));
        let current = self
            .dead_jobs
            .lock()
            .unwrap()
            .iter()
            .find(|j| j.target == *key)
            .map(|j| (j.owner_generation, j.heartbeat_at));
        let beat = self.heartbeat_after_listing.lock().unwrap().remove(key);
        let answer = *self.record_still_running.lock().unwrap()
            && current.is_some_and(|(generation, heartbeat)| {
                generation == owner_generation && beat.unwrap_or(heartbeat) == heartbeat_at
            });
        Box::pin(async move { Ok(answer) })
    }

    fn read_store_ids<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxedFuture<'a, Result<Vec<String>, StorageError>> {
        let mut ids: Vec<String> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|row| row.store.store_kind == store_kind && row.store.store_root == store_root)
            .map(|row| row.store.store_id.clone())
            .collect();
        ids.sort();
        ids.dedup();
        Box::pin(async move { Ok(ids) })
    }

    fn maintenance_claim<'a>(
        &'a self,
        _holder: &'a str,
        _stale_after: Duration,
    ) -> BoxedFuture<'a, Result<Box<dyn extenddb_storage::MaintenanceClaim>, StorageError>> {
        unused()
    }

    fn check_maintenance_claim(
        &self,
        stale_after: Duration,
    ) -> BoxedFuture<'_, Result<bool, StorageError>> {
        self.maintenance_checks.lock().unwrap().push(stale_after);
        let held = match *self.maintenance_beat_age.lock().unwrap() {
            Some(age) => age < stale_after,
            None => *self.maintenance_held.lock().unwrap(),
        };
        let error = self.maintenance_error.lock().unwrap().clone();
        let delay = *self.maintenance_delay.lock().unwrap();
        Box::pin(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            match error {
                Some(e) => Err(StorageError::Internal(e)),
                None => Ok(held),
            }
        })
    }
}

impl SnapshotEngine for Fake {
    /// The fixture for the table, or `TableNotFound`; the description and
    /// the items are the fixture's, in its order, through the gate when it
    /// has one.
    fn open_table_snapshot(
        &self,
        _account_id: &str,
        table_name: &str,
    ) -> BoxedFuture<'_, Result<TableSnapshot, StorageError>> {
        self.snapshots_opened
            .lock()
            .unwrap()
            .push(table_name.to_owned());
        if let Some(e) = self.snapshot_error.lock().unwrap().take() {
            return Box::pin(async move { Err(e) });
        }
        let mut snapshots = self.snapshots.lock().unwrap();
        let Some(fixture) = snapshots.get_mut(table_name) else {
            let name = table_name.to_owned();
            return Box::pin(async move {
                Err(StorageError::TableNotFound(format!(
                    "Table not found: {name}"
                )))
            });
        };
        let items = std::mem::take(&mut fixture.items);
        let gate = fixture.gate.clone();
        let snapshot = TableSnapshot {
            description: fixture.description.clone(),
            info: extenddb_core::backup_format::SnapshotDescriptor {
                kind: "test".to_owned(),
                marker: String::new(),
            },
            key_ordered: fixture.key_ordered,
            items: Box::pin(futures::stream::unfold(
                (items.into_iter(), gate),
                |(mut items, gate)| async move {
                    let next = items.next()?;
                    if let Some(gate) = &gate {
                        gate.acquire()
                            .await
                            .expect("the gate is never closed")
                            .forget();
                    }
                    Some((next, (items, gate)))
                },
            )),
        };
        Box::pin(async move { Ok(snapshot) })
    }
}

impl MetadataEngine for Fake {
    fn describe_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxedFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let ttl = self
            .ttl
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(TimeToLiveDescription {
                time_to_live_status: TimeToLiveStatus::Disabled,
                attribute_name: None,
            });
        Box::pin(async move { Ok(ttl) })
    }

    fn update_ttl(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: bool,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unused()
    }

    fn tag_resource(
        &self,
        _: &str,
        _: &[extenddb_core::types::Tag],
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unused()
    }

    fn untag_resource(&self, _: &str, _: &[String]) -> BoxedFuture<'_, Result<(), StorageError>> {
        unused()
    }

    fn list_tags(
        &self,
        _: &str,
    ) -> BoxedFuture<'_, Result<Vec<extenddb_core::types::Tag>, StorageError>> {
        unused()
    }

    fn tables_with_ttl(
        &self,
        _: &str,
    ) -> BoxedFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unused()
    }

    fn all_tables_with_ttl(
        &self,
    ) -> BoxedFuture<'_, Result<Vec<extenddb_storage::TtlTableInfo>, StorageError>> {
        unused()
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxedFuture<'_, Result<Vec<extenddb_storage::TtlTableInfo>, StorageError>> {
        unused()
    }

    fn create_ttl_index(
        &self,
        _: &str,
        _: &str,
        _: &str,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unused()
    }

    fn drop_ttl_index(
        &self,
        _: &str,
        _: &str,
        _: &str,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unused()
    }

    fn find_expired_items_indexed(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: usize,
    ) -> BoxedFuture<'_, Result<Vec<Item>, StorageError>> {
        unused()
    }

    fn refresh_table_size(&self, _: &str, _: &str) -> BoxedFuture<'_, Result<(), StorageError>> {
        unused()
    }

    fn list_active_table_names(
        &self,
        _: &str,
    ) -> BoxedFuture<'_, Result<Vec<String>, StorageError>> {
        unused()
    }

    fn all_active_tables(&self) -> BoxedFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unused()
    }
}

impl TableEngine for Fake {
    fn create_table(
        &self,
        _account_id: &str,
        _input: CreateTableInput,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        unused()
    }

    fn delete_table(
        &self,
        _account_id: &str,
        _input: DeleteTableInput,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        unused()
    }

    /// The account's table of that name, or `TableNotFound`.
    fn describe_table(
        &self,
        _account_id: &str,
        input: DescribeTableInput,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        self.table_reads
            .lock()
            .unwrap()
            .push(input.table_name.clone());
        let found = self.tables.lock().unwrap().get(&input.table_name).cloned();
        Box::pin(async move {
            found.ok_or_else(|| {
                StorageError::TableNotFound(format!("Table not found: {}", input.table_name))
            })
        })
    }

    fn list_tables(
        &self,
        _account_id: &str,
        _input: ListTablesInput,
    ) -> BoxedFuture<'_, Result<ListTablesOutput, StorageError>> {
        unused()
    }

    fn update_table(
        &self,
        _account_id: &str,
        _input: UpdateTableInput,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        unused()
    }

    fn table_key_info(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxedFuture<'_, Result<TableKeyInfo, StorageError>> {
        unused()
    }

    fn index_info(
        &self,
        _account_id: &str,
        _table_name: &str,
        _index_name: &str,
    ) -> BoxedFuture<'_, Result<IndexInfo, StorageError>> {
        unused()
    }

    fn index_info_by_table_id(
        &self,
        _table_id: &str,
        _index_name: &str,
    ) -> BoxedFuture<'_, Result<IndexInfo, StorageError>> {
        unused()
    }
}

impl LegacyBackupReader for Fake {
    fn create(
        &self,
        _: &str,
        table_name: &str,
        backup_name: &str,
        permit: JobPermit,
    ) -> BoxedFuture<'_, Result<BackupDetails, StorageError>> {
        self.creates
            .lock()
            .unwrap()
            .push((table_name.to_owned(), backup_name.to_owned()));
        self.permits.lock().unwrap().push(permit);
        Box::pin(async {
            Ok(BackupDetails {
                backup_arn: arn(ACCOUNT),
                backup_name: "nightly".to_owned(),
                backup_status: "AVAILABLE".to_owned(),
                backup_type: "USER".to_owned(),
                backup_size_bytes: 0,
                backup_creation_date_time: CREATED,
            })
        })
    }

    fn describe(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxedFuture<'_, Result<BackupDescription, StorageError>> {
        let found = self.descriptions.lock().unwrap().get(backup_arn).cloned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            found.ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))
        })
    }

    fn list(
        &self,
        _account_id: &str,
        table_name: Option<&str>,
    ) -> BoxedFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        self.listed_for
            .lock()
            .unwrap()
            .push(table_name.map(str::to_owned));
        let all = self.summaries.lock().unwrap().clone();
        Box::pin(async move { Ok(all) })
    }

    /// Removes the backup and answers its description as the backends do:
    /// the row's status (not yet `DELETED`), `SourceTableDetails` without
    /// `ProvisionedThroughput`, no feature details.
    fn delete(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxedFuture<'_, Result<BackupDescription, StorageError>> {
        self.deleted.lock().unwrap().push(backup_arn.to_owned());
        let found = self.descriptions.lock().unwrap().remove(backup_arn);
        self.sources.lock().unwrap().remove(backup_arn);
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            found.ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))
        })
    }

    fn restore_source(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxedFuture<'_, Result<BackupRestoreSource, StorageError>> {
        *self.source_reads.lock().unwrap() += 1;
        let err = self.source_error.lock().unwrap().take();
        let found = self.sources.lock().unwrap().get(backup_arn).cloned();
        let backup_arn = backup_arn.to_owned();
        let gate = self.source_gate.clone();
        Box::pin(async move {
            if let Some(gate) = gate {
                let _permit = gate.acquire().await.expect("gate closed");
            }
            if let Some(e) = err {
                return Err(e);
            }
            found.ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))
        })
    }

    fn restore(
        &self,
        _account_id: &str,
        request: RestoreRequest,
        permit: JobPermit,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        let err = self.restore_error.lock().unwrap().take();
        let desc = described(&request);
        self.restores.lock().unwrap().push(request);
        if err.is_none() {
            self.permits.lock().unwrap().push(permit);
        }
        Box::pin(async move {
            match err {
                Some(e) => Err(e),
                None => Ok(desc),
            }
        })
    }

    fn abandoned_creating_backups(
        &self,
        _stale_after: Duration,
    ) -> BoxedFuture<'_, Result<Vec<AbandonedLegacyBackup>, StorageError>> {
        let found = self.abandoned_backups.lock().unwrap().clone();
        Box::pin(async move { Ok(found) })
    }
}

/// What a backend reports for a CREATING target: deliberately the shape the
/// engine must correct (ACTIVE GSI status, a pay-per-request timestamp, a
/// backend-side summary with the wrong time).
fn described(request: &RestoreRequest) -> TableDescription {
    let input = &request.input;
    TableDescription {
        table_name: input.table_name.clone(),
        key_schema: input.key_schema.clone(),
        attribute_definitions: input.attribute_definitions.clone(),
        table_status: TableStatus::Creating,
        billing_mode_summary: Some(BillingModeSummary {
            billing_mode: input.billing_mode.unwrap_or(BillingMode::Provisioned),
            last_update_to_pay_per_request_date_time: Some(1.0),
        }),
        provisioned_throughput: ProvisionedThroughputDescription {
            read_capacity_units: 9,
            write_capacity_units: 9,
            ..Default::default()
        },
        global_secondary_indexes: input.global_secondary_indexes.as_ref().map(|gsis| {
            gsis.iter()
                .map(|g| GsiDescription {
                    index_name: g.index_name.clone(),
                    key_schema: g.key_schema.clone(),
                    projection: g.projection.clone(),
                    index_status: "ACTIVE".to_owned(),
                    provisioned_throughput: Some(ProvisionedThroughputDescription {
                        read_capacity_units: 9,
                        write_capacity_units: 9,
                        ..Default::default()
                    }),
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn: String::new(),
                })
                .collect()
        }),
        restore_summary: Some(RestoreSummary {
            source_backup_arn: Some("wrong".to_owned()),
            source_table_arn: None,
            restore_date_time: 42.0,
            restore_in_progress: false,
        }),
        ..Default::default()
    }
}

// --- fixtures ---------------------------------------------------------------

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn attr(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    }
}

fn throughput(read: i64, write: i64) -> ProvisionedThroughput {
    ProvisionedThroughput {
        read_capacity_units: read,
        write_capacity_units: write,
    }
}

fn gsi(name: &str, pk: &str, pt: Option<ProvisionedThroughput>) -> GsiInput {
    GsiInput {
        index_name: name.to_owned(),
        key_schema: vec![key(pk, KeyType::Hash)],
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
        provisioned_throughput: pt,
    }
}

fn definition(billing: &str) -> BackupTableDefinition {
    let provisioned = billing == "PROVISIONED";
    BackupTableDefinition {
        version: BACKUP_DEFINITION_VERSION,
        billing_mode: billing.to_owned(),
        provisioned_throughput: provisioned.then(|| throughput(7, 8)),
        global_secondary_indexes: vec![gsi("gsi1", "gpk", provisioned.then(|| throughput(3, 4)))],
        local_secondary_indexes: vec![LsiInput {
            index_name: "lsi1".to_owned(),
            key_schema: vec![key("pk", KeyType::Hash), key("lsk", KeyType::Range)],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
        }],
        table_class: None,
        sse_specification: None,
        on_demand_throughput: None,
        vector_index_names: Vec::new(),
        time_to_live: None,
        stream_specification: None,
    }
}

fn source(definition: Option<BackupTableDefinition>) -> BackupRestoreSource {
    let billing = definition
        .as_ref()
        .map_or("PROVISIONED".to_owned(), |d| d.billing_mode.clone());
    BackupRestoreSource {
        backup_arn: arn(ACCOUNT),
        backup_creation_date_time: CREATED,
        source_table_name: "Music".to_owned(),
        source_table_arn: source_arn(),
        source_table_id: "tid".to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: if definition.is_some() {
            vec![attr("pk"), attr("sk"), attr("gpk"), attr("lsk")]
        } else {
            vec![attr("pk"), attr("sk")]
        },
        provisioned_throughput: definition
            .as_ref()
            .and_then(|d| d.provisioned_throughput.clone()),
        billing_mode: billing,
        item_count: 3,
        table_size_bytes: 30,
        table_creation_date_time: 1.0,
        definition,
        legacy_vector_index_count: 0,
    }
}

fn restore_body(extra: Value) -> Value {
    let mut body = json!({ "TargetTableName": "MusicRestored", "BackupArn": arn(ACCOUNT) });
    for (k, v) in extra.as_object().unwrap() {
        body[k] = v.clone();
    }
    body
}

async fn restore(fake: &Fake, body: Value) -> Result<Value, DynamoDbError> {
    restore_on(fake, &runtime(), &body).await
}

/// `restore` on `runtime` rather than the tests' default one.
async fn restore_on(
    fake: &Fake,
    runtime: &BackupRuntime,
    body: &Value,
) -> Result<Value, DynamoDbError> {
    restore_table_from_backup(
        fake,
        runtime,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        body,
    )
    .await
    .map(|(_, v)| v)
}

fn validation_message(err: DynamoDbError) -> String {
    match err {
        DynamoDbError::ValidationException(m) => m,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

// --- BackupArn ---------------------------------------------------------------

/// The ARN the engine looks a `BackupArn` up under first: the one this
/// deployment issues for the backup named.
fn arn_field(body: &Value) -> Result<String, DynamoDbError> {
    backup_arn_field(body, ACCOUNT, REGION).map(|a| a.canonical().to_owned())
}

/// The spellings of an ARN as `backup_arn_field` resolves them for `arn`.
fn own(arn: &str) -> BackupArnSpellings {
    backup_arn_field(&json!({ "BackupArn": arn }), ACCOUNT, REGION).unwrap()
}

fn limits() -> LimitsConfig {
    LimitsConfig::default()
}

#[test]
fn own_account_arn_is_accepted() {
    let body = json!({ "BackupArn": arn(ACCOUNT) });
    assert_eq!(arn_field(&body).unwrap(), arn(ACCOUNT));
}

#[test]
fn other_account_arn_is_denied() {
    let body = json!({ "BackupArn": arn("999999999999") });
    let err = arn_field(&body).unwrap_err();
    assert!(
        matches!(err, DynamoDbError::AccessDeniedException(_)),
        "{err:?}"
    );
}

#[test]
fn account_prefix_does_not_match() {
    let body = json!({ "BackupArn": arn("12345678901") });
    assert!(matches!(
        arn_field(&body).unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));
}

/// Measured on DynamoDB: the shapes the service refuses as `ValidationException`
/// with `Invalid Request: BackupArn is not valid` (not an ARN, oversize,
/// control characters, a slash or `..` in the region) or `Invalid Backup ARN`
/// (an id suffix of the wrong length or not hex, a wildcard id), on every
/// member that carries a backup ARN. Which text goes with which input was
/// not captured, so the split is the parser's (see `BackupArnError`).
#[tokio::test]
async fn malformed_arn_is_a_validation_exception_with_the_services_text() {
    const NOT_VALID: &str = "Invalid Request: BackupArn is not valid";
    const INVALID_ID: &str = "Invalid Backup ARN";
    let id = "01791976431438-73d8d5bc";
    let long_table = "x".repeat(1000);
    let cases: Vec<(String, &str)> = vec![
        ("not-an-arn".to_owned(), NOT_VALID),
        ("arn:aws:dynamodb".to_owned(), NOT_VALID),
        (format!("arn:aws:dynamodb:{REGION}"), NOT_VALID),
        (String::new(), NOT_VALID),
        (
            format!("arn:aws:dynamodb:{REGION}::table/Music/backup/{id}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:table/Music/backup/{ACCOUNT}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/stream/{id}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:999999999999:table/Music/index/x"),
            NOT_VALID,
        ),
        // Oversize (the model's max is 1024).
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{long_table}/backup/{id}"),
            NOT_VALID,
        ),
        // Control characters, inside and at the end (a NUL byte was a 500 on
        // PostgreSQL before the check).
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/{id}\u{0}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Mu\u{1}sic/backup/{id}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/{id}\n\u{7}"),
            NOT_VALID,
        ),
        // A slash or `..` in the region.
        (
            format!("arn:aws:dynamodb:us/east-1:{ACCOUNT}:table/Music/backup/{id}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:..:{ACCOUNT}:table/Music/backup/{id}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:us-east-1/..:{ACCOUNT}:table/Music/backup/{id}"),
            NOT_VALID,
        ),
        // The id: wrong length, not hex, a wildcard.
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/1"),
            INVALID_ID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/*"),
            INVALID_ID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-*"),
            INVALID_ID,
        ),
        (
            format!(
                "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5b"
            ),
            INVALID_ID,
        ),
        (
            format!(
                "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bcd"
            ),
            INVALID_ID,
        ),
        (
            format!(
                "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-73D8D5BC"
            ),
            INVALID_ID,
        ),
        (
            format!(
                "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bg"
            ),
            INVALID_ID,
        ),
        (
            format!(
                "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/0179197643143x-73d8d5bc"
            ),
            INVALID_ID,
        ),
    ];
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    for (candidate, expected) in &cases {
        for (member, result) in [
            ("BackupArn", arn_field(&json!({ "BackupArn": candidate }))),
            (
                "RestoreTableFromBackup",
                restore(&fake, restore_body(json!({ "BackupArn": candidate })))
                    .await
                    .map(|_| String::new()),
            ),
            (
                "ExclusiveStartBackupArn",
                list(&fake, json!({ "ExclusiveStartBackupArn": candidate }))
                    .await
                    .map(|_| String::new()),
            ),
        ] {
            let msg = validation_message(result.unwrap_err());
            assert_eq!(msg, *expected, "{member} {candidate:?}");
        }
    }
    assert_eq!(fake.source_reads(), 0);
    assert!(fake.restores().is_empty());
}

/// Measured on DynamoDB: the service trims a trailing newline and resolves an ARN
/// naming another region of the caller's account. The engine hands the
/// backend the ARN it issued: trimmed, in its own region.
#[tokio::test]
async fn whitespace_is_trimmed_and_another_region_resolves() {
    let ours = arn(ACCOUNT);
    for candidate in [
        format!("{ours}\n"),
        format!("  {ours}\t\r\n"),
        format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bc"),
        format!(
            "arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bc\n"
        ),
    ] {
        assert_eq!(
            arn_field(&json!({ "BackupArn": candidate })).unwrap(),
            ours,
            "{candidate:?}"
        );
        let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
        restore(&fake, restore_body(json!({ "BackupArn": candidate })))
            .await
            .unwrap_or_else(|e| panic!("{candidate:?}: {e:?}"));
        assert_eq!(fake.restores()[0].backup_arn, ours, "{candidate:?}");
    }
    // Another region of another account is still that account's.
    let foreign =
        "arn:aws:dynamodb:eu-west-1:999999999999:table/Music/backup/01791976431438-73d8d5bc";
    assert!(matches!(
        arn_field(&json!({ "BackupArn": foreign })).unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));
    // Whitespace inside is not trimmed.
    let inside =
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Mu sic/backup/01791976431438-73d8d5bc");
    assert_eq!(
        validation_message(arn_field(&json!({ "BackupArn": inside })).unwrap_err()),
        "Invalid Request: BackupArn is not valid"
    );
}

/// A backup created under an earlier `server.region` is stored with that
/// region in its ARN, which `ListBackups` reports as is. `DescribeBackup`,
/// `DeleteBackup`, and `RestoreTableFromBackup` look the ARN up rebuilt in
/// the server's region first and, when that is not found, as written: the
/// stored backup stays reachable by the ARN the server reports for it, and
/// the engine acts on whichever spelling resolved.
#[tokio::test]
async fn a_backup_stored_under_another_region_resolves_as_written() {
    let stored =
        format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bc");
    let ours = arn(ACCOUNT);
    let spellings = own(&stored);
    assert_eq!(spellings.canonical(), ours);
    assert_eq!(spellings.as_written.as_deref(), Some(stored.as_str()));
    // The server's own spelling has no second spelling to try.
    assert_eq!(own(&ours).as_written, None);
    assert_eq!(own(&format!(" {ours}\n")).as_written, None, "trimmed first");

    let stored_source = || {
        let mut src = source(Some(definition("PROVISIONED")));
        src.backup_arn.clone_from(&stored);
        src
    };
    let with_stored = || {
        let fake = Fake::with_source(stored_source());
        fake.descriptions.lock().unwrap().insert(
            stored.clone(),
            backup_description("AVAILABLE", "PROVISIONED"),
        );
        fake
    };

    // DescribeBackup: canonical not found, as written found.
    let fake = with_stored();
    let desc = describe_backup(&fake, ACCOUNT, REGION, &spellings)
        .await
        .unwrap();
    assert_eq!(desc.backup_details.backup_status, "AVAILABLE");
    assert!(
        desc.source_table_feature_details.is_some(),
        "source read under the stored ARN"
    );
    assert_eq!(fake.source_reads(), 1);

    // RestoreTableFromBackup: registered against the stored spelling.
    let fake = with_stored();
    restore(&fake, restore_body(json!({ "BackupArn": stored })))
        .await
        .unwrap();
    assert_eq!(fake.restores()[0].backup_arn, stored);
    assert_eq!(
        fake.source_reads(),
        2,
        "canonical (not found), then as written"
    );

    // DeleteBackup: the full description, and the stored row is gone.
    let fake = with_stored();
    let deleted = delete_backup(&fake, &runtime(), ACCOUNT, REGION, &spellings)
        .await
        .unwrap();
    assert_eq!(deleted.backup_details.backup_status, "DELETED");
    assert!(deleted.source_table_feature_details.is_some());
    assert!(fake.descriptions.lock().unwrap().is_empty());
    assert!(fake.sources.lock().unwrap().is_empty());

    // The canonical spelling wins when both exist: the backend is asked for
    // it alone (one read), and the stored other-region row is untouched.
    let fake = with_stored();
    fake.sources
        .lock()
        .unwrap()
        .insert(ours.clone(), source(Some(definition("PAY_PER_REQUEST"))));
    fake.descriptions.lock().unwrap().insert(
        ours.clone(),
        backup_description("AVAILABLE", "PAY_PER_REQUEST"),
    );
    restore(&fake, restore_body(json!({ "BackupArn": stored })))
        .await
        .unwrap();
    assert_eq!(fake.restores()[0].backup_arn, ours);
    assert_eq!(fake.source_reads(), 1);
    let deleted = delete_backup(&fake, &runtime(), ACCOUNT, REGION, &spellings)
        .await
        .unwrap();
    assert_eq!(deleted.backup_details.backup_status, "DELETED");
    assert!(fake.descriptions.lock().unwrap().contains_key(&stored));
    assert!(!fake.descriptions.lock().unwrap().contains_key(&ours));

    // Neither spelling: BackupNotFoundException from the backend, after both
    // were tried; the server's own spelling is tried once.
    let fake = Fake::default();
    for (body, reads) in [(stored.clone(), 2), (ours.clone(), 1)] {
        let before = fake.source_reads();
        let err = restore(&fake, restore_body(json!({ "BackupArn": body })))
            .await
            .unwrap_err();
        assert!(
            matches!(err, DynamoDbError::BackupNotFoundException(_)),
            "{err:?}"
        );
        assert_eq!(fake.source_reads() - before, reads);
        assert!(matches!(
            describe_backup(&fake, ACCOUNT, REGION, &own(&body))
                .await
                .unwrap_err(),
            DynamoDbError::BackupNotFoundException(_)
        ));
        assert!(matches!(
            delete_backup(&fake, &runtime(), ACCOUNT, REGION, &own(&body))
                .await
                .unwrap_err(),
            DynamoDbError::BackupNotFoundException(_)
        ));
    }

    // An error other than not-found on the canonical spelling is not a
    // reason to try the other one.
    let fake = with_stored();
    *fake.source_error.lock().unwrap() =
        Some(StorageError::Connection("connection refused".to_owned()));
    let err = restore(&fake, restore_body(json!({ "BackupArn": stored })))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::InternalServerError(_)),
        "{err:?}"
    );
    assert_eq!(fake.source_reads(), 1);
    assert!(fake.restores().is_empty());
}

/// An ARN in another partition names nothing here: `ValidationException`
/// (the service's class for one naming the caller's account), before the
/// account is compared, on every member that carries a backup ARN.
#[tokio::test]
async fn an_arn_in_another_partition_is_a_validation_exception() {
    let ours = arn(ACCOUNT);
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    fake.descriptions
        .lock()
        .unwrap()
        .insert(ours.clone(), backup_description("AVAILABLE", "PROVISIONED"));
    *fake.summaries.lock().unwrap() = vec![summary(1, 1)];
    for partition in ["aws-cn", "aws-us-gov", "aws-iso", "AWS", "x"] {
        for account in [ACCOUNT, "999999999999"] {
            let candidate = format!(
                "arn:{partition}:dynamodb:{REGION}:{account}:table/Music/backup/01791976431438-73d8d5bc"
            );
            let err = arn_field(&json!({ "BackupArn": candidate })).unwrap_err();
            assert_eq!(
                validation_message(err),
                "Invalid Request: BackupArn is not valid"
            );
            let err = restore(&fake, restore_body(json!({ "BackupArn": candidate })))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DynamoDbError::ValidationException(_)),
                "{err:?}"
            );
            let err = list(&fake, json!({ "ExclusiveStartBackupArn": candidate }))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DynamoDbError::ValidationException(_)),
                "{err:?}"
            );
        }
    }
    assert_eq!(fake.source_reads(), 0);
    assert!(fake.restores().is_empty());
    // Liveness: the same ARN in this server's partition resolves.
    assert_eq!(
        ours,
        extenddb_storage::util::backup_arn(REGION, ACCOUNT, "Music", "01791976431438-73d8d5bc")
    );
    assert_eq!(arn_field(&json!({ "BackupArn": ours })).unwrap(), ours);
    restore(&fake, restore_body(json!({}))).await.unwrap();
    assert_eq!(fake.restores().len(), 1);
}

#[test]
fn missing_arn_field_is_a_validation_error() {
    let err = arn_field(&json!({})).unwrap_err();
    assert!(matches!(err, DynamoDbError::ValidationException(m) if m.contains("backupArn")));
}

/// A request dropped while the engine is still reading the backup (the client
/// disconnected) does not cancel the restore: it still reaches the backend.
#[tokio::test]
async fn a_restore_request_dropped_while_the_backup_is_read_still_restores() {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Waker};

    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    fake.source_gate = Some(Arc::clone(&gate));
    let fake = Arc::new(fake);

    let after_ran = Arc::new(tokio::sync::Notify::new());
    let notify = Arc::clone(&after_ran);
    let mut request = Box::pin(restore_table_from_backup_detached(
        Arc::clone(&fake),
        Arc::new(runtime()),
        Arc::from(ACCOUNT),
        Arc::from(REGION),
        Arc::new(LimitsConfig::default()),
        restore_body(json!({})),
        move |_, target| async move {
            assert_eq!(target, "MusicRestored");
            notify.notify_one();
        },
    ));
    // Poll once so the restore starts, wait until it is blocked reading the
    // backup, then drop the request as a disconnecting client's would be.
    let mut cx = Context::from_waker(Waker::noop());
    assert!(request.as_mut().poll(&mut cx).is_pending());
    for _ in 0..200 {
        if fake.source_reads() == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(fake.source_reads(), 1, "the restore never read the backup");
    drop(request);
    assert!(
        fake.restores().is_empty(),
        "precondition: not registered yet"
    );

    gate.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), after_ran.notified())
        .await
        .expect("the dropped request's restore never reached the backend");
    assert_eq!(fake.restores().len(), 1);
}

#[tokio::test]
async fn restore_checks_the_arn_before_the_target_name() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    // "x" is shorter than the 3-character minimum table name.
    let malformed = json!({ "TargetTableName": "x", "BackupArn": "garbage" });
    let msg = validation_message(restore(&fake, malformed).await.unwrap_err());
    assert!(msg.contains("BackupArn"), "{msg}");

    let foreign = json!({ "TargetTableName": "x", "BackupArn": arn("999999999999") });
    assert!(matches!(
        restore(&fake, foreign).await.unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));

    let missing_name = json!({ "BackupArn": arn("999999999999") });
    assert!(matches!(
        restore(&fake, missing_name).await.unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));

    // With a good ARN, the bad name is what is reported.
    let bad_name = json!({ "TargetTableName": "x", "BackupArn": arn(ACCOUNT) });
    let msg = validation_message(restore(&fake, bad_name).await.unwrap_err());
    assert!(!msg.contains("BackupArn"), "{msg}");
    assert_eq!(fake.source_reads(), 0, "no storage read for a bad request");
    assert!(fake.restores().is_empty());
}

// --- overrides ---------------------------------------------------------------

#[test]
fn restore_request_without_overrides_is_accepted() {
    assert!(
        parse_restore_table_overrides(&restore_body(json!({})))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn provisioned_override_with_throughput_is_accepted() {
    let overrides = parse_restore_table_overrides(&json!({
        "BillingModeOverride": "PROVISIONED",
        "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
    }))
    .unwrap();
    validate_override_shape(&overrides).unwrap();
    validate_override_for_source(&overrides, "PAY_PER_REQUEST").unwrap();
    assert_eq!(overrides.billing_mode, Some(BillingMode::Provisioned));
    assert_eq!(overrides.provisioned_throughput, Some(throughput(5, 5)));
}

#[test]
fn provisioned_override_without_throughput_is_refused() {
    let overrides =
        parse_restore_table_overrides(&json!({ "BillingModeOverride": "PROVISIONED" })).unwrap();
    assert!(matches!(
        validate_override_shape(&overrides).unwrap_err(),
        DynamoDbError::ValidationException(_)
    ));
}

#[test]
fn throughput_override_under_pay_per_request_is_refused() {
    let overrides = parse_restore_table_overrides(&json!({
        "BillingModeOverride": "PAY_PER_REQUEST",
        "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
    }))
    .unwrap();
    let msg =
        validation_message(validate_override_for_source(&overrides, "PROVISIONED").unwrap_err());
    assert!(msg.contains(
        "ProvisionedThroughputOverride can only be specified with BillingModeOverride PROVISIONED"
    ));
}

#[test]
fn invalid_provisioned_throughput_override_matches_create_table() {
    let expected = "One or more parameter values were invalid: ReadCapacityUnits and \
                    WriteCapacityUnits must both be greater than or equal to 1 for table";
    for (read, write) in [(0, 5), (5, 0), (-1, 5), (5, -1)] {
        let overrides = parse_restore_table_overrides(&json!({
            "BillingModeOverride": "PROVISIONED",
            "ProvisionedThroughputOverride": { "ReadCapacityUnits": read, "WriteCapacityUnits": write },
        }))
        .unwrap();
        assert_eq!(
            validation_message(validate_override_shape(&overrides).unwrap_err()),
            expected
        );
    }
}

#[test]
fn missing_provisioned_throughput_override_member_matches_create_table() {
    for (member, body) in [
        (
            "ReadCapacityUnits",
            json!({ "BillingModeOverride": "PROVISIONED",
                    "ProvisionedThroughputOverride": { "WriteCapacityUnits": 5 } }),
        ),
        (
            "WriteCapacityUnits",
            json!({ "BillingModeOverride": "PROVISIONED",
                    "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5 } }),
        ),
    ] {
        match parse_restore_table_overrides(&body).unwrap_err() {
            DynamoDbError::SerializationException(m) => {
                assert!(m.contains(&format!("missing field `{member}`")), "{m}");
            }
            other => panic!("expected SerializationException, got {other:?}"),
        }
    }
}

#[test]
fn vector_index_override_is_refused() {
    let body = restore_body(json!({ "VectorIndexOverride": {} }));
    assert_eq!(
        validation_message(parse_restore_table_overrides(&body).unwrap_err()),
        "RestoreTableFromBackup does not support VectorIndexOverride on ExtendDB"
    );
}

#[test]
fn null_override_members_are_absent() {
    let body = restore_body(json!({
        "GlobalSecondaryIndexOverride": null,
        "LocalSecondaryIndexOverride": null,
        "SSESpecificationOverride": null,
        "OnDemandThroughputOverride": null,
        "VectorIndexOverride": null,
        "BillingModeOverride": null,
        "ProvisionedThroughputOverride": null,
    }));
    assert!(parse_restore_table_overrides(&body).unwrap().is_empty());
}

#[test]
fn index_and_on_demand_overrides_of_the_wrong_type_are_serialization_errors() {
    for (field, value) in [
        ("GlobalSecondaryIndexOverride", json!("nope")),
        ("LocalSecondaryIndexOverride", json!({})),
        ("OnDemandThroughputOverride", json!([1])),
        // An entry without its required members.
        (
            "GlobalSecondaryIndexOverride",
            json!([{ "IndexName": "gsi1" }]),
        ),
        (
            "LocalSecondaryIndexOverride",
            json!([{ "IndexName": "lsi1" }]),
        ),
        (
            "OnDemandThroughputOverride",
            json!({ "MaxReadRequestUnits": "x" }),
        ),
    ] {
        let body = restore_body(json!({ field: value }));
        assert!(
            matches!(
                overrides::IndexOverrides::parse(&body).unwrap_err(),
                DynamoDbError::SerializationException(_)
            ),
            "{field} {value}"
        );
    }
}

#[tokio::test]
async fn a_malformed_index_override_is_refused_before_any_storage_read() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    let err = restore(
        &fake,
        restore_body(json!({ "GlobalSecondaryIndexOverride": [{ "IndexName": "gsi1" }] })),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::SerializationException(_)),
        "{err:?}"
    );
    assert_eq!(fake.source_reads(), 0);
}

/// The backup's GSI and LSI as a client would send them back: the shape
/// `definition` records, in the wire's own form.
fn gsi_entry(pt: Option<(i64, i64)>) -> Value {
    let mut entry = json!({
        "IndexName": "gsi1",
        "KeySchema": [{ "AttributeName": "gpk", "KeyType": "HASH" }],
        "Projection": { "ProjectionType": "ALL" },
    });
    if let Some((r, w)) = pt {
        entry["ProvisionedThroughput"] = json!({ "ReadCapacityUnits": r, "WriteCapacityUnits": w });
    }
    entry
}

fn lsi_entry() -> Value {
    json!({
        "IndexName": "lsi1",
        "KeySchema": [
            { "AttributeName": "pk", "KeyType": "HASH" },
            { "AttributeName": "lsk", "KeyType": "RANGE" },
        ],
        "Projection": { "ProjectionType": "KEYS_ONLY" },
    })
}

/// The target input of a restore of `src` with `body`'s overrides, or the
/// engine's refusal.
async fn restored_input(
    src: BackupRestoreSource,
    body: Value,
) -> Result<CreateTableInput, DynamoDbError> {
    let fake = Fake::with_source(src);
    restore(&fake, restore_body(body)).await?;
    Ok(fake.restores()[0].input.clone())
}

/// The attribute names `input` defines.
fn attr_names(input: &CreateTableInput) -> Vec<&str> {
    input
        .attribute_definitions
        .iter()
        .map(|a| a.attribute_name.as_str())
        .collect()
}

fn gsi_names(input: &CreateTableInput) -> Vec<&str> {
    input
        .global_secondary_indexes
        .iter()
        .flatten()
        .map(|g| g.index_name.as_str())
        .collect()
}

fn gsi_throughputs(input: &CreateTableInput) -> Vec<Option<(i64, i64)>> {
    input
        .global_secondary_indexes
        .iter()
        .flatten()
        .map(|g| {
            g.provisioned_throughput
                .as_ref()
                .map(|t| (t.read_capacity_units, t.write_capacity_units))
        })
        .collect()
}

/// A backup with two GSIs, the second with an INCLUDE projection.
fn two_gsis(billing: &str) -> BackupTableDefinition {
    let mut def = definition(billing);
    let mut second = gsi(
        "gsi2",
        "gpk",
        (billing == "PROVISIONED").then(|| throughput(1, 2)),
    );
    second.projection = Projection {
        projection_type: ProjectionType::Include,
        non_key_attributes: Some(vec!["a".to_owned(), "b".to_owned()]),
    };
    def.global_secondary_indexes.push(second);
    def
}

fn gsi2_entry(nka: Value, pt: Option<(i64, i64)>) -> Value {
    let mut entry = json!({
        "IndexName": "gsi2",
        "KeySchema": [{ "AttributeName": "gpk", "KeyType": "HASH" }],
        "Projection": { "ProjectionType": "INCLUDE", "NonKeyAttributes": nka },
    });
    if let Some((r, w)) = pt {
        entry["ProvisionedThroughput"] = json!({ "ReadCapacityUnits": r, "WriteCapacityUnits": w });
    }
    entry
}

/// Measured on DynamoDB: an empty index override list
/// restores no index of that kind. The attribute definitions only those
/// indexes used go with them, or CreateTable's validation would refuse the
/// input.
#[tokio::test]
async fn an_empty_index_override_list_restores_no_indexes_of_that_kind() {
    for def in [definition("PROVISIONED"), definition("PAY_PER_REQUEST")] {
        let input = restored_input(
            source(Some(def.clone())),
            json!({ "GlobalSecondaryIndexOverride": [] }),
        )
        .await
        .unwrap();
        assert!(input.global_secondary_indexes.is_none(), "{input:?}");
        assert_eq!(
            input.local_secondary_indexes.as_ref().map(Vec::len),
            Some(1)
        );
        assert_eq!(attr_names(&input), ["pk", "sk", "lsk"]);

        let input = restored_input(
            source(Some(def.clone())),
            json!({ "LocalSecondaryIndexOverride": [] }),
        )
        .await
        .unwrap();
        assert!(input.local_secondary_indexes.is_none());
        assert_eq!(gsi_names(&input), ["gsi1"]);
        assert_eq!(attr_names(&input), ["pk", "sk", "gpk"]);

        let input = restored_input(
            source(Some(def)),
            json!({ "GlobalSecondaryIndexOverride": [], "LocalSecondaryIndexOverride": [] }),
        )
        .await
        .unwrap();
        assert!(input.global_secondary_indexes.is_none());
        assert!(input.local_secondary_indexes.is_none());
        assert_eq!(attr_names(&input), ["pk", "sk"]);
        extenddb_core::validation::validate_create_table(&input, &LimitsConfig::default())
            .expect("CreateTable accepts the pruned input");
    }
    // Also with a billing override to PROVISIONED on an on-demand backup:
    // with no GSIs left there is nothing that needs throughput.
    let input = restored_input(
        source(Some(definition("PAY_PER_REQUEST"))),
        json!({
            "BillingModeOverride": "PROVISIONED",
            "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 6 },
            "GlobalSecondaryIndexOverride": [],
        }),
    )
    .await
    .unwrap();
    assert!(input.global_secondary_indexes.is_none());
    assert_eq!(input.provisioned_throughput, Some(throughput(5, 6)));
}

/// Measured on DynamoDB: the service applies a changed
/// index list. A subset restores those indexes, a changed GSI throughput is
/// applied, the list's order is the restored order, and a list that
/// describes the backup's indexes unchanged restores the same table as no
/// list at all.
#[tokio::test]
async fn index_override_lists_are_applied() {
    let plain = restored_input(source(Some(definition("PROVISIONED"))), json!({}))
        .await
        .unwrap();
    assert_eq!(gsi_throughputs(&plain), [Some((3, 4))]);
    assert_eq!(attr_names(&plain), ["pk", "sk", "gpk", "lsk"]);

    // Unchanged, as the console sends it (the backup's throughput).
    let same = restored_input(
        source(Some(definition("PROVISIONED"))),
        json!({
            "GlobalSecondaryIndexOverride": [gsi_entry(Some((3, 4)))],
            "LocalSecondaryIndexOverride": [lsi_entry()],
        }),
    )
    .await
    .unwrap();
    assert_eq!(same, plain);

    // A changed GSI throughput.
    let changed = restored_input(
        source(Some(definition("PROVISIONED"))),
        json!({ "GlobalSecondaryIndexOverride": [gsi_entry(Some((30, 40)))] }),
    )
    .await
    .unwrap();
    assert_eq!(gsi_throughputs(&changed), [Some((30, 40))]);
    assert_eq!(gsi_names(&changed), ["gsi1"]);
    assert_eq!(attr_names(&changed), attr_names(&plain));

    // A subset: the backup has two GSIs, the list names one. The other
    // index's attribute definition goes with it.
    let subset = restored_input(
        source(Some(two_gsis("PROVISIONED"))),
        json!({ "GlobalSecondaryIndexOverride": [gsi2_entry(json!(["b", "a"]), Some((1, 2)))] }),
    )
    .await
    .unwrap();
    assert_eq!(gsi_names(&subset), ["gsi2"]);
    assert_eq!(gsi_throughputs(&subset), [Some((1, 2))]);
    assert_eq!(
        subset.global_secondary_indexes.as_ref().unwrap()[0]
            .projection
            .non_key_attributes,
        Some(vec!["a".to_owned(), "b".to_owned()]),
        "the backup's projection is restored, not the entry's spelling of it"
    );
    assert_eq!(attr_names(&subset), ["pk", "sk", "gpk", "lsk"]);
    extenddb_core::validation::validate_create_table(&subset, &LimitsConfig::default()).unwrap();

    // The list's order is the restored order.
    let reordered = restored_input(
        source(Some(two_gsis("PROVISIONED"))),
        json!({ "GlobalSecondaryIndexOverride": [
            gsi2_entry(json!(["a", "b"]), Some((1, 2))),
            gsi_entry(Some((3, 4))),
        ] }),
    )
    .await
    .unwrap();
    assert_eq!(gsi_names(&reordered), ["gsi2", "gsi1"]);

    // An LSI subset of one (the only one) and an on-demand backup's GSIs,
    // listed without throughput as DescribeBackup's {0, 0} cannot be sent.
    let on_demand = restored_input(source(Some(two_gsis("PAY_PER_REQUEST"))), json!({}))
        .await
        .unwrap();
    let listed = restored_input(
        source(Some(two_gsis("PAY_PER_REQUEST"))),
        json!({
            "GlobalSecondaryIndexOverride": [gsi_entry(None), gsi2_entry(json!(["a", "b"]), None)],
            "LocalSecondaryIndexOverride": [lsi_entry()],
        }),
    )
    .await
    .unwrap();
    assert_eq!(listed, on_demand);
}

/// The DescribeBackup echo (a console or SDK restore flow) restores exactly
/// the table the restore creates without it, for both billing modes; sent
/// with the `{0, 0}` DescribeBackup reports for an on-demand backup's GSI it
/// is refused as CreateTable refuses `{0, 0}`.
#[tokio::test]
async fn index_overrides_echoing_describe_backup_restore_the_same_table() {
    for def in [definition("PAY_PER_REQUEST"), definition("PROVISIONED")] {
        let on_demand = def.billing_mode == "PAY_PER_REQUEST";
        let fake = fake_with_backup("AVAILABLE", source(Some(def.clone())));
        let features = describe(&fake).await["SourceTableFeatureDetails"].clone();
        let mut gsis = features["GlobalSecondaryIndexes"].clone();
        let lsis = features["LocalSecondaryIndexes"].clone();
        assert!(
            gsis.as_array().is_some_and(|g| g.len() == 1),
            "precondition, DescribeBackup reports the GSI: {features}"
        );
        if on_demand {
            assert_eq!(
                gsis[0]["ProvisionedThroughput"],
                json!({ "ReadCapacityUnits": 0, "WriteCapacityUnits": 0 }),
                "precondition: an on-demand backup's GSI is reported as {{0, 0}}"
            );
            let zero_echo = restored_input(
                source(Some(def.clone())),
                json!({ "GlobalSecondaryIndexOverride": gsis.clone() }),
            )
            .await
            .unwrap_err();
            let msg = validation_message(zero_echo);
            assert!(
                msg.contains("gsi1") && msg.contains("should not be specified"),
                "{msg}"
            );
            for g in gsis.as_array_mut().unwrap() {
                g.as_object_mut().unwrap().remove("ProvisionedThroughput");
            }
        }
        let plain = restored_input(source(Some(def.clone())), json!({}))
            .await
            .unwrap();
        let echoed = restored_input(
            source(Some(def)),
            json!({
                "GlobalSecondaryIndexOverride": gsis,
                "LocalSecondaryIndexOverride": lsis,
            }),
        )
        .await
        .unwrap_or_else(|e| panic!("the DescribeBackup echo was refused: {e:?}"));
        assert_eq!(echoed, plain);
    }
}

/// Measured on DynamoDB: refused by the service and by ExtendDB: an unknown or
/// changed index definition, a throughput member contradicting the billing
/// mode, `{0, 0}`. ExtendDB's own: a member it cannot apply.
#[tokio::test]
async fn index_overrides_the_service_refuses_are_refused() {
    let entry_with = |key: &str, value: Value| {
        let mut e = gsi_entry(Some((3, 4)));
        e[key] = value;
        e
    };
    let cases: [(&str, Value, &str); 13] = [
        (
            "an index the backup does not have",
            json!({ "LocalSecondaryIndexOverride": [], "GlobalSecondaryIndexOverride": [
                entry_with("IndexName", json!("other")),
            ] }),
            "global secondary index other is not in the backup",
        ),
        (
            "listed twice",
            json!({ "GlobalSecondaryIndexOverride": [
                gsi_entry(Some((3, 4))), gsi_entry(Some((3, 4))),
            ] }),
            "global secondary index gsi1 is listed more than once",
        ),
        (
            "key schema",
            json!({ "GlobalSecondaryIndexOverride": [entry_with(
                "KeySchema", json!([{ "AttributeName": "pk", "KeyType": "HASH" }]),
            )] }),
            "index gsi1: KeySchema differs from the backup's",
        ),
        (
            "key schema order",
            json!({ "LocalSecondaryIndexOverride": [{
                "IndexName": "lsi1",
                "KeySchema": [
                    { "AttributeName": "lsk", "KeyType": "RANGE" },
                    { "AttributeName": "pk", "KeyType": "HASH" },
                ],
                "Projection": { "ProjectionType": "KEYS_ONLY" },
            }] }),
            "index lsi1: KeySchema differs from the backup's",
        ),
        (
            "projection type",
            json!({ "GlobalSecondaryIndexOverride": [entry_with(
                "Projection", json!({ "ProjectionType": "KEYS_ONLY" }),
            )] }),
            "index gsi1: Projection differs from the backup's",
        ),
        (
            "projected attributes",
            json!({ "GlobalSecondaryIndexOverride": [entry_with(
                "Projection", json!({ "ProjectionType": "ALL", "NonKeyAttributes": ["x"] }),
            )] }),
            "index gsi1: Projection differs from the backup's",
        ),
        (
            "throughput on an on-demand restore",
            json!({
                "BillingModeOverride": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexOverride": [gsi_entry(Some((3, 4)))],
            }),
            "ProvisionedThroughput should not be specified for index: gsi1",
        ),
        (
            "{0, 0}",
            json!({ "GlobalSecondaryIndexOverride": [gsi_entry(Some((0, 0)))] }),
            "ReadCapacityUnits and WriteCapacityUnits must both be greater than or equal to 1",
        ),
        (
            "an index on-demand throughput on a PROVISIONED restore",
            json!({ "GlobalSecondaryIndexOverride": [entry_with(
                "OnDemandThroughput", json!({ "MaxReadRequestUnits": 10 }),
            )] }),
            "index gsi1: OnDemandThroughput can only be specified when the restored table's \
             BillingMode is PAY_PER_REQUEST",
        ),
        (
            "a member ExtendDB cannot apply",
            json!({ "GlobalSecondaryIndexOverride": [entry_with(
                "WarmThroughput", json!({ "ReadUnitsPerSecond": 12000 }),
            )] }),
            "index gsi1: member WarmThroughput is not supported",
        ),
        (
            "an LSI member ExtendDB cannot apply",
            json!({ "LocalSecondaryIndexOverride": [{
                "IndexName": "lsi1",
                "KeySchema": [
                    { "AttributeName": "pk", "KeyType": "HASH" },
                    { "AttributeName": "lsk", "KeyType": "RANGE" },
                ],
                "Projection": { "ProjectionType": "KEYS_ONLY" },
                "ProvisionedThroughput": { "ReadCapacityUnits": 1, "WriteCapacityUnits": 1 },
            }] }),
            "index lsi1: member ProvisionedThroughput is not supported",
        ),
        (
            "an LSI the backup does not have",
            json!({ "LocalSecondaryIndexOverride": [{
                "IndexName": "lsi2",
                "KeySchema": [
                    { "AttributeName": "pk", "KeyType": "HASH" },
                    { "AttributeName": "lsk", "KeyType": "RANGE" },
                ],
                "Projection": { "ProjectionType": "KEYS_ONLY" },
            }] }),
            "local secondary index lsi2 is not in the backup",
        ),
        (
            "an LSI listed twice",
            json!({ "LocalSecondaryIndexOverride": [lsi_entry(), lsi_entry()] }),
            "local secondary index lsi1 is listed more than once",
        ),
    ];
    for (what, body, expect) in cases {
        let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
        let msg = validation_message(restore(&fake, restore_body(body)).await.unwrap_err());
        assert!(msg.contains(expect), "{what}: {msg}");
        assert!(fake.restores().is_empty(), "{what}: reached storage");
    }

    // A legacy backup restores no index, so any listed one is not in it; an
    // empty list changes nothing.
    let fake = Fake::with_source(source(None));
    let msg = validation_message(
        restore(
            &fake,
            restore_body(json!({ "GlobalSecondaryIndexOverride": [gsi_entry(Some((1, 1)))] })),
        )
        .await
        .unwrap_err(),
    );
    assert!(
        msg.ends_with("global secondary index gsi1 is not in the backup"),
        "{msg}"
    );
    let plain = restored_input(source(None), json!({})).await.unwrap();
    let empty = restored_input(
        source(None),
        json!({ "GlobalSecondaryIndexOverride": [], "LocalSecondaryIndexOverride": [] }),
    )
    .await
    .unwrap();
    assert_eq!(empty, plain);
}

/// Measured on DynamoDB: the two requests the service refuses and ExtendDB
/// accepted: a GSI entry without throughput on a PROVISIONED restore, and
/// `BillingModeOverride` `PROVISIONED` with no GSI list on a backup with
/// GSIs, both with the service's text. The service's text conditions on the
/// override being present, so the rule applies whatever the backup's billing
/// mode and whatever throughput it recorded for the index (the PROVISIONED
/// backup and stale-throughput cases were not measured). The
/// on-demand-to-PROVISIONED route works when the list supplies each index's
/// throughput.
#[tokio::test]
async fn a_provisioned_restore_needs_each_gsis_throughput_overridden() {
    let expected = "Invalid Request: Must override ProvisionedThroughput if BillingMode is \
                    overridden to PROVISIONED for index gsi1";
    let provisioned = json!({
        "BillingModeOverride": "PROVISIONED",
        "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
    });
    let with = |extra: Value| {
        let mut body = provisioned.clone();
        for (k, v) in extra.as_object().unwrap() {
            body[k] = v.clone();
        }
        body
    };
    // An on-demand backup whose GSI still carries a throughput from before a
    // billing switch (what UpdateTable to PAY_PER_REQUEST leaves behind).
    let mut stale = definition("PAY_PER_REQUEST");
    stale.global_secondary_indexes[0].provisioned_throughput = Some(throughput(3, 4));
    let refused: [(&str, BackupTableDefinition, Value); 6] = [
        (
            "no GSI list, on-demand backup",
            definition("PAY_PER_REQUEST"),
            provisioned.clone(),
        ),
        (
            "no GSI list, PROVISIONED backup (the override, not the recorded throughput, decides)",
            definition("PROVISIONED"),
            provisioned.clone(),
        ),
        (
            "no GSI list, on-demand backup with a stale recorded throughput",
            stale,
            provisioned.clone(),
        ),
        (
            "a GSI entry without throughput, on-demand backup",
            definition("PAY_PER_REQUEST"),
            with(json!({ "GlobalSecondaryIndexOverride": [gsi_entry(None)] })),
        ),
        (
            "a GSI entry without throughput, PROVISIONED backup with the override",
            definition("PROVISIONED"),
            with(json!({ "GlobalSecondaryIndexOverride": [gsi_entry(None)] })),
        ),
        (
            "a GSI entry without throughput, PROVISIONED backup without the override",
            definition("PROVISIONED"),
            json!({ "GlobalSecondaryIndexOverride": [gsi_entry(None)] }),
        ),
    ];
    for (what, def, body) in refused {
        let fake = Fake::with_source(source(Some(def)));
        let msg = validation_message(restore(&fake, restore_body(body)).await.unwrap_err());
        assert_eq!(msg, expected, "{what}");
        assert!(fake.restores().is_empty(), "{what}: reached storage");
    }
    // The first GSI of two is the one named.
    let fake = Fake::with_source(source(Some(two_gsis("PAY_PER_REQUEST"))));
    let msg = validation_message(
        restore(&fake, restore_body(provisioned.clone()))
            .await
            .unwrap_err(),
    );
    assert_eq!(msg, expected);

    // The route that works: the list supplies the throughput.
    let input = restored_input(
        source(Some(two_gsis("PAY_PER_REQUEST"))),
        with(json!({ "GlobalSecondaryIndexOverride": [
            gsi_entry(Some((7, 8))),
            gsi2_entry(json!(["a", "b"]), Some((9, 10))),
        ] })),
    )
    .await
    .unwrap();
    assert_eq!(input.billing_mode, Some(BillingMode::Provisioned));
    assert_eq!(input.provisioned_throughput, Some(throughput(5, 5)));
    assert_eq!(gsi_throughputs(&input), [Some((7, 8)), Some((9, 10))]);
    extenddb_core::validation::validate_create_table(&input, &LimitsConfig::default()).unwrap();

    // A PROVISIONED backup restored with no override keeps its recorded GSI
    // throughput; only the override asks for the list.
    let input = restored_input(source(Some(definition("PROVISIONED"))), json!({}))
        .await
        .unwrap();
    assert_eq!(gsi_throughputs(&input), [Some((3, 4))]);
    // And with no GSIs, the billing override to PROVISIONED needs nothing.
    let mut none = definition("PAY_PER_REQUEST");
    none.global_secondary_indexes.clear();
    let mut src = source(Some(none));
    src.attribute_definitions
        .retain(|a| a.attribute_name != "gpk");
    let input = restored_input(src, provisioned).await.unwrap();
    assert!(input.global_secondary_indexes.is_none());
    assert_eq!(input.provisioned_throughput, Some(throughput(5, 5)));
}

fn on_demand(read: Option<i64>, write: Option<i64>) -> OnDemandThroughput {
    OnDemandThroughput {
        max_read_request_units: read,
        max_write_request_units: write,
    }
}

/// Measured on DynamoDB: the service applies `OnDemandThroughputOverride` on an
/// on-demand restore, changed or with one member, including on a PROVISIONED
/// backup restored as on-demand; a GSI entry's `OnDemandThroughput` is
/// accepted (and, ExtendDB's choice, not recorded). On a PROVISIONED restore
/// the member contradicts the billing mode and is refused.
#[tokio::test]
async fn an_on_demand_throughput_override_is_applied_on_an_on_demand_restore() {
    let with = |recorded: Option<OnDemandThroughput>| {
        let mut def = definition("PAY_PER_REQUEST");
        def.on_demand_throughput = recorded;
        source(Some(def))
    };
    let applied: [(
        Option<OnDemandThroughput>,
        Value,
        Option<OnDemandThroughput>,
    ); 6] = [
        (
            Some(on_demand(Some(50), Some(60))),
            json!({ "MaxReadRequestUnits": 50, "MaxWriteRequestUnits": 60 }),
            Some(on_demand(Some(50), Some(60))),
        ),
        (
            Some(on_demand(Some(50), Some(60))),
            json!({ "MaxReadRequestUnits": 50, "MaxWriteRequestUnits": 61 }),
            Some(on_demand(Some(50), Some(61))),
        ),
        (
            None,
            json!({ "MaxReadRequestUnits": 10 }),
            Some(on_demand(Some(10), None)),
        ),
        (
            Some(on_demand(Some(50), Some(60))),
            json!({ "MaxWriteRequestUnits": -1 }),
            Some(on_demand(None, Some(-1))),
        ),
        (
            None,
            json!({ "MaxReadRequestUnits": -1, "MaxWriteRequestUnits": -1 }),
            Some(on_demand(Some(-1), Some(-1))),
        ),
        // An empty override records no maxima, as CreateTable without the
        // member does.
        (Some(on_demand(Some(50), Some(60))), json!({}), None),
    ];
    for (recorded, given, expected) in applied {
        let input = restored_input(
            with(recorded.clone()),
            json!({ "OnDemandThroughputOverride": given }),
        )
        .await
        .unwrap_or_else(|e| panic!("{recorded:?} {given}: {e:?}"));
        assert_eq!(input.on_demand_throughput, expected, "{recorded:?} {given}");
        assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
    }
    // Without the member the backup's is restored.
    let input = restored_input(with(Some(on_demand(Some(50), Some(60)))), json!({}))
        .await
        .unwrap();
    assert_eq!(
        input.on_demand_throughput,
        Some(on_demand(Some(50), Some(60)))
    );

    // A PROVISIONED backup restored as on-demand takes it.
    let input = restored_input(
        source(Some(definition("PROVISIONED"))),
        json!({
            "BillingModeOverride": "PAY_PER_REQUEST",
            "OnDemandThroughputOverride": { "MaxReadRequestUnits": 7, "MaxWriteRequestUnits": 8 },
        }),
    )
    .await
    .unwrap();
    assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
    assert!(input.provisioned_throughput.is_none());
    assert_eq!(
        input.on_demand_throughput,
        Some(on_demand(Some(7), Some(8)))
    );

    // A GSI entry's OnDemandThroughput on an on-demand restore: accepted, the
    // index restored without a maximum (ExtendDB records none per index).
    let plain = restored_input(source(Some(definition("PAY_PER_REQUEST"))), json!({}))
        .await
        .unwrap();
    let input = restored_input(
        source(Some(definition("PAY_PER_REQUEST"))),
        json!({ "GlobalSecondaryIndexOverride": [{
            "IndexName": "gsi1",
            "KeySchema": [{ "AttributeName": "gpk", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" },
            "OnDemandThroughput": { "MaxReadRequestUnits": 10, "MaxWriteRequestUnits": -1 },
        }] }),
    )
    .await
    .unwrap();
    assert_eq!(input, plain);

    // Refused: an unknown member, and the member on a PROVISIONED restore
    // (from a PROVISIONED backup or by override).
    let fake = Fake::with_source(with(None));
    let msg = validation_message(
        restore(
            &fake,
            restore_body(json!({ "OnDemandThroughputOverride": {
                "MaxReadRequestUnits": 50, "MaxWriteRequestUnits": 60, "Other": 1,
            } })),
        )
        .await
        .unwrap_err(),
    );
    assert!(
        msg.ends_with("OnDemandThroughputOverride: member Other is not supported"),
        "{msg}"
    );
    assert!(fake.restores().is_empty());
    for (def, body) in [
        (
            definition("PROVISIONED"),
            json!({ "OnDemandThroughputOverride": { "MaxReadRequestUnits": 1 } }),
        ),
        (
            definition("PAY_PER_REQUEST"),
            json!({
                "OnDemandThroughputOverride": {},
                "BillingModeOverride": "PROVISIONED",
                "ProvisionedThroughputOverride": { "ReadCapacityUnits": 1, "WriteCapacityUnits": 1 },
            }),
        ),
    ] {
        let mut def = def;
        def.global_secondary_indexes.clear();
        let fake = Fake::with_source(source(Some(def)));
        let msg = validation_message(restore(&fake, restore_body(body)).await.unwrap_err());
        assert!(
            msg.ends_with(
                "OnDemandThroughputOverride can only be specified when the restored table's \
                 BillingMode is PAY_PER_REQUEST"
            ),
            "{msg}"
        );
        assert!(fake.restores().is_empty());
    }
}

/// Measured on DynamoDB: the service restores `STANDARD` whatever the source's
/// table class; the recorded class is for DescribeBackup only.
#[tokio::test]
async fn table_class_is_not_restored() {
    let mut def = definition("PROVISIONED");
    def.table_class = Some("STANDARD_INFREQUENT_ACCESS".to_owned());
    let input = restored_input(source(Some(def)), json!({})).await.unwrap();
    assert_eq!(input.table_class, None);
}

#[tokio::test]
async fn sse_override_replaces_the_recorded_specification() {
    let mut def = definition("PROVISIONED");
    def.sse_specification = Some(json!({ "Enabled": false }));
    let fake = Fake::with_source(source(Some(def)));
    let sse = json!({ "Enabled": true, "SSEType": "KMS", "KMSMasterKeyId": "alias/k" });
    restore(
        &fake,
        restore_body(json!({ "SSESpecificationOverride": sse })),
    )
    .await
    .unwrap();
    assert_eq!(fake.restores()[0].input.sse_specification, Some(sse));

    // A legacy backup takes the override too.
    let fake = Fake::with_source(source(None));
    restore(
        &fake,
        restore_body(json!({ "SSESpecificationOverride": { "Enabled": true } })),
    )
    .await
    .unwrap();
    assert_eq!(
        fake.restores()[0].input.sse_specification,
        Some(json!({ "Enabled": true }))
    );

    let body = restore_body(json!({ "SSESpecificationOverride": "on" }));
    assert!(matches!(
        parse_restore_table_overrides(&body).unwrap_err(),
        DynamoDbError::SerializationException(_)
    ));
}

#[tokio::test]
async fn billing_overrides_shape_the_restore_input() {
    // PROVISIONED -> PAY_PER_REQUEST drops table and GSI throughput.
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    let out = restore(
        &fake,
        restore_body(json!({ "BillingModeOverride": "PAY_PER_REQUEST" })),
    )
    .await
    .unwrap();
    let input = &fake.restores()[0].input;
    assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
    assert!(input.provisioned_throughput.is_none());
    assert!(
        input.global_secondary_indexes.as_ref().unwrap()[0]
            .provisioned_throughput
            .is_none()
    );
    let td = &out["TableDescription"];
    assert_eq!(td["ProvisionedThroughput"]["ReadCapacityUnits"], 0);
    assert_eq!(
        td["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"]["ReadCapacityUnits"],
        0
    );

    // A throughput override on a PROVISIONED backup replaces the table's.
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    restore(
        &fake,
        restore_body(json!({
            "ProvisionedThroughputOverride": { "ReadCapacityUnits": 11, "WriteCapacityUnits": 12 },
        })),
    )
    .await
    .unwrap();
    let input = &fake.restores()[0].input;
    assert_eq!(input.provisioned_throughput, Some(throughput(11, 12)));
    assert_eq!(
        input.global_secondary_indexes.as_ref().unwrap()[0].provisioned_throughput,
        Some(throughput(3, 4))
    );

    // A throughput override on an on-demand backup without a billing override.
    let fake = Fake::with_source(source(Some(definition("PAY_PER_REQUEST"))));
    let err = restore(
        &fake,
        restore_body(json!({
            "ProvisionedThroughputOverride": { "ReadCapacityUnits": 1, "WriteCapacityUnits": 1 },
        })),
    )
    .await
    .unwrap_err();
    assert!(validation_message(err).contains("can only be specified"));
    assert!(fake.restores().is_empty());
}

#[tokio::test]
async fn provisioned_override_on_an_on_demand_gsi_has_the_services_text() {
    let fake = Fake::with_source(source(Some(definition("PAY_PER_REQUEST"))));
    let err = restore(
        &fake,
        restore_body(json!({
            "BillingModeOverride": "PROVISIONED",
            "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
        })),
    )
    .await
    .unwrap_err();
    assert_eq!(
        validation_message(err),
        "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden to \
         PROVISIONED for index gsi1"
    );
    assert!(fake.restores().is_empty());
}

#[tokio::test]
async fn override_shape_is_refused_before_any_storage_read() {
    let fake = Fake::default();
    let err = restore(
        &fake,
        restore_body(json!({ "BillingModeOverride": "PROVISIONED" })),
    )
    .await
    .unwrap_err();
    assert!(validation_message(err).contains("ProvisionedThroughputOverride must"));
    assert_eq!(fake.source_reads(), 0);
}

// --- refusals and validation --------------------------------------------------

#[tokio::test]
async fn multi_part_key_backups_are_refused_in_the_engine() {
    let mut src = source(Some(definition("PROVISIONED")));
    src.key_schema.push(key("pk2", KeyType::Hash));
    let fake = Fake::with_source(src);
    let msg = validation_message(restore(&fake, restore_body(json!({}))).await.unwrap_err());
    assert!(msg.contains("multi-part key"), "{msg}");
    assert!(fake.restores().is_empty());
}

#[tokio::test]
async fn vector_index_backups_are_refused_in_the_engine() {
    let mut def = definition("PAY_PER_REQUEST");
    def.vector_index_names.push("v".to_owned());
    let fake = Fake::with_source(source(Some(def)));
    let msg = validation_message(restore(&fake, restore_body(json!({}))).await.unwrap_err());
    assert!(msg.contains("vector index"), "{msg}");
    assert!(fake.restores().is_empty());

    let mut legacy = source(None);
    legacy.legacy_vector_index_count = 1;
    let fake = Fake::with_source(legacy);
    let msg = validation_message(restore(&fake, restore_body(json!({}))).await.unwrap_err());
    assert!(msg.contains("vector index"), "{msg}");
    assert!(fake.restores().is_empty());
}

#[tokio::test]
async fn a_stored_definition_create_table_would_refuse_is_refused() {
    type Corrupt = fn(&mut BackupRestoreSource);
    let cases: [(&str, Corrupt); 7] = [
        (
            "101 projected attributes split across a GSI and an LSI",
            |s| {
                let d = s.definition.as_mut().unwrap();
                d.global_secondary_indexes[0].projection = include(0..60);
                d.local_secondary_indexes[0].projection = include(60..101);
            },
        ),
        ("GSI on an undefined attribute", |s| {
            let d = s.definition.as_mut().unwrap();
            d.global_secondary_indexes[0].key_schema[0].attribute_name = "nowhere".to_owned();
        }),
        ("negative GSI throughput", |s| {
            let d = s.definition.as_mut().unwrap();
            d.global_secondary_indexes[0].provisioned_throughput = Some(throughput(-1, 1));
        }),
        ("GSI throughput with one unit at 0", |s| {
            let d = s.definition.as_mut().unwrap();
            d.global_secondary_indexes[0].provisioned_throughput = Some(throughput(0, 1));
        }),
        ("negative table throughput", |s| {
            s.definition.as_mut().unwrap().provisioned_throughput = Some(throughput(1, -1));
        }),
        ("too many GSIs", |s| {
            let d = s.definition.as_mut().unwrap();
            d.global_secondary_indexes = (0..30)
                .map(|i| gsi(&format!("g{i}"), "gpk", Some(throughput(1, 1))))
                .collect();
        }),
        ("too many projected attributes", |s| {
            let d = s.definition.as_mut().unwrap();
            d.global_secondary_indexes[0].projection = Projection {
                projection_type: ProjectionType::Include,
                non_key_attributes: Some((0..101).map(|i| format!("a{i}")).collect()),
            };
        }),
    ];
    for (what, corrupt) in cases {
        let mut src = source(Some(definition("PROVISIONED")));
        corrupt(&mut src);
        let fake = Fake::with_source(src);
        let err = restore(&fake, restore_body(json!({}))).await.unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(_)),
            "{what}: {err:?}"
        );
        assert!(fake.restores().is_empty(), "{what}: reached storage");
    }
}

fn include(range: std::ops::Range<usize>) -> Projection {
    Projection {
        projection_type: ProjectionType::Include,
        non_key_attributes: Some(range.map(|i| format!("a{i}")).collect()),
    }
}

#[tokio::test]
async fn one_hundred_projected_attributes_are_accepted() {
    let mut src = source(Some(definition("PROVISIONED")));
    let d = src.definition.as_mut().unwrap();
    d.global_secondary_indexes[0].projection = include(0..60);
    d.local_secondary_indexes[0].projection = include(60..100);
    let fake = Fake::with_source(src);
    restore(&fake, restore_body(json!({}))).await.unwrap();
    assert_eq!(fake.restores().len(), 1);
}

/// A PROVISIONED table whose GSI has no throughput (CreateTable accepts one)
/// or 0/0 (what UpdateTable to PROVISIONED leaves on an on-demand table's
/// GSIs) restores with no override, the GSI taking the target table's
/// throughput. With `BillingModeOverride` `PROVISIONED` and no GSI list it is
/// refused like any backup with a GSI (the override, not the recorded
/// throughput, decides); the list route restores it.
#[tokio::test]
async fn a_table_create_table_accepted_restores() {
    // The CreateTable half: the shape is accepted there.
    let mut created = source(Some(definition("PROVISIONED")))
        .restore_input("Music", &RestoreTableOverrides::default());
    created.global_secondary_indexes.as_mut().unwrap()[0].provisioned_throughput = None;
    extenddb_core::validation::validate_create_table(&created, &LimitsConfig::default())
        .expect("CreateTable accepts a PROVISIONED GSI without throughput");

    let provisioned = json!({
        "BillingModeOverride": "PROVISIONED",
        "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 6 },
    });
    for recorded in [None, Some(throughput(0, 0))] {
        let src = || {
            let mut src = source(Some(definition("PROVISIONED")));
            src.definition.as_mut().unwrap().global_secondary_indexes[0].provisioned_throughput =
                recorded.clone();
            src
        };
        let fake = Fake::with_source(src());
        restore(&fake, restore_body(json!({})))
            .await
            .unwrap_or_else(|e| panic!("{recorded:?}: {e:?}"));
        let input = &fake.restores()[0].input;
        assert_eq!(
            input.global_secondary_indexes.as_ref().unwrap()[0].provisioned_throughput,
            Some(throughput(7, 8)),
            "{recorded:?}"
        );

        let fake = Fake::with_source(src());
        let msg = validation_message(
            restore(&fake, restore_body(provisioned.clone()))
                .await
                .unwrap_err(),
        );
        assert_eq!(
            msg,
            "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden \
             to PROVISIONED for index gsi1",
            "{recorded:?}"
        );
        assert!(fake.restores().is_empty());

        let mut with_list = provisioned.clone();
        with_list["GlobalSecondaryIndexOverride"] = json!([gsi_entry(Some((5, 6)))]);
        let fake = Fake::with_source(src());
        restore(&fake, restore_body(with_list))
            .await
            .unwrap_or_else(|e| panic!("{recorded:?}: {e:?}"));
        let input = &fake.restores()[0].input;
        assert_eq!(
            input.global_secondary_indexes.as_ref().unwrap()[0].provisioned_throughput,
            Some(throughput(5, 6)),
            "{recorded:?}"
        );
    }
}

#[tokio::test]
async fn a_missing_backup_is_backup_not_found() {
    let fake = Fake::default();
    assert!(matches!(
        restore(&fake, restore_body(json!({}))).await.unwrap_err(),
        DynamoDbError::BackupNotFoundException(_)
    ));
}

// --- the storage request and the response ------------------------------------

#[tokio::test]
async fn the_backend_receives_the_resolved_request() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    restore(&fake, restore_body(json!({}))).await.unwrap();
    let [request] = fake.restores().try_into().unwrap();
    assert_eq!(request.backup_arn, arn(ACCOUNT));
    assert_eq!(request.source_table_arn, source_arn());
    assert!((request.restore_date_time - CREATED).abs() < f64::EPSILON);
    let input = request.input;
    assert_eq!(input.table_name, "MusicRestored");
    assert_eq!(input.key_schema.len(), 2);
    assert_eq!(input.billing_mode, Some(BillingMode::Provisioned));
    assert_eq!(input.provisioned_throughput, Some(throughput(7, 8)));
    assert_eq!(
        input.global_secondary_indexes.unwrap()[0].provisioned_throughput,
        Some(throughput(3, 4))
    );
    assert_eq!(input.local_secondary_indexes.unwrap().len(), 1);
    assert!(input.stream_specification.is_none());
}

#[tokio::test]
async fn a_legacy_backup_restores_with_its_old_defaults() {
    let fake = Fake::with_source(source(None));
    restore(&fake, restore_body(json!({}))).await.unwrap();
    let input = &fake.restores()[0].input;
    assert_eq!(input.provisioned_throughput, Some(throughput(5, 5)));
    assert!(input.global_secondary_indexes.is_none());
    assert!(input.local_secondary_indexes.is_none());
}

#[tokio::test]
async fn the_response_matches_the_service_shape() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    let out = restore(&fake, restore_body(json!({}))).await.unwrap();
    let td = &out["TableDescription"];
    assert_eq!(td["TableStatus"], "CREATING");
    let summary = &td["RestoreSummary"];
    assert_eq!(summary["SourceBackupArn"], arn(ACCOUNT));
    assert_eq!(summary["SourceTableArn"], source_arn());
    assert_eq!(summary["RestoreDateTime"], CREATED);
    assert_eq!(summary["RestoreInProgress"], true);
    let gsi = td["GlobalSecondaryIndexes"][0].as_object().unwrap();
    assert!(!gsi.contains_key("IndexStatus"), "{gsi:?}");
    assert!(gsi.contains_key("IndexName"));
    assert!(
        td["BillingModeSummary"]
            .get("LastUpdateToPayPerRequestDateTime")
            .is_none()
    );
    assert!(
        td["TableThroughputModeSummary"]
            .get("LastUpdateToPayPerRequestDateTime")
            .is_none()
    );
    assert_eq!(
        td["TableThroughputModeSummary"]["TableThroughputMode"],
        "PROVISIONED"
    );
    // Omitted by the service here; the backend's description carries both.
    assert!(td.get("DeletionProtectionEnabled").is_none(), "{td}");
    let pt = td["ProvisionedThroughput"].as_object().unwrap();
    assert!(!pt.contains_key("NumberOfDecreasesToday"), "{pt:?}");
    assert_eq!(pt["ReadCapacityUnits"], 9);
    let backend = serde_json::to_value(described(&fake.restores()[0])).unwrap();
    assert_eq!(backend["DeletionProtectionEnabled"], false);
    assert_eq!(
        backend["ProvisionedThroughput"]["NumberOfDecreasesToday"],
        0
    );
}

#[tokio::test]
async fn a_taken_name_is_table_already_exists_on_restore_only() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    *fake.restore_error.lock().unwrap() =
        Some(StorageError::TableAlreadyExists("MusicRestored".to_owned()));
    let err = restore(&fake, restore_body(json!({}))).await.unwrap_err();
    match &err {
        DynamoDbError::TableAlreadyExistsException(m) => {
            assert_eq!(m, "Table already exists: MusicRestored");
        }
        other => panic!("expected TableAlreadyExistsException, got {other:?}"),
    }
    assert_eq!(
        err.full_error_type(),
        "com.amazonaws.dynamodb.v20120810#TableAlreadyExistsException"
    );

    // CreateTable keeps ResourceInUseException.
    assert!(matches!(
        crate::create_table::storage_err_to_dynamo(StorageError::TableAlreadyExists(
            "t".to_owned()
        )),
        DynamoDbError::ResourceInUseException(_)
    ));
}

/// Measured on DynamoDB: a target name taken by a
/// `CREATING` restore is `TableInUseException` `Table: <name> is already
/// being restored from backup: <arn>`; a backup with a restore from it still
/// `CREATING` is `BackupInUseException`. The backends raise the storage
/// errors; this is their mapping.
#[tokio::test]
async fn a_name_being_restored_is_table_in_use_and_a_backup_being_restored_is_backup_in_use() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    *fake.restore_error.lock().unwrap() = Some(StorageError::TableBeingRestored {
        table_name: "MusicRestored".to_owned(),
        backup_arn: arn(ACCOUNT),
    });
    let err = restore(&fake, restore_body(json!({}))).await.unwrap_err();
    match &err {
        DynamoDbError::TableInUseException(m) => assert_eq!(
            m,
            &format!(
                "Table: MusicRestored is already being restored from backup: {}",
                arn(ACCOUNT)
            )
        ),
        other => panic!("expected TableInUseException, got {other:?}"),
    }
    assert_eq!(
        err.full_error_type(),
        "com.amazonaws.dynamodb.v20120810#TableInUseException"
    );

    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    *fake.restore_error.lock().unwrap() = Some(StorageError::BackupInUse(format!(
        "Backup {} is being restored",
        arn(ACCOUNT)
    )));
    let err = restore(&fake, restore_body(json!({}))).await.unwrap_err();
    assert!(
        matches!(err, DynamoDbError::BackupInUseException(_)),
        "{err:?}"
    );

    // Elsewhere the name is a table in use. A plain CreateTable into the
    // name answers the service's text for a taken name (measured on
    // DynamoDB), without the backup ARN.
    assert!(matches!(
        storage_err_to_dynamo(StorageError::TableBeingRestored {
            table_name: "t".to_owned(),
            backup_arn: "a".to_owned(),
        }),
        DynamoDbError::ResourceInUseException(_)
    ));
    match crate::create_table::storage_err_to_dynamo(StorageError::TableBeingRestored {
        table_name: "MusicRestored".to_owned(),
        backup_arn: arn(ACCOUNT),
    }) {
        DynamoDbError::ResourceInUseException(m) => {
            assert_eq!(m, "Table already exists: MusicRestored");
        }
        other => panic!("expected ResourceInUseException, got {other:?}"),
    }
}

#[tokio::test]
async fn synchronous_backend_refusals_keep_their_classes() {
    for (storage, check) in [
        (
            StorageError::LimitExceeded("busy".to_owned()),
            (|e: &DynamoDbError| matches!(e, DynamoDbError::LimitExceededException(_)))
                as fn(&DynamoDbError) -> bool,
        ),
        (
            StorageError::Validation(format!("Backup not found: {}", arn(ACCOUNT))),
            |e| matches!(e, DynamoDbError::BackupNotFoundException(_)),
        ),
    ] {
        let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
        *fake.restore_error.lock().unwrap() = Some(storage);
        let err = restore(&fake, restore_body(json!({}))).await.unwrap_err();
        assert!(check(&err), "{err:?}");
    }
}

#[test]
fn an_unsupported_feature_is_a_validation_exception() {
    let msg = "restoring a table with vector indexes is not supported by this storage backend";
    assert_eq!(
        validation_message(storage_err_to_dynamo(StorageError::Unsupported(
            msg.to_owned()
        ))),
        msg
    );
}

// --- CreateBackup -------------------------------------------------------------

/// Measured on DynamoDB: `BackupName` and `TableName` are
/// constrained as the service model says, with the service's texts; both
/// constraints failing join under `2 validation errors detected:`.
#[test]
fn backup_and_table_names_are_validated_as_the_model_says() {
    let ok = ["abc", "a.b-c_D", &"x".repeat(255)];
    for name in ok {
        validate_name_member(name, "backupName", BACKUP_NAME_BOUNDS)
            .unwrap_or_else(|e| panic!("{name}: {e:?}"));
    }
    let constraint = |v: &str, path: &str, c: &str| {
        format!("Value '{v}' at '{path}' failed to satisfy constraint: {c}")
    };
    let short = "Member must have length greater than or equal to 3";
    let long = "Member must have length less than or equal to 255";
    let pattern = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+";
    let too_long = "x".repeat(256);
    let huge = "y".repeat(200 * 1024);
    let cases: Vec<(&str, &str, String)> = vec![
        (
            "",
            "backupName",
            format!(
                "1 validation error detected: {}",
                constraint("", "backupName", short)
            ),
        ),
        (
            "ab",
            "backupName",
            format!(
                "1 validation error detected: {}",
                constraint("ab", "backupName", short)
            ),
        ),
        (
            &too_long,
            "tableName",
            format!(
                "1 validation error detected: {}",
                constraint(&too_long, "tableName", long)
            ),
        ),
        (
            "a b c",
            "backupName",
            format!(
                "1 validation error detected: {}",
                constraint("a b c", "backupName", pattern)
            ),
        ),
        (
            "abc\u{0}",
            "backupName",
            format!(
                "1 validation error detected: {}",
                constraint("abc\u{0}", "backupName", pattern)
            ),
        ),
        (
            "b\u{e9}k",
            "backupName",
            format!(
                "1 validation error detected: {}",
                constraint("b\u{e9}k", "backupName", pattern)
            ),
        ),
        (
            "a!",
            "backupName",
            format!(
                "2 validation errors detected: {}; {}",
                constraint("a!", "backupName", short),
                constraint("a!", "backupName", pattern)
            ),
        ),
    ];
    for (value, path, expected) in cases {
        assert_eq!(
            validation_message(validate_name_member(value, path, BACKUP_NAME_BOUNDS).unwrap_err()),
            expected,
            "{value:?}"
        );
    }
    // A 200 KB name with a NUL byte (a 500 on Cassandra and PostgreSQL
    // before the check) is refused with both constraints named.
    let bad = format!("{huge}\u{0}");
    let msg = validation_message(
        validate_name_member(&bad, "backupName", BACKUP_NAME_BOUNDS).unwrap_err(),
    );
    assert!(
        msg.starts_with("2 validation errors detected: "),
        "{}",
        &msg[..60]
    );
    assert!(msg.contains(long) && msg.contains(pattern));

    // Through the handlers' members.
    let body = json!({ "TableName": "ok_table", "BackupName": "x" });
    let name = create_backup_table_name(&body, ACCOUNT, REGION, &limits()).unwrap();
    assert_eq!(name, "ok_table");
    assert!(matches!(
        validate_name_member(body["BackupName"].as_str().unwrap(), "backupName", BACKUP_NAME_BOUNDS)
            .unwrap_err(),
        DynamoDbError::ValidationException(m) if m.contains("'backupName'")
    ));
    let msg = validation_message(
        create_backup_table_name(&json!({ "TableName": "a*" }), ACCOUNT, REGION, &limits())
            .unwrap_err(),
    );
    assert!(
        msg.contains("'tableName'") && msg.contains(pattern),
        "{msg}"
    );
}

/// Length is counted in characters, as the model's bounds are: a two-byte
/// character is one. `TableName` members take the deployment's table-name
/// limits (`CreateBackup` and `ListBackups` agree with `CreateTable`);
/// `BackupName` keeps the model's 3 to 255.
#[tokio::test]
async fn name_lengths_are_counted_in_characters_and_table_names_take_the_limits() {
    let pattern = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+";
    // Two characters, four bytes: too short, and not in the pattern (two
    // clauses, not one).
    let msg = validation_message(
        validate_name_member("\u{e9}\u{e9}", "backupName", BACKUP_NAME_BOUNDS).unwrap_err(),
    );
    assert!(msg.starts_with("2 validation errors detected: "), "{msg}");
    assert!(msg.contains("length greater than or equal to 3"), "{msg}");
    assert!(msg.contains(pattern), "{msg}");
    // 255 two-byte characters (510 bytes): within the length bound, so only
    // the pattern clause.
    let wide = "\u{e9}".repeat(255);
    let msg = validation_message(
        validate_name_member(&wide, "backupName", BACKUP_NAME_BOUNDS).unwrap_err(),
    );
    assert_eq!(
        msg,
        format!(
            "1 validation error detected: Value '{wide}' at 'backupName' failed to satisfy \
             constraint: {pattern}"
        )
    );

    // A deployment that raised the maximum: a 300-character table can be
    // backed up and listed; the backup name stays at 255.
    let raised = LimitsConfig {
        max_table_name_length: 300,
        min_table_name_length: 2,
        ..Default::default()
    };
    let long = "t".repeat(300);
    assert_eq!(
        create_backup_table_name(&json!({ "TableName": long }), ACCOUNT, REGION, &raised).unwrap(),
        long
    );
    assert_eq!(
        create_backup_table_name(&json!({ "TableName": "ab" }), ACCOUNT, REGION, &raised).unwrap(),
        "ab"
    );
    let arn_form = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{long}");
    assert_eq!(
        create_backup_table_name(&json!({ "TableName": arn_form }), ACCOUNT, REGION, &raised)
            .unwrap(),
        long
    );
    let fake = fake_with_summaries(vec![]);
    list_backups(
        &fake,
        ACCOUNT,
        REGION,
        &raised,
        &json!({ "TableName": long }),
    )
    .await
    .unwrap();
    assert_eq!(
        fake.listed_for.lock().unwrap()[0].as_deref(),
        Some(long.as_str())
    );
    // The defaults still refuse it, with the bound in the text.
    let msg = validation_message(
        create_backup_table_name(&json!({ "TableName": long }), ACCOUNT, REGION, &limits())
            .unwrap_err(),
    );
    assert!(msg.contains("length less than or equal to 255"), "{msg}");
    let msg = validation_message(
        list_backups(
            &fake,
            ACCOUNT,
            REGION,
            &limits(),
            &json!({ "TableName": long }),
        )
        .await
        .unwrap_err(),
    );
    assert!(msg.contains("length less than or equal to 255"), "{msg}");
    // A lowered maximum is reported with its own number.
    let lowered = LimitsConfig {
        max_table_name_length: 10,
        ..Default::default()
    };
    let msg = validation_message(
        create_backup_table_name(
            &json!({ "TableName": "elevenchars" }),
            ACCOUNT,
            REGION,
            &lowered,
        )
        .unwrap_err(),
    );
    assert!(msg.contains("length less than or equal to 10"), "{msg}");
    let msg = validation_message(
        validate_name_member(&"b".repeat(300), "backupName", BACKUP_NAME_BOUNDS).unwrap_err(),
    );
    assert!(msg.contains("length less than or equal to 255"), "{msg}");
}

/// Measured on DynamoDB: `CreateBackup` accepts the
/// table's ARN as `TableName`. Another account's is refused as a backup ARN
/// of another account is (ExtendDB's choice, not measured); another region's,
/// or the server's region in another case, is the service's
/// `ValidationException` naming the ARN (measured on DynamoDB, twice each).
#[test]
fn create_backup_table_name_may_be_the_tables_arn() {
    let own = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music");
    assert_eq!(
        create_backup_table_name(&json!({ "TableName": own }), ACCOUNT, REGION, &limits()).unwrap(),
        "Music"
    );
    for other_region in [
        format!("arn:aws:dynamodb:us-west-2:{ACCOUNT}:table/Music"),
        format!(
            "arn:aws:dynamodb:{}:{ACCOUNT}:table/Music",
            REGION.to_ascii_uppercase()
        ),
    ] {
        let err = create_backup_table_name(
            &json!({ "TableName": other_region }),
            ACCOUNT,
            REGION,
            &limits(),
        )
        .unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m)
                if *m == format!("1 validation error detected: Invalid AWS region in '{other_region}'")),
            "{other_region}: {err:?}"
        );
    }
    let foreign = format!("arn:aws:dynamodb:{REGION}:999999999999:table/Music");
    assert!(matches!(
        create_backup_table_name(&json!({ "TableName": foreign }), ACCOUNT, REGION, &limits())
            .unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));
    for bad in [
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bc"),
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/index/i"),
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:index/Music"),
        format!("arn:aws:s3:{REGION}:{ACCOUNT}:table/Music"),
        format!("arn:aws-cn:dynamodb:{REGION}:{ACCOUNT}:table/Music"),
        format!("arn:aws:dynamodb::{ACCOUNT}:table/Music"),
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}"),
        "arn:".to_owned(),
    ] {
        let msg = validation_message(
            create_backup_table_name(&json!({ "TableName": bad }), ACCOUNT, REGION, &limits())
                .unwrap_err(),
        );
        assert_eq!(
            msg, "Invalid Request: TableName is not a valid table name or table ARN",
            "{bad}"
        );
    }
    // The name part is checked as a name.
    let short = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/ab");
    let msg = validation_message(
        create_backup_table_name(&json!({ "TableName": short }), ACCOUNT, REGION, &limits())
            .unwrap_err(),
    );
    assert!(
        msg.contains("'tableName'") && msg.contains("length greater than or equal to 3"),
        "{msg}"
    );
    // Missing: the null-member text.
    let msg = validation_message(
        create_backup_table_name(&json!({}), ACCOUNT, REGION, &limits()).unwrap_err(),
    );
    assert!(msg.contains("Value null at 'tableName'"), "{msg}");
}

/// Measured on DynamoDB: `CreateBackup` on a table that
/// does not exist or is still `CREATING` is `TableNotFoundException`
/// `Table not found: <name>`, whatever the backend's own payload says.
#[test]
fn create_backup_on_a_missing_or_creating_table_is_table_not_found() {
    for storage in [
        // PostgreSQL's payload and the bare name the other backends give.
        StorageError::TableNotFound("Table not found: Music".to_owned()),
        StorageError::TableNotFound("Music".to_owned()),
        StorageError::TableNotActive("Music".to_owned()),
    ] {
        let what = format!("{storage:?}");
        let err = create_backup_storage_err_to_dynamo(storage, "Music");
        match &err {
            DynamoDbError::TableNotFoundException(m) => {
                assert_eq!(m, "Table not found: Music", "{what}")
            }
            other => panic!("{what}: expected TableNotFoundException, got {other:?}"),
        }
        assert_eq!(
            err.full_error_type(),
            "com.amazonaws.dynamodb.v20120810#TableNotFoundException"
        );
    }
    // Other errors keep the generic mapping.
    assert!(matches!(
        create_backup_storage_err_to_dynamo(StorageError::LimitExceeded("x".to_owned()), "Music"),
        DynamoDbError::LimitExceededException(_)
    ));
    // The data plane keeps ResourceNotFoundException for a missing table.
    assert!(matches!(
        storage_err_to_dynamo(StorageError::TableNotFound("Music".to_owned())),
        DynamoDbError::ResourceNotFoundException(_)
    ));
}

// --- DeleteBackup -------------------------------------------------------------

/// Measured on DynamoDB: `DeleteBackup` answers the full
/// DescribeBackup description with `BackupStatus` `DELETED`, including the
/// required `SourceTableDetails.ProvisionedThroughput` and the
/// `SourceTableFeatureDetails` the backend's row alone does not carry.
#[tokio::test]
async fn delete_backup_answers_the_full_description_as_deleted() {
    for def in [definition("PROVISIONED"), definition("PAY_PER_REQUEST")] {
        let on_demand = def.billing_mode == "PAY_PER_REQUEST";
        let fake = fake_with_backup("AVAILABLE", source(Some(def)));
        let described = describe(&fake).await;
        let deleted = delete_backup(&fake, &runtime(), ACCOUNT, REGION, &own(&arn(ACCOUNT)))
            .await
            .unwrap();
        let deleted = serde_json::to_value(deleted).unwrap();
        assert_eq!(deleted["BackupDetails"]["BackupStatus"], "DELETED");
        assert_eq!(
            deleted["SourceTableDetails"]["ProvisionedThroughput"],
            if on_demand {
                json!({ "ReadCapacityUnits": 0, "WriteCapacityUnits": 0 })
            } else {
                json!({ "ReadCapacityUnits": 7, "WriteCapacityUnits": 8 })
            }
        );
        assert_eq!(
            deleted["SourceTableFeatureDetails"]["GlobalSecondaryIndexes"][0]["IndexName"],
            "gsi1"
        );
        assert_eq!(
            deleted["SourceTableFeatureDetails"]["LocalSecondaryIndexes"][0]["IndexName"],
            "lsi1"
        );
        // Everything but the status is what DescribeBackup said.
        let mut expected = described.clone();
        expected["BackupDetails"]["BackupStatus"] = json!("DELETED");
        assert_eq!(deleted, expected);
        // The backup is gone.
        assert!(matches!(
            delete_backup(&fake, &runtime(), ACCOUNT, REGION, &own(&arn(ACCOUNT)))
                .await
                .unwrap_err(),
            DynamoDbError::BackupNotFoundException(_)
        ));
        assert_eq!(
            fake.source_reads(),
            3,
            "one read by DescribeBackup, one per delete attempt"
        );
    }

    // A backup whose definition this build cannot read, or that does not
    // parse, is still deleted, described without feature details.
    for err in [
        StorageError::Unsupported("version 9".to_owned()),
        StorageError::Internal("does not parse".to_owned()),
    ] {
        let what = format!("{err:?}");
        let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
        *fake.source_error.lock().unwrap() = Some(err);
        let deleted = delete_backup(&fake, &runtime(), ACCOUNT, REGION, &own(&arn(ACCOUNT)))
            .await
            .unwrap_or_else(|e| panic!("{what}: {e:?}"));
        assert_eq!(deleted.backup_details.backup_status, "DELETED", "{what}");
        assert!(deleted.source_table_feature_details.is_none(), "{what}");
        assert!(
            deleted
                .source_table_details
                .provisioned_throughput
                .is_some(),
            "{what}"
        );
        assert!(
            fake.descriptions.lock().unwrap().is_empty(),
            "{what}: not deleted"
        );
    }
    // A failure to reach the database propagates, and nothing is deleted.
    let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
    *fake.source_error.lock().unwrap() = Some(StorageError::Connection("refused".to_owned()));
    assert!(matches!(
        delete_backup(&fake, &runtime(), ACCOUNT, REGION, &own(&arn(ACCOUNT)))
            .await
            .unwrap_err(),
        DynamoDbError::InternalServerError(_)
    ));
    assert_eq!(fake.descriptions.lock().unwrap().len(), 1);
}

#[test]
fn create_backup_refuses_a_table_with_vector_indexes() {
    let plain = TableDescription {
        table_name: "T".to_owned(),
        ..Default::default()
    };
    assert!(refuse_backup_of_vector_table(&plain).is_ok());

    let with_vectors: TableDescription = serde_json::from_value(json!({
        "TableName": "T",
        "KeySchema": [],
        "AttributeDefinitions": [],
        "TableStatus": "ACTIVE",
        "CreationDateTime": 0.0,
        "TableSizeBytes": 0,
        "ItemCount": 0,
        "TableArn": "",
        "TableId": "",
        "ProvisionedThroughput": { "ReadCapacityUnits": 0, "WriteCapacityUnits": 0,
                                   "NumberOfDecreasesToday": 0 },
        "DeletionProtectionEnabled": false,
        "VectorIndexes": [{
            "IndexName": "v",
            "VectorAttribute": { "AttributeName": "emb" },
            "Dimensions": 3,
            "DistanceFunction": "COSINE",
            "IndexStatus": "ACTIVE",
            "IndexSizeBytes": 0,
            "ItemCount": 0,
            "IndexArn": ""
        }]
    }))
    .unwrap();
    let msg = validation_message(refuse_backup_of_vector_table(&with_vectors).unwrap_err());
    assert!(msg.starts_with("table T has 1 vector index(es)"), "{msg}");

    // The restore's refusal reads the same after the subject.
    let mut def = definition("PAY_PER_REQUEST");
    def.vector_index_names.push("v".to_owned());
    let restore_msg = validation_message(storage_err_to_dynamo(
        def.ensure_restorable(&arn(ACCOUNT)).unwrap_err(),
    ));
    assert_eq!(
        msg.trim_start_matches("table T"),
        restore_msg.trim_start_matches(&format!("backup {}", arn(ACCOUNT)))
    );
}

// --- DescribeBackup -----------------------------------------------------------

fn backup_description(status: &str, billing: &str) -> BackupDescription {
    BackupDescription {
        backup_details: BackupDetails {
            backup_arn: arn(ACCOUNT),
            backup_name: "b".to_owned(),
            backup_status: status.to_owned(),
            backup_type: "USER".to_owned(),
            backup_size_bytes: 30,
            backup_creation_date_time: CREATED,
        },
        source_table_details: SourceTableDetails {
            table_name: "Music".to_owned(),
            table_id: "tid".to_owned(),
            table_arn: source_arn(),
            billing_mode: Some(billing.to_owned()),
            ..Default::default()
        },
        source_table_feature_details: None,
    }
}

fn fake_with_backup(status: &str, src: BackupRestoreSource) -> Fake {
    let billing = src.billing_mode.clone();
    let fake = Fake::with_source(src);
    fake.descriptions
        .lock()
        .unwrap()
        .insert(arn(ACCOUNT), backup_description(status, &billing));
    fake
}

async fn describe(fake: &Fake) -> Value {
    serde_json::to_value(
        describe_backup(fake, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn describe_backup_reports_features_of_an_on_demand_table() {
    let mut def = definition("PAY_PER_REQUEST");
    def.time_to_live = Some(TimeToLiveDescription {
        time_to_live_status: TimeToLiveStatus::Enabled,
        attribute_name: Some("exp".to_owned()),
    });
    def.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewImage),
    });
    def.sse_specification = Some(json!({ "Enabled": true }));
    let fake = fake_with_backup("AVAILABLE", source(Some(def)));
    let v = describe(&fake).await;

    let pt = &v["SourceTableDetails"]["ProvisionedThroughput"];
    assert_eq!(
        pt,
        &json!({ "ReadCapacityUnits": 0, "WriteCapacityUnits": 0 })
    );
    let f = &v["SourceTableFeatureDetails"];
    assert_eq!(f["GlobalSecondaryIndexes"][0]["IndexName"], "gsi1");
    assert_eq!(
        f["GlobalSecondaryIndexes"][0]["KeySchema"][0]["AttributeName"],
        "gpk"
    );
    assert_eq!(
        f["GlobalSecondaryIndexes"][0]["Projection"]["ProjectionType"],
        "ALL"
    );
    assert_eq!(
        f["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"],
        json!({ "ReadCapacityUnits": 0, "WriteCapacityUnits": 0 })
    );
    assert_eq!(f["LocalSecondaryIndexes"][0]["IndexName"], "lsi1");
    assert!(
        f["LocalSecondaryIndexes"][0]
            .get("ProvisionedThroughput")
            .is_none()
    );
    assert_eq!(
        f["TimeToLiveDescription"],
        json!({ "TimeToLiveStatus": "ENABLED", "AttributeName": "exp" })
    );
    assert_eq!(
        f["StreamDescription"],
        json!({ "StreamEnabled": true, "StreamViewType": "NEW_IMAGE" })
    );
    assert_eq!(f["SSEDescription"]["Status"], "ENABLED");
    assert_eq!(
        f["SSEDescription"]["KMSMasterKeyArn"],
        format!("arn:aws:kms:{REGION}:{ACCOUNT}:key/default")
    );
}

#[tokio::test]
async fn describe_backup_reports_provisioned_throughput_and_omits_absent_members() {
    let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
    let v = describe(&fake).await;
    assert_eq!(
        v["SourceTableDetails"]["ProvisionedThroughput"],
        json!({ "ReadCapacityUnits": 7, "WriteCapacityUnits": 8 })
    );
    let f = v["SourceTableFeatureDetails"].as_object().unwrap();
    assert_eq!(
        f["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"],
        json!({ "ReadCapacityUnits": 3, "WriteCapacityUnits": 4 })
    );
    for absent in [
        "TimeToLiveDescription",
        "StreamDescription",
        "SSEDescription",
    ] {
        assert!(!f.contains_key(absent), "{absent}: {f:?}");
    }
}

#[tokio::test]
async fn describe_backup_of_a_legacy_backup_has_no_feature_details() {
    let mut src = source(None);
    src.provisioned_throughput = Some(throughput(2, 3));
    let fake = fake_with_backup("AVAILABLE", src);
    let v = describe(&fake).await;
    assert!(v.get("SourceTableFeatureDetails").is_none(), "{v}");
    assert_eq!(
        v["SourceTableDetails"]["ProvisionedThroughput"],
        json!({ "ReadCapacityUnits": 2, "WriteCapacityUnits": 3 })
    );
}

/// A definition this build cannot read (a later version) does not stop
/// DescribeBackup: the backend's BackupDetails are returned without feature
/// details. A definition that does not parse is `Internal`, and propagates as
/// a 500 like a failure to reach the database (`Transient`, `Connection`):
/// a wrong definition row is a database fault, and describing the backup as
/// on-demand `{0, 0}` without feature details would mask it as a wrong answer
/// while RestoreTableFromBackup on the same read is a 500.
/// The errors are the ones every backend returns for those cases, built by
/// `BackupTableDefinition::from_json` itself.
#[tokio::test]
async fn describe_backup_survives_a_definition_that_does_not_read() {
    let arn_text = arn(ACCOUNT);
    let undecodable = BackupTableDefinition::from_json(
        json!({ "Version": BACKUP_DEFINITION_VERSION, "BillingMode": 7 }),
        &arn_text,
    )
    .expect_err("a member of the wrong type does not parse");
    assert!(
        matches!(undecodable, StorageError::Internal(_)),
        "{undecodable:?}"
    );
    let unknown_version = BackupTableDefinition::from_json(
        json!({ "Version": BACKUP_DEFINITION_VERSION + 1 }),
        &arn_text,
    )
    .expect_err("an unknown version is refused");
    assert!(
        matches!(unknown_version, StorageError::Unsupported(_)),
        "{unknown_version:?}"
    );
    {
        let err = unknown_version;
        let what = format!("{err:?}");
        let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
        *fake.source_error.lock().unwrap() = Some(err);
        let desc = describe_backup(&fake, ACCOUNT, REGION, &own(&arn_text))
            .await
            .unwrap_or_else(|e| panic!("{what}: {e:?}"));
        assert_eq!(fake.source_reads(), 1, "{what}");
        assert_eq!(desc.backup_details.backup_status, "AVAILABLE", "{what}");
        assert_eq!(desc.backup_details.backup_arn, arn_text, "{what}");
        assert!(desc.source_table_feature_details.is_none(), "{what}");
        assert!(
            desc.source_table_details.provisioned_throughput.is_some(),
            "{what}: required member"
        );
    }

    for err in [
        undecodable,
        StorageError::Internal("column \"definition\" does not exist".to_owned()),
        StorageError::Transient(
            "Database error: pool timed out while waiting for an open connection".to_owned(),
        ),
        StorageError::Connection("connection refused".to_owned()),
    ] {
        let what = format!("{err:?}");
        let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
        *fake.source_error.lock().unwrap() = Some(err);
        // The backup operations' mapping reports both as a 500
        // (`storage_err_to_dynamo` in backup.rs), which SDKs retry.
        match describe_backup(&fake, ACCOUNT, REGION, &own(&arn_text)).await {
            Err(DynamoDbError::InternalServerError(_)) => {}
            other => panic!("{what} must reach the client as an error: {other:?}"),
        }
        assert_eq!(fake.source_reads(), 1, "{what}");
    }
}

#[tokio::test]
async fn describe_backup_of_a_backup_that_is_not_available_uses_the_backend_row() {
    let fake = fake_with_backup("CREATING", source(Some(definition("PROVISIONED"))));
    let v = describe(&fake).await;
    assert_eq!(fake.source_reads(), 0);
    assert!(v.get("SourceTableFeatureDetails").is_none());
    assert_eq!(v["BackupDetails"]["BackupStatus"], "CREATING");
    // Required member, still present.
    assert!(
        v["SourceTableDetails"]
            .get("ProvisionedThroughput")
            .is_some()
    );

    // AVAILABLE in describe, gone by the source read: described without
    // feature details rather than failing.
    let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
    fake.sources.lock().unwrap().clear();
    let v = describe(&fake).await;
    assert_eq!(fake.source_reads(), 1);
    assert!(v.get("SourceTableFeatureDetails").is_none());
}

// --- ListBackups ----------------------------------------------------------------

fn summary(n: u64, millis: u64) -> BackupSummary {
    let id = extenddb_storage::util::format_backup_id(millis, u32::try_from(n).unwrap());
    BackupSummary {
        backup_arn: extenddb_storage::util::backup_arn(REGION, ACCOUNT, "Music", &id),
        backup_name: format!("b{n}"),
        table_name: "Music".to_owned(),
        table_id: "tid".to_owned(),
        table_arn: source_arn(),
        backup_status: "AVAILABLE".to_owned(),
        backup_type: "USER".to_owned(),
        backup_size_bytes: 0,
        backup_creation_date_time: extenddb_storage::util::backup_creation_date_time(&id).unwrap(),
    }
}

fn fake_with_summaries(summaries: Vec<BackupSummary>) -> Fake {
    let fake = Fake::default();
    *fake.summaries.lock().unwrap() = summaries;
    fake
}

async fn list(fake: &Fake, body: Value) -> Result<Value, DynamoDbError> {
    list_backups(fake, ACCOUNT, REGION, &limits(), &body).await
}

fn arns(page: &Value) -> Vec<String> {
    page["BackupSummaries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["BackupArn"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn list_backups_defaults_to_one_hundred() {
    let fake = fake_with_summaries((0..150).map(|n| summary(n, 1_000 + n)).collect());
    let page = list(&fake, json!({})).await.unwrap();
    assert_eq!(arns(&page).len(), 100);
    assert_eq!(
        page["LastEvaluatedBackupArn"].as_str(),
        Some(arns(&page)[99].as_str())
    );
    assert_eq!(page["BackupSummaries"][0]["TableId"], "tid");
}

#[tokio::test]
async fn list_backups_limit_is_bounded() {
    let fake = fake_with_summaries(vec![summary(1, 1)]);
    for bad in [0, -1, 101] {
        let msg = validation_message(list(&fake, json!({ "Limit": bad })).await.unwrap_err());
        assert!(msg.contains("'limit'"), "{msg}");
    }
    assert!(matches!(
        list(&fake, json!({ "Limit": "ten" })).await.unwrap_err(),
        DynamoDbError::SerializationException(_)
    ));
    for ok in [1, 100] {
        list(&fake, json!({ "Limit": ok })).await.unwrap();
    }
}

#[tokio::test]
async fn list_backups_pages_in_a_stable_order_exactly_once() {
    // Out of order, with two backups in the same millisecond.
    let all = vec![
        summary(5, 500),
        summary(1, 100),
        summary(3, 300),
        summary(2, 300),
        summary(4, 400),
    ];
    let mut expected: Vec<BackupSummary> = all.clone();
    expected.sort_by(|a, b| {
        a.backup_creation_date_time
            .total_cmp(&b.backup_creation_date_time)
            .then_with(|| a.backup_arn.cmp(&b.backup_arn))
    });
    let expected: Vec<String> = expected.into_iter().map(|s| s.backup_arn).collect();

    let fake = fake_with_summaries(all);
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut body = json!({ "Limit": 2 });
        if let Some(c) = &cursor {
            body["ExclusiveStartBackupArn"] = json!(c);
        }
        let page = list(&fake, body).await.unwrap();
        pages += 1;
        seen.extend(arns(&page));
        match page.get("LastEvaluatedBackupArn") {
            Some(c) => cursor = Some(c.as_str().unwrap().to_owned()),
            None => break,
        }
        assert!(pages < 10, "paging did not terminate");
    }
    assert_eq!(seen, expected);
    assert_eq!(pages, 3);
}

#[tokio::test]
async fn list_backups_last_page_has_no_cursor() {
    let fake = fake_with_summaries((0..4).map(|n| summary(n, n)).collect());
    let first = list(&fake, json!({ "Limit": 2 })).await.unwrap();
    let cursor = first["LastEvaluatedBackupArn"].as_str().unwrap().to_owned();
    let second = list(
        &fake,
        json!({ "Limit": 2, "ExclusiveStartBackupArn": cursor }),
    )
    .await
    .unwrap();
    assert_eq!(arns(&second).len(), 2);
    assert!(second.get("LastEvaluatedBackupArn").is_none());

    // Exactly the limit, from the start.
    let all = list(&fake, json!({ "Limit": 4 })).await.unwrap();
    assert!(all.get("LastEvaluatedBackupArn").is_none());
    // Empty listing.
    let none = list(&fake_with_summaries(vec![]), json!({})).await.unwrap();
    assert_eq!(none, json!({ "BackupSummaries": [] }));
}

#[tokio::test]
async fn list_backups_resumes_after_a_cursor_whose_backup_was_deleted() {
    let all: Vec<BackupSummary> = (0..5).map(|n| summary(n, 100 * (n + 1))).collect();
    let deleted = all[2].backup_arn.clone();
    let remaining: Vec<BackupSummary> = all
        .iter()
        .filter(|s| s.backup_arn != deleted)
        .cloned()
        .collect();
    let fake = fake_with_summaries(remaining);
    let page = list(&fake, json!({ "ExclusiveStartBackupArn": deleted }))
        .await
        .unwrap();
    assert_eq!(
        arns(&page),
        vec![all[3].backup_arn.clone(), all[4].backup_arn.clone()]
    );

    // A deleted cursor after every remaining backup: an empty last page.
    let fake = fake_with_summaries(all[..2].to_vec());
    let page = list(&fake, json!({ "ExclusiveStartBackupArn": deleted }))
        .await
        .unwrap();
    assert!(arns(&page).is_empty());
    assert!(page.get("LastEvaluatedBackupArn").is_none());
}

#[tokio::test]
async fn list_backups_refuses_a_cursor_it_cannot_place() {
    let fake = fake_with_summaries(vec![summary(1, 1)]);
    // Well formed, ours, not listed, and its id (one of the two shapes
    // earlier builds issued: 13 digits with the hex suffix, 13 bare digits)
    // carries no time: `backup_id_epoch_millis` needs the 14-digit form.
    for id in ["1791976431438-73d8d5bc", "1791976431438"] {
        let legacy = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/{id}");
        let msg = validation_message(
            list(&fake, json!({ "ExclusiveStartBackupArn": legacy }))
                .await
                .unwrap_err(),
        );
        assert!(msg.contains("ExclusiveStartBackupArn"), "{id}: {msg}");
        assert!(msg.contains("carries no creation time"), "{id}: {msg}");
    }

    assert_eq!(
        validation_message(
            list(&fake, json!({ "ExclusiveStartBackupArn": "garbage" }))
                .await
                .unwrap_err()
        ),
        "Invalid Request: BackupArn is not valid"
    );
}

/// Measured on DynamoDB: a cursor of another account,
/// another region, or a table other than `TableName`'s is a
/// `ValidationException`, in that order; the account text is the service's,
/// the other two complete the fragments captured ("does not match the
/// region", "does not match the table name predicate").
#[tokio::test]
async fn list_backups_cursor_must_match_the_account_region_and_table() {
    let fake = fake_with_summaries(vec![summary(1, 1), summary(2, 2)]);
    let id = "01791976431438-73d8d5bc";
    let cases = [
        (
            format!("arn:aws:dynamodb:{REGION}:999999999999:table/Music/backup/{id}"),
            None,
            "account",
        ),
        (
            format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Music/backup/{id}"),
            None,
            "region",
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Other/backup/{id}"),
            Some("Music"),
            "table name predicate",
        ),
        // Account before region before table.
        (
            format!("arn:aws:dynamodb:eu-west-1:999999999999:table/Other/backup/{id}"),
            Some("Music"),
            "account",
        ),
        (
            format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Other/backup/{id}"),
            Some("Music"),
            "region",
        ),
    ];
    for (cursor, table, what) in cases {
        let mut body = json!({ "ExclusiveStartBackupArn": cursor });
        if let Some(table) = table {
            body["TableName"] = json!(table);
        }
        let msg = validation_message(list(&fake, body).await.unwrap_err());
        assert_eq!(
            msg,
            format!(
                "Invalid Request: The supplied ExclusiveStartBackupArn does not match the {what}"
            ),
            "{cursor}"
        );
    }
    // Another table's cursor without TableName is fine (the listing is the
    // account's), and a trailing newline is trimmed.
    let cursor = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Other/backup/{id}\n");
    list(&fake, json!({ "ExclusiveStartBackupArn": cursor }))
        .await
        .unwrap();
    // The cursor of the listing itself resumes after it.
    let first = summary(1, 1).backup_arn;
    let page = list(
        &fake,
        json!({ "ExclusiveStartBackupArn": format!("{first}\n"), "TableName": "Music" }),
    )
    .await
    .unwrap();
    assert_eq!(arns(&page), vec![summary(2, 2).backup_arn]);
}

/// A backup created under an earlier `server.region` keeps that region in its
/// stored ARN, and `ListBackups` hands that ARN out as the cursor; the next
/// page accepts it (trimmed). Another region's ARN that is not a listed
/// backup's is still refused.
#[tokio::test]
async fn list_backups_pages_past_a_backup_stored_under_another_region() {
    let mut old = summary(2, 2);
    old.backup_arn = old.backup_arn.replace(REGION, "eu-west-1");
    let all = vec![summary(1, 1), old.clone(), summary(3, 3)];
    let fake = fake_with_summaries(all);
    let mut seen = Vec::new();
    let mut body = json!({ "Limit": 2 });
    let first = list(&fake, body.clone()).await.unwrap();
    seen.extend(arns(&first));
    assert_eq!(
        first["LastEvaluatedBackupArn"].as_str(),
        Some(old.backup_arn.as_str())
    );
    body["ExclusiveStartBackupArn"] = json!(format!("{}\n", old.backup_arn));
    for table in [None, Some("Music")] {
        if let Some(t) = table {
            body["TableName"] = json!(t);
        }
        let second = list(&fake, body.clone()).await.unwrap();
        assert_eq!(arns(&second), vec![summary(3, 3).backup_arn], "{body}");
    }
    seen.extend(arns(&list(&fake, body.clone()).await.unwrap()));
    assert_eq!(seen.len(), 3);
    let stranger = summary(9, 9).backup_arn.replace(REGION, "eu-west-1");
    let msg = validation_message(
        list(&fake, json!({ "ExclusiveStartBackupArn": stranger }))
            .await
            .unwrap_err(),
    );
    assert_eq!(
        msg,
        "Invalid Request: The supplied ExclusiveStartBackupArn does not match the region"
    );
}

/// Measured on DynamoDB: `TableName` on `ListBackups` is checked as the service
/// model constrains it.
#[tokio::test]
async fn list_backups_table_name_is_validated() {
    let fake = fake_with_summaries(vec![summary(1, 1)]);
    let msg = validation_message(list(&fake, json!({ "TableName": "a" })).await.unwrap_err());
    assert_eq!(
        msg,
        "1 validation error detected: Value 'a' at 'tableName' failed to satisfy constraint: \
         Member must have length greater than or equal to 3"
    );
    assert!(fake.listed_for.lock().unwrap().is_empty());
    list(&fake, json!({ "TableName": "Music" })).await.unwrap();
    assert_eq!(
        fake.listed_for.lock().unwrap().as_slice(),
        [Some("Music".to_owned())]
    );
}

#[tokio::test]
async fn list_backups_passes_the_table_filter_through() {
    let fake = fake_with_summaries(vec![]);
    list(&fake, json!({ "TableName": "Music" })).await.unwrap();
    list(&fake, json!({})).await.unwrap();
    assert_eq!(
        *fake.listed_for.lock().unwrap(),
        vec![Some("Music".to_owned()), None]
    );
}

#[tokio::test]
async fn list_backups_filters_on_backup_type() {
    let mut system = summary(2, 200);
    system.backup_type = "SYSTEM".to_owned();
    let mut aws_backup = summary(3, 300);
    aws_backup.backup_type = "AWS_BACKUP".to_owned();
    let user = summary(1, 100);
    let fake = fake_with_summaries(vec![user.clone(), system.clone(), aws_backup.clone()]);
    for (filter, expected) in [
        (json!(null), vec![user.backup_arn.clone()]),
        (json!("USER"), vec![user.backup_arn.clone()]),
        (json!("SYSTEM"), vec![system.backup_arn.clone()]),
        (json!("AWS_BACKUP"), vec![aws_backup.backup_arn.clone()]),
        (
            json!("ALL"),
            vec![
                user.backup_arn.clone(),
                system.backup_arn.clone(),
                aws_backup.backup_arn.clone(),
            ],
        ),
    ] {
        let page = list(&fake, json!({ "BackupType": filter })).await.unwrap();
        assert_eq!(arns(&page), expected, "{filter}");
    }
    let page = list(&fake, json!({})).await.unwrap();
    assert_eq!(arns(&page), vec![user.backup_arn.clone()]);

    let msg = validation_message(
        list(&fake, json!({ "BackupType": "user" }))
            .await
            .unwrap_err(),
    );
    assert_eq!(
        msg,
        "1 validation error detected: Value 'user' at 'backupType' failed to satisfy \
         constraint: Member must satisfy enum value set: [USER, SYSTEM, AWS_BACKUP, ALL]"
    );
    assert!(matches!(
        list(&fake, json!({ "BackupType": 1 })).await.unwrap_err(),
        DynamoDbError::SerializationException(_)
    ));
}

#[tokio::test]
async fn list_backups_filters_on_the_time_range_before_paging() {
    // Creation times 1.000, 2.000, 3.000, 4.000, 5.000 s.
    let all: Vec<BackupSummary> = (1..=5).map(|n| summary(n, 1_000 * n)).collect();
    let arn_of = |n: usize| all[n - 1].backup_arn.clone();
    let fake = fake_with_summaries(all.clone());
    for (body, expected) in [
        // Lower bound inclusive, upper bound exclusive, at the millisecond.
        (json!({ "TimeRangeLowerBound": 2.0 }), vec![2, 3, 4, 5]),
        (json!({ "TimeRangeLowerBound": 2.001 }), vec![3, 4, 5]),
        (json!({ "TimeRangeUpperBound": 4.0 }), vec![1, 2, 3]),
        (json!({ "TimeRangeUpperBound": 4.001 }), vec![1, 2, 3, 4]),
        (
            json!({ "TimeRangeLowerBound": 2, "TimeRangeUpperBound": 4 }),
            vec![2, 3],
        ),
        (
            json!({ "TimeRangeLowerBound": 3, "TimeRangeUpperBound": 3 }),
            vec![],
        ),
        (
            json!({ "TimeRangeLowerBound": null, "TimeRangeUpperBound": null }),
            vec![1, 2, 3, 4, 5],
        ),
        (
            json!({ "TimeRangeLowerBound": 0, "TimeRangeUpperBound": 0 }),
            vec![],
        ),
    ] {
        let page = list(&fake, body.clone()).await.unwrap();
        let want: Vec<String> = expected.into_iter().map(arn_of).collect();
        assert_eq!(arns(&page), want, "{body}");
    }

    // Measured on DynamoDB: a lower bound above the upper
    // bound and a bound before the epoch are refused.
    assert_eq!(
        validation_message(
            list(
                &fake,
                json!({ "TimeRangeLowerBound": 4, "TimeRangeUpperBound": 2 })
            )
            .await
            .unwrap_err()
        ),
        "Time range lower bound must be less than or equal to upper bound"
    );
    assert_eq!(
        validation_message(
            list(
                &fake,
                json!({ "TimeRangeLowerBound": 2.5, "TimeRangeUpperBound": 2.4999 })
            )
            .await
            .unwrap_err()
        ),
        "Time range lower bound must be less than or equal to upper bound"
    );
    for member in ["TimeRangeLowerBound", "TimeRangeUpperBound"] {
        for at in [-1, -1_000_000_000] {
            let msg = validation_message(list(&fake, json!({ member: at })).await.unwrap_err());
            assert!(msg.contains(member), "{member} {at}: {msg}");
            assert!(msg.contains("1970-01-01T00:00:00Z"), "{member} {at}: {msg}");
        }
    }

    // Paging walks the filtered list: Limit counts only backups in range, and
    // the last in-range page has no cursor although later backups exist.
    let mut seen = Vec::new();
    let mut body = json!({ "TimeRangeLowerBound": 2, "TimeRangeUpperBound": 4.5, "Limit": 2 });
    let first = list(&fake, body.clone()).await.unwrap();
    seen.extend(arns(&first));
    assert_eq!(
        first["LastEvaluatedBackupArn"].as_str(),
        Some(arn_of(3).as_str())
    );
    body["ExclusiveStartBackupArn"] = first["LastEvaluatedBackupArn"].clone();
    let second = list(&fake, body).await.unwrap();
    seen.extend(arns(&second));
    assert!(
        second.get("LastEvaluatedBackupArn").is_none(),
        "the last page carries a cursor"
    );
    assert_eq!(seen, vec![arn_of(2), arn_of(3), arn_of(4)]);

    for member in ["TimeRangeLowerBound", "TimeRangeUpperBound"] {
        assert!(matches!(
            list(&fake, json!({ member: "2026-01-01" })).await.unwrap_err(),
            DynamoDbError::SerializationException(m) if m.contains(member)
        ));
    }
}

// --- the catalog index ---------------------------------------------------------

/// An ARN of the `Music` table in account `account` with backup id `id`.
fn music_arn(account: &str, id: &str) -> String {
    format!("arn:aws:dynamodb:{REGION}:{account}:table/Music/backup/{id}")
}

fn fake_with_row(status: BackupStatus) -> Fake {
    let fake = Fake::default();
    fake.rows
        .lock()
        .unwrap()
        .insert(arn(ACCOUNT), row(&arn(ACCOUNT), status));
    fake
}

fn runtime_with_store() -> BackupRuntime {
    runtime_on(&Arc::new(MemStore::default()), store())
}

/// DescribeBackup answers from the catalog row when one exists, derived from
/// the recorded source table, and never reads the legacy reader for it.
#[tokio::test]
async fn describe_backup_reads_the_catalog_row_first() {
    let fake = fake_with_row(BackupStatus::Available);
    // A legacy description under the same ARN would answer differently.
    fake.descriptions.lock().unwrap().insert(
        arn(ACCOUNT),
        backup_description("AVAILABLE", "PAY_PER_REQUEST"),
    );
    let desc = describe_backup(&fake, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
        .await
        .unwrap();
    assert_eq!(desc.backup_details.backup_name, "music-nightly");
    assert_eq!(desc.backup_details.backup_status, "AVAILABLE");
    assert_eq!(desc.backup_details.backup_size_bytes, 61828);
    assert_eq!(
        desc.source_table_details.table_id,
        "12345a12-abcd-123a-ab12-1234abc12345"
    );
    assert_eq!(
        desc.source_table_details.billing_mode.as_deref(),
        Some("PROVISIONED")
    );
    let features = desc.source_table_feature_details.expect("features");
    assert_eq!(
        features.global_secondary_indexes.unwrap()[0].index_name,
        "GenreIndex"
    );
    assert_eq!(
        fake.source_reads(),
        0,
        "the legacy reader was not consulted"
    );
}

/// The status mapping of the catalog index: CREATING as itself, DELETING as
/// DELETED, DELETED and FAILED as `BackupNotFoundException`, with no fall
/// back to the legacy reader for a tombstone.
#[tokio::test]
async fn describe_backup_maps_the_row_status() {
    let fake = fake_with_row(BackupStatus::Creating);
    let desc = describe_backup(&fake, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
        .await
        .unwrap();
    assert_eq!(desc.backup_details.backup_status, "CREATING");

    let fake = fake_with_row(BackupStatus::Deleting);
    let desc = describe_backup(&fake, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
        .await
        .unwrap();
    assert_eq!(desc.backup_details.backup_status, "DELETED");

    for status in [BackupStatus::Deleted, BackupStatus::Failed] {
        let fake = fake_with_row(status);
        fake.descriptions
            .lock()
            .unwrap()
            .insert(arn(ACCOUNT), backup_description("AVAILABLE", "PROVISIONED"));
        let err = describe_backup(&fake, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
            .await
            .unwrap_err();
        assert!(
            matches!(err, DynamoDbError::BackupNotFoundException(ref m) if m.contains(&arn(ACCOUNT))),
            "{status}: {err:?}"
        );
        assert_eq!(fake.source_reads(), 0, "{status}");
    }
}

/// An ARN of account B that carries the id of account A's backup is read by
/// B's account and the id, matches no row, and is BackupNotFound: never A's
/// row.
#[tokio::test]
async fn describe_backup_of_another_accounts_id_is_not_found() {
    const OTHER: &str = "999999999999";
    let fake = fake_with_row(BackupStatus::Available);
    let other = own_for(&music_arn(OTHER, "01791976431438-73d8d5bc"), OTHER);
    let err = describe_backup(&fake, OTHER, REGION, &other)
        .await
        .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::BackupNotFoundException(_)),
        "{err:?}"
    );
    // The lookup carried B's account, so B's own rows (none) were read.
    assert_eq!(
        fake.row_reads.lock().unwrap().as_slice(),
        [music_arn(OTHER, "01791976431438-73d8d5bc")]
    );
}

/// `own` for another account.
fn own_for(arn: &str, account: &str) -> BackupArnSpellings {
    backup_arn_field(&json!({ "BackupArn": arn }), account, REGION).unwrap()
}

/// A row stored under another region resolves under the ARN as written,
/// after the canonical spelling found nothing, as a legacy backup does.
#[tokio::test]
async fn describe_backup_tries_the_row_under_the_arn_as_written() {
    let stored =
        format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bc");
    let fake = Fake::default();
    fake.rows
        .lock()
        .unwrap()
        .insert(stored.clone(), row(&stored, BackupStatus::Available));
    let desc = describe_backup(&fake, ACCOUNT, REGION, &own(&stored))
        .await
        .unwrap();
    assert_eq!(desc.backup_details.backup_arn, stored);
    assert_eq!(
        fake.row_reads.lock().unwrap().as_slice(),
        [arn(ACCOUNT), stored.clone()],
        "canonical first, then as written"
    );
}

/// ListBackups merges the catalog's rows with the legacy reader's summaries
/// into one (creation time, ARN) order, lists CREATING and AVAILABLE rows
/// only, and pages the merged list with one cursor.
#[tokio::test]
async fn list_backups_merges_the_catalog_rows_with_the_legacy_summaries() {
    let fake = fake_with_summaries(vec![summary(1, 1_000), summary(3, 3_000)]);
    let ids = |n: u32, millis: u64| extenddb_storage::util::format_backup_id(millis, n);
    let catalog = |n: u32, millis: u64, status: BackupStatus| {
        let a = music_arn(ACCOUNT, &ids(n, millis));
        row(&a, status)
    };
    {
        let mut rows = fake.rows.lock().unwrap();
        for r in [
            catalog(2, 2_000, BackupStatus::Available),
            catalog(4, 4_000, BackupStatus::Creating),
            catalog(5, 5_000, BackupStatus::Deleting),
            catalog(6, 6_000, BackupStatus::Deleted),
            catalog(7, 7_000, BackupStatus::Failed),
        ] {
            rows.insert(r.backup_arn.clone(), r);
        }
    }
    let page = list(&fake, json!({ "Limit": 3 })).await.unwrap();
    assert_eq!(
        arns(&page),
        [
            summary(1, 1_000).backup_arn,
            music_arn(ACCOUNT, &ids(2, 2_000)),
            summary(3, 3_000).backup_arn,
        ]
    );
    let cursor = page["LastEvaluatedBackupArn"].as_str().unwrap().to_owned();
    let page = list(&fake, json!({ "ExclusiveStartBackupArn": cursor }))
        .await
        .unwrap();
    assert_eq!(arns(&page), [music_arn(ACCOUNT, &ids(4, 4_000))]);
    assert!(page.get("LastEvaluatedBackupArn").is_none());
    let creating = &page["BackupSummaries"][0];
    assert_eq!(creating["BackupStatus"], "CREATING");
    assert_eq!(creating["TableId"], "12345a12-abcd-123a-ab12-1234abc12345");
}

/// `TableName` may be the table's ARN: it lists what the name lists, and the
/// cursor's table check runs against the resolved name.
#[tokio::test]
async fn list_backups_accepts_a_table_arn_as_table_name() {
    let fake = fake_with_summaries(vec![summary(1, 1_000)]);
    let by_name = list(&fake, json!({ "TableName": "Music" })).await.unwrap();
    let by_arn = list(&fake, json!({ "TableName": source_arn() }))
        .await
        .unwrap();
    assert_eq!(arns(&by_name), arns(&by_arn));
    assert_eq!(
        fake.listed_for.lock().unwrap().as_slice(),
        [Some("Music".to_owned()), Some("Music".to_owned())]
    );
    let cursor = summary(1, 1_000).backup_arn;
    list(
        &fake,
        json!({ "TableName": source_arn(), "ExclusiveStartBackupArn": cursor }),
    )
    .await
    .unwrap();
    let other = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Other");
    let msg = validation_message(
        list(
            &fake,
            json!({ "TableName": other, "ExclusiveStartBackupArn": summary(1, 1_000).backup_arn }),
        )
        .await
        .unwrap_err(),
    );
    assert!(msg.contains("table name predicate"), "{msg}");
    let foreign = format!("arn:aws:dynamodb:{REGION}:999999999999:table/Music");
    assert!(matches!(
        list(&fake, json!({ "TableName": foreign }))
            .await
            .unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));
    // Another region's ARN, or the server's region in another case: the
    // service's ValidationException (measured on DynamoDB), not the
    // server-region table's backups.
    for other_region in [
        format!("arn:aws:dynamodb:us-west-2:{ACCOUNT}:table/Music"),
        format!(
            "arn:aws:dynamodb:{}:{ACCOUNT}:table/Music",
            REGION.to_ascii_uppercase()
        ),
    ] {
        let msg = validation_message(
            list(&fake, json!({ "TableName": other_region }))
                .await
                .unwrap_err(),
        );
        assert_eq!(
            msg,
            format!("1 validation error detected: Invalid AWS region in '{other_region}'")
        );
    }
    assert_eq!(
        fake.listed_for.lock().unwrap().len(),
        3,
        "the refused requests reached no listing"
    );
}

/// The time range is handed to the catalog as bounds and applied to the
/// legacy summaries as before, so both sides are filtered before paging.
#[tokio::test]
async fn list_backups_applies_the_time_range_to_both_sources() {
    let fake = fake_with_summaries(vec![summary(1, 1_000), summary(3, 3_000)]);
    let a2 = music_arn(ACCOUNT, &extenddb_storage::util::format_backup_id(2_000, 2));
    let a4 = music_arn(ACCOUNT, &extenddb_storage::util::format_backup_id(4_000, 4));
    {
        let mut rows = fake.rows.lock().unwrap();
        rows.insert(a2.clone(), row(&a2, BackupStatus::Available));
        rows.insert(a4.clone(), row(&a4, BackupStatus::Available));
    }
    let page = list(
        &fake,
        json!({ "TimeRangeLowerBound": 2.0, "TimeRangeUpperBound": 4.0 }),
    )
    .await
    .unwrap();
    assert_eq!(arns(&page), [a2, summary(3, 3_000).backup_arn]);
    assert_eq!(
        fake.bounds_seen.lock().unwrap().as_slice(),
        [TimeBounds {
            lower: Some(2.0),
            upper: Some(4.0),
        }],
        "the bounds reached the catalog as bounds"
    );
}

/// DeleteBackup over a catalog row, in the request's order: a tombstone is
/// BackupNotFound, a CREATING row BackupInUse, a row of another store a
/// ValidationException that names no root, and nothing reaches the row's
/// lock for any of them.
#[tokio::test]
async fn delete_backup_over_a_row_refuses_before_the_lock() {
    for status in [BackupStatus::Deleted, BackupStatus::Failed] {
        let fake = fake_with_row(status);
        let err = delete_backup(
            &fake,
            &runtime_with_store(),
            ACCOUNT,
            REGION,
            &own(&arn(ACCOUNT)),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, DynamoDbError::BackupNotFoundException(_)),
            "{status}: {err:?}"
        );
        assert!(fake.delete_starts_seen.lock().unwrap().is_empty());
    }
    let fake = fake_with_row(BackupStatus::Creating);
    let err = delete_backup(
        &fake,
        &runtime_with_store(),
        ACCOUNT,
        REGION,
        &own(&arn(ACCOUNT)),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::BackupInUseException(_)),
        "{err:?}"
    );
    assert!(fake.delete_starts_seen.lock().unwrap().is_empty());

    // No store configured: every row is a row of another store.
    let fake = fake_with_row(BackupStatus::Available);
    let msg = validation_message(
        delete_backup(&fake, &runtime(), ACCOUNT, REGION, &own(&arn(ACCOUNT)))
            .await
            .unwrap_err(),
    );
    assert!(msg.contains("store other than"), "{msg}");
    assert!(
        !msg.contains(&store().store_root),
        "no root is named: {msg}"
    );
    assert!(fake.delete_starts_seen.lock().unwrap().is_empty());
    // The legacy reader was never asked.
    assert!(fake.deleted.lock().unwrap().is_empty());
    assert_eq!(fake.source_reads(), 0);

    // The in-use check precedes the other-store check, as it does for a
    // restore: a row of another store with a CREATING target registered
    // from it (by an instance configured with that store) is BackupInUse
    // naming the target, not the other-store refusal.
    let fake = fake_with_row(BackupStatus::Available);
    fake.in_use
        .lock()
        .unwrap()
        .insert(arn(ACCOUNT), "Earlier".to_owned());
    let err = delete_backup(&fake, &runtime(), ACCOUNT, REGION, &own(&arn(ACCOUNT)))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::BackupInUseException(ref m)
            if *m == format!("Backup is being used to restore another table: {}", arn(ACCOUNT))),
        "the service's text, naming the backup and not the target: {err:?}"
    );
    assert!(fake.delete_starts_seen.lock().unwrap().is_empty());
}

/// DeleteBackup over an AVAILABLE row of the configured store: the in-use
/// check and the DELETING write run under the row's lock
/// (`start_backup_delete`), whose answer decides: a registration that won is
/// BackupInUse naming its target; a row that was CREATING under the lock is
/// BackupInUse; one that moved to a tombstone is BackupNotFound; DELETING
/// (new or resumed) answers the full description as DELETED.
#[tokio::test]
async fn delete_backup_over_a_row_runs_the_in_use_check_under_the_lock() {
    let fake = fake_with_row(BackupStatus::Available);
    let desc = delete_backup(
        &fake,
        &runtime_with_store(),
        ACCOUNT,
        REGION,
        &own(&arn(ACCOUNT)),
    )
    .await
    .unwrap();
    assert_eq!(desc.backup_details.backup_status, "DELETED");
    assert_eq!(desc.backup_details.backup_name, "music-nightly");
    assert!(desc.source_table_details.provisioned_throughput.is_some());
    assert!(desc.source_table_feature_details.is_some());
    assert_eq!(
        fake.delete_starts_seen.lock().unwrap().as_slice(),
        [BackupKey::from_arn(
            &BackupArn::parse(&arn(ACCOUNT)).unwrap()
        )]
    );

    let fake = fake_with_row(BackupStatus::Deleting);
    let desc = delete_backup(
        &fake,
        &runtime_with_store(),
        ACCOUNT,
        REGION,
        &own(&arn(ACCOUNT)),
    )
    .await
    .unwrap();
    assert_eq!(
        desc.backup_details.backup_status, "DELETED",
        "a retry resumes"
    );

    for (answer, want) in [
        (
            DeleteStart::InUse("MusicRestored".to_owned()),
            "BackupInUseException",
        ),
        (
            DeleteStart::Status(BackupStatus::Creating),
            "BackupInUseException",
        ),
        (
            DeleteStart::Status(BackupStatus::Deleted),
            "BackupNotFoundException",
        ),
        (
            DeleteStart::Status(BackupStatus::Failed),
            "BackupNotFoundException",
        ),
    ] {
        let fake = fake_with_row(BackupStatus::Available);
        fake.delete_starts
            .lock()
            .unwrap()
            .insert(arn(ACCOUNT), answer.clone());
        let err = delete_backup(
            &fake,
            &runtime_with_store(),
            ACCOUNT,
            REGION,
            &own(&arn(ACCOUNT)),
        )
        .await
        .unwrap_err();
        let got = err.full_error_type();
        assert!(got.ends_with(want), "{answer:?}: {got}");
        if let DeleteStart::InUse(_) = &answer {
            let DynamoDbError::BackupInUseException(m) = err else {
                unreachable!()
            };
            assert_eq!(
                m,
                format!(
                    "Backup is being used to restore another table: {}",
                    arn(ACCOUNT)
                ),
                "the service's text names the backup, not the target"
            );
        }
    }

    // `Status(AVAILABLE)` is an answer the contract excludes (an AVAILABLE
    // row becomes `Deleting`): a backend that gives it has broken the
    // contract, which is an internal error, not an absent backup.
    let fake = fake_with_row(BackupStatus::Available);
    fake.delete_starts
        .lock()
        .unwrap()
        .insert(arn(ACCOUNT), DeleteStart::Status(BackupStatus::Available));
    let err = delete_backup(
        &fake,
        &runtime_with_store(),
        ACCOUNT,
        REGION,
        &own(&arn(ACCOUNT)),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::InternalServerError(_)),
        "{err:?}"
    );
}

/// A row whose `source_table_details_json` this build cannot describe: the
/// listing leaves it out and lists the rest (not an internal error for the
/// whole account), and DeleteBackup answers the internal error before the
/// DELETING write, so the row stays AVAILABLE and a retry sees the same.
#[tokio::test]
async fn an_unreadable_source_table_is_left_out_of_the_listing_and_stops_a_delete_before_the_write()
{
    let readable = music_arn(ACCOUNT, &extenddb_storage::util::format_backup_id(1_000, 1));
    let unreadable = music_arn(ACCOUNT, &extenddb_storage::util::format_backup_id(2_000, 2));
    let fake = Fake::default();
    {
        let mut rows = fake.rows.lock().unwrap();
        rows.insert(readable.clone(), row(&readable, BackupStatus::Available));
        let mut damaged = row(&unreadable, BackupStatus::Available);
        damaged.source_table_details_json = json!({ "table_name": "Music" });
        rows.insert(unreadable.clone(), damaged);
    }
    let page = list(&fake, json!({})).await.unwrap();
    assert_eq!(arns(&page), std::slice::from_ref(&readable));

    let err = delete_backup(
        &fake,
        &runtime_with_store(),
        ACCOUNT,
        REGION,
        &own(&unreadable),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::InternalServerError(_)),
        "{err:?}"
    );
    assert!(
        fake.delete_starts_seen.lock().unwrap().is_empty(),
        "the description is built before the DELETING write"
    );
    assert_eq!(
        fake.rows.lock().unwrap()[&unreadable].status,
        BackupStatus::Available
    );
    // A row the summary can still read (its `table_id` and `table_arn` are
    // there, the rest is not) lists; its description is the internal error.
    fake.rows
        .lock()
        .unwrap()
        .get_mut(&unreadable)
        .unwrap()
        .source_table_details_json = json!({
        "table_id": "tid-2",
        "table_arn": source_arn(),
    });
    let page = list(&fake, json!({})).await.unwrap();
    assert_eq!(arns(&page), [readable, unreadable.clone()]);
    assert_eq!(page["BackupSummaries"][1]["TableId"], "tid-2");
    let err = describe_backup(&fake, ACCOUNT, REGION, &own(&unreadable))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::InternalServerError(_)),
        "{err:?}"
    );
}

/// A legacy backup of a table whose name the store-prefix rules refuse (a
/// name made only of dots, which main's CreateTable and CreateBackup accept;
/// a name over 255 bytes under a raised `limits.max_table_name_length`) has
/// no parsed ARN and no catalog-index read, and describes, restores and
/// deletes through the legacy reader as on main.
#[tokio::test]
async fn a_backup_of_a_name_the_store_prefix_refuses_resolves_through_the_legacy_reader() {
    for table in ["...", &"x".repeat(256)] {
        let id = "01791976431438-73d8d5bc";
        let legacy_arn = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{table}/backup/{id}");
        assert!(
            BackupArn::parse(&legacy_arn).is_err(),
            "the store-prefix rules refuse {table:?}"
        );
        let spellings = own(&legacy_arn);
        assert_eq!(spellings.canonical(), legacy_arn);
        assert_eq!(spellings.arns().count(), 0, "no parsed spelling");

        let mut src = source(Some(definition("PROVISIONED")));
        src.backup_arn = legacy_arn.clone();
        src.source_table_name = table.to_owned();
        let fake = Fake::with_source(src);
        let mut description = backup_description("AVAILABLE", "PROVISIONED");
        description.backup_details.backup_arn = legacy_arn.clone();
        description.source_table_details.table_name = table.to_owned();
        fake.descriptions
            .lock()
            .unwrap()
            .insert(legacy_arn.clone(), description);

        let desc = describe_backup(&fake, ACCOUNT, REGION, &spellings)
            .await
            .unwrap();
        assert_eq!(desc.backup_details.backup_arn, legacy_arn);
        assert_eq!(desc.source_table_details.table_name, table);
        assert!(desc.source_table_feature_details.is_some());

        let limits = LimitsConfig {
            max_table_name_length: 1024,
            ..LimitsConfig::default()
        };
        let (_, response) = restore_table_from_backup(
            &fake,
            &runtime(),
            ACCOUNT,
            REGION,
            &limits,
            &json!({ "BackupArn": legacy_arn, "TargetTableName": "Restored" }),
        )
        .await
        .unwrap();
        assert_eq!(response["TableDescription"]["TableName"], "Restored");
        assert_eq!(fake.restores().len(), 1);
        assert_eq!(fake.restores()[0].backup_arn, legacy_arn);

        let desc = delete_backup(&fake, &runtime(), ACCOUNT, REGION, &spellings)
            .await
            .unwrap();
        assert_eq!(desc.backup_details.backup_status, "DELETED");
        assert_eq!(
            fake.deleted.lock().unwrap().as_slice(),
            std::slice::from_ref(&legacy_arn)
        );
        assert!(
            fake.row_reads.lock().unwrap().is_empty(),
            "the catalog index was not read for {table:?}"
        );
    }
}

/// RestoreTableFromBackup over a catalog row, before the permit and reading
/// no store: CREATING is BackupInUse; DELETING, DELETED and FAILED are
/// BackupNotFound; a backup in use is BackupInUse naming the target, whether
/// or not the row is of the configured store (the in-use check runs before
/// the other-store check, as the delete's does); a row
/// of another store is a ValidationException, with no store configured and
/// with a different one configured; and an AVAILABLE row of the
/// configured store is refused by this release. The runtime has no free
/// slot, so a permit taken anywhere before the row's refusal, even one
/// dropped again, would turn the refusal into LimitExceeded; the legacy
/// reader is never reached.
#[tokio::test]
async fn restore_over_a_row_refuses_before_the_permit() {
    async fn check(fake: Fake, runtime: &BackupRuntime) -> DynamoDbError {
        assert_eq!(runtime.free_slots(), 0, "every slot is held");
        let err = restore_table_from_backup(
            &fake,
            runtime,
            ACCOUNT,
            REGION,
            &LimitsConfig::default(),
            &restore_body(json!({})),
        )
        .await
        .unwrap_err();
        assert!(
            !matches!(err, DynamoDbError::LimitExceededException(_)),
            "the row's refusal, not the permit's: {err:?}"
        );
        assert_eq!(fake.source_reads(), 0, "the legacy reader was not reached");
        assert!(fake.restores().is_empty());
        err
    }
    let configured = BackupRuntime::new(1)
        .with_store(ConfiguredStore::new(
            store().store_kind,
            store().store_root,
            Arc::new(MemStore::default()) as Arc<dyn extenddb_backup_store::BackupStore>,
            None,
            Ok(store()),
        ))
        .with_permit_wait(Duration::ZERO);
    let _held_configured = JobPermit::try_acquire(configured.slots()).unwrap();
    let none = BackupRuntime::new(1).with_permit_wait(Duration::ZERO);
    let _held_none = JobPermit::try_acquire(none.slots()).unwrap();
    let err = check(fake_with_row(BackupStatus::Creating), &configured).await;
    assert!(
        matches!(err, DynamoDbError::BackupInUseException(_)),
        "{err:?}"
    );
    for status in [
        BackupStatus::Deleting,
        BackupStatus::Deleted,
        BackupStatus::Failed,
    ] {
        let err = check(fake_with_row(status), &configured).await;
        assert!(
            matches!(err, DynamoDbError::BackupNotFoundException(_)),
            "{status}: {err:?}"
        );
    }
    let fake = fake_with_row(BackupStatus::Available);
    fake.in_use
        .lock()
        .unwrap()
        .insert(arn(ACCOUNT), "Earlier".to_owned());
    let err = check(fake, &configured).await;
    assert!(
        matches!(err, DynamoDbError::BackupInUseException(ref m)
            if *m == format!("Backup is being used to restore another table: {}", arn(ACCOUNT))),
        "the service's text, naming the backup and not the target: {err:?}"
    );
    // The in-use check runs before the other-store check, as the delete's
    // does: a row of another store that is in use is BackupInUse, not the
    // other-store ValidationException.
    let fake = fake_with_row(BackupStatus::Available);
    fake.in_use
        .lock()
        .unwrap()
        .insert(arn(ACCOUNT), "Earlier".to_owned());
    let err = check(fake, &none).await;
    assert!(
        matches!(err, DynamoDbError::BackupInUseException(ref m) if m.contains("another table")),
        "the in-use check runs before the other-store check: {err:?}"
    );
    let msg = validation_message(check(fake_with_row(BackupStatus::Available), &configured).await);
    assert!(msg.contains("in-catalog backups only"), "{msg}");
    let msg = validation_message(check(fake_with_row(BackupStatus::Available), &none).await);
    assert!(msg.contains("store other than"), "{msg}");
    // A configured store that is not the row's: the row is of another store,
    // not of "the" store because one is configured.
    let other = BackupRuntime::new(1)
        .with_store(ConfiguredStore::new(
            store().store_kind,
            store().store_root,
            Arc::new(MemStore::default()) as Arc<dyn extenddb_backup_store::BackupStore>,
            None,
            Ok(StoreIdentity {
                store_id: "store-2".to_owned(),
                ..store()
            }),
        ))
        .with_permit_wait(Duration::ZERO);
    let _held_other = JobPermit::try_acquire(other.slots()).unwrap();
    let msg = validation_message(check(fake_with_row(BackupStatus::Available), &other).await);
    assert!(
        msg.contains("store other than"),
        "a configured store other than the row's: {msg}"
    );
}

/// The job permit is the last check of a restore: a request refused earlier
/// takes no slot, a request that reaches the backend hands its slot to the
/// backend for the job's life, and with no slot free the request is
/// LimitExceeded after every other check passed, with the backend untouched.
#[tokio::test]
async fn restore_takes_the_permit_last() {
    async fn restore_with(
        fake: &Fake,
        runtime: &BackupRuntime,
    ) -> Result<(String, Value), DynamoDbError> {
        let body = restore_body(json!({}));
        restore_table_from_backup(
            fake,
            runtime,
            ACCOUNT,
            REGION,
            &LimitsConfig::default(),
            &body,
        )
        .await
    }
    let runtime = BackupRuntime::new(1).with_permit_wait(Duration::from_millis(50));
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    restore_with(&fake, &runtime).await.unwrap();
    assert_eq!(runtime.free_slots(), 0, "the backend holds the slot");
    assert_eq!(fake.restores().len(), 1);

    let err = restore_with(&fake, &runtime).await.unwrap_err();
    assert!(
        matches!(
            err,
            DynamoDbError::LimitExceededException(ref m)
                if m.contains("(at most 1 at once, the [backup] max_concurrent_jobs bound)")
        ),
        "the refusal names the bound: {err:?}"
    );
    assert_eq!(fake.restores().len(), 1, "the backend was not asked");
    assert_eq!(
        fake.source_reads(),
        2,
        "the source was read before the permit"
    );

    // A refusal earlier in the order takes no slot: a missing backup.
    let runtime = BackupRuntime::new(1);
    let empty = Fake::default();
    let err = restore_with(&empty, &runtime).await.unwrap_err();
    assert!(
        matches!(err, DynamoDbError::BackupNotFoundException(_)),
        "{err:?}"
    );
    assert_eq!(runtime.free_slots(), 1);

    // The backend's refusal returns the slot with the dropped permit.
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    *fake.restore_error.lock().unwrap() =
        Some(StorageError::TableAlreadyExists("MusicRestored".to_owned()));
    let err = restore_with(&fake, &runtime).await.unwrap_err();
    assert!(
        matches!(err, DynamoDbError::TableAlreadyExistsException(_)),
        "{err:?}"
    );
    assert_eq!(runtime.free_slots(), 1);

    // Dropping the backend's permit frees the slot.
    fake.permits.lock().unwrap().clear();
    assert_eq!(runtime.free_slots(), 1);
}

/// The target name is checked after the backup resolved and before the
/// overrides and the permit, the order the service uses: with every permit
/// held and the backup restorable, a restore into a name a table has is
/// TableAlreadyExists, a retry into a CREATING restore target is TableInUse
/// naming the target's source backup in the backend's own text, a CREATING
/// table with no RestoreSummary (an ordinary create) and an ACTIVE former
/// target (its summary no longer in progress) are TableAlreadyExists; the
/// backup was read once and the backend not asked. With a missing backup,
/// the same three targets answer BackupNotFound (measured on DynamoDB, twice
/// each, and on main), the name never read. A free name passes.
#[tokio::test]
async fn restore_checks_the_target_name_after_the_backup_and_before_the_permit() {
    fn table(status: TableStatus, source_backup_arn: Option<&str>) -> TableDescription {
        TableDescription {
            table_name: "MusicRestored".to_owned(),
            table_status: status,
            restore_summary: source_backup_arn.map(|arn| RestoreSummary {
                source_backup_arn: Some(arn.to_owned()),
                source_table_arn: Some(source_arn()),
                restore_date_time: CREATED,
                restore_in_progress: status == TableStatus::Creating,
            }),
            ..Default::default()
        }
    }
    async fn check(fake: Fake, target: TableDescription) -> (Fake, DynamoDbError) {
        let full = BackupRuntime::new(1).with_permit_wait(Duration::ZERO);
        let _held = full.take_permit().await.unwrap();
        fake.tables
            .lock()
            .unwrap()
            .insert("MusicRestored".to_owned(), target);
        let err = restore_table_from_backup(
            &fake,
            &full,
            ACCOUNT,
            REGION,
            &LimitsConfig::default(),
            &restore_body(json!({})),
        )
        .await
        .unwrap_err();
        assert!(
            !matches!(err, DynamoDbError::LimitExceededException(_)),
            "the request's refusal, not the permit's: {err:?}"
        );
        assert!(fake.restores().is_empty(), "the backend was not asked");
        (fake, err)
    }
    let restorable = || Fake::with_source(source(Some(definition("PROVISIONED"))));
    let targets = [
        table(TableStatus::Active, None),
        table(TableStatus::Creating, None),
        table(TableStatus::Creating, Some(&arn(ACCOUNT))),
        table(TableStatus::Active, Some(&arn(ACCOUNT))),
    ];

    // The backup restorable: the name's refusal, after one read of the backup.
    let in_use_text = format!(
        "Table: MusicRestored is already being restored from backup: {}",
        arn(ACCOUNT)
    );
    for (i, target) in targets.iter().enumerate() {
        let (fake, err) = check(restorable(), target.clone()).await;
        assert_eq!(fake.source_reads(), 1, "{i}: the backup was read first");
        assert_eq!(
            fake.table_reads.lock().unwrap().as_slice(),
            ["MusicRestored"],
            "{i}: the name was read once, under the request's spelling"
        );
        if i == 2 {
            assert!(
                matches!(err, DynamoDbError::TableInUseException(ref m) if *m == in_use_text),
                "a CREATING restore target: {err:?}"
            );
        } else {
            assert!(
                matches!(err, DynamoDbError::TableAlreadyExistsException(ref m) if m.contains("MusicRestored")),
                "{i}: {err:?}"
            );
        }
    }

    // The backup missing: BackupNotFound whatever the target's state, and
    // the name is never read.
    for (i, target) in targets.iter().enumerate() {
        let (fake, err) = check(Fake::default(), target.clone()).await;
        assert!(
            matches!(err, DynamoDbError::BackupNotFoundException(ref m)
                if *m == format!("Backup not found: {}", arn(ACCOUNT))),
            "{i}: a missing backup is BackupNotFound before the name: {err:?}"
        );
        assert!(
            fake.table_reads.lock().unwrap().is_empty(),
            "{i}: the name was not read"
        );
    }

    // A free name: the check passes and the restore reaches the backend.
    let runtime = BackupRuntime::new(1);
    let fake = restorable();
    restore_table_from_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        &restore_body(json!({})),
    )
    .await
    .unwrap();
    assert_eq!(
        fake.table_reads.lock().unwrap().as_slice(),
        ["MusicRestored"]
    );
    assert_eq!(fake.restores().len(), 1);
}

/// A request over `max_concurrent_jobs` waits for a slot up to the runtime's
/// bound (30 s in the server; shortened here) rather than refusing at once: a
/// slot freed within the bound is taken and the request proceeds, and a bound
/// that passes with every slot held is LimitExceeded naming the bound. The
/// waiting request holds nothing: the backend is asked only once it has the
/// permit.
#[tokio::test]
async fn a_request_over_the_job_bound_waits_a_bounded_time_for_a_slot() {
    let runtime = Arc::new(BackupRuntime::new(1).with_permit_wait(Duration::from_secs(5)));
    let held = runtime.take_permit().await.unwrap();
    let fake = Arc::new(Fake::with_source(source(Some(definition("PROVISIONED")))));
    let waiting = {
        let runtime = Arc::clone(&runtime);
        let fake = Arc::clone(&fake);
        tokio::spawn(async move {
            restore_table_from_backup(
                &*fake,
                &runtime,
                ACCOUNT,
                REGION,
                &LimitsConfig::default(),
                &restore_body(json!({})),
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished(), "the request waits for the slot");
    assert!(
        fake.restores().is_empty(),
        "the backend is not asked while it waits"
    );
    drop(held);
    waiting
        .await
        .unwrap()
        .expect("the request takes the freed slot and proceeds");
    assert_eq!(fake.restores().len(), 1);
    assert_eq!(runtime.free_slots(), 0, "the backend holds the slot");

    let bounded = BackupRuntime::new(1).with_permit_wait(Duration::from_millis(50));
    let _held = bounded.take_permit().await.unwrap();
    let started = tokio::time::Instant::now();
    let err = restore_table_from_backup(
        &*fake,
        &bounded,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        &restore_body(json!({})),
    )
    .await
    .unwrap_err();
    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "the request waited the bound"
    );
    assert!(
        matches!(err, DynamoDbError::LimitExceededException(ref m)
            if m.contains("(at most 1 at once, the [backup] max_concurrent_jobs bound)")),
        "{err:?}"
    );
    assert_eq!(fake.restores().len(), 1, "the backend was not asked again");
}

/// CreateBackup's checks run in the service's order, every one before the
/// permit: a missing or not-ACTIVE table is TableNotFound whether or not a
/// slot is free; a table with vector indexes is refused; then the permit,
/// LimitExceeded when none is free; then the in-catalog create holds the slot.
#[tokio::test]
async fn create_backup_checks_the_table_then_takes_the_permit_last() {
    let active = TableDescription {
        table_name: "Music".to_owned(),
        table_status: TableStatus::Active,
        ..Default::default()
    };
    let full = BackupRuntime::new(1).with_permit_wait(Duration::ZERO);
    let _held = full.take_permit().await.unwrap();
    let fake = Arc::new(Fake::default());

    let err = create_backup(
        &fake,
        &full,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Err(StorageError::TableNotFound("Music".to_owned())),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::TableNotFoundException(ref m) if m == "Table not found: Music"),
        "{err:?}"
    );
    let creating = TableDescription {
        table_status: TableStatus::Creating,
        ..active.clone()
    };
    let err = create_backup(
        &fake,
        &full,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Ok(creating),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::TableNotFoundException(_)),
        "{err:?}"
    );

    let with_vectors: TableDescription = serde_json::from_value(json!({
        "TableName": "Music",
        "KeySchema": [],
        "AttributeDefinitions": [],
        "TableStatus": "ACTIVE",
        "CreationDateTime": 0.0,
        "TableSizeBytes": 0,
        "ItemCount": 0,
        "TableArn": "",
        "TableId": "",
        "ProvisionedThroughput": { "ReadCapacityUnits": 0, "WriteCapacityUnits": 0,
                                   "NumberOfDecreasesToday": 0 },
        "DeletionProtectionEnabled": false,
        "VectorIndexes": [{
            "IndexName": "v",
            "VectorAttribute": { "AttributeName": "emb" },
            "Dimensions": 3,
            "DistanceFunction": "COSINE",
            "IndexStatus": "ACTIVE",
            "IndexSizeBytes": 0,
            "ItemCount": 0,
            "IndexArn": ""
        }]
    }))
    .unwrap();
    let msg = validation_message(
        create_backup(
            &fake,
            &full,
            ACCOUNT,
            REGION,
            &LimitsConfig::default(),
            "Music",
            "nightly",
            Ok(with_vectors),
        )
        .await
        .unwrap_err(),
    );
    assert!(msg.contains("vector"), "{msg}");

    let err = create_backup(
        &fake,
        &full,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Ok(active.clone()),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::LimitExceededException(_)),
        "{err:?}"
    );
    assert!(
        fake.creates.lock().unwrap().is_empty(),
        "the backend was not asked"
    );

    let free = BackupRuntime::new(1);
    create_backup(
        &fake,
        &free,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Ok(active),
    )
    .await
    .unwrap();
    assert_eq!(
        fake.creates.lock().unwrap().as_slice(),
        [("Music".to_owned(), "nightly".to_owned())]
    );
    assert_eq!(free.free_slots(), 0, "the backend holds the slot");
    fake.permits.lock().unwrap().clear();
    assert_eq!(free.free_slots(), 1);
}

/// A table name that fits CreateTable but not a backup ARN's store prefix (over
/// 255 bytes under a raised `limits.max_table_name_length`, or made only of
/// dots) is backed up by the in-catalog create as on main: the service backs
/// up any table it can create (measured on DynamoDB: a table named `...`
/// answers 200 with BackupStatus CREATING), and the in-catalog format writes
/// no store prefix. The store-prefix rules belong to the store-backed writer.
#[tokio::test]
async fn create_backup_accepts_a_name_the_store_prefix_refuses() {
    let fake = Arc::new(Fake::default());
    for name in ["x".repeat(256), "x".repeat(1024), "...".to_owned()] {
        let runtime = BackupRuntime::new(1);
        let desc = TableDescription {
            table_name: name.clone(),
            table_status: TableStatus::Active,
            ..Default::default()
        };
        create_backup(
            &fake,
            &runtime,
            ACCOUNT,
            REGION,
            &LimitsConfig::default(),
            &name,
            "nightly",
            Ok(desc),
        )
        .await
        .unwrap_or_else(|e| panic!("{name:?}: {e:?}"));
        assert_eq!(runtime.free_slots(), 0, "the backend holds the slot");
    }
    let created: Vec<String> = fake
        .creates
        .lock()
        .unwrap()
        .iter()
        .map(|(table, _)| table.clone())
        .collect();
    assert_eq!(
        created,
        ["x".repeat(256), "x".repeat(1024), "...".to_owned()]
    );
}

/// The store-backed create applies the store-prefix rules the in-catalog one
/// does not (`check_store_prefix_fits`): a name over 255 bytes or made only
/// of dots fits no prefix, so it is refused before a snapshot is opened or a
/// row is written, naming the rule (the length before the ARN is formed, so
/// the limit is named whatever the length).
#[tokio::test]
async fn a_store_backed_create_refuses_a_name_no_store_prefix_can_carry() {
    let fake = Arc::new(Fake::default());
    let mem = Arc::new(MemStore::default());
    let runtime = job_runtime(&mem);
    for (name, needle) in [
        ("x".repeat(256), "256 bytes"),
        ("x".repeat(1024), "1024 bytes"),
        ("...".to_owned(), "only dots"),
    ] {
        let desc = TableDescription {
            table_name: name.clone(),
            table_status: TableStatus::Active,
            ..Default::default()
        };
        let msg = validation_message(
            create_backup(
                &fake,
                &runtime,
                ACCOUNT,
                REGION,
                &LimitsConfig::default(),
                &name,
                "nightly",
                Ok(desc),
            )
            .await
            .unwrap_err(),
        );
        assert!(
            msg.contains("cannot be backed up") && msg.contains(needle),
            "{name:?}: {msg}"
        );
        assert!(!msg.contains("not a backup ARN"), "{msg}");
    }
    assert!(fake.rows.lock().unwrap().is_empty(), "no row was claimed");
    assert!(mem.keys().is_empty(), "nothing was written to the store");
    assert_eq!(runtime.free_slots(), 4, "no slot was taken");
}

/// Under a `server.region` outside `[a-z0-9-]` no backup ARN can be formed
/// for any table, so CreateBackup is refused naming `server.region`, not the
/// table name, before the permit (the runtime's one permit is held). The
/// other operations are not affected for a backup created under a valid
/// region: its stored ARN is reached as written, the canonical spelling in
/// the server's region having no parsed form. A backup an earlier release
/// created under such a region keeps an ARN `check_backup_arn` refuses as
/// not valid, before any lookup.
#[tokio::test]
async fn create_backup_under_a_region_outside_the_arn_alphabet_names_the_setting() {
    let runtime = BackupRuntime::new(1).with_permit_wait(Duration::ZERO);
    let _held = runtime.take_permit().await.unwrap();
    let fake = Arc::new(Fake::default());
    let desc = TableDescription {
        table_name: "Music".to_owned(),
        table_status: TableStatus::Active,
        ..Default::default()
    };
    let msg = validation_message(
        create_backup(
            &fake,
            &runtime,
            ACCOUNT,
            "LOCAL",
            &LimitsConfig::default(),
            "Music",
            "nightly",
            Ok(desc),
        )
        .await
        .unwrap_err(),
    );
    assert!(
        msg.contains("server.region") && msg.contains("\"LOCAL\"") && msg.contains("[a-z0-9-]"),
        "{msg}"
    );
    assert!(
        !msg.contains("TableName"),
        "the table is not the cause: {msg}"
    );
    assert_eq!(runtime.free_slots(), 0, "the test still holds the permit");
    assert!(fake.creates.lock().unwrap().is_empty());

    // A backup created under a valid region, described under "LOCAL": the
    // row is read under the ARN as written, and only that spelling.
    let stored = arn(ACCOUNT);
    fake.rows
        .lock()
        .unwrap()
        .insert(stored.clone(), row(&stored, BackupStatus::Available));
    let spellings = backup_arn_field(&json!({ "BackupArn": stored }), ACCOUNT, "LOCAL").unwrap();
    let desc = describe_backup(&*fake, ACCOUNT, "LOCAL", &spellings)
        .await
        .unwrap();
    assert_eq!(desc.backup_details.backup_arn, stored);
    assert_eq!(
        fake.row_reads.lock().unwrap().as_slice(),
        std::slice::from_ref(&stored),
        "as written only: the canonical spelling under \"LOCAL\" has no parsed form"
    );

    // A backup created under "LOCAL" by an earlier release: its ARN is
    // refused before any lookup.
    let local =
        format!("arn:aws:dynamodb:LOCAL:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bc");
    let msg = validation_message(
        backup_arn_field(&json!({ "BackupArn": local }), ACCOUNT, "LOCAL").unwrap_err(),
    );
    assert_eq!(msg, BACKUP_ARN_NOT_VALID);
    assert_eq!(fake.row_reads.lock().unwrap().len(), 1, "no further read");
}

/// While a maintenance command holds the catalog's claim, CreateBackup and
/// RestoreTableFromBackup are refused with LimitExceeded naming a maintenance command
/// (generically: every maintenance command holds the same claim),
/// after every other check (a missing table is still TableNotFound) and
/// before the permit (no slot is taken, and the backend is not asked); once
/// the claim is gone both run.
#[tokio::test]
async fn jobs_are_refused_while_a_maintenance_claim_is_held() {
    let fake = Arc::new(Fake::with_source(source(Some(definition("PROVISIONED")))));
    *fake.maintenance_held.lock().unwrap() = true;
    let runtime = BackupRuntime::new(1).with_stale_job_after(Duration::from_secs(7));
    let err = create_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Ok(active_table("Music")),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::LimitExceededException(m)
            if m.contains("A backup maintenance command holds") && !m.contains("purge")),
        "{err:?}"
    );
    assert!(fake.creates.lock().unwrap().is_empty());
    assert_eq!(runtime.free_slots(), 1, "no slot was taken");
    let err = create_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Err(StorageError::TableNotFound("Music".to_owned())),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::TableNotFoundException(_)),
        "the table check comes first: {err:?}"
    );

    let err = restore_on(&fake, &runtime, &restore_body(json!({})))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::LimitExceededException(m)
            if m.contains("A backup maintenance command holds") && !m.contains("purge")),
        "{err:?}"
    );
    assert!(fake.restores().is_empty(), "the backend was not asked");
    assert_eq!(runtime.free_slots(), 1);
    assert_eq!(
        *fake.maintenance_checks.lock().unwrap(),
        vec![Duration::from_secs(7); 2],
        "the claim is read with stale_job_after_secs"
    );

    *fake.maintenance_held.lock().unwrap() = false;
    create_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Ok(active_table("Music")),
    )
    .await
    .unwrap();
    fake.permits.lock().unwrap().clear();
    restore_on(&fake, &runtime, &restore_body(json!({})))
        .await
        .unwrap();
    assert_eq!(fake.creates.lock().unwrap().len(), 1);
    assert_eq!(fake.restores().len(), 1);

    // Before the permit: with every slot held and the claim held, the
    // refusal names the maintenance claim, not the slot limit, so the check cannot
    // have run after `take_permit`.
    *fake.maintenance_held.lock().unwrap() = true;
    let full = BackupRuntime::new(1).with_stale_job_after(Duration::from_secs(7));
    let held_slot = full.take_permit().await.unwrap();
    assert_eq!(full.free_slots(), 0, "every slot is held");
    let err = create_backup(
        &fake,
        &full,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Ok(active_table("Music")),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::LimitExceededException(m)
            if m.contains("A backup maintenance command holds") && !m.contains("purge")),
        "the claim is checked before the permit: {err:?}"
    );
    let err = restore_on(&fake, &full, &restore_body(json!({})))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::LimitExceededException(m)
            if m.contains("A backup maintenance command holds") && !m.contains("purge")),
        "the claim is checked before the permit: {err:?}"
    );
    drop(held_slot);
}

// --- purge: the wait and the row deletion (`maintenance`) ----------------------

/// A maintenance claim handle for `HeldClaim`: heartbeats confirm until
/// `lost` is set.
#[derive(Default)]
struct FakeHolder {
    lost: Mutex<bool>,
    released: Mutex<bool>,
}

impl extenddb_storage::MaintenanceClaim for FakeHolder {
    fn generation(&self) -> u64 {
        11
    }
    fn heartbeat(
        &self,
    ) -> BoxedFuture<'_, Result<extenddb_storage::backup_catalog::Heartbeat, StorageError>> {
        let lost = *self.lost.lock().unwrap();
        Box::pin(async move {
            Ok(if lost {
                extenddb_storage::backup_catalog::Heartbeat::Lost
            } else {
                extenddb_storage::backup_catalog::Heartbeat::Confirmed
            })
        })
    }
    fn release(&self) -> BoxedFuture<'_, Result<(), StorageError>> {
        *self.released.lock().unwrap() = true;
        Box::pin(async { Ok(()) })
    }
}

/// A forwarding handle, so the test keeps its own reference to the holder.
struct Forward(Arc<FakeHolder>);

impl extenddb_storage::MaintenanceClaim for Forward {
    fn generation(&self) -> u64 {
        self.0.generation()
    }
    fn heartbeat(
        &self,
    ) -> BoxedFuture<'_, Result<extenddb_storage::backup_catalog::Heartbeat, StorageError>> {
        self.0.heartbeat()
    }
    fn release(&self) -> BoxedFuture<'_, Result<(), StorageError>> {
        self.0.release()
    }
}

fn held(holder: &Arc<FakeHolder>) -> maintenance::HeldClaim {
    maintenance::HeldClaim::start(
        Box::new(Forward(Arc::clone(holder))),
        Duration::from_secs(1),
    )
}

fn seconds_since_epoch() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// A row of `store()` under ARN `arn` in `status`, with a manifest and a
/// data file under its prefix in `mem`.
fn seed(fake: &Fake, mem: &MemStore, arn: &str, status: BackupStatus) {
    let prefix = BackupArn::parse(arn).unwrap().store_prefix();
    mem.insert(&format!("{prefix}extenddb-manifest.json"), b"{}");
    mem.insert(&format!("{prefix}data/00001.json.gz"), b"x");
    fake.rows
        .lock()
        .unwrap()
        .insert(arn.to_owned(), row(arn, status));
}

fn arn_n(n: u32) -> String {
    format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/0179197643143{n}-0000000{n}")
}

/// The states a row can be in when purge reaches it after reading it
/// AVAILABLE, a DeleteBackup having run meanwhile: still AVAILABLE; moved to
/// DELETING (the other deleter mid-way); moved to DELETED (the other
/// finished); gone (finished and its tombstone removed). Each ends with no
/// row and no object, counted as deleted; a status that cannot follow
/// AVAILABLE (FAILED) is the one case left for a look. A tombstone read
/// and removed meanwhile (a second purge) counts as deleted too.
#[tokio::test]
async fn purge_finishes_a_row_another_deleter_moved_or_removed_meanwhile() {
    let fake = Fake::default();
    let mem = MemStore::default();
    let arns: Vec<String> = (1..=6).map(arn_n).collect();
    for arn in &arns[..5] {
        seed(&fake, &mem, arn, BackupStatus::Available);
    }
    // A DELETED tombstone the command read, whose prefix is already empty.
    fake.rows
        .lock()
        .unwrap()
        .insert(arns[5].clone(), row(&arns[5], BackupStatus::Deleted));
    // The command's read under the claim.
    let read: Vec<BackupRow> = fake
        .list_backup_rows(None, &store())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(read.len(), 6);
    // Between the read and the rows' turns, DeleteBackup moves rows 2 and 3
    // and finishes 4 (its tombstone removed too); a writer moves 5 to
    // FAILED, which no DeleteBackup does; the tombstone 6 is removed by a
    // second purge.
    {
        let mut rows = fake.rows.lock().unwrap();
        rows.get_mut(&arns[1]).unwrap().status = BackupStatus::Deleting;
        rows.get_mut(&arns[2]).unwrap().status = BackupStatus::Deleted;
        rows.remove(&arns[3]);
        rows.get_mut(&arns[4]).unwrap().status = BackupStatus::Failed;
        rows.remove(&arns[5]);
    }
    // The deleter that finished rows 3 and 4 deleted their objects.
    for arn in [&arns[2], &arns[3]] {
        let prefix = BackupArn::parse(arn).unwrap().store_prefix();
        extenddb_backup_store::BackupStore::delete_prefix(&mem, &prefix)
            .await
            .unwrap();
    }

    let mut outcomes = Vec::new();
    for row in &read {
        outcomes.push(maintenance::purge_row(&fake, &mem, row, Duration::from_secs(300)).await);
    }
    assert_eq!(
        outcomes[0],
        Ok(()),
        "still AVAILABLE: moved and deleted here"
    );
    assert_eq!(
        outcomes[1],
        Ok(()),
        "moved to DELETING by another deleter: finished here"
    );
    assert_eq!(
        outcomes[2],
        Ok(()),
        "moved to DELETED by another deleter: the tombstone removed here, counted as deleted"
    );
    assert_eq!(outcomes[3], Ok(()), "gone: counted as deleted");
    assert_eq!(
        outcomes[4],
        Err("its status changed to FAILED".to_owned()),
        "a status that cannot follow AVAILABLE is left"
    );
    assert_eq!(
        outcomes[5],
        Ok(()),
        "a tombstone removed meanwhile: counted as deleted"
    );
    let left: Vec<String> = fake.rows.lock().unwrap().keys().cloned().collect();
    assert_eq!(left, vec![arns[4].clone()], "no row of the five remains");
    let failed_prefix = BackupArn::parse(&arns[4]).unwrap().store_prefix();
    assert_eq!(
        mem.keys(),
        vec![
            format!("{failed_prefix}data/00001.json.gz"),
            format!("{failed_prefix}extenddb-manifest.json"),
        ],
        "only the left row's objects remain"
    );
    // The row moved to DELETED was never moved by purge (DELETING to
    // DELETED is the other deleter's write); the one left DELETING and the
    // one still AVAILABLE were finished by purge; the three tombstones were
    // removed here.
    assert_eq!(
        *fake.set_deleted.lock().unwrap(),
        vec![arns[0].clone(), arns[1].clone()]
    );
    assert_eq!(
        *fake.removed_rows.lock().unwrap(),
        vec![arns[0].clone(), arns[1].clone(), arns[2].clone()]
    );
}

/// A CREATING row with a dead owner goes through the dead-owner removal and
/// then its prefix; one whose heartbeat is young is left with its objects
/// under the live window, and removed under the zero window SQLite uses.
#[tokio::test]
async fn purge_removes_a_dead_creating_row_and_leaves_a_live_one() {
    let fake = Fake::default();
    let mem = MemStore::default();
    let dead = arn_n(1);
    let live = arn_n(2);
    seed(&fake, &mem, &dead, BackupStatus::Creating);
    seed(&fake, &mem, &live, BackupStatus::Creating);
    fake.rows
        .lock()
        .unwrap()
        .get_mut(&live)
        .unwrap()
        .heartbeat_at = seconds_since_epoch();
    let window = Duration::from_secs(300);
    let dead_row = fake.rows.lock().unwrap()[&dead].clone();
    let live_row = fake.rows.lock().unwrap()[&live].clone();
    assert_eq!(
        maintenance::purge_row(&fake, &mem, &dead_row, window).await,
        Ok(())
    );
    let err = maintenance::purge_row(&fake, &mem, &live_row, window)
        .await
        .unwrap_err();
    assert!(
        err.contains("the CREATING row could not be removed"),
        "{err}"
    );
    let left: Vec<String> = fake.rows.lock().unwrap().keys().cloned().collect();
    assert_eq!(left, vec![live.clone()]);
    assert_eq!(mem.keys().len(), 2, "the live row's objects stay");
    // A heartbeat written a moment ago is older than a zero window once a
    // moment has passed.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        maintenance::purge_row(&fake, &mem, &live_row, Duration::ZERO).await,
        Ok(())
    );
    assert!(fake.rows.lock().unwrap().is_empty());
    assert!(mem.keys().is_empty());
}

/// The wait's five outcomes are told apart: quiet, also with a FAILED
/// restore record whose target is still CREATING (the reconciler's duty,
/// not a running job) and a FAILED backup row whose owner is dead; busy at
/// the limit, with the jobs named (a CREATING row and a FAILED row with a
/// live heartbeat, a RUNNING restore) and announced once; a signal, during
/// a sleep and before the first read; the catalog unreadable; the claim
/// lost.
#[tokio::test(start_paused = true)]
async fn the_wait_reports_each_outcome_as_itself() {
    use extenddb_storage::backup_catalog::{JobStatus, RestoreKind};
    use maintenance::{Interrupt, Wait, WaitOutcome, wait_for_quiet};
    let fake = Fake::default();
    let holder = Arc::new(FakeHolder::default());
    let claim = held(&holder);
    let limit = Duration::from_secs(20);
    let wait = Wait {
        owner_window: Duration::from_secs(10),
        limit,
        poll: Duration::from_secs(2),
    };
    let announced: Mutex<Vec<Vec<String>>> = Mutex::new(Vec::new());
    let announce = |jobs: &[String]| announced.lock().unwrap().push(jobs.to_vec());

    let (_tx, mut quiet) = Interrupt::new();
    assert_eq!(
        wait_for_quiet(&fake, &store(), &claim, wait, &mut quiet, announce).await,
        WaitOutcome::Quiet
    );
    // A FAILED record whose target is still CREATING is listed by the
    // zero-window dead-job listing with the RUNNING ones; it is the
    // reconciler's to finish and no job runs under it. A FAILED backup row
    // whose heartbeat is older than the window has a dead owner.
    let mut failed_record = dead_job(RestoreKind::Legacy, "FailedTarget");
    failed_record.status = JobStatus::Failed;
    fake.dead_jobs.lock().unwrap().push(failed_record);
    fake.rows
        .lock()
        .unwrap()
        .insert(arn_n(3), row(&arn_n(3), BackupStatus::Failed));
    let started = tokio::time::Instant::now();
    assert_eq!(
        wait_for_quiet(&fake, &store(), &claim, wait, &mut quiet, announce).await,
        WaitOutcome::Quiet,
        "a FAILED restore record and a FAILED row with a dead owner are not waited on"
    );
    assert_eq!(started.elapsed(), Duration::ZERO, "quiet at the first read");
    assert!(announced.lock().unwrap().is_empty());

    // Busy: a CREATING row and a FAILED row whose heartbeats are young, and
    // a RUNNING restore.
    for (n, status) in [(1, BackupStatus::Creating), (2, BackupStatus::Failed)] {
        fake.rows.lock().unwrap().insert(arn_n(n), {
            let mut r = row(&arn_n(n), status);
            r.heartbeat_at = seconds_since_epoch();
            r
        });
    }
    fake.dead_jobs
        .lock()
        .unwrap()
        .push(dead_job(RestoreKind::Legacy, "Target"));
    let started = tokio::time::Instant::now();
    let busy = wait_for_quiet(&fake, &store(), &claim, wait, &mut quiet, announce).await;
    let expected = vec![
        format!(
            "CreateBackup {} has a live owner (heartbeat within stale_job_after_secs)",
            arn_n(1)
        ),
        format!(
            "the FAILED backup {} has a live owner (heartbeat within stale_job_after_secs)",
            arn_n(2)
        ),
        format!("a restore into Target (account {ACCOUNT}) is RUNNING"),
    ];
    assert_eq!(busy, WaitOutcome::Busy(expected.clone()));
    assert!(started.elapsed() >= limit, "{:?}", started.elapsed());
    assert_eq!(*announced.lock().unwrap(), vec![expected]);

    // A signal during a sleep ends the wait before the limit.
    let (tx, mut interrupted) = Interrupt::new();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = tx.send(Some("SIGTERM"));
    });
    let started = tokio::time::Instant::now();
    assert_eq!(
        wait_for_quiet(&fake, &store(), &claim, wait, &mut interrupted, announce).await,
        WaitOutcome::Interrupted("SIGTERM")
    );
    assert!(
        started.elapsed() < Duration::from_millis(3_500),
        "the signal ends the sleep, not the next poll: {:?}",
        started.elapsed()
    );
    // One already received ends it before the first read.
    assert_eq!(interrupted.signalled(), Some("SIGTERM"));
    assert_eq!(
        wait_for_quiet(&fake, &store(), &claim, wait, &mut interrupted, announce).await,
        WaitOutcome::Interrupted("SIGTERM")
    );

    // The catalog unreadable.
    *fake.list_error.lock().unwrap() = Some("pool exhausted".to_owned());
    assert_eq!(
        wait_for_quiet(&fake, &store(), &claim, wait, &mut quiet, announce).await,
        WaitOutcome::CatalogError(
            "cannot list the catalog's backup rows: Internal error: pool exhausted".to_owned()
        )
    );
    *fake.list_error.lock().unwrap() = None;

    // The claim lost: the next heartbeat records it, and the wait stops.
    *holder.lost.lock().unwrap() = true;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(
        wait_for_quiet(&fake, &store(), &claim, wait, &mut quiet, announce).await,
        WaitOutcome::Lost(
            "the maintenance claim was lost (another holder took it over)".to_owned()
        )
    );
    claim.release().await.unwrap();
    assert!(*holder.released.lock().unwrap());
}

// --- the reconciler -------------------------------------------------------------

fn dead_job(kind: extenddb_storage::backup_catalog::RestoreKind, name: &str) -> RestoreJobRow {
    RestoreJobRow {
        target: RestoreTargetKey {
            account_id: ACCOUNT.to_owned(),
            table_name: name.to_owned(),
            table_id: format!("tid-{name}"),
        },
        source_backup_arn: arn(ACCOUNT),
        kind,
        status: extenddb_storage::backup_catalog::JobStatus::Running,
        owner_generation: 0,
        heartbeat_at: 0.0,
    }
}

/// A legacy restore whose owner is dead is marked FAILED and its target
/// removed; a manifest restore is left for its resumer; an abandoned
/// in-catalog CREATING backup is removed through the reader's delete.
#[tokio::test]
async fn the_reconciler_fails_dead_legacy_restores_and_removes_abandoned_backups() {
    use extenddb_storage::backup_catalog::RestoreKind;
    let fake = Fake::default();
    *fake.target_still_creating.lock().unwrap() = true;
    *fake.record_still_running.lock().unwrap() = true;
    fake.dead_jobs.lock().unwrap().extend([
        dead_job(RestoreKind::Legacy, "LegacyTarget"),
        dead_job(RestoreKind::Manifest, "ManifestTarget"),
    ]);
    fake.abandoned_backups
        .lock()
        .unwrap()
        .push(AbandonedLegacyBackup {
            account_id: ACCOUNT.to_owned(),
            backup_arn: arn(ACCOUNT),
            removed: false,
        });
    fake.descriptions
        .lock()
        .unwrap()
        .insert(arn(ACCOUNT), backup_description("CREATING", "PROVISIONED"));

    let report = reconciler::cycle(
        &fake,
        &runtime(),
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(!report.maintenance_held);
    assert_eq!(report.legacy_restores_removed, ["LegacyTarget"]);
    assert!(report.failed_targets_removed.is_empty());
    assert_eq!(report.manifest_restores_seen, ["ManifestTarget"]);
    assert_eq!(report.manifest_restores_logged, ["ManifestTarget"]);
    assert_eq!(report.legacy_backups_removed, [arn(ACCOUNT)]);

    let failed = fake.failed_jobs.lock().unwrap().clone();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].0.table_name, "LegacyTarget");
    assert_eq!(failed[0].1, reconciler::OWNER_DEAD);
    assert_eq!(
        fake.removed_targets.lock().unwrap().as_slice(),
        [dead_job(RestoreKind::Legacy, "LegacyTarget").target]
    );
    assert_eq!(fake.deleted.lock().unwrap().as_slice(), [arn(ACCOUNT)]);
}

/// A record that is no longer RUNNING when the reconciler reaches it belongs
/// to whoever moved it: the target is not removed.
#[tokio::test]
async fn the_reconciler_leaves_a_target_whose_record_moved() {
    use extenddb_storage::backup_catalog::RestoreKind;
    let fake = Fake::default();
    *fake.record_still_running.lock().unwrap() = false;
    *fake.target_still_creating.lock().unwrap() = true;
    fake.dead_jobs
        .lock()
        .unwrap()
        .push(dead_job(RestoreKind::Legacy, "LegacyTarget"));
    let report = reconciler::cycle(
        &fake,
        &runtime(),
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(report.legacy_restores_removed.is_empty());
    assert!(report.targets_left_to_owner.is_empty());
    assert_eq!(fake.failed_jobs.lock().unwrap().len(), 1);
    assert!(fake.removed_targets.lock().unwrap().is_empty());
}

/// A removal that finds the target no longer a CREATING restore target
/// (`remove_restore_target` answered `false`: the target flipped ACTIVE, was
/// deleted, or was re-created under another id) leaves it to its current
/// owner: the target is not counted as removed, the cycle reports it as left,
/// and the record is not written again, whether this cycle wrote the FAILED
/// (a dead legacy restore) or found it (the FAILED-target duty).
#[tokio::test]
async fn the_reconciler_leaves_a_target_that_is_no_longer_creating_after_the_failed_write() {
    use extenddb_storage::backup_catalog::{JobStatus, RestoreKind};
    let fake = Fake::default();
    *fake.record_still_running.lock().unwrap() = true;
    *fake.target_still_creating.lock().unwrap() = false;
    let dead = dead_job(RestoreKind::Legacy, "LegacyTarget");
    let mut failed = dead_job(RestoreKind::Legacy, "FailedTarget");
    failed.status = JobStatus::Failed;
    fake.dead_jobs
        .lock()
        .unwrap()
        .extend([dead.clone(), failed.clone()]);
    let report = reconciler::cycle(
        &fake,
        &runtime(),
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(
        report.legacy_restores_removed.is_empty() && report.failed_targets_removed.is_empty(),
        "a target the removal found moved is not counted as removed: {report:?}"
    );
    assert_eq!(
        report.targets_left_to_owner,
        ["LegacyTarget", "FailedTarget"]
    );
    assert_eq!(
        fake.removed_targets.lock().unwrap().as_slice(),
        [dead.target.clone(), failed.target]
    );
    assert_eq!(
        fake.failed_jobs.lock().unwrap().as_slice(),
        [(dead.target, reconciler::OWNER_DEAD.to_owned())],
        "the dead record is written FAILED once; the FAILED record is not written again"
    );
}

/// The maintenance claim is checked first and reported; the duties that
/// resolve dead owners and the DELETING rows of the configured store still
/// run while it is held (the command waits on the first and finishes the
/// second itself, so neither races it).
#[tokio::test]
async fn the_reconciler_reports_a_held_maintenance_claim_and_still_resolves_dead_owners() {
    use extenddb_storage::backup_catalog::RestoreKind;
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
    let fake = fake_with_rows(&[(arn(ACCOUNT), BackupStatus::Deleting)]);
    *fake.maintenance_held.lock().unwrap() = true;
    *fake.target_still_creating.lock().unwrap() = true;
    *fake.record_still_running.lock().unwrap() = true;
    fake.dead_jobs
        .lock()
        .unwrap()
        .push(dead_job(RestoreKind::Legacy, "LegacyTarget"));
    let report = reconciler::cycle(
        &fake,
        &runtime_on(&mem, store()),
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(report.maintenance_held);
    assert_eq!(report.legacy_restores_removed, ["LegacyTarget"]);
    assert_eq!(
        report.deletions_finished,
        [arn(ACCOUNT)],
        "a DELETING row is finished while the claim is held"
    );
    assert_eq!(
        fake.rows.lock().unwrap()[&arn(ACCOUNT)].status,
        BackupStatus::Deleted
    );
    assert_eq!(mem.keys(), [STORE_IDENTITY_KEY]);
}

/// The wait tells a live owner from a dead one on the catalog's clock, the
/// clock the owner's heartbeat was written on, and never on this host's:
/// with the catalog's clock 400 s behind this host's, a CREATING row whose
/// heartbeat is the catalog's now has a live owner although this host's
/// clock reads it 400 s old, past the 300 s window; with the catalog's clock
/// 400 s ahead, a row stamped with this host's now is 400 s old to the
/// catalog and its owner dead.
#[tokio::test(start_paused = true)]
async fn the_wait_reads_a_live_owner_on_the_catalogs_clock() {
    use maintenance::{Interrupt, Wait, WaitOutcome, wait_for_quiet};
    let fake = Fake::default();
    let holder = Arc::new(FakeHolder::default());
    let claim = held(&holder);
    let wait = Wait {
        owner_window: Duration::from_secs(300),
        limit: Duration::from_secs(4),
        poll: Duration::from_secs(2),
    };
    let (_tx, mut quiet) = Interrupt::new();

    *fake.catalog_clock_offset_secs.lock().unwrap() = -400.0;
    fake.rows.lock().unwrap().insert(arn_n(1), {
        let mut r = row(&arn_n(1), BackupStatus::Creating);
        r.heartbeat_at = seconds_since_epoch() - 400.0;
        r
    });
    assert_eq!(
        wait_for_quiet(&fake, &store(), &claim, wait, &mut quiet, |_| {}).await,
        WaitOutcome::Busy(vec![format!(
            "CreateBackup {} has a live owner (heartbeat within stale_job_after_secs)",
            arn_n(1)
        )]),
        "an owner whose heartbeat is young on the catalog's clock is live"
    );

    *fake.catalog_clock_offset_secs.lock().unwrap() = 400.0;
    fake.rows
        .lock()
        .unwrap()
        .get_mut(&arn_n(1))
        .unwrap()
        .heartbeat_at = seconds_since_epoch();
    assert_eq!(
        wait_for_quiet(&fake, &store(), &claim, wait, &mut quiet, |_| {}).await,
        WaitOutcome::Quiet,
        "an owner whose heartbeat is old on the catalog's clock is dead"
    );
}

/// A cycle run by the holder of the maintenance claim with a zero dead-owner
/// window (`backup purge` on SQLite) takes the held branch without reading
/// the claim: the command's claim was heartbeated a millisecond ago, which
/// a zero window reads as stale, so a cycle that read it would take the
/// branch for a free claim. The held branch skips the deleting duties the
/// command must not race: a stale FAILED row keeps its prefix and the
/// registrations sweep does not run. The same cycle without the holder's
/// state reads the claim through the window, finds it free and runs them;
/// that is the read the holder's state replaces.
#[tokio::test]
async fn a_cycle_run_by_the_claim_holder_takes_the_held_branch_whatever_its_window() {
    use extenddb_storage::backup_catalog::RestoreKind;
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
    let fake = fake_with_rows(&[(arn(ACCOUNT), BackupStatus::Deleting)]);
    let stale_failed = arn_n(1);
    let mut r = row(&stale_failed, BackupStatus::Failed);
    r.heartbeat_at = seconds_since_epoch() - 1000.0;
    r.completed_at = Some(seconds_since_epoch());
    fake.rows.lock().unwrap().insert(stale_failed.clone(), r);
    let prefix = BackupArn::parse(&stale_failed).unwrap().store_prefix();
    mem.insert(&format!("{prefix}data/000001.json.gz"), b"x");
    fake.swept_registrations.lock().unwrap().push(arn_n(9));
    *fake.maintenance_beat_age.lock().unwrap() = Some(Duration::from_millis(1));
    *fake.target_still_creating.lock().unwrap() = true;
    *fake.record_still_running.lock().unwrap() = true;
    fake.dead_jobs
        .lock()
        .unwrap()
        .push(dead_job(RestoreKind::Legacy, "LegacyTarget"));
    let report = reconciler::cycle(
        &fake,
        &runtime_on(&mem, store()),
        Duration::ZERO,
        "test",
        &mut reconciler::ReconcilerState::for_maintenance_holder(),
    )
    .await
    .unwrap();
    assert!(
        report.failed_prefixes_deleted.is_empty() && objects_under(&mem, &stale_failed).len() == 1,
        "the holder's cycle deleted a FAILED row's prefix: {:?}",
        report.failed_prefixes_deleted
    );
    assert!(
        report.registrations_swept.is_empty(),
        "the holder's cycle swept the registrations"
    );
    assert!(
        report.maintenance_held,
        "the holder's cycle took the branch for a free claim"
    );
    assert!(
        fake.maintenance_checks.lock().unwrap().is_empty(),
        "the holder's cycle read the claim: {:?}",
        fake.maintenance_checks.lock().unwrap()
    );
    // The duties the command waits on still ran under the zero window.
    assert_eq!(report.legacy_restores_removed, ["LegacyTarget"]);
    assert_eq!(report.deletions_finished, [arn(ACCOUNT)]);

    // Without the holder's state the zero window reads the same claim as
    // free.
    let report = reconciler::cycle(
        &fake,
        &runtime_on(&mem, store()),
        Duration::ZERO,
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(!report.maintenance_held);
    assert_eq!(
        *fake.maintenance_checks.lock().unwrap(),
        [Duration::ZERO],
        "a default state reads the claim through the window it is given"
    );
    assert_eq!(
        report.failed_prefixes_deleted,
        std::slice::from_ref(&stale_failed)
    );
    assert!(objects_under(&mem, &stale_failed).is_empty());
    assert_eq!(report.registrations_swept, [arn_n(9)]);
}

/// The worker runs a cycle at once (observed through the maintenance check,
/// the cycle's first read), records the worker metrics the other workers
/// record, then waits an interval, and stops at the shutdown token.
#[tokio::test]
async fn the_reconciler_worker_runs_at_startup_and_stops_on_shutdown() {
    use extenddb_core::metrics::{MetricName, MetricsCollector};
    let fake = Arc::new(Fake::default());
    *fake.maintenance_held.lock().unwrap() = true;
    let metrics = Arc::new(MetricsCollector::new());
    let shutdown = extenddb_storage::CancellationToken::new();
    let worker = tokio::spawn(reconciler::run(
        Arc::clone(&fake),
        Arc::new(runtime()),
        reconciler::ReconcilerSettings {
            interval: Duration::from_secs(3600),
            stale_job_after: Duration::from_secs(300),
        },
        "test".to_owned(),
        Arc::clone(&metrics),
        Arc::new(tokio::sync::Mutex::new(
            reconciler::ReconcilerState::default(),
        )),
        shutdown.clone(),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while fake.maintenance_checks.lock().unwrap().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first cycle did not run"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        fake.maintenance_checks.lock().unwrap().len(),
        1,
        "one cycle before the first interval"
    );
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("the worker stops at shutdown")
        .unwrap();
    assert_eq!(fake.maintenance_checks.lock().unwrap().len(), 1);
    let recorded: Vec<MetricName> = metrics
        .drain(Duration::ZERO)
        .into_iter()
        .filter(|b| b.operation == "BackupReconciler")
        .map(|b| b.metric)
        .collect();
    assert!(
        recorded.contains(&MetricName::WorkerLastSuccess)
            && recorded.contains(&MetricName::WorkerCycleLatency),
        "{recorded:?}"
    );
    assert!(!recorded.contains(&MetricName::WorkerErrorCount));
}

/// A cycle that fails is recorded as a worker error, as the other workers
/// record theirs, and the worker goes on to the next cycle.
#[tokio::test]
async fn a_failed_reconciler_cycle_records_a_worker_error() {
    use extenddb_core::metrics::{MetricName, MetricsCollector};
    let fake = Arc::new(Fake::default());
    *fake.maintenance_error.lock().unwrap() = Some("catalog unreachable".to_owned());
    let metrics = Arc::new(MetricsCollector::new());
    let shutdown = extenddb_storage::CancellationToken::new();
    let worker = tokio::spawn(reconciler::run(
        Arc::clone(&fake),
        Arc::new(runtime()),
        reconciler::ReconcilerSettings {
            interval: Duration::from_millis(5),
            stale_job_after: Duration::from_secs(300),
        },
        "test".to_owned(),
        Arc::clone(&metrics),
        Arc::new(tokio::sync::Mutex::new(
            reconciler::ReconcilerState::default(),
        )),
        shutdown.clone(),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while fake.maintenance_checks.lock().unwrap().len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the worker did not go on after a failed cycle"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("the worker stops at shutdown")
        .unwrap();
    let errors: f64 = metrics
        .drain(Duration::ZERO)
        .into_iter()
        .filter(|b| b.operation == "BackupReconciler")
        .filter(|b| b.metric == MetricName::WorkerErrorCount)
        .map(|b| b.sum)
        .sum();
    assert!(errors >= 2.0, "{errors} worker errors recorded");
}

/// The FAILED-target duty: a FAILED record whose target is still CREATING
/// (the FAILED write landed, the removal did not) is listed by the backend
/// once stale, and the reconciler removes the target with no second FAILED
/// write.
#[tokio::test]
async fn the_reconciler_removes_the_target_of_a_failed_record_without_writing_failed_again() {
    use extenddb_storage::backup_catalog::{JobStatus, RestoreKind};
    let fake = Fake::default();
    *fake.target_still_creating.lock().unwrap() = true;
    let mut failed = dead_job(RestoreKind::Legacy, "FailedTarget");
    failed.status = JobStatus::Failed;
    let mut failed_manifest = dead_job(RestoreKind::Manifest, "FailedManifestTarget");
    failed_manifest.status = JobStatus::Failed;
    fake.dead_jobs
        .lock()
        .unwrap()
        .extend([failed.clone(), failed_manifest.clone()]);
    let report = reconciler::cycle(
        &fake,
        &runtime(),
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(
        fake.failed_jobs.lock().unwrap().is_empty(),
        "a FAILED record is not written FAILED again"
    );
    assert_eq!(
        report.failed_targets_removed,
        ["FailedTarget", "FailedManifestTarget"]
    );
    assert!(report.legacy_restores_removed.is_empty());
    assert!(report.manifest_restores_seen.is_empty());
    assert_eq!(
        fake.removed_targets.lock().unwrap().as_slice(),
        [failed.target, failed_manifest.target]
    );
}

/// A dead manifest record, which this release cannot act on, is reported
/// once per record for as long as the process runs, not every cycle.
#[tokio::test]
async fn a_dead_manifest_record_is_logged_once_per_record() {
    use extenddb_storage::backup_catalog::RestoreKind;
    let fake = Fake::default();
    fake.dead_jobs
        .lock()
        .unwrap()
        .push(dead_job(RestoreKind::Manifest, "ManifestTarget"));
    let mut state = reconciler::ReconcilerState::default();
    let first = reconciler::cycle(
        &fake,
        &runtime(),
        Duration::from_secs(300),
        "test",
        &mut state,
    )
    .await
    .unwrap();
    assert_eq!(first.manifest_restores_seen, ["ManifestTarget"]);
    assert_eq!(first.manifest_restores_logged, ["ManifestTarget"]);
    let second = reconciler::cycle(
        &fake,
        &runtime(),
        Duration::from_secs(300),
        "test",
        &mut state,
    )
    .await
    .unwrap();
    assert_eq!(second.manifest_restores_seen, ["ManifestTarget"]);
    assert!(
        second.manifest_restores_logged.is_empty(),
        "the same record is not reported twice"
    );
    fake.dead_jobs
        .lock()
        .unwrap()
        .push(dead_job(RestoreKind::Manifest, "Another"));
    let third = reconciler::cycle(
        &fake,
        &runtime(),
        Duration::from_secs(300),
        "test",
        &mut state,
    )
    .await
    .unwrap();
    assert_eq!(third.manifest_restores_logged, ["Another"]);
    assert!(
        fake.failed_jobs.lock().unwrap().is_empty()
            && fake.removed_targets.lock().unwrap().is_empty(),
        "a manifest record is never acted on by this release"
    );
}

/// The FAILED write for a dead legacy restore is conditioned on the
/// `owner_generation` and `heartbeat_at` the listing read (design 4(e)): a
/// heartbeat that lands between the listing and the write says the owner is
/// alive, the write matches nothing, and the target is left to it. The same
/// record without the late heartbeat is failed and removed.
#[tokio::test]
async fn the_reconciler_leaves_a_record_whose_owner_heartbeated_after_the_listing() {
    use extenddb_storage::backup_catalog::RestoreKind;
    let fake = Fake::default();
    *fake.target_still_creating.lock().unwrap() = true;
    *fake.record_still_running.lock().unwrap() = true;
    let mut listed = dead_job(RestoreKind::Legacy, "LateBeat");
    listed.owner_generation = 77;
    listed.heartbeat_at = 1_000.0;
    fake.dead_jobs.lock().unwrap().push(listed.clone());
    fake.heartbeat_after_listing
        .lock()
        .unwrap()
        .insert(listed.target.clone(), 1_011.6);

    let report = reconciler::cycle(
        &fake,
        &runtime(),
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(
        report.legacy_restores_removed.is_empty() && report.targets_left_to_owner.is_empty(),
        "{report:?}"
    );
    assert_eq!(
        fake.failed_jobs.lock().unwrap().len(),
        1,
        "the FAILED write was attempted under the listed claim"
    );
    assert!(
        fake.removed_targets.lock().unwrap().is_empty(),
        "a record whose owner heartbeated keeps its target"
    );

    // Without the late heartbeat the listed values still match: failed and
    // removed.
    let report = reconciler::cycle(
        &fake,
        &runtime(),
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.legacy_restores_removed, ["LateBeat"]);
    assert_eq!(
        fake.removed_targets.lock().unwrap().as_slice(),
        [listed.target]
    );
}

// --- the store identity file and the store's health ---------------------------

const MANIFEST_KEY_SUFFIX: &str = "extenddb-manifest.json";

/// The manifest key of `arn`'s prefix.
fn manifest_key(arn: &str) -> String {
    format!(
        "{}{MANIFEST_KEY_SUFFIX}",
        BackupArn::parse(arn).unwrap().store_prefix()
    )
}

/// An ACTIVE table description, the probe CreateBackup's handler ran.
fn active_table(name: &str) -> TableDescription {
    TableDescription {
        table_name: name.to_owned(),
        table_status: TableStatus::Active,
        ..Default::default()
    }
}

/// A second, newer backup of the `Music` table.
fn newer_arn(account: &str) -> String {
    format!("arn:aws:dynamodb:{REGION}:{account}:table/Music/backup/01791976999999-0badf00d")
}

fn fake_with_rows(rows: &[(String, BackupStatus)]) -> Fake {
    let fake = Fake::default();
    for (arn, status) in rows {
        fake.rows
            .lock()
            .unwrap()
            .insert(arn.clone(), row(arn, *status));
    }
    fake
}

/// A restore of a store-backed row checks the target name once the row is
/// known to exist and AVAILABLE, before the backup's in-use rule and before
/// this release's refusal of the store-backed restore (the service's order:
/// a restore of a backup into its own CREATING target is
/// `TableInUseException`, measured): an existing table of the name is
/// `TableAlreadyExistsException`, a CREATING restore target
/// `TableInUseException` naming its source backup, and a free name reaches
/// the refusal naming the store. A row that is not AVAILABLE answers for
/// itself before the name is read.
#[tokio::test]
async fn a_store_backed_restore_checks_the_target_name_after_the_row_and_before_its_refusal() {
    let mem = Arc::new(MemStore::default());
    let runtime = runtime_on(&mem, store());
    let fake = fake_with_rows(&[(arn(ACCOUNT), BackupStatus::Available)]);

    fake.tables.lock().unwrap().insert(
        "MusicRestored".to_owned(),
        TableDescription {
            table_name: "MusicRestored".to_owned(),
            table_status: TableStatus::Active,
            ..Default::default()
        },
    );
    let err = restore_on(&fake, &runtime, &restore_body(json!({})))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::TableAlreadyExistsException(_)),
        "{err:?}"
    );

    fake.tables.lock().unwrap().insert(
        "MusicRestored".to_owned(),
        TableDescription {
            table_name: "MusicRestored".to_owned(),
            table_status: TableStatus::Creating,
            restore_summary: Some(RestoreSummary {
                source_backup_arn: Some(arn(ACCOUNT)),
                source_table_arn: None,
                restore_date_time: 0.0,
                restore_in_progress: true,
            }),
            ..Default::default()
        },
    );
    let err = restore_on(&fake, &runtime, &restore_body(json!({})))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::TableInUseException(ref m) if m.contains(&arn(ACCOUNT))),
        "{err:?}"
    );

    fake.tables.lock().unwrap().remove("MusicRestored");
    let err = restore_on(&fake, &runtime, &restore_body(json!({})))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::ValidationException(ref m)
            if m.contains("is in a backup store; this release restores in-catalog backups only")),
        "{err:?}"
    );

    // A row that is not AVAILABLE: its own answer, the name unread.
    let fake = fake_with_rows(&[(arn(ACCOUNT), BackupStatus::Deleted)]);
    fake.tables.lock().unwrap().insert(
        "MusicRestored".to_owned(),
        TableDescription {
            table_name: "MusicRestored".to_owned(),
            table_status: TableStatus::Active,
            ..Default::default()
        },
    );
    let err = restore_on(&fake, &runtime, &restore_body(json!({})))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::BackupNotFoundException(_)),
        "{err:?}"
    );
    assert!(fake.table_reads.lock().unwrap().is_empty());
}

/// An empty root and a catalog with no rows for it: startup creates the file
/// with this catalog's id and a fresh store id, re-reads it and uses that id.
#[tokio::test]
async fn establish_creates_the_identity_file_on_an_empty_root() {
    let mem = MemStore::default();
    let fake = Fake::default();
    let identity = store_identity::establish(&mem, StoreKind::Filesystem, "/r", &fake, "test")
        .await
        .unwrap();
    assert_eq!(identity.store_kind, StoreKind::Filesystem);
    assert_eq!(identity.store_root, "/r");
    assert_eq!(mem.keys(), [STORE_IDENTITY_KEY]);
    let file = StoreIdentityFile::parse(&mem.objects.lock().unwrap()[STORE_IDENTITY_KEY]).unwrap();
    assert_eq!(file.store_id, identity.store_id);
    assert_eq!(file.catalog_id, "catalog-1");
    assert_eq!(file.backend, "test");
    assert_eq!(mem.puts.lock().unwrap().as_slice(), [STORE_IDENTITY_KEY]);
    // A second startup against the same root reads the file and uses its id.
    let again = store_identity::establish(&mem, StoreKind::Filesystem, "/r", &fake, "test")
        .await
        .unwrap();
    assert_eq!(again, identity);
    assert_eq!(
        mem.puts.lock().unwrap().len(),
        1,
        "the file is not rewritten"
    );
}

/// Two instances of one catalog starting against one empty root: one creates
/// the file, the other finds it present, and both use the winner's id.
#[tokio::test]
async fn two_instances_on_one_empty_root_end_with_one_id() {
    let mem = Arc::new(MemStore::default());
    let fakes = [Fake::default(), Fake::default()];
    let (a, b) = tokio::join!(
        store_identity::establish(&*mem, StoreKind::S3, "s3://b/", &fakes[0], "test"),
        store_identity::establish(&*mem, StoreKind::S3, "s3://b/", &fakes[1], "test"),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a, b);
    assert_eq!(mem.keys(), [STORE_IDENTITY_KEY]);
    assert_eq!(
        mem.puts.lock().unwrap().len(),
        1,
        "one create stored its file"
    );
}

/// A root whose file names another catalog is unhealthy, and stays so until
/// the file is restored; the problem names both catalogs and no root.
#[tokio::test]
async fn a_file_naming_another_catalog_is_unhealthy_until_restored() {
    let mem = MemStore::default();
    mem.insert(
        STORE_IDENTITY_KEY,
        &identity_file("store-1", "another-catalog"),
    );
    let fake = fake_with_row(BackupStatus::Available);
    let err = store_identity::establish(&mem, StoreKind::Filesystem, "/r", &fake, "test")
        .await
        .unwrap_err();
    assert_eq!(
        err,
        StoreProblem::ForeignCatalog {
            file_catalog_id: "another-catalog".to_owned(),
            catalog_id: "catalog-1".to_owned(),
        }
    );
    assert_eq!(err.category(), "owned by another catalog");
    assert!(!err.public_reason().contains("/r"));
    // Restored: the check passes.
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
    let identity = store_identity::recheck(&mem, &store(), &fake, "test").await;
    assert_eq!(identity, Ok(()));
}

/// Something that is not an object at the identity file's key (a FIFO, a
/// socket): the create finds the key taken and the read finds no object, so
/// nothing was created and nothing can be read, and the problem says so
/// rather than claiming a file was created.
#[tokio::test]
async fn a_non_object_at_the_identity_key_is_neither_readable_nor_creatable() {
    let mem = MemStore::default();
    mem.key_taken_by_a_non_object
        .store(true, std::sync::atomic::Ordering::Release);
    let fake = Fake::default();
    let err = store_identity::establish(&mem, StoreKind::Filesystem, "/r", &fake, "test")
        .await
        .unwrap_err();
    assert_eq!(
        err,
        StoreProblem::Transport("extenddb-store.json is neither readable nor creatable".to_owned())
    );
    assert!(mem.keys().is_empty(), "nothing was created");
}

/// The rows for the root carry an id the file does not, or more than one id,
/// or the root has no file while rows exist: each is unhealthy.
#[tokio::test]
async fn ids_that_disagree_or_a_missing_file_with_rows_are_unhealthy() {
    // The file holds another id than the rows.
    let mem = MemStore::default();
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-2", "catalog-1"));
    let fake = fake_with_row(BackupStatus::Available);
    let err = store_identity::establish(
        &mem,
        StoreKind::Filesystem,
        store().store_root.as_str(),
        &fake,
        "test",
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        StoreProblem::IdMismatch {
            file_id: "store-2".to_owned(),
            row_ids: vec!["store-1".to_owned()],
        }
    );
    assert_eq!(err.category(), "identity mismatch");

    // No file, rows exist.
    let mem = MemStore::default();
    let err = store_identity::establish(
        &mem,
        StoreKind::Filesystem,
        store().store_root.as_str(),
        &fake,
        "test",
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        StoreProblem::FileMissingWithRows(vec!["store-1".to_owned()])
    );
    assert!(mem.keys().is_empty(), "nothing is created over rows");
    // The file put back: healthy.
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
    store_identity::establish(
        &mem,
        StoreKind::Filesystem,
        store().store_root.as_str(),
        &fake,
        "test",
    )
    .await
    .unwrap();

    // Rows carrying two ids.
    let fake = fake_with_row(BackupStatus::Available);
    let mut other = row(&newer_arn(ACCOUNT), BackupStatus::Available);
    other.store.store_id = "store-9".to_owned();
    fake.rows.lock().unwrap().insert(newer_arn(ACCOUNT), other);
    let err = store_identity::establish(
        &mem,
        StoreKind::Filesystem,
        store().store_root.as_str(),
        &fake,
        "test",
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        StoreProblem::MultipleRowIds(vec!["store-1".to_owned(), "store-9".to_owned()])
    );
}

/// The manifest probe: the newest AVAILABLE row's manifest must be under the
/// root. A second instance with a host-local root at the same path and the
/// first's file copied to it has the file and none of the objects, which is
/// what the probe catches; a root with no AVAILABLE row is not probed.
#[tokio::test]
async fn the_manifest_probe_reads_the_newest_available_rows_manifest() {
    let mem = MemStore::default();
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = fake_with_rows(&[
        (arn(ACCOUNT), BackupStatus::Available),
        (newer_arn(ACCOUNT), BackupStatus::Available),
        (
            music_arn(ACCOUNT, "01791976999999-0bad0bad"),
            BackupStatus::Creating,
        ),
    ]);
    // Only the older manifest is there: the newest is probed and missing.
    mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
    let err = store_identity::establish(
        &mem,
        StoreKind::Filesystem,
        &store().store_root,
        &fake,
        "test",
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        StoreProblem::ManifestMissing {
            backup_arn: newer_arn(ACCOUNT),
        }
    );
    assert_eq!(err.category(), "manifest probe failed");
    mem.insert(&manifest_key(&newer_arn(ACCOUNT)), b"{}");
    store_identity::establish(
        &mem,
        StoreKind::Filesystem,
        &store().store_root,
        &fake,
        "test",
    )
    .await
    .unwrap();
    // The CREATING row's manifest is not probed: it has none yet.
    // A root with rows but none AVAILABLE is not probed either.
    let fake = fake_with_rows(&[(arn(ACCOUNT), BackupStatus::Creating)]);
    let empty = MemStore::default();
    empty.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    store_identity::establish(
        &empty,
        StoreKind::Filesystem,
        &store().store_root,
        &fake,
        "test",
    )
    .await
    .unwrap();
}

/// A store that ignores the conditional put makes the create a plain put: a
/// race lost before the re-read leaves another id in the file, which the
/// re-read catches, naming both ids.
#[tokio::test]
async fn a_create_race_lost_on_a_store_ignoring_the_condition_is_caught_by_the_re_read() {
    let mem = MemStore::default();
    *mem.overwrite_after_conditional_put.lock().unwrap() =
        Some(identity_file("the-other-instance", "catalog-1"));
    let fake = Fake::default();
    let err = store_identity::establish(&mem, StoreKind::S3, "s3://b/", &fake, "test")
        .await
        .unwrap_err();
    match err {
        StoreProblem::CreateRaceLost { ours, file } => {
            assert_eq!(file, "the-other-instance");
            assert_ne!(ours, file);
            let reason = StoreProblem::CreateRaceLost {
                ours: ours.clone(),
                file: file.clone(),
            }
            .public_reason();
            assert!(reason.contains(&ours) && reason.contains(&file), "{reason}");
        }
        other => panic!("expected CreateRaceLost, got {other:?}"),
    }
}

/// An unreachable store is unhealthy with the store's error in the log line
/// and not in the `/health` text; the check that runs again (the
/// reconciler's) recovers it once the store answers.
#[tokio::test]
async fn an_unreachable_store_is_unhealthy_and_recovers_on_recheck() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    mem.set_down(true);
    let fake = Fake::default();
    let err = store_identity::establish(
        &*mem,
        StoreKind::Filesystem,
        &store().store_root,
        &fake,
        "test",
    )
    .await
    .unwrap_err();
    // The operator's line carries the whole cause chain: the top link names
    // the class ("dispatch failure"), the cause is the link below it.
    assert!(
        matches!(&err, StoreProblem::Transport(m)
            if m.contains("dispatch failure") && m.contains("connection refused")),
        "{err:?}"
    );
    assert_eq!(err.public_reason(), "the store could not be reached");
    assert!(err.to_string().contains("connection refused"));

    let runtime = runtime_unhealthy(&mem, err);
    assert_eq!(
        runtime.configured_store().unwrap().identity(),
        None,
        "the id is unknown until a check passes"
    );
    let report = runtime.store_health().unwrap();
    assert_eq!(report.kind, "filesystem");
    assert_eq!(report.status, "unhealthy");
    assert_eq!(report.reason, Some("unreachable"));

    // Still down: the cycle leaves it unhealthy.
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.store_healthy, Some(false));
    // Up again: the cycle's recheck passes and the component recovers.
    mem.set_down(false);
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.store_healthy, Some(true));
    assert_eq!(runtime.store_health().unwrap().status, "healthy");
    assert_eq!(runtime.store_health().unwrap().reason, None);
    assert_eq!(
        runtime.configured_store().unwrap().identity(),
        Some(store()),
        "the check that passed settled the id from the file"
    );
    // And back to unhealthy when it goes away again.
    mem.set_down(true);
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.store_healthy, Some(false));
}

/// A store that has stopped answering costs a cycle its check bound and
/// nothing more: the dead-owner duties ran before the check, the check is
/// abandoned at `check_timeout` (5 s for a filesystem root) and recorded as
/// a transport failure naming the bound, the cycle returns, and the next
/// cycle on a store that answers again recovers it.
#[tokio::test(start_paused = true)]
async fn a_stalled_store_check_is_abandoned_at_its_bound() {
    use extenddb_storage::backup_catalog::RestoreKind;
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    mem.set_stalled(true);
    let runtime = runtime_on(&mem, store());
    let configured = runtime.configured_store().unwrap();
    assert_eq!(configured.check_timeout(), LOCAL_CHECK_TIMEOUT);
    let fake = Fake::default();
    *fake.record_still_running.lock().unwrap() = true;
    *fake.target_still_creating.lock().unwrap() = true;
    fake.dead_jobs
        .lock()
        .unwrap()
        .push(dead_job(RestoreKind::Legacy, "LegacyTarget"));

    let started = tokio::time::Instant::now();
    // The test's own bound, far past the check's: a cycle that waits on the
    // store for longer than it fails here instead of hanging.
    let report = tokio::time::timeout(
        Duration::from_secs(120),
        reconciler::cycle(
            &fake,
            &runtime,
            Duration::from_secs(300),
            "test",
            &mut reconciler::ReconcilerState::default(),
        ),
    )
    .await
    .expect("the cycle returns within the check's bound")
    .unwrap();
    assert_eq!(
        started.elapsed(),
        LOCAL_CHECK_TIMEOUT,
        "the check's bound is the whole cost"
    );
    assert_eq!(
        report.legacy_restores_removed,
        ["LegacyTarget"],
        "the duties ran"
    );
    assert_eq!(report.store_healthy, Some(false));
    assert_eq!(
        configured.health(),
        StoreHealth::Unhealthy(StoreProblem::Transport(
            "the check did not finish within 5 s".to_owned()
        ))
    );
    assert_eq!(runtime.store_health().unwrap().reason, Some("unreachable"));
    assert_eq!(mem.stalls.load(std::sync::atomic::Ordering::Acquire), 1);
    // The abandoned check's call is still in the store (on a filesystem
    // root, a thread parked on the mount). A cycle during the stall starts
    // no second check: it returns at once, the store stays `unreachable`
    // with the reason, and the store sees no second call.
    assert!(configured.check_call_in_flight());
    let started = tokio::time::Instant::now();
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        started.elapsed(),
        Duration::ZERO,
        "a cycle that starts no check waits no bound"
    );
    assert_eq!(report.store_healthy, Some(false));
    assert_eq!(
        configured.health(),
        StoreHealth::Unhealthy(StoreProblem::Transport(
            "a call of an earlier check has not returned".to_owned()
        ))
    );
    assert_eq!(
        mem.stalls.load(std::sync::atomic::Ordering::Acquire),
        1,
        "no second call reached the store while the first was in flight"
    );
    // Checking stores wait the same bound: an S3 store's is 30 s, under the
    // SDK's attempt timeout so no retry outlives the check.
    let s3 = ConfiguredStore::checking(
        StoreKind::S3,
        "s3://b/".to_owned(),
        Arc::clone(&mem) as Arc<dyn extenddb_backup_store::BackupStore>,
        None,
    );
    assert_eq!(s3.check_timeout(), S3_CHECK_TIMEOUT);
    assert_eq!(S3_CHECK_TIMEOUT, Duration::from_secs(30));
    assert!(S3_CHECK_TIMEOUT < extenddb_backup_store::S3_ATTEMPT_TIMEOUT);
    // An S3 store's abandoned request is dropped with the check (the SDK
    // would otherwise retry it past the bound), so nothing is in flight
    // afterwards and the next cycle sends a fresh request.
    let s3_runtime = BackupRuntime::new(4).with_store(s3);
    let s3 = s3_runtime.configured_store().unwrap();
    let calls_before = mem.stalls.load(std::sync::atomic::Ordering::Acquire);
    for _ in 0..2 {
        let started = tokio::time::Instant::now();
        reconciler::cycle(
            &fake,
            &s3_runtime,
            Duration::from_secs(300),
            "test",
            &mut reconciler::ReconcilerState::default(),
        )
        .await
        .unwrap();
        assert_eq!(started.elapsed(), S3_CHECK_TIMEOUT);
        assert!(
            !s3.check_call_in_flight(),
            "an S3 request ends with the check"
        );
    }
    assert_eq!(
        mem.stalls.load(std::sync::atomic::Ordering::Acquire),
        calls_before + 2,
        "each cycle's check reached the endpoint"
    );

    // The store answers again: the parked call returns (its task clears the
    // in-flight mark), and the next cycle's check passes.
    mem.set_stalled(false);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !configured.check_call_in_flight(),
        "the parked call returned once the store answered"
    );
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.store_healthy, Some(true));
    assert!(configured.is_healthy());
}

/// A shutdown that fires while a cycle waits on the store's check ends the
/// cycle at once (the health is left as it was), so the worker drains
/// without waiting the check's bound.
#[tokio::test(start_paused = true)]
async fn a_shutdown_during_the_store_check_ends_the_cycle() {
    use extenddb_core::metrics::MetricsCollector;
    let mem = Arc::new(MemStore::default());
    mem.set_stalled(true);
    // An S3 store, whose bound (30 s) is the longer one.
    let runtime = Arc::new(runtime().with_store(ConfiguredStore::checking(
        StoreKind::S3,
        "s3://b/".to_owned(),
        Arc::clone(&mem) as Arc<dyn extenddb_backup_store::BackupStore>,
        None,
    )));
    let fake = Arc::new(Fake::default());
    let shutdown = extenddb_storage::CancellationToken::new();
    let worker = tokio::spawn(reconciler::run(
        Arc::clone(&fake),
        Arc::clone(&runtime),
        reconciler::ReconcilerSettings {
            interval: Duration::from_secs(3600),
            stale_job_after: Duration::from_secs(300),
        },
        "test".to_owned(),
        Arc::new(MetricsCollector::new()),
        Arc::new(tokio::sync::Mutex::new(
            reconciler::ReconcilerState::default(),
        )),
        shutdown.clone(),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mem.stalls.load(std::sync::atomic::Ordering::Acquire) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first cycle's check reached the store"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let cancelled = tokio::time::Instant::now();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("the worker stops at shutdown without waiting the check's bound")
        .unwrap();
    assert!(
        cancelled.elapsed() < Duration::from_secs(1),
        "the drain took {:?}",
        cancelled.elapsed()
    );
    assert_eq!(
        runtime.configured_store().unwrap().health(),
        StoreHealth::Checking,
        "a check shutdown ended records nothing"
    );
}

/// A command's identity read (`backup list`, the purge's dry run) runs under
/// the store's check bound as the server's check does: a store that does not
/// answer costs the command the bound (5 s for a filesystem root) and
/// reports it, where the unbounded read would wait out the store's retries
/// or, on a stalled mount, never return; a store that answers is read.
#[tokio::test(start_paused = true)]
async fn a_commands_identity_read_is_bounded_as_the_check_is() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let configured = ConfiguredStore::checking(
        StoreKind::Filesystem,
        store().store_root,
        Arc::clone(&mem) as Arc<dyn extenddb_backup_store::BackupStore>,
        None,
    );
    let read = runtime::read_identity_bounded(&configured, &fake)
        .await
        .unwrap();
    assert_eq!(read.map(|i| i.store_id), Some("store-1".to_owned()));

    mem.set_stalled(true);
    let started = tokio::time::Instant::now();
    // The test's own bound, far past the read's: an unbounded read fails
    // here instead of hanging.
    let problem = tokio::time::timeout(
        Duration::from_secs(120),
        runtime::read_identity_bounded(&configured, &fake),
    )
    .await
    .expect("the read returns at its bound")
    .unwrap_err();
    assert_eq!(started.elapsed(), LOCAL_CHECK_TIMEOUT);
    assert_eq!(
        problem,
        StoreProblem::Transport("the identity read did not finish within 5 s".to_owned())
    );
    assert_eq!(problem.category(), "unreachable");
    assert_eq!(mem.stalls.load(std::sync::atomic::Ordering::Acquire), 1);
    mem.set_stalled(false);
}

/// `record_check` logs a transition once per category: it reports a change
/// of category, and not a new text within the same one, so a store that
/// stays down with a varying error costs one line.
#[test]
fn record_check_reports_a_transition_only_across_categories() {
    let mem = Arc::new(MemStore::default());
    let runtime = runtime_on(&mem, store());
    let store = runtime.configured_store().unwrap();
    assert!(store.record_check(Err(StoreProblem::Transport("refused".to_owned()))));
    assert!(
        !store.record_check(Err(StoreProblem::Transport("timed out".to_owned()))),
        "a new text in the same category is not a transition"
    );
    assert_eq!(
        store.health(),
        StoreHealth::Unhealthy(StoreProblem::Transport("timed out".to_owned())),
        "the latest text is kept"
    );
    assert!(store.record_check(Err(StoreProblem::Validate("no bucket".to_owned()))));
    assert!(store.record_check(Ok(self::store())));
    assert!(!store.record_check(Ok(self::store())));
    assert!(
        !store.record_check(Err(StoreProblem::Catalog("pool exhausted".to_owned()))),
        "a catalog read failure changes nothing"
    );
}

/// A recheck of a root that lost its file while the catalog has no rows for
/// it writes the file back with the same id.
#[tokio::test]
async fn a_recheck_rewrites_a_lost_file_when_no_rows_exist() {
    let mem = MemStore::default();
    let fake = Fake::default();
    store_identity::recheck(&mem, &store(), &fake, "test")
        .await
        .unwrap();
    let file = StoreIdentityFile::parse(&mem.objects.lock().unwrap()[STORE_IDENTITY_KEY]).unwrap();
    assert_eq!(file.store_id, "store-1");
    assert_eq!(file.catalog_id, "catalog-1");
    // A file that changed under the running server (the root replaced) is
    // an id mismatch.
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-2", "catalog-1"));
    let err = store_identity::recheck(&mem, &store(), &fake, "test")
        .await
        .unwrap_err();
    assert_eq!(
        err,
        StoreProblem::IdMismatch {
            file_id: "store-2".to_owned(),
            row_ids: vec!["store-1".to_owned()],
        }
    );
}

/// No store configured: `/health` has no component and no operation is
/// refused for the store.
#[test]
fn no_configured_store_reports_no_component() {
    assert_eq!(runtime().store_health(), None);
    assert_eq!(
        runtime_with_store().store_health().unwrap().status,
        "healthy"
    );
}

/// The `/health` reason is the problem's category and nothing else: the
/// endpoint is unauthenticated, and the ARN (account, table and backup id)
/// a manifest probe names, and the catalog and store ids the identity
/// problems name, go to the operator's log line only.
#[test]
fn the_health_reason_is_the_category_only() {
    let mem = Arc::new(MemStore::default());
    for (problem, category) in [
        (
            StoreProblem::ManifestMissing {
                backup_arn: arn("999988887777"),
            },
            "manifest probe failed",
        ),
        (
            StoreProblem::ForeignCatalog {
                file_catalog_id: "cat-other".to_owned(),
                catalog_id: "cat-mine".to_owned(),
            },
            "owned by another catalog",
        ),
        (
            StoreProblem::IdMismatch {
                file_id: "store-9".to_owned(),
                row_ids: vec!["store-1".to_owned()],
            },
            "identity mismatch",
        ),
        (
            StoreProblem::FileMissingWithRows(vec!["store-1".to_owned()]),
            "identity file missing",
        ),
        (
            StoreProblem::Transport("connect /var/lib/extenddb: refused".to_owned()),
            "unreachable",
        ),
    ] {
        let report = runtime_unhealthy(&mem, problem.clone())
            .store_health()
            .unwrap();
        assert_eq!(report.status, "unhealthy");
        assert_eq!(report.reason, Some(category), "{problem:?}");
        for secret in ["999988887777", "cat-", "store-", "/var/lib", "arn:"] {
            assert!(
                !report.reason.unwrap().contains(secret),
                "{problem:?}: the reason carries {secret:?}"
            );
        }
    }
}

/// An identity file over the read bound is reported unreadable without
/// being read in full; one under it parses.
#[tokio::test]
async fn an_oversized_identity_file_is_unreadable() {
    let mem = MemStore::default();
    let fake = Fake::default();
    let mut big = identity_file("store-1", "catalog-1");
    big.resize(store_identity::STORE_IDENTITY_MAX_BYTES + 1, b' ');
    mem.insert(STORE_IDENTITY_KEY, &big);
    let err = store_identity::establish(&mem, StoreKind::Filesystem, "/r", &fake, "test")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, StoreProblem::FileUnreadable(m) if m.contains("larger than 65536 bytes")),
        "{err:?}"
    );
    assert_eq!(err.category(), "identity file unreadable");
    // Trailing blanks up to the bound are JSON whitespace: the file parses.
    let mut padded = identity_file("store-1", "catalog-1");
    padded.resize(store_identity::STORE_IDENTITY_MAX_BYTES, b' ');
    mem.insert(STORE_IDENTITY_KEY, &padded);
    let identity = store_identity::establish(&mem, StoreKind::Filesystem, "/r", &fake, "test")
        .await
        .unwrap();
    assert_eq!(identity.store_id, "store-1");
}

/// A check that could not read the catalog leaves the store's health as the
/// last check left it, healthy or unhealthy, and keeps its id.
#[tokio::test]
async fn a_catalog_read_failure_leaves_the_health_unchanged() {
    let mem = Arc::new(MemStore::default());
    let healthy = runtime_on(&mem, store());
    let store = healthy.configured_store().unwrap();
    store.record_check(Err(StoreProblem::Catalog("pool exhausted".to_owned())));
    assert_eq!(store.health(), StoreHealth::Healthy);
    assert_eq!(store.identity(), Some(self::store()));
    let unhealthy = runtime_unhealthy(&mem, StoreProblem::Transport("refused".to_owned()));
    let store = unhealthy.configured_store().unwrap();
    store.record_check(Err(StoreProblem::Catalog("pool exhausted".to_owned())));
    assert_eq!(
        store.health(),
        StoreHealth::Unhealthy(StoreProblem::Transport("refused".to_owned()))
    );
    // And through a cycle: a catalog that fails `catalog_id` leaves a
    // healthy store healthy.
    let fake = Fake::default();
    *fake.catalog_id_error.lock().unwrap() = true;
    let report = reconciler::cycle(
        &fake,
        &healthy,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.store_healthy, Some(true));
    // A problem of another category replaces the last one; one of the same
    // category is kept as the latest text.
    let store = healthy.configured_store().unwrap();
    store.record_check(Err(StoreProblem::Transport("refused".to_owned())));
    store.record_check(Err(StoreProblem::Transport("timed out".to_owned())));
    assert_eq!(
        store.health(),
        StoreHealth::Unhealthy(StoreProblem::Transport("timed out".to_owned()))
    );
}

/// A store whose checks run after the listener is up is `checking` until
/// the first check records: `/health` says so with no reason, the
/// operations that touch the store refuse naming the pending check, and
/// the check runs `validate` first (a store that cannot be reached fails
/// there, with its id still unknown), then the identity check, which
/// settles the id.
#[tokio::test]
async fn a_store_checked_after_startup_is_checking_until_the_first_check() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    mem.set_down(true);
    let runtime = runtime().with_store(ConfiguredStore::checking(
        store().store_kind,
        store().store_root,
        Arc::clone(&mem) as Arc<dyn extenddb_backup_store::BackupStore>,
        None,
    ));
    let configured = runtime.configured_store().unwrap();
    assert_eq!(configured.health(), StoreHealth::Checking);
    assert!(!configured.is_validated());
    let report = runtime.store_health().unwrap();
    assert_eq!((report.status, report.reason), ("checking", None));
    let err = runtime.require_healthy_store().err().unwrap();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m) if m.contains("first check is pending")),
        "{err:?}"
    );
    // The restore check of a row of the store refuses the same way, before
    // the permit: with no slot free, the refusal is still the store's.
    let fake = fake_with_row(BackupStatus::Available);
    let full = BackupRuntime::new(0).with_store(ConfiguredStore::checking(
        store().store_kind,
        store().store_root,
        Arc::clone(&mem) as Arc<dyn extenddb_backup_store::BackupStore>,
        None,
    ));
    let err = restore_on(&fake, &full, &restore_body(json!({})))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m) if m.contains("first check is pending")),
        "{err:?}"
    );

    // The first check: `validate` fails on the unreachable store.
    let fake = Fake::default();
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.store_healthy, Some(false));
    assert!(!configured.is_validated());
    assert_eq!(runtime.store_health().unwrap().reason, Some("unreachable"));
    assert_eq!(configured.identity(), None);
    // Reachable: `validate` passes once and is not run again, the identity
    // check settles the id, and the store is healthy.
    mem.set_down(false);
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.store_healthy, Some(true));
    assert!(configured.is_validated());
    assert_eq!(configured.identity(), Some(store()));
    assert_eq!(runtime.store_health().unwrap().status, "healthy");
}

/// An unhealthy store refuses the operations that touch it, before a slot is
/// taken and naming the store's kind and the problem's category and never
/// its root or the cause: CreateBackup (store-backed on this runtime),
/// DeleteBackup of a row of the store, and the restore of one (with no slot
/// free it is still the store's refusal, not LimitExceeded). A backend
/// bound to the in-catalog create is not refused: its create touches no
/// store. A row of another store is refused as another store's, whatever
/// the configured one's health.
#[tokio::test]
async fn an_unhealthy_store_refuses_create_delete_and_restore_unless_bound_to_the_in_catalog_create()
 {
    let mem = Arc::new(MemStore::default());
    let unhealthy = || {
        runtime_unhealthy(
            &mem,
            StoreProblem::Transport("connect /var/lib/extenddb/backups: refused".to_owned()),
        )
    };
    let check = |e: DynamoDbError| {
        let DynamoDbError::ValidationException(m) = e else {
            panic!("expected ValidationException, got {e:?}");
        };
        assert!(m.contains("filesystem backup store"), "{m}");
        assert!(m.contains("unreachable"), "{m}");
        assert!(!m.contains("/var/lib"), "{m}");
        assert!(!m.contains("connect"), "{m}");
    };

    // The store-backed create is refused before the permit and the open.
    let fake = Arc::new(Fake::default());
    let runtime = unhealthy();
    let err = create_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Ok(active_table("Music")),
    )
    .await
    .unwrap_err();
    check(err);
    assert_eq!(runtime.free_slots(), 4, "no slot was taken");
    assert!(fake.snapshots_opened.lock().unwrap().is_empty());
    assert!(fake.creates.lock().unwrap().is_empty());

    // A backend bound to the in-catalog create runs it, taking its slot.
    let runtime = unhealthy().with_legacy_create_only(true);
    create_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Ok(active_table("Music")),
    )
    .await
    .unwrap();
    assert_eq!(fake.creates.lock().unwrap().len(), 1);
    assert_eq!(runtime.free_slots(), 3, "the create holds its slot");
    fake.permits.lock().unwrap().clear();

    // DeleteBackup of a row of the store.
    let fake = Arc::new(fake_with_row(BackupStatus::Available));
    let err = delete_backup(&*fake, &unhealthy(), ACCOUNT, REGION, &own(&arn(ACCOUNT)))
        .await
        .unwrap_err();
    check(err);
    assert!(
        fake.delete_starts_seen.lock().unwrap().is_empty(),
        "no DELETING write"
    );

    // The restore of a row of the store, with no slot free: the refusal is
    // the store's, so it came before the permit.
    let full = BackupRuntime::new(0).with_store(ConfiguredStore::new(
        store().store_kind,
        store().store_root,
        Arc::clone(&mem) as Arc<dyn extenddb_backup_store::BackupStore>,
        None,
        Err(StoreProblem::Transport(
            "connect /var/lib/extenddb/backups: refused".to_owned(),
        )),
    ));
    let err = restore_on(&fake, &full, &restore_body(json!({})))
        .await
        .unwrap_err();
    check(err);
    assert!(fake.restores().is_empty(), "the backend was not asked");

    // A row of another store is the other-store refusal, whatever the health
    // of the configured one.
    let mut other = row(&arn(ACCOUNT), BackupStatus::Available);
    other.store.store_root = "/elsewhere".to_owned();
    let fake = Arc::new(Fake::default());
    fake.rows.lock().unwrap().insert(arn(ACCOUNT), other);
    let err = delete_backup(&*fake, &unhealthy(), ACCOUNT, REGION, &own(&arn(ACCOUNT)))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m) if m.contains("other than")),
        "{err:?}"
    );
}

/// With a healthy store configured, CreateBackup is store-backed: the
/// legacy reader is not asked, the table's snapshot is opened and the answer
/// is CREATING; a table with vector indexes is backed up on this path (the
/// refusal belongs to the in-catalog create alone).
#[tokio::test]
async fn a_healthy_store_makes_create_backup_store_backed() {
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let mut table = music_table();
    table.vector_indexes = Some(vec![vector_index()]);
    snapshot_fixture(&fake, &table, Vec::new(), true, None);
    let runtime = job_runtime(&mem);
    let details = create_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Ok(table),
    )
    .await
    .unwrap();
    assert_eq!(details.backup_status, "CREATING");
    assert!(
        fake.creates.lock().unwrap().is_empty(),
        "the legacy reader was not asked"
    );
    assert_eq!(fake.snapshots_opened.lock().unwrap().as_slice(), ["Music"]);
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Available);
    assert_eq!(
        row.source_table_details_json["vector_indexes"][0]["IndexName"],
        "v"
    );
}

/// DeleteBackup of a row of the configured store deletes the manifest first
/// (a `delete_prefix` on its exact key), then the prefix, and moves the row
/// to DELETED once the prefix lists nothing; a retry on a DELETING row runs
/// the same deletion. The identity file and other backups' prefixes stay.
#[tokio::test]
async fn delete_backup_of_a_configured_store_row_deletes_manifest_prefix_then_marks_deleted() {
    let mem = Arc::new(MemStore::default());
    let prefix = BackupArn::parse(&arn(ACCOUNT)).unwrap().store_prefix();
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
    mem.insert(&format!("{prefix}data/00001.json.gz"), b"gz");
    mem.insert(&format!("{prefix}manifest-files.json"), b"{}");
    mem.insert(&manifest_key(&newer_arn(ACCOUNT)), b"{}");
    let fake = fake_with_rows(&[
        (arn(ACCOUNT), BackupStatus::Deleting),
        (newer_arn(ACCOUNT), BackupStatus::Available),
    ]);
    let runtime = runtime_on(&mem, store());
    let desc = delete_backup(&fake, &runtime, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
        .await
        .unwrap();
    assert_eq!(desc.backup_details.backup_status, "DELETED");
    assert_eq!(
        mem.deletes.lock().unwrap().as_slice(),
        [manifest_key(&arn(ACCOUNT)), prefix.clone()],
        "the manifest first, then the prefix"
    );
    assert_eq!(
        mem.keys(),
        [
            manifest_key(&newer_arn(ACCOUNT)),
            STORE_IDENTITY_KEY.to_owned()
        ],
        "the other backup and the identity file stay"
    );
    assert_eq!(fake.set_deleted.lock().unwrap().as_slice(), [arn(ACCOUNT)]);
    assert_eq!(
        fake.rows.lock().unwrap()[&arn(ACCOUNT)].status,
        BackupStatus::Deleted
    );
}

/// A store failure during the deletion leaves the row DELETING and still
/// answers the description with `BackupStatus` DELETED (the service has no
/// counterpart for a failed object deletion, and every reader reports the
/// backup deleted from the DELETING write on); the reconciler's next cycle
/// finishes it once the store answers, and leaves it alone while the store
/// is unhealthy.
#[tokio::test]
async fn a_failed_deletion_stays_deleting_and_the_reconciler_finishes_it() {
    let mem = Arc::new(MemStore::default());
    let prefix = BackupArn::parse(&arn(ACCOUNT)).unwrap().store_prefix();
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
    mem.insert(&format!("{prefix}data/00001.json.gz"), b"gz");
    let fake = fake_with_rows(&[(arn(ACCOUNT), BackupStatus::Deleting)]);
    let runtime = runtime_on(&mem, store());

    mem.set_down(true);
    let out = delete_backup(&fake, &runtime, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
        .await
        .unwrap();
    assert_eq!(
        out.backup_details.backup_status, "DELETED",
        "the description answers DELETED once DELETING committed"
    );
    assert_eq!(
        fake.rows.lock().unwrap()[&arn(ACCOUNT)].status,
        BackupStatus::Deleting
    );
    assert_eq!(mem.keys().len(), 3, "nothing was deleted");

    // The reconciler finds the store down: unhealthy, and no row touched.
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.store_healthy, Some(false));
    assert!(report.deletions_finished.is_empty());
    assert!(runtime.require_healthy_store().is_err());

    // The store answers again: the cycle recovers the health and finishes
    // the deletion.
    mem.set_down(false);
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.store_healthy, Some(true));
    assert_eq!(report.deletions_finished, [arn(ACCOUNT)]);
    assert_eq!(mem.keys(), [STORE_IDENTITY_KEY]);
    assert_eq!(
        fake.rows.lock().unwrap()[&arn(ACCOUNT)].status,
        BackupStatus::Deleted
    );
    // Nothing left to finish.
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(report.deletions_finished.is_empty());
}

/// The reconciler finishes DELETING rows of the configured store only: a
/// DELETING row of another store is left alone, and so is one whose prefix
/// still lists an object after the deletion (a put that overlapped it).
#[tokio::test]
async fn the_reconciler_finishes_only_the_configured_stores_deleting_rows() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = fake_with_rows(&[(arn(ACCOUNT), BackupStatus::Deleting)]);
    let mut other = row(&newer_arn(ACCOUNT), BackupStatus::Deleting);
    other.store.store_id = "store-9".to_owned();
    other.store.store_root = "/elsewhere".to_owned();
    fake.rows.lock().unwrap().insert(newer_arn(ACCOUNT), other);
    let runtime = runtime_on(&mem, store());
    let report = reconciler::cycle(
        &fake,
        &runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.deletions_finished, [arn(ACCOUNT)]);
    assert_eq!(
        fake.rows.lock().unwrap()[&newer_arn(ACCOUNT)].status,
        BackupStatus::Deleting,
        "another store's row is not touched"
    );
    assert!(
        mem.deletes
            .lock()
            .unwrap()
            .iter()
            .all(|p| !p.contains("0badf00d")),
        "nothing of the other store's prefix was deleted"
    );
}

/// `finish_backup_delete` reports a prefix that still lists an object after
/// the deletion and leaves the row DELETING for the next run.
#[tokio::test]
async fn a_prefix_that_still_lists_an_object_is_left_deleting() {
    struct Refilling(MemStore);
    impl extenddb_backup_store::BackupStore for Refilling {
        fn put<'a>(
            &'a self,
            key: &'a str,
            body: extenddb_backup_store::ByteStream,
        ) -> BoxedFuture<
            'a,
            Result<extenddb_backup_store::ObjectMeta, extenddb_backup_store::StoreError>,
        > {
            self.0.put(key, body)
        }
        fn put_if_absent<'a>(
            &'a self,
            key: &'a str,
            body: extenddb_backup_store::ByteStream,
        ) -> BoxedFuture<
            'a,
            Result<Option<extenddb_backup_store::ObjectMeta>, extenddb_backup_store::StoreError>,
        > {
            self.0.put_if_absent(key, body)
        }
        fn get<'a>(
            &'a self,
            key: &'a str,
        ) -> BoxedFuture<
            'a,
            Result<extenddb_backup_store::ByteStream, extenddb_backup_store::StoreError>,
        > {
            self.0.get(key)
        }
        fn head<'a>(
            &'a self,
            key: &'a str,
        ) -> BoxedFuture<
            'a,
            Result<Option<extenddb_backup_store::ObjectMeta>, extenddb_backup_store::StoreError>,
        > {
            self.0.head(key)
        }
        fn list<'a>(
            &'a self,
            prefix: &'a str,
        ) -> BoxStream<
            'a,
            Result<extenddb_backup_store::ObjectMeta, extenddb_backup_store::StoreError>,
        > {
            self.0.list(prefix)
        }
        /// The deletion runs, and a late put lands under the prefix after
        /// it.
        fn delete_prefix<'a>(
            &'a self,
            prefix: &'a str,
        ) -> BoxedFuture<'a, Result<u64, extenddb_backup_store::StoreError>> {
            Box::pin(async move {
                let n = self.0.delete_prefix(prefix).await?;
                if !prefix.ends_with(MANIFEST_KEY_SUFFIX) {
                    self.0
                        .insert(&format!("{prefix}data/late.json.gz"), b"late");
                }
                Ok(n)
            })
        }
        fn validate(&self) -> BoxedFuture<'_, Result<(), extenddb_backup_store::StoreError>> {
            self.0.validate()
        }
    }
    let store_handle = Refilling(MemStore::default());
    let fake = fake_with_rows(&[(arn(ACCOUNT), BackupStatus::Deleting)]);
    let parsed = BackupArn::parse(&arn(ACCOUNT)).unwrap();
    let err = finish_backup_delete(&fake, &store_handle, &parsed)
        .await
        .unwrap_err();
    assert!(err.contains("still lists"), "{err}");
    assert_eq!(
        fake.rows.lock().unwrap()[&arn(ACCOUNT)].status,
        BackupStatus::Deleting
    );
}

// --- the second review round: the store's health, the purge's steps, the orders --

mod review_round_2 {
    use super::*;
    use extenddb_storage::backup_catalog::{JobStatus, RestoreKind};
    use maintenance::{
        Interrupt, PurgeOutcome, Stop, Wait, WaitOutcome, purge_rows, wait_for_quiet,
    };

    async fn cycle_on(fake: &Fake, runtime: &BackupRuntime) -> reconciler::CycleReport {
        reconciler::cycle(
            fake,
            runtime,
            Duration::from_secs(300),
            "test",
            &mut reconciler::ReconcilerState::default(),
        )
        .await
        .unwrap()
    }

    /// A store whose startup `validate` failed (an I/O error on the probe
    /// write) is built unvalidated, so the next check runs `validate` again
    /// and the store stays unhealthy while the probe fails, where a store
    /// built validated would re-read the identity file alone and report
    /// itself healthy after one cycle. Once the probe passes the store is
    /// healthy.
    #[tokio::test]
    async fn a_store_whose_startup_validate_failed_is_rechecked_by_validate() {
        let mem = Arc::new(MemStore::default());
        mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
        *mem.validate_error.lock().unwrap() =
            Some("write probe: Input/output error (os error 5)".to_owned());
        let fake = Fake::default();
        let runtime = runtime_unhealthy(
            &mem,
            StoreProblem::Validate("write probe: Input/output error (os error 5)".to_owned()),
        );
        let configured = runtime.configured_store().unwrap();
        assert!(
            !configured.is_validated(),
            "a failed validate leaves the store unvalidated"
        );
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(report.store_healthy, Some(false));
        assert_eq!(
            configured.health(),
            StoreHealth::Unhealthy(StoreProblem::Validate(
                "write probe: Input/output error (os error 5)".to_owned()
            )),
            "validate ran again and failed again"
        );
        assert!(!configured.is_validated());
        assert_eq!(runtime.store_health().unwrap().status, "unhealthy");

        *mem.validate_error.lock().unwrap() = None;
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(report.store_healthy, Some(true));
        assert!(configured.is_validated());
        assert_eq!(
            configured.identity().map(|i| i.store_id),
            Some("store-1".to_owned())
        );

        // A startup check that failed after `validate` passed (an identity
        // problem) is built validated: the probe is not repeated.
        let identity_problem = runtime_unhealthy(
            &mem,
            StoreProblem::ForeignCatalog {
                file_catalog_id: "other".to_owned(),
                catalog_id: "catalog-1".to_owned(),
            },
        );
        assert!(identity_problem.configured_store().unwrap().is_validated());
        for problem in [
            StoreProblem::Transport("refused".to_owned()),
            StoreProblem::Denied("mode 0755".to_owned()),
        ] {
            assert!(
                !runtime_unhealthy(&mem, problem)
                    .configured_store()
                    .unwrap()
                    .is_validated(),
                "the problems validate can end in leave the store unvalidated"
            );
        }
    }

    /// `purge_row` deletes the manifest before the rest of the prefix (so
    /// the prefix is no longer a complete backup to any reader while the
    /// rest goes), lists the prefix after the deletion and leaves a row whose
    /// prefix still holds an object (a put that overlapped the deletion),
    /// and does not continue past `DeleteStart::InUse`: a restore reading
    /// the backup keeps its source and the row stays AVAILABLE.
    #[tokio::test]
    async fn purge_row_deletes_manifest_first_lists_after_and_stops_at_in_use() {
        // The order: the manifest key, then the prefix.
        let fake = Fake::default();
        let mem = MemStore::default();
        let arn = arn_n(1);
        seed(&fake, &mem, &arn, BackupStatus::Available);
        let prefix = BackupArn::parse(&arn).unwrap().store_prefix();
        let row = fake.rows.lock().unwrap()[&arn].clone();
        maintenance::purge_row(&fake, &mem, &row, Duration::from_secs(300))
            .await
            .unwrap();
        assert_eq!(
            *mem.deletes.lock().unwrap(),
            vec![format!("{prefix}{MANIFEST_KEY_SUFFIX}"), prefix.clone()],
            "the manifest first, then the prefix"
        );
        assert!(mem.keys().is_empty());
        assert!(fake.rows.lock().unwrap().is_empty());

        // An object left under the prefix: the row is left, after the
        // DELETING write, for the operator and the reconciler.
        let fake = Fake::default();
        let mem = MemStore::default();
        *mem.late_put_after_delete.lock().unwrap() = Some("data/late.json.gz".to_owned());
        seed(&fake, &mem, &arn, BackupStatus::Available);
        let row = fake.rows.lock().unwrap()[&arn].clone();
        let err = maintenance::purge_row(&fake, &mem, &row, Duration::from_secs(300))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            format!("the prefix still lists {prefix}data/late.json.gz after the deletion")
        );
        assert_eq!(
            fake.rows.lock().unwrap()[&arn].status,
            BackupStatus::Deleting,
            "the row is left DELETING, not removed, over objects"
        );
        assert!(fake.set_deleted.lock().unwrap().is_empty());
        assert!(fake.removed_rows.lock().unwrap().is_empty());

        // In use: a restore reads the backup, so nothing is deleted.
        let fake = Fake::default();
        let mem = MemStore::default();
        seed(&fake, &mem, &arn, BackupStatus::Available);
        fake.delete_starts
            .lock()
            .unwrap()
            .insert(arn.clone(), DeleteStart::InUse("Restored".to_owned()));
        let row = fake.rows.lock().unwrap()[&arn].clone();
        let err = maintenance::purge_row(&fake, &mem, &row, Duration::from_secs(300))
            .await
            .unwrap_err();
        assert_eq!(err, "a restore into Restored is reading it");
        assert!(mem.deletes.lock().unwrap().is_empty(), "no delete ran");
        assert_eq!(mem.keys().len(), 2, "the objects stay");
        assert_eq!(
            fake.rows.lock().unwrap()[&arn].status,
            BackupStatus::Available
        );
    }

    /// The reconciler's DELETING duty runs only while the store is healthy:
    /// with a foreign `extenddb-store.json` under the root (another
    /// catalog's), and a store that answers deletes, no delete runs and the
    /// row stays DELETING, so the reconciler never deletes under a root
    /// another catalog owns.
    #[tokio::test]
    async fn an_unhealthy_store_whose_deletes_would_answer_has_no_row_touched() {
        let mem = Arc::new(MemStore::default());
        mem.insert(
            STORE_IDENTITY_KEY,
            &identity_file("store-1", "other-catalog"),
        );
        mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
        let fake = fake_with_rows(&[(arn(ACCOUNT), BackupStatus::Deleting)]);
        let runtime = runtime_on(&mem, store());
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(report.store_healthy, Some(false));
        assert_eq!(
            runtime.configured_store().unwrap().health(),
            StoreHealth::Unhealthy(StoreProblem::ForeignCatalog {
                file_catalog_id: "other-catalog".to_owned(),
                catalog_id: "catalog-1".to_owned(),
            })
        );
        assert!(
            report.deletions_finished.is_empty() && mem.deletes.lock().unwrap().is_empty(),
            "no delete ran under a root another catalog owns"
        );
        assert_eq!(
            fake.rows.lock().unwrap()[&arn(ACCOUNT)].status,
            BackupStatus::Deleting
        );
        assert_eq!(mem.keys().len(), 2, "the manifest and the file stay");
    }

    /// The maintenance refusal comes after every other check: with the claim
    /// held, a create under a `server.region` `check_backup_arn_fits`
    /// refuses and a restore whose throughput override does not fit the
    /// backup's billing mode (`validate_override_for_source`) each answer
    /// their own ValidationException, not LimitExceededException.
    #[tokio::test]
    async fn the_maintenance_refusal_comes_after_the_requests_own_checks() {
        let fake = Arc::new(Fake::with_source(source(Some(definition(
            "PAY_PER_REQUEST",
        )))));
        *fake.maintenance_held.lock().unwrap() = true;
        let runtime = BackupRuntime::new(1).with_stale_job_after(Duration::from_secs(7));

        let err = create_backup(
            &fake,
            &runtime,
            ACCOUNT,
            "US_EAST_1",
            &LimitsConfig::default(),
            "Music",
            "nightly",
            Ok(active_table("Music")),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m) if m.contains("server.region")),
            "the region rule answers before the claim: {err:?}"
        );

        let body = restore_body(json!({
            "BillingModeOverride": "PAY_PER_REQUEST",
            "ProvisionedThroughputOverride": {
                "ReadCapacityUnits": 5, "WriteCapacityUnits": 5
            },
        }));
        let err = restore_on(&fake, &runtime, &body).await.unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m)
                if m.contains("ProvisionedThroughputOverride can only be specified")),
            "the override rule answers before the claim: {err:?}"
        );
        assert!(
            fake.maintenance_checks.lock().unwrap().is_empty(),
            "the claim was not read for a request refused on its own terms"
        );

        // The same requests with their own faults removed are refused on the
        // claim, so the claim is the next check.
        let err = create_backup(
            &fake,
            &runtime,
            ACCOUNT,
            REGION,
            &LimitsConfig::default(),
            "Music",
            "nightly",
            Ok(active_table("Music")),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, DynamoDbError::LimitExceededException(_)));
        let err = restore_on(&fake, &runtime, &restore_body(json!({})))
            .await
            .unwrap_err();
        assert!(matches!(err, DynamoDbError::LimitExceededException(_)));
        assert_eq!(fake.maintenance_checks.lock().unwrap().len(), 2);
    }

    /// `recheck` on a store startup settled: rows that arrive carrying
    /// another id are an identity mismatch; a file deleted while rows exist
    /// is `identity file missing` and is not rewritten; a file replaced with
    /// another catalog's is `owned by another catalog`. Each runs through
    /// the reconciler, as a running server runs it.
    #[tokio::test]
    async fn a_recheck_catches_rows_with_another_id_a_lost_file_and_a_foreign_file() {
        let mem = Arc::new(MemStore::default());
        mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
        let fake = Fake::default();
        let runtime = runtime_on(&mem, store());
        let configured = runtime.configured_store().unwrap();
        assert_eq!(cycle_on(&fake, &runtime).await.store_healthy, Some(true));

        // Rows of the root with another id arrive after startup.
        let mut foreign_row = row(&arn(ACCOUNT), BackupStatus::Available);
        foreign_row.store.store_id = "store-2".to_owned();
        mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
        fake.rows.lock().unwrap().insert(arn(ACCOUNT), foreign_row);
        assert_eq!(cycle_on(&fake, &runtime).await.store_healthy, Some(false));
        assert_eq!(
            configured.health(),
            StoreHealth::Unhealthy(StoreProblem::IdMismatch {
                file_id: "store-1".to_owned(),
                row_ids: vec!["store-2".to_owned()],
            })
        );
        fake.rows.lock().unwrap().clear();
        assert_eq!(cycle_on(&fake, &runtime).await.store_healthy, Some(true));

        // The file is deleted while rows exist: not rewritten.
        fake.rows
            .lock()
            .unwrap()
            .insert(arn(ACCOUNT), row(&arn(ACCOUNT), BackupStatus::Available));
        let puts_before = mem.puts.lock().unwrap().len();
        mem.objects.lock().unwrap().remove(STORE_IDENTITY_KEY);
        assert_eq!(cycle_on(&fake, &runtime).await.store_healthy, Some(false));
        assert_eq!(
            configured.health(),
            StoreHealth::Unhealthy(StoreProblem::FileMissingWithRows(vec![
                "store-1".to_owned()
            ]))
        );
        assert_eq!(
            mem.puts.lock().unwrap().len(),
            puts_before,
            "a root that lost its file while rows exist gets no new one"
        );
        assert!(!mem.keys().contains(&STORE_IDENTITY_KEY.to_owned()));
        assert_eq!(
            runtime.store_health().unwrap().reason,
            Some("identity file missing")
        );

        // The file is replaced with another catalog's.
        mem.insert(
            STORE_IDENTITY_KEY,
            &identity_file("store-1", "other-catalog"),
        );
        assert_eq!(cycle_on(&fake, &runtime).await.store_healthy, Some(false));
        assert_eq!(
            configured.health(),
            StoreHealth::Unhealthy(StoreProblem::ForeignCatalog {
                file_catalog_id: "other-catalog".to_owned(),
                catalog_id: "catalog-1".to_owned(),
            })
        );
        // Restored, the store is healthy again.
        mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
        assert_eq!(cycle_on(&fake, &runtime).await.store_healthy, Some(true));
    }

    /// The wait reads RUNNING restores by status, with no age: a record
    /// stamped this instant (one the dead-job listing never returns, its
    /// heartbeat being within any window) holds the wait, and the wait
    /// reports it.
    #[tokio::test(start_paused = true)]
    async fn the_wait_sees_a_running_restore_with_a_fresh_heartbeat() {
        let fake = Fake::default();
        let holder = Arc::new(FakeHolder::default());
        let claim = held(&holder);
        let mut fresh = dead_job(RestoreKind::Legacy, "Fresh");
        fresh.heartbeat_at = seconds_since_epoch();
        fresh.status = JobStatus::Running;
        fake.running_jobs.lock().unwrap().push(fresh);
        let wait = Wait {
            owner_window: Duration::from_secs(10),
            limit: Duration::from_secs(4),
            poll: Duration::from_secs(2),
        };
        let (_tx, mut quiet) = Interrupt::new();
        let outcome = wait_for_quiet(&fake, &store(), &claim, wait, &mut quiet, |_| {}).await;
        assert_eq!(
            outcome,
            WaitOutcome::Busy(vec![format!(
                "a restore into Fresh (account {ACCOUNT}) is RUNNING"
            )])
        );
        fake.running_jobs.lock().unwrap().clear();
        assert_eq!(
            wait_for_quiet(&fake, &store(), &claim, wait, &mut quiet, |_| {}).await,
            WaitOutcome::Quiet
        );
        claim.release().await.unwrap();
    }

    /// The deletion loop stops before its next row, never inside one: once a
    /// heartbeat has found the claim lost, and once a signal has been
    /// received (here sent as the first row's deletion is reported, so the
    /// loop stops before the second). A row left for a reason is skipped
    /// and the loop goes on.
    #[tokio::test(start_paused = true)]
    async fn the_deletion_loop_stops_at_a_lost_claim_and_at_a_signal() {
        let fake = Fake::default();
        let mem = MemStore::default();
        let arns: Vec<String> = (1..=3).map(arn_n).collect();
        for arn in &arns {
            seed(&fake, &mem, arn, BackupStatus::Available);
        }
        let rows: Vec<BackupRow> = fake
            .list_backup_rows(None, &store())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);

        // The claim lost before the loop: no row is deleted.
        let holder = Arc::new(FakeHolder::default());
        let claim = held(&holder);
        *holder.lost.lock().unwrap() = true;
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        assert!(claim.lost().is_some());
        let (_tx, interrupt) = Interrupt::new();
        let mut reported = Vec::new();
        let outcome = purge_rows(
            &fake,
            &mem,
            &rows,
            Duration::from_secs(300),
            &claim,
            &interrupt,
            |row| reported.push(row.backup_arn.clone()),
        )
        .await;
        assert_eq!(
            outcome,
            PurgeOutcome {
                deleted: vec![],
                skipped: vec![],
                stopped: Some(Stop::Lost(
                    "the maintenance claim was lost (another holder took it over)".to_owned()
                )),
            }
        );
        assert!(reported.is_empty());
        assert_eq!(fake.rows.lock().unwrap().len(), 3, "no row was touched");
        assert_eq!(mem.keys().len(), 6, "no object was deleted");
        claim.release().await.unwrap();

        // A signal received as the first row is reported stops the loop
        // before the second; the third is untouched.
        let holder = Arc::new(FakeHolder::default());
        let claim = held(&holder);
        let (tx, interrupt) = Interrupt::new();
        let mut reported = Vec::new();
        let outcome = purge_rows(
            &fake,
            &mem,
            &rows,
            Duration::from_secs(300),
            &claim,
            &interrupt,
            |row| {
                reported.push(row.backup_arn.clone());
                let _ = tx.send(Some("SIGTERM"));
            },
        )
        .await;
        assert_eq!(
            outcome,
            PurgeOutcome {
                deleted: vec![arns[0].clone()],
                skipped: vec![],
                stopped: Some(Stop::Interrupted("SIGTERM")),
            }
        );
        assert_eq!(reported, vec![arns[0].clone()]);
        let mut left: Vec<String> = fake.rows.lock().unwrap().keys().cloned().collect();
        left.sort();
        assert_eq!(left, vec![arns[1].clone(), arns[2].clone()]);
        assert_eq!(mem.keys().len(), 4);

        // Nothing stops it: the rest goes, a row left for a reason skipped.
        fake.delete_starts
            .lock()
            .unwrap()
            .insert(arns[1].clone(), DeleteStart::InUse("Restored".to_owned()));
        let (_tx, interrupt) = Interrupt::new();
        let rest: Vec<BackupRow> = rows[1..].to_vec();
        let outcome = purge_rows(
            &fake,
            &mem,
            &rest,
            Duration::from_secs(300),
            &claim,
            &interrupt,
            |_| {},
        )
        .await;
        assert_eq!(
            outcome,
            PurgeOutcome {
                deleted: vec![arns[2].clone()],
                skipped: vec![format!(
                    "{}: a restore into Restored is reading it",
                    arns[1]
                )],
                stopped: None,
            }
        );
        claim.release().await.unwrap();
    }

    /// A defaulted root whose startup checks did not finish starts checking
    /// with the rule kept: a first check that fails while the catalog has no
    /// rows for the root sets the store aside (no component, no store), as
    /// the startup failure would have; with rows for it the same failure is
    /// an unhealthy configured store.
    #[tokio::test]
    async fn a_defaulted_root_set_aside_after_its_first_failed_check() {
        let mem = Arc::new(MemStore::default());
        mem.insert(
            STORE_IDENTITY_KEY,
            &identity_file("store-1", "other-catalog"),
        );
        let fake = Fake::default();
        let checking = || {
            ConfiguredStore::checking(
                store().store_kind,
                store().store_root,
                Arc::clone(&mem) as Arc<dyn extenddb_backup_store::BackupStore>,
                None,
            )
            .defaulted(true)
        };
        let runtime = BackupRuntime::new(4).with_store(checking());
        assert!(runtime.configured_store().is_some());
        assert_eq!(runtime.store_health().unwrap().status, "checking");
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(
            report.store_healthy, None,
            "the store is set aside, as none configured"
        );
        assert!(runtime.configured_store().is_none());
        assert!(runtime.store_health().is_none());
        assert!(!runtime.is_configured_store(&store()));
        // The next cycle runs no check: the store is unconfigured for good.
        let puts = mem.puts.lock().unwrap().len();
        assert_eq!(cycle_on(&fake, &runtime).await.store_healthy, None);
        assert_eq!(mem.puts.lock().unwrap().len(), puts);

        // With rows for the root, the same failure is an unhealthy store.
        fake.rows
            .lock()
            .unwrap()
            .insert(arn(ACCOUNT), row(&arn(ACCOUNT), BackupStatus::Available));
        let runtime = BackupRuntime::new(4).with_store(checking());
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(report.store_healthy, Some(false));
        assert_eq!(
            runtime.store_health().unwrap().reason,
            Some("owned by another catalog")
        );
        // A root that is not the defaulted one is never set aside.
        let explicit = BackupRuntime::new(4).with_store(checking().defaulted(false));
        fake.rows.lock().unwrap().clear();
        assert_eq!(cycle_on(&fake, &explicit).await.store_healthy, Some(false));
        assert!(explicit.configured_store().is_some());
    }

    /// The deletions run under the store's bound and the in-flight rule:
    /// DeleteBackup on a stalled store answers DELETED at the bound with the
    /// row left DELETING, two DeleteBackups together park one thread between
    /// them (the second waits for the first's slot and answers at its bound
    /// with nothing parked), and the reconciler's deletion of those rows
    /// returns at the bound too, with no second call parked beside the first.
    #[tokio::test(start_paused = true)]
    async fn the_deletions_run_under_the_stores_bound() {
        let mem = Arc::new(MemStore::default());
        mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
        mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
        mem.insert(&manifest_key(&newer_arn(ACCOUNT)), b"{}");
        let fake = fake_with_rows(&[
            (arn(ACCOUNT), BackupStatus::Available),
            (newer_arn(ACCOUNT), BackupStatus::Available),
        ]);
        let runtime = runtime_on(&mem, store());
        let configured = runtime.configured_store().unwrap();
        mem.set_stalled(true);
        let started = tokio::time::Instant::now();
        let (first, second) = tokio::time::timeout(
            Duration::from_secs(120),
            futures::future::join(
                delete_backup(&fake, &runtime, ACCOUNT, REGION, &own(&arn(ACCOUNT))),
                delete_backup(&fake, &runtime, ACCOUNT, REGION, &own(&newer_arn(ACCOUNT))),
            ),
        )
        .await
        .expect("both deletes return at the store's bound");
        assert_eq!(started.elapsed(), LOCAL_CHECK_TIMEOUT);
        assert_eq!(first.unwrap().backup_details.backup_status, "DELETED");
        assert_eq!(second.unwrap().backup_details.backup_status, "DELETED");
        for row in [arn(ACCOUNT), newer_arn(ACCOUNT)] {
            assert_eq!(
                fake.rows.lock().unwrap()[&row].status,
                BackupStatus::Deleting,
                "the row is left for the reconciler"
            );
        }
        assert!(
            configured.check_call_in_flight(),
            "the abandoned call is parked"
        );
        assert_eq!(
            mem.stalls.load(std::sync::atomic::Ordering::Acquire),
            1,
            "the second delete's call waited for the first's slot and parked nothing"
        );

        // The reconciler's cycle: the check is refused at once (a call is
        // parked), so no deletion runs and nothing new is parked.
        let started = tokio::time::Instant::now();
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(report.store_healthy, Some(false));
        assert!(report.deletions_finished.is_empty());
        assert_eq!(mem.stalls.load(std::sync::atomic::Ordering::Acquire), 1);

        // The mount answers: the parked call returns, the next cycle checks
        // and finishes the deletions.
        mem.set_stalled(false);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!configured.check_call_in_flight());
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(report.store_healthy, Some(true));
        let mut finished = report.deletions_finished.clone();
        finished.sort();
        let mut expected = [arn(ACCOUNT), newer_arn(ACCOUNT)];
        expected.sort();
        assert_eq!(finished, expected);
        assert_eq!(mem.keys(), [STORE_IDENTITY_KEY]);
    }

    /// A call's bound starts once it holds the root's slot: on a healthy
    /// root whose calls take 3 s under a 5 s bound, two DeleteBackups
    /// together both finish (the second waits 3 s for the slot and then has
    /// the bound for its own 3 s call), both rows reach DELETED, nothing is
    /// marked in flight and the next check finds the store healthy. With one
    /// bound over the wait and the call the second was abandoned at 5 s, its
    /// row left DELETING, and the next check marked the store unreachable.
    #[tokio::test(start_paused = true)]
    async fn a_calls_bound_starts_when_it_holds_the_slot() {
        let mem = Arc::new(MemStore::default());
        mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
        mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
        mem.insert(&manifest_key(&newer_arn(ACCOUNT)), b"{}");
        let fake = fake_with_rows(&[
            (arn(ACCOUNT), BackupStatus::Available),
            (newer_arn(ACCOUNT), BackupStatus::Available),
        ]);
        let runtime = runtime_on(&mem, store());
        let configured = runtime.configured_store().unwrap();
        mem.set_delay(Some(Duration::from_secs(3)));
        let started = tokio::time::Instant::now();
        let (first, second) = futures::future::join(
            delete_backup(&fake, &runtime, ACCOUNT, REGION, &own(&arn(ACCOUNT))),
            delete_backup(&fake, &runtime, ACCOUNT, REGION, &own(&newer_arn(ACCOUNT))),
        )
        .await;
        assert_eq!(first.unwrap().backup_details.backup_status, "DELETED");
        assert_eq!(second.unwrap().backup_details.backup_status, "DELETED");
        assert!(
            started.elapsed() > LOCAL_CHECK_TIMEOUT,
            "the second call ran after the first's 3 s: {:?}",
            started.elapsed()
        );
        for row in [arn(ACCOUNT), newer_arn(ACCOUNT)] {
            assert_eq!(
                fake.rows.lock().unwrap()[&row].status,
                BackupStatus::Deleted,
                "both deletions finished"
            );
        }
        assert!(!configured.check_call_in_flight(), "nothing abandoned");
        mem.set_delay(None);
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(report.store_healthy, Some(true));
    }

    /// The manifest probe reads one row (`newest_available_backup_row`) and
    /// not the listing: the catalog double records the read, and the probe
    /// heads the newest AVAILABLE row's manifest.
    #[tokio::test]
    async fn the_manifest_probe_reads_one_row() {
        let mem = Arc::new(MemStore::default());
        mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
        mem.insert(&manifest_key(&arn(ACCOUNT)), b"{}");
        let fake = fake_with_rows(&[
            (arn(ACCOUNT), BackupStatus::Available),
            (newer_arn(ACCOUNT), BackupStatus::Available),
        ]);
        let runtime = runtime_on(&mem, store());
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(report.store_healthy, Some(false));
        assert_eq!(
            runtime.configured_store().unwrap().health(),
            StoreHealth::Unhealthy(StoreProblem::ManifestMissing {
                backup_arn: newer_arn(ACCOUNT),
            }),
            "the newest row's manifest is the one probed"
        );
        assert_eq!(fake.newest_reads.lock().unwrap().as_slice(), [store()]);
        mem.insert(&manifest_key(&newer_arn(ACCOUNT)), b"{}");
        assert_eq!(cycle_on(&fake, &runtime).await.store_healthy, Some(true));
    }
}

// --- the store-backed create and its job ------------------------------------

/// The `Music` table as a snapshot describes it: composite key, one GSI,
/// provisioned.
fn music_table() -> TableDescription {
    TableDescription {
        table_name: "Music".to_owned(),
        key_schema: vec![
            key("Artist", KeyType::Hash),
            key("SongTitle", KeyType::Range),
        ],
        attribute_definitions: vec![attr("Artist"), attr("SongTitle"), attr("Genre")],
        table_status: TableStatus::Active,
        creation_date_time: 1_757_400_000.0,
        table_size_bytes: 123_456,
        item_count: 100_000,
        table_arn: source_arn(),
        table_id: "12345a12-abcd-123a-ab12-1234abc12345".to_owned(),
        provisioned_throughput: ProvisionedThroughputDescription {
            read_capacity_units: 5,
            write_capacity_units: 5,
            ..Default::default()
        },
        billing_mode_summary: Some(BillingModeSummary {
            billing_mode: BillingMode::Provisioned,
            last_update_to_pay_per_request_date_time: None,
        }),
        global_secondary_indexes: Some(vec![GsiDescription {
            index_name: "GenreIndex".to_owned(),
            key_schema: vec![key("Genre", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            index_status: "ACTIVE".to_owned(),
            provisioned_throughput: Some(ProvisionedThroughputDescription {
                read_capacity_units: 1,
                write_capacity_units: 1,
                ..Default::default()
            }),
            index_size_bytes: 0,
            item_count: 0,
            index_arn: String::new(),
        }]),
        ..Default::default()
    }
}

/// A vector index description for a table with vector support.
fn vector_index() -> extenddb_core::types::VectorIndexDescription {
    serde_json::from_value(json!({
        "IndexName": "v",
        "VectorAttribute": { "AttributeName": "emb" },
        "Dimensions": 3,
        "DistanceFunction": "COSINE",
        "IndexStatus": "ACTIVE",
        "IndexSizeBytes": 0,
        "ItemCount": 0,
        "IndexArn": ""
    }))
    .unwrap()
}

/// An item of the `Music` table with a `Year` number.
fn song(artist: &str, title: &str) -> Item {
    use extenddb_core::types::AttributeValue;
    Item::from([
        ("Artist".to_owned(), AttributeValue::S(artist.to_owned())),
        ("SongTitle".to_owned(), AttributeValue::S(title.to_owned())),
        ("Year".to_owned(), AttributeValue::N("1999".to_owned())),
    ])
}

/// `song` with a `Year` the wire refuses (#618's shape).
fn song_with_bad_number(artist: &str, title: &str, year: &str) -> Item {
    use extenddb_core::types::AttributeValue;
    let mut item = song(artist, title);
    item.insert("Year".to_owned(), AttributeValue::N(year.to_owned()));
    item
}

/// `n` songs in key order.
fn songs(n: usize) -> Vec<Item> {
    (0..n)
        .map(|i| song(&format!("artist-{i:05}"), &format!("title-{i:05}")))
        .collect()
}

/// Make `fake` serve a snapshot of `table` yielding `items` (through `gate`
/// when given).
fn snapshot_fixture(
    fake: &Fake,
    table: &TableDescription,
    items: Vec<Item>,
    key_ordered: bool,
    gate: Option<Arc<tokio::sync::Semaphore>>,
) {
    fake.snapshots.lock().unwrap().insert(
        table.table_name.clone(),
        SnapshotFixture {
            description: table.clone(),
            key_ordered,
            items: items.into_iter().map(Ok).collect(),
            gate,
        },
    );
}

/// A gate with no permit: the stream yields nothing until the test adds
/// some.
fn closed_gate() -> Arc<tokio::sync::Semaphore> {
    Arc::new(tokio::sync::Semaphore::new(0))
}

/// The settings the job tests run with: small files, a short heartbeat.
fn job_settings() -> JobSettings {
    JobSettings {
        data_file_target_bytes: 64 * 1024 * 1024,
        heartbeat_interval: Duration::from_millis(50),
        stale_job_after: Duration::from_secs(300),
        failed_row_retention: Duration::from_secs(7 * 24 * 3600),
    }
}

/// A healthy runtime on `mem` with the job settings above and a collector.
fn job_runtime(mem: &Arc<MemStore>) -> BackupRuntime {
    runtime_on(mem, store())
        .with_job_settings(job_settings())
        .with_metrics(Arc::new(extenddb_core::metrics::MetricsCollector::new()))
}

/// CreateBackup of `Music` through `runtime`.
async fn create_music_backup(
    fake: &Arc<Fake>,
    runtime: &BackupRuntime,
) -> Result<BackupDetails, DynamoDbError> {
    create_backup(
        fake,
        runtime,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Ok(music_table()),
    )
    .await
}

/// Wait until no job task of `runtime` runs, failing after 30 s: a hang
/// guard for jobs that take well under a second alone. A test whose job
/// takes seconds calls [`wait_for_jobs_within`] with a bound at least ten
/// times its time alone on an idle core, so a loaded host cannot reach it.
async fn wait_for_jobs(runtime: &BackupRuntime) {
    wait_for_jobs_within(runtime, Duration::from_secs(30)).await;
}

/// Wait until no job task of `runtime` runs, failing after `bound`.
async fn wait_for_jobs_within(runtime: &BackupRuntime, bound: Duration) {
    let deadline = tokio::time::Instant::now() + bound;
    while runtime.running_jobs() > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "a job did not end within {bound:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The objects under the backup's prefix, by key.
fn objects_under(mem: &MemStore, arn: &str) -> Vec<String> {
    let prefix = BackupArn::parse(arn).unwrap().store_prefix();
    mem.keys()
        .into_iter()
        .filter(|k| k.starts_with(&prefix))
        .collect()
}

/// The stored manifest of `arn`, parsed.
fn stored_manifest(mem: &MemStore, arn: &str) -> extenddb_core::backup_format::ExtenddbManifest {
    let bytes = mem.objects.lock().unwrap()[&manifest_key(arn)].clone();
    extenddb_core::backup_format::ExtenddbManifest::from_json_bytes(&bytes).unwrap()
}

/// The status writes the fake's claims received, as `(to, failure_reason)`.
fn writes(fake: &Fake) -> Vec<(JobStatus, Option<String>)> {
    fake.status_writes
        .lock()
        .unwrap()
        .iter()
        .map(|w| (w.to, w.fields.failure_reason.clone()))
        .collect()
}

/// The `BackupJobs` outcomes the collector drained, with their count.
fn job_outcomes(runtime: &BackupRuntime) -> Vec<(String, f64)> {
    use extenddb_core::metrics::MetricName;
    let mut outcomes: Vec<(String, f64)> = runtime
        .metrics()
        .unwrap()
        .drain(Duration::ZERO)
        .into_iter()
        .filter(|b| b.metric == MetricName::BackupJobs)
        .map(|b| (b.operation, b.sum))
        .collect();
    outcomes.sort_by(|a, b| a.0.cmp(&b.0));
    outcomes
}

/// The request's order and the job's happy path. The checks before the
/// permit: a missing table is TableNotFound with every slot held and the
/// store untouched. Then the detached task: the snapshot is opened, the row
/// is inserted CREATING under the claim with the manifest's `source_table`
/// (the TTL read beside the open), the maintenance claim is read, and the
/// request is answered CREATING with the id's instant. The job then writes
/// the data file, `manifest-files.json`, `manifest-summary.json`, the
/// sidecar and the manifest, in that order and nothing else, and moves the
/// row to AVAILABLE with the manifest's size, the stream's count and the
/// manifest's `source_table`; the ARN's id, the row's `created_at`, the
/// response's time and the manifest's `backup_creation_date_time` are one
/// instant; the data file decodes back to the items; the slot is freed; the
/// metrics name the outcome, and `BackupSnapshotAge` was recorded by the
/// beats the job's three periods inside the stream gave it.
#[tokio::test]
async fn a_store_backed_create_answers_creating_and_its_job_writes_the_backup() {
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let items = songs(5);
    let gate = closed_gate();
    snapshot_fixture(
        &fake,
        &music_table(),
        items.clone(),
        true,
        Some(Arc::clone(&gate)),
    );
    *fake.ttl.lock().unwrap() = Some(TimeToLiveDescription {
        time_to_live_status: TimeToLiveStatus::Enabled,
        attribute_name: Some("expires_at".to_owned()),
    });
    let full = job_runtime(&mem);
    let mut _held = Vec::new();
    for _ in 0..4 {
        _held.push(full.take_permit().await.unwrap());
    }
    let err = create_backup(
        &fake,
        &full,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        "Music",
        "nightly",
        Err(StorageError::TableNotFound("Music".to_owned())),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::TableNotFoundException(_)),
        "{err:?}"
    );
    let err = create_music_backup(&fake, &full).await.unwrap_err();
    assert!(
        matches!(err, DynamoDbError::LimitExceededException(_)),
        "{err:?}"
    );
    assert!(
        fake.snapshots_opened.lock().unwrap().is_empty(),
        "nothing was opened"
    );
    drop(_held);

    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    assert_eq!(details.backup_status, "CREATING");
    assert_eq!(details.backup_type, "USER");
    assert_eq!(details.backup_name, "nightly");
    assert_eq!(details.backup_size_bytes, 0);
    let arn = BackupArn::parse(&details.backup_arn).unwrap();
    assert_eq!(
        extenddb_storage::util::backup_creation_date_time(&details.backup_arn),
        Some(details.backup_creation_date_time),
        "the response's time is the id's instant"
    );
    let claimed = fake.claims.lock().unwrap().clone();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].status, BackupStatus::Creating);
    assert_eq!(claimed[0].store, store());
    assert_eq!(claimed[0].created_at, details.backup_creation_date_time);
    assert_eq!(claimed[0].item_count, 0);
    assert_eq!(claimed[0].source_table_details_json["item_count"], 100_000);
    assert_eq!(
        claimed[0].source_table_details_json["time_to_live"]["AttributeName"],
        "expires_at"
    );
    assert_eq!(
        fake.maintenance_checks.lock().unwrap().as_slice(),
        [Duration::from_secs(300), Duration::from_secs(300)],
        "the maintenance claim is read before the permit, by the refused request too"
    );

    // Three heartbeat periods inside the stream, so the timer beats before
    // the job ends.
    tokio::time::sleep(3 * job_settings().heartbeat_interval).await;
    gate.add_permits(items.len());
    wait_for_jobs(&runtime).await;
    assert_eq!(runtime.free_slots(), 4, "the job freed its slot");
    let prefix = arn.store_prefix();
    assert_eq!(
        mem.puts.lock().unwrap().as_slice(),
        [
            format!("{prefix}data/000001.json.gz"),
            format!("{prefix}manifest-files.json"),
            format!("{prefix}manifest-summary.json"),
            format!("{prefix}extenddb-manifest.sha256"),
            format!("{prefix}extenddb-manifest.json"),
        ],
        "the write order"
    );
    assert!(mem.deletes.lock().unwrap().is_empty());
    let manifest = stored_manifest(&mem, &details.backup_arn);
    assert_eq!(manifest.backup_arn, details.backup_arn);
    assert_eq!(
        manifest.backup_creation_date_time,
        details.backup_creation_date_time
    );
    assert_eq!(manifest.source_table.item_count, 5, "the stream's count");
    assert_eq!(
        manifest.source_table.table_size_bytes,
        items
            .iter()
            .map(|item| extenddb_core::types::item_size_bytes(item) as i64)
            .sum::<i64>(),
        "the stream's size, not the stats pass's 123,456"
    );
    assert_eq!(manifest.source_backend, "test");
    assert_eq!(
        manifest.item_order,
        extenddb_core::backup_format::ItemOrder::KeyOrdered
    );
    assert_eq!(manifest.data_files.len(), 1);
    assert!(manifest.data_files[0].key_range.is_some());
    let file = mem.objects.lock().unwrap()[&format!("{prefix}data/000001.json.gz")].clone();
    assert_eq!(manifest.data_files[0].bytes, file.len() as u64);
    let mut reader = manifest.reader(&manifest.data_files[0], &file[..]).unwrap();
    let mut decoded = Vec::new();
    while let Some(item) = reader.next_item().unwrap() {
        decoded.push(item);
    }
    assert_eq!(decoded, items);
    let manifest_bytes =
        mem.objects.lock().unwrap()[&format!("{prefix}extenddb-manifest.json")].clone();
    let sidecar = mem.objects.lock().unwrap()[&format!("{prefix}extenddb-manifest.sha256")].clone();
    extenddb_core::backup_format::verify_manifest_checksum(&manifest_bytes, &sidecar).unwrap();

    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Available);
    assert_eq!(row.size_bytes, manifest.backup_size_bytes);
    assert_eq!(row.item_count, 5);
    assert_eq!(
        row.source_table_details_json,
        serde_json::to_value(&manifest.source_table).unwrap()
    );
    assert!(row.completed_at.is_some());
    assert_eq!(writes(&fake), [(JobStatus::Available, None)]);
    let write = fake.status_writes.lock().unwrap()[0].clone();
    assert_eq!(
        (write.arn.as_str(), write.from, write.generation),
        (
            details.backup_arn.as_str(),
            JobStatus::Creating,
            row.owner_generation
        ),
        "every status write is conditioned on CREATING under the claim's generation"
    );
    assert!(fake.heartbeats.load(std::sync::atomic::Ordering::Acquire) >= 1);

    use extenddb_core::metrics::MetricName;
    let drained = runtime.metrics().unwrap().drain(Duration::ZERO);
    let find = |metric: MetricName| {
        drained
            .iter()
            .filter(|b| b.metric == metric)
            .map(|b| (b.operation.clone(), b.sum))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        find(MetricName::BackupJobs),
        [("succeeded".to_owned(), 1.0)]
    );
    assert_eq!(find(MetricName::BackupJobDuration).len(), 1);
    let total: u64 = mem.put_sizes.lock().unwrap().iter().map(|(_, n)| n).sum();
    assert_eq!(
        find(MetricName::BackupBytesUploaded),
        [(String::new(), total as f64)]
    );
    assert_eq!(
        find(MetricName::WorkerLastSuccess)
            .iter()
            .map(|(op, _)| op.as_str())
            .collect::<Vec<_>>(),
        ["BackupJob"]
    );
    assert!(find(MetricName::WorkerErrorCount).is_empty());
    let beats = fake.heartbeats.load(std::sync::atomic::Ordering::Acquire);
    let ages: usize = drained
        .iter()
        .filter(|b| b.metric == MetricName::BackupSnapshotAge)
        .map(|b| b.count as usize)
        .sum();
    assert!(
        ages >= 1 && ages < beats,
        "the snapshot's age is recorded at each confirmed timer beat and not at the re-check \
         before the manifest: {ages} recorded, {beats} beats"
    );
}

/// The maintenance claim is checked before the permit, as on the
/// in-catalog path: held, the request is LimitExceeded naming a backup
/// maintenance command and the claim, no snapshot is opened, no row is
/// inserted (a FAILED `maintenance` row carried the insert's heartbeat and
/// `backup purge`'s wait counted it as a live job), nothing reaches the
/// store and every slot is free. A claim that cannot be read answers the
/// read's error, with no row either.
#[tokio::test]
async fn a_store_backed_create_under_a_maintenance_claim_is_refused_before_its_row() {
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(3), true, None);
    *fake.maintenance_held.lock().unwrap() = true;
    let runtime = job_runtime(&mem);
    let err = create_music_backup(&fake, &runtime).await.unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::LimitExceededException(m)
            if m.contains("A backup maintenance command holds") && !m.contains("purge")),
        "{err:?}"
    );
    wait_for_jobs(&runtime).await;
    assert!(fake.snapshots_opened.lock().unwrap().is_empty());
    assert!(fake.rows.lock().unwrap().is_empty(), "no row is inserted");
    assert!(fake.claims.lock().unwrap().is_empty());
    assert!(mem.puts.lock().unwrap().is_empty());
    assert!(mem.deletes.lock().unwrap().is_empty());
    assert_eq!(runtime.free_slots(), 4);
    assert_eq!(job_outcomes(&runtime), []);

    *fake.maintenance_held.lock().unwrap() = false;
    *fake.maintenance_error.lock().unwrap() = Some("the claim table is locked".to_owned());
    let err = create_music_backup(&fake, &runtime).await.unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::InternalServerError(_)),
        "{err:?}"
    );
    assert!(fake.rows.lock().unwrap().is_empty(), "no row is inserted");
    assert_eq!(runtime.free_slots(), 4);
}

/// A snapshot the backend refuses to open answers the request with the
/// open's error (here the backend's bound, LimitExceeded) and leaves no row
/// and no object; the slot is freed.
#[tokio::test]
async fn an_open_that_fails_answers_the_request_and_leaves_nothing() {
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    *fake.snapshot_error.lock().unwrap() =
        Some(StorageError::LimitExceeded("too many snapshots".to_owned()));
    let runtime = job_runtime(&mem);
    let err = create_music_backup(&fake, &runtime).await.unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::LimitExceededException(m) if m == "too many snapshots"),
        "{err:?}"
    );
    wait_for_jobs(&runtime).await;
    assert!(fake.rows.lock().unwrap().is_empty());
    assert!(fake.claims.lock().unwrap().is_empty());
    assert!(mem.keys().is_empty());
    assert_eq!(runtime.free_slots(), 4);
}

/// The file close rule: a file closes at the first item that takes its
/// uncompressed bytes to or past `data_file_target_bytes` (a knob of exactly
/// three lines closes at the third), so every file but the last is at least
/// the knob and under the knob plus one line, and the items are split across
/// files in order. At the knob's minimum (32 MiB) with 5 MiB items, each
/// file takes the items until the sum passes 32 MiB.
#[tokio::test]
async fn a_data_file_closes_at_the_first_item_that_reaches_the_knob() {
    use extenddb_core::types::AttributeValue;
    let line_bytes = |item: &Item| {
        extenddb_core::backup_format::encode_item_line(item)
            .unwrap()
            .len() as u64
    };

    // A low knob and small items: the exact rule.
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let items = songs(10);
    let line = line_bytes(&items[0]);
    let target = 3 * line;
    snapshot_fixture(&fake, &music_table(), items.clone(), true, None);
    let runtime = job_runtime(&mem).with_job_settings(JobSettings {
        data_file_target_bytes: target,
        ..job_settings()
    });
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    {
        let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
        assert_eq!(
            (row.status, row.failure_reason.clone()),
            (BackupStatus::Available, None),
            "{:?}",
            mem.puts.lock().unwrap()
        );
    }
    let manifest = stored_manifest(&mem, &details.backup_arn);
    let counts: Vec<u64> = manifest.data_files.iter().map(|f| f.item_count).collect();
    assert_eq!(
        counts,
        [3, 3, 3, 1],
        "the third line takes the file to the knob, so it closes there"
    );
    for file in &manifest.data_files[..3] {
        assert!(file.uncompressed_bytes >= target);
        assert!(file.uncompressed_bytes - line < target);
    }
    assert_eq!(
        manifest
            .data_files
            .iter()
            .map(|f| f.key.as_str())
            .collect::<Vec<_>>(),
        [
            "data/000001.json.gz",
            "data/000002.json.gz",
            "data/000003.json.gz",
            "data/000004.json.gz"
        ]
    );
    let prefix = BackupArn::parse(&details.backup_arn)
        .unwrap()
        .store_prefix();
    let mut decoded = Vec::new();
    for file in &manifest.data_files {
        let bytes = mem.objects.lock().unwrap()[&format!("{prefix}{}", file.key)].clone();
        let mut reader = manifest.reader(file, &bytes[..]).unwrap();
        while let Some(item) = reader.next_item().unwrap() {
            decoded.push(item);
        }
    }
    assert_eq!(decoded, items);
    assert_eq!(
        fake.rows.lock().unwrap()[&details.backup_arn].item_count,
        10
    );

    // The knob's minimum, 32 MiB, with 5 MiB binary items (a 6.7 MiB
    // line each): five lines pass 32 MiB, then three.
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let items: Vec<Item> = (0..8)
        .map(|i| {
            let mut item = song("a", &format!("t{i}"));
            item.insert(
                "blob".to_owned(),
                AttributeValue::B(vec![0u8; 5 * 1024 * 1024]),
            );
            item
        })
        .collect();
    let line = line_bytes(&items[0]);
    snapshot_fixture(&fake, &music_table(), items, true, None);
    let runtime = job_runtime(&mem).with_job_settings(JobSettings {
        data_file_target_bytes: 32 * 1024 * 1024,
        ..job_settings()
    });
    let limits = LimitsConfig {
        max_item_size_bytes: 8 * 1024 * 1024,
        ..LimitsConfig::default()
    };
    let details = create_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &limits,
        "Music",
        "nightly",
        Ok(music_table()),
    )
    .await
    .unwrap();
    // The whole test takes about 5 s alone on an idle core (eight 6.7 MiB
    // lines encoded and compressed into two files), so the guard is 120 s.
    wait_for_jobs_within(&runtime, Duration::from_secs(120)).await;
    assert_eq!(
        fake.rows.lock().unwrap()[&details.backup_arn].failure_reason,
        None
    );
    let manifest = stored_manifest(&mem, &details.backup_arn);
    let counts: Vec<u64> = manifest.data_files.iter().map(|f| f.item_count).collect();
    assert_eq!(counts, [5, 3]);
    assert!(manifest.data_files[0].uncompressed_bytes >= 32 * 1024 * 1024);
    assert!(manifest.data_files[0].uncompressed_bytes - line < 32 * 1024 * 1024);
}

/// An unordered snapshot writes `unordered` with no key ranges; an ordered
/// one `key_ordered` with a range per file.
#[tokio::test]
async fn the_manifest_records_the_snapshots_order() {
    for key_ordered in [false, true] {
        let mem = Arc::new(MemStore::default());
        let fake = Arc::new(Fake::default());
        snapshot_fixture(&fake, &music_table(), songs(3), key_ordered, None);
        let runtime = job_runtime(&mem);
        let details = create_music_backup(&fake, &runtime).await.unwrap();
        wait_for_jobs(&runtime).await;
        let manifest = stored_manifest(&mem, &details.backup_arn);
        let expected = if key_ordered {
            extenddb_core::backup_format::ItemOrder::KeyOrdered
        } else {
            extenddb_core::backup_format::ItemOrder::Unordered
        };
        assert_eq!(manifest.item_order, expected);
        assert_eq!(manifest.data_files[0].key_range.is_some(), key_ordered);
    }
}

/// A snapshot stream that fails after a refused item: the FAILED reason
/// carries both, the refusals' count and first item and then the stream's
/// error, so the item the operator is told to find is not lost behind the
/// read failure. Nothing is uploaded and the prefix is deleted.
#[tokio::test]
async fn a_stream_error_after_a_refusal_keeps_the_refusal_in_the_reason() {
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let items = vec![
        song("a", "1"),
        song_with_bad_number("a", "2", "1E126"),
        song_with_bad_number("a", "3", "abc"),
    ];
    snapshot_fixture(&fake, &music_table(), items, true, None);
    fake.snapshots
        .lock()
        .unwrap()
        .get_mut("Music")
        .unwrap()
        .items
        .push(Err(StorageError::Internal(
            "the page read failed".to_owned(),
        )));
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    let reason = row.failure_reason.unwrap();
    assert!(
        reason.starts_with(
            "2 item(s) cannot be restored; first: key {\"Artist\":{\"S\":\"a\"},\"SongTitle\":{\"S\":\"2\"}}, \
             attribute Year: "
        ),
        "{reason}"
    );
    assert!(
        reason.ends_with(" (#618); the snapshot then failed: Internal error: the page read failed"),
        "{reason}"
    );
    assert!(mem.puts.lock().unwrap().is_empty());
    assert!(objects_under(&mem, &details.backup_arn).is_empty());
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);

    // The stream's error alone keeps its own reason.
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(1), true, None);
    fake.snapshots
        .lock()
        .unwrap()
        .get_mut("Music")
        .unwrap()
        .items
        .push(Err(StorageError::Internal(
            "the page read failed".to_owned(),
        )));
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    assert_eq!(
        fake.rows.lock().unwrap()[&details.backup_arn]
            .failure_reason
            .as_deref(),
        Some("reading the snapshot: Internal error: the page read failed")
    );
}

/// The validator's rule: with three items the wire let in, the job reads
/// and checks the whole stream past the first refusal (every gate permit is
/// taken), encodes nothing more, and fails the row with the count and the
/// first item, naming #618 for an invalid number; the FAILED write comes
/// first and the prefix is deleted after it. The stream is fed one item
/// every 20 ms, slower than the pipeline, so the encoder's stop lands
/// before the stream's end and an uploader that read that stop as its own
/// failure would end the feeder with the rest unchecked. An item refused
/// for its size gives a reason that does not name #618.
#[tokio::test]
async fn the_validator_fails_the_backup_after_the_rest_is_validated() {
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let items = vec![
        song("a", "1"),
        song_with_bad_number("a", "2", "1E126"),
        song("a", "3"),
        song_with_bad_number("a", "4", "abc"),
        song_with_bad_number("a", "5", "1E126"),
        song("a", "6"),
    ];
    let count = items.len();
    let gate = closed_gate();
    snapshot_fixture(&fake, &music_table(), items, true, Some(Arc::clone(&gate)));
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    let feed = tokio::spawn({
        let gate = Arc::clone(&gate);
        async move {
            for _ in 0..count {
                tokio::time::sleep(Duration::from_millis(20)).await;
                gate.add_permits(1);
            }
        }
    });
    wait_for_jobs(&runtime).await;
    feed.await.unwrap();
    assert_eq!(gate.available_permits(), 0, "every item was read");
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    let reason = row.failure_reason.unwrap();
    assert!(
        reason.starts_with(
            "3 item(s) cannot be restored; first: key {\"Artist\":{\"S\":\"a\"},\"SongTitle\":{\"S\":\"2\"}}, \
             attribute Year: "
        ),
        "{reason}"
    );
    assert!(reason.ends_with(" (#618)"), "{reason}");
    let prefix = BackupArn::parse(&details.backup_arn)
        .unwrap()
        .store_prefix();
    assert_eq!(writes(&fake).len(), 1);
    assert_eq!(writes(&fake)[0].0, JobStatus::Failed);
    assert_eq!(
        mem.deletes.lock().unwrap().as_slice(),
        std::slice::from_ref(&prefix)
    );
    assert!(objects_under(&mem, &details.backup_arn).is_empty());
    assert!(
        mem.puts.lock().unwrap().is_empty(),
        "after the first refused item nothing is uploaded, the first file with its one valid \
         item included: {:?}",
        mem.puts.lock().unwrap()
    );
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);

    // Over `max_item_size_bytes`: the reason names the size rule, not #618.
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(2), true, None);
    let runtime = job_runtime(&mem);
    let limits = LimitsConfig {
        max_item_size_bytes: 20,
        ..LimitsConfig::default()
    };
    let details = create_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &limits,
        "Music",
        "nightly",
        Ok(music_table()),
    )
    .await
    .unwrap();
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    let reason = row.failure_reason.unwrap();
    assert!(
        reason.starts_with("2 item(s) cannot be restored; first: key "),
        "{reason}"
    );
    assert!(reason.contains("20 bytes"), "{reason}");
    assert!(!reason.contains("#618"), "{reason}");
}

/// A put that fails fails the row with the file named, and only then
/// deletes the prefix; the store error is counted.
#[tokio::test]
async fn a_put_failure_fails_the_row_then_deletes_the_prefix() {
    let mem = Arc::new(MemStore::default());
    mem.fail_puts
        .store(true, std::sync::atomic::Ordering::Release);
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(3), true, None);
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    assert!(
        row.failure_reason
            .as_deref()
            .unwrap()
            .starts_with("writing data/000001.json.gz: "),
        "{:?}",
        row.failure_reason
    );
    let prefix = BackupArn::parse(&details.backup_arn)
        .unwrap()
        .store_prefix();
    assert_eq!(
        writes(&fake),
        [(JobStatus::Failed, row.failure_reason.clone())]
    );
    assert_eq!(mem.deletes.lock().unwrap().as_slice(), [prefix]);
    use extenddb_core::metrics::MetricName;
    let drained = runtime.metrics().unwrap().drain(Duration::ZERO);
    assert!(drained.iter().any(|b| {
        b.metric == MetricName::BackupStoreErrors && b.operation == "put" && b.sum == 1.0
    }));
    assert!(
        drained
            .iter()
            .any(|b| { b.metric == MetricName::BackupJobs && b.operation == "failed" })
    );
    assert!(
        drained
            .iter()
            .any(|b| { b.metric == MetricName::WorkerErrorCount && b.operation == "BackupJob" })
    );
}

/// A FAILED write that matches no row (the row's generation changed under
/// a heartbeat that kept confirming) ends the job with no store access: no
/// `delete_prefix`, the row untouched.
#[tokio::test]
async fn a_failed_write_that_does_not_match_makes_no_store_call() {
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let gate = closed_gate();
    snapshot_fixture(
        &fake,
        &music_table(),
        songs(3),
        true,
        Some(Arc::clone(&gate)),
    );
    *fake.heartbeat_answer.lock().unwrap() = HeartbeatAnswer::AlwaysConfirm;
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    // Another generation takes the row while the job waits on its stream;
    // the put then fails.
    fake.rows
        .lock()
        .unwrap()
        .get_mut(&details.backup_arn)
        .unwrap()
        .owner_generation = 999;
    mem.fail_puts
        .store(true, std::sync::atomic::Ordering::Release);
    gate.add_permits(3);
    wait_for_jobs(&runtime).await;
    assert_eq!(writes(&fake).len(), 1);
    assert_eq!(writes(&fake)[0].0, JobStatus::Failed);
    assert!(
        mem.deletes.lock().unwrap().is_empty(),
        "no store access after NotMatched"
    );
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Creating);
    assert_eq!(row.owner_generation, 999);
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
}

/// A status write whose outcome is unknown is re-read, not treated as a
/// failure: an AVAILABLE write that landed is AVAILABLE with no store
/// access after it; one that was lost is written once more. The same for
/// the FAILED write: landed, the deletion follows; lost, it is written again
/// and the deletion follows.
#[tokio::test]
async fn an_unknown_write_is_re_read() {
    for (unknown, writes_expected) in [(UnknownWrite::Applied, 1), (UnknownWrite::Lost, 2)] {
        let mem = Arc::new(MemStore::default());
        let fake = Arc::new(Fake::default());
        snapshot_fixture(&fake, &music_table(), songs(2), true, None);
        *fake.unknown_write.lock().unwrap() = Some(unknown);
        let runtime = job_runtime(&mem);
        let details = create_music_backup(&fake, &runtime).await.unwrap();
        wait_for_jobs(&runtime).await;
        let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
        assert_eq!(row.status, BackupStatus::Available, "{unknown:?}");
        assert_eq!(writes(&fake).len(), writes_expected, "{unknown:?}");
        assert!(mem.deletes.lock().unwrap().is_empty());
        assert_eq!(objects_under(&mem, &details.backup_arn).len(), 5);
        assert_eq!(job_outcomes(&runtime), [("succeeded".to_owned(), 1.0)]);
    }
    for (unknown, writes_expected) in [(UnknownWrite::Applied, 1), (UnknownWrite::Lost, 2)] {
        let mem = Arc::new(MemStore::default());
        mem.fail_puts
            .store(true, std::sync::atomic::Ordering::Release);
        let fake = Arc::new(Fake::default());
        snapshot_fixture(&fake, &music_table(), songs(2), true, None);
        *fake.unknown_write.lock().unwrap() = Some(unknown);
        let runtime = job_runtime(&mem);
        let details = create_music_backup(&fake, &runtime).await.unwrap();
        wait_for_jobs(&runtime).await;
        let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
        assert_eq!(row.status, BackupStatus::Failed, "{unknown:?}");
        assert_eq!(writes(&fake).len(), writes_expected, "{unknown:?}");
        let prefix = BackupArn::parse(&details.backup_arn)
            .unwrap()
            .store_prefix();
        assert_eq!(mem.deletes.lock().unwrap().as_slice(), [prefix]);
    }
}

/// A heartbeat that reports the claim lost, or that does not return within
/// a period, ends the job with no put and no catalog write: the row is left
/// as it is for its owner or the reconciler.
#[tokio::test]
async fn a_lost_or_stalled_heartbeat_stops_the_job_with_no_put() {
    for answer in [
        HeartbeatAnswer::Lost,
        HeartbeatAnswer::Stall,
        HeartbeatAnswer::Error,
    ] {
        let mem = Arc::new(MemStore::default());
        let fake = Arc::new(Fake::default());
        let gate = closed_gate();
        snapshot_fixture(&fake, &music_table(), songs(3), true, Some(gate));
        let runtime = job_runtime(&mem);
        let details = create_music_backup(&fake, &runtime).await.unwrap();
        // The job waits on its stream (the encoder is a blocking task, so
        // the clock runs: a paused clock would not advance behind it); the
        // first beat, a period later, finds the claim lost (or never
        // returns: a period more).
        *fake.heartbeat_answer.lock().unwrap() = answer;
        wait_for_jobs(&runtime).await;
        assert!(mem.puts.lock().unwrap().is_empty(), "{answer:?}: no put");
        assert!(
            mem.deletes.lock().unwrap().is_empty(),
            "{answer:?}: no delete"
        );
        assert!(writes(&fake).is_empty(), "{answer:?}: no catalog write");
        let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
        assert_eq!(row.status, BackupStatus::Creating, "{answer:?}");
        assert_eq!(runtime.free_slots(), 4);
        assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
    }
}

/// The pre-put check's numbers: with the claim confirming, the last
/// confirmed beat stays recent however long the job runs; a timer that
/// stopped running leaves the check true for two periods after the last
/// confirmed beat and false after; with a beat that never returns, the
/// check still passes until the timer has waited one period for the beat
/// (two periods from the last confirmation), and the claim is lost from
/// then on.
#[tokio::test(start_paused = true)]
async fn the_heartbeat_handle_confirms_the_claim_within_two_periods() {
    let fake = Arc::new(Fake::default());
    fake.rows
        .lock()
        .unwrap()
        .insert(arn(ACCOUNT), row(&arn(ACCOUNT), BackupStatus::Creating));
    let claim: Arc<dyn JobClaim> = Arc::from(fake.claim_on(&arn(ACCOUNT), 7));
    let period = Duration::from_secs(10);
    let handle = job::heartbeat::HeartbeatHandle::start(
        Arc::clone(&claim),
        period,
        None,
        0.0,
        tokio::time::Instant::now(),
    );
    // Half a period past the tenth tick, so that tick's beat has confirmed.
    tokio::time::sleep(10 * period + period / 2).await;
    assert!(handle.confirms_the_claim());
    assert!(fake.heartbeats.load(std::sync::atomic::Ordering::Acquire) >= 10);
    // A timer that stopped running (no loss raised): the check holds for two
    // periods after the last confirmed beat, the second period's end
    // included, and then refuses.
    handle.stop_timer();
    tokio::time::sleep(period).await;
    assert!(handle.confirms_the_claim(), "a period and a half");
    tokio::time::sleep(period / 2).await;
    assert!(
        handle.confirms_the_claim(),
        "exactly two periods: the bound is inclusive"
    );
    tokio::time::sleep(period / 2).await;
    assert!(!handle.lost().is_cancelled());
    assert!(
        !handle.confirms_the_claim(),
        "two and a half periods without a confirmed beat"
    );
    drop(handle);

    *fake.heartbeat_answer.lock().unwrap() = HeartbeatAnswer::Stall;
    let handle = job::heartbeat::HeartbeatHandle::start(
        claim,
        period,
        None,
        0.0,
        tokio::time::Instant::now(),
    );
    tokio::time::sleep(period + period / 2).await;
    assert!(handle.confirms_the_claim(), "the beat may still return");
    assert!(!handle.lost().is_cancelled());
    tokio::time::sleep(period).await;
    assert!(!handle.confirms_the_claim());
    assert!(handle.lost().is_cancelled());
    assert!(handle.reason().contains("did not return"));
}

/// The shutdown token runs the failure path: FAILED with reason `shutdown`,
/// then the prefix deleted; the outcome is `cancelled`. `drain_jobs` is what
/// the server calls: it cancels and waits for the job.
#[tokio::test]
async fn cancellation_runs_the_failure_path() {
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    snapshot_fixture(
        &fake,
        &music_table(),
        songs(3),
        true,
        Some(Arc::clone(&gate)),
    );
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    // The job holds its first item and waits for the second.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while gate.available_permits() > 0 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(runtime.running_jobs(), 1);
    let left = runtime.drain_jobs(Duration::from_secs(10)).await;
    assert_eq!(left, 0);
    assert_eq!(runtime.running_jobs(), 0);
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    assert_eq!(row.failure_reason.as_deref(), Some("shutdown"));
    let prefix = BackupArn::parse(&details.backup_arn)
        .unwrap()
        .store_prefix();
    assert_eq!(
        writes(&fake),
        [(JobStatus::Failed, Some("shutdown".to_owned()))]
    );
    assert_eq!(mem.deletes.lock().unwrap().as_slice(), [prefix]);
    assert!(objects_under(&mem, &details.backup_arn).is_empty());
    use extenddb_core::metrics::MetricName;
    let drained = runtime.metrics().unwrap().drain(Duration::ZERO);
    assert_eq!(
        drained
            .iter()
            .filter(|b| b.metric == MetricName::BackupJobs)
            .map(|b| (b.operation.clone(), b.sum))
            .collect::<Vec<_>>(),
        [("cancelled".to_owned(), 1.0)]
    );
    assert!(
        !drained
            .iter()
            .any(|b| b.metric == MetricName::WorkerErrorCount),
        "a clean shutdown is not a worker error"
    );
    assert!(
        !drained
            .iter()
            .any(|b| b.metric == MetricName::WorkerLastSuccess),
        "nor a success"
    );
}

/// With `max_concurrent_jobs = 1`, a second CreateBackup while a job runs
/// is LimitExceeded naming the bound; once the job ends the slot is free.
#[tokio::test]
async fn a_second_create_over_the_job_bound_is_limit_exceeded() {
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let gate = closed_gate();
    snapshot_fixture(
        &fake,
        &music_table(),
        songs(2),
        true,
        Some(Arc::clone(&gate)),
    );
    let runtime = BackupRuntime::new(1)
        .with_store(ConfiguredStore::new(
            store().store_kind,
            store().store_root,
            Arc::clone(&mem) as Arc<dyn extenddb_backup_store::BackupStore>,
            None,
            Ok(store()),
        ))
        .with_job_settings(job_settings());
    create_music_backup(&fake, &runtime).await.unwrap();
    let err = create_music_backup(&fake, &runtime).await.unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::LimitExceededException(m) if m.contains("max_concurrent_jobs")),
        "{err:?}"
    );
    assert_eq!(
        fake.snapshots_opened.lock().unwrap().len(),
        1,
        "the second opened nothing"
    );
    gate.add_permits(2);
    wait_for_jobs(&runtime).await;
    assert_eq!(runtime.free_slots(), 1);
    snapshot_fixture(&fake, &music_table(), songs(1), true, None);
    create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    assert_eq!(fake.rows.lock().unwrap().len(), 2);
}

/// A `tracing` subscriber that keeps every event's level and message, for
/// the tests of the job's log lines. Set as the thread's default for the
/// test's life through [`LogLines::capture`]; a `#[tokio::test]` runtime
/// runs the job's tasks on the test's thread, so their events reach it (the
/// encoders, on the blocking pool, log nothing).
#[derive(Clone, Default)]
struct LogLines(Arc<Mutex<Vec<(tracing::Level, String)>>>);

/// The process's global `tracing` subscriber while these tests run:
/// interested in every callsite and enabled for none. `tracing` caches each
/// callsite's interest the first time any thread reaches it; while exactly
/// one scoped subscriber is registered in the process, that cache is
/// computed from the default of the thread that got there first. A test
/// without a subscriber running beside a `LogLines` test on another thread
/// would so cache the job's log callsites as "never", and the `LogLines`
/// test would then see no line from them (tokio-rs/tracing#2743). With this
/// global default the cache reads "sometimes" instead, and each event asks
/// the thread's own default, the `LogLines` where one is set.
struct InterestedInEveryCallsite;

impl tracing::Subscriber for InterestedInEveryCallsite {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

impl LogLines {
    /// A fresh `LogLines` set as this thread's `tracing` default until the
    /// guard drops, with the process's global default installed once.
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        static GLOBAL: std::sync::Once = std::sync::Once::new();
        GLOBAL.call_once(|| {
            // Another global may already be set by a test outside this
            // module; then its interest rule applies and this one is moot.
            let _ = tracing::subscriber::set_global_default(InterestedInEveryCallsite);
        });
        let lines = Self::default();
        let guard = tracing::subscriber::set_default(lines.clone());
        (lines, guard)
    }

    /// The messages containing `needle`, in order.
    fn containing(&self, needle: &str) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, m)| m.contains(needle))
            .map(|(_, m)| m.clone())
            .collect()
    }
}

struct MessageVisitor<'a>(&'a mut String);

/// A callsite met first by a thread without a scoped subscriber is still
/// seen by a `LogLines` on another thread (the global default's interest
/// rule; the doc of `InterestedInEveryCallsite`). Without that default the
/// callsite's interest is cached as "never" whenever this is the only
/// `LogLines` alive in the process, and nothing is captured.
#[test]
fn a_callsite_first_met_on_another_thread_still_reaches_the_lines() {
    fn shared_callsite(n: u32) {
        tracing::error!("probe {n}: a callsite shared by two threads");
    }
    let (lines, _guard) = LogLines::capture();
    std::thread::spawn(|| shared_callsite(1)).join().unwrap();
    shared_callsite(2);
    assert_eq!(
        lines.containing("probe "),
        ["probe 2: a callsite shared by two threads"]
    );
}

impl tracing::field::Visit for MessageVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            use std::fmt::Write as _;
            let _ = write!(self.0, "{value:?}");
        }
    }
}

impl tracing::Subscriber for LogLines {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut message = String::new();
        event.record(&mut MessageVisitor(&mut message));
        self.0
            .lock()
            .unwrap()
            .push((*event.metadata().level(), message));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// The bytes of one item's data file line.
fn line_bytes(item: &Item) -> u64 {
    extenddb_core::backup_format::encode_item_line(item)
        .unwrap()
        .len() as u64
}

/// A put that fails while the snapshot stream is between pages ends the job
/// at once: the stream holds one permit, so it yields the first item and
/// then waits; the first file closes at that item (a one-line knob), its put
/// fails, and the FAILED write lands with the stream still held, no permit
/// released. Without the uploader's stop reaching the feeder, the feeder
/// would wait for the stream's next item and the row would stay CREATING
/// with a live heartbeat for as long as the stream did.
#[tokio::test]
async fn a_put_failure_ends_the_job_without_waiting_for_the_streams_next_item() {
    let mem = Arc::new(MemStore::default());
    mem.fail_puts
        .store(true, std::sync::atomic::Ordering::Release);
    let fake = Arc::new(Fake::default());
    let items = songs(3);
    let target = line_bytes(&items[0]);
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    snapshot_fixture(&fake, &music_table(), items, true, Some(Arc::clone(&gate)));
    let runtime = job_runtime(&mem).with_job_settings(JobSettings {
        data_file_target_bytes: target,
        ..job_settings()
    });
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    assert_eq!(gate.available_permits(), 0, "the stream was never released");
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    assert!(
        row.failure_reason
            .as_deref()
            .unwrap()
            .starts_with("writing data/000001.json.gz: "),
        "{:?}",
        row.failure_reason
    );
    let prefix = BackupArn::parse(&details.backup_arn)
        .unwrap()
        .store_prefix();
    assert_eq!(mem.deletes.lock().unwrap().as_slice(), [prefix]);
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
}

/// `BackupJobDuration` of a FAILED job covers its failure path: with the
/// store's deletion taking 300 ms, the duration recorded under `failed` is
/// at least that, so the metric measures the job to the end of its last
/// store call for either outcome.
#[tokio::test]
async fn a_failed_jobs_duration_covers_its_failed_write_and_deletion() {
    use extenddb_core::metrics::MetricName;
    let mem = Arc::new(MemStore::default());
    mem.fail_puts
        .store(true, std::sync::atomic::Ordering::Release);
    *mem.delete_delay.lock().unwrap() = Some(Duration::from_millis(300));
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(2), true, None);
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    assert_eq!(mem.deletes.lock().unwrap().len(), 1);
    let drained = runtime.metrics().unwrap().drain(Duration::ZERO);
    let durations: Vec<(String, f64)> = drained
        .iter()
        .filter(|b| b.metric == MetricName::BackupJobDuration)
        .map(|b| (b.operation.clone(), b.sum))
        .collect();
    assert_eq!(durations.len(), 1, "{durations:?}");
    assert_eq!(durations[0].0, "failed");
    assert!(
        durations[0].1 >= 300_000.0,
        "{} µs: the duration stops before the deletion",
        durations[0].1
    );
}

/// The two info lines: the start line gives the table's count as the stats
/// pass last counted it (the description's figure, here 100,000), the
/// completion line the stream's count (the items written) and the bytes.
#[tokio::test]
async fn the_start_and_completion_lines_name_the_two_item_counts() {
    let (lines, _guard) = LogLines::capture();
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(5), true, None);
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    let arn = &details.backup_arn;
    let started = lines.containing(": started, ");
    assert_eq!(
        started,
        [format!(
            "backup {arn}: started, 100000 items in the table as last counted by the stats pass, \
             key-ordered"
        )]
    );
    let total: u64 = mem.put_sizes.lock().unwrap().iter().map(|(_, n)| n).sum();
    let available = lines.containing(": AVAILABLE, ");
    assert_eq!(available.len(), 1, "{available:?}");
    assert!(
        available[0].starts_with(&format!(
            "backup {arn}: AVAILABLE, 5 items, {total} bytes written in "
        )),
        "{}",
        available[0]
    );
}

/// The refusal log cap: with 1,001 refused items the first 1,000 are
/// logged one by one, one line counts the remainder, the reason carries the
/// full count and the first item, the stream is read to its end and nothing
/// is uploaded.
#[tokio::test]
async fn refusals_past_the_log_cap_are_counted_in_one_line() {
    let (lines, _guard) = LogLines::capture();
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let mut items = vec![song("a", "0")];
    items.extend((1..=1001).map(|i| song_with_bad_number("a", &format!("{i:05}"), "1E126")));
    items.push(song("a", "zzz"));
    let gate = Arc::new(tokio::sync::Semaphore::new(items.len()));
    snapshot_fixture(&fake, &music_table(), items, true, Some(Arc::clone(&gate)));
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    assert_eq!(gate.available_permits(), 0, "every item was read");
    let logged = lines.containing(": item cannot be restored: key ");
    assert_eq!(logged.len(), 1000);
    assert!(
        logged[0].contains(r#""SongTitle":{"S":"00001"}"#)
            && logged[0].contains(", attribute Year: "),
        "{}",
        logged[0]
    );
    assert!(
        logged[999].contains(r#""SongTitle":{"S":"01000"}"#),
        "{}",
        logged[999]
    );
    assert_eq!(
        lines.containing(" more item(s) cannot be restored (not listed)"),
        [format!(
            "backup {}: 1 more item(s) cannot be restored (not listed)",
            details.backup_arn
        )]
    );
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    let reason = row.failure_reason.unwrap();
    assert!(
        reason.starts_with("1001 item(s) cannot be restored; first: key "),
        "{reason}"
    );
    assert!(reason.contains(r#""SongTitle":{"S":"00001"}"#), "{reason}");
    assert!(mem.puts.lock().unwrap().is_empty());
}

/// An item whose data file line would exceed the format's cap is refused
/// through the validator's path, not the encoder's: under a raised item
/// limit, a 600 KB item of the worst-expansion shape (a list of number
/// sets, a 40 MB line) is counted, logged with its key and the cap, the
/// rest of the stream is checked, nothing is uploaded and the FAILED
/// reason names the item without #618. The item passes the validator
/// itself (its numbers are valid, its size within the limit). The line is
/// measured through a counting writer that stops at the cap (the format's
/// own test pins that it holds no line), so the reason carries the cap and
/// not the line's length.
#[tokio::test]
async fn an_item_whose_line_exceeds_the_cap_is_refused_and_named() {
    use extenddb_core::backup_format::MAX_LINE_BYTES;
    use extenddb_core::types::AttributeValue;
    use std::collections::BTreeSet;
    let (lines, _guard) = LogLines::capture();
    // 81 two-digit values at `E-130`, 133 characters each in normalized
    // form, 2 bytes of item size each (the `MAX_LINE_BYTES` doc's shape).
    let set: BTreeSet<String> = (11..100)
        .filter(|m| m % 10 != 0)
        .map(|m| format!("0.{}{m}", "0".repeat(129)))
        .collect();
    assert_eq!(set.len(), 81);
    let mut worst = song("a", "2");
    worst.insert(
        "l".to_owned(),
        AttributeValue::L(vec![AttributeValue::NS(set); 3_700]),
    );
    let size = extenddb_core::types::item_size_bytes(&worst);
    assert!(size > 409_600 && size < 2 * 1024 * 1024, "{size}");
    let line = line_bytes(&worst);
    assert!(line > MAX_LINE_BYTES as u64, "{line}");
    let items = vec![song("a", "1"), worst, song("a", "3")];
    let gate = Arc::new(tokio::sync::Semaphore::new(items.len()));
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), items, true, Some(Arc::clone(&gate)));
    let runtime = job_runtime(&mem);
    let limits = LimitsConfig {
        max_item_size_bytes: 2 * 1024 * 1024,
        ..LimitsConfig::default()
    };
    let details = create_backup(
        &fake,
        &runtime,
        ACCOUNT,
        REGION,
        &limits,
        "Music",
        "nightly",
        Ok(music_table()),
    )
    .await
    .unwrap();
    // The test takes about 20 s alone on an idle core, so the guard is
    // 300 s.
    wait_for_jobs_within(&runtime, Duration::from_secs(300)).await;
    assert_eq!(
        gate.available_permits(),
        0,
        "the rest of the stream was checked"
    );
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    let reason = row.failure_reason.unwrap();
    assert_eq!(
        reason,
        format!(
            "1 item(s) cannot be restored; first: key {{\"Artist\":{{\"S\":\"a\"}},\"SongTitle\":\
             {{\"S\":\"2\"}}}}: data file line exceeds the line cap of {MAX_LINE_BYTES} bytes"
        )
    );
    assert_eq!(lines.containing(": item cannot be restored: key ").len(), 1);
    assert!(
        mem.puts.lock().unwrap().is_empty(),
        "{:?}",
        mem.puts.lock().unwrap()
    );
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
}

/// A status write that errors on the claim connection makes no store call,
/// whichever write it is. The FAILED write: no `delete_prefix`, the row
/// CREATING for the reconciler. The AVAILABLE write: the manifest was
/// already put, so the five objects stay for the reconciler's probe, which
/// is the order the design rests on (the manifest is the commit record, the
/// AVAILABLE write follows it).
#[tokio::test]
async fn a_status_write_error_makes_no_store_call_and_leaves_the_manifest() {
    let mem = Arc::new(MemStore::default());
    mem.fail_puts
        .store(true, std::sync::atomic::Ordering::Release);
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(2), true, None);
    fake.status_write_error
        .store(true, std::sync::atomic::Ordering::Release);
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Creating);
    assert_eq!(writes(&fake).len(), 1);
    assert!(
        mem.deletes.lock().unwrap().is_empty(),
        "no delete after a claim error on the FAILED write"
    );
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);

    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(2), true, None);
    fake.status_write_error
        .store(true, std::sync::atomic::Ordering::Release);
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Creating);
    assert_eq!(writes(&fake), [(JobStatus::Available, None)]);
    assert!(mem.deletes.lock().unwrap().is_empty());
    let prefix = BackupArn::parse(&details.backup_arn)
        .unwrap()
        .store_prefix();
    assert_eq!(
        mem.puts.lock().unwrap().as_slice(),
        [
            format!("{prefix}data/000001.json.gz"),
            format!("{prefix}manifest-files.json"),
            format!("{prefix}manifest-summary.json"),
            format!("{prefix}extenddb-manifest.sha256"),
            format!("{prefix}extenddb-manifest.json"),
        ],
        "the manifest was put before the AVAILABLE write"
    );
    assert_eq!(objects_under(&mem, &details.backup_arn).len(), 5);
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
}

/// A second unknown outcome, on the write sent once more, counts as
/// matching no row: two writes and no third, the row left as it is, no
/// delete, the outcome `failed` (the job ended by its own path, not a
/// panic).
#[tokio::test]
async fn a_second_unknown_outcome_ends_the_job_without_a_third_write() {
    let mem = Arc::new(MemStore::default());
    mem.fail_puts
        .store(true, std::sync::atomic::Ordering::Release);
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(2), true, None);
    *fake.unknown_write.lock().unwrap() = Some(UnknownWrite::Lost);
    fake.unknown_write_sticky
        .store(true, std::sync::atomic::Ordering::Release);
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    assert_eq!(writes(&fake).len(), 2);
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Creating);
    assert!(mem.deletes.lock().unwrap().is_empty());
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
}

/// A second unknown outcome whose write landed counts as matched: the first
/// FAILED write is lost with its connection (the re-read shows CREATING, so
/// the write is sent once more), the second lands and its outcome is
/// unknown too, and the second re-read shows the row FAILED under this
/// claim, so the failure path goes on to the one `delete_prefix`. The row
/// is FAILED under this generation and the prefix is the job's, so the
/// deletion is right; a job that treated the second unknown outcome as
/// matching no row would leave the prefix to the reconciler for nothing.
#[tokio::test]
async fn a_second_unknown_outcome_whose_write_landed_is_matched() {
    let mem = Arc::new(MemStore::default());
    mem.fail_puts
        .store(true, std::sync::atomic::Ordering::Release);
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(2), true, None);
    *fake.unknown_write.lock().unwrap() = Some(UnknownWrite::Lost);
    *fake.unknown_write_then.lock().unwrap() = Some(UnknownWrite::Applied);
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    assert_eq!(
        writes(&fake).iter().map(|w| w.0).collect::<Vec<_>>(),
        [JobStatus::Failed, JobStatus::Failed]
    );
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    let prefix = BackupArn::parse(&details.backup_arn)
        .unwrap()
        .store_prefix();
    assert_eq!(
        mem.deletes.lock().unwrap().as_slice(),
        [prefix],
        "the second re-read shows FAILED under this claim: the deletion follows"
    );
    assert!(objects_under(&mem, &details.backup_arn).is_empty());
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
}

/// The pre-put check in its two places: with the heartbeat timer stopped
/// without raising the loss (a timer task that died) and two periods gone,
/// neither a data file's put nor a document's put starts, each stopping the
/// job as a lost claim naming the object.
#[tokio::test]
async fn a_stopped_heartbeat_timer_starts_no_put() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = arn(ACCOUNT);
    fake.rows
        .lock()
        .unwrap()
        .insert(arn.clone(), row(&arn, BackupStatus::Creating));
    let claim: Arc<dyn JobClaim> = Arc::from(fake.claim_on(&arn, 7));
    let period = Duration::from_millis(50);
    let heartbeat = job::heartbeat::HeartbeatHandle::start(
        claim,
        period,
        None,
        0.0,
        tokio::time::Instant::now(),
    );
    heartbeat.stop_timer();
    tokio::time::sleep(3 * period).await;
    assert!(!heartbeat.lost().is_cancelled());
    let shutdown = extenddb_storage::CancellationToken::new();
    let parsed = BackupArn::parse(&arn).unwrap();
    let prefix = parsed.store_prefix();
    let runtime = runtime_on(&mem, store());
    let identity = store();
    let pipeline = job::writer::Pipeline {
        store: &*mem,
        configured: runtime.configured_store().unwrap(),
        identity: &identity,
        prefix: &prefix,
        arn: &arn,
        heartbeat: &heartbeat,
        shutdown: &shutdown,
        metrics: None,
        target_bytes: 64 * 1024 * 1024,
        key_prefix: "",
        bucket: &store().store_root,
    };
    let table = music_table();
    let key = extenddb_core::validation::RestorableKeySchema::resolve(
        &table.key_schema,
        &table.attribute_definitions,
    )
    .unwrap();
    let stream: BoxStream<'static, Result<Item, StorageError>> =
        Box::pin(futures::stream::iter(songs(1).into_iter().map(Ok)));
    let limits = extenddb_core::validation::RestorableLimits::from(&LimitsConfig::default());
    let stop = job::writer::write_data_files(&pipeline, stream, key, limits, None)
        .await
        .unwrap_err();
    assert_eq!(
        stop,
        job::writer::Stop::ClaimLost(
            "no heartbeat confirmed the claim recently enough for data file 1".to_owned()
        )
    );
    assert!(mem.puts.lock().unwrap().is_empty());
    let stop = job::put_document(&pipeline, "manifest-files.json", b"{}")
        .await
        .unwrap_err();
    assert_eq!(
        stop,
        job::writer::Stop::ClaimLost(
            "no heartbeat confirmed the claim recently enough for manifest-files.json".to_owned()
        )
    );
    assert!(mem.puts.lock().unwrap().is_empty());
    assert!(mem.keys().is_empty());
}

/// The maintenance check runs before the permit and the row, so a slow
/// check delays the request and not the job's first put: with a period of
/// 100 ms, `stale_job_after` of 300 ms (the floor's ratio) and a check of
/// 400 ms, the request takes at least the check, the row is inserted after
/// it (its heartbeat the insert's, within two periods of the first put), and
/// the backup completes AVAILABLE.
#[tokio::test]
async fn a_slow_maintenance_check_delays_the_request_and_not_the_first_put() {
    let period = Duration::from_millis(100);
    let settings = JobSettings {
        heartbeat_interval: period,
        stale_job_after: 3 * period,
        ..job_settings()
    };
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(2), true, None);
    *fake.maintenance_delay.lock().unwrap() = Some(4 * period);
    let runtime = job_runtime(&mem).with_job_settings(settings);
    let asked = tokio::time::Instant::now();
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    assert!(
        asked.elapsed() >= 4 * period,
        "the request waited for the check"
    );
    assert_eq!(fake.maintenance_checks.lock().unwrap().len(), 1);
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(
        (row.status, row.failure_reason),
        (BackupStatus::Available, None)
    );
    assert_eq!(objects_under(&mem, &details.backup_arn).len(), 5);
    assert_eq!(job_outcomes(&runtime), [("succeeded".to_owned(), 1.0)]);
    assert_eq!(runtime.free_slots(), 4);
}

/// The claim re-check before the manifest: the heartbeat answers `Lost`
/// from the sidecar's put on, with a period long enough that no timer beat
/// runs during the job, so the re-check is the one beat the job sends; it
/// finds the claim lost, the manifest is not written, the row is left
/// CREATING with its four objects and no catalog write.
#[tokio::test]
async fn the_claim_re_check_before_the_manifest_stops_a_lost_claim() {
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(2), true, None);
    let answer = Arc::clone(&fake.heartbeat_answer);
    *mem.on_put.lock().unwrap() = Some(Box::new(move |key: &str| {
        if key.ends_with("extenddb-manifest.sha256") {
            *answer.lock().unwrap() = HeartbeatAnswer::Lost;
        }
    }));
    let runtime = job_runtime(&mem).with_job_settings(JobSettings {
        heartbeat_interval: Duration::from_secs(10),
        ..job_settings()
    });
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    assert_eq!(
        fake.heartbeats.load(std::sync::atomic::Ordering::Acquire),
        1,
        "the re-check is the one beat"
    );
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Creating);
    assert!(
        writes(&fake).is_empty(),
        "no catalog write after a lost claim"
    );
    assert!(mem.deletes.lock().unwrap().is_empty());
    let objects = objects_under(&mem, &details.backup_arn);
    assert_eq!(objects.len(), 4, "{objects:?}");
    assert!(
        !objects
            .iter()
            .any(|k| k.ends_with("extenddb-manifest.json")),
        "the manifest was not written"
    );
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
}

// --- the reconciler's duties over the store's rows ----------------------------

/// A CREATING row whose owner is dead, with a complete, verified backup
/// under its prefix (the golden manifest's documents and data files of the
/// recorded sizes), in `mem` and `fake`; returns its ARN.
fn seed_dead_creating_with_manifest(fake: &Fake, mem: &MemStore, complete: bool) -> String {
    use extenddb_core::backup_format::{BackupDocuments, ExtenddbManifest, ManifestHeader};
    let golden = ExtenddbManifest::from_json_bytes(include_bytes!(
        "../../../core/testdata/extenddb-manifest/v1.json"
    ))
    .unwrap();
    let docs = BackupDocuments::build(
        ManifestHeader {
            backup_arn: golden.backup_arn().unwrap(),
            backup_name: golden.backup_name.clone(),
            source_backend: golden.source_backend.clone(),
            source_extenddb_version: golden.source_extenddb_version.clone(),
            snapshot: golden.snapshot.clone(),
            item_order: golden.item_order,
            source_table: golden.source_table.clone(),
        },
        golden
            .data_files
            .iter()
            .cloned()
            .map(|entry| (entry, None))
            .collect(),
        "",
        &store().store_root,
    )
    .unwrap();
    let arn = golden.backup_arn.clone();
    let prefix = golden.backup_arn().unwrap().store_prefix();
    for (i, file) in golden.data_files.iter().enumerate() {
        let size = if complete || i == 0 {
            file.bytes
        } else {
            file.bytes - 1
        };
        mem.insert(
            &format!("{prefix}{}", file.key),
            &vec![0u8; usize::try_from(size).unwrap()],
        );
    }
    for (name, bytes) in docs.documents() {
        mem.insert(&format!("{prefix}{name}"), bytes);
    }
    let mut dead = row(&arn, BackupStatus::Creating);
    dead.heartbeat_at = 1234.5;
    dead.size_bytes = 0;
    dead.item_count = 0;
    fake.rows.lock().unwrap().insert(arn.clone(), dead);
    fake.dead.lock().unwrap().insert(arn.clone());
    arn
}

/// One cycle on `fake` and `mem`, the store healthy.
async fn cycle_on(fake: &Fake, runtime: &BackupRuntime) -> reconciler::CycleReport {
    reconciler::cycle(
        fake,
        runtime,
        Duration::from_secs(300),
        "test",
        &mut reconciler::ReconcilerState::default(),
    )
    .await
    .unwrap()
}

/// The reconciler's store calls that fail (not an absent object) record
/// `BackupStoreErrors` under the call's name, as the job's do: a `get` of a
/// dead owner's manifest, a `list` of a DELETED row's prefix, a
/// `delete_prefix` of a stale FAILED row's prefix. Each row is left for the
/// next cycle and the cycle itself completes, so `WorkerErrorCount` would
/// not move; the store's health is untouched. The next cycle, with the
/// store answering, finishes each duty and records nothing more.
#[tokio::test]
async fn the_reconcilers_failed_store_calls_are_counted_by_call() {
    use extenddb_core::metrics::{MetricName, MetricsCollector};
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let now = seconds_since_epoch();
    let dead = seed_dead_creating_with_manifest(&fake, &mem, true);
    let dead_prefix = BackupArn::parse(&dead).unwrap().store_prefix();
    let stale_failed = arn_n(1);
    let deleted = arn_n(2);
    for (a, status) in [
        (&stale_failed, BackupStatus::Failed),
        (&deleted, BackupStatus::Deleted),
    ] {
        let mut r = row(a, status);
        r.heartbeat_at = now - 1000.0;
        r.completed_at = Some(now);
        fake.rows.lock().unwrap().insert(a.clone(), r);
        let prefix = BackupArn::parse(a).unwrap().store_prefix();
        mem.insert(&format!("{prefix}data/000001.json.gz"), b"x");
    }
    let failed_prefix = BackupArn::parse(&stale_failed).unwrap().store_prefix();
    let deleted_prefix = BackupArn::parse(&deleted).unwrap().store_prefix();
    *mem.fail_gets_under.lock().unwrap() = Some(dead_prefix.clone());
    *mem.fail_deletes_of.lock().unwrap() = Some(failed_prefix.clone());
    *mem.fail_lists_of.lock().unwrap() = Some(deleted_prefix.clone());
    let metrics = Arc::new(MetricsCollector::new());
    let runtime = runtime_on(&mem, store()).with_metrics(Arc::clone(&metrics));

    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(report.store_healthy, Some(true));
    assert!(report.creating_resolved.is_empty());
    assert!(report.failed_prefixes_deleted.is_empty());
    assert!(report.deleted_prefixes_swept.is_empty());
    let errors = |drained: &[extenddb_core::metrics::FlushBucket]| {
        let mut ops: Vec<(String, f64)> = drained
            .iter()
            .filter(|b| b.metric == MetricName::BackupStoreErrors)
            .map(|b| (b.operation.clone(), b.sum))
            .collect();
        ops.sort_by(|a, b| a.0.cmp(&b.0));
        ops
    };
    assert_eq!(
        errors(&metrics.drain(Duration::ZERO)),
        [
            ("delete_prefix".to_owned(), 1.0),
            ("get".to_owned(), 1.0),
            ("list".to_owned(), 1.0),
        ]
    );

    *mem.fail_gets_under.lock().unwrap() = None;
    *mem.fail_deletes_of.lock().unwrap() = None;
    *mem.fail_lists_of.lock().unwrap() = None;
    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(report.creating_resolved, [(dead, BackupStatus::Available)]);
    assert_eq!(report.failed_prefixes_deleted, [stale_failed]);
    assert_eq!(report.deleted_prefixes_swept, [deleted]);
    assert!(errors(&metrics.drain(Duration::ZERO)).is_empty());
}

/// A dead CREATING row whose prefix holds a complete, verified manifest is
/// AVAILABLE with the manifest's size, count and `source_table` (the row's
/// own `source_table_details_json`, the stats pass's, is replaced: it is
/// seeded here with another object so the write is seen to carry the
/// manifest's); the takeover leaves `heartbeat_at` as the dead owner wrote
/// it and heartbeats nothing; the objects stay. A row whose owner is alive
/// is left alone.
#[tokio::test]
async fn the_reconciler_resolves_a_dead_creating_row_with_a_manifest_to_available() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let dead = seed_dead_creating_with_manifest(&fake, &mem, true);
    let stats_pass = serde_json::json!({"TableName": "Music", "ItemCount": 0});
    fake.rows
        .lock()
        .unwrap()
        .get_mut(&dead)
        .unwrap()
        .source_table_details_json = stats_pass.clone();
    let live = arn(ACCOUNT);
    fake.rows
        .lock()
        .unwrap()
        .insert(live.clone(), row(&live, BackupStatus::Creating));
    let objects_before = mem.keys().len();
    let runtime = runtime_on(&mem, store());
    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(
        report.creating_resolved,
        [(dead.clone(), BackupStatus::Available)]
    );
    assert_eq!(
        fake.takeovers.lock().unwrap().as_slice(),
        std::slice::from_ref(&dead)
    );
    let manifest = stored_manifest(&mem, &dead);
    let row = fake.rows.lock().unwrap()[&dead].clone();
    assert_eq!(row.status, BackupStatus::Available);
    assert_eq!(row.size_bytes, manifest.backup_size_bytes);
    assert_eq!(row.item_count, 100_000);
    assert_ne!(
        stats_pass,
        serde_json::to_value(&manifest.source_table).unwrap()
    );
    assert_eq!(
        row.source_table_details_json,
        serde_json::to_value(&manifest.source_table).unwrap(),
        "the AVAILABLE write carries the manifest's source_table"
    );
    assert_eq!(row.heartbeat_at, 1234.5, "the takeover left the heartbeat");
    assert_ne!(row.owner_generation, 7, "a fresh generation");
    assert!(row.completed_at.is_some());
    assert_eq!(
        fake.heartbeats.load(std::sync::atomic::Ordering::Acquire),
        0,
        "the reconciler heartbeats no takeover claim"
    );
    assert_eq!(mem.keys().len(), objects_before, "nothing deleted");
    assert_eq!(
        fake.rows.lock().unwrap()[&live].status,
        BackupStatus::Creating
    );
    assert!(mem.deletes.lock().unwrap().is_empty());
}

/// A dead CREATING row without a complete manifest is FAILED with the
/// owner-dead reason and `completed_at`, and the resolution itself deletes
/// nothing; the FAILED-prefix duty deletes the prefix once it runs, the
/// heartbeat the owner left being stale. Every guard of the probe has its
/// shape: no manifest; a manifest without its sidecar; a sidecar that does
/// not match; a manifest the reader refuses under a sidecar computed over
/// its bytes (a sidecar matches what was stored, not what parses); a data
/// file missing; a data file shorter than recorded; one larger than
/// recorded; a manifest naming another backup under the row's prefix. A
/// store error during the probe (not an absent object) is none of these:
/// the row stays CREATING for the next cycle, nothing written.
#[tokio::test]
async fn the_reconciler_fails_a_dead_creating_row_without_a_complete_manifest() {
    for shape in [
        "none",
        "no sidecar",
        "sidecar mismatch",
        "unparseable manifest",
        "missing data file",
        "short file",
        "oversized file",
        "foreign ARN",
    ] {
        let mem = Arc::new(MemStore::default());
        mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
        let fake = Fake::default();
        let mut dead = seed_dead_creating_with_manifest(&fake, &mem, shape != "short file");
        let prefix = BackupArn::parse(&dead).unwrap().store_prefix();
        let golden = stored_manifest(&mem, &dead);
        match shape {
            "none" => {
                mem.objects
                    .lock()
                    .unwrap()
                    .remove(&format!("{prefix}extenddb-manifest.json"));
            }
            "no sidecar" => {
                mem.objects
                    .lock()
                    .unwrap()
                    .remove(&format!("{prefix}extenddb-manifest.sha256"));
            }
            "sidecar mismatch" => {
                mem.insert(&format!("{prefix}extenddb-manifest.sha256"), b"0000");
            }
            "unparseable manifest" => {
                use sha2::Digest as _;
                let garbage = b"{\"format_version\":1,\"backup_arn\":";
                mem.insert(&format!("{prefix}extenddb-manifest.json"), garbage);
                let sidecar = format!(
                    "{}  extenddb-manifest.json\n",
                    hex::encode(sha2::Sha256::digest(garbage))
                );
                mem.insert(
                    &format!("{prefix}extenddb-manifest.sha256"),
                    sidecar.as_bytes(),
                );
            }
            "missing data file" => {
                mem.objects
                    .lock()
                    .unwrap()
                    .remove(&format!("{prefix}{}", golden.data_files[1].key));
            }
            "oversized file" => {
                let key = format!("{prefix}{}", golden.data_files[1].key);
                let bytes = usize::try_from(golden.data_files[1].bytes).unwrap() + 1;
                mem.insert(&key, &vec![0u8; bytes]);
            }
            "foreign ARN" => {
                // The golden backup's objects, whole, under another row's
                // prefix: that row's manifest names the golden backup.
                let other = arn_n(3);
                let other_prefix = BackupArn::parse(&other).unwrap().store_prefix();
                let objects: Vec<(String, Vec<u8>)> = mem
                    .objects
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                for (k, v) in objects {
                    mem.objects.lock().unwrap().remove(&k);
                    mem.insert(&format!("{other_prefix}{}", &k[prefix.len()..]), &v);
                }
                fake.rows.lock().unwrap().remove(&dead);
                fake.dead.lock().unwrap().remove(&dead);
                let mut r = row(&other, BackupStatus::Creating);
                r.heartbeat_at = 1234.5;
                fake.rows.lock().unwrap().insert(other.clone(), r);
                fake.dead.lock().unwrap().insert(other.clone());
                dead = other;
            }
            _ => {}
        }
        let objects_before = mem.keys().len();
        // Under the maintenance claim, so the FAILED-prefix duty of the same
        // cycle does not run and the resolution's own effect is visible.
        *fake.maintenance_held.lock().unwrap() = true;
        let runtime = runtime_on(&mem, store());
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(
            report.creating_resolved,
            [(dead.clone(), BackupStatus::Failed)],
            "{shape}"
        );
        assert!(report.failed_prefixes_deleted.is_empty());
        let row = fake.rows.lock().unwrap()[&dead].clone();
        assert_eq!(row.status, BackupStatus::Failed, "{shape}");
        assert_eq!(
            row.failure_reason.as_deref(),
            Some(reconciler::OWNER_DEAD_NO_MANIFEST)
        );
        assert!(row.completed_at.is_some());
        assert_eq!(row.heartbeat_at, 1234.5);
        assert_eq!(mem.keys().len(), objects_before, "{shape}: nothing deleted");
        assert!(mem.deletes.lock().unwrap().is_empty(), "{shape}");

        // The next cycle without the claim: the FAILED row's heartbeat is
        // stale (the dead owner's), the prefix lists objects, so it is
        // deleted.
        *fake.maintenance_held.lock().unwrap() = false;
        let report = cycle_on(&fake, &runtime).await;
        assert_eq!(
            report.failed_prefixes_deleted,
            std::slice::from_ref(&dead),
            "{shape}"
        );
        assert!(objects_under(&mem, &dead).is_empty(), "{shape}");
        assert_eq!(mem.keys(), [STORE_IDENTITY_KEY]);
    }

    // A store error during the probe: the row is taken over and left
    // CREATING, no status write, nothing deleted; the next cycle, with the
    // store answering, resolves it.
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let dead = seed_dead_creating_with_manifest(&fake, &mem, true);
    *mem.fail_gets_under.lock().unwrap() = Some(BackupArn::parse(&dead).unwrap().store_prefix());
    let runtime = runtime_on(&mem, store());
    let report = cycle_on(&fake, &runtime).await;
    assert!(report.creating_resolved.is_empty());
    assert!(
        report.creating_left_to_owner.is_empty(),
        "a probe that failed is neither resolved nor left to an owner"
    );
    assert_eq!(
        fake.takeovers.lock().unwrap().as_slice(),
        std::slice::from_ref(&dead)
    );
    let row = fake.rows.lock().unwrap()[&dead].clone();
    assert_eq!(
        row.status,
        BackupStatus::Creating,
        "left for the next cycle"
    );
    assert!(fake.status_writes.lock().unwrap().is_empty());
    assert!(mem.deletes.lock().unwrap().is_empty());
    *mem.fail_gets_under.lock().unwrap() = None;
    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(report.creating_resolved, [(dead, BackupStatus::Available)]);
}

/// A dead CREATING row whose status write fails on the claim connection is
/// counted neither as resolved nor as left to its owner: the row stays
/// CREATING for the next cycle (its takeover done, one write attempted),
/// nothing deleted; with the connection back the next cycle resolves it.
#[tokio::test]
async fn a_dead_creating_row_whose_status_write_errors_is_left_for_the_next_cycle() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let dead = seed_dead_creating_with_manifest(&fake, &mem, true);
    fake.status_write_error
        .store(true, std::sync::atomic::Ordering::Release);
    let runtime = runtime_on(&mem, store());
    let report = cycle_on(&fake, &runtime).await;
    assert!(report.creating_resolved.is_empty(), "{report:?}");
    assert!(report.creating_left_to_owner.is_empty(), "{report:?}");
    assert!(!report.acted(), "nothing changed");
    assert_eq!(
        fake.takeovers.lock().unwrap().as_slice(),
        std::slice::from_ref(&dead)
    );
    assert_eq!(writes(&fake), [(JobStatus::Available, None)]);
    let row = fake.rows.lock().unwrap()[&dead].clone();
    assert_eq!(row.status, BackupStatus::Creating);
    assert!(mem.deletes.lock().unwrap().is_empty());
    fake.status_write_error
        .store(false, std::sync::atomic::Ordering::Release);
    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(report.creating_resolved, [(dead, BackupStatus::Available)]);
}

/// Within one cycle the dead-owner duty runs before the FAILED-prefix duty,
/// so a row the cycle just failed, whose heartbeat the dead owner left
/// stale, loses its prefix in that same cycle (the design states this as a
/// property of the cycle, not a convenience).
#[tokio::test]
async fn a_row_failed_by_the_cycle_loses_its_prefix_in_that_cycle() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let dead = seed_dead_creating_with_manifest(&fake, &mem, false);
    assert!(!objects_under(&mem, &dead).is_empty());
    let runtime = runtime_on(&mem, store());
    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(
        report.creating_resolved,
        [(dead.clone(), BackupStatus::Failed)]
    );
    assert_eq!(
        report.failed_prefixes_deleted,
        std::slice::from_ref(&dead),
        "the same cycle"
    );
    assert!(objects_under(&mem, &dead).is_empty());
    assert_eq!(
        fake.rows.lock().unwrap()[&dead].status,
        BackupStatus::Failed
    );
}

/// A dead CREATING row whose status write matches no row (another
/// reconciler took the row over in the gap before this one's write, item
/// 29's race on MongoDB and Cassandra) is left to that owner: counted as
/// such and not as resolved, the row still CREATING under the other
/// generation, nothing deleted.
#[tokio::test]
async fn a_dead_creating_row_taken_over_by_another_is_left_to_that_owner() {
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let dead = seed_dead_creating_with_manifest(&fake, &mem, true);
    fake.takeover_lost_to_another
        .store(true, std::sync::atomic::Ordering::Release);
    let runtime = runtime_on(&mem, store());
    let report = cycle_on(&fake, &runtime).await;
    assert!(report.creating_resolved.is_empty());
    assert_eq!(report.creating_left_to_owner, std::slice::from_ref(&dead));
    assert!(!report.acted(), "nothing changed");
    let writes = fake.status_writes.lock().unwrap().clone();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].to, JobStatus::Available);
    let row = fake.rows.lock().unwrap()[&dead].clone();
    assert_eq!(row.status, BackupStatus::Creating);
    assert_ne!(row.owner_generation, writes[0].generation);
    assert!(mem.deletes.lock().unwrap().is_empty());
}

/// The deleting duties and their gates. A FAILED row's prefix is deleted
/// only once its heartbeat is older than `stale_job_after` and only while
/// no maintenance claim is held; a DELETED row's objects are swept; a FAILED
/// or DELETED row whose prefix is empty is removed once its `completed_at`
/// is older than `failed_row_retention` (a prefix emptied earlier in the
/// cycle counts), and kept while the retention has not passed or while its
/// prefix still lists objects (a FAILED row past its retention whose
/// heartbeat is fresh: no duty empties its prefix, so the tombstone duty
/// keeps the row); an AVAILABLE row is no tombstone, whatever its clock and
/// prefix. The dead-CREATING duty runs under the maintenance claim;
/// the deleting duties and the registrations sweep do not. With the store
/// unhealthy none of them runs.
#[tokio::test]
async fn the_reconciler_deletes_failed_and_deleted_prefixes_and_removes_tombstones_on_their_clocks()
{
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let now = seconds_since_epoch();
    let stale_failed = arn_n(1);
    let fresh_failed = arn_n(2);
    let deleted_with_objects = arn_n(3);
    let old_failed_tombstone = arn_n(4);
    let old_deleted_tombstone = arn_n(5);
    let young_tombstone = arn_n(6);
    let old_tombstone_with_objects = arn_n(7);
    let old_failed_with_objects_and_a_live_heartbeat = arn_n(8);
    for (a, status, heartbeat_at, completed_at, objects) in [
        (
            &stale_failed,
            BackupStatus::Failed,
            now - 1000.0,
            Some(now - 1000.0),
            true,
        ),
        (&fresh_failed, BackupStatus::Failed, now, Some(now), true),
        (
            &deleted_with_objects,
            BackupStatus::Deleted,
            now - 1000.0,
            Some(now - 1000.0),
            true,
        ),
        (
            &old_failed_tombstone,
            BackupStatus::Failed,
            now - 1000.0,
            Some(now - 1000.0),
            false,
        ),
        (
            &old_deleted_tombstone,
            BackupStatus::Deleted,
            now - 1000.0,
            Some(now - 1000.0),
            false,
        ),
        (
            &young_tombstone,
            BackupStatus::Failed,
            now - 1000.0,
            Some(now - 10.0),
            false,
        ),
        (
            &old_tombstone_with_objects,
            BackupStatus::Deleted,
            now - 1000.0,
            Some(now - 1000.0),
            true,
        ),
        (
            &old_failed_with_objects_and_a_live_heartbeat,
            BackupStatus::Failed,
            now,
            Some(now - 1000.0),
            true,
        ),
    ] {
        let mut r = row(a, status);
        r.heartbeat_at = heartbeat_at;
        r.completed_at = completed_at;
        fake.rows.lock().unwrap().insert(a.clone(), r);
        if objects {
            let prefix = BackupArn::parse(a).unwrap().store_prefix();
            mem.insert(&format!("{prefix}data/000001.json.gz"), b"x");
        }
    }
    let retention = Duration::from_secs(600);
    let settings = JobSettings {
        failed_row_retention: retention,
        ..job_settings()
    };

    // Unhealthy (the root's identity file names another store while the
    // store answers): no duty over the store's rows runs, the dead-CREATING
    // one included.
    let dead = seed_dead_creating_with_manifest(&fake, &mem, true);
    mem.insert(
        STORE_IDENTITY_KEY,
        &identity_file("another-store", "catalog-1"),
    );
    let runtime = runtime_on(&mem, store()).with_job_settings(settings);
    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(report.store_healthy, Some(false));
    assert!(report.creating_resolved.is_empty());
    assert!(report.failed_prefixes_deleted.is_empty());
    assert!(report.deleted_prefixes_swept.is_empty());
    assert!(report.tombstones_removed.is_empty());
    assert!(mem.deletes.lock().unwrap().is_empty());
    assert_eq!(fake.rows.lock().unwrap().len(), 9);
    assert_eq!(
        fake.rows.lock().unwrap()[&dead].status,
        BackupStatus::Creating
    );
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));

    // Under the maintenance claim, the store healthy again: the
    // dead-CREATING duty runs, the deleting duties and the registrations
    // sweep (which the backend has something for) do not.
    fake.swept_registrations.lock().unwrap().push(arn_n(9));
    *fake.maintenance_held.lock().unwrap() = true;
    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(report.store_healthy, Some(true));
    assert!(report.maintenance_held);
    assert_eq!(report.creating_resolved, [(dead, BackupStatus::Available)]);
    assert!(report.failed_prefixes_deleted.is_empty());
    assert!(report.deleted_prefixes_swept.is_empty());
    assert!(report.tombstones_removed.is_empty());
    assert!(
        report.registrations_swept.is_empty(),
        "the sweep does not run under the claim"
    );
    assert!(mem.deletes.lock().unwrap().is_empty());
    assert_eq!(fake.rows.lock().unwrap().len(), 9);

    // An AVAILABLE row past the retention with an empty prefix: a tombstone
    // is a FAILED or DELETED row and nothing else. It enters the catalog
    // here, older than the row the cycle above resolved, because the
    // store's check heads the newest AVAILABLE row's manifest and this row
    // has none.
    let old_available_with_an_empty_prefix =
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01700000000000-00000000");
    let mut r = row(&old_available_with_an_empty_prefix, BackupStatus::Available);
    r.heartbeat_at = now - 1000.0;
    r.completed_at = Some(now - 1000.0);
    fake.rows
        .lock()
        .unwrap()
        .insert(old_available_with_an_empty_prefix.clone(), r);

    // The claim released: each duty on its clock.
    *fake.maintenance_held.lock().unwrap() = false;
    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(
        report.failed_prefixes_deleted,
        std::slice::from_ref(&stale_failed)
    );
    assert_eq!(
        report.deleted_prefixes_swept,
        [
            deleted_with_objects.clone(),
            old_tombstone_with_objects.clone()
        ]
    );
    // The tombstone duty runs after the two deleting duties, so a prefix
    // they emptied this cycle counts as empty here.
    let mut removed = report.tombstones_removed.clone();
    removed.sort();
    assert_eq!(
        removed,
        [
            stale_failed.clone(),
            deleted_with_objects.clone(),
            old_failed_tombstone.clone(),
            old_deleted_tombstone.clone(),
            old_tombstone_with_objects.clone(),
        ]
    );
    assert_eq!(report.registrations_swept, [arn_n(9)]);
    assert!(report.acted());
    assert_eq!(
        objects_under(&mem, &fresh_failed).len(),
        1,
        "a FAILED row with a live heartbeat keeps its prefix"
    );
    assert!(objects_under(&mem, &stale_failed).is_empty());
    assert!(objects_under(&mem, &deleted_with_objects).is_empty());
    assert_eq!(
        objects_under(&mem, &old_failed_with_objects_and_a_live_heartbeat).len(),
        1,
        "no duty empties the prefix of a FAILED row with a live heartbeat"
    );
    {
        let rows = fake.rows.lock().unwrap();
        assert!(rows.contains_key(&fresh_failed), "its heartbeat is live");
        assert!(
            rows.contains_key(&young_tombstone),
            "its retention has not passed"
        );
        assert!(
            rows.contains_key(&old_failed_with_objects_and_a_live_heartbeat),
            "its prefix still lists an object"
        );
        assert!(
            rows.contains_key(&old_available_with_an_empty_prefix),
            "AVAILABLE is not a tombstone status"
        );
        assert_eq!(
            rows[&old_available_with_an_empty_prefix].status,
            BackupStatus::Available
        );
        assert_eq!(rows.len(), 5, "the four kept and the resolved CREATING row");
    }
    assert!(
        !fake
            .remove_attempts
            .lock()
            .unwrap()
            .contains(&old_available_with_an_empty_prefix),
        "the duty asks the catalog to remove FAILED and DELETED rows only; the backends would \
         refuse an AVAILABLE one, and the duty does not lean on that"
    );

    // The next cycle has nothing left to do.
    let report = cycle_on(&fake, &runtime).await;
    assert!(report.tombstones_removed.is_empty());
    assert!(report.deleted_prefixes_swept.is_empty());
    assert!(report.failed_prefixes_deleted.is_empty());
    assert!(report.creating_resolved.is_empty());
}

// --- the review's cells (PR 8, Lee's items) -------------------------------

/// An item of the `Music` table carrying `n` pseudo-random bytes (a `B`
/// attribute gzip cannot shrink), under the default item limit at 300,000.
fn song_with_noise(i: usize, n: usize) -> Item {
    use extenddb_core::types::AttributeValue;
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15 ^ (i as u64 + 1);
    let noise: Vec<u8> = (0..n)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect();
    let mut item = song(&format!("artist-{i:05}"), &format!("title-{i:05}"));
    item.insert("Noise".to_owned(), AttributeValue::B(noise));
    item
}

/// The mid-file case of
/// `a_put_failure_ends_the_job_without_waiting_for_the_streams_next_item`:
/// the data file's first chunk is out during the third item (three
/// incompressible items of 380,000 bytes, 1.15 MB once base64-encoded and
/// gzipped), the stream is held before the fourth, and the put fails 200 ms
/// after reading that chunk, by when the encoder has finished the third
/// item and waits on the empty item channel while the feeder waits on the
/// stream, which the test never releases. The uploader cancels the feeder's
/// token before it awaits the encoder, so the feeder stops, closes the item
/// channel, the encoder returns, and the row is FAILED naming the file
/// within the test's bound; with the encoder awaited first the job runs for
/// as long as the stream is held.
#[tokio::test]
async fn a_mid_file_put_failure_ends_the_job_while_the_stream_is_held() {
    let mem = Arc::new(MemStore::default());
    mem.fail_puts_after_first_chunk
        .store(true, std::sync::atomic::Ordering::Release);
    let fake = Arc::new(Fake::default());
    let items: Vec<Item> = (0..5).map(|i| song_with_noise(i, 380_000)).collect();
    let gate = Arc::new(tokio::sync::Semaphore::new(3));
    snapshot_fixture(&fake, &music_table(), items, true, Some(Arc::clone(&gate)));
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs_within(&runtime, Duration::from_secs(10)).await;
    assert_eq!(gate.available_permits(), 0, "the stream was never released");
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    assert!(
        row.failure_reason
            .as_deref()
            .unwrap()
            .starts_with("writing data/000001.json.gz: "),
        "{:?}",
        row.failure_reason
    );
    let prefix = BackupArn::parse(&details.backup_arn)
        .unwrap()
        .store_prefix();
    assert_eq!(mem.deletes.lock().unwrap().as_slice(), [prefix]);
    assert!(mem.puts.lock().unwrap().is_empty(), "the put never stored");
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
    assert_eq!(runtime.free_slots(), 4);
}

/// The check before every put reads the store as the runtime holds it. A
/// job held in its stream while the reconciler marks the store unhealthy
/// (here `record_check` with the problem a check would report) fails its
/// row with the store's reason and makes no store call: no put, no
/// `delete_prefix` (the FAILED-prefix duty deletes the prefix once the store
/// is healthy again). A store whose check passed with another id (the
/// identity file names another store: a root swapped under the job) fails
/// the row the same way, naming both ids.
#[tokio::test]
async fn a_store_marked_unhealthy_or_renamed_mid_job_fails_the_row_with_no_store_call() {
    // Unhealthy.
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let gate = closed_gate();
    snapshot_fixture(
        &fake,
        &music_table(),
        songs(3),
        true,
        Some(Arc::clone(&gate)),
    );
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    runtime
        .configured_store()
        .unwrap()
        .record_check(Err(StoreProblem::FileUnreadable(
            "not an identity file".to_owned(),
        )));
    assert!(!runtime.configured_store().unwrap().is_healthy());
    gate.add_permits(3);
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    let reason = row.failure_reason.clone().unwrap();
    assert!(
        reason.starts_with("the filesystem backup store is unhealthy (")
            && reason.contains("extenddb-store.json could not be read: not an identity file")
            && reason.ends_with("); data/000001.json.gz was not written"),
        "{reason}"
    );
    assert!(row.completed_at.is_some());
    assert!(mem.puts.lock().unwrap().is_empty(), "no put");
    assert!(mem.deletes.lock().unwrap().is_empty(), "no delete_prefix");
    assert!(mem.keys().is_empty());
    assert_eq!(
        writes(&fake),
        [(JobStatus::Failed, Some(reason))],
        "the FAILED write and nothing else"
    );
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
    assert_eq!(runtime.free_slots(), 4);

    // Another id.
    let mem = Arc::new(MemStore::default());
    let fake = Arc::new(Fake::default());
    let gate = closed_gate();
    snapshot_fixture(
        &fake,
        &music_table(),
        songs(3),
        true,
        Some(Arc::clone(&gate)),
    );
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    let swapped = StoreIdentity {
        store_id: "store-2".to_owned(),
        ..store()
    };
    runtime
        .configured_store()
        .unwrap()
        .record_check(Ok(swapped));
    assert!(runtime.configured_store().unwrap().is_healthy());
    gate.add_permits(3);
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    assert_eq!(
        row.failure_reason.as_deref(),
        Some(
            "the filesystem backup store's id is store-2 and the row's is store-1; \
             data/000001.json.gz was not written"
        )
    );
    assert!(mem.puts.lock().unwrap().is_empty(), "no put");
    assert!(mem.deletes.lock().unwrap().is_empty(), "no delete_prefix");
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
}

/// A key-ordered stream that is not in key order: the manifest would say
/// `KeyOrdered` over items a restore's key-range seek cannot trust, so the
/// tracker refuses the item and the job fails `encoding data file 1` with
/// an empty prefix (no chunk was out, so no put started). One cell with a
/// lower key after a higher one, one with a key repeated.
#[tokio::test]
async fn a_key_ordered_stream_out_of_order_fails_the_first_data_file() {
    let lower = vec![
        song("artist-00002", "title-00002"),
        song("artist-00001", "title-00001"),
    ];
    let repeated = vec![
        song("artist-00001", "title-00001"),
        song("artist-00001", "title-00001"),
    ];
    for (what, items) in [("a lower key", lower), ("a repeated key", repeated)] {
        let mem = Arc::new(MemStore::default());
        let fake = Arc::new(Fake::default());
        snapshot_fixture(&fake, &music_table(), items, true, None);
        let runtime = job_runtime(&mem);
        let details = create_music_backup(&fake, &runtime).await.unwrap();
        wait_for_jobs(&runtime).await;
        let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
        assert_eq!(row.status, BackupStatus::Failed, "{what}");
        assert!(
            row.failure_reason
                .as_deref()
                .unwrap()
                .starts_with("encoding data file 1: "),
            "{what}: {:?}",
            row.failure_reason
        );
        assert!(mem.puts.lock().unwrap().is_empty(), "{what}: no put");
        assert!(
            objects_under(&mem, &details.backup_arn).is_empty(),
            "{what}: an empty prefix"
        );
        assert_eq!(
            job_outcomes(&runtime),
            [("failed".to_owned(), 1.0)],
            "{what}"
        );
    }
}

/// `BackupStoreErrors` is recorded for the job's `delete_prefix` when its
/// failure path's deletion fails (the row FAILED, the prefix left to the
/// reconciler), and for the probe's `list` when the listing of a dead
/// owner's prefix fails after its manifest read (the row left for the next
/// cycle); each once, under the call's name.
#[tokio::test]
async fn the_jobs_failed_deletion_and_the_probes_failed_listing_are_counted() {
    use extenddb_core::metrics::MetricName;
    let errors = |drained: &[extenddb_core::metrics::FlushBucket]| {
        let mut ops: Vec<(String, f64)> = drained
            .iter()
            .filter(|b| b.metric == MetricName::BackupStoreErrors)
            .map(|b| (b.operation.clone(), b.sum))
            .collect();
        ops.sort_by(|a, b| a.0.cmp(&b.0));
        ops
    };

    // The job's deletion.
    let mem = Arc::new(MemStore::default());
    mem.fail_puts
        .store(true, std::sync::atomic::Ordering::Release);
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(2), true, None);
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    let prefix = BackupArn::parse(&details.backup_arn)
        .unwrap()
        .store_prefix();
    // Set before the job reaches its deletion: the put fails first and the
    // FAILED write comes between; the prefix is known from the response.
    *mem.fail_deletes_of.lock().unwrap() = Some(prefix.clone());
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    assert!(
        mem.deletes.lock().unwrap().is_empty(),
        "the deletion failed"
    );
    assert_eq!(
        errors(&runtime.metrics().unwrap().drain(Duration::ZERO)),
        [("delete_prefix".to_owned(), 1.0), ("put".to_owned(), 1.0)]
    );

    // The probe's listing.
    let mem = Arc::new(MemStore::default());
    mem.insert(STORE_IDENTITY_KEY, &identity_file("store-1", "catalog-1"));
    let fake = Fake::default();
    let dead = seed_dead_creating_with_manifest(&fake, &mem, true);
    let dead_prefix = BackupArn::parse(&dead).unwrap().store_prefix();
    *mem.fail_lists_of.lock().unwrap() = Some(dead_prefix);
    let metrics = Arc::new(extenddb_core::metrics::MetricsCollector::new());
    let runtime = runtime_on(&mem, store()).with_metrics(Arc::clone(&metrics));
    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(report.store_healthy, Some(true));
    assert!(report.creating_resolved.is_empty(), "{report:?}");
    assert_eq!(
        fake.rows.lock().unwrap()[&dead].status,
        BackupStatus::Creating,
        "left for the next cycle"
    );
    assert_eq!(
        errors(&metrics.drain(Duration::ZERO)),
        [("list".to_owned(), 1.0)]
    );
    *mem.fail_lists_of.lock().unwrap() = None;
    let report = cycle_on(&fake, &runtime).await;
    assert_eq!(report.creating_resolved, [(dead, BackupStatus::Available)]);
    assert!(errors(&metrics.drain(Duration::ZERO)).is_empty());
}

/// `write_status`'s generation check: a FAILED write whose outcome is
/// unknown is re-read, and a row that shows FAILED under another generation
/// is `NotMatched` (the reconciler resolved the same backup; the row is not
/// this claim's), so the failure path makes no `delete_prefix`. Without the
/// check the status alone would read as `Matched`.
#[tokio::test]
async fn a_re_read_showing_the_target_status_under_another_generation_is_not_matched() {
    let mem = Arc::new(MemStore::default());
    let fake = Fake::default();
    let arn = arn(ACCOUNT);
    let prefix = BackupArn::parse(&arn).unwrap().store_prefix();
    mem.insert(&format!("{prefix}data/000001.json.gz"), b"x");
    let mut r = row(&arn, BackupStatus::Failed);
    r.owner_generation = 9;
    fake.rows.lock().unwrap().insert(arn.clone(), r);
    let claim = fake.claim_on(&arn, 7);
    *fake.unknown_write.lock().unwrap() = Some(UnknownWrite::Lost);
    let path = job::failure::fail_row(&*claim, &*mem, &prefix, "a reason", None).await;
    match path {
        job::failure::FailurePath::NoStoreAccess(why) => {
            assert_eq!(why, "the row carries generation 9 (this claim's is 7)");
        }
        other => panic!("{other:?}"),
    }
    assert!(mem.deletes.lock().unwrap().is_empty(), "no delete_prefix");
    assert_eq!(objects_under(&mem, &arn).len(), 1);
    assert_eq!(fake.status_writes.lock().unwrap().len(), 1, "one write");
}

/// A data file whose stored size, as the store reports it, differs from the
/// encoder's compressed count fails the job: the manifest records the
/// encoder's count, the reconciler's dead-owner probe refuses a data file
/// whose stored size differs from the recorded one, and a live job must not
/// commit a manifest that probe would call incomplete. FAILED names both
/// figures; the prefix is deleted.
#[tokio::test]
async fn a_data_file_whose_stored_size_differs_from_the_encoders_fails_the_job() {
    let mem = Arc::new(MemStore::default());
    mem.misreport_put_size
        .store(true, std::sync::atomic::Ordering::Release);
    let fake = Arc::new(Fake::default());
    snapshot_fixture(&fake, &music_table(), songs(2), true, None);
    let runtime = job_runtime(&mem);
    let details = create_music_backup(&fake, &runtime).await.unwrap();
    wait_for_jobs(&runtime).await;
    let row = fake.rows.lock().unwrap()[&details.backup_arn].clone();
    assert_eq!(row.status, BackupStatus::Failed);
    let stored = mem.put_sizes.lock().unwrap()[0].1;
    assert_eq!(
        row.failure_reason.as_deref(),
        Some(
            format!(
                "the store reports {} bytes for data/000001.json.gz and the encoder wrote {stored}",
                stored + 1
            )
            .as_str()
        )
    );
    assert_eq!(mem.puts.lock().unwrap().len(), 1, "no document followed");
    assert!(objects_under(&mem, &details.backup_arn).is_empty());
    assert_eq!(job_outcomes(&runtime), [("failed".to_owned(), 1.0)]);
}

// --- the maintainer's second round on PR 8: a beat parked on the write lock --

mod review_round_3 {
    use super::*;

    /// The pre-put check's third clause: a beat parked outside its bound
    /// (SQLite's engine write lock, held by a GSI build) confirms the claim
    /// however old the last confirmed beat is, and stops confirming the
    /// moment the beat returns `Lost`. A parked beat that confirms on its
    /// release refreshes the last confirmed instant and clears the flight,
    /// so a timer that then stops leaves the check to the two-period rule.
    #[tokio::test(start_paused = true)]
    async fn a_beat_parked_on_the_write_lock_confirms_the_claim_until_it_returns_lost() {
        let fake = Arc::new(Fake::default());
        fake.rows
            .lock()
            .unwrap()
            .insert(arn(ACCOUNT), row(&arn(ACCOUNT), BackupStatus::Creating));
        // SQLite's shape: the park is the write lock's queue.
        fake.parks_before_bound
            .store(true, std::sync::atomic::Ordering::Release);
        let claim: Arc<dyn JobClaim> = Arc::from(fake.claim_on(&arn(ACCOUNT), 7));
        let period = Duration::from_secs(10);
        *fake.heartbeat_answer.lock().unwrap() = HeartbeatAnswer::Park;
        let handle = job::heartbeat::HeartbeatHandle::start(
            Arc::clone(&claim),
            period,
            None,
            0.0,
            tokio::time::Instant::now(),
        );
        // The first tick's beat parks at one period; at two and a half the
        // claim's insert is too old for the two-period clause alone.
        tokio::time::sleep(2 * period + period / 2).await;
        assert_eq!(
            fake.heartbeats.load(std::sync::atomic::Ordering::Acquire),
            0
        );
        assert!(!handle.lost().is_cancelled());
        assert!(
            handle.confirms_the_claim(),
            "a beat parked on the lock confirms the claim"
        );
        tokio::time::sleep(10 * period).await;
        assert!(handle.confirms_the_claim(), "however long it is parked");
        assert!(!handle.lost().is_cancelled());
        // The lock is released and the beat finds the claim lost.
        *fake.heartbeat_answer.lock().unwrap() = HeartbeatAnswer::Lost;
        fake.park_release.notify_one();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            fake.heartbeats.load(std::sync::atomic::Ordering::Acquire),
            1
        );
        assert!(handle.lost().is_cancelled());
        assert!(
            !handle.confirms_the_claim(),
            "the loss is read before the flight"
        );
        assert!(handle.reason().contains("the claim was lost"));
        drop(handle);

        // The same park, released with a confirmation: the check passes
        // from the fresh beat, and once the timer stops the flight is gone
        // and the two-period rule refuses two and a half periods later.
        *fake.heartbeat_answer.lock().unwrap() = HeartbeatAnswer::Park;
        let handle = job::heartbeat::HeartbeatHandle::start(
            claim,
            period,
            None,
            0.0,
            tokio::time::Instant::now(),
        );
        tokio::time::sleep(5 * period).await;
        assert!(handle.confirms_the_claim());
        *fake.heartbeat_answer.lock().unwrap() = HeartbeatAnswer::Confirm;
        fake.park_release.notify_one();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(!handle.lost().is_cancelled());
        assert!(handle.confirms_the_claim());
        handle.stop_timer();
        tokio::time::sleep(2 * period + period / 2).await;
        assert!(
            !handle.confirms_the_claim(),
            "no beat in flight and the last confirmed beat too old"
        );
        assert!(!handle.lost().is_cancelled());
    }

    /// On a backend whose beat does not park before its bound (PostgreSQL,
    /// MongoDB, Cassandra), a beat in flight confirms the claim only within
    /// one period of its start: a process stopped mid-beat (SIGSTOP, a
    /// paused VM) and resumed keeps the beat in flight until its timer runs
    /// again and raises the loss, and in that window another owner may hold
    /// the row, so the check refuses it; the same beat on a backend whose
    /// beat parks (SQLite) confirms the claim for as long as it waits.
    #[tokio::test(start_paused = true)]
    async fn a_beat_in_flight_past_its_bound_confirms_the_claim_only_where_the_beat_parks() {
        let fake = Arc::new(Fake::default());
        fake.rows
            .lock()
            .unwrap()
            .insert(arn(ACCOUNT), row(&arn(ACCOUNT), BackupStatus::Creating));
        let period = Duration::from_secs(10);
        for parks in [false, true] {
            fake.parks_before_bound
                .store(parks, std::sync::atomic::Ordering::Release);
            let claim: Arc<dyn JobClaim> = Arc::from(fake.claim_on(&arn(ACCOUNT), 7));
            // `Park` holds the beat outside the bound: the stop, on a backend
            // that does not park, or the lock's queue on one that does.
            *fake.heartbeat_answer.lock().unwrap() = HeartbeatAnswer::Park;
            // The claim's insert is old enough that the two-period clause
            // alone refuses from the first tick on.
            let handle = job::heartbeat::HeartbeatHandle::start(
                claim,
                period,
                None,
                0.0,
                tokio::time::Instant::now() - 2 * period,
            );
            // The first tick's beat starts at one period; half a period in it
            // is within its bound on every backend.
            tokio::time::sleep(period + period / 2).await;
            assert!(!handle.lost().is_cancelled());
            assert!(
                handle.confirms_the_claim(),
                "parks={parks}: a beat within its bound confirms the claim"
            );
            // One and a half periods into the beat: past its bound.
            tokio::time::sleep(period).await;
            assert!(!handle.lost().is_cancelled(), "the timer has not run again");
            assert_eq!(
                handle.confirms_the_claim(),
                parks,
                "parks={parks}: a beat past its bound confirms the claim only where it parks"
            );
            // The beat returns Lost: the loss is read before the flight.
            *fake.heartbeat_answer.lock().unwrap() = HeartbeatAnswer::Lost;
            fake.park_release.notify_one();
            tokio::time::sleep(Duration::from_millis(1)).await;
            assert!(handle.lost().is_cancelled());
            assert!(!handle.confirms_the_claim());
            drop(handle);
            *fake.heartbeat_answer.lock().unwrap() = HeartbeatAnswer::Confirm;
        }
    }
}

/// A restore of a backup the catalog index has a row for checks the target
/// name once the row is known to exist and AVAILABLE, before the backup's
/// in-use rule and before the store checks, the service's order (measured on
/// DynamoDB: an in-use backup into an existing table is
/// `TableAlreadyExistsException`, a backup into its own CREATING target
/// `TableInUseException`): an existing table of the name answers
/// `TableAlreadyExistsException` though the backup is in use, a CREATING
/// restore target `TableInUseException` naming its source backup, and a free
/// name reaches the in-use rule and then this release's refusal of a
/// store-backed restore. A row that is not AVAILABLE answers for itself
/// before the name is read.
#[tokio::test]
async fn a_catalog_row_restore_checks_the_target_name_after_the_row_and_before_its_in_use_rule() {
    // The row's store is the configured, healthy one, so a free name reaches
    // the store's refusal after the in-use rule.
    let runtime = runtime_on(&Arc::new(MemStore::default()), store());
    let fake = fake_with_row(BackupStatus::Available);
    fake.in_use
        .lock()
        .unwrap()
        .insert(arn(ACCOUNT), "Elsewhere".to_owned());
    let limits = LimitsConfig::default();
    let body = restore_body(json!({}));
    let restore_on = async |fake: &Fake| {
        restore_table_from_backup(fake, &runtime, ACCOUNT, REGION, &limits, &body).await
    };

    fake.tables.lock().unwrap().insert(
        "MusicRestored".to_owned(),
        TableDescription {
            table_name: "MusicRestored".to_owned(),
            table_status: TableStatus::Active,
            ..Default::default()
        },
    );
    let err = restore_on(&fake).await.unwrap_err();
    assert!(
        matches!(err, DynamoDbError::TableAlreadyExistsException(_)),
        "an in-use backup into an existing table: {err:?}"
    );

    fake.tables.lock().unwrap().insert(
        "MusicRestored".to_owned(),
        TableDescription {
            table_name: "MusicRestored".to_owned(),
            table_status: TableStatus::Creating,
            restore_summary: Some(RestoreSummary {
                source_backup_arn: Some(arn(ACCOUNT)),
                source_table_arn: None,
                restore_date_time: 0.0,
                restore_in_progress: true,
            }),
            ..Default::default()
        },
    );
    let err = restore_on(&fake).await.unwrap_err();
    assert!(
        matches!(err, DynamoDbError::TableInUseException(ref m) if m.contains(&arn(ACCOUNT))),
        "{err:?}"
    );

    fake.tables.lock().unwrap().remove("MusicRestored");
    let err = restore_on(&fake).await.unwrap_err();
    assert!(
        matches!(err, DynamoDbError::BackupInUseException(ref m)
            if *m == extenddb_storage::backup_catalog::backup_in_use_message(&arn(ACCOUNT))),
        "a free name reaches the in-use rule: {err:?}"
    );
    fake.in_use.lock().unwrap().clear();
    let err = restore_on(&fake).await.unwrap_err();
    assert!(
        matches!(err, DynamoDbError::ValidationException(ref m)
            if m.contains("is in a backup store; this release restores in-catalog backups only")),
        "{err:?}"
    );

    // A row that is not AVAILABLE: its own answer, the name unread. A
    // CREATING row is the backup being created (not measured on the service:
    // its backups were AVAILABLE 4 ms after CreateBackup answered).
    for (status, expect) in [
        (BackupStatus::Deleted, "not found"),
        (BackupStatus::Creating, "being created"),
    ] {
        let fake = fake_with_row(status);
        fake.tables.lock().unwrap().insert(
            "MusicRestored".to_owned(),
            TableDescription {
                table_name: "MusicRestored".to_owned(),
                table_status: TableStatus::Active,
                ..Default::default()
            },
        );
        let err = restore_on(&fake).await.unwrap_err();
        match expect {
            "not found" => assert!(
                matches!(err, DynamoDbError::BackupNotFoundException(_)),
                "{status:?}: {err:?}"
            ),
            _ => assert!(
                matches!(err, DynamoDbError::BackupInUseException(ref m)
                    if *m == format!("Backup is being created: {}", arn(ACCOUNT))),
                "{status:?}: {err:?}"
            ),
        }
        assert!(
            fake.table_reads.lock().unwrap().is_empty(),
            "{status:?}: the name is not read"
        );
    }
}

/// A `TableName` ARN of another account in another region is
/// `AccessDeniedException` on CreateBackup and ListBackups: the account is
/// checked before the region (measured on DynamoDB, twice each).
#[tokio::test]
async fn another_accounts_arn_in_another_region_is_access_denied_before_the_region_check() {
    let fake = Fake::default();
    let arn = "arn:aws:dynamodb:eu-west-1:210987654321:table/Music";
    let err = create_backup_table_name(
        &json!({ "TableName": arn, "BackupName": "nightly" }),
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
    )
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::AccessDeniedException(ref m) if m == "Access is denied"),
        "{err:?}"
    );
    let err = list_backups(
        &fake,
        ACCOUNT,
        REGION,
        &LimitsConfig::default(),
        &json!({ "TableName": arn }),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::AccessDeniedException(ref m) if m == "Access is denied"),
        "{err:?}"
    );
}
