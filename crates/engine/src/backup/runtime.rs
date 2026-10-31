// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The per-process state of the backup operations: the job slots every
//! backup and restore job holds one of, and the store this instance is
//! configured to write, with its health.
//!
//! The store's health is a state the checks set ([`super::store_identity`]
//! runs them; [`check_configured_store`] is the one the reconciler calls each
//! cycle): `Checking` until the first check of a store whose checks run after
//! the listener is up (an S3 store; the server binds and serves first, so an
//! endpoint that stalls never holds the data plane), `Healthy`, or
//! `Unhealthy` with the problem. A transition between the three is logged
//! once; while checking or unhealthy, the operations that touch the store
//! refuse with a `ValidationException` that names the store's kind and not
//! its root ([`BackupRuntime::require_healthy_store`]), `/health` reports the
//! `backup_store` component `checking` or `unhealthy`
//! ([`BackupRuntime::store_health`]), and the reconciler acts on no row of
//! the store. A check that cannot read the catalog changes nothing: the
//! condition is the catalog's, not the store's, and the next cycle reads
//! again.
//!
//! A check that does not finish within its bound is abandoned. An S3 store's
//! stalled request ends with the dropped future, before the SDK retries it,
//! and the next cycle sends a fresh one. A filesystem store's stalled call is
//! a blocking thread parked in a filesystem call until the mount answers,
//! which dropping the future cannot end, so a filesystem root's calls run as
//! tasks of their own that the abandoned check does not take with it, and
//! the store remembers that a call is in flight
//! ([`ConfiguredStore::check_call_in_flight`]): the next
//! [`check_configured_store`] starts no check while one is, recording the
//! same `unreachable` the abandoned one did, so a mount that stays stalled
//! parks one thread, not one per cycle. The thread costs nothing at exit:
//! the binary ends its runtime with a bounded shutdown that detaches it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use extenddb_backup_store::{BackupStore, ByteStream, ObjectMeta, StoreError};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::backup_catalog::{
    JobBound, StoreIdentity, StoreKind, limit_exceeded_message,
};
use extenddb_storage::{BackupCatalog, JobPermit};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use tokio::sync::Semaphore;

use super::store_identity::{self, StoreProblem};

/// The configuration key of the job bound the runtime enforces, as the
/// refusal names it.
const MAX_CONCURRENT_JOBS_KEY: &str = "[backup] max_concurrent_jobs";

/// How long a filesystem store's check may run, at startup and in a
/// reconciler cycle, before the store is treated as not answering: a local
/// disk answers in milliseconds, and a network mount that takes longer is
/// one that has stopped answering.
pub const LOCAL_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How long an S3 store's check may run in a reconciler cycle. The check
/// sends a few small requests (`validate`'s listing, the identity file's
/// `GetObject`, the manifest's `HeadObject`), each answered in well under a
/// second by a store that answers at all; the bound sits under the SDK's
/// per-attempt timeout ([`extenddb_backup_store::S3_ATTEMPT_TIMEOUT`], 60 s)
/// so a timed-out check is dropped before the SDK starts a retry of its
/// stalled request, and no retry outlives the check. A stalled endpoint thus
/// costs a cycle this bound, not the SDK's whole retry budget.
pub const S3_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const _: () = assert!(
    S3_CHECK_TIMEOUT.as_secs() < extenddb_backup_store::S3_ATTEMPT_TIMEOUT.as_secs(),
    "the S3 check bound must fire before the SDK's attempt timeout"
);

/// One per server process, shared by every request through the operation
/// context.
pub struct BackupRuntime {
    slots: Arc<Semaphore>,
    max_concurrent_jobs: usize,
    /// The store this instance writes new backups to and deletes from;
    /// `None` while no store is configured, when every catalog row is a row
    /// of another store.
    configured_store: Option<ConfiguredStore>,
    /// Whether CreateBackup takes the in-catalog path whatever the store
    /// configuration (a backend that cannot pin a table snapshot); such a
    /// create is not refused by an unhealthy store.
    legacy_create_only: bool,
    /// `[backup] stale_job_after_secs`: the window a job's claim check reads
    /// the maintenance claim with.
    stale_job_after: std::time::Duration,
}

impl std::fmt::Debug for BackupRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupRuntime")
            .field("max_concurrent_jobs", &self.max_concurrent_jobs)
            .field(
                "configured_store",
                &self
                    .configured_store
                    .as_ref()
                    .map(ConfiguredStore::identity),
            )
            .field("legacy_create_only", &self.legacy_create_only)
            .finish_non_exhaustive()
    }
}

/// The store this instance is configured with: its kind and root (the
/// configuration's), its id once the identity check has read it, the handle,
/// the spool for restores from it, and its health.
///
/// The id is `None` until a check passes: a store whose startup check failed
/// (a transport failure, say) has an unknown id, and a row of its kind and
/// root is still a row of this store for the refusals, so those rows answer
/// "the store is unhealthy" rather than "another store".
pub struct ConfiguredStore {
    /// `filesystem` or `s3`.
    pub kind: StoreKind,
    /// The root label rows record (`store_root`).
    pub root: String,
    /// The opened store.
    pub store: Arc<dyn BackupStore>,
    /// The restore spool for an S3 store: the directory and the
    /// `<hostname>:<spool_path>` label a restore job record names.
    pub spool: Option<Spool>,
    store_id: RwLock<Option<String>>,
    health: RwLock<StoreHealth>,
    /// Whether the store's `validate` has passed; the checks run it first
    /// until it has.
    validated: AtomicBool,
    /// Whether a call of a check on a filesystem root is still running
    /// (module doc): set when the call starts, cleared by the call's own task
    /// when the mount answers, so a check abandoned at its bound leaves it
    /// set until then. Never set on an S3 store.
    check_call_in_flight: Arc<AtomicBool>,
}

/// The restore spool of an S3 store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spool {
    pub path: std::path::PathBuf,
    /// `<hostname>:<spool_path>`, the host whose spool holds a restore's
    /// downloaded files.
    pub spool_host: String,
}

/// The store's health as the last check left it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreHealth {
    /// No check has finished yet: the store's checks run after the listener
    /// is up and the first has not returned.
    Checking,
    Healthy,
    Unhealthy(StoreProblem),
}

impl StoreHealth {
    /// The one or two words the API refusal and `/health` name: the
    /// problem's category ([`StoreProblem::category`]), or `check pending`.
    #[must_use]
    pub fn category(&self) -> Option<&'static str> {
        match self {
            Self::Healthy => None,
            Self::Checking => Some("check pending"),
            Self::Unhealthy(problem) => Some(problem.category()),
        }
    }
}

/// What `/health` reports for the `backup_store` component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreHealthReport {
    /// `filesystem` or `s3`.
    pub kind: &'static str,
    /// `healthy`, `unhealthy` or `checking`.
    pub status: &'static str,
    /// The problem's category and nothing else: `/health` is unauthenticated,
    /// and the ids, ARNs and catalog id the operator's log line carries stay
    /// out of it. `None` when healthy or checking.
    pub reason: Option<&'static str>,
}

impl ConfiguredStore {
    /// A store of `kind` at `root` whose startup check (`validate` passed,
    /// then `establish`) ended in `outcome`: the identity it settled
    /// (healthy), or the problem that makes the store unhealthy from the
    /// start (a transport failure, or a failed identity check or probe:
    /// section 7's rule that a store problem never stops the data plane). A
    /// problem leaves the id unknown until a later check passes.
    #[must_use]
    pub fn new(
        kind: StoreKind,
        root: String,
        store: Arc<dyn BackupStore>,
        spool: Option<Spool>,
        outcome: Result<StoreIdentity, StoreProblem>,
    ) -> Self {
        let (store_id, health) = match outcome {
            Ok(identity) => (Some(identity.store_id), StoreHealth::Healthy),
            Err(problem) => (None, StoreHealth::Unhealthy(problem)),
        };
        Self {
            kind,
            root,
            store,
            spool,
            store_id: RwLock::new(store_id),
            health: RwLock::new(health),
            validated: AtomicBool::new(true),
            check_call_in_flight: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A store of `kind` at `root` that `open` accepted and whose checks
    /// (`validate`, then the identity check) have not run: the first
    /// [`check_configured_store`] runs both and records the result. Until
    /// then the store is [`StoreHealth::Checking`].
    #[must_use]
    pub fn checking(
        kind: StoreKind,
        root: String,
        store: Arc<dyn BackupStore>,
        spool: Option<Spool>,
    ) -> Self {
        Self {
            kind,
            root,
            store,
            spool,
            store_id: RwLock::new(None),
            health: RwLock::new(StoreHealth::Checking),
            validated: AtomicBool::new(false),
            check_call_in_flight: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The store's identity, once a check has read its id.
    #[must_use]
    pub fn identity(&self) -> Option<StoreIdentity> {
        let id = self
            .store_id
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        Some(StoreIdentity {
            store_kind: self.kind,
            store_root: self.root.clone(),
            store_id: id,
        })
    }

    /// Whether a row's store is this one: the kind and the root, and the id
    /// when this store's is known.
    #[must_use]
    pub fn is_store_of(&self, row_store: &StoreIdentity) -> bool {
        if row_store.store_kind != self.kind || row_store.store_root != self.root {
            return false;
        }
        match self
            .store_id
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_deref()
        {
            Some(id) => id == row_store.store_id,
            None => true,
        }
    }

    /// The health as the last check left it.
    #[must_use]
    pub fn health(&self) -> StoreHealth {
        self.health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Whether the last check passed.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        matches!(self.health(), StoreHealth::Healthy)
    }

    /// Record the outcome of a check: the identity it settled (the id is
    /// kept), or the problem. A transition is logged once: to unhealthy at
    /// `error` with the problem's detail, to healthy at `info`; a check
    /// whose outcome is in the last one's category logs nothing, so a store
    /// that stays down costs one line however its error text varies between
    /// cycles. A check that could not read the catalog
    /// ([`StoreProblem::Catalog`]) leaves the health as it was and logs the
    /// read failure: the condition is not the store's. Returns whether this
    /// check changed the health's category (and so logged a transition).
    pub fn record_check(&self, outcome: Result<StoreIdentity, StoreProblem>) -> bool {
        let next = match outcome {
            Ok(identity) => {
                *self
                    .store_id
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(identity.store_id);
                StoreHealth::Healthy
            }
            Err(StoreProblem::Catalog(e)) => {
                tracing::warn!(
                    "backup store ({}) check could not read the catalog ({e}); the store's \
                     health is left as it was and the next cycle reads again",
                    self.kind
                );
                return false;
            }
            Err(problem) => StoreHealth::Unhealthy(problem),
        };
        let mut health = self
            .health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if health.category() == next.category() {
            *health = next;
            return false;
        }
        match (&*health, &next) {
            (StoreHealth::Checking, StoreHealth::Healthy) => tracing::info!(
                "backup store ({}) {} passed its check (store id {}); it serves restores and \
                 deletes of the backups it holds",
                self.kind,
                self.root,
                self.identity().map(|i| i.store_id).unwrap_or_default()
            ),
            (_, StoreHealth::Healthy) => {
                tracing::info!("backup store ({}) is healthy again", self.kind);
            }
            (_, StoreHealth::Unhealthy(problem)) => tracing::error!(
                "backup store ({}) {} is unhealthy: {problem}; restore and delete of a backup \
                 in the store are refused until the check passes; the check runs again each \
                 reconciler cycle",
                self.kind,
                self.root
            ),
            (_, StoreHealth::Checking) => {}
        }
        *health = next;
        true
    }

    /// Whether the store's `validate` has passed.
    #[must_use]
    pub fn is_validated(&self) -> bool {
        self.validated.load(Ordering::Acquire)
    }

    /// How long one check of this store may run before it is abandoned
    /// ([`LOCAL_CHECK_TIMEOUT`] for a filesystem root, [`S3_CHECK_TIMEOUT`]
    /// for a bucket).
    #[must_use]
    pub fn check_timeout(&self) -> std::time::Duration {
        match self.kind {
            StoreKind::Filesystem => LOCAL_CHECK_TIMEOUT,
            StoreKind::S3 => S3_CHECK_TIMEOUT,
        }
    }

    /// Whether a call of an earlier check has not returned (module doc).
    #[must_use]
    pub fn check_call_in_flight(&self) -> bool {
        self.check_call_in_flight.load(Ordering::Acquire)
    }
}

/// The problem a check, or a command's identity read, that did not finish
/// within `bound` is recorded as: a transport failure naming the bound.
fn did_not_finish(what: &str, bound: std::time::Duration) -> StoreProblem {
    StoreProblem::Transport(format!(
        "the {what} did not finish within {} s",
        bound.as_secs()
    ))
}

/// The check of the configured store a reconciler cycle, and a command that
/// needs the store's identity, run and record through
/// [`ConfiguredStore::record_check`]: `validate` first while it has not
/// passed (its failure is the problem [`StoreProblem::from`] makes of the
/// store's error, a transport failure [`StoreProblem::Transport`] with the
/// cause chain, a denial [`StoreProblem::Denied`], anything else
/// [`StoreProblem::Validate`]; each is retried next cycle), then the
/// identity check, which is [`store_identity::recheck`] for a store whose
/// id is known and [`store_identity::establish`] for one whose checks have
/// not passed yet (it settles the id when it passes). `backend` names the
/// storage backend in an identity file the check has to write.
///
/// The whole check runs under [`ConfiguredStore::check_timeout`]: a store
/// that does not answer within it (a stalled endpoint, a mount that has
/// stopped answering) is [`StoreProblem::Transport`] naming the bound, and
/// the caller's cycle goes on. An S3 store's abandoned request is dropped
/// with the future. A filesystem root's calls run as tasks of their own
/// ([`CheckCalls`]), which the abandoned check does not take with it: the
/// call's synchronous work runs on the blocking pool, where the thread stays
/// parked until the mount answers, and the task is what clears the store's
/// in-flight mark when it does. While that mark is set no check starts: the
/// store is recorded `unreachable` again, with the reason, and nothing new
/// is parked.
///
/// # Errors
///
/// The problem that makes the store unhealthy
/// ([`store_identity`]'s module doc).
pub async fn check_configured_store<C: BackupCatalog + ?Sized>(
    store: &ConfiguredStore,
    catalog: &C,
    backend: &str,
) -> Result<StoreIdentity, StoreProblem> {
    if store.check_call_in_flight() {
        tracing::info!(
            "backup store ({}) check was not started: a call of the check abandoned at an \
             earlier cycle has not returned (the store is still not answering); the next cycle \
             checks again",
            store.kind
        );
        return Err(StoreProblem::Transport(
            "a call of an earlier check has not returned".to_owned(),
        ));
    }
    let bound = store.check_timeout();
    match tokio::time::timeout(bound, check_unbounded(store, catalog, backend)).await {
        Ok(outcome) => outcome,
        Err(_) => {
            tracing::info!(
                "backup store ({}) check did not finish within {} s and was abandoned; the \
                 cycle's other duties ran, and the next cycle checks again",
                store.kind,
                bound.as_secs()
            );
            Err(did_not_finish("check", bound))
        }
    }
}

/// [`check_configured_store`] without its bound.
async fn check_unbounded<C: BackupCatalog + ?Sized>(
    store: &ConfiguredStore,
    catalog: &C,
    backend: &str,
) -> Result<StoreIdentity, StoreProblem> {
    // A filesystem root's calls run as tasks of their own (`CheckCalls`): a
    // stalled call is a parked thread, which dropping the future cannot
    // end. An S3 store's calls are the SDK's requests, which dropping the
    // future does end, before the SDK retries them (the bound sits under
    // its attempt timeout for that), so they run in the check's own future
    // and the next cycle sends a fresh request.
    let calls;
    let via: &dyn BackupStore = match store.kind {
        StoreKind::Filesystem => {
            calls = CheckCalls {
                store: &store.store,
                in_flight: &store.check_call_in_flight,
            };
            &calls
        }
        StoreKind::S3 => &*store.store,
    };
    if !store.is_validated() {
        match via.validate().await {
            Ok(()) => store.validated.store(true, Ordering::Release),
            Err(e) => return Err(StoreProblem::from(e)),
        }
    }
    match store.identity() {
        Some(identity) => store_identity::recheck(via, &identity, catalog, backend)
            .await
            .map(|()| identity),
        None => store_identity::establish(via, store.kind, &store.root, catalog, backend).await,
    }
}

/// A command's read of the store's identity ([`store_identity::read_identity`],
/// which writes nothing), under the store's [`ConfiguredStore::check_timeout`]
/// as the server's check is: a read that does not finish within the bound is
/// [`StoreProblem::Transport`] naming it, where the store's own retries (the
/// SDK's three attempts of 60 s on an endpoint that accepts and never
/// answers) or a stalled mount would hold the command for minutes or for
/// good.
///
/// # Errors
///
/// The problem that would make the store unhealthy
/// ([`store_identity`]'s module doc), or the bound.
pub async fn read_identity_bounded<C: BackupCatalog + ?Sized>(
    store: &ConfiguredStore,
    catalog: &C,
) -> Result<Option<StoreIdentity>, StoreProblem> {
    let bound = store.check_timeout();
    tokio::time::timeout(
        bound,
        store_identity::read_identity(&*store.store, store.kind, &store.root, catalog),
    )
    .await
    .unwrap_or_else(|_| Err(did_not_finish("identity read", bound)))
}

/// A filesystem store as a check calls it (module doc): each call runs as a
/// task of its own, owning the store handle, so a check abandoned at its
/// bound leaves the call running until the mount answers, and the task
/// clears the store's in-flight mark when the call returns. A call that finds the mark
/// set (a call of an earlier check has not returned) is refused at once as
/// a transport failure, so no second call is parked beside the first. The
/// check calls `validate`, `get`, `head` and `put_if_absent`; `put`, `list`
/// and `delete_prefix` are forwarded for the trait's sake and marked the
/// same way.
struct CheckCalls<'a> {
    store: &'a Arc<dyn BackupStore>,
    in_flight: &'a Arc<AtomicBool>,
}

/// Clears the in-flight mark when the call's task ends, by any exit.
struct ClearOnDrop(Arc<AtomicBool>);

impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl CheckCalls<'_> {
    /// Run `call` on the store as a task of its own, marked in flight until
    /// it returns.
    fn run<T, F>(&self, call: F) -> BoxFuture<'static, Result<T, StoreError>>
    where
        T: Send + 'static,
        F: FnOnce(Arc<dyn BackupStore>) -> BoxFuture<'static, Result<T, StoreError>>
            + Send
            + 'static,
    {
        if self.in_flight.swap(true, Ordering::AcqRel) {
            return Box::pin(std::future::ready(Err(StoreError::Transport {
                source: "a call of an earlier check has not returned".into(),
            })));
        }
        let clear = ClearOnDrop(Arc::clone(self.in_flight));
        let store = Arc::clone(self.store);
        let task = tokio::spawn(async move {
            let _clear = clear;
            call(store).await
        });
        Box::pin(async move {
            task.await
                .map_err(|e| StoreError::Other(format!("check call task failed: {e}")))?
        })
    }
}

impl BackupStore for CheckCalls<'_> {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: ByteStream,
    ) -> BoxFuture<'a, Result<ObjectMeta, StoreError>> {
        let key = key.to_owned();
        self.run(move |store| Box::pin(async move { store.put(&key, body).await }))
    }

    fn put_if_absent<'a>(
        &'a self,
        key: &'a str,
        body: ByteStream,
    ) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
        let key = key.to_owned();
        self.run(move |store| Box::pin(async move { store.put_if_absent(&key, body).await }))
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<ByteStream, StoreError>> {
        let key = key.to_owned();
        self.run(move |store| Box::pin(async move { store.get(&key).await }))
    }

    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
        let key = key.to_owned();
        self.run(move |store| Box::pin(async move { store.head(&key).await }))
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxStream<'a, Result<ObjectMeta, StoreError>> {
        let prefix = prefix.to_owned();
        let listed = self.run(move |store| {
            Box::pin(async move {
                use futures::TryStreamExt as _;
                store.list(&prefix).try_collect::<Vec<_>>().await
            })
        });
        Box::pin(
            futures::stream::once(listed).flat_map(|listed| match listed {
                Ok(metas) => futures::stream::iter(metas.into_iter().map(Ok)).boxed(),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }),
        )
    }

    fn delete_prefix<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<u64, StoreError>> {
        let prefix = prefix.to_owned();
        self.run(move |store| Box::pin(async move { store.delete_prefix(&prefix).await }))
    }

    fn validate(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        self.run(|store| Box::pin(async move { store.validate().await }))
    }
}

impl BackupRuntime {
    /// `max_concurrent_jobs` slots and no configured store.
    #[must_use]
    pub fn new(max_concurrent_jobs: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(max_concurrent_jobs)),
            max_concurrent_jobs,
            configured_store: None,
            legacy_create_only: false,
            stale_job_after: std::time::Duration::from_secs(300),
        }
    }

    /// The same runtime with `[backup] stale_job_after_secs` set, the window
    /// the jobs' maintenance-claim check reads with (the default is the
    /// configuration's default, 300 s).
    #[must_use]
    pub fn with_stale_job_after(mut self, stale_job_after: std::time::Duration) -> Self {
        self.stale_job_after = stale_job_after;
        self
    }

    /// `[backup] stale_job_after_secs`.
    #[must_use]
    pub fn stale_job_after(&self) -> std::time::Duration {
        self.stale_job_after
    }

    /// The same runtime with `store` configured.
    #[must_use]
    pub fn with_store(mut self, store: ConfiguredStore) -> Self {
        self.configured_store = Some(store);
        self
    }

    /// The same runtime with CreateBackup bound to the in-catalog path
    /// whatever the store configuration.
    #[must_use]
    pub fn with_legacy_create_only(mut self, legacy_create_only: bool) -> Self {
        self.legacy_create_only = legacy_create_only;
        self
    }

    /// The slots jobs take permits from.
    #[must_use]
    pub fn slots(&self) -> &Arc<Semaphore> {
        &self.slots
    }

    /// Permits no job holds right now.
    #[must_use]
    pub fn free_slots(&self) -> usize {
        self.slots.available_permits()
    }

    /// The configured store, if any.
    #[must_use]
    pub fn configured_store(&self) -> Option<&ConfiguredStore> {
        self.configured_store.as_ref()
    }

    /// The configured store, taken out of a runtime a command built only to
    /// reach it.
    #[must_use]
    pub fn into_configured_store(self) -> Option<ConfiguredStore> {
        self.configured_store
    }

    /// Whether CreateBackup takes the in-catalog path whatever the store
    /// configuration.
    #[must_use]
    pub fn legacy_create_only(&self) -> bool {
        self.legacy_create_only
    }

    /// What `/health` reports for the `backup_store` component; `None` when
    /// no store is configured.
    #[must_use]
    pub fn store_health(&self) -> Option<StoreHealthReport> {
        let store = self.configured_store.as_ref()?;
        let kind = store.kind.as_str();
        Some(match store.health() {
            StoreHealth::Healthy => StoreHealthReport {
                kind,
                status: "healthy",
                reason: None,
            },
            StoreHealth::Checking => StoreHealthReport {
                kind,
                status: "checking",
                reason: None,
            },
            StoreHealth::Unhealthy(problem) => StoreHealthReport {
                kind,
                status: "unhealthy",
                reason: Some(problem.category()),
            },
        })
    }

    /// Take a job slot without waiting, or `LimitExceededException` when every
    /// slot is held: a queued job would hold a snapshot or a transaction
    /// while it waited. The text is the one every refusal over a job bound
    /// answers ([`limit_exceeded_message`]), naming this one.
    pub(crate) fn take_permit(&self) -> Result<JobPermit, DynamoDbError> {
        JobPermit::try_acquire(&self.slots).ok_or_else(|| {
            DynamoDbError::LimitExceededException(limit_exceeded_message(JobBound::Counted {
                name: MAX_CONCURRENT_JOBS_KEY,
                limit: self.max_concurrent_jobs,
            }))
        })
    }

    /// Whether `store` is the one this instance is configured with. With no
    /// store configured, no row's store is.
    pub(crate) fn is_configured_store(&self, store: &StoreIdentity) -> bool {
        self.configured_store
            .as_ref()
            .is_some_and(|configured| configured.is_store_of(store))
    }

    /// The configured store, when its last check passed; otherwise (no
    /// check has finished, or the last one failed) the `ValidationException`
    /// the operations that touch the store answer, naming the store's kind
    /// and the problem's category and not its root (no API error names a
    /// root).
    pub(crate) fn require_healthy_store(&self) -> Result<&ConfiguredStore, DynamoDbError> {
        let store = self.configured_store.as_ref().ok_or_else(|| {
            DynamoDbError::ValidationException(
                "No backup store is configured on this server ([backup.store])".to_owned(),
            )
        })?;
        match store.health() {
            StoreHealth::Healthy => Ok(store),
            StoreHealth::Checking => Err(DynamoDbError::ValidationException(format!(
                "The {} backup store configured on this server has not passed a check yet (its \
                 first check is pending); the operation is refused until it does",
                store.kind
            ))),
            StoreHealth::Unhealthy(problem) => Err(DynamoDbError::ValidationException(format!(
                "The {} backup store configured on this server is unhealthy ({}); the operation \
                 is refused until the store's check passes",
                store.kind,
                problem.category()
            ))),
        }
    }
}
