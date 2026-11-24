// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog and data-store migrations, distributed migration lease, and
//! `ADR-0023` serving registrations for the Cloud Bigtable storage backend.
//!
//! - Catalog ledger rows live in `__extenddb_catalog__` on `instance_id` under
//!   `migration_ledger:catalog:<id:04>`.
//! - Data-store ledger rows live in `__extenddb_data_ledger__` on
//!   `effective_data_instance_id()` under `migration_ledger:data:<id:04>`.
//! - Migrators acquire `lock:catalog_migrations` in `__extenddb_catalog__`,
//!   renew it in the background while making progress, and wait for all live
//!   `server_registration:<token>` rows to drain before applying migrations.
//! - Servers write `server_registration:<token>`, verify that
//!   `lock:catalog_migrations` is free, and verify that `Runner::status` is
//!   `Current` (or `AheadCompatible`) before opening stores or spawning workers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use extenddb_storage::error::StorageError;
use extenddb_storage::hooks::{Reregistration, ServingRegistrar};
use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, GroupDetail, GroupListEntry, ManagementStore, OpError,
    OpResult, RoleDetail, RoleListEntry, SecretPage, SecretWrite, SessionPolicyEntry,
    SettingsStore, StoredDocument, UserDetail, UserListEntry,
};
use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, CatalogTraces, Done, HeldLock, Kind, LedgerRow, Listed,
    LockedStores, Migration, MigrationBackend, MigrationError, MigrationId, Released, RunKind,
    RunReport, Runner, Step, StoreBody, StoreKind, StoreRef, Target,
};
use extenddb_storage::secret_keys::SecretTable;
use futures::future::BoxFuture;
use serde_json::json;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::BigtableCatalogStore;
use crate::catalog::{CATALOG_TABLE, CF, Catalog, DATA_LEDGER_TABLE, keys};
use crate::config::BigtableStorageConfig;
use crate::data::admin::{AdminClient, gc_max_versions};
use crate::data::client::BigtableClient;
use crate::m0001_baseline::{M0001CatalogBaseline, M0001DataBaseline};

/// Catalog lock name under `lock:catalog_migrations`.
pub const MIGRATION_LOCK_NAME: &str = "catalog_migrations";

/// Default command label recorded in `server_registration:<token>`.
pub const SERVE_COMMAND: &str = "extenddb serve";

/// Per-RPC timeout for migration lock and serving registration requests.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

fn now_epoch_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

fn rfc3339_now() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| time::OffsetDateTime::now_utc().to_string())
}

fn host_name() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .unwrap_or_else(|| "unknown-host".to_owned())
}

/// Timings governing the migration lease and server registrations (`ADR-0023`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    pub lease: Duration,
    pub renew_every: Duration,
    pub margin: Duration,
    pub idle_limit: Duration,
    pub registration: Duration,
    pub registration_deadline: Duration,
    pub registration_renew_every: Duration,
    pub registration_retry_every: Duration,
    pub retry_every: Duration,
}

impl Timing {
    /// Production timings matching `ADR-0023`: 60 s migration lease renewed
    /// every 15 s with a 25 s safety margin, and 45 s server registrations
    /// renewed every 5 s with a 30 s loss deadline.
    pub const PRODUCTION: Self = Self {
        lease: Duration::from_mins(1),
        renew_every: Duration::from_secs(15),
        margin: Duration::from_secs(25),
        idle_limit: Duration::from_secs(30),
        registration: Duration::from_secs(45),
        registration_deadline: Duration::from_secs(30),
        registration_renew_every: Duration::from_secs(5),
        registration_retry_every: Duration::from_secs(1),
        retry_every: Duration::from_millis(250),
    };

    #[must_use]
    pub fn valid_for(self) -> Duration {
        self.lease.saturating_sub(self.margin)
    }
}

/// Monotonic lease tracker shared between the migration renewal task,
/// migration runner steps, and `BigtableBootstrapper`.
pub struct MigrationLease {
    renewed: Mutex<Instant>,
    progress: Mutex<Instant>,
    lost: Mutex<Option<String>>,
    timing: Timing,
}

impl MigrationLease {
    #[must_use]
    pub fn new(renewed: Instant, timing: Timing) -> Arc<Self> {
        Arc::new(Self {
            renewed: Mutex::new(renewed),
            progress: Mutex::new(Instant::now()),
            lost: Mutex::new(None),
            timing,
        })
    }

    /// Verify that the holder still owns a valid, un-expired lease within the
    /// safety margin.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError::Store`] if the lease was lost or its safety
    /// window has elapsed since the last confirmed renewal.
    pub fn check(&self) -> Result<(), MigrationError> {
        if let Some(reason) = self
            .lost
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            return Err(MigrationError::Store(reason));
        }
        let since = self
            .renewed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .elapsed();
        if since >= self.timing.valid_for() {
            return Err(self.mark_lost(format!(
                "the migration lock was last renewed {} s ago, exceeding the {} s safety window; \
                 stopping before another process can claim the lock",
                since.as_secs(),
                self.timing.valid_for().as_secs()
            )));
        }
        Ok(())
    }

    /// Record forward progress so the renewal task knows the holder is not hung.
    pub fn progressed(&self) {
        *self.progress.lock().unwrap_or_else(PoisonError::into_inner) = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.progress
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .elapsed()
    }

    fn renewed_at(&self, sent: Instant) {
        *self.renewed.lock().unwrap_or_else(PoisonError::into_inner) = sent;
    }

    fn mark_lost(&self, reason: String) -> MigrationError {
        let mut lost = self.lost.lock().unwrap_or_else(PoisonError::into_inner);
        MigrationError::Store(lost.get_or_insert(reason).clone())
    }
}

/// Active migration lock in `__extenddb_catalog__` (`lock:catalog_migrations`)
/// paired with its background renewal task.
pub struct MigrationLock {
    catalog_client: Arc<BigtableClient>,
    owner: String,
    lease: Arc<MigrationLease>,
    renewal: JoinHandle<()>,
    released: AtomicBool,
}

impl MigrationLock {
    /// Acquire `lock:catalog_migrations`, polling until `timeout` elapses, and
    /// start background renewal.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError::Locked`] if another migrator holds the lock
    /// past `timeout`, or [`MigrationError::Store`] on RPC failure.
    pub async fn acquire(
        catalog_client: Arc<BigtableClient>,
        timeout: Duration,
        timing: Timing,
    ) -> Result<Self, MigrationError> {
        let owner = format!(
            "migrate-{}-pid{}-{}",
            host_name(),
            std::process::id(),
            Uuid::new_v4()
        );
        let deadline = Instant::now() + timeout;
        let cat = Catalog::new(&catalog_client);

        let claimed_at = loop {
            let sent = Instant::now();
            match tokio::time::timeout(
                REQUEST_TIMEOUT,
                cat.try_lock(MIGRATION_LOCK_NAME, &owner, timing.lease),
            )
            .await
            {
                Ok(Ok(true)) => break sent,
                Ok(Ok(false)) => {
                    if Instant::now() >= deadline {
                        let holder = cat
                            .read_live_lock_owner(MIGRATION_LOCK_NAME)
                            .await
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| "another migrator".to_owned());
                        return Err(MigrationError::Locked(format!(
                            "{holder} holds lock:{MIGRATION_LOCK_NAME} in {CATALOG_TABLE}"
                        )));
                    }
                    tokio::time::sleep(timing.retry_every).await;
                }
                Ok(Err(e)) => return Err(MigrationError::store("acquire migration lock", e)),
                Err(_) => {
                    return Err(MigrationError::Store(format!(
                        "acquire migration lock: timed out after {} s",
                        REQUEST_TIMEOUT.as_secs()
                    )));
                }
            }
        };

        let lease = MigrationLease::new(claimed_at, timing);
        let renewal =
            spawn_lock_renewal(catalog_client.clone(), owner.clone(), lease.clone(), timing);
        Ok(Self {
            catalog_client,
            owner,
            lease,
            renewal,
            released: AtomicBool::new(false),
        })
    }

    #[must_use]
    pub fn lease(&self) -> Arc<MigrationLease> {
        self.lease.clone()
    }

    /// Wait up to `timeout` for all live `server_registration:<token>` rows to
    /// disappear or expire.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError::Locked`] if servers remain registered past
    /// `timeout`, or [`MigrationError::Store`] on lease loss or RPC failure.
    pub async fn wait_for_servers(&self, timeout: Duration) -> Result<(), MigrationError> {
        let deadline = Instant::now() + timeout;
        loop {
            self.lease.check()?;
            let live = live_server_registrations(&self.catalog_client).await?;
            self.lease.progressed();
            if live.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(MigrationError::Locked(format!(
                    "cannot migrate while {} server(s) are still serving this catalog: {}",
                    live.len(),
                    live.join("; ")
                )));
            }
            tokio::time::sleep(self.lease.timing.retry_every).await;
        }
    }

    /// Stop background renewal and release `lock:catalog_migrations` if still
    /// owned by this holder.
    pub async fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.renewal.abort();
        let cat = Catalog::new(&self.catalog_client);
        let _ = tokio::time::timeout(
            REQUEST_TIMEOUT,
            cat.release_lock(MIGRATION_LOCK_NAME, &self.owner),
        )
        .await;
    }
}

impl Drop for MigrationLock {
    fn drop(&mut self) {
        self.renewal.abort();
        if self.released.load(Ordering::Acquire) {
            return;
        }
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            let client = self.catalog_client.clone();
            let owner = self.owner.clone();
            rt.spawn(async move {
                let cat = Catalog::new(&client);
                let _ = tokio::time::timeout(
                    REQUEST_TIMEOUT,
                    cat.release_lock(MIGRATION_LOCK_NAME, &owner),
                )
                .await;
            });
        }
    }
}

fn spawn_lock_renewal(
    catalog_client: Arc<BigtableClient>,
    owner: String,
    lease: Arc<MigrationLease>,
    timing: Timing,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut wait = timing.renew_every;
        loop {
            let remaining = {
                let since = lease
                    .renewed
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .elapsed();
                timing.valid_for().saturating_sub(since)
            };
            if remaining.is_zero() {
                let _ = lease.check();
                return;
            }
            tokio::time::sleep(wait.min(remaining)).await;
            if lease.check().is_err() {
                return;
            }
            if lease.idle_for() >= timing.idle_limit {
                let _ = lease.mark_lost(format!(
                    "migration holder made no progress for {} s; stopping lock renewal",
                    timing.idle_limit.as_secs()
                ));
                return;
            }
            let remaining = {
                let since = lease
                    .renewed
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .elapsed();
                timing.valid_for().saturating_sub(since)
            };
            if remaining.is_zero() {
                let _ = lease.check();
                return;
            }
            let sent = Instant::now();
            let bound = REQUEST_TIMEOUT.min(timing.renew_every).min(remaining);
            let cat = Catalog::new(&catalog_client);
            match tokio::time::timeout(
                bound,
                cat.renew_lock(MIGRATION_LOCK_NAME, &owner, timing.lease),
            )
            .await
            {
                Ok(Ok(true)) => {
                    lease.renewed_at(sent);
                    wait = timing.renew_every;
                }
                Ok(Ok(false)) => {
                    let _ = lease.mark_lost(format!(
                        "lock:{MIGRATION_LOCK_NAME} in {CATALOG_TABLE} was lost or stolen"
                    ));
                    return;
                }
                Ok(Err(e)) => {
                    if lease.check().is_err() {
                        return;
                    }
                    tracing::warn!(error = %e, "migration lock renewal failed; retrying");
                    wait = Duration::from_secs(1).min(timing.renew_every);
                }
                Err(_) => {
                    if lease.check().is_err() {
                        return;
                    }
                    tracing::warn!("migration lock renewal timed out; retrying");
                    wait = Duration::from_secs(1).min(timing.renew_every);
                }
            }
        }
    })
}

async fn live_server_registrations(
    catalog_client: &BigtableClient,
) -> Result<Vec<String>, MigrationError> {
    let cat = Catalog::new(catalog_client);
    let rows = match tokio::time::timeout(
        REQUEST_TIMEOUT,
        cat.scan_prefix(keys::SERVER_REGISTRATION_SCAN_PREFIX),
    )
    .await
    {
        Ok(Ok(rows)) => rows,
        Ok(Err(StorageError::TableNotFound(_))) => return Ok(Vec::new()),
        Ok(Err(e)) => return Err(MigrationError::store("read serving registrations", e)),
        Err(_) => {
            return Err(MigrationError::Store(format!(
                "read serving registrations: timed out after {} s",
                REQUEST_TIMEOUT.as_secs()
            )));
        }
    };
    let now_ms = now_epoch_ms();
    let mut active = Vec::new();
    for (_, val) in rows {
        let expires_at = val
            .get("expires_at_ms")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        if expires_at > now_ms {
            let cmd = val
                .get("command")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(SERVE_COMMAND);
            let pid = val
                .get("pid")
                .and_then(serde_json::Value::as_u64)
                .map_or_else(|| "unknown".to_owned(), |n| n.to_string());
            let host = val
                .get("host")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown-host");
            let release = val
                .get("release")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            active.push(format!("{cmd} (pid {pid} on {host}, release {release})"));
        }
    }
    Ok(active)
}

/// Active serving registration row (`server_registration:<token>`) held in
/// `__extenddb_catalog__` while a server serves.
pub struct ServingRegistration {
    catalog_client: Arc<BigtableClient>,
    token: String,
    renewal: JoinHandle<()>,
    lost: watch::Receiver<Option<String>>,
    released: AtomicBool,
}

impl ServingRegistration {
    /// Register a serving process with the default [`SERVE_COMMAND`] label.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError`] if catalog connection fails, the catalog is
    /// uninitialized, or a migration holds `lock:catalog_migrations`.
    pub async fn register(
        connection_string: &str,
        release: &str,
        timing: Timing,
    ) -> Result<Self, MigrationError> {
        Self::register_as(connection_string, release, SERVE_COMMAND, timing).await
    }

    /// Register a serving process under `command` and verify no migration lock
    /// is currently held.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError`] if catalog connection fails, the catalog is
    /// uninitialized, or a migration holds `lock:catalog_migrations`.
    pub async fn register_as(
        connection_string: &str,
        release: &str,
        command: &str,
        timing: Timing,
    ) -> Result<Self, MigrationError> {
        let mut cfg = BigtableStorageConfig::from_connection_string(connection_string)
            .map_err(|e| MigrationError::store("parse bigtable connection string", e))?;
        if let Some(cp) = cfg.catalog_pool_size {
            cfg.pool_size = cp;
        }
        let catalog_client = Arc::new(
            BigtableClient::connect(&cfg)
                .await
                .map_err(|e| MigrationError::store("connect to Bigtable catalog", e))?,
        );

        let token = Uuid::new_v4().to_string();
        let host = host_name();
        let pid = std::process::id();
        let now_ms = now_epoch_ms();
        let lease_ms = i64::try_from(timing.registration.as_millis()).unwrap_or(45_000);
        let doc = json!({
            "token": token,
            "command": command,
            "host": host,
            "pid": pid,
            "release": release,
            "registered_at": rfc3339_now(),
            "registered_at_ms": now_ms,
            "expires_at_ms": now_ms.saturating_add(lease_ms),
        });

        let cat = Catalog::new(&catalog_client);
        let reg_key = keys::server_registration(&token);
        let sent = Instant::now();
        match tokio::time::timeout(REQUEST_TIMEOUT, cat.put(&reg_key, &doc)).await {
            Ok(Ok(())) => {}
            Ok(Err(StorageError::TableNotFound(_))) => {
                return Err(MigrationError::Status(CatalogStatus::Uninitialized));
            }
            Ok(Err(e)) => return Err(MigrationError::store("register server in catalog", e)),
            Err(_) => {
                return Err(MigrationError::Store(format!(
                    "register server in catalog: timed out after {} s",
                    REQUEST_TIMEOUT.as_secs()
                )));
            }
        }

        let (tx, lost) = watch::channel(None);
        let renewal = spawn_registration_renewal(
            catalog_client.clone(),
            token.clone(),
            doc,
            timing,
            sent,
            tx,
        );
        let registration = Self {
            catalog_client: catalog_client.clone(),
            token,
            renewal,
            lost,
            released: AtomicBool::new(false),
        };

        let live_holder = match tokio::time::timeout(
            REQUEST_TIMEOUT,
            cat.read_live_lock_owner(MIGRATION_LOCK_NAME),
        )
        .await
        {
            Ok(Ok(holder)) => holder,
            Ok(Err(e)) => {
                registration.release().await;
                return Err(MigrationError::store("read migration lock", e));
            }
            Err(_) => {
                registration.release().await;
                return Err(MigrationError::Store(format!(
                    "read migration lock: timed out after {} s",
                    REQUEST_TIMEOUT.as_secs()
                )));
            }
        };

        if let Some(holder) = live_holder {
            registration.release().await;
            let suffix = if command == SERVE_COMMAND {
                ", so the server does not start; start it again once the migration finishes"
            } else {
                ""
            };
            return Err(MigrationError::Locked(format!(
                "{holder} holds the migration lock on this catalog{suffix}"
            )));
        }

        Ok(registration)
    }

    #[must_use]
    pub fn lost(&self) -> watch::Receiver<Option<String>> {
        self.lost.clone()
    }

    pub async fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.renewal.abort();
        let cat = Catalog::new(&self.catalog_client);
        let key = keys::server_registration(&self.token);
        let _ = tokio::time::timeout(REQUEST_TIMEOUT, cat.delete(&key)).await;
    }
}

impl Drop for ServingRegistration {
    fn drop(&mut self) {
        self.renewal.abort();
        if self.released.load(Ordering::Acquire) {
            return;
        }
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            let client = self.catalog_client.clone();
            let token = self.token.clone();
            rt.spawn(async move {
                let cat = Catalog::new(&client);
                let key = keys::server_registration(&token);
                let _ = tokio::time::timeout(REQUEST_TIMEOUT, cat.delete(&key)).await;
            });
        }
    }
}

fn spawn_registration_renewal(
    catalog_client: Arc<BigtableClient>,
    token: String,
    mut doc: serde_json::Value,
    timing: Timing,
    registered: Instant,
    lost: watch::Sender<Option<String>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let key = keys::server_registration(&token);
        let lease_ms = i64::try_from(timing.registration.as_millis()).unwrap_or(45_000);
        let mut renewed = registered;
        let mut wait = timing.registration_renew_every;
        let left = |renewed: Instant| {
            timing
                .registration_deadline
                .saturating_sub(renewed.elapsed())
        };
        loop {
            tokio::time::sleep(wait.min(left(renewed))).await;
            let sent = Instant::now();
            doc["expires_at_ms"] = json!(now_epoch_ms().saturating_add(lease_ms));
            let bound = REQUEST_TIMEOUT
                .min(timing.registration_renew_every)
                .min(left(renewed));
            let cat = Catalog::new(&catalog_client);
            let answer = tokio::time::timeout(bound, cat.put_if_present(&key, &doc)).await;
            let reason = match answer {
                Ok(Ok(true)) => {
                    renewed = sent;
                    wait = timing.registration_renew_every;
                    continue;
                }
                Ok(Ok(false)) => {
                    format!("this server's registration ({key}) in {CATALOG_TABLE} was deleted")
                }
                Ok(Err(_)) | Err(_) if renewed.elapsed() < timing.registration_deadline => {
                    wait = timing.registration_retry_every;
                    continue;
                }
                Ok(Err(e)) => format!(
                    "this server could not renew its registration for {} s ({e})",
                    renewed.elapsed().as_secs()
                ),
                Err(_) => format!(
                    "this server could not renew its registration for {} s (timed out)",
                    renewed.elapsed().as_secs()
                ),
            };
            let _ = lost.send(Some(reason));
            return;
        }
    })
}

/// Server-lifetime [`ServingRegistrar`] handle capable of detecting lost
/// registrations and re-registering if no migration intervened.
pub struct ServerRegistration {
    connection_string: String,
    release: String,
    command: &'static str,
    timing: Timing,
    started: CatalogStatus,
    current: Mutex<Arc<ServingRegistration>>,
}

impl ServerRegistration {
    #[must_use]
    pub fn started(&self) -> &CatalogStatus {
        &self.started
    }

    #[must_use]
    pub fn current(&self) -> Arc<ServingRegistration> {
        self.current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl ServingRegistrar for ServerRegistration {
    async fn lost(&self) -> String {
        let mut lost = self.current().lost();
        loop {
            let reason = lost.borrow_and_update().clone();
            if let Some(reason) = reason {
                return reason;
            }
            if lost.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    async fn register_again(&self) -> Reregistration {
        let registration = match ServingRegistration::register_as(
            &self.connection_string,
            &self.release,
            self.command,
            self.timing,
        )
        .await
        {
            Ok(reg) => reg,
            Err(MigrationError::Locked(why)) => return Reregistration::Refused(why),
            Err(e) => return Reregistration::Unavailable(e.to_string()),
        };

        let status = match catalog_status(&self.connection_string).await {
            Ok(s) => s,
            Err(e) => {
                registration.release().await;
                return Reregistration::Unavailable(format!(
                    "the catalog's status could not be read: {e}"
                ));
            }
        };

        if status != self.started {
            registration.release().await;
            return Reregistration::Refused(format!(
                "the catalog's status changed while the server held no registration (started \
                 with: {}; now: {status}); restart the server to serve the updated catalog",
                self.started
            ));
        }

        *self.current.lock().unwrap_or_else(PoisonError::into_inner) = Arc::new(registration);
        Reregistration::Registered
    }

    async fn release(&self) {
        self.current().release().await;
    }
}

/// Read-only catalog status check across both `StoreKind::Catalog` and
/// `StoreKind::Data`.
///
/// # Errors
///
/// Returns [`MigrationError`] if connecting to Bigtable or reading ledgers fails.
pub async fn catalog_status(connection_string: &str) -> Result<CatalogStatus, MigrationError> {
    let target = Target::at(connection_string);
    let backend = BigtableMigrationRunnerBackend::connect(&target, None).await?;
    backend.resolve_status("").await
}

/// Register as a serving process, verify that no migration lock is held, and
/// verify that `Runner::status` is `Current` or `AheadCompatible`.
///
/// # Errors
///
/// Returns [`MigrationError`] if the migration lock is held, the catalog status
/// forbids serving, or a Bigtable request fails.
pub async fn register_and_check(
    connection_string: &str,
    release: &str,
    timing: Timing,
) -> Result<ServerRegistration, MigrationError> {
    register_and_check_as(connection_string, release, SERVE_COMMAND, timing).await
}

/// [`register_and_check`] for the process named by `command`.
///
/// # Errors
///
/// Returns [`MigrationError`] if the migration lock is held, the catalog status
/// forbids serving, or a Bigtable request fails.
pub async fn register_and_check_as(
    connection_string: &str,
    release: &str,
    command: &'static str,
    timing: Timing,
) -> Result<ServerRegistration, MigrationError> {
    let registration =
        ServingRegistration::register_as(connection_string, release, command, timing).await?;
    let status = match catalog_status(connection_string).await {
        Ok(s) => s,
        Err(e) => {
            registration.release().await;
            return Err(e);
        }
    };
    match status {
        CatalogStatus::Current { .. } => {}
        CatalogStatus::AheadCompatible { .. } => {
            tracing::warn!("{status}; serving it because all unknown migrations are additive");
        }
        other => {
            registration.release().await;
            return Err(MigrationError::Status(other));
        }
    }
    Ok(ServerRegistration {
        connection_string: connection_string.to_owned(),
        release: release.to_owned(),
        command,
        timing,
        started: status,
        current: Mutex::new(Arc::new(registration)),
    })
}

pub struct BigtableCatalogMigrations;

/// Store handles passed to Bigtable code migrations.
pub struct BigtableMigrationStores {
    pub catalog_client: Arc<BigtableClient>,
    pub data_client: Arc<BigtableClient>,
    lease: Option<Arc<MigrationLease>>,
}

impl BigtableMigrationStores {
    /// Verify the active migration lease is still held within its safety window.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError::Store`] if the held lease was lost or expired.
    pub fn check_lease(&self) -> Result<(), MigrationError> {
        if let Some(ref lease) = self.lease {
            lease.check()?;
        }
        Ok(())
    }

    pub fn progressed(&self) {
        if let Some(ref lease) = self.lease {
            lease.progressed();
        }
    }
}

/// Bigtable's entry for every [`MigrationId`] in the workspace registry.
#[must_use]
pub fn migration(id: MigrationId) -> Migration<BigtableMigrationStores> {
    match id {
        MigrationId::Baseline => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![
                StoreBody::code(
                    StoreKind::Catalog,
                    &M0001CatalogBaseline,
                    include_str!("m0001_baseline.rs"),
                ),
                StoreBody::code(
                    StoreKind::Data,
                    &M0001DataBaseline,
                    include_str!("m0001_baseline.rs"),
                ),
            ],
        },
        MigrationId::ResourceTagConditionKeys => Migration::NotApplicable(
            "the Bigtable backend did not exist when dynamodb:ResourceTag was accepted",
        ),
        MigrationId::SortKeyDefinitions => Migration::NotApplicable(
            "the Bigtable backend did not exist before the #259 UpdateTable fix",
        ),
        MigrationId::IndexTableBaseKey => {
            Migration::NotApplicable("Bigtable GSI shadow row keys always embed the base key")
        }
        MigrationId::DropContinuousBackups => {
            Migration::NotApplicable("the Bigtable backend never created continuous_backups")
        }
        MigrationId::BackupCatalogIndex => Migration::NotApplicable(
            "logical backups and restores are not supported on the Bigtable backend",
        ),
        MigrationId::StreamPending => {
            Migration::NotApplicable("stream_pending is specific to PostgreSQL")
        }
        MigrationId::RestoreProgress => {
            Migration::NotApplicable("restore_progress is specific to PostgreSQL")
        }
        MigrationId::WarmThroughput => Migration::NotApplicable(
            "Bigtable stores table and GSI descriptions as schemaless JSON documents",
        ),
        MigrationId::AccountMetrics => {
            Migration::NotApplicable("Bigtable metrics rows are keyed by account from baseline")
        }
        MigrationId::AttributeNameEscape => Migration::NotApplicable(
            "Bigtable serializes item attributes as raw JSON bytes rather than PostgreSQL jsonb",
        ),
        MigrationId::UniqueAccessKeyIds => Migration::NotApplicable(
            "Bigtable keys access_key:<id> rows directly by the access key ID",
        ),
    }
}

pub(crate) const RELEASED: &[Released] = &[
    Released {
        id: MigrationId::Baseline,
        store: StoreKind::Catalog,
        checksum: "2ba54650d2b66866667b135bc279c265c0bef7f450d538bc3c71ff06a79f83dc",
        fingerprint: Some("0001_baseline/bigtable-catalog-v1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::Baseline,
        store: StoreKind::Data,
        checksum: "2ba54650d2b66866667b135bc279c265c0bef7f450d538bc3c71ff06a79f83dc",
        fingerprint: Some("0001_baseline/bigtable-data-v1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::ResourceTagConditionKeys,
        store: StoreKind::Catalog,
        checksum: "6005df4c45ba91a78216e20daea4c083fdb4e4626cec340fac63d6398a9a7b1b",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::SortKeyDefinitions,
        store: StoreKind::Catalog,
        checksum: "4c1a7f957a15e18ce8b67d5e315dd8842ce2c130321767ed6a74d7c3bd2be8ff",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::IndexTableBaseKey,
        store: StoreKind::Catalog,
        checksum: "9360520018e48ab95430c4e4860e190d02bd2b1d4eaa7fee3c25dd5799485710",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::DropContinuousBackups,
        store: StoreKind::Catalog,
        checksum: "bb58e89ff037a5b8d62046f62324a77a55c77a1f5ea4ca388503e078dd91306d",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::BackupCatalogIndex,
        store: StoreKind::Catalog,
        checksum: "40f635d7ae8f4b84d699d8e1570dbbc500215f994b0650b10301b5d843e32d65",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::StreamPending,
        store: StoreKind::Catalog,
        checksum: "c52d4a33a9de8c2cd067ade20403ac84bdcfb6ed2f5b941c9883815dd74e530b",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::RestoreProgress,
        store: StoreKind::Catalog,
        checksum: "27863f1f409b47b66bc466b8370ac3a89b4c29c127aea3e64f74d632b51b46f3",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::WarmThroughput,
        store: StoreKind::Catalog,
        checksum: "113674d9f30006a2b205ab9b743cfa4d796bcff710e57bf412c1c6386679f6ff",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::AccountMetrics,
        store: StoreKind::Catalog,
        checksum: "0725f80ca491bf7569c9c6dc1a97ed3fa53bac4776d93b7ecfcace13a3e313a1",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::AttributeNameEscape,
        store: StoreKind::Catalog,
        checksum: "b17aff6973f9fc66b763d6f6f2d7b44b49bdb4e38e437ed0afddc4b25f64a34c",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::UniqueAccessKeyIds,
        store: StoreKind::Catalog,
        checksum: "5f398633feda577647c16b44461219db4d1d3b2d68c4a25ba7fbf015d182ecc4",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
];

async fn connect_catalog_and_data(
    cfg: &BigtableStorageConfig,
) -> Result<(Arc<BigtableClient>, Arc<BigtableClient>, String), MigrationError> {
    let mut cat_cfg = cfg.clone();
    if let Some(cp) = cfg.catalog_pool_size {
        cat_cfg.pool_size = cp;
    }
    let catalog_client = Arc::new(
        BigtableClient::connect(&cat_cfg)
            .await
            .map_err(|e| MigrationError::store("connect to Bigtable catalog instance", e))?,
    );
    let data_instance_id = cfg.effective_data_instance_id().to_owned();
    let data_client = if data_instance_id == cfg.instance_id {
        catalog_client.clone()
    } else {
        let mut data_cfg = cfg.clone();
        data_cfg.instance_id.clone_from(&data_instance_id);
        Arc::new(
            BigtableClient::connect(&data_cfg)
                .await
                .map_err(|e| MigrationError::store("connect to Bigtable data instance", e))?,
        )
    };
    Ok((catalog_client, data_client, data_instance_id))
}

struct BigtableMigrationRunnerBackend {
    stores: BigtableMigrationStores,
    data_instance_id: String,
}

impl BigtableMigrationRunnerBackend {
    async fn connect(
        target: &Target,
        lease: Option<Arc<MigrationLease>>,
    ) -> Result<Self, MigrationError> {
        let cfg = BigtableStorageConfig::from_connection_string(&target.location)
            .map_err(|e| MigrationError::store("parse bigtable connection string", e))?;
        let (catalog_client, data_client, data_instance_id) =
            connect_catalog_and_data(&cfg).await?;
        Ok(Self {
            stores: BigtableMigrationStores {
                catalog_client,
                data_client,
                lease,
            },
            data_instance_id,
        })
    }

    fn ledger_catalog(&self, kind: StoreKind) -> Catalog<'_> {
        match kind {
            StoreKind::Catalog => Catalog::new(&self.stores.catalog_client),
            StoreKind::Data | StoreKind::Account => {
                Catalog::with_table(&self.stores.data_client, DATA_LEDGER_TABLE)
            }
        }
    }

    /// Inspect `__extenddb_catalog__` for non-ephemeral rows when neither
    /// `migration_ledger:catalog:*` nor `version:catalog` / `setting:catalog_version`
    /// is present, distinguishing an empty catalog from a damaged or foreign
    /// `__extenddb_catalog__` table across any column family or qualifier.
    async fn unexpected_uninitialized_catalog_rows(&self) -> Result<Vec<String>, MigrationError> {
        self.stores.check_lease()?;
        let cat = Catalog::new(&self.stores.catalog_client);
        let row_keys = match cat.scan_any_row_keys(16).await {
            Ok(keys) => keys,
            Err(StorageError::TableNotFound(_)) => {
                self.stores.progressed();
                return Ok(Vec::new());
            }
            Err(e) => return Err(MigrationError::store("scan __extenddb_catalog__ rows", e)),
        };
        self.stores.progressed();
        let lock_key = format!("lock:{MIGRATION_LOCK_NAME}");
        let catalog_id_key = keys::setting("catalog_id");
        let unexpected: Vec<String> = row_keys
            .into_iter()
            .filter(|k| {
                k != &lock_key
                    && k != &catalog_id_key
                    && !k.starts_with(keys::SERVER_REGISTRATION_SCAN_PREFIX)
            })
            .collect();
        Ok(unexpected)
    }

    async fn resolve_status(&self, applied_by: &str) -> Result<CatalogStatus, MigrationError> {
        let status = Runner::new(self, applied_by).status().await?;
        if status == CatalogStatus::Uninitialized {
            let unexpected = self.unexpected_uninitialized_catalog_rows().await?;
            if !unexpected.is_empty() {
                return Ok(CatalogStatus::NeedsAttention {
                    items: vec![format!(
                        "{CATALOG_TABLE} is not an ExtendDB catalog or is missing its migration ledger/catalog_version records (holds: {})",
                        unexpected.join(", ")
                    )],
                });
            }
        }
        Ok(status)
    }
}

fn value_to_ledger_row(store: &str, v: &serde_json::Value) -> Result<LedgerRow, MigrationError> {
    let id = v
        .get("id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| MigrationError::Store(format!("missing id in {store} ledger row")))?;
    let str_field = |k: &str| {
        v.get(k)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned()
    };
    LedgerRow::from_columns(
        Some(store),
        (
            id,
            str_field("name"),
            str_field("kind"),
            str_field("checksum"),
            str_field("outcome"),
            str_field("notes"),
            str_field("applied_at"),
            str_field("applied_by"),
        ),
    )
}

#[async_trait]
impl MigrationBackend for BigtableMigrationRunnerBackend {
    type Stores = BigtableMigrationStores;

    fn backend_name(&self) -> &'static str {
        crate::BACKEND_NAME
    }

    fn migration(&self, id: MigrationId) -> Migration<Self::Stores> {
        migration(id)
    }

    fn released(&self) -> &'static [Released] {
        RELEASED
    }

    fn stores(&self) -> &Self::Stores {
        &self.stores
    }

    async fn store_names(&self, kind: StoreKind) -> Result<Vec<String>, MigrationError> {
        match kind {
            StoreKind::Catalog => Ok(vec![CATALOG_TABLE.to_owned()]),
            StoreKind::Data => Ok(vec![format!(
                "{}:{DATA_LEDGER_TABLE}",
                self.data_instance_id
            )]),
            StoreKind::Account => Ok(Vec::new()),
        }
    }

    async fn read_ledger(&self, store: &StoreRef) -> Result<Vec<LedgerRow>, MigrationError> {
        self.stores.check_lease()?;
        let cat = self.ledger_catalog(store.kind);
        let prefix = keys::migration_ledger_prefix_for_store(store.kind.as_str());
        let rows = match cat.scan_prefix(&prefix).await {
            Ok(rows) => rows,
            Err(StorageError::TableNotFound(_)) => {
                self.stores.progressed();
                return Ok(Vec::new());
            }
            Err(e) => return Err(MigrationError::store("read migration ledger", e)),
        };
        self.stores.progressed();
        let mut parsed = Vec::with_capacity(rows.len());
        for (_, v) in rows {
            parsed.push(value_to_ledger_row(&store.name, &v)?);
        }
        parsed.sort_by_key(|r| r.id);
        Ok(parsed)
    }

    async fn read_traces(&self) -> Result<CatalogTraces, MigrationError> {
        self.stores.check_lease()?;
        let cat = Catalog::new(&self.stores.catalog_client);
        let row = match cat.get(keys::VERSION).await {
            Ok(row) => row,
            Err(StorageError::TableNotFound(_)) => {
                self.stores.progressed();
                return Ok(CatalogTraces::default());
            }
            Err(e) => return Err(MigrationError::store("read catalog version", e)),
        };
        self.stores.progressed();
        let mut catalog_version = row.and_then(|v| {
            v.get("catalog_version")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
        if catalog_version.is_none() {
            self.stores.check_lease()?;
            let setting_row = cat
                .get(&keys::setting("catalog_version"))
                .await
                .map_err(|e| MigrationError::store("read setting:catalog_version", e))?;
            self.stores.progressed();
            catalog_version = setting_row.and_then(|v| {
                v.get("value")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
        }
        self.stores.check_lease()?;
        let marker_row = cat
            .get(&keys::setting("resource_tag_migration"))
            .await
            .map_err(|e| MigrationError::store("read setting:resource_tag_migration", e))?;
        self.stores.progressed();
        Ok(CatalogTraces {
            catalog_version,
            resource_tag_marker: marker_row.is_some(),
        })
    }

    async fn catalog_is_empty(&self) -> Result<bool, MigrationError> {
        Ok(self
            .unexpected_uninitialized_catalog_rows()
            .await?
            .is_empty())
    }

    async fn before_first_write(
        &self,
        _from: Option<u32>,
        _to: u32,
    ) -> Result<Vec<String>, MigrationError> {
        self.stores.check_lease()?;
        Ok(Vec::new())
    }

    async fn apply(
        &self,
        store: &StoreRef,
        _id: MigrationId,
        step: Step<'_, Self::Stores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, MigrationError> {
        self.stores.check_lease()?;
        let done = match step {
            Step::File { name, .. } => {
                return Err(MigrationError::Store(format!(
                    "Bigtable backend does not execute SQL/CQL file body {name}"
                )));
            }
            Step::Code(code) => code
                .run(&self.stores)
                .await
                .map_err(|e| self.stores.check_lease().err().unwrap_or(e))?,
            Step::Record => Done::default(),
        };

        let now_str = rfc3339_now();
        if let Some(mut ledger_row) = row {
            self.stores.check_lease()?;
            if !done.notes.is_empty() {
                ledger_row.notes = done.notes.join("\n");
            }
            let cat = self.ledger_catalog(store.kind);
            let key = keys::migration_ledger_row(store.kind.as_str(), ledger_row.id);
            cat.put(
                &key,
                &json!({
                    "id": ledger_row.id,
                    "name": ledger_row.name,
                    "kind": ledger_row.kind.as_str(),
                    "checksum": ledger_row.checksum,
                    "outcome": ledger_row.outcome.as_str(),
                    "notes": ledger_row.notes,
                    "applied_at": now_str,
                    "applied_by": ledger_row.applied_by,
                }),
            )
            .await
            .map_err(|e| MigrationError::store("write migration ledger row", e))?;
            self.stores.progressed();
        }

        if let Some(ver) = catalog_version {
            self.write_catalog_version(ver).await?;
        }

        Ok(done)
    }

    async fn write_catalog_version(&self, version: u32) -> Result<(), MigrationError> {
        self.stores.check_lease()?;
        let cat = Catalog::new(&self.stores.catalog_client);
        let ver_str = version.to_string();
        let applied_at = rfc3339_now();
        cat.put(
            keys::VERSION,
            &json!({
                "catalog_version": ver_str,
                "applied_at": applied_at,
            }),
        )
        .await
        .map_err(|e| MigrationError::store("write catalog_version", e))?;
        self.stores.progressed();

        self.stores.check_lease()?;
        cat.put(
            &keys::setting("catalog_version"),
            &json!({
                "key": "catalog_version",
                "value": ver_str,
            }),
        )
        .await
        .map_err(|e| MigrationError::store("write setting:catalog_version", e))?;
        self.stores.progressed();
        Ok(())
    }

    async fn delete_ledger_row(&self, store: &StoreRef, id: u32) -> Result<(), MigrationError> {
        self.stores.check_lease()?;
        let cat = self.ledger_catalog(store.kind);
        let key = keys::migration_ledger_row(store.kind.as_str(), id);
        cat.delete(&key)
            .await
            .map_err(|e| MigrationError::store("delete migration ledger row", e))?;
        self.stores.progressed();
        Ok(())
    }
}

struct HeldMigrationLock {
    lock: MigrationLock,
    catalog_client: Arc<BigtableClient>,
    data_client: Arc<BigtableClient>,
    data_instance_id: String,
}

fn held(lock: &HeldLock) -> Result<&HeldMigrationLock, MigrationError> {
    lock.get::<HeldMigrationLock>()
        .ok_or_else(|| MigrationError::Refused("the migration lock is not a Bigtable lock".into()))
}

/// Extract the [`MigrationLease`] held inside a [`HeldLock`], if taken by the
/// Bigtable backend.
#[must_use]
pub(crate) fn lease_of(lock: &HeldLock) -> Option<Arc<MigrationLease>> {
    held(lock).ok().map(|h| h.lock.lease())
}

struct LeasedBigtableStores {
    inner: BigtableCatalogStore,
    lease: Arc<MigrationLease>,
}

impl LeasedBigtableStores {
    fn check(&self) -> OpResult<()> {
        self.lease
            .check()
            .map_err(|e| OpError::Internal(e.to_string()))
    }
}

fn leased_no_answer(what: &str) -> OpError {
    OpError::Internal(format!(
        "{what}: no answer within {} s",
        REQUEST_TIMEOUT.as_secs()
    ))
}

macro_rules! checked_leased_store {
    ($( fn $name:ident(&self $(, $arg:ident: $ty:ty)* $(,)?) -> $ret:ty; )+) => {
        $(
            fn $name(&self $(, $arg: $ty)*) -> $ret {
                if let Err(e) = self.check() {
                    return Box::pin(async move { Err(e) });
                }
                let call = self.inner.$name($($arg),*);
                Box::pin(async move {
                    let answer = tokio::time::timeout(REQUEST_TIMEOUT, call).await;
                    self.lease.progressed();
                    answer.unwrap_or_else(|_| Err(leased_no_answer(stringify!($name))))
                })
            }
        )+
    };
}

impl ManagementStore for LeasedBigtableStores {
    checked_leased_store! {
        fn create_account(&self, account_id: &str, account_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn delete_account(&self, account_id: &str) -> BoxFuture<'_, OpResult<()>>;
        fn default_account_id(&self) -> BoxFuture<'_, OpResult<Option<String>>>;
        fn list_all_accounts(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
        fn list_all_accounts_full(&self) -> BoxFuture<'_, OpResult<Vec<(String, String, time::OffsetDateTime)>>>;
        fn list_accounts_for(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
        fn get_account_detail(&self, account_id: &str) -> BoxFuture<'_, OpResult<Option<AccountDetail>>>;
        fn dashboard_counts(&self) -> BoxFuture<'_, OpResult<(i64, i64)>>;
        fn create_user(&self, account_id: &str, user_name: &str, password_hash: Option<&str>) -> BoxFuture<'_, OpResult<()>>;
        fn delete_user(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn list_users(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<UserListEntry>>>;
        fn get_user_detail(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<Option<UserDetail>>>;
        fn verify_iam_user_password(&self, account_id: &str, user_name: &str, password: &str) -> BoxFuture<'_, OpResult<bool>>;
        fn change_user_password(&self, account_id: &str, user_name: &str, password_hash: &str) -> BoxFuture<'_, OpResult<()>>;
        fn tag_user(&self, account_id: &str, user_name: &str, tags: &[(String, String)]) -> BoxFuture<'_, OpResult<()>>;
        fn untag_user(&self, account_id: &str, user_name: &str, tag_keys: &[String]) -> BoxFuture<'_, OpResult<()>>;
        fn list_user_tags(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
        fn create_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn delete_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn list_groups(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<GroupListEntry>>>;
        fn get_group_detail(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<Option<GroupDetail>>>;
        fn add_group_member(&self, account_id: &str, group_name: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn remove_group_member(&self, account_id: &str, group_name: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn create_role(&self, account_id: &str, role_name: &str, trust_policy: &serde_json::Value) -> BoxFuture<'_, OpResult<()>>;
        fn delete_role(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn list_roles(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<RoleListEntry>>>;
        fn get_role_detail(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<Option<RoleDetail>>>;
        fn get_role_trust_policy(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>>;
        fn tag_role(&self, account_id: &str, role_name: &str, tags: &[(String, String)]) -> BoxFuture<'_, OpResult<()>>;
        fn untag_role(&self, account_id: &str, role_name: &str, tag_keys: &[String]) -> BoxFuture<'_, OpResult<()>>;
        fn list_role_tags(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
        fn put_policy(&self, account_id: &str, principal_type: &str, principal_name: &str, policy_name: &str, document: &serde_json::Value) -> BoxFuture<'_, OpResult<()>>;
        fn delete_policy(&self, account_id: &str, principal_type: &str, principal_name: &str, policy_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn list_policies(&self, account_id: &str, principal_type: &str, principal_name: &str) -> BoxFuture<'_, OpResult<Vec<(String, serde_json::Value, time::OffsetDateTime)>>>;
        fn list_policy_documents(&self, account_id: &str, principal_type: &str, principal_name: &str) -> BoxFuture<'_, OpResult<Vec<(String, StoredDocument)>>>;
        fn set_user_boundary(&self, account_id: &str, user_name: &str, document: &serde_json::Value) -> BoxFuture<'_, OpResult<()>>;
        fn get_user_boundary(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>>;
        fn delete_user_boundary(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn set_role_boundary(&self, account_id: &str, role_name: &str, document: &serde_json::Value) -> BoxFuture<'_, OpResult<()>>;
        fn get_role_boundary(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>>;
        fn boundary_document(&self, account_id: &str, principal_type: &str, principal_name: &str) -> BoxFuture<'_, OpResult<Option<StoredDocument>>>;
        fn delete_role_boundary(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn create_access_key(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<AccessKeyCreated>>;
        fn delete_access_key(&self, account_id: &str, user_name: &str, key_id: &str) -> BoxFuture<'_, OpResult<()>>;
        fn list_access_keys(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<Vec<(String, bool, time::OffsetDateTime)>>>;
        fn import_access_key(&self, account_id: &str, user_name: &str, access_key_id: &str, secret_access_key: &str) -> BoxFuture<'_, OpResult<()>>;
        fn store_session(&self, session_token: &str, access_key_id: &str, secret_key_encrypted: &[u8], account_id: &str, role_name: &str, session_name: &str, session_tags: &Option<serde_json::Value>, session_policy: &Option<serde_json::Value>, expires_at: time::OffsetDateTime) -> BoxFuture<'_, OpResult<()>>;
        fn list_session_policies(&self) -> BoxFuture<'_, OpResult<Vec<SessionPolicyEntry>>>;
        fn update_session_policy(&self, session_token: &str, document: &serde_json::Value) -> BoxFuture<'_, OpResult<()>>;
        fn fetch_caller_tags(&self, account_id: &str, resource: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
        fn read_secret_page(&self, table: SecretTable, page: Option<&str>, page_size: usize) -> BoxFuture<'_, OpResult<SecretPage>>;
        fn write_secrets(&self, table: SecretTable, rows: &[SecretWrite]) -> BoxFuture<'_, OpResult<()>>;
        fn delete_secrets(&self, table: SecretTable, keys: &[String]) -> BoxFuture<'_, OpResult<()>>;
        fn delete_setting(&self, key: &str) -> BoxFuture<'_, OpResult<()>>;
    }
}

impl SettingsStore for LeasedBigtableStores {
    checked_leased_store! {
        fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>>;
        fn set_setting(&self, key: &str, value: &str) -> BoxFuture<'_, OpResult<()>>;
        fn replace_setting(&self, key: &str, expected: &str, value: &str) -> BoxFuture<'_, OpResult<bool>>;
        fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
    }
}

impl LockedStores for LeasedBigtableStores {
    fn management(&self) -> &dyn ManagementStore {
        self
    }

    fn settings(&self) -> &dyn SettingsStore {
        self
    }
}

#[async_trait]
impl CatalogMigrations for BigtableCatalogMigrations {
    fn listed(&self, id: MigrationId) -> Listed {
        Listed::of(&migration(id))
    }

    fn backup_advice(&self, target: &Target) -> String {
        match BigtableStorageConfig::from_connection_string(&target.location) {
            Ok(cfg) => {
                let data_inst = cfg.effective_data_instance_id();
                if data_inst == cfg.instance_id {
                    format!(
                        "back up Cloud Bigtable tables '{CATALOG_TABLE}' and '{DATA_LEDGER_TABLE}' \
                         in projects/{}/instances/{}",
                        cfg.project_id, cfg.instance_id
                    )
                } else {
                    format!(
                        "back up Cloud Bigtable catalog table '{CATALOG_TABLE}' in \
                         projects/{}/instances/{} and data ledger table '{DATA_LEDGER_TABLE}' in \
                         projects/{}/instances/{data_inst}",
                        cfg.project_id, cfg.instance_id, cfg.project_id
                    )
                }
            }
            Err(_) => {
                format!("back up Cloud Bigtable tables '{CATALOG_TABLE}' and '{DATA_LEDGER_TABLE}'")
            }
        }
    }

    async fn status(&self, target: &Target) -> Result<CatalogStatus, MigrationError> {
        let backend = BigtableMigrationRunnerBackend::connect(target, None).await?;
        backend.resolve_status(env!("CARGO_PKG_VERSION")).await
    }

    async fn status_under_lock(
        &self,
        lock: &HeldLock,
        _target: &Target,
    ) -> Result<CatalogStatus, MigrationError> {
        let h = held(lock)?;
        let backend = BigtableMigrationRunnerBackend {
            stores: BigtableMigrationStores {
                catalog_client: h.catalog_client.clone(),
                data_client: h.data_client.clone(),
                lease: Some(h.lock.lease()),
            },
            data_instance_id: h.data_instance_id.clone(),
        };
        backend.resolve_status(env!("CARGO_PKG_VERSION")).await
    }

    async fn catalog_ledger(&self, target: &Target) -> Result<Vec<LedgerRow>, MigrationError> {
        let backend = BigtableMigrationRunnerBackend::connect(target, None).await?;
        Runner::new(&backend, "").catalog_ledger().await
    }

    async fn lock(&self, target: &Target, timeout: Duration) -> Result<HeldLock, MigrationError> {
        self.lock_with_timing(target, timeout, Timing::PRODUCTION)
            .await
    }

    async fn run(
        &self,
        lock: &HeldLock,
        release: &str,
        kind: RunKind,
    ) -> Result<RunReport, MigrationError> {
        let h = held(lock)?;
        let backend = BigtableMigrationRunnerBackend {
            stores: BigtableMigrationStores {
                catalog_client: h.catalog_client.clone(),
                data_client: h.data_client.clone(),
                lease: Some(h.lock.lease()),
            },
            data_instance_id: h.data_instance_id.clone(),
        };
        if kind != RunKind::Reconverge
            && let status @ CatalogStatus::NeedsAttention { .. } =
                backend.resolve_status(release).await?
        {
            return Err(MigrationError::Status(status));
        }
        let runner = Runner::new(&backend, release);
        match kind {
            RunKind::Create | RunKind::Migrate => runner.migrate().await,
            RunKind::Reconverge => runner.reconverge().await,
        }
    }

    async fn forget(&self, lock: &HeldLock, id: u32) -> Result<(), MigrationError> {
        let h = held(lock)?;
        let backend = BigtableMigrationRunnerBackend {
            stores: BigtableMigrationStores {
                catalog_client: h.catalog_client.clone(),
                data_client: h.data_client.clone(),
                lease: Some(h.lock.lease()),
            },
            data_instance_id: h.data_instance_id.clone(),
        };
        Runner::new(&backend, env!("CARGO_PKG_VERSION"))
            .forget(id)
            .await
    }

    async fn release(&self, lock: HeldLock) {
        if let Ok(h) = held(&lock) {
            h.lock.release().await;
        }
    }

    async fn locked_stores(
        &self,
        lock: &HeldLock,
        _data_connection: Option<&str>,
    ) -> Result<Box<dyn LockedStores>, MigrationError> {
        let h = held(lock)?;
        Ok(Box::new(LeasedBigtableStores {
            inner: BigtableCatalogStore::new(h.catalog_client.clone(), None),
            lease: h.lock.lease(),
        }))
    }
}

impl BigtableCatalogMigrations {
    /// Acquire the catalog migration lock with explicit [`Timing`] (exposed for
    /// fast lease-renewal and server-fence tests).
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError`] if the catalog is uninitialized (when
    /// `!creating`), holds unexpected foreign/damaged rows without ledger
    /// records (when `creating`), or lock acquisition / server drain fails.
    pub async fn lock_with_timing(
        &self,
        target: &Target,
        timeout: Duration,
        timing: Timing,
    ) -> Result<HeldLock, MigrationError> {
        let cfg = BigtableStorageConfig::from_connection_string(&target.location)
            .map_err(|e| MigrationError::store("parse bigtable connection string", e))?;
        let (catalog_client, data_client, data_instance_id) =
            connect_catalog_and_data(&cfg).await?;
        let creating = target.data_connection.is_some();

        let probe = BigtableMigrationRunnerBackend {
            stores: BigtableMigrationStores {
                catalog_client: catalog_client.clone(),
                data_client: data_client.clone(),
                lease: None,
            },
            data_instance_id: data_instance_id.clone(),
        };

        if creating {
            if !Runner::new(&probe, "").initialized().await? {
                let unexpected = probe.unexpected_uninitialized_catalog_rows().await?;
                if !unexpected.is_empty() {
                    return Err(MigrationError::Refused(format!(
                        "the table {CATALOG_TABLE} is not an ExtendDB catalog: it holds {}. \
                         Nothing was written to it. Run init against a Bigtable instance where \
                         {CATALOG_TABLE} is empty or absent",
                        unexpected.join(", ")
                    )));
                }
            }
            // `init`: ensure `__extenddb_catalog__` exists before taking the
            // lock so a fresh catalog always writes and renews
            // `lock:catalog_migrations` prior to running Migration 1.
            let mut admin = AdminClient::connect(&catalog_client)
                .await
                .map_err(|e| MigrationError::store("connect Bigtable catalog admin client", e))?;
            admin
                .create_table(CATALOG_TABLE, &[(CF, Some(gc_max_versions(1)))])
                .await
                .map_err(|e| MigrationError::store("ensure __extenddb_catalog__ for lock", e))?;
        } else if !Runner::new(&probe, "").initialized().await? {
            let unexpected = probe.unexpected_uninitialized_catalog_rows().await?;
            if !unexpected.is_empty() {
                return Err(MigrationError::Status(CatalogStatus::NeedsAttention {
                    items: vec![format!(
                        "{CATALOG_TABLE} is not an ExtendDB catalog or is missing its migration ledger/catalog_version records (holds: {})",
                        unexpected.join(", ")
                    )],
                }));
            }
            return Err(MigrationError::Status(CatalogStatus::Uninitialized));
        }

        let started = Instant::now();
        let lock = MigrationLock::acquire(catalog_client.clone(), timeout, timing).await?;
        let remaining = timeout.saturating_sub(started.elapsed());
        if let Err(e) = lock.wait_for_servers(remaining).await {
            lock.release().await;
            return Err(e);
        }

        Ok(HeldLock::new(
            target.location.clone(),
            HeldMigrationLock {
                lock,
                catalog_client,
                data_client,
                data_instance_id,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_storage::migrations::RELEASED_THROUGH;

    #[test]
    fn migration_table_covers_all_ids_and_refuses_additive() {
        extenddb_storage::migrations::refuse_additive(crate::BACKEND_NAME, migration)
            .expect("no additive migrations without previous-release CI");
        if let Err(problems) = extenddb_storage::migrations::check_released(
            crate::BACKEND_NAME,
            migration,
            RELEASED,
            RELEASED_THROUGH,
        ) {
            panic!("{problems}");
        }
        for id in MigrationId::ALL {
            let _ = BigtableCatalogMigrations.listed(id);
        }
    }

    #[test]
    fn backup_advice_mentions_separate_data_instance_when_configured() {
        let single = Target::at("bigtable://proj/inst");
        let single_advice = BigtableCatalogMigrations.backup_advice(&single);
        assert!(single_advice.contains(CATALOG_TABLE));
        assert!(single_advice.contains(DATA_LEDGER_TABLE));

        let split = Target::at("bigtable://proj/cat-inst?data_instance_id=data-inst");
        let split_advice = BigtableCatalogMigrations.backup_advice(&split);
        assert!(split_advice.contains("instances/cat-inst"));
        assert!(split_advice.contains("instances/data-inst"));
    }
}
