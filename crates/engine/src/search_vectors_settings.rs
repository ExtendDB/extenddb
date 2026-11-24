// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `search_inequality_filters` setting, as each server reads it.
//!
//! The server installs its settings store against its storage engine with
//! [`install_search_filter_settings`], so two servers in one process read
//! their own catalogs. An absent or empty value enables every account, as
//! does a storage engine without an installed store. A malformed value is
//! logged once per change and enables every account.
//!
//! A read is kept for [`REFRESH`], so a busy server reads the catalog about
//! once per interval rather than once per request, and a change made through
//! the management API applies at once through
//! [`search_filter_settings_changed`]. A read that fails or outlasts
//! [`READ_TIMEOUT`] answers `ServiceUnavailableException`, as other transient
//! storage faults do; it never changes which grammar an account gets.
//!
//! A server sends at most one read of the setting at a time. A read that
//! outlasts [`READ_TIMEOUT`] keeps running in its own task, and the next
//! request waits on that read rather than sending another, so a catalog
//! that does not answer holds at most one of its connections for the
//! setting while requests that do not read it keep the rest.

use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::Instant;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::search_filter::EnabledAccounts;
use extenddb_core::settings_keys::SEARCH_INEQUALITY_FILTERS;
use extenddb_storage::StorageEngine;
use extenddb_storage::management_store::{OpResult, SettingsStore};

/// How long a read of the setting is used before the next request reads it
/// again.
pub(crate) const REFRESH: Duration = Duration::from_secs(1);

/// How long a read of the setting may take before the request is answered
/// with `ServiceUnavailableException`.
pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Each storage engine of this process with the settings of its server.
type Servers = Vec<(Weak<dyn StorageEngine>, Arc<Settings>)>;

/// The servers of this process.
static SERVERS: Mutex<Servers> = Mutex::new(Vec::new());

/// Read the `search_inequality_filters` setting from `store` for the
/// SearchVectors requests that `storage` serves. The server calls this once
/// at startup.
pub fn install_search_filter_settings(
    storage: &Arc<dyn StorageEngine>,
    store: Arc<dyn SettingsStore>,
) {
    let mut servers = SERVERS.lock().unwrap_or_else(PoisonError::into_inner);
    servers.retain(|(engine, _)| engine.strong_count() > 0 && !same(engine, storage));
    servers.push((Arc::downgrade(storage), Arc::new(Settings::new(store))));
}

/// Drop every kept read of the setting, so the next request reads it. The
/// management API calls this after it writes the setting.
pub fn search_filter_settings_changed() {
    let servers = SERVERS.lock().unwrap_or_else(PoisonError::into_inner);
    for (_, settings) in servers.iter() {
        settings.changed();
    }
}

/// The accounts the setting enables on the server that `storage` belongs
/// to. An absent, empty or malformed value enables every account.
///
/// # Errors
///
/// Returns `ServiceUnavailableException` when the setting cannot be read.
pub(crate) async fn enabled_accounts(
    storage: &Arc<dyn StorageEngine>,
) -> Result<EnabledAccounts, DynamoDbError> {
    let settings = SERVERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|(engine, _)| same(engine, storage))
        .map(|(_, settings)| Arc::clone(settings));
    match settings {
        Some(settings) => settings.enabled_accounts().await,
        None => Ok(EnabledAccounts::All),
    }
}

fn same(engine: &Weak<dyn StorageEngine>, storage: &Arc<dyn StorageEngine>) -> bool {
    std::ptr::addr_eq(engine.as_ptr(), Arc::as_ptr(storage))
}

/// One read of the setting.
struct Kept {
    read_at: Instant,
    /// The stored value, so a malformed value is logged once per change.
    raw: Option<String>,
    /// `None` when the read failed.
    accounts: Option<EnabledAccounts>,
}

/// A read of the store running in its own task.
struct InFlight {
    /// The value of [`Settings::changes`] when the read was sent.
    changes: u64,
    task: JoinHandle<OpResult<Option<String>>>,
}

struct Settings {
    store: Arc<dyn SettingsStore>,
    kept: Mutex<Option<Kept>>,
    /// How many times the management API changed the setting, so a read
    /// sent before a change is not used after it.
    changes: Mutex<u64>,
    /// Held by the one request that reads the store when the kept read is
    /// stale, so concurrent requests wait for that read instead of sending
    /// their own. It holds a read that outlasted [`READ_TIMEOUT`] until the
    /// read ends, so the next request waits on it instead of sending another.
    reading: tokio::sync::Mutex<Option<InFlight>>,
}

impl Settings {
    fn new(store: Arc<dyn SettingsStore>) -> Self {
        Self {
            store,
            kept: Mutex::new(None),
            changes: Mutex::new(0),
            reading: tokio::sync::Mutex::new(None),
        }
    }

    fn changed(&self) {
        let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        *self.changes.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        *kept = None;
    }

    fn changes(&self) -> u64 {
        *self.changes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Send a read of the store in its own task, so it runs to its end when
    /// the request that waits on it stops waiting.
    fn send_read(&self) -> InFlight {
        let store = Arc::clone(&self.store);
        InFlight {
            changes: self.changes(),
            task: tokio::spawn(async move { store.get_setting(SEARCH_INEQUALITY_FILTERS).await }),
        }
    }

    /// The kept answer, while it is fresh.
    fn fresh(&self) -> Option<Result<EnabledAccounts, DynamoDbError>> {
        let kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        let kept = kept.as_ref().filter(|k| k.read_at.elapsed() < REFRESH)?;
        Some(kept.accounts.clone().ok_or_else(unavailable))
    }

    async fn enabled_accounts(&self) -> Result<EnabledAccounts, DynamoDbError> {
        if let Some(answer) = self.fresh() {
            return answer;
        }
        let mut reading = self.reading.lock().await;
        if let Some(answer) = self.fresh() {
            return answer;
        }
        // A read sent before a change through the management API may answer
        // the old value: it is left to end on its own and a new one is sent.
        if reading
            .as_ref()
            .is_some_and(|r| r.changes != self.changes())
        {
            *reading = None;
        }
        let in_flight = reading.get_or_insert_with(|| self.send_read());
        let sent = in_flight.changes;
        let read = tokio::time::timeout(READ_TIMEOUT, &mut in_flight.task).await;
        if read.is_ok() {
            *reading = None;
        }
        let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        let previous = kept.take().and_then(|k| k.raw);
        let (raw, accounts) = match read {
            Ok(Ok(Ok(raw))) => {
                let (accounts, ignored) = Self::parse(raw.as_deref(), previous.as_deref());
                if let Some(ignored) = ignored {
                    tracing::warn!("{ignored}");
                }
                (raw, Some(accounts))
            }
            Ok(Ok(Err(e))) => {
                tracing::warn!("{SEARCH_INEQUALITY_FILTERS} could not be read: {e:?}");
                (previous, None)
            }
            Ok(Err(e)) => {
                tracing::warn!("{SEARCH_INEQUALITY_FILTERS} could not be read: {e}");
                (previous, None)
            }
            Err(_) => {
                tracing::warn!(
                    "{SEARCH_INEQUALITY_FILTERS} could not be read within {READ_TIMEOUT:?}"
                );
                (previous, None)
            }
        };
        let answer = accounts.clone().ok_or_else(unavailable);
        if sent == self.changes() {
            *kept = Some(Kept {
                read_at: Instant::now(),
                raw,
                accounts,
            });
        }
        answer
    }

    /// The accounts a stored value enables, and the line to log when the
    /// value is malformed and differs from the previous read. An absent
    /// value, like a malformed one, enables every account.
    fn parse(raw: Option<&str>, previous: Option<&str>) -> (EnabledAccounts, Option<String>) {
        let Some(value) = raw else {
            return (EnabledAccounts::All, None);
        };
        match EnabledAccounts::parse(value) {
            Ok(accounts) => (accounts, None),
            Err(reason) => (
                EnabledAccounts::All,
                (previous != Some(value))
                    .then(|| format!("{SEARCH_INEQUALITY_FILTERS} = {value:?} ignored: {reason}")),
            ),
        }
    }
}

fn unavailable() -> DynamoDbError {
    DynamoDbError::ServiceUnavailable("Service is temporarily unavailable".to_owned())
}

#[cfg(test)]
#[path = "search_vectors_settings_tests.rs"]
mod tests;
