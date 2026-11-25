// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `search_inequality_filters` setting, as each server reads it.
//!
//! The server installs its settings store against its storage engine with
//! [`install_search_filter_settings`], so two servers in one process read
//! their own catalogs. An absent or empty value enables every account, as
//! does a storage engine without an installed store. A malformed value is
//! logged once per change and enables no account.
//!
//! A read is kept for [`REFRESH`], so a busy server reads the catalog about
//! once per interval rather than once per request, and a change made through
//! the management API applies at once through
//! [`search_filter_settings_changed`]. When a read fails or outlasts
//! [`READ_TIMEOUT`], the server uses the value of its last read that
//! answered, a change through the management API notwithstanding. Before any
//! read has answered, such a read answers `ServiceUnavailable`, as
//! other transient storage faults do. A fault never changes which grammar an
//! account gets.
//!
//! A server sends at most one read of the setting at a time, a read sent
//! before a change through the management API included. A read that
//! outlasts [`READ_TIMEOUT`] keeps running in its own task until it ends,
//! and no other read is sent meanwhile, so a catalog that does not answer
//! holds at most one of its connections for the setting while requests that
//! do not read it keep the rest. Once a read has answered, a request never
//! waits on a read another request sent: while a read is in flight, it uses
//! the value of the last read that answered. Before any read has answered,
//! a request waits on the read in flight until [`READ_TIMEOUT`] after it
//! was sent, and once that has passed, it answers at once. A change through
//! the management API applies at the first read sent after it, which is the
//! next one once no read is in flight.

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
/// with `ServiceUnavailable`.
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
/// to. An absent or empty value enables every account, a malformed one none.
///
/// # Errors
///
/// Returns `ServiceUnavailable` when the setting cannot be read.
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
    /// The value of the last read that answered, this one or an earlier one.
    /// `None` when no read has answered yet.
    accounts: Option<EnabledAccounts>,
}

/// A read of the store running in its own task.
struct InFlight {
    /// The value of [`Settings::changes`] when the read was sent.
    changes: u64,
    /// When the read was sent.
    sent_at: Instant,
    task: JoinHandle<OpResult<Option<String>>>,
}

struct Settings {
    store: Arc<dyn SettingsStore>,
    kept: Mutex<Option<Kept>>,
    /// The value of the last read that answered. A change through the
    /// management API leaves it, so a read that fails after a change uses it.
    last: Mutex<Option<EnabledAccounts>>,
    /// How many times the management API changed the setting, so a read
    /// sent before a change is not used after it.
    changes: Mutex<u64>,
    /// Held by the one request that reads the store when the kept read is
    /// stale, so concurrent requests do not send reads of their own. It
    /// holds a read that outlasted [`READ_TIMEOUT`] until the read ends, so
    /// no other read is sent while it runs.
    reading: tokio::sync::Mutex<Option<InFlight>>,
}

impl Settings {
    fn new(store: Arc<dyn SettingsStore>) -> Self {
        Self {
            store,
            kept: Mutex::new(None),
            last: Mutex::new(None),
            changes: Mutex::new(0),
            reading: tokio::sync::Mutex::new(None),
        }
    }

    fn changed(&self) {
        let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        *self.changes.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        *kept = None;
    }

    /// The value of the last read that answered.
    fn last(&self) -> Option<EnabledAccounts> {
        self.last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
            sent_at: Instant::now(),
            task: tokio::spawn(async move {
                store.get_setting_bounded(SEARCH_INEQUALITY_FILTERS).await
            }),
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
        // Once a read has answered, a request does not wait on a read that
        // another request waits on, or that outlasted READ_TIMEOUT: it uses
        // the value of the last read that answered.
        let mut reading = match self.reading.try_lock() {
            Ok(reading) => reading,
            Err(_) => match self.last() {
                Some(accounts) => return Ok(accounts),
                None => self.reading.lock().await,
            },
        };
        if let Some(answer) = self.fresh() {
            return answer;
        }
        if let Some(in_flight) = reading.as_ref() {
            if !in_flight.task.is_finished() {
                // The read has not ended, whenever it was sent: no other is
                // sent while it runs.
                if let Some(accounts) = self.last() {
                    return Ok(accounts);
                }
            } else if in_flight.changes != self.changes() {
                // A read that ended but was sent before a change through the
                // management API may hold the old value: a new one is sent.
                *reading = None;
            }
        }
        let in_flight = reading.get_or_insert_with(|| self.send_read());
        let sent = in_flight.changes;
        // A request waits on a read only until READ_TIMEOUT after it was
        // sent, so a read that does not end delays the first requests
        // rather than one request after each refresh.
        let wait = READ_TIMEOUT.saturating_sub(in_flight.sent_at.elapsed());
        let read = tokio::time::timeout(wait, &mut in_flight.task).await;
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
                *self.last.lock().unwrap_or_else(PoisonError::into_inner) = Some(accounts.clone());
                (raw, Some(accounts))
            }
            Ok(Ok(Err(e))) => {
                tracing::warn!("{SEARCH_INEQUALITY_FILTERS} could not be read: {e:?}");
                (previous, self.last())
            }
            Ok(Err(e)) => {
                tracing::warn!("{SEARCH_INEQUALITY_FILTERS} could not be read: {e}");
                (previous, self.last())
            }
            Err(_) => {
                tracing::warn!(
                    "{SEARCH_INEQUALITY_FILTERS} could not be read within {READ_TIMEOUT:?}"
                );
                (previous, self.last())
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
    /// value enables every account, a malformed one none.
    fn parse(raw: Option<&str>, previous: Option<&str>) -> (EnabledAccounts, Option<String>) {
        let Some(value) = raw else {
            return (EnabledAccounts::All, None);
        };
        match EnabledAccounts::parse(value) {
            Ok(accounts) => (accounts, None),
            Err(reason) => (
                EnabledAccounts::Off,
                (previous != Some(value)).then(|| {
                    format!("{SEARCH_INEQUALITY_FILTERS} = {value:?} enables no account: {reason}")
                }),
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
