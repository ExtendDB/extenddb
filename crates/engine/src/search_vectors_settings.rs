// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `search_inequality_filters` setting, as each server reads it.
//!
//! The server installs its settings store against its storage engine with
//! [`install_search_filter_settings`], so two servers in one process read
//! their own catalogs. A storage engine without an installed store enables no
//! account.
//!
//! A read is kept for [`REFRESH`], so a busy server reads the catalog about
//! once per interval rather than once per request, and a change made through
//! the management API applies at once through
//! [`search_filter_settings_changed`]. A read that fails or outlasts
//! [`READ_TIMEOUT`] answers `ServiceUnavailableException`, as other transient
//! storage faults do; it never changes which grammar an account gets.

use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use tokio::time::Instant;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::search_filter::EnabledAccounts;
use extenddb_core::settings_keys::SEARCH_INEQUALITY_FILTERS;
use extenddb_storage::StorageEngine;
use extenddb_storage::management_store::SettingsStore;

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
        *settings.kept.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }
}

/// The accounts the setting enables on the server that `storage` belongs
/// to. An absent or malformed value enables none.
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
        None => Ok(EnabledAccounts::Off),
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

struct Settings {
    store: Arc<dyn SettingsStore>,
    kept: Mutex<Option<Kept>>,
    /// Held by the one request that reads the store when the kept read is
    /// stale, so concurrent requests wait for that read instead of sending
    /// their own.
    reading: tokio::sync::Mutex<()>,
}

impl Settings {
    fn new(store: Arc<dyn SettingsStore>) -> Self {
        Self {
            store,
            kept: Mutex::new(None),
            reading: tokio::sync::Mutex::new(()),
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
        let _reading = self.reading.lock().await;
        if let Some(answer) = self.fresh() {
            return answer;
        }
        let read = tokio::time::timeout(
            READ_TIMEOUT,
            self.store.get_setting(SEARCH_INEQUALITY_FILTERS),
        )
        .await;
        let previous = self
            .kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .and_then(|k| k.raw);
        let (raw, accounts) = match read {
            Ok(Ok(raw)) => {
                let accounts = Self::parse(raw.as_deref(), previous.as_deref());
                (raw, Some(accounts))
            }
            Ok(Err(e)) => {
                tracing::warn!("{SEARCH_INEQUALITY_FILTERS} could not be read: {e:?}");
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
        *self.kept.lock().unwrap_or_else(PoisonError::into_inner) = Some(Kept {
            read_at: Instant::now(),
            raw,
            accounts,
        });
        answer
    }

    fn parse(raw: Option<&str>, previous: Option<&str>) -> EnabledAccounts {
        let Some(value) = raw else {
            return EnabledAccounts::Off;
        };
        EnabledAccounts::parse(value).unwrap_or_else(|reason| {
            if previous != Some(value) {
                tracing::warn!("{SEARCH_INEQUALITY_FILTERS} = {value:?} ignored: {reason}");
            }
            EnabledAccounts::Off
        })
    }
}

fn unavailable() -> DynamoDbError {
    DynamoDbError::ServiceUnavailable("Service is temporarily unavailable".to_owned())
}

#[cfg(test)]
#[path = "search_vectors_settings_tests.rs"]
mod tests;
