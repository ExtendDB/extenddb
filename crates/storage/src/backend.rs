// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The compiled-in storage backend.
//!
//! A binary is built for exactly one backend. The thin `main` installs it once
//! with [`set_backend`] before dispatching any subcommand:
//!
//! ```ignore
//! fn main() -> anyhow::Result<()> {
//!     extenddb_storage::set_backend(extenddb_storage_postgres::backend())?;
//!     extenddb_app::run(extenddb_app::BuildInfo { .. })
//! }
//! ```
//!
//! This replaces both the previous `inventory`-based auto-registration and the
//! name-keyed registry that succeeded it. Auto-registration relied on the linker
//! preserving `submit!` statics, which only happened if the binary referenced the
//! backend crate — an invisible, compiles-fine failure mode. A name-keyed
//! registry fixed that but kept string dispatch, which allows a class of runtime
//! error that cannot exist here: there is nothing to look up, so a mistyped or
//! absent backend name can no longer produce an "unknown backend" failure after
//! startup.
//!
//! The backend carries its own [`Backend::name`], so configuration can locate
//! its `[storage.<name>]` section without taking that name from the config file.

use std::sync::OnceLock;

use crate::bootstrapper::BootstrapperFactory;
use crate::config::StorageConfigDeserializer;
use crate::diagnostics_store::DiagnosticsStoreFactory;
use crate::operations::OperationsEngine;
use crate::server_components::ServerComponentsFactory;
use crate::settings_store::SettingsStoreFactory;

/// The complete set of factories a storage backend provides.
///
/// A backend crate exposes one constructor returning this value (by convention
/// `backend()`), which the thin bin hands to [`set_backend`].
pub struct Backend {
    /// Backend name, used for the `[storage.<name>]` config section, the startup
    /// banner, and diagnostics. This is the authoritative name: the config
    /// file's `backend` key is validated against it rather than driving dispatch.
    pub name: &'static str,
    /// Creates the deployment bootstrapper (`init`, `destroy`, `migrate`).
    pub bootstrapper: BootstrapperFactory,
    /// Deserializes the backend's `[storage.<name>]` config section.
    pub storage_config: StorageConfigDeserializer,
    /// Backend operations engine (catalog version, connection redaction).
    pub operations: &'static dyn OperationsEngine,
    /// Creates the runtime settings store.
    pub settings_store: SettingsStoreFactory,
    /// Creates the diagnostics store (`catalog-check`, `verify`).
    pub diagnostics_store: DiagnosticsStoreFactory,
    /// Creates the assembled server components for `serve`.
    pub server_components: ServerComponentsFactory,
}

static BACKEND: OnceLock<Backend> = OnceLock::new();

/// Error returned by [`set_backend`] when a backend was already installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendAlreadySet;

impl std::fmt::Display for BackendAlreadySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage backend already installed")
    }
}

impl std::error::Error for BackendAlreadySet {}

/// Install the process-wide storage backend.
///
/// Call exactly once, from `main`, before dispatching any subcommand.
///
/// # Errors
///
/// Returns [`BackendAlreadySet`] if a backend was already installed; the first
/// one wins and the argument is dropped.
pub fn set_backend(backend: Backend) -> Result<(), BackendAlreadySet> {
    BACKEND.set(backend).map_err(|_| BackendAlreadySet)
}

/// Borrow the installed backend, if one has been installed.
///
/// Returns `None` before [`set_backend`] runs. Callers treat that the same way
/// they treated a missing registry: a clear runtime error rather than a panic.
#[must_use]
pub fn try_backend() -> Option<&'static Backend> {
    BACKEND.get()
}

/// Name of the installed backend, or `None` before one is installed.
#[must_use]
pub fn backend_name() -> Option<&'static str> {
    try_backend().map(|b| b.name)
}

#[cfg(test)]
mod tests {
    use super::{BackendAlreadySet, backend_name, try_backend};

    /// Before `set_backend` runs there is no backend, and the lookup helpers say
    /// so rather than panicking. (`set_backend` installs into a process-wide
    /// `OnceLock`, so a positive test would leak into every other test in this
    /// binary and is covered by the integration suite instead.)
    #[test]
    fn no_backend_is_installed_by_default() {
        assert!(try_backend().is_none());
        assert_eq!(backend_name(), None);
    }

    #[test]
    fn already_set_error_renders() {
        assert_eq!(
            BackendAlreadySet.to_string(),
            "storage backend already installed"
        );
    }
}
