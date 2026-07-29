// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Explicit backend registry.
//!
//! Backends are wired into a server via a [`BackendRegistry`] rather than by
//! link-time collection. A thin `main` constructs a registry, lets each backend
//! crate populate it through its `register(&mut BackendRegistry)` function, and
//! installs it once with [`set_registry`] before dispatching any subcommand:
//!
//! ```ignore
//! fn main() -> anyhow::Result<()> {
//!     let mut registry = extenddb_storage::registry::BackendRegistry::new();
//!     extenddb_storage_postgres::register(&mut registry);
//!     extenddb_storage::registry::set_registry(registry);
//!     extenddb_app::run()
//! }
//! ```
//!
//! This replaces the previous `inventory`-based auto-registration. Auto
//! registration relied on the linker preserving `submit!` statics, which only
//! happened if the binary referenced the backend crate — an invisible,
//! compiles-fine failure mode. An explicit registry makes registration a plain
//! function call that the compiler checks, and makes "which backends exist" a
//! single greppable location instead of a link-time side effect.
//!
//! A backend registers a coherent set of six factories keyed by its name:
//! bootstrapper, storage-config deserializer, operations engine, settings
//! store, diagnostics store, and server components.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::bootstrapper::BootstrapperFactory;
use crate::config::StorageConfigDeserializer;
use crate::diagnostics_store::DiagnosticsStoreFactory;
use crate::operations::OperationsEngine;
use crate::server_components::ServerComponentsFactory;
use crate::settings_store::SettingsStoreFactory;

/// Registry of all backends available to this process.
///
/// Construct with [`BackendRegistry::new`], populate via each backend's
/// `register` function, then install with [`set_registry`]. Reads go through
/// the free functions in the [`bootstrapper`](crate::bootstrapper),
/// [`config`](crate::config), [`operations`](crate::operations),
/// [`settings_store`](crate::settings_store),
/// [`diagnostics_store`](crate::diagnostics_store), and
/// [`server_components`](crate::server_components) modules, which resolve
/// against the installed registry.
#[derive(Default)]
pub struct BackendRegistry {
    pub(crate) bootstrappers: HashMap<&'static str, BootstrapperFactory>,
    pub(crate) storage_configs: HashMap<&'static str, StorageConfigDeserializer>,
    pub(crate) operations: HashMap<&'static str, &'static dyn OperationsEngine>,
    pub(crate) settings_stores: HashMap<&'static str, SettingsStoreFactory>,
    pub(crate) diagnostics_stores: HashMap<&'static str, DiagnosticsStoreFactory>,
    pub(crate) server_components: HashMap<&'static str, ServerComponentsFactory>,
    /// Registrations that displaced an existing entry for the same
    /// `(slot, backend name)` pair. Reported by [`set_registry`] so a wiring
    /// mistake fails startup instead of silently electing the last writer.
    duplicates: Vec<String>,
}

impl BackendRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a backend bootstrapper factory.
    pub fn register_bootstrapper(&mut self, name: &'static str, factory: BootstrapperFactory) {
        if self.bootstrappers.insert(name, factory).is_some() {
            self.record_duplicate("bootstrapper", name);
        }
    }

    /// Register a backend storage-config deserializer.
    pub fn register_storage_config(
        &mut self,
        backend: &'static str,
        deserializer: StorageConfigDeserializer,
    ) {
        if self.storage_configs.insert(backend, deserializer).is_some() {
            self.record_duplicate("storage config", backend);
        }
    }

    /// Register a backend operations engine.
    pub fn register_operations(
        &mut self,
        name: &'static str,
        operations: &'static dyn OperationsEngine,
    ) {
        if self.operations.insert(name, operations).is_some() {
            self.record_duplicate("operations engine", name);
        }
    }

    /// Register a backend settings-store factory.
    pub fn register_settings_store(
        &mut self,
        backend: &'static str,
        factory: SettingsStoreFactory,
    ) {
        if self.settings_stores.insert(backend, factory).is_some() {
            self.record_duplicate("settings store", backend);
        }
    }

    /// Register a backend diagnostics-store factory.
    pub fn register_diagnostics_store(
        &mut self,
        backend: &'static str,
        factory: DiagnosticsStoreFactory,
    ) {
        if self.diagnostics_stores.insert(backend, factory).is_some() {
            self.record_duplicate("diagnostics store", backend);
        }
    }

    /// Register a backend server-components factory.
    pub fn register_server_components(
        &mut self,
        backend: &'static str,
        factory: ServerComponentsFactory,
    ) {
        if self.server_components.insert(backend, factory).is_some() {
            self.record_duplicate("server components", backend);
        }
    }

    fn record_duplicate(&mut self, slot: &str, backend: &str) {
        self.duplicates
            .push(format!("{slot} for backend '{backend}'"));
    }
}

static REGISTRY: OnceLock<BackendRegistry> = OnceLock::new();

/// Error returned by [`set_registry`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    /// A registry was already installed in this process.
    AlreadySet,
    /// Two backends claimed the same registry slot. Each entry names the slot
    /// and the backend name that was registered twice.
    DuplicateRegistrations(Vec<String>),
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadySet => write!(f, "backend registry already installed"),
            Self::DuplicateRegistrations(dupes) => write!(
                f,
                "duplicate backend registration(s): {}. Two backends registered \
                 the same name; rename one or register only one of them.",
                dupes.join(", ")
            ),
        }
    }
}

impl std::error::Error for RegistryError {}

/// Install the process-wide backend registry.
///
/// Call exactly once, from `main`, before dispatching any subcommand.
///
/// # Errors
///
/// Returns [`RegistryError::DuplicateRegistrations`] if two backends claimed
/// the same registry slot — silently keeping the last writer would make the
/// effective backend depend on registration order. Returns
/// [`RegistryError::AlreadySet`] if a registry was already installed; the first
/// installed registry wins and the argument is dropped.
pub fn set_registry(registry: BackendRegistry) -> Result<(), RegistryError> {
    if !registry.duplicates.is_empty() {
        return Err(RegistryError::DuplicateRegistrations(registry.duplicates));
    }
    REGISTRY
        .set(registry)
        .map_err(|_| RegistryError::AlreadySet)
}

/// Borrow the installed registry, if one has been installed.
///
/// Returns `None` before [`set_registry`] runs. The lookup free functions treat
/// `None` the same as an empty registry (unknown-backend error / empty list),
/// so a missing registry degrades to a clear runtime error rather than a panic.
#[must_use]
pub fn try_registry() -> Option<&'static BackendRegistry> {
    REGISTRY.get()
}

#[cfg(test)]
mod tests {
    use super::{BackendRegistry, RegistryError, set_registry};
    use crate::config::StorageConfig;

    /// Minimal deserializer used only to occupy a registry slot.
    fn stub_deserializer(_: &toml::Table) -> Result<Box<dyn StorageConfig>, String> {
        Err("stub".to_owned())
    }

    #[test]
    fn distinct_backends_do_not_report_duplicates() {
        let mut registry = BackendRegistry::new();
        registry.register_storage_config("alpha", stub_deserializer);
        registry.register_storage_config("beta", stub_deserializer);
        assert_eq!(registry.duplicates, Vec::<String>::new());
    }

    #[test]
    fn duplicate_registration_fails_set_registry() {
        // Two backends claiming the same name must not silently elect the last
        // writer — the effective backend would then depend on the order of
        // `register` calls in `main`.
        let mut registry = BackendRegistry::new();
        registry.register_storage_config("postgres", stub_deserializer);
        registry.register_storage_config("postgres", stub_deserializer);

        let err = set_registry(registry).expect_err("duplicate registration must be rejected");
        match err {
            RegistryError::DuplicateRegistrations(dupes) => {
                assert_eq!(dupes.len(), 1);
                assert!(
                    dupes[0].contains("storage config") && dupes[0].contains("postgres"),
                    "error should name the slot and backend, got: {}",
                    dupes[0]
                );
            }
            other => panic!("expected DuplicateRegistrations, got {other:?}"),
        }
    }
}
