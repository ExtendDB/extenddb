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
}

impl BackendRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a backend bootstrapper factory.
    pub fn register_bootstrapper(&mut self, name: &'static str, factory: BootstrapperFactory) {
        self.bootstrappers.insert(name, factory);
    }

    /// Register a backend storage-config deserializer.
    pub fn register_storage_config(
        &mut self,
        backend: &'static str,
        deserializer: StorageConfigDeserializer,
    ) {
        self.storage_configs.insert(backend, deserializer);
    }

    /// Register a backend operations engine.
    pub fn register_operations(
        &mut self,
        name: &'static str,
        operations: &'static dyn OperationsEngine,
    ) {
        self.operations.insert(name, operations);
    }

    /// Register a backend settings-store factory.
    pub fn register_settings_store(
        &mut self,
        backend: &'static str,
        factory: SettingsStoreFactory,
    ) {
        self.settings_stores.insert(backend, factory);
    }

    /// Register a backend diagnostics-store factory.
    pub fn register_diagnostics_store(
        &mut self,
        backend: &'static str,
        factory: DiagnosticsStoreFactory,
    ) {
        self.diagnostics_stores.insert(backend, factory);
    }

    /// Register a backend server-components factory.
    pub fn register_server_components(
        &mut self,
        backend: &'static str,
        factory: ServerComponentsFactory,
    ) {
        self.server_components.insert(backend, factory);
    }
}

static REGISTRY: OnceLock<BackendRegistry> = OnceLock::new();

/// Error returned by [`set_registry`] when a registry was already installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryAlreadySet;

impl std::fmt::Display for RegistryAlreadySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "backend registry already installed")
    }
}

impl std::error::Error for RegistryAlreadySet {}

/// Install the process-wide backend registry.
///
/// Call exactly once, from `main`, before dispatching any subcommand.
///
/// # Errors
///
/// Returns [`RegistryAlreadySet`] if a registry was already installed; the
/// first installed registry wins and the argument is dropped.
pub fn set_registry(registry: BackendRegistry) -> Result<(), RegistryAlreadySet> {
    REGISTRY.set(registry).map_err(|_| RegistryAlreadySet)
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
