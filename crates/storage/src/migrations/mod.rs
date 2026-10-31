// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog migrations: one numbered list for every backend, a ledger in every
//! store, and the runner that applies the list.
//!
//! - [`MigrationId`] numbers every migration once, for all backends.
//! - Each backend declares, per id, the body it runs in each of its stores or
//!   why the id does not apply to it ([`Migration`]), in an exhaustive `match`
//!   with no wildcard arm, so a backend that lacks an entry does not compile.
//! - [`Runner`] applies the pending bodies in order, catalog store first, and
//!   commits each body with its ledger row. The catalog version is read from
//!   the ledgers, never from a constant or a file.
//! - [`Runner::status`] computes the [`CatalogStatus`] that `serve`,
//!   `migrate` and `verify` act on, and [`CatalogStatus::exit_code`] maps it
//!   to the process exit status.
//!
//! A backend implements [`MigrationBackend`]: reading and writing its ledgers,
//! running one body inside one store's commit, and whatever its store needs
//! before the first write of a run (the SQLite copy).

pub mod advice;
mod backend;
mod catalog;
mod checksum;
mod code;
mod ledger;
mod record;
mod registry;
mod runner;
mod status;
mod table;

// Frozen once released: neither rustfmt nor a later clippy lint may require
// an edit that changes its checksum.
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
pub mod m0002_resource_tag_condition_keys;

pub use backend::{MigrationBackend, Step, StoreRef};
pub use catalog::{
    AdminRole, CatalogMigrations, HeldLock, Listed, RunKind, StatusNote, Target, before_lock,
    binary_catalog_label, forget_locked, installed, run_locked,
};
#[cfg(any(test, feature = "test-util"))]
pub use checksum::check_released;
pub use checksum::{Released, file_checksum, split_comments};
pub use code::{CodeBody, Done, HasManagementStore};
pub use ledger::{CatalogTraces, LedgerColumns, LedgerRow, Outcome};
pub use record::{LedgerWrites, in_transaction, record_rows, sql_ledger_ddl};
pub use registry::{MigrationId, RELEASED_THROUGH};
pub use runner::Runner;
pub use status::{AppliedStep, CatalogStatus, MigrationError, Pending, RunReport, exit};
#[cfg(any(test, feature = "test-util"))]
pub use table::refuse_additive;
pub use table::{Body, Kind, Migration, StoreBody, StoreKind};

#[cfg(test)]
mod checksum_tests;
#[cfg(test)]
mod reconverge_tests;
#[cfg(test)]
mod tests;
