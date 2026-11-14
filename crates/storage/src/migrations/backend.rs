// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The trait a backend implements for the runner.

use async_trait::async_trait;

use super::checksum::Released;
use super::code::{CodeBody, Done};
use super::ledger::{CatalogTraces, LedgerRow};
use super::registry::MigrationId;
use super::status::MigrationError;
use super::table::{Migration, StoreKind};

/// One store instance: its kind and the name a backend knows it by (the
/// SQLite file, a database name, a keyspace).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreRef {
    pub kind: StoreKind,
    pub name: String,
}

/// What the runner asks a backend to apply in one store.
pub enum Step<'a, H: 'static> {
    /// Execute a file body.
    File {
        name: &'static str,
        text: &'static str,
    },
    /// Run a code body.
    Code(&'a dyn CodeBody<H>),
    /// Run nothing and record the row only: a `NotApplicable` entry, or the
    /// catalog row of an id whose bodies all run in other stores.
    Record,
}

/// What the runner needs from a backend.
///
/// A backend owns its connections and its ledgers. The runner owns the order,
/// the version, the checksum rules and the status.
#[async_trait]
pub trait MigrationBackend: Send + Sync {
    /// The handle code bodies receive, which reaches every store.
    type Stores: Send + Sync + 'static;

    /// The backend's name, for messages.
    fn backend_name(&self) -> &'static str;

    /// The backend's entry for `id`: an exhaustive `match` with no wildcard.
    fn migration(&self, id: MigrationId) -> Migration<Self::Stores>;

    /// The backend's released checksums (see [`Released`]).
    fn released(&self) -> &'static [Released];

    /// The handle passed to code bodies.
    fn stores(&self) -> &Self::Stores;

    /// The instances of one kind of store. Exactly one for
    /// [`StoreKind::Catalog`].
    async fn store_names(&self, kind: StoreKind) -> Result<Vec<String>, MigrationError>;

    /// A store's ledger rows. A ledger table that does not exist reads as no
    /// rows; any other failure is an error, never "not applied".
    async fn read_ledger(&self, store: &StoreRef) -> Result<Vec<LedgerRow>, MigrationError>;

    /// The pre-1.0 traces in the catalog's settings.
    async fn read_traces(&self) -> Result<CatalogTraces, MigrationError>;

    /// Whether the catalog store holds no tables (or collections) at all, so
    /// a code body's `detect` has nothing to read in it.
    async fn catalog_is_empty(&self) -> Result<bool, MigrationError>;

    /// Called once, before the first write of a run that applies something.
    /// `from` is `None` for a catalog with no ledger. Returns lines for the
    /// operator (the SQLite copy's path).
    async fn before_first_write(
        &self,
        from: Option<u32>,
        to: u32,
    ) -> Result<Vec<String>, MigrationError>;

    /// Apply one step in one store. `row` is the ledger row to commit with
    /// it, for a store that has no row for `id`; `None` means the store has
    /// one (`--reconverge`), which is left as it is, notes and `applied_at`
    /// included. When `catalog_version` is given (the catalog store), write
    /// it to the `catalog_version` settings row in the same commit. The
    /// backend appends its own notes (a post-check's) to the body's and
    /// returns all of them.
    async fn apply(
        &self,
        store: &StoreRef,
        id: MigrationId,
        step: Step<'_, Self::Stores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, MigrationError>;

    /// Write the `catalog_version` row alone, after an id whose last body ran
    /// outside the catalog store.
    async fn write_catalog_version(&self, version: u32) -> Result<(), MigrationError>;

    /// Delete one id's row from a store's ledger (`forget`).
    async fn delete_ledger_row(&self, store: &StoreRef, id: u32) -> Result<(), MigrationError>;
}
