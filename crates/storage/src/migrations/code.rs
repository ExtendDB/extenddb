// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Code bodies: migrations that DDL cannot express.

use async_trait::async_trait;

use crate::management_store::ManagementStore;

use super::status::MigrationError;

/// What a code body's `run` returns when it finishes. The notes are recorded
/// in the ledger row and printed by `migrate` and `verify`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Done {
    pub notes: Vec<String>,
}

/// A migration written in Rust. `H` is the backend's handle to every one of
/// its stores, because a body recorded in one store may read another.
#[async_trait]
pub trait CodeBody<H: ?Sized>: Send + Sync {
    /// The checksum recorded in the ledger. A constant of the module, named
    /// `NNNN_<name>/...` after its id, that changes only with a new
    /// migration, so a driver upgrade that edits a released body (with a
    /// `rebless:` note) leaves deployed ledgers valid.
    fn fingerprint(&self) -> &'static str;

    /// Read-only. Returns the items `run` would refuse to proceed past, each
    /// a line for the operator. The runner calls it for every pending code
    /// body before the first write of a run and stops on any item.
    async fn detect(&self, stores: &H) -> Result<Vec<String>, MigrationError>;

    /// Idempotent and safe to stop at any point.
    async fn run(&self, stores: &H) -> Result<Done, MigrationError>;
}

/// A backend handle that reaches the catalog through [`ManagementStore`],
/// which the resource-tag rewrite (migration 2) uses on every backend.
pub trait HasManagementStore {
    fn management_store(&self) -> &dyn ManagementStore;
}
