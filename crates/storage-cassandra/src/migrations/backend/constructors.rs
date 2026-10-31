// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The constructors of [`CassandraMigrations`], one for each kind of run,
//! and the settings a run is built with.

use std::sync::Arc;

use super::{Accounts, CassandraMigrations, CassandraStores};
use crate::cassandra_util::CassandraSession;
use crate::migrations::locks::{LeaseHandle, MigrationLock};

impl<'l> CassandraMigrations<'l> {
    /// The deployment's every store, read only: for the status.
    #[must_use]
    pub fn for_status(session: Arc<CassandraSession>, keyspace_prefix: &str) -> Self {
        Self {
            stores: CassandraStores::new(session, keyspace_prefix, None),
            accounts: Accounts::All,
            lock: None,
            lease: None,
            stop_after: None,
            app_role: None,
        }
    }

    /// The deployment's every store, for a run under `lock`.
    #[must_use]
    pub fn locked(lock: &'l MigrationLock, keyspace_prefix: &str) -> Self {
        Self {
            stores: CassandraStores::new(
                lock.session().clone(),
                keyspace_prefix,
                Some(lock.handle()),
            ),
            accounts: Accounts::All,
            lock: Some(lock),
            lease: None,
            stop_after: None,
            app_role: None,
        }
    }

    /// The catalog keyspace alone, for a run under `lock`: for tests.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn locked_catalog_only(lock: &'l MigrationLock, keyspace_prefix: &str) -> Self {
        Self {
            stores: CassandraStores::new(
                lock.session().clone(),
                keyspace_prefix,
                Some(lock.handle()),
            ),
            accounts: Accounts::None,
            lock: Some(lock),
            lease: None,
            stop_after: None,
            app_role: None,
        }
    }

    /// One account keyspace, for CreateAccount: the runner records the
    /// account migrations in that keyspace only.
    #[must_use]
    pub fn for_account(
        session: Arc<CassandraSession>,
        keyspace_prefix: &str,
        keyspace: &str,
    ) -> Self {
        Self {
            stores: CassandraStores::new(session, keyspace_prefix, None),
            accounts: Accounts::One(keyspace.to_owned()),
            lock: None,
            lease: None,
            stop_after: None,
            app_role: None,
        }
    }

    /// One account keyspace, for `init`'s default account, under the lease
    /// of the migration lock `init` holds: the lease is checked before each
    /// write, each completed request records progress on it, and each schema
    /// statement gets the bound of one sent under the lock.
    #[must_use]
    pub(crate) fn for_account_under(
        session: Arc<CassandraSession>,
        keyspace_prefix: &str,
        keyspace: &str,
        lease: LeaseHandle,
    ) -> Self {
        Self {
            stores: CassandraStores::new(session, keyspace_prefix, Some(lease.clone())),
            accounts: Accounts::One(keyspace.to_owned()),
            lock: None,
            lease: Some(lease),
            stop_after: None,
            app_role: None,
        }
    }

    /// Grant `role`, the application role, its permissions on an account
    /// keyspace this run creates: for a run that connects as another role
    /// (`migrate --cassandra-user`).
    #[must_use]
    pub fn granting(mut self, role: Option<String>) -> Self {
        self.app_role = role;
        self
    }

    /// Stop this run before its `requests`-th write, as a migrator killed
    /// there stops: for the crash-and-rerun tests. Every write the backend
    /// sends counts (a file's statements, the ledger table and its rows, the
    /// version row, an account keyspace, the start of a code body) and so does
    /// each policy write of migration 2's body; reads do not.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn stop_after(mut self, requests: usize) -> Self {
        let left = Arc::new(std::sync::atomic::AtomicUsize::new(requests));
        self.stores.management.stop_after(Arc::clone(&left));
        self.stop_after = Some(left);
        self
    }

    /// The writes [`Self::stop_after`]'s budget still allows: a run given
    /// `usize::MAX` that finishes has sent `usize::MAX` minus this many.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn requests_left(&self) -> Option<usize> {
        self.stop_after
            .as_deref()
            .map(|left| left.load(std::sync::atomic::Ordering::SeqCst))
    }
}
