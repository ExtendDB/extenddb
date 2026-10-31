// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The per-process state of the backup operations: the job slots every
//! backup and restore job holds one of, and the store this instance is
//! configured to write.

use std::sync::Arc;

use extenddb_core::error::DynamoDbError;
use extenddb_storage::JobPermit;
use extenddb_storage::backup_catalog::{JobBound, StoreIdentity, limit_exceeded_message};
use tokio::sync::Semaphore;

/// The configuration key of the job bound the runtime enforces, as the
/// refusal names it.
const MAX_CONCURRENT_JOBS_KEY: &str = "[backup] max_concurrent_jobs";

/// One per server process, shared by every request through the operation
/// context.
#[derive(Debug)]
pub struct BackupRuntime {
    slots: Arc<Semaphore>,
    max_concurrent_jobs: usize,
    /// The store this instance writes new backups to and deletes from;
    /// `None` while no store is configured, when every catalog row is a row
    /// of another store.
    configured_store: Option<StoreIdentity>,
}

impl BackupRuntime {
    /// `max_concurrent_jobs` slots and no configured store.
    #[must_use]
    pub fn new(max_concurrent_jobs: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(max_concurrent_jobs)),
            max_concurrent_jobs,
            configured_store: None,
        }
    }

    /// The same runtime with `store` configured.
    #[must_use]
    pub fn with_store(mut self, store: StoreIdentity) -> Self {
        self.configured_store = Some(store);
        self
    }

    /// The slots jobs take permits from.
    #[must_use]
    pub fn slots(&self) -> &Arc<Semaphore> {
        &self.slots
    }

    /// Permits no job holds right now.
    #[must_use]
    pub fn free_slots(&self) -> usize {
        self.slots.available_permits()
    }

    /// Take a job slot without waiting, or `LimitExceededException` when every
    /// slot is held: a queued job would hold a snapshot or a transaction
    /// while it waited. The text is the one every refusal over a job bound
    /// answers ([`limit_exceeded_message`]), naming this one.
    pub(crate) fn take_permit(&self) -> Result<JobPermit, DynamoDbError> {
        JobPermit::try_acquire(&self.slots).ok_or_else(|| {
            DynamoDbError::LimitExceededException(limit_exceeded_message(JobBound::Counted {
                name: MAX_CONCURRENT_JOBS_KEY,
                limit: self.max_concurrent_jobs,
            }))
        })
    }

    /// Whether `store` is the one this instance is configured with. With no
    /// store configured, no row's store is.
    pub(crate) fn is_configured_store(&self, store: &StoreIdentity) -> bool {
        self.configured_store.as_ref() == Some(store)
    }
}
