// SPDX-License-Identifier: Apache-2.0

//! The checks `init`'s bootstrap writes go through: the lease of the
//! migration lock `init` holds while it writes them, and the names it writes
//! into its CQL statements.

use extenddb_storage::management_store::{OpError, OpResult};
use extenddb_storage::migrations::MigrationError;

use crate::migrations::{LeaseHandle, REQUEST_TIMEOUT};

/// The lease of the lock `init` holds, checked before each bootstrap write
/// as the runner checks it before each migration request: a process paused
/// past its deadline writes nothing after another process may have taken
/// the lock. Every write waits at most the request timeout for its answer
/// and records progress when it completes. Holds nothing until `init` hands
/// the lock over, and for every command other than `init`.
#[derive(Default)]
pub(super) struct HeldLease {
    lease: std::sync::OnceLock<LeaseHandle>,
}

impl HeldLease {
    pub(super) fn hold(&self, lease: LeaseHandle) {
        let _ = self.lease.set(lease);
    }

    /// Refuse a write once the held lease's deadline has passed.
    pub(super) fn check(&self, what: &str) -> OpResult<()> {
        match self.lease.get() {
            Some(lease) => lease
                .check()
                .map_err(|e| OpError::Internal(format!("{what}: {e}"))),
            None => Ok(()),
        }
    }

    /// [`Self::check`] as a runner error, for code that takes a check
    /// closure.
    pub(super) fn check_run(&self, what: &str) -> Result<(), MigrationError> {
        match self.lease.get() {
            Some(lease) => lease
                .check()
                .map_err(|e| MigrationError::Store(format!("{what}: {e}"))),
            None => Ok(()),
        }
    }

    /// The held lease, for a run that sends its own requests under it.
    pub(super) fn handle(&self) -> Option<LeaseHandle> {
        self.lease.get().cloned()
    }

    /// Record progress on the held lease: a request sent under it completed.
    pub(super) fn progress(&self) {
        if let Some(lease) = self.lease.get() {
            lease.progress();
        }
    }

    /// Send `request` after the lease check, wait at most the request
    /// timeout for its answer, and record progress when it completes.
    pub(super) async fn request<T, E: std::fmt::Display>(
        &self,
        what: &str,
        request: impl std::future::Future<Output = Result<T, E>>,
    ) -> OpResult<T> {
        self.request_within(what, REQUEST_TIMEOUT, request).await
    }

    /// [`Self::request`] waiting at most `bound` for the answer: a schema
    /// statement gets the bound of one sent under the lock.
    pub(super) async fn request_within<T, E: std::fmt::Display>(
        &self,
        what: &str,
        bound: std::time::Duration,
        request: impl std::future::Future<Output = Result<T, E>>,
    ) -> OpResult<T> {
        self.check(what)?;
        let answer = tokio::time::timeout(bound, request).await;
        self.progress();
        match answer {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(OpError::Internal(format!("{what}: {e}"))),
            Err(_) => Err(OpError::Internal(format!(
                "{what}: no answer within {} s",
                bound.as_secs()
            ))),
        }
    }
}

/// Refuse an application role or keyspace prefix holding anything other
/// than ASCII letters, digits and `_`. `init` writes the role between single
/// quotes and the keyspaces unquoted into its `CREATE ROLE`, `CREATE
/// KEYSPACE` and `GRANT` statements, so a quote or a space in either would
/// break a statement partway through `init`.
pub(super) fn check_name(
    what: &str,
    value: &str,
) -> Result<(), extenddb_storage::error::StorageError> {
    if !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Ok(());
    }
    Err(extenddb_storage::error::StorageError::Internal(format!(
        "{what} {value:?} may hold only ASCII letters, digits and '_'"
    )))
}

#[cfg(test)]
mod tests {
    use super::HeldLease;
    use crate::bootstrapper::CassandraBootstrapper;

    /// `init` writes its bootstrap rows under the migration lock: a process
    /// paused past its lease's deadline sends none of them, and one that
    /// holds no lock (every other command) sends them unchecked.
    #[tokio::test]
    async fn a_bootstrap_write_is_refused_once_the_held_lease_lapses() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let sent = AtomicBool::new(false);
        let write = || async {
            sent.store(true, Ordering::SeqCst);
            Ok::<u8, String>(1)
        };

        let unheld = HeldLease::default();
        assert_eq!(unheld.request("a row", write()).await.unwrap(), 1);
        assert!(sent.swap(false, Ordering::SeqCst));
        assert!(unheld.check("a row").is_ok());

        let held = HeldLease::default();
        held.hold(crate::migrations::LeaseHandle::lapsed("p2m_catalog"));
        let refused = held.request("Create admin user", write()).await;
        let message = format!("{refused:?}");
        assert!(
            message.contains("Create admin user")
                && message.contains("migration lock in p2m_catalog.settings was not renewed"),
            "{message}"
        );
        assert!(!sent.load(Ordering::SeqCst), "the write was sent");
        assert!(held.check("Create account").is_err());

        let failed = unheld
            .request("a row", async {
                Err::<u8, String>("refused by the node".to_owned())
            })
            .await;
        assert_eq!(
            format!("{failed:?}"),
            format!(
                "{:?}",
                Err::<u8, _>(super::OpError::Internal(
                    "a row: refused by the node".to_owned()
                ))
            )
        );
    }

    /// The application role and the keyspace prefix go into `init`'s CQL
    /// statements, so a value holding anything but ASCII letters, digits
    /// and `_` is refused before `init` connects. The contact point is a
    /// closed port: without the check the call fails on the connection
    /// instead, with another message.
    #[tokio::test]
    async fn a_role_or_prefix_with_another_character_is_refused_before_connecting() {
        for (flag, value) in [
            ("--extenddb-user", "app'role"),
            ("--extenddb-user", "app role"),
            ("--extenddb-user", ""),
            ("--keyspace-prefix", "pfx'x"),
            ("--keyspace-prefix", "pfx-x"),
        ] {
            let args: Vec<String> = ["--cassandra-contact-points", "127.0.0.1:1", flag, value]
                .iter()
                .map(|s| (*s).to_owned())
                .collect();
            let message =
                match CassandraBootstrapper::from_config("/nonexistent/x.toml", &args).await {
                    Err(e) => e.to_string(),
                    Ok(_) => panic!("{flag} {value:?} was accepted"),
                };
            assert!(
                message.contains(flag)
                    && message.contains(&format!("{value:?}"))
                    && message.contains("ASCII letters, digits and '_'"),
                "{flag} {value:?}: {message}"
            );
        }
        for value in ["extenddb", "app_role_2", "A1", "_x"] {
            assert!(super::check_name("a name", value).is_ok(), "{value}");
        }
    }
}
