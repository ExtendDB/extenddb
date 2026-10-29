// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog status, the runner's errors, what a run did, and the process
//! exit codes.

use super::table::{Kind, StoreKind};

/// One step a run applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedStep {
    pub id: u32,
    pub name: &'static str,
    pub store_name: String,
    /// The body's notes from this run. A reconverge leaves the row's notes
    /// from the first application as they are, so these are printed only.
    pub notes: Vec<String>,
}

/// What a run did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunReport {
    /// The catalog version before the run; `None` without a ledger.
    pub from: Option<u32>,
    /// The catalog version after it.
    pub to: Option<u32>,
    pub applied: Vec<AppliedStep>,
    /// Lines from the backend's preparation (the SQLite copy's path).
    pub notes: Vec<String>,
}

/// Process exit codes shared by `serve`, `migrate`, `migrate --yes` and
/// `verify`.
pub mod exit {
    /// Current, or applied. `AheadCompatible` also exits 0, with a warning.
    pub const OK: u8 = 0;
    /// An error, and `Uninitialized` in a production build.
    pub const ERROR: u8 = 1;
    /// Migrations are pending that `migrate --yes` will apply.
    pub const PENDING: u8 = 3;
    /// The catalog has a breaking migration this binary does not know.
    pub const AHEAD_INCOMPATIBLE: u8 = 4;
    /// A person must act: `NeedsAttention`, `Changed`, `WrittenByOlderRelease`.
    pub const ATTENTION: u8 = 5;
    /// The lock or a running server outlasted the timeout; retry.
    pub const LOCKED: u8 = 6;
}

/// A migration this binary carries that a store has no ledger row for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pending {
    pub id: u32,
    pub name: &'static str,
    pub store: StoreKind,
    /// The store instance (the SQLite file, a keyspace name).
    pub store_name: String,
    pub kind: Kind,
}

/// The status `serve`, `migrate` and `verify` act on. When several apply, it
/// is the first in this order: `Changed`, `WrittenByOlderRelease`,
/// `AheadIncompatible`, `NeedsAttention`, `Behind`, `AheadCompatible`,
/// `Current`. `Uninitialized` is decided before all of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogStatus {
    /// Every migration this binary carries is recorded in every store it
    /// touches.
    Current { version: u32 },
    /// Migrations this binary carries are missing. `version` is `None` for a
    /// catalog with no ledger (a release before 1.0, behind at migration 1).
    Behind {
        version: Option<u32>,
        pending: Vec<Pending>,
    },
    /// The catalog ledger has ids above this binary's, all additive.
    AheadCompatible {
        version: u32,
        unknown: Vec<(u32, String)>,
    },
    /// The catalog ledger has an id above this binary's that is breaking.
    AheadIncompatible { unknown: Vec<(u32, String)> },
    /// A pending code body's `detect` returned items.
    NeedsAttention { items: Vec<String> },
    /// A recorded checksum differs from this binary's body.
    Changed {
        id: u32,
        store_name: String,
        recorded: String,
        expected: String,
    },
    /// The catalog ledger has a row for migration 1 and a release before 1.0
    /// wrote to the catalog afterwards.
    WrittenByOlderRelease { trace: String },
    /// Not a catalog: no ledger row and no `catalog_version` row.
    Uninitialized,
}

impl CatalogStatus {
    /// The exit code for this status as read, for `serve`, `migrate` without
    /// `--yes` and `verify` in a production build. `migrate --yes` exits 0
    /// once it has applied a `Behind` catalog.
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Current { .. } | Self::AheadCompatible { .. } => exit::OK,
            Self::Behind { .. } => exit::PENDING,
            Self::AheadIncompatible { .. } => exit::AHEAD_INCOMPATIBLE,
            Self::NeedsAttention { .. }
            | Self::Changed { .. }
            | Self::WrittenByOlderRelease { .. } => exit::ATTENTION,
            Self::Uninitialized => exit::ERROR,
        }
    }

    /// A short name for tests.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Current { .. } => "Current",
            Self::Behind { .. } => "Behind",
            Self::AheadCompatible { .. } => "AheadCompatible",
            Self::AheadIncompatible { .. } => "AheadIncompatible",
            Self::NeedsAttention { .. } => "NeedsAttention",
            Self::Changed { .. } => "Changed",
            Self::WrittenByOlderRelease { .. } => "WrittenByOlderRelease",
            Self::Uninitialized => "Uninitialized",
        }
    }
}

impl std::fmt::Display for CatalogStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Current { version } => write!(f, "the catalog is at migration {version}"),
            Self::Behind { version, pending } => {
                let at = version.map_or_else(
                    || "has no migration ledger (a release before 1.0)".to_owned(),
                    |v| format!("is at migration {v}"),
                );
                let list: Vec<String> = pending
                    .iter()
                    .map(|p| {
                        format!(
                            "{} {} ({}, {})",
                            p.id,
                            p.name,
                            p.store.as_str(),
                            p.kind.as_str()
                        )
                    })
                    .collect();
                write!(f, "the catalog {at}; pending: {}", list.join(", "))
            }
            Self::AheadCompatible { version, unknown } => write!(
                f,
                "the catalog is at migration {version}, ahead of this binary by additive \
                 migrations only: {}",
                unknown_list(unknown)
            ),
            Self::AheadIncompatible { unknown } => write!(
                f,
                "the catalog has migrations this binary does not know, at least one of them \
                 breaking: {}",
                unknown_list(unknown)
            ),
            Self::NeedsAttention { items } => write!(
                f,
                "a pending migration needs attention before it can run: {}",
                items.join("; ")
            ),
            Self::Changed {
                id,
                store_name,
                recorded,
                expected,
            } => write!(
                f,
                "migration {id} in {store_name} was recorded with checksum {recorded}, and this \
                 binary's body has {expected}"
            ),
            Self::WrittenByOlderRelease { trace } => write!(
                f,
                "a release before 1.0 wrote to this catalog after it was migrated ({trace}); \
                 run 'extenddb migrate --yes --reconverge'"
            ),
            Self::Uninitialized => write!(f, "the catalog is not initialized"),
        }
    }
}

fn unknown_list(unknown: &[(u32, String)]) -> String {
    unknown
        .iter()
        .map(|(id, name)| format!("{id} {name}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Why the runner stopped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MigrationError {
    /// A connection or statement failed, or a body returned an error.
    #[error("{0}")]
    Store(String),
    /// A statement failed with a database error code (PostgreSQL's
    /// SQLSTATE). The runner reads the code, never the message, to run a
    /// body again after a lock timeout or to name the owners of tables the
    /// role may not alter.
    #[error("{message}")]
    Database { message: String, sqlstate: String },
    /// The catalog's status forbids the requested run.
    #[error("{0}")]
    Status(CatalogStatus),
    /// `forget`, `reconverge` or a backend precondition refused the request.
    #[error("{0}")]
    Refused(String),
    /// The lock was not free within the timeout.
    #[error("{0}")]
    Locked(String),
    /// Sessions an earlier run left behind still hold what this run needs,
    /// past the timeout, and do not end by themselves: the message names
    /// them and the statement that ends them. Exits as [`Self::Locked`].
    #[error("{0}")]
    Blocked(String),
    /// A run stopped on `error` after its preparation: `report` holds what
    /// it did first, the steps it committed and the backend's lines (the
    /// SQLite copy's path).
    #[error("{error}")]
    Stopped {
        error: Box<MigrationError>,
        report: RunReport,
    },
}

impl MigrationError {
    /// The process exit code for this error.
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Store(_) | Self::Database { .. } | Self::Refused(_) => exit::ERROR,
            Self::Status(status) => status.exit_code(),
            Self::Locked(_) | Self::Blocked(_) => exit::LOCKED,
            Self::Stopped { error, .. } => error.exit_code(),
        }
    }

    /// Shorthand for a store error with context.
    pub fn store(context: impl std::fmt::Display, err: impl std::fmt::Display) -> Self {
        Self::Store(format!("{context}: {err}"))
    }

    /// A store error with context that keeps the database's error code when
    /// the driver reported one. Each backend's helper that maps its driver's
    /// error calls this.
    pub fn coded(
        context: impl std::fmt::Display,
        err: impl std::fmt::Display,
        sqlstate: Option<String>,
    ) -> Self {
        let message = format!("{context}: {err}");
        match sqlstate {
            Some(sqlstate) => Self::Database { message, sqlstate },
            None => Self::Store(message),
        }
    }

    /// The database error code of the statement that failed, when the error
    /// carries one.
    #[must_use]
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Self::Database { sqlstate, .. } => Some(sqlstate),
            Self::Stopped { error, .. } => error.sqlstate(),
            Self::Store(_)
            | Self::Status(_)
            | Self::Refused(_)
            | Self::Locked(_)
            | Self::Blocked(_) => None,
        }
    }
}
