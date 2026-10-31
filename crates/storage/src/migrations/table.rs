// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The types a backend declares its migration table with.

use super::code::CodeBody;

/// Whether the previous release reads and writes correctly against a catalog
/// with the migration applied. An older binary serves a catalog whose
/// unknown ids are all additive and refuses one with a breaking id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The previous release serves the result correctly.
    Additive,
    /// The previous release must not serve the result.
    Breaking,
}

impl Kind {
    /// The value recorded in the ledger's `kind` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Additive => "additive",
            Self::Breaking => "breaking",
        }
    }

    /// Parse a ledger value.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "additive" => Some(Self::Additive),
            "breaking" => Some(Self::Breaking),
            _ => None,
        }
    }
}

/// A kind of store a backend keeps. The runner applies an id's bodies in this
/// order, so the catalog store always comes first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StoreKind {
    /// The catalog database, keyspace or file. Its ledger has a row for every
    /// id and is the deployment's record.
    Catalog,
    /// The data database (PostgreSQL, MongoDB).
    Data,
    /// One account keyspace (Cassandra): the body runs in each of them.
    Account,
}

impl StoreKind {
    /// Lower-case name for messages and the checksum lines.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Data => "data",
            Self::Account => "account",
        }
    }
}

/// What the runner runs in one store.
pub enum Body<H: 'static> {
    /// SQL or CQL embedded with `include_str!`. `name` is the file name; its
    /// leading number must equal the id.
    File {
        name: &'static str,
        text: &'static str,
    },
    /// A Rust body. `source` is its module's source, embedded with
    /// `include_str!`, whose SHA-256 the checksum test pins; the ledger
    /// records the body's fingerprint instead.
    Code {
        body: &'static dyn CodeBody<H>,
        source: &'static str,
    },
}

/// One body and the kind of store it runs in.
pub struct StoreBody<H: 'static> {
    pub store: StoreKind,
    pub body: Body<H>,
}

impl<H: 'static> StoreBody<H> {
    /// A file body.
    #[must_use]
    pub fn file(store: StoreKind, name: &'static str, text: &'static str) -> Self {
        Self {
            store,
            body: Body::File { name, text },
        }
    }

    /// A code body.
    #[must_use]
    pub fn code(store: StoreKind, body: &'static dyn CodeBody<H>, source: &'static str) -> Self {
        Self {
            store,
            body: Body::Code { body, source },
        }
    }
}

/// A migration file of the crate that invokes the macro, named once.
///
/// - `file_body!(store, "dir", "NNNN_<name>.sql")` is the [`StoreBody`] that
///   executes `dir/NNNN_<name>.sql` in `store`; the checksum rules read the
///   file name.
/// - `file_body!("dir", "NNNN_<name>.sql")` is the file's text, for a code
///   body that executes a file and pins it together with its own source.
///
/// `dir` starts at the invoking crate's root (`CARGO_MANIFEST_DIR`), so the
/// path is the same from every module of that crate.
#[macro_export]
macro_rules! file_body {
    ($dir:literal, $name:literal) => {
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $dir, "/", $name))
    };
    ($store:expr, $dir:literal, $name:literal) => {
        $crate::migrations::StoreBody::file($store, $name, $crate::file_body!($dir, $name))
    };
}

/// One backend's entry for one id.
pub enum Migration<H: 'static> {
    /// The migration changes this backend.
    Applies {
        kind: Kind,
        /// Safe to run again on a store at any later id. `--reconverge` runs
        /// only convergent migrations.
        convergent: bool,
        /// One body per kind of store, catalog first.
        bodies: Vec<StoreBody<H>>,
    },
    /// The change has no effect on this backend. Always additive and
    /// convergent; recorded in the catalog ledger as `not_applicable`.
    NotApplicable(&'static str),
}

impl<H: 'static> Migration<H> {
    /// The kind declared for this backend.
    #[must_use]
    pub fn kind(&self) -> Kind {
        match self {
            Self::Applies { kind, .. } => *kind,
            Self::NotApplicable(_) => Kind::Additive,
        }
    }

    /// Whether every body is safe to run again.
    #[must_use]
    pub fn convergent(&self) -> bool {
        match self {
            Self::Applies { convergent, .. } => *convergent,
            Self::NotApplicable(_) => true,
        }
    }

    /// The bodies, empty for `NotApplicable`.
    #[must_use]
    pub fn bodies(&self) -> &[StoreBody<H>] {
        match self {
            Self::Applies { bodies, .. } => bodies,
            Self::NotApplicable(_) => &[],
        }
    }

    /// The kinds of store this migration records rows in, catalog first. The
    /// catalog ledger has a row for every id, including one whose bodies run
    /// only in other stores, so an older binary reading the catalog sees
    /// every id and its kind.
    #[must_use]
    pub fn store_kinds(&self) -> Vec<StoreKind> {
        let mut kinds: Vec<StoreKind> = std::iter::once(StoreKind::Catalog)
            .chain(self.bodies().iter().map(|b| b.store))
            .collect();
        kinds.sort_unstable();
        kinds.dedup();
        kinds
    }
}

/// Refuse a table with an entry declared [`Kind::Additive`]. A binary serves
/// a catalog that is ahead of it by additive migrations only, so an
/// additive declaration is safe only once CI runs the previous release
/// against a catalog this binary migrated (CONTRIBUTING.md, "Adding a
/// catalog migration", step 4). Until that job exists, each backend's tests
/// call this; the PR that adds the job deletes the calls.
///
/// # Errors
///
/// The first id declared additive, with what to do.
#[cfg(any(test, feature = "test-util"))]
pub fn refuse_additive<H: 'static>(
    backend: &str,
    table: impl Fn(super::MigrationId) -> Migration<H>,
) -> Result<(), String> {
    for id in super::MigrationId::ALL {
        let migration = table(id);
        if matches!(migration, Migration::Applies { .. }) && migration.kind() == Kind::Additive {
            return Err(format!(
                "migration {id} is declared Kind::Additive on {backend}, and no CI job runs the \
                 previous release against a catalog this binary migrated yet; declare it \
                 Kind::Breaking (CONTRIBUTING.md, \"Adding a catalog migration\", step 4)"
            ));
        }
    }
    Ok(())
}
