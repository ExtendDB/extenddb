// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The rule every catalog writer applies before stamping its compiled
//! version over the stored one, and the rule `serve` applies to a stored
//! version string on the backends without a migration ledger.

use extenddb_core::version::CatalogVersion;

use crate::migrations::{CatalogStatus, MigrationError};

/// Refuse to stamp `compiled` over a stored catalog version unless doing so
/// cannot hide a newer schema.
///
/// - absent: proceed (a fresh catalog);
/// - parses and is `<= compiled`: proceed;
/// - parses and is `> compiled`: refused, a newer binary wrote it;
/// - does not parse: refused. A version this binary cannot read is exactly
///   the case the downgrade guard exists for (it may be a scheme a later
///   release introduced), so it is never overwritten.
///
/// Called by the PostgreSQL runner, the SQLite schema apply, and
/// `extenddb migrate`, so the three cannot disagree.
///
/// # Errors
///
/// A message naming the stored value, for the caller to wrap in its own
/// error type.
pub fn check_stored_catalog_version(
    stored: Option<&str>,
    compiled: CatalogVersion,
) -> Result<(), String> {
    let Some(stored) = stored else {
        return Ok(());
    };
    match stored.parse::<CatalogVersion>() {
        Ok(stored_v) if stored_v > compiled => Err(format!(
            "catalog version {stored} is newer than this binary's {compiled}; not downgrading it. \
             Run a binary that expects {stored} or later."
        )),
        Ok(_) => Ok(()),
        Err(e) => Err(format!(
            "stored catalog version {stored:?} is not a version this binary ({compiled}) can \
             read ({e}); refusing to overwrite it. Inspect the 'catalog_version' row in the \
             catalog's settings table: it was written by a binary this one cannot compare \
             itself with, or edited by hand."
        )),
    }
}

/// The rule `serve` applies on a backend whose catalog carries a version
/// string in `settings` (MongoDB, Cassandra) rather than a migration ledger:
/// the stored `catalog_version` must be the one this binary writes, else the
/// server does not start.
///
/// The PostgreSQL and SQLite factories read the ledgers' status instead and
/// refuse a catalog that is behind or ahead; this is the same refusal in the
/// one form those two backends can express. A catalog behind the binary is
/// refused with the command that brings it forward; one ahead of it, or one
/// whose version this binary cannot read, with the advice to run the binary
/// that wrote it. Without the guard a server of this release on an older
/// catalog served table and item operations and failed every backup
/// operation on the collection or column the migration adds, so an operator
/// learned of the pending migration from a client's error.
///
/// `backend` names the backend in the message. The error carries the exit
/// code `serve` ends with: `Uninitialized` (1, with `extenddb init` advice)
/// when there is no version row, else a refusal (1) with the message.
///
/// # Errors
///
/// A [`MigrationError`] for `serve` to print through its catalog-refusal path.
pub fn require_served_catalog_version(
    stored: Option<&str>,
    expected: &str,
    backend: &str,
) -> Result<(), MigrationError> {
    let Some(stored) = stored else {
        return Err(MigrationError::Status(CatalogStatus::Uninitialized));
    };
    if stored == expected {
        return Ok(());
    }
    let expected_v = expected.parse::<CatalogVersion>().map_err(|e| {
        MigrationError::Refused(format!(
            "this binary's expected {backend} catalog version {expected:?} is invalid: {e}"
        ))
    })?;
    match stored.parse::<CatalogVersion>() {
        Ok(stored_v) if stored_v < expected_v => Err(MigrationError::Refused(format!(
            "The {backend} catalog is at version {stored}; this release serves version \
             {expected}. Stop every server built before this release, run \
             `extenddb migrate --yes` once, then start the server."
        ))),
        Ok(_) => Err(MigrationError::Refused(format!(
            "The {backend} catalog is at version {stored}, newer than this binary's {expected}. \
             Run a binary that expects {stored} or later."
        ))),
        Err(e) => Err(MigrationError::Refused(format!(
            "The stored {backend} catalog version {stored:?} is not a version this binary \
             ({expected}) can read ({e}). Inspect the 'catalog_version' row in the catalog's \
             settings: it was written by a binary this one cannot compare itself with, or \
             edited by hand."
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V4: CatalogVersion = CatalogVersion::new(0, 0, 4);

    #[test]
    fn serve_accepts_only_the_version_it_writes() {
        assert!(require_served_catalog_version(Some("0.0.5"), "0.0.5", "MongoDB").is_ok());
        let behind = require_served_catalog_version(Some("0.0.2"), "0.0.5", "MongoDB")
            .unwrap_err()
            .to_string();
        assert!(behind.contains("extenddb migrate --yes"), "{behind}");
        assert!(behind.contains("at version 0.0.2"), "{behind}");
        assert!(behind.contains("MongoDB"), "{behind}");
        let ahead = require_served_catalog_version(Some("0.0.6"), "0.0.5", "Cassandra")
            .unwrap_err()
            .to_string();
        assert!(ahead.contains("newer than this binary"), "{ahead}");
        let unreadable = require_served_catalog_version(Some("five"), "0.0.5", "Cassandra")
            .unwrap_err()
            .to_string();
        assert!(unreadable.contains("cannot compare"), "{unreadable}");
        assert!(matches!(
            require_served_catalog_version(None, "0.0.5", "MongoDB"),
            Err(MigrationError::Status(CatalogStatus::Uninitialized))
        ));
    }

    #[test]
    fn absent_older_and_equal_proceed() {
        assert!(check_stored_catalog_version(None, V4).is_ok());
        assert!(check_stored_catalog_version(Some("0.0.3"), V4).is_ok());
        assert!(check_stored_catalog_version(Some("0.0.4"), V4).is_ok());
    }

    #[test]
    fn newer_is_refused() {
        for v in ["0.0.5", "0.1.0", "1.0.0"] {
            let err = check_stored_catalog_version(Some(v), V4).unwrap_err();
            assert!(err.contains("newer than this binary"), "{err}");
        }
    }

    #[test]
    fn unparseable_is_refused_not_overwritten() {
        for v in [
            "garbage",
            "",
            "0.0",
            "0.0.4.1",
            " 0.0.4",
            "00.0.4",
            "0.0.4-rc1",
        ] {
            let err = check_stored_catalog_version(Some(v), V4).unwrap_err();
            assert!(err.contains("refusing to overwrite"), "{v:?}: {err}");
        }
    }
}
