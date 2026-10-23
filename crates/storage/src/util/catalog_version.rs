// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The rule every catalog writer applies before stamping its compiled
//! version over the stored one.

use extenddb_core::version::CatalogVersion;

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

#[cfg(test)]
mod tests {
    use super::*;

    const V4: CatalogVersion = CatalogVersion::new(0, 0, 4);

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
