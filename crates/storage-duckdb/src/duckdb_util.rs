// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the DuckDB backend: path normalisation, RFC 3339
//! timestamp conversion, and constraint-violation classification.

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::db;

/// Normalize a configured path or connection string into a path DuckDB opens.
///
/// Accepts the in-memory sentinel `:memory:`, a bare filesystem path, or a
/// `duckdb:` / `duckdb://` URL (the scheme is stripped). Nothing else is
/// interpreted: DuckDB takes a path, not a URL.
pub(crate) fn duckdb_path(path_or_url: &str) -> String {
    if let Some(rest) = path_or_url.strip_prefix("duckdb://") {
        rest.to_owned()
    } else if let Some(rest) = path_or_url.strip_prefix("duckdb:") {
        rest.to_owned()
    } else {
        path_or_url.to_owned()
    }
}

/// Format a timestamp as RFC 3339 UTC, matching the catalog's stored format.
pub(crate) fn format_timestamp(ts: OffsetDateTime) -> String {
    ts.to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .unwrap_or_else(|_| ts.to_string())
}

/// Parse a catalog timestamp string into an `OffsetDateTime`.
///
/// Accepts RFC 3339 (the canonical stored form, e.g. `2026-06-08T12:00:00.123Z`)
/// and tolerates a bare `YYYY-MM-DD HH:MM:SS[.ffffff]` form (assumed UTC), which
/// is what DuckDB produces when a `TIMESTAMP` is cast to text.
pub(crate) fn parse_timestamp(s: &str) -> Result<OffsetDateTime, time::error::Parse> {
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(dt);
    }
    let mut normalized = s.replace(' ', "T");
    if !normalized.ends_with('Z') && !normalized.contains('+') {
        normalized.push('Z');
    }
    OffsetDateTime::parse(&normalized, &Rfc3339)
}

/// True if the error is a DuckDB UNIQUE / PRIMARY KEY constraint violation.
pub(crate) fn is_unique_violation(e: &db::Error) -> bool {
    e.message().is_some_and(|msg| {
        msg.contains("violates primary key constraint")
            || msg.contains("violates unique constraint")
            || msg.contains("Duplicate key")
    })
}

/// True if the error is a DuckDB FOREIGN KEY constraint violation.
///
/// The catalog schema declares no foreign keys (DuckDB cannot cascade deletes,
/// so referential integrity is maintained explicitly by the stores), but the
/// classifier is kept so any constraint that is later added maps correctly.
pub(crate) fn is_fk_violation(e: &db::Error) -> bool {
    e.message()
        .is_some_and(|msg| msg.contains("foreign key constraint"))
}

/// Map a database error to a `StorageError`, classifying transient failures.
///
/// Transient means retry is expected to succeed: a lost worker connection, an
/// I/O failure, or a DuckDB optimistic-concurrency conflict (`Conflict on
/// update`, or a transaction invalidated by another writer). Everything else is
/// `Internal`, which the queue worker treats as poison. The classification errs
/// narrow on purpose: a mis-classified poison row retries forever (visible,
/// bounded to one row), while a mis-classified transient error drops a row
/// silently.
pub(crate) fn map_db_err(e: db::Error) -> extenddb_storage::error::StorageError {
    use extenddb_storage::error::StorageError;
    let transient = match &e {
        db::Error::Worker(_) | db::Error::Connection(_) => true,
        db::Error::Db(duckdb::Error::DuckDBFailure(_, Some(msg))) => {
            msg.contains("Conflict on update")
                || msg.contains("TransactionContext Error")
                || msg.contains("IO Error")
                || msg.contains("Could not set lock")
        }
        _ => false,
    };
    if transient {
        StorageError::Transient(e.to_string())
    } else {
        StorageError::Internal(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duckdb_path_variants() {
        assert_eq!(duckdb_path(":memory:"), ":memory:");
        assert_eq!(duckdb_path("extenddb.duckdb"), "extenddb.duckdb");
        assert_eq!(duckdb_path("duckdb:/var/lib/x.duckdb"), "/var/lib/x.duckdb");
        assert_eq!(
            duckdb_path("duckdb:///var/lib/x.duckdb"),
            "/var/lib/x.duckdb"
        );
    }

    #[test]
    fn timestamp_round_trips_rfc3339() {
        let now = OffsetDateTime::now_utc();
        let s = format_timestamp(now);
        let parsed = parse_timestamp(&s).expect("parse RFC3339");
        assert_eq!(parsed.unix_timestamp(), now.unix_timestamp());
    }

    #[test]
    fn timestamp_parses_bare_datetime() {
        let parsed = parse_timestamp("2026-06-08 12:00:00").expect("parse bare datetime");
        assert_eq!(parsed.year(), 2026);
        assert_eq!(parsed.offset(), time::UtcOffset::UTC);
        let parsed = parse_timestamp("2026-06-08 12:00:00.123456").expect("parse with micros");
        assert_eq!(parsed.millisecond(), 123);
    }
}

#[cfg(test)]
mod map_db_err_tests {
    use super::map_db_err;
    use crate::db;
    use extenddb_storage::error::StorageError;

    #[test]
    fn worker_errors_are_transient() {
        let e = db::Error::Worker("thread died".to_owned());
        assert!(matches!(map_db_err(e), StorageError::Transient(_)));
    }

    #[test]
    fn connection_errors_are_transient() {
        assert!(matches!(
            map_db_err(db::Error::Connection("slot empty".to_owned())),
            StorageError::Transient(_)
        ));
    }

    /// Anything not positively identified as retryable stays Internal, which
    /// the worker treats as poison. Narrow on purpose: a mis-classified poison
    /// row retries forever (visible), a mis-classified transient error drops a
    /// row silently.
    #[test]
    fn unknown_errors_stay_internal() {
        assert!(matches!(
            map_db_err(db::Error::RowNotFound),
            StorageError::Internal(_)
        ));
    }
}
