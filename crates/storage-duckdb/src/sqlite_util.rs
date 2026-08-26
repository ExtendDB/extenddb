// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the DuckDB backend: connection-URL building, RFC 3339
//! timestamp conversion, and constraint-violation classification.

use crate::db;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// Normalize a configured path or connection string into a sqlx-openable
/// DuckDB URL.
///
/// Accepts a value that is already a `duckdb:` URL (returned unchanged), the
/// in-memory sentinel `:memory:`, or a filesystem path (wrapped as a
/// read-write-create file URL).
pub(crate) fn duckdb_path(path_or_url: &str) -> String {
    if path_or_url.starts_with("duckdb:") {
        path_or_url.to_owned()
    } else if path_or_url == ":memory:" {
        "duckdb::memory:".to_owned()
    } else {
        format!("duckdb://{path_or_url}?mode=rwc")
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
/// and tolerates DuckDB's bare `datetime()` form (`YYYY-MM-DD HH:MM:SS`, assumed
/// UTC) for robustness against rows written by older tooling.
pub(crate) fn parse_timestamp(s: &str) -> Result<OffsetDateTime, time::error::Parse> {
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(dt);
    }
    // Fallback: "YYYY-MM-DD HH:MM:SS" (DuckDB datetime('now')), interpret as UTC.
    let normalized = format!("{}Z", s.replace(' ', "T"));
    OffsetDateTime::parse(&normalized, &Rfc3339)
}

/// True if the error is a DuckDB UNIQUE / PRIMARY KEY constraint violation.
pub(crate) fn is_unique_violation(e: &db::Error) -> bool {
    matches!(e, db::Error::Database(db) if {
        let msg = db.message();
        msg.contains("UNIQUE constraint failed") || msg.contains("PRIMARY KEY constraint failed")
    })
}

/// True if the error is a DuckDB FOREIGN KEY constraint violation.
pub(crate) fn is_fk_violation(e: &db::Error) -> bool {
    matches!(e, db::Error::Database(db) if db.message().contains("FOREIGN KEY constraint failed"))
}

#[cfg(test)]
mod tests {
    use crate::db;
    use super::*;

    #[test]
    fn duckdb_path_variants() {
        assert_eq!(duckdb_path(":memory:"), "duckdb::memory:");
        assert_eq!(
            duckdb_path("extenddb.duckdb"),
            "duckdb://extenddb.duckdb?mode=rwc"
        );
        assert_eq!(
            duckdb_path("duckdb://already.db?mode=rwc"),
            "duckdb://already.db?mode=rwc"
        );
    }

    #[test]
    fn timestamp_round_trips_rfc3339() {
        let now = OffsetDateTime::now_utc();
        let s = format_timestamp(now);
        let parsed = parse_timestamp(&s).expect("parse RFC3339");
        // Equal to at least second precision.
        assert_eq!(parsed.unix_timestamp(), now.unix_timestamp());
    }

    #[test]
    fn timestamp_parses_bare_datetime() {
        let parsed = parse_timestamp("2026-06-08 12:00:00").expect("parse bare datetime");
        assert_eq!(parsed.year(), 2026);
        assert_eq!(parsed.offset(), time::UtcOffset::UTC);
    }
}

/// Map a sqlx error to a `StorageError`, classifying transient failures.
///
/// Transient means retry is expected to succeed: connection/pool trouble, I/O
/// errors, and DuckDB's BUSY (5) / LOCKED (6) result codes, including their
/// extended forms (`code & 0xff`). Everything else is `Internal`, which the
/// queue worker treats as poison. The classification errs narrow on purpose: a
/// mis-classified poison row retries forever (visible, bounded to one row),
/// while a mis-classified transient error drops a row silently.
pub(crate) fn map_db_err(e: db::Error) -> extenddb_storage::error::StorageError {
    use extenddb_storage::error::StorageError;
    let transient = match &e {
        db::Error::Io(_) | db::Error::PoolTimedOut | db::Error::WorkerCrashed => true,
        db::Error::Database(db) => db
            .code()
            .and_then(|c| c.parse::<i64>().ok())
            .is_some_and(|code| matches!(code & 0xff, 5 | 6)),
        _ => false,
    };
    if transient {
        StorageError::Transient(e.to_string())
    } else {
        StorageError::Internal(e.to_string())
    }
}

#[cfg(test)]
mod map_db_err_tests {
    use crate::db;
    use super::map_db_err;
    use extenddb_storage::error::StorageError;

    #[test]
    fn io_errors_are_transient() {
        let e = db::Error::Io(std::io::Error::other("disk hiccup"));
        assert!(matches!(map_db_err(e), StorageError::Transient(_)));
    }

    #[test]
    fn pool_timeout_is_transient() {
        assert!(matches!(
            map_db_err(db::Error::PoolTimedOut),
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
