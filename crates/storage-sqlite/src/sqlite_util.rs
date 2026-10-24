// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the SQLite backend: connection-URL building, RFC 3339
//! timestamp conversion, and constraint-violation classification.

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// Normalize a configured path or connection string into a sqlx-openable
/// SQLite URL.
///
/// Accepts a value that is already a `sqlite:` URL (returned unchanged), the
/// in-memory sentinel `:memory:`, or a filesystem path (wrapped as a
/// read-write-create file URL).
pub(crate) fn sqlite_url(path_or_url: &str) -> String {
    if path_or_url.starts_with("sqlite:") {
        path_or_url.to_owned()
    } else if path_or_url == ":memory:" {
        "sqlite::memory:".to_owned()
    } else {
        format!("sqlite://{path_or_url}?mode=rwc")
    }
}

/// Format a timestamp as RFC 3339 UTC, matching the catalog's stored format.
pub(crate) fn format_timestamp(ts: OffsetDateTime) -> String {
    ts.to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .unwrap_or_else(|_| ts.to_string())
}

/// Format epoch seconds as a catalog timestamp with microsecond precision
/// (`2026-10-15T12:00:00.438000Z`). Fixed width, so stored values sort as
/// text in time order.
///
/// Rounded to the microsecond before conversion: epoch seconds near 1.8e9
/// carry about 0.24 us of f64 precision, so finer digits would be noise.
pub(crate) fn format_epoch_seconds(secs: f64) -> String {
    #[allow(clippy::cast_possible_truncation)] // saturating; out of range falls back below
    let micros = (secs * 1e6).round() as i128;
    let dt = OffsetDateTime::from_unix_timestamp_nanos(micros * 1000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
        dt.year(),
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
        dt.microsecond()
    )
}

/// Format epoch milliseconds in the shape the `stream_records.created_at`
/// column default produces (`strftime('%Y-%m-%dT%H:%M:%fZ')`, millisecond
/// precision, 24 characters): `2026-10-15T12:00:00.438Z`. Same width as the
/// default, so rows written before and after this formatter was introduced
/// sort together as text in time order.
pub(crate) fn format_epoch_millis(ms: i64) -> String {
    let dt = OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        dt.year(),
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
        dt.millisecond()
    )
}

/// Epoch seconds, sub-second precision, of a catalog timestamp; 0 if it does
/// not parse.
#[allow(clippy::cast_precision_loss)] // epoch seconds fit f64's mantissa
pub(crate) fn timestamp_epoch_seconds(s: &str) -> f64 {
    parse_timestamp(s).map_or(0.0, |dt| {
        dt.unix_timestamp() as f64 + f64::from(dt.nanosecond()) / 1e9
    })
}

/// Parse a catalog timestamp string into an `OffsetDateTime`.
///
/// Accepts RFC 3339 (the canonical stored form, e.g. `2026-06-08T12:00:00.123Z`)
/// and tolerates SQLite's bare `datetime()` form (`YYYY-MM-DD HH:MM:SS`, assumed
/// UTC) for robustness against rows written by older tooling.
pub(crate) fn parse_timestamp(s: &str) -> Result<OffsetDateTime, time::error::Parse> {
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(dt);
    }
    // Fallback: "YYYY-MM-DD HH:MM:SS" (SQLite datetime('now')), interpret as UTC.
    let normalized = format!("{}Z", s.replace(' ', "T"));
    OffsetDateTime::parse(&normalized, &Rfc3339)
}

/// True if the error is a SQLite UNIQUE / PRIMARY KEY constraint violation.
pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if {
        let msg = db.message();
        msg.contains("UNIQUE constraint failed") || msg.contains("PRIMARY KEY constraint failed")
    })
}

/// True if the error is a SQLite FOREIGN KEY constraint violation.
pub(crate) fn is_fk_violation(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.message().contains("FOREIGN KEY constraint failed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_url_variants() {
        assert_eq!(sqlite_url(":memory:"), "sqlite::memory:");
        assert_eq!(
            sqlite_url("extenddb.sqlite"),
            "sqlite://extenddb.sqlite?mode=rwc"
        );
        assert_eq!(
            sqlite_url("sqlite://already.db?mode=rwc"),
            "sqlite://already.db?mode=rwc"
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
/// errors, and SQLite's BUSY (5) / LOCKED (6) result codes, including their
/// extended forms (`code & 0xff`). Everything else is `Internal`, which the
/// queue worker treats as poison. The classification errs narrow on purpose: a
/// mis-classified poison row retries forever (visible, bounded to one row),
/// while a mis-classified transient error drops a row silently.
pub(crate) fn map_sqlx_err(e: sqlx::Error) -> extenddb_storage::error::StorageError {
    use extenddb_storage::error::StorageError;
    let transient = match &e {
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(db) => db
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
mod map_sqlx_err_tests {
    use super::map_sqlx_err;
    use extenddb_storage::error::StorageError;

    #[test]
    fn io_errors_are_transient() {
        let e = sqlx::Error::Io(std::io::Error::other("disk hiccup"));
        assert!(matches!(map_sqlx_err(e), StorageError::Transient(_)));
    }

    #[test]
    fn pool_timeout_is_transient() {
        assert!(matches!(
            map_sqlx_err(sqlx::Error::PoolTimedOut),
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
            map_sqlx_err(sqlx::Error::RowNotFound),
            StorageError::Internal(_)
        ));
    }
}
