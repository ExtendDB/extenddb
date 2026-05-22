// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared SQLite error classification helpers.

/// Check if a sqlx error is a unique constraint violation.
pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = e {
        let msg = db_err.message();
        return msg.contains("UNIQUE constraint failed");
    }
    false
}

/// Check if a sqlx error is a foreign key violation.
pub(crate) fn is_fk_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = e {
        let msg = db_err.message();
        return msg.contains("FOREIGN KEY constraint failed");
    }
    false
}

/// Format a `time::OffsetDateTime` as RFC 3339 text for SQLite storage.
pub(crate) fn format_timestamp(dt: time::OffsetDateTime) -> String {
    dt.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| dt.to_string())
}

/// Parse an RFC 3339 timestamp text from SQLite.
pub(crate) fn parse_timestamp(
    s: &str,
) -> Result<time::OffsetDateTime, extenddb_storage::error::StorageError> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .or_else(|_| {
            // Fall back to SQLite's default CURRENT_TIMESTAMP format (no timezone).
            let format = time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]")
                .map_err(|e| e.to_string())?;
            time::PrimitiveDateTime::parse(s, &format)
                .map(|dt| dt.assume_utc())
                .map_err(|e| e.to_string())
        })
        .map_err(|e| extenddb_storage::error::StorageError::Internal(format!("Timestamp parse error: {e}")))
}
