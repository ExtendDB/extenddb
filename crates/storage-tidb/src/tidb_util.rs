// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared TiDB error classification helpers.

/// Check if a sqlx error is a unique constraint violation (MySQL/TiDB code 1062).
pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = e {
        return db_err.kind() == sqlx::error::ErrorKind::UniqueViolation;
    }
    false
}

/// Check if a sqlx error is a foreign key violation (MySQL/TiDB code 1451/1452).
pub(crate) fn is_fk_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = e {
        return db_err.kind() == sqlx::error::ErrorKind::ForeignKeyViolation;
    }
    false
}
