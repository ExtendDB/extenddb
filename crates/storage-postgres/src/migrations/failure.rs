// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How one attempt at a body fails, and the runner statements that report a
//! failure that way.
//!
//! The runner classifies an error by the SQLSTATE it carries
//! ([`MigrationError::sqlstate`]), which every code body and runner
//! statement keeps through [`super::connect::sql_error`]. Its text is never
//! read: a server whose `lc_messages` names another language translates it.

use extenddb_storage::migrations::MigrationError;
use sqlx::PgPool;

use super::connect::sql_error;
use crate::data::SQLSTATE_LOCK_NOT_AVAILABLE as LOCK_NOT_AVAILABLE;

/// SQLSTATE `insufficient_privilege`, which altering a table the role does
/// not own raises.
const INSUFFICIENT_PRIVILEGE: &str = "42501";

/// Why one attempt at a body failed.
pub(super) enum Failure {
    /// A lock wait passed `lock_timeout`: the body runs again.
    LockTimeout(MigrationError),
    /// The role may not alter an object: the stop names the owners.
    Privilege(MigrationError),
    Other(MigrationError),
}

impl Failure {
    pub(super) fn sqlx(context: &str, e: &sqlx::Error) -> Self {
        sql_error(context, e).into()
    }

    /// The error the attempt failed with.
    pub(super) fn into_error(self) -> MigrationError {
        match self {
            Self::LockTimeout(e) | Self::Privilege(e) | Self::Other(e) => e,
        }
    }
}

impl From<MigrationError> for Failure {
    fn from(e: MigrationError) -> Self {
        match e.sqlstate() {
            Some(LOCK_NOT_AVAILABLE) => Self::LockTimeout(e),
            Some(INSUFFICIENT_PRIVILEGE) => Self::Privilege(e),
            _ => Self::Other(e),
        }
    }
}

pub(super) async fn exec(pool: &PgPool, sql: &str) -> Result<(), Failure> {
    sqlx::raw_sql(sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| Failure::sqlx(sql, &e))
}

pub(super) async fn current_txid(pool: &PgPool) -> Result<i64, Failure> {
    sqlx::query_scalar("SELECT txid_current()")
        .fetch_one(pool)
        .await
        .map_err(|e| Failure::sqlx("read the transaction id", &e))
}

#[cfg(test)]
mod tests {
    use extenddb_storage::migrations::MigrationError;

    use super::Failure;

    fn coded(sqlstate: &str) -> MigrationError {
        MigrationError::coded(
            "a statement",
            "an error in any language",
            Some(sqlstate.into()),
        )
    }

    #[test]
    fn a_failure_is_classified_by_its_sqlstate() {
        assert!(matches!(
            Failure::from(coded("55P03")),
            Failure::LockTimeout(_)
        ));
        assert!(matches!(
            Failure::from(coded("42501")),
            Failure::Privilege(_)
        ));
        assert!(matches!(Failure::from(coded("23505")), Failure::Other(_)));
        // The text of a lock timeout without its code is not one.
        let text = MigrationError::Store("canceling statement due to lock timeout".into());
        assert!(matches!(Failure::from(text), Failure::Other(_)));
        // A stopped run keeps its error's code.
        let failure = Failure::from(coded("55P03")).into_error();
        assert_eq!(failure.sqlstate(), Some("55P03"));
    }
}
