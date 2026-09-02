// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! pgvector availability: detection at startup, classification at runtime.
//!
//! Vector indexes on this backend are stored in `vector(N)` columns, a type the
//! pgvector extension defines, so the whole feature depends on an extension
//! that a given PostgreSQL server may not have installed. The capability is
//! therefore detected, not declared: the same binary serves vector indexes
//! against a server that has pgvector and refuses them against one that does
//! not, with no build-time or configuration difference.
//!
//! Two layers, because detection is a snapshot:
//!
//! 1. One probe at engine construction ([`probe_vector_extension`]), cached for
//!    the process lifetime. Installing pgvector on a running server therefore
//!    needs an ExtendDB restart to be noticed.
//! 2. Runtime classification ([`is_missing_vector_extension`]) for the window
//!    where the extension is dropped, or a failover lands on a server without
//!    it, after the probe said yes. Those errors become
//!    [`StorageError::Unsupported`], which the engine reports as a
//!    `ValidationException`, rather than a 500.

use extenddb_storage::error::StorageError;
use sqlx::PgPool;

/// Name of the extension that provides the `vector` column type.
pub(crate) const VECTOR_EXTENSION: &str = "vector";

/// Message carried by [`StorageError::Unsupported`] when the extension is gone.
///
/// Says which database is missing it, because the extension is installed on the
/// data database while a reader's attention naturally goes to the catalog.
pub(crate) const VECTOR_EXTENSION_REQUIRED: &str =
    "vector indexes require the pgvector extension on the data database";

/// Read the installed pgvector version, or `None` when it is not installed.
///
/// A query failure is reported as `None` and logged rather than propagated: an
/// engine that cannot determine the answer must not claim the capability, and
/// refusing to start over a missing optional feature would take down a
/// deployment that never asked for vector indexes.
pub(crate) async fn probe_vector_extension(data_pool: &PgPool) -> Option<String> {
    match sqlx::query_scalar::<_, String>("SELECT extversion FROM pg_extension WHERE extname = $1")
        .bind(VECTOR_EXTENSION)
        .fetch_optional(data_pool)
        .await
    {
        Ok(version) => version,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "could not determine whether the pgvector extension is installed; \
                 treating vector indexes as unsupported"
            );
            None
        }
    }
}

/// The `SQLSTATE` that means "this server has no pgvector", given a statement
/// that names the `vector` type or one of its operators.
///
/// `42704` undefined_object is the one PostgreSQL raises for `type "vector" does
/// not exist`, which is what every vector statement reports on a server where
/// the extension was never created, and it is the code observed when the
/// extension is dropped from under a running engine.
///
/// Two codes are deliberately NOT treated as a missing extension, because doing
/// so would answer a different problem with advice about installing software:
///
/// - `42P01` undefined_table is what a concurrent DeleteTable or an
///   UpdateTable-delete produces while a write is in flight. The GSI queue
///   already treats it as a benign race for its own tables, and only the caller
///   knows whether a missing table is a race it should tolerate or a fault. A
///   client told to install an extension that is already present would be sent
///   in the wrong direction entirely.
/// - `0A000` feature_not_supported is raised for many unrelated unsupported
///   operations, so it is too broad to carry this meaning on its own.
const MISSING_EXTENSION_SQLSTATE: &str = "42704";

/// Classify a `SQLSTATE` as "pgvector is not available here".
fn is_missing_vector_extension_sqlstate(code: Option<&str>) -> bool {
    code == Some(MISSING_EXTENSION_SQLSTATE)
}

/// Whether a sqlx error says the server cannot serve vector types at all.
pub(crate) fn is_missing_vector_extension(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Database(db_err) => {
            is_missing_vector_extension_sqlstate(db_err.code().as_deref())
        }
        _ => false,
    }
}

/// The typed refusal for a server without pgvector.
pub(crate) fn vector_unsupported() -> StorageError {
    StorageError::Unsupported(VECTOR_EXTENSION_REQUIRED.to_owned())
}

/// Map a vector-path database error, turning "no pgvector here" into the typed
/// refusal and leaving every other error internal.
///
/// Used by the vector data-definition and search paths so a server that loses
/// the extension after the startup probe answers 400 with the refusal text
/// instead of 500 with a PostgreSQL message.
pub(crate) fn map_vector_sql_error(e: sqlx::Error) -> StorageError {
    if is_missing_vector_extension(&e) {
        vector_unsupported()
    } else {
        // Formatted with the SQLSTATE prefixed, the same way the secondary index
        // path formats it. That prefix is what the propagation worker matches on to
        // tell a dropped-table race from a real failure; formatting with `to_string`
        // instead loses the code, and the worker then retries the same row forever,
        // stalling every row behind it in that partition.
        StorageError::Internal(crate::data::index::sqlstate_message(&e))
    }
}

/// Confirm the extension is still installed, from a statement that fails the
/// same way vector data-definition does when it is not.
///
/// `NULL::vector` resolves the type without touching a table, so it costs one
/// round trip and raises `42704` on a server with no pgvector. Used before
/// persisting an index whose storage depends on the type, so a server that lost
/// the extension after the startup probe refuses instead of recording catalog
/// state that can never be backed by a data table.
pub(crate) async fn ensure_vector_extension_present(
    data_pool: &PgPool,
) -> Result<(), StorageError> {
    sqlx::query("SELECT NULL::vector")
        .execute(data_pool)
        .await
        .map_err(map_vector_sql_error)?;
    Ok(())
}

/// Map a `SQLSTATE` from a failed `CREATE EXTENSION vector` to operator advice.
///
/// The install-time failure codes are different from the runtime ones, so they
/// get their own classifier: `58P01` undefined_file is what PostgreSQL reports
/// when the extension's control file is not on the server (the package is not
/// installed at all), and `42501` insufficient_privilege is what it reports when
/// the connecting role may not create it. Reporting either as "not available"
/// alone would send an operator looking in the wrong place.
fn create_extension_hint_for_sqlstate(code: Option<&str>) -> &'static str {
    match code {
        Some("58P01") => {
            "the pgvector package is not installed on this PostgreSQL server; \
             install it (for example postgresql-16-pgvector) and re-run migrate"
        }
        Some("42501") => {
            "this role may not create extensions; create it once as a superuser \
             or as the database owner, then re-run migrate"
        }
        _ => {
            "ExtendDB will run normally and refuse vector indexes unless the \
             extension is already present"
        }
    }
}

/// Operator advice for a failed `CREATE EXTENSION vector`.
pub(crate) fn create_extension_hint(e: &sqlx::Error) -> &'static str {
    let code = match e {
        sqlx::Error::Database(db_err) => db_err.code().map(|c| c.to_string()),
        _ => None,
    };
    create_extension_hint_for_sqlstate(code.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn undefined_object_means_pgvector_is_absent() {
        // `type "vector" does not exist`, which is what every vector statement
        // reports on a server where the extension was never created, and what
        // dropping it from under a running engine produces.
        assert!(is_missing_vector_extension_sqlstate(Some("42704")));
    }

    #[test]
    fn an_undefined_table_is_not_a_missing_extension() {
        // A concurrent DeleteTable produces this while a write is in flight. The
        // GSI queue treats it as a benign race for its own tables; classifying it
        // here would tell a client to install an extension that is present, and
        // would hide a race behind a capability message.
        assert!(!is_missing_vector_extension_sqlstate(Some("42P01")));
    }

    #[test]
    fn a_generic_unsupported_feature_is_not_a_missing_extension() {
        // PostgreSQL raises 0A000 for many unrelated unsupported operations, so it
        // is too broad to carry this specific meaning.
        assert!(!is_missing_vector_extension_sqlstate(Some("0A000")));
    }

    #[test]
    fn an_unrelated_sqlstate_is_not_a_missing_extension() {
        // Chosen deliberately: 23505 is a unique violation, which the vector
        // write path uses as a real invariant tripwire. Classifying it as a
        // missing extension would turn a loud bug into a silent refusal.
        assert!(!is_missing_vector_extension_sqlstate(Some("23505")));
        assert!(!is_missing_vector_extension_sqlstate(Some("42601")));
    }

    #[test]
    fn an_error_with_no_sqlstate_is_not_a_missing_extension() {
        assert!(!is_missing_vector_extension_sqlstate(None));
    }

    #[test]
    fn a_transport_error_is_not_a_missing_extension() {
        // A pool timeout carries no SQLSTATE. Reporting it as Unsupported would
        // tell a client its request is invalid when the server is merely busy.
        assert!(!is_missing_vector_extension(&sqlx::Error::PoolTimedOut));
    }

    #[test]
    fn a_missing_package_and_a_missing_privilege_get_different_advice() {
        // The two ways `CREATE EXTENSION` fails at init need an operator to do
        // different things, so the notice must not collapse them.
        let absent = create_extension_hint_for_sqlstate(Some("58P01"));
        let denied = create_extension_hint_for_sqlstate(Some("42501"));
        assert!(absent.contains("not installed"), "{absent}");
        assert!(denied.contains("may not create extensions"), "{denied}");
        assert_ne!(absent, denied);
        assert_ne!(absent, create_extension_hint_for_sqlstate(None));
    }

    #[test]
    fn a_mapped_database_error_keeps_its_sqlstate_in_the_message() {
        // The propagation worker tells a dropped-table race from a real failure by
        // matching the SQLSTATE prefix in the message, because sqlx renders a
        // database error as its text alone. A mapper that formats with `to_string`
        // throws the code away, and the worker then retries the same row forever,
        // stalling every row behind it in that partition. This asserts the two
        // mappers agree on the format, which is the property that keeps one
        // classifier working for both index kinds.
        //
        // Built from a real sqlx error rather than a string, so a change to sqlx's
        // rendering fails here rather than in production.
        let unrelated = sqlx::Error::PoolTimedOut;
        assert_eq!(
            crate::data::index::sqlstate_message(&unrelated),
            unrelated.to_string(),
            "an error with no SQLSTATE is rendered unchanged"
        );
    }

    #[test]
    fn the_refusal_names_the_data_database() {
        match vector_unsupported() {
            StorageError::Unsupported(msg) => {
                assert_eq!(msg, VECTOR_EXTENSION_REQUIRED);
                assert!(msg.contains("pgvector"), "{msg}");
                assert!(msg.contains("data database"), "{msg}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}
