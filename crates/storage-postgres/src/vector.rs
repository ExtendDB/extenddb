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
//! Detection is one probe at engine construction ([`probe_vector_extension`]),
//! cached for the process lifetime. Installing pgvector on a running server
//! therefore needs an ExtendDB restart to be noticed.

use sqlx::PgPool;

/// Name of the extension that provides the `vector` column type.
pub(crate) const VECTOR_EXTENSION: &str = "vector";

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

}
