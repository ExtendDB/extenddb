// SPDX-License-Identifier: Apache-2.0

//! The `application_name` the migrate command sets on the PostgreSQL sessions
//! it opens through the configured connection string.

/// `application_name` the migrate command carries on every PostgreSQL session
/// it opens. The exclusive-access check for `004_escape_control_chars`
/// (`extenddb-storage-postgres`, `migrations::MIGRATE_APPLICATION_NAME`) counts
/// every session on the catalog and data databases except those with this
/// name. The bootstrapper sets it on its own pools; the catalog stores opened
/// by `CatalogResourceTagStep` connect through the configured connection
/// string and would otherwise be counted as foreign sessions, which makes
/// `migrate --yes` refuse every upgrade from a release without that migration.
const MIGRATE_APPLICATION_NAME: &str = "extenddb-migrate";

/// `connection` with `application_name=extenddb-migrate` added to its query
/// string when it is a PostgreSQL URL. Other backends' connection strings and a
/// URL that already names an application are returned unchanged.
pub(crate) fn with_migrate_application_name(connection: &str) -> String {
    let is_postgres =
        connection.starts_with("postgres://") || connection.starts_with("postgresql://");
    if !is_postgres || connection.contains("application_name=") {
        return connection.to_owned();
    }
    let separator = if connection.contains('?') { '&' } else { '?' };
    format!("{connection}{separator}application_name={MIGRATE_APPLICATION_NAME}")
}

#[cfg(test)]
mod application_name_tests {
    use super::with_migrate_application_name;

    #[test]
    fn a_postgres_url_without_a_query_string_gets_the_name() {
        assert_eq!(
            with_migrate_application_name("postgresql://app:pw@db:5432/extenddb_catalog"),
            "postgresql://app:pw@db:5432/extenddb_catalog?application_name=extenddb-migrate"
        );
        assert_eq!(
            with_migrate_application_name("postgres://db/cat"),
            "postgres://db/cat?application_name=extenddb-migrate"
        );
    }

    #[test]
    fn a_postgres_url_with_a_query_string_gets_the_name_appended() {
        assert_eq!(
            with_migrate_application_name("postgresql://db/cat?sslmode=require"),
            "postgresql://db/cat?sslmode=require&application_name=extenddb-migrate"
        );
    }

    #[test]
    fn an_existing_application_name_is_kept() {
        let given = "postgresql://db/cat?application_name=ops";
        assert_eq!(with_migrate_application_name(given), given);
    }

    #[test]
    fn other_backends_are_unchanged() {
        for given in [
            "/var/lib/extenddb/extenddb.sqlite",
            "mongodb://127.0.0.1:27017/?replicaSet=rs0",
            "cassandra://127.0.0.1:9042",
        ] {
            assert_eq!(with_migrate_application_name(given), given);
        }
    }
}
