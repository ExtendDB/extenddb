// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB cluster topology validation.

use extenddb_storage::error::StorageError;
use sqlx::MySqlPool;

use crate::config::parse_connection_string;

#[derive(Debug, Clone, Eq, PartialEq)]
enum ClusterFingerprint {
    Visible(Vec<String>),
    Unavailable(String),
}

pub(crate) async fn validate_catalog_data_same_cluster(
    catalog_pool: &MySqlPool,
    catalog_connection_string: &str,
    data_pool: &MySqlPool,
    data_connection_string: &str,
) -> Result<(), StorageError> {
    let same_sql_login = same_sql_login(catalog_connection_string, data_connection_string)?;
    let catalog_fingerprint = read_cluster_fingerprint(catalog_pool).await?;
    let data_fingerprint = read_cluster_fingerprint(data_pool).await?;

    validate_cluster_fingerprints(&catalog_fingerprint, &data_fingerprint, same_sql_login)
}

async fn read_cluster_fingerprint(pool: &MySqlPool) -> Result<ClusterFingerprint, StorageError> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT CONCAT(COALESCE(`TYPE`, ''), '\t', \
                       COALESCE(`INSTANCE`, ''), '\t', \
                       COALESCE(`STATUS_ADDRESS`, '')) \
           FROM information_schema.cluster_info \
          ORDER BY `TYPE`, `INSTANCE`, `STATUS_ADDRESS`",
    )
    .fetch_all(pool)
    .await;

    match rows {
        Ok(rows) if rows.is_empty() => Err(StorageError::Configuration(
            "TiDB information_schema.cluster_info returned no rows; cannot validate catalog/data cluster topology".to_owned(),
        )),
        Ok(rows) => Ok(ClusterFingerprint::Visible(rows)),
        Err(error) if cluster_info_unavailable(&error) => {
            Ok(ClusterFingerprint::Unavailable(database_error_text(&error)))
        }
        Err(error) if matches!(error, sqlx::Error::Database(_)) => {
            Err(StorageError::Configuration(format!(
                "failed to read TiDB information_schema.cluster_info: {error}"
            )))
        }
        Err(error) => Err(StorageError::Connection(error.to_string())),
    }
}

fn validate_cluster_fingerprints(
    catalog: &ClusterFingerprint,
    data: &ClusterFingerprint,
    same_sql_login: bool,
) -> Result<(), StorageError> {
    match (catalog, data) {
        (ClusterFingerprint::Visible(catalog_rows), ClusterFingerprint::Visible(data_rows))
            if catalog_rows == data_rows =>
        {
            Ok(())
        }
        (ClusterFingerprint::Visible(catalog_rows), ClusterFingerprint::Visible(data_rows)) => {
            Err(StorageError::Configuration(format!(
                "TiDB catalog and data databases must be in the same cluster; catalog topology differs from data topology (catalog={}, data={})",
                summarize_fingerprint(catalog_rows),
                summarize_fingerprint(data_rows)
            )))
        }
        (
            ClusterFingerprint::Unavailable(catalog_reason),
            ClusterFingerprint::Unavailable(data_reason),
        ) if same_sql_login => {
            tracing::warn!(
                catalog_reason,
                data_reason,
                "TiDB cluster topology metadata is unavailable; accepting catalog/data split because both databases use the same SQL endpoint and user"
            );
            Ok(())
        }
        (
            ClusterFingerprint::Unavailable(catalog_reason),
            ClusterFingerprint::Unavailable(data_reason),
        ) => Err(StorageError::Configuration(format!(
            "TiDB catalog and data databases must be in the same cluster; information_schema.cluster_info is unavailable for both connections and the SQL endpoints or users differ (catalog: {catalog_reason}; data: {data_reason})"
        ))),
        (ClusterFingerprint::Unavailable(reason), ClusterFingerprint::Visible(_)) => {
            Err(StorageError::Configuration(format!(
                "TiDB catalog and data databases must be in the same cluster; catalog connection cannot read information_schema.cluster_info but data connection can ({reason})"
            )))
        }
        (ClusterFingerprint::Visible(_), ClusterFingerprint::Unavailable(reason)) => {
            Err(StorageError::Configuration(format!(
                "TiDB catalog and data databases must be in the same cluster; data connection cannot read information_schema.cluster_info but catalog connection can ({reason})"
            )))
        }
    }
}

fn same_sql_login(left: &str, right: &str) -> Result<bool, StorageError> {
    let left = parse_connection_string(left)
        .map_err(|error| StorageError::Configuration(error.to_string()))?;
    let right = parse_connection_string(right)
        .map_err(|error| StorageError::Configuration(error.to_string()))?;

    Ok(left.host.eq_ignore_ascii_case(&right.host)
        && left.port == right.port
        && left.user == right.user)
}

fn cluster_info_unavailable(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db_error) = error else {
        return false;
    };

    let message = db_error.message().to_ascii_lowercase();
    db_error
        .code()
        .is_some_and(|code| matches!(code.as_ref(), "1044" | "1142" | "1146" | "1227"))
        || ((message.contains("cluster_info")
            || message.contains("information_schema.cluster_info"))
            && (message.contains("not available")
                || message.contains("doesn't exist")
                || message.contains("does not exist")
                || message.contains("access denied")
                || message.contains("denied")
                || message.contains("privilege")))
}

fn database_error_text(error: &sqlx::Error) -> String {
    match error {
        sqlx::Error::Database(db_error) => db_error.message().to_owned(),
        _ => error.to_string(),
    }
}

fn summarize_fingerprint(rows: &[String]) -> String {
    const MAX_ROWS: usize = 5;

    let preview = rows
        .iter()
        .take(MAX_ROWS)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    if rows.len() <= MAX_ROWS {
        format!("[{preview}]")
    } else {
        format!("[{preview}, ... {} more]", rows.len() - MAX_ROWS)
    }
}

#[cfg(test)]
mod tests {
    use extenddb_storage::error::StorageError;

    use super::{ClusterFingerprint, same_sql_login, validate_cluster_fingerprints};

    fn visible(rows: &[&str]) -> ClusterFingerprint {
        ClusterFingerprint::Visible(rows.iter().map(|row| (*row).to_owned()).collect())
    }

    fn unavailable(reason: &str) -> ClusterFingerprint {
        ClusterFingerprint::Unavailable(reason.to_owned())
    }

    #[test]
    fn validates_matching_native_topology() {
        let fingerprint = visible(&[
            "pd\t127.0.0.1:2379\t127.0.0.1:2379",
            "tidb\t127.0.0.1:4000\t127.0.0.1:10080",
            "tikv\t127.0.0.1:20160\t127.0.0.1:20180",
        ]);

        validate_cluster_fingerprints(&fingerprint, &fingerprint, false)
            .expect("same native topology should validate");
    }

    #[test]
    fn rejects_different_native_topology() {
        let catalog = visible(&["pd\t127.0.0.1:2379\t127.0.0.1:2379"]);
        let data = visible(&["pd\t127.0.0.2:2379\t127.0.0.2:2379"]);

        let err = validate_cluster_fingerprints(&catalog, &data, true).unwrap_err();

        assert!(matches!(err, StorageError::Configuration(_)));
        assert!(err.to_string().contains("topology differs"));
    }

    #[test]
    fn accepts_unavailable_topology_only_for_same_sql_login() {
        validate_cluster_fingerprints(
            &unavailable("not available"),
            &unavailable("not available"),
            true,
        )
        .expect("same SQL login is the only safe fallback when topology metadata is hidden");
    }

    #[test]
    fn rejects_unavailable_topology_for_different_sql_endpoints() {
        let err = validate_cluster_fingerprints(
            &unavailable("not available"),
            &unavailable("not available"),
            false,
        )
        .unwrap_err();

        assert!(matches!(err, StorageError::Configuration(_)));
        assert!(err.to_string().contains("SQL endpoints or users differ"));
    }

    #[test]
    fn rejects_mixed_topology_visibility() {
        let err = validate_cluster_fingerprints(
            &unavailable("access denied"),
            &visible(&["pd\t127.0.0.1:2379\t127.0.0.1:2379"]),
            true,
        )
        .unwrap_err();

        assert!(matches!(err, StorageError::Configuration(_)));
        assert!(err.to_string().contains("catalog connection cannot read"));
    }

    #[test]
    fn compares_sql_login_without_database_name() {
        assert!(
            same_sql_login(
                "mysql://user:pass@example.com:4000/extenddb_catalog",
                "mysql://user:pass@example.com:4000/extenddb_data"
            )
            .expect("valid connection strings")
        );
        assert!(
            !same_sql_login(
                "mysql://user:pass@example.com:4000/extenddb_catalog",
                "mysql://user:pass@other.example.com:4000/extenddb_data"
            )
            .expect("valid connection strings")
        );
        assert!(
            !same_sql_login(
                "mysql://catalog_user:pass@example.com:4000/extenddb_catalog",
                "mysql://data_user:pass@example.com:4000/extenddb_data"
            )
            .expect("valid connection strings")
        );
    }
}
