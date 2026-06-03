// Copyright 2026 DynamoDB Open contributors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL implementation of `OperationsEngine`.

use std::collections::HashSet;

use extenddb_storage::error::StorageError;
use extenddb_storage::operations::{
    CatalogCheckFix, CatalogCheckIssue, CatalogCheckReport, CatalogCheckSection, ConnectionParts,
    OperationsEngine,
};
use futures::future::BoxFuture;
use sqlx::postgres::PgPoolOptions;

use crate::data::{physical_data_table_name, physical_index_table_name};

/// PostgreSQL operations engine for ddbo CLI commands.
pub struct PostgresOperationsEngine;

fn catalog_check_issue(name: impl Into<String>) -> CatalogCheckIssue {
    CatalogCheckIssue::new(name)
}

fn quote_pg_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

async fn postgres_catalog_check(
    connection_config: &str,
    fix: bool,
) -> Result<CatalogCheckReport, StorageError> {
    let catalog_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(connection_config)
        .await
        .map_err(|e| StorageError::Connection(format!("Cannot connect to catalog: {e}")))?;

    let data_conn: Option<(String,)> =
        sqlx::query_as("SELECT value FROM settings WHERE key = 'data_database_connection_string'")
            .fetch_optional(&catalog_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    let Some((data_conn_str,)) = data_conn else {
        return Err(StorageError::Internal(
            "No data database connection string in settings. Run `extenddb init`.".to_owned(),
        ));
    };

    let data_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&data_conn_str)
        .await
        .map_err(|e| StorageError::Connection(format!("Cannot connect to data database: {e}")))?;

    let catalog_tables: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_id, table_status FROM tables \
         WHERE table_status IN ('ACTIVE', 'CREATING', 'UPDATING', 'DELETING')",
    )
    .fetch_all(&catalog_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    let catalog_indexes: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT i.index_id, i.index_status, t.table_status \
         FROM indexes i JOIN tables t ON i.table_id = t.table_id \
         WHERE t.table_status IN ('ACTIVE', 'CREATING', 'UPDATING', 'DELETING')",
    )
    .fetch_all(&catalog_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    let mut owned_artifacts: HashSet<String> = HashSet::new();
    let mut required_artifacts: HashSet<String> = HashSet::new();
    for (table_id, table_status) in &catalog_tables {
        let physical_name = physical_data_table_name(table_id);
        owned_artifacts.insert(physical_name.clone());
        if matches!(table_status.as_str(), "ACTIVE" | "CREATING" | "UPDATING") {
            required_artifacts.insert(physical_name);
        }
    }
    for (index_id, index_status, table_status) in &catalog_indexes {
        let physical_name = physical_index_table_name(index_id);
        owned_artifacts.insert(physical_name.clone());
        if matches!(table_status.as_str(), "ACTIVE" | "CREATING" | "UPDATING")
            && matches!(index_status.as_str(), "ACTIVE" | "CREATING")
        {
            required_artifacts.insert(physical_name);
        }
    }

    let actual_tables: Vec<(String,)> = sqlx::query_as(
        "SELECT tablename FROM pg_tables \
         WHERE schemaname = 'public' AND tablename LIKE '\\_ddb\\_%' ESCAPE '\\'",
    )
    .fetch_all(&data_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    let actual: HashSet<String> = actual_tables.into_iter().map(|(name,)| name).collect();

    let mut sections = Vec::new();

    let mut orphaned = actual
        .difference(&owned_artifacts)
        .cloned()
        .collect::<Vec<_>>();
    orphaned.sort();
    let mut orphaned_issues = Vec::new();
    for name in orphaned {
        let issue = if fix {
            let ddl = format!("DROP TABLE IF EXISTS {}", quote_pg_identifier(&name));
            match sqlx::query(&ddl).execute(&data_pool).await {
                Ok(_) => catalog_check_issue(&name)
                    .with_fix(CatalogCheckFix::Applied("dropped table".to_owned())),
                Err(error) => {
                    catalog_check_issue(&name).with_fix(CatalogCheckFix::Failed(error.to_string()))
                }
            }
        } else {
            catalog_check_issue(name)
        };
        orphaned_issues.push(issue);
    }
    sections.push(CatalogCheckSection::new(
        "Checking for orphaned data tables",
        "No orphaned tables.",
        orphaned_issues,
    ));

    let mut missing = required_artifacts
        .difference(&actual)
        .cloned()
        .collect::<Vec<_>>();
    missing.sort();
    sections.push(CatalogCheckSection::new(
        "Checking for missing data tables",
        "All catalog tables have backing data tables.",
        missing.into_iter().map(catalog_check_issue).collect(),
    ));

    let orphaned_indexes: Vec<(String,)> = sqlx::query_as(
        "SELECT i.index_name FROM indexes i \
         LEFT JOIN tables t ON i.table_id = t.table_id \
         WHERE t.table_id IS NULL",
    )
    .fetch_all(&catalog_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    sections.push(CatalogCheckSection::new(
        "Checking for orphaned index catalog entries",
        "No orphaned index catalog entries.",
        orphaned_indexes
            .into_iter()
            .map(|(name,)| catalog_check_issue(name))
            .collect(),
    ));

    let stuck: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_name, table_status FROM tables \
         WHERE table_status IN ('CREATING', 'UPDATING', 'DELETING') \
         AND status_transition_at < NOW() - INTERVAL '10 minutes'",
    )
    .fetch_all(&catalog_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    sections.push(CatalogCheckSection::new(
        "Checking for stuck transitions",
        "No stuck transitions.",
        stuck
            .into_iter()
            .map(|(name, status)| catalog_check_issue(name).with_detail(status))
            .collect(),
    ));

    Ok(CatalogCheckReport { sections })
}

impl OperationsEngine for PostgresOperationsEngine {
    fn parse_connection_string(&self, s: &str) -> Result<ConnectionParts, StorageError> {
        let parts = crate::config::parse_connection_string(s)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Convert ConnParts to ConnectionParts
        Ok(ConnectionParts {
            host: parts.host,
            port: parts.port,
            user: parts.user,
            password: parts.password,
            database: parts.database,
        })
    }

    fn redact_connection_string(&self, s: &str) -> String {
        // Redact password from postgresql://user:password@host:port/database
        if let Some(at) = s.find('@')
            && let Some(colon) = s[..at].rfind(':')
        {
            let scheme_end = s.find("://").map_or(0, |i| i + 3);
            if colon >= scheme_end {
                return format!("{}:***@{}", &s[..colon], &s[at + 1..]);
            }
        }
        s.to_owned()
    }

    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError> {
        // PostgreSQL identifier validation for format!-based DDL.
        // Rejects double quotes, null bytes, and non-ASCII characters.
        if name.contains('"') {
            return Err(StorageError::Internal(format!(
                "{label} must not contain double quotes"
            )));
        }
        if name.contains('\0') {
            return Err(StorageError::Internal(format!(
                "{label} must not contain null bytes"
            )));
        }
        if !name.is_ascii() {
            return Err(StorageError::Internal(format!(
                "{label} must contain only ASCII characters"
            )));
        }
        Ok(())
    }

    fn catalog_version(&self) -> String {
        crate::CATALOG_VERSION.to_string()
    }

    fn is_sensitive_key(&self, key: &str) -> bool {
        let lower = key.to_lowercase();
        [
            "connection_string",
            "password",
            "secret",
            "token",
            "encryption_key",
        ]
        .iter()
        .any(|pattern| lower.contains(pattern))
    }

    fn catalog_check<'a>(
        &'a self,
        connection_config: &'a str,
        fix: bool,
    ) -> BoxFuture<'a, Result<CatalogCheckReport, StorageError>> {
        Box::pin(postgres_catalog_check(connection_config, fix))
    }
}

#[cfg(test)]
mod tests {
    use super::{physical_data_table_name, physical_index_table_name, quote_pg_identifier};

    #[test]
    fn catalog_check_uses_current_uuid_physical_names() {
        assert_eq!(physical_data_table_name("table-1"), "_ddb_table-1");
        assert_eq!(physical_index_table_name("idx-1"), "_ddb_idx-1");
    }

    #[test]
    fn catalog_check_quotes_physical_table_names_for_cleanup() {
        assert_eq!(quote_pg_identifier("_ddb_a\"b"), "\"_ddb_a\"\"b\"");
    }
}
