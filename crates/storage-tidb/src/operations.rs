// Copyright 2026 DynamoDB Open contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB implementation of `OperationsEngine`.

use std::collections::{HashMap, HashSet};

use extenddb_core::types::{AttributeDefinition, KeySchemaElement};
use extenddb_storage::error::StorageError;
use extenddb_storage::operations::{
    CatalogCheckFix, CatalogCheckIssue, CatalogCheckReport, CatalogCheckSection, ConnectionParts,
    OperationsEngine,
};
use futures::future::BoxFuture;

use crate::data::{
    data_table_name, native_index_key_tuple_columns, native_index_name, physical_data_table_name,
};
use crate::metadata_engine::{create_table_has_disabled_ttl, create_table_has_native_ttl};
use crate::tidb_util::{execute_tidb_idempotent_ddl, tidb_pool_options};

/// TiDB operations engine for ddbo CLI commands.
pub struct TidbOperationsEngine;

type NativeIndexArtifactRow = (
    String,
    String,
    serde_json::Value,
    String,
    String,
    serde_json::Value,
);

struct RequiredCatalogLookupIndex {
    table: &'static str,
    index: &'static str,
    columns: &'static str,
}

const REQUIRED_CATALOG_LOOKUP_INDEXES: &[RequiredCatalogLookupIndex] =
    &[RequiredCatalogLookupIndex {
        table: "iam_group_members",
        index: "idx_iam_group_members_user",
        columns: "account_id,user_name,group_name",
    }];

fn catalog_check_issue(name: impl Into<String>) -> CatalogCheckIssue {
    CatalogCheckIssue::new(name)
}

fn quote_tidb_identifier(value: &str) -> Result<String, StorageError> {
    if value.contains('`') || value.contains('\0') {
        return Err(StorageError::Internal(
            "TiDB physical table name is not safe for SQL identifiers".to_owned(),
        ));
    }
    Ok(format!("`{value}`"))
}

fn parse_json<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
    label: &str,
) -> Result<T, StorageError> {
    serde_json::from_value(value)
        .map_err(|e| StorageError::Internal(format!("invalid {label}: {e}")))
}

async fn tidb_catalog_check(
    connection_config: &str,
    fix: bool,
) -> Result<CatalogCheckReport, StorageError> {
    let catalog_connection = crate::config::sqlx_connection_string(connection_config);
    let catalog_pool = tidb_pool_options(2, 0)
        .connect(&catalog_connection)
        .await
        .map_err(|e| StorageError::Connection(format!("Cannot connect to catalog: {e}")))?;

    let data_conn: Option<(String,)> = sqlx::query_as(
        "SELECT value FROM settings WHERE `key` = 'data_database_connection_string'",
    )
    .fetch_optional(&catalog_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    let data_connection = data_conn
        .map(|(value,)| value)
        .filter(|value| !value.is_empty())
        .map_or_else(
            || catalog_connection.clone(),
            |value| crate::config::sqlx_connection_string(&value),
        );
    let data_pool = tidb_pool_options(2, 0)
        .connect(&data_connection)
        .await
        .map_err(|e| StorageError::Connection(format!("Cannot connect to data database: {e}")))?;

    let catalog_tables: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_id, table_status FROM tables \
         WHERE table_status IN ('ACTIVE', 'CREATING', 'UPDATING', 'DELETING')",
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

    let actual_tables: Vec<(String,)> = sqlx::query_as(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = DATABASE() AND table_name LIKE ? ESCAPE '!'",
    )
    .bind("!_ddb!_%")
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
            let ddl = format!("DROP TABLE IF EXISTS {}", quote_tidb_identifier(&name)?);
            match execute_tidb_idempotent_ddl(
                &data_pool,
                "catalog_check_drop_orphaned_data_table",
                &ddl,
            )
            .await
            {
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
         AND status_transition_at < CURRENT_TIMESTAMP(6) - INTERVAL 10 MINUTE",
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

    sections.push(check_tidb_catalog_lookup_indexes(&catalog_pool).await?);
    sections.push(check_tidb_native_index_artifacts(&catalog_pool, &data_pool, &actual).await?);
    sections.push(check_tidb_native_ttl_artifacts(&catalog_pool, &data_pool, &actual).await?);

    Ok(CatalogCheckReport { sections })
}

async fn check_tidb_catalog_lookup_indexes(
    catalog_pool: &sqlx::MySqlPool,
) -> Result<CatalogCheckSection, StorageError> {
    let actual: HashMap<(String, String), String> = sqlx::query_as::<_, (String, String, String)>(
        "SELECT table_name, index_name, \
                    GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') AS columns \
             FROM information_schema.statistics \
             WHERE table_schema = DATABASE() \
               AND table_name = 'iam_group_members' \
             GROUP BY table_name, index_name",
    )
    .fetch_all(catalog_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?
    .into_iter()
    .map(|(table, index, columns)| ((table, index), columns))
    .collect();

    Ok(CatalogCheckSection::new(
        "Checking TiDB catalog authorization lookup indexes",
        "All hot authorization lookups have native TiDB indexes.",
        catalog_lookup_index_issues(&actual),
    ))
}

fn catalog_lookup_index_issues(
    actual: &HashMap<(String, String), String>,
) -> Vec<CatalogCheckIssue> {
    REQUIRED_CATALOG_LOOKUP_INDEXES
        .iter()
        .filter_map(|required| {
            let key = (required.table.to_owned(), required.index.to_owned());
            match actual.get(&key) {
                Some(columns) if columns == required.columns => None,
                Some(columns) => Some(
                    catalog_check_issue(format!("{}.{}", required.table, required.index))
                        .with_detail(format!(
                            "expected columns {}, found {columns}",
                            required.columns
                        )),
                ),
                None => Some(
                    catalog_check_issue(format!("{}.{}", required.table, required.index))
                        .with_detail("missing native TiDB catalog lookup index"),
                ),
            }
        })
        .collect()
}

async fn check_tidb_native_index_artifacts(
    catalog_pool: &sqlx::MySqlPool,
    data_pool: &sqlx::MySqlPool,
    actual_tables: &HashSet<String>,
) -> Result<CatalogCheckSection, StorageError> {
    let rows: Vec<NativeIndexArtifactRow> = sqlx::query_as(
        "SELECT t.table_id, t.table_name, t.attribute_definitions, \
                i.index_id, i.index_name, i.key_schema \
         FROM indexes i JOIN tables t ON i.table_id = t.table_id \
         WHERE t.table_status IN ('ACTIVE', 'CREATING', 'UPDATING') \
           AND i.index_status IN ('ACTIVE', 'CREATING')",
    )
    .fetch_all(catalog_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    let actual_columns: HashSet<(String, String)> = sqlx::query_as::<_, (String, String)>(
        "SELECT table_name, column_name FROM information_schema.columns \
         WHERE table_schema = DATABASE() AND table_name LIKE ? ESCAPE '!'",
    )
    .bind("!_ddb!_%")
    .fetch_all(data_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?
    .into_iter()
    .collect();

    let actual_indexes: HashSet<(String, String)> = sqlx::query_as::<_, (String, String)>(
        "SELECT table_name, index_name FROM information_schema.statistics \
         WHERE table_schema = DATABASE() AND table_name LIKE ? ESCAPE '!'",
    )
    .bind("!_ddb!_%")
    .fetch_all(data_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?
    .into_iter()
    .collect();

    let mut issues = Vec::new();
    for (table_id, table_name, attr_defs_json, index_id, index_name, key_schema_json) in rows {
        let physical_table = physical_data_table_name(&table_id);
        if !actual_tables.contains(&physical_table) {
            continue;
        }
        let attr_defs: Vec<AttributeDefinition> =
            parse_json(attr_defs_json, "table attribute definitions")?;
        let key_schema: Vec<KeySchemaElement> = parse_json(key_schema_json, "index key schema")?;
        let expected_index_name = native_index_name(&index_id);
        if !actual_indexes.contains(&(physical_table.clone(), expected_index_name.clone())) {
            issues.push(
                catalog_check_issue(format!("{table_name}.{index_name}"))
                    .with_detail(format!("missing native TiDB index {expected_index_name}")),
            );
        }

        for column in native_index_key_tuple_columns(&index_id, &key_schema, &attr_defs) {
            if !actual_columns.contains(&(physical_table.clone(), column.clone())) {
                issues.push(
                    catalog_check_issue(format!("{table_name}.{index_name}"))
                        .with_detail(format!("missing generated column {column}")),
                );
            }
        }
    }

    Ok(CatalogCheckSection::new(
        "Checking TiDB native secondary-index artifacts",
        "All catalog indexes have native TiDB index artifacts.",
        issues,
    ))
}

async fn check_tidb_native_ttl_artifacts(
    catalog_pool: &sqlx::MySqlPool,
    data_pool: &sqlx::MySqlPool,
    actual_tables: &HashSet<String>,
) -> Result<CatalogCheckSection, StorageError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_id, table_name FROM tables \
         WHERE ttl_status = 'ENABLED' AND table_status IN ('ACTIVE', 'UPDATING')",
    )
    .fetch_all(catalog_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    let mut issues = Vec::new();
    for (table_id, table_name) in rows {
        let physical_table = physical_data_table_name(&table_id);
        if !actual_tables.contains(&physical_table) {
            continue;
        }
        let (_, create_table): (String, String) =
            sqlx::query_as(&format!("SHOW CREATE TABLE {}", data_table_name(&table_id)))
                .fetch_one(data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        let create_table = create_table.to_ascii_uppercase();
        if !create_table_has_native_ttl(&create_table) {
            issues.push(
                catalog_check_issue(&table_name)
                    .with_detail("catalog TTL is ENABLED but native TiDB TTL is absent"),
            );
        } else if create_table_has_disabled_ttl(&create_table) {
            issues.push(
                catalog_check_issue(&table_name)
                    .with_detail("catalog TTL is ENABLED but TiDB TTL_ENABLE is OFF"),
            );
        }
    }

    Ok(CatalogCheckSection::new(
        "Checking TiDB native TTL artifacts",
        "All TTL-enabled tables have native TiDB TTL enabled.",
        issues,
    ))
}

impl OperationsEngine for TidbOperationsEngine {
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
        crate::config::redact_connection_string(s)
    }

    fn validate_identifier(&self, name: &str, label: &str) -> Result<(), StorageError> {
        // TiDB identifier validation for format!-based DDL.
        // Rejects backticks, null bytes, and non-ASCII characters.
        if name.contains('`') {
            return Err(StorageError::Internal(format!(
                "{label} must not contain backticks"
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
        Box::pin(tidb_catalog_check(connection_config, fix))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{catalog_lookup_index_issues, quote_tidb_identifier};

    #[test]
    fn catalog_check_quotes_tidb_physical_table_names_for_cleanup() {
        assert_eq!(quote_tidb_identifier("_ddb_abc").unwrap(), "`_ddb_abc`");
        assert!(quote_tidb_identifier("_ddb_bad`name").is_err());
    }

    #[test]
    fn catalog_check_requires_hot_authorization_lookup_indexes() {
        let actual = HashMap::from([(
            (
                "iam_group_members".to_owned(),
                "idx_iam_group_members_user".to_owned(),
            ),
            "account_id,user_name,group_name".to_owned(),
        )]);

        let issues = catalog_lookup_index_issues(&actual);

        assert!(issues.is_empty());
    }

    #[test]
    fn catalog_check_reports_missing_group_membership_lookup_index() {
        let actual = HashMap::new();

        let issues = catalog_lookup_index_issues(&actual);

        assert_eq!(issues.len(), 1);
        assert_eq!(
            issues[0].name,
            "iam_group_members.idx_iam_group_members_user"
        );
        assert_eq!(
            issues[0].detail.as_deref(),
            Some("missing native TiDB catalog lookup index")
        );
    }
}
