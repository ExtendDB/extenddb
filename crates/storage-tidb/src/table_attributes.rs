// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB table attributes for preserving native Region layouts.

use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::MySqlPool;

use crate::data::DATA_TABLE_METADATA_LIKE_CLAUSE;
use crate::tidb_util::execute_tidb_idempotent_ddl;

const REGION_MERGE_OPTION: &str = "merge_option=deny";

pub(crate) async fn deny_table_region_merges(
    pool: &MySqlPool,
    physical_table_name: &str,
) -> Result<(), StorageError> {
    let sql = table_region_merge_option_sql(&quote_identifier(physical_table_name));
    execute_tidb_idempotent_ddl(pool, "deny_table_region_merges", &sql).await?;
    Ok(())
}

pub(crate) async fn fixed_table_region_merge_option_sqls(
    pool: &MySqlPool,
    physical_table_names: &[&str],
) -> OpResult<Vec<String>> {
    let mut statements = Vec::new();
    for table_name in physical_table_names {
        if table_needs_region_merge_option(pool, table_name).await? {
            statements.push(table_region_merge_option_sql(&quote_identifier(table_name)));
        }
    }
    Ok(statements)
}

pub(crate) async fn user_data_table_region_merge_option_sqls(
    pool: &MySqlPool,
) -> OpResult<Vec<String>> {
    let sql = format!(
        r"SELECT t.TABLE_NAME, a.ATTRIBUTES
          FROM information_schema.tables t
          LEFT JOIN information_schema.attributes a
            ON a.id = CONCAT('schema/', t.TABLE_SCHEMA, '/', t.TABLE_NAME)
          WHERE t.TABLE_SCHEMA = DATABASE()
            AND t.TABLE_TYPE = 'BASE TABLE'
            AND t.TABLE_NAME {DATA_TABLE_METADATA_LIKE_CLAUSE}
          ORDER BY t.TABLE_NAME"
    );
    let rows: Vec<(String, Option<String>)> = sqlx::query_as(&sql)
        .fetch_all(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Inspect TiDB user data table attributes: {e}")))?;

    Ok(rows
        .into_iter()
        .filter(|(_, attributes)| !tidb_attributes_deny_region_merges(attributes.as_deref()))
        .map(|(table_name, _)| table_region_merge_option_sql(&quote_identifier(&table_name)))
        .collect())
}

async fn table_needs_region_merge_option(pool: &MySqlPool, table_name: &str) -> OpResult<bool> {
    let row: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT a.ATTRIBUTES \
         FROM information_schema.tables t \
         LEFT JOIN information_schema.attributes a \
           ON a.id = CONCAT('schema/', t.TABLE_SCHEMA, '/', t.TABLE_NAME) \
         WHERE t.TABLE_SCHEMA = DATABASE() \
           AND t.TABLE_TYPE = 'BASE TABLE' \
           AND t.TABLE_NAME = ?",
    )
    .bind(table_name)
    .fetch_optional(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Inspect TiDB table attributes: {e}")))?;

    Ok(row
        .map(|(attributes,)| !tidb_attributes_deny_region_merges(attributes.as_deref()))
        .unwrap_or(false))
}

#[cfg(test)]
fn tidb_table_attribute_id(schema: &str, table: &str) -> String {
    format!("schema/{schema}/{table}")
}

fn tidb_attributes_deny_region_merges(attributes: Option<&str>) -> bool {
    attributes
        .map(|value| {
            value
                .chars()
                .filter(|c| !c.is_whitespace() && *c != '"')
                .collect::<String>()
                .to_ascii_lowercase()
                .contains(REGION_MERGE_OPTION)
        })
        .unwrap_or(false)
}

fn table_region_merge_option_sql(table: &str) -> String {
    format!("ALTER TABLE {table} ATTRIBUTES '{REGION_MERGE_OPTION}'")
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::{
        REGION_MERGE_OPTION, quote_identifier, table_region_merge_option_sql,
        tidb_attributes_deny_region_merges, tidb_table_attribute_id,
    };

    #[test]
    fn tables_deny_region_merges_with_tidb_table_attributes() {
        assert_eq!(
            table_region_merge_option_sql("`stream_records`"),
            "ALTER TABLE `stream_records` ATTRIBUTES 'merge_option=deny'"
        );
        assert_eq!(REGION_MERGE_OPTION, "merge_option=deny");
    }

    #[test]
    fn table_attribute_repair_quotes_physical_table_names() {
        assert_eq!(quote_identifier("_ddb_table`id"), "`_ddb_table``id`");
    }

    #[test]
    fn tidb_table_attribute_id_matches_information_schema_shape() {
        assert_eq!(
            tidb_table_attribute_id("extenddb_data", "_ddb_tableid"),
            "schema/extenddb_data/_ddb_tableid"
        );
    }

    #[test]
    fn merge_option_detection_accepts_tidb_jsonish_attribute_text() {
        assert!(tidb_attributes_deny_region_merges(Some(
            "\"merge_option=deny\""
        )));
        assert!(tidb_attributes_deny_region_merges(Some(
            "\"other=x, merge_option=deny\""
        )));
        assert!(!tidb_attributes_deny_region_merges(Some(
            "\"merge_option=allow\""
        )));
        assert!(!tidb_attributes_deny_region_merges(None));
    }
}
