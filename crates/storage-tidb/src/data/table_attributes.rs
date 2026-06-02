// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB table attributes for user data tables.

use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::MySqlPool;

use super::data_table_name;
use crate::tidb_util::execute_tidb_idempotent_ddl;

const USER_DATA_TABLE_REGION_MERGE_OPTION: &str = "merge_option=deny";

pub(crate) async fn deny_data_table_region_merges(
    pool: &MySqlPool,
    table_id: &str,
) -> Result<(), StorageError> {
    let table = data_table_name(table_id);
    let sql = user_data_table_region_merge_option_sql(&table);
    execute_tidb_idempotent_ddl(pool, "deny_data_table_region_merges", &sql).await?;
    Ok(())
}

pub(crate) async fn user_data_table_region_merge_option_sqls(
    pool: &MySqlPool,
) -> OpResult<Vec<String>> {
    let table_names: Vec<String> = sqlx::query_scalar(
        r"SELECT t.TABLE_NAME
          FROM information_schema.tables t
          LEFT JOIN information_schema.attributes a
            ON a.id = CONCAT('schema/', t.TABLE_SCHEMA, '/', t.TABLE_NAME)
          WHERE t.TABLE_SCHEMA = DATABASE()
            AND t.TABLE_TYPE = 'BASE TABLE'
            AND t.TABLE_NAME LIKE '\\_ddb\\_%' ESCAPE '\\'
            AND LOWER(REPLACE(COALESCE(a.attributes, ''), ' ', ''))
                NOT LIKE '%merge_option=deny%'
          ORDER BY t.TABLE_NAME",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Inspect TiDB user data table attributes: {e}")))?;

    Ok(table_names
        .into_iter()
        .map(|table_name| user_data_table_region_merge_option_sql(&quote_identifier(&table_name)))
        .collect())
}

fn user_data_table_region_merge_option_sql(table: &str) -> String {
    format!("ALTER TABLE {table} ATTRIBUTES '{USER_DATA_TABLE_REGION_MERGE_OPTION}'")
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::{
        USER_DATA_TABLE_REGION_MERGE_OPTION, quote_identifier,
        user_data_table_region_merge_option_sql,
    };

    #[test]
    fn user_data_tables_deny_region_merges_with_tidb_table_attributes() {
        assert_eq!(
            user_data_table_region_merge_option_sql("`_ddb_tableid`"),
            "ALTER TABLE `_ddb_tableid` ATTRIBUTES 'merge_option=deny'"
        );
        assert_eq!(USER_DATA_TABLE_REGION_MERGE_OPTION, "merge_option=deny");
    }

    #[test]
    fn table_attribute_repair_quotes_physical_table_names() {
        assert_eq!(quote_identifier("_ddb_table`id"), "`_ddb_table``id`");
    }
}
