// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB Region split helpers for existing user data tables.

use std::collections::BTreeMap;

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::MySqlPool;

use super::{
    DATA_TABLE_METADATA_LIKE_CLAUSE, DATA_TABLE_SPLIT_REGIONS, DECIMAL_SPLIT_LOWER,
    DECIMAL_SPLIT_UPPER, NATIVE_INDEX_METADATA_LIKE_CLAUSE, VARBINARY_SPLIT_LOWER,
    varbinary_split_upper,
};

pub(crate) const USER_TABLE_FULL_KEYSPACE_SPLITS_MIGRATION: &str =
    "003_full_user_table_split_bounds.sql";

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegionSplitColumn {
    name: String,
    column_type: String,
}

pub(crate) async fn user_data_table_region_split_sqls(pool: &MySqlPool) -> OpResult<Vec<String>> {
    let sql = format!(
        r"SELECT s.TABLE_NAME, s.INDEX_NAME, s.COLUMN_NAME, s.SEQ_IN_INDEX, c.COLUMN_TYPE
          FROM information_schema.statistics s
          JOIN information_schema.columns c
            ON c.TABLE_SCHEMA = s.TABLE_SCHEMA
           AND c.TABLE_NAME = s.TABLE_NAME
           AND c.COLUMN_NAME = s.COLUMN_NAME
          WHERE s.TABLE_SCHEMA = DATABASE()
            AND s.TABLE_NAME {DATA_TABLE_METADATA_LIKE_CLAUSE}
            AND (
                s.INDEX_NAME = 'PRIMARY'
                OR s.INDEX_NAME {NATIVE_INDEX_METADATA_LIKE_CLAUSE}
            )
          ORDER BY s.TABLE_NAME, s.INDEX_NAME, s.SEQ_IN_INDEX"
    );
    let rows: Vec<(String, String, String, i64, String)> = sqlx::query_as(&sql)
        .fetch_all(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Inspect TiDB user data indexes: {e}")))?;

    let mut grouped: BTreeMap<(String, String), Vec<(i64, RegionSplitColumn)>> = BTreeMap::new();
    for (table_name, index_name, column_name, seq_in_index, column_type) in rows {
        grouped.entry((table_name, index_name)).or_default().push((
            seq_in_index,
            RegionSplitColumn {
                name: column_name,
                column_type,
            },
        ));
    }

    grouped
        .into_iter()
        .map(|((table_name, index_name), mut columns)| {
            columns.sort_by_key(|(seq_in_index, _)| *seq_in_index);
            let columns = columns
                .into_iter()
                .map(|(_, column)| column)
                .collect::<Vec<_>>();
            user_data_region_split_sql(&table_name, &index_name, &columns)
        })
        .collect()
}

fn user_data_region_split_sql(
    table_name: &str,
    index_name: &str,
    columns: &[RegionSplitColumn],
) -> OpResult<String> {
    let split_columns = if index_name == "PRIMARY" {
        columns
    } else {
        columns.get(..1).unwrap_or(columns)
    };
    if split_columns.is_empty() {
        return Err(OpError::Internal(format!(
            "TiDB user data split target {table_name}.{index_name} has no indexed columns"
        )));
    }

    let lower = split_columns
        .iter()
        .map(|column| split_bound_for_column(table_name, index_name, column, true))
        .collect::<OpResult<Vec<_>>>()?;
    let upper = split_columns
        .iter()
        .map(|column| split_bound_for_column(table_name, index_name, column, false))
        .collect::<OpResult<Vec<_>>>()?;

    let table = quote_identifier(table_name);
    let sql = if index_name == "PRIMARY" {
        format!(
            "SPLIT TABLE {table} BETWEEN ({}) AND ({}) REGIONS {DATA_TABLE_SPLIT_REGIONS}",
            lower.join(", "),
            upper.join(", ")
        )
    } else {
        let index = quote_identifier(index_name);
        format!(
            "SPLIT TABLE {table} INDEX {index} BETWEEN ({}) AND ({}) REGIONS {DATA_TABLE_SPLIT_REGIONS}",
            lower.join(", "),
            upper.join(", ")
        )
    };
    Ok(sql)
}

fn split_bound_for_column(
    table_name: &str,
    index_name: &str,
    column: &RegionSplitColumn,
    lower: bool,
) -> OpResult<String> {
    if let Some(bytes) = varbinary_column_bytes(&column.column_type) {
        return Ok(if lower {
            VARBINARY_SPLIT_LOWER.to_owned()
        } else {
            varbinary_split_upper(bytes)
        });
    }

    if is_decimal_split_column(&column.column_type) {
        return Ok(if lower {
            DECIMAL_SPLIT_LOWER.to_owned()
        } else {
            DECIMAL_SPLIT_UPPER.to_owned()
        });
    }

    Err(OpError::Internal(format!(
        "TiDB user data split target {table_name}.{index_name}.{} uses unsupported indexed column type {}; expected VARBINARY or DECIMAL",
        column.name, column.column_type
    )))
}

fn varbinary_column_bytes(column_type: &str) -> Option<usize> {
    let normalized = column_type.trim().to_ascii_lowercase();
    let inner = normalized.strip_prefix("varbinary(")?.strip_suffix(')')?;
    inner.parse().ok()
}

fn is_decimal_split_column(column_type: &str) -> bool {
    column_type
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .eq_ignore_ascii_case("decimal(65,30)")
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use extenddb_storage::management_store::OpError;

    use super::{
        RegionSplitColumn, USER_TABLE_FULL_KEYSPACE_SPLITS_MIGRATION, user_data_region_split_sql,
        varbinary_column_bytes,
    };
    use crate::data::varbinary_split_upper;

    #[test]
    fn dynamic_data_migration_repairs_user_table_splits_once() {
        assert_eq!(
            USER_TABLE_FULL_KEYSPACE_SPLITS_MIGRATION,
            "003_full_user_table_split_bounds.sql"
        );
    }

    #[test]
    fn user_data_split_repair_uses_full_primary_keyspace() {
        let sql = user_data_region_split_sql(
            "_ddb_tableid",
            "PRIMARY",
            &[RegionSplitColumn {
                name: "pk".to_owned(),
                column_type: "varbinary(2048)".to_owned(),
            }],
        )
        .expect("split sql");

        assert_eq!(
            sql,
            format!(
                "SPLIT TABLE `_ddb_tableid` BETWEEN (X'') AND ({}) REGIONS 16",
                varbinary_split_upper(2048)
            )
        );
    }

    #[test]
    fn user_data_split_repair_uses_full_clustered_sort_keyspace() {
        let sql = user_data_region_split_sql(
            "_ddb_tableid",
            "PRIMARY",
            &[
                RegionSplitColumn {
                    name: "pk".to_owned(),
                    column_type: "varbinary(2048)".to_owned(),
                },
                RegionSplitColumn {
                    name: "sk_b".to_owned(),
                    column_type: "VARBINARY(1024)".to_owned(),
                },
            ],
        )
        .expect("split sql");

        assert_eq!(
            sql,
            format!(
                "SPLIT TABLE `_ddb_tableid` BETWEEN (X'', X'') AND ({}, {}) REGIONS 16",
                varbinary_split_upper(2048),
                varbinary_split_upper(1024)
            )
        );
    }

    #[test]
    fn user_data_split_repair_splits_native_indexes_by_hash_prefix() {
        let sql = user_data_region_split_sql(
            "_ddb_tableid",
            "idx_idx1",
            &[
                RegionSplitColumn {
                    name: "edbidx_idx1_pk".to_owned(),
                    column_type: "varbinary(2048)".to_owned(),
                },
                RegionSplitColumn {
                    name: "edbidx_idx1_sk_n".to_owned(),
                    column_type: "decimal(65,30)".to_owned(),
                },
            ],
        )
        .expect("split sql");

        assert_eq!(
            sql,
            format!(
                "SPLIT TABLE `_ddb_tableid` INDEX `idx_idx1` BETWEEN (X'') AND ({}) REGIONS 16",
                varbinary_split_upper(2048)
            )
        );
    }

    #[test]
    fn user_data_split_repair_rejects_unknown_index_column_types() {
        let err = user_data_region_split_sql(
            "_ddb_tableid",
            "PRIMARY",
            &[RegionSplitColumn {
                name: "pk".to_owned(),
                column_type: "varchar(2048)".to_owned(),
            }],
        )
        .expect_err("unsupported type");

        let OpError::Internal(message) = err else {
            panic!("expected internal error for unsupported split column type");
        };
        assert!(message.contains("unsupported indexed column type"));
    }

    #[test]
    fn varbinary_column_width_parser_accepts_tidb_metadata_shape() {
        assert_eq!(varbinary_column_bytes("varbinary(2048)"), Some(2048));
        assert_eq!(varbinary_column_bytes(" VARBINARY(1024) "), Some(1024));
        assert_eq!(varbinary_column_bytes("varchar(2048)"), None);
    }
}
