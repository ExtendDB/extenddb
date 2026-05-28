// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Native secondary-index helpers for the `TiDB` backend.
//!
//! `TiDB` owns secondary-index maintenance. ExtendDB stores every item once in
//! the base `_ddb_*` table, exposes DynamoDB index keys as generated columns,
//! and creates a native TiDB secondary index over those generated columns plus
//! the base table key for stable pagination.

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::sk_column_n;

use super::{all_sort_key_info, data_table_name};

pub(crate) struct WriteIndexKeys {
    key_schema: Vec<KeySchemaElement>,
}

struct GeneratedColumn {
    name: String,
    ddl_type: &'static str,
    expression: String,
}

/// Fetch secondary-index key schemas that must be validated on writes.
///
/// CREATING indexes are included because TiDB's online ADD INDEX backfill will
/// observe existing base rows. Letting malformed index-key attributes into the
/// base table during that window would create permanently sparse index entries.
pub(crate) async fn fetch_write_index_key_schemas(
    table_id: &str,
    pool: &sqlx::MySqlPool,
) -> Result<Vec<WriteIndexKeys>, StorageError> {
    let rows: Vec<(serde_json::Value,)> = sqlx::query_as(
        "SELECT key_schema FROM indexes \
         WHERE table_id = ? AND index_status IN ('ACTIVE', 'CREATING')",
    )
    .bind(table_id)
    .fetch_all(pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    rows.into_iter()
        .map(|(ks_json,)| {
            let key_schema = serde_json::from_value(ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(WriteIndexKeys { key_schema })
        })
        .collect()
}

pub(crate) fn validate_item_index_key_types(
    item: &Item,
    indexes: &[WriteIndexKeys],
    attr_defs: &[AttributeDefinition],
) -> Result<(), StorageError> {
    for index in indexes {
        for key in &index.key_schema {
            let Some(value) = item.get(&key.attribute_name) else {
                continue;
            };
            let expected = attr_defs
                .iter()
                .find(|ad| ad.attribute_name == key.attribute_name)
                .map(|ad| ad.attribute_type)
                .ok_or_else(|| {
                    StorageError::Internal(format!(
                        "missing attribute definition for index key {}",
                        key.attribute_name
                    ))
                })?;
            if !attribute_value_matches_type(value, expected) {
                return Err(StorageError::Validation(format!(
                    "One or more parameter values were invalid: Type mismatch for key attribute {}: expected: {}",
                    key.attribute_name,
                    scalar_type_name(expected)
                )));
            }
        }
    }
    Ok(())
}

pub(crate) async fn create_native_secondary_index(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    index_id: &str,
    index_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    base_key_schema: &[KeySchemaElement],
    base_attr_defs: &[AttributeDefinition],
) -> Result<(), StorageError> {
    let table = data_table_name(table_id);
    for column in generated_columns(index_id, index_key_schema, attr_defs)? {
        let ddl = format!(
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS `{}` {} AS ({}) VIRTUAL",
            column.name, column.ddl_type, column.expression
        );
        sqlx::query(&ddl)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }

    let index_name = native_index_name(index_id);
    let index_columns = native_index_physical_columns(
        index_id,
        index_key_schema,
        attr_defs,
        base_key_schema,
        base_attr_defs,
    );
    let ddl = format!(
        "CREATE INDEX IF NOT EXISTS `{index_name}` ON {table} ({})",
        index_columns.join(", ")
    );
    sqlx::query(&ddl)
        .execute(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(())
}

pub(crate) async fn drop_native_secondary_index(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    index_id: &str,
    index_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), StorageError> {
    let table = data_table_name(table_id);
    let index_name = native_index_name(index_id);
    let drop_index = format!("DROP INDEX IF EXISTS `{index_name}` ON {table}");
    sqlx::query(&drop_index)
        .execute(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    for column in native_index_key_tuple_columns(index_id, index_key_schema, attr_defs) {
        let ddl = format!("ALTER TABLE {table} DROP COLUMN IF EXISTS `{column}`");
        sqlx::query(&ddl)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }

    Ok(())
}

pub(crate) fn native_index_hash_column(index_id: &str) -> String {
    format!("{}_pk", native_index_prefix(index_id))
}

pub(crate) fn native_index_sort_columns(
    index_id: &str,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Vec<String> {
    all_sort_key_info(key_schema, attr_defs)
        .into_iter()
        .enumerate()
        .map(|(i, (_, sk_type))| {
            format!(
                "{}_{}",
                native_index_prefix(index_id),
                sk_column_n(i, sk_type)
            )
        })
        .collect()
}

pub(crate) fn native_index_key_tuple_columns(
    index_id: &str,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Vec<String> {
    let mut columns = vec![native_index_hash_column(index_id)];
    columns.extend(native_index_sort_columns(index_id, key_schema, attr_defs));
    columns
}

pub(crate) fn native_index_non_null_predicates(
    index_id: &str,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Vec<String> {
    native_index_key_tuple_columns(index_id, key_schema, attr_defs)
        .into_iter()
        .map(|column| format!("{column} IS NOT NULL"))
        .collect()
}

fn native_index_physical_columns(
    index_id: &str,
    index_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    base_key_schema: &[KeySchemaElement],
    base_attr_defs: &[AttributeDefinition],
) -> Vec<String> {
    let mut columns = native_index_key_tuple_columns(index_id, index_key_schema, attr_defs);
    columns.push("pk".to_owned());
    columns.extend(
        all_sort_key_info(base_key_schema, base_attr_defs)
            .into_iter()
            .enumerate()
            .map(|(i, (_, sk_type))| sk_column_n(i, sk_type)),
    );
    columns
}

fn generated_columns(
    index_id: &str,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<Vec<GeneratedColumn>, StorageError> {
    let mut columns = vec![GeneratedColumn {
        name: native_index_hash_column(index_id),
        ddl_type: "VARBINARY(2048)",
        expression: hash_key_expression(key_schema, attr_defs)?,
    }];

    for (i, (attr_name, attr_type)) in all_sort_key_info(key_schema, attr_defs)
        .into_iter()
        .enumerate()
    {
        columns.push(GeneratedColumn {
            name: format!(
                "{}_{}",
                native_index_prefix(index_id),
                sk_column_n(i, attr_type)
            ),
            ddl_type: generated_sort_column_type(attr_type),
            expression: sort_key_expression(attr_name, attr_type),
        });
    }

    Ok(columns)
}

fn hash_key_expression(
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<String, StorageError> {
    let hash_parts = key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Hash)
        .map(|ks| {
            let attr_type = attribute_type(&ks.attribute_name, attr_defs)?;
            Ok(key_scalar_expression(&ks.attribute_name, attr_type))
        })
        .collect::<Result<Vec<_>, StorageError>>()?;

    if hash_parts.len() == 1 {
        return Ok(format!("CAST({} AS BINARY)", hash_parts[0]));
    }

    let mut concat_parts = Vec::with_capacity(hash_parts.len() * 4);
    for part in hash_parts {
        concat_parts.push(format!("OCTET_LENGTH({part})"));
        concat_parts.push("':'".to_owned());
        concat_parts.push(part);
        concat_parts.push("','".to_owned());
    }
    Ok(format!("CONCAT({})", concat_parts.join(", ")))
}

fn sort_key_expression(attr_name: &str, attr_type: ScalarAttributeType) -> String {
    let scalar = key_scalar_expression(attr_name, attr_type);
    match attr_type {
        ScalarAttributeType::S => format!("CAST({scalar} AS BINARY)"),
        ScalarAttributeType::N => format!("CAST({scalar} AS DECIMAL(65, 30))"),
        ScalarAttributeType::B => format!("FROM_BASE64({scalar})"),
    }
}

fn key_scalar_expression(attr_name: &str, attr_type: ScalarAttributeType) -> String {
    format!(
        "JSON_UNQUOTE(JSON_EXTRACT(item_data, {}))",
        sql_string_literal(&json_attribute_type_path(attr_name, attr_type))
    )
}

fn generated_sort_column_type(attr_type: ScalarAttributeType) -> &'static str {
    match attr_type {
        ScalarAttributeType::S | ScalarAttributeType::B => "VARBINARY(1024)",
        ScalarAttributeType::N => "DECIMAL(65, 30)",
    }
}

fn attribute_type(
    attr_name: &str,
    attr_defs: &[AttributeDefinition],
) -> Result<ScalarAttributeType, StorageError> {
    attr_defs
        .iter()
        .find(|ad| ad.attribute_name == attr_name)
        .map(|ad| ad.attribute_type)
        .ok_or_else(|| {
            StorageError::Internal(format!(
                "missing attribute definition for index key {attr_name}"
            ))
        })
}

fn attribute_value_matches_type(value: &AttributeValue, attr_type: ScalarAttributeType) -> bool {
    matches!(
        (attr_type, value),
        (ScalarAttributeType::S, AttributeValue::S(_))
            | (ScalarAttributeType::N, AttributeValue::N(_))
            | (ScalarAttributeType::B, AttributeValue::B(_))
    )
}

fn scalar_type_name(attr_type: ScalarAttributeType) -> &'static str {
    match attr_type {
        ScalarAttributeType::S => "S",
        ScalarAttributeType::N => "N",
        ScalarAttributeType::B => "B",
    }
}

fn native_index_prefix(index_id: &str) -> String {
    let suffix: String = index_id
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    format!("edbidx_{suffix}")
}

fn native_index_name(index_id: &str) -> String {
    let suffix: String = index_id
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    format!("idx_{suffix}")
}

fn json_attribute_type_path(attr_name: &str, attr_type: ScalarAttributeType) -> String {
    format!(
        "$.\"{}\".\"{}\"",
        json_path_key_escape(attr_name),
        scalar_type_name(attr_type)
    )
}

fn json_path_key_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    use super::{hash_key_expression, native_index_hash_column, native_index_key_tuple_columns};

    #[test]
    fn native_index_columns_are_stable_and_identifier_safe() {
        let index_id = "2f98c5ac-6c16-4418-b607-cd56ffc1b7a5";
        assert_eq!(
            native_index_hash_column(index_id),
            "edbidx_2f98c5ac6c164418b607cd56ffc1b7a5_pk"
        );
    }

    #[test]
    fn native_index_tuple_uses_generated_hash_and_typed_range_columns() {
        let ks = vec![
            KeySchemaElement {
                attribute_name: "gpk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsk".to_owned(),
                key_type: KeyType::Range,
            },
        ];
        let attrs = vec![
            AttributeDefinition {
                attribute_name: "gpk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsk".to_owned(),
                attribute_type: ScalarAttributeType::N,
            },
        ];

        assert_eq!(
            native_index_key_tuple_columns("idx-1", &ks, &attrs),
            vec!["edbidx_idx1_pk".to_owned(), "edbidx_idx1_sk_n".to_owned()]
        );
    }

    #[test]
    fn multipart_hash_expression_matches_netstring_shape() {
        let ks = vec![
            KeySchemaElement {
                attribute_name: "a".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "b".to_owned(),
                key_type: KeyType::Hash,
            },
        ];
        let attrs = vec![
            AttributeDefinition {
                attribute_name: "a".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "b".to_owned(),
                attribute_type: ScalarAttributeType::B,
            },
        ];

        let expr = hash_key_expression(&ks, &attrs).expect("hash expression");
        assert!(expr.starts_with("CONCAT("));
        assert!(expr.contains("OCTET_LENGTH(JSON_UNQUOTE(JSON_EXTRACT"));
        assert!(expr.contains("$.\"a\".\"S\""));
        assert!(expr.contains("$.\"b\".\"B\""));
    }
}
