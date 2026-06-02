// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Native secondary-index helpers for the `TiDB` backend.
//!
//! `TiDB` owns secondary-index maintenance. ExtendDB stores every item once in
//! the base `_ddb_*` table, exposes DynamoDB index keys as generated columns,
//! and creates a native TiDB secondary index over those generated columns.
//! TiDB secondary-index entries already carry the clustered row handle, so the
//! base table key must not be duplicated into the index definition: doing so
//! would waste write bandwidth and can exceed TiDB's 3072-byte index key limit
//! for legal DynamoDB key sizes.
//!
//! Generated columns are intentional here. TiDB documents them as the
//! production path for indexing JSON-derived values; generic expression indexes
//! would make DynamoDB's casts, binary decoding, and composite-key expressions
//! depend on the expression-index experimental function surface.

use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeDefinition, Item, KeySchemaElement, KeyType, ScalarAttributeType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::sk_column_n;

use super::{
    DYNAMODB_HASH_KEY_COLUMN_TYPE, DYNAMODB_SORT_KEY_COLUMN_TYPE, all_sort_key_info,
    data_table_name,
};
use crate::tidb_util::execute_tidb_idempotent_ddl;

pub(crate) struct NativeSecondaryIndex<'a> {
    pub index_id: &'a str,
    pub key_schema: &'a [KeySchemaElement],
}

struct GeneratedColumn {
    name: String,
    ddl_type: &'static str,
    expression: String,
}

pub(crate) fn validate_item_secondary_index_key_constraints(
    item: &Item,
    secondary_index_key_schemas: &[Vec<KeySchemaElement>],
    attr_defs: &[AttributeDefinition],
    limits: &LimitsConfig,
) -> Result<(), StorageError> {
    extenddb_core::validation::validate_item_secondary_index_key_constraints(
        item,
        secondary_index_key_schemas,
        attr_defs,
        limits,
    )
    .map_err(dynamodb_validation_to_storage_error)
}

pub(crate) fn validate_item_index_key_constraints(
    item: &Item,
    indexes: &[Vec<KeySchemaElement>],
    attr_defs: &[AttributeDefinition],
    limits: &LimitsConfig,
) -> Result<(), StorageError> {
    extenddb_core::validation::validate_item_index_key_constraints(item, indexes, attr_defs, limits)
        .map_err(dynamodb_validation_to_storage_error)
}

fn dynamodb_validation_to_storage_error(
    error: extenddb_core::error::DynamoDbError,
) -> StorageError {
    match error {
        extenddb_core::error::DynamoDbError::ValidationException(message) => {
            StorageError::Validation(message)
        }
        other => StorageError::Internal(other.to_string()),
    }
}

pub(crate) async fn create_native_secondary_indexes(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    indexes: &[NativeSecondaryIndex<'_>],
    attr_defs: &[AttributeDefinition],
) -> Result<(), StorageError> {
    if indexes.is_empty() {
        return Ok(());
    }

    let table = data_table_name(table_id);
    let columns = indexes
        .iter()
        .map(|index| generated_columns(index.index_id, index.key_schema, attr_defs))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let ddl = add_generated_columns_ddl(&table, &columns);
    execute_tidb_idempotent_ddl(pool, "create_native_secondary_index_add_columns", &ddl).await?;

    // Keep this as a second online DDL job. TiDB validates multi-change ALTER
    // statements against the starting schema, so ADD INDEX cannot safely refer
    // to generated columns added earlier in the same ALTER statement.
    let ddl = add_native_indexes_ddl(&table, indexes, attr_defs);
    execute_tidb_idempotent_ddl(pool, "create_native_secondary_indexes_add_indexes", &ddl).await?;

    Ok(())
}

fn add_generated_columns_ddl(table: &str, columns: &[GeneratedColumn]) -> String {
    let specs = columns
        .iter()
        .map(|column| {
            format!(
                "ADD COLUMN IF NOT EXISTS {}",
                generated_column_definition(column)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ALTER TABLE {table} {specs}")
}

fn generated_column_definition(column: &GeneratedColumn) -> String {
    format!(
        "`{}` {} AS ({}) VIRTUAL",
        column.name, column.ddl_type, column.expression
    )
}

pub(crate) fn native_index_generated_column_definitions(
    indexes: &[NativeSecondaryIndex<'_>],
    attr_defs: &[AttributeDefinition],
) -> Result<Vec<String>, StorageError> {
    let columns = indexes
        .iter()
        .map(|index| generated_columns(index.index_id, index.key_schema, attr_defs))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    Ok(columns
        .iter()
        .map(generated_column_definition)
        .collect::<Vec<_>>())
}

fn add_native_indexes_ddl(
    table: &str,
    indexes: &[NativeSecondaryIndex<'_>],
    attr_defs: &[AttributeDefinition],
) -> String {
    let specs = indexes
        .iter()
        .map(|index| {
            let (index_name, index_columns) = native_index_name_and_columns(index, attr_defs);
            format!(
                "ADD INDEX IF NOT EXISTS `{index_name}` ({})",
                index_columns.join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ALTER TABLE {table} {specs}")
}

pub(crate) fn native_index_create_table_definitions(
    indexes: &[NativeSecondaryIndex<'_>],
    attr_defs: &[AttributeDefinition],
) -> Vec<String> {
    indexes
        .iter()
        .map(|index| native_index_definition(index, attr_defs))
        .collect()
}

fn native_index_definition(
    index: &NativeSecondaryIndex<'_>,
    attr_defs: &[AttributeDefinition],
) -> String {
    let (index_name, index_columns) = native_index_name_and_columns(index, attr_defs);
    format!("INDEX `{index_name}` ({})", index_columns.join(", "))
}

fn native_index_name_and_columns(
    index: &NativeSecondaryIndex<'_>,
    attr_defs: &[AttributeDefinition],
) -> (String, Vec<String>) {
    (
        native_index_name(index.index_id),
        native_index_key_tuple_columns(index.index_id, index.key_schema, attr_defs),
    )
}

pub(crate) async fn drop_native_secondary_indexes(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    indexes: &[NativeSecondaryIndex<'_>],
    attr_defs: &[AttributeDefinition],
) -> Result<(), StorageError> {
    if indexes.is_empty() {
        return Ok(());
    }

    let table = data_table_name(table_id);
    let ddl = drop_native_indexes_and_columns_ddl(&table, indexes, attr_defs);
    execute_tidb_idempotent_ddl(pool, "drop_native_secondary_index_artifacts", &ddl).await?;

    Ok(())
}

fn drop_native_indexes_and_columns_ddl(
    table: &str,
    indexes: &[NativeSecondaryIndex<'_>],
    attr_defs: &[AttributeDefinition],
) -> String {
    let mut specs = indexes
        .iter()
        .map(|index| {
            let index_name = native_index_name(index.index_id);
            format!("DROP INDEX IF EXISTS `{index_name}`")
        })
        .collect::<Vec<_>>();
    specs.extend(
        indexes
            .iter()
            .flat_map(|index| {
                native_index_key_tuple_columns(index.index_id, index.key_schema, attr_defs)
            })
            .map(|column| format!("DROP COLUMN IF EXISTS `{column}`")),
    );
    format!("ALTER TABLE {table} {}", specs.join(", "))
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

fn generated_columns(
    index_id: &str,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<Vec<GeneratedColumn>, StorageError> {
    let mut columns = vec![GeneratedColumn {
        name: native_index_hash_column(index_id),
        ddl_type: DYNAMODB_HASH_KEY_COLUMN_TYPE,
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
            Ok(key_binary_expression(&ks.attribute_name, attr_type))
        })
        .collect::<Result<Vec<_>, StorageError>>()?;

    if hash_parts.len() == 1 {
        return Ok(hash_parts[0].clone());
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

fn key_binary_expression(attr_name: &str, attr_type: ScalarAttributeType) -> String {
    let scalar = key_scalar_expression(attr_name, attr_type);
    match attr_type {
        ScalarAttributeType::S | ScalarAttributeType::N => format!("CAST({scalar} AS BINARY)"),
        ScalarAttributeType::B => format!("FROM_BASE64({scalar})"),
    }
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
        ScalarAttributeType::S | ScalarAttributeType::B => DYNAMODB_SORT_KEY_COLUMN_TYPE,
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

pub(crate) fn native_index_name(index_id: &str) -> String {
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
    use std::collections::BTreeMap;

    use extenddb_core::limits::LimitsConfig;
    use extenddb_core::types::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    use super::{
        NativeSecondaryIndex, add_generated_columns_ddl, add_native_indexes_ddl,
        drop_native_indexes_and_columns_ddl, generated_columns, hash_key_expression,
        native_index_hash_column, native_index_key_tuple_columns,
        validate_item_index_key_constraints,
    };

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
        assert!(expr.contains("OCTET_LENGTH(CAST(JSON_UNQUOTE(JSON_EXTRACT"));
        assert!(expr.contains("OCTET_LENGTH(FROM_BASE64(JSON_UNQUOTE(JSON_EXTRACT"));
        assert!(expr.contains("$.\"a\".\"S\""));
        assert!(expr.contains("$.\"b\".\"B\""));
    }

    #[test]
    fn binary_hash_expression_uses_raw_bytes_not_base64_text() {
        let ks = vec![KeySchemaElement {
            attribute_name: "gpk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let attrs = vec![AttributeDefinition {
            attribute_name: "gpk".to_owned(),
            attribute_type: ScalarAttributeType::B,
        }];

        let expr = hash_key_expression(&ks, &attrs).expect("hash expression");

        assert!(expr.starts_with("FROM_BASE64(JSON_UNQUOTE(JSON_EXTRACT"));
        assert!(!expr.contains("CAST("));
    }

    #[test]
    fn generated_index_columns_are_added_in_one_online_ddl() {
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

        let columns = generated_columns("idx-1", &ks, &attrs).expect("generated columns");
        let ddl = add_generated_columns_ddl("`_ddb_table`", &columns);

        assert_eq!(ddl.matches("ADD COLUMN IF NOT EXISTS").count(), 2);
        assert!(ddl.starts_with("ALTER TABLE `_ddb_table` ADD COLUMN IF NOT EXISTS"));
        assert!(ddl.contains(", ADD COLUMN IF NOT EXISTS"));
        assert!(ddl.contains("`edbidx_idx1_pk` VARBINARY(2048) AS"));
        assert!(ddl.contains("`edbidx_idx1_sk_n` DECIMAL(65, 30) AS"));
    }

    #[test]
    fn multiple_indexes_share_one_generated_column_online_ddl() {
        let ks = vec![KeySchemaElement {
            attribute_name: "gpk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let attrs = vec![AttributeDefinition {
            attribute_name: "gpk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];

        let mut columns = generated_columns("idx-1", &ks, &attrs).expect("first index columns");
        columns.extend(generated_columns("idx-2", &ks, &attrs).expect("second index columns"));
        let ddl = add_generated_columns_ddl("`_ddb_table`", &columns);

        assert_eq!(ddl.matches("ALTER TABLE").count(), 1);
        assert_eq!(ddl.matches("ADD COLUMN IF NOT EXISTS").count(), 2);
        assert!(ddl.contains("`edbidx_idx1_pk` VARBINARY(2048) AS"));
        assert!(ddl.contains("`edbidx_idx2_pk` VARBINARY(2048) AS"));
    }

    #[test]
    fn multiple_native_indexes_are_created_in_one_online_ddl() {
        let index_ks = vec![KeySchemaElement {
            attribute_name: "gpk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let attrs = vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::N,
            },
            AttributeDefinition {
                attribute_name: "gpk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ];
        let indexes = vec![
            NativeSecondaryIndex {
                index_id: "idx-1",
                key_schema: &index_ks,
            },
            NativeSecondaryIndex {
                index_id: "idx-2",
                key_schema: &index_ks,
            },
        ];

        let ddl = add_native_indexes_ddl("`_ddb_table`", &indexes, &attrs);

        assert_eq!(ddl.matches("ALTER TABLE").count(), 1);
        assert_eq!(ddl.matches("ADD INDEX IF NOT EXISTS").count(), 2);
        assert!(ddl.starts_with("ALTER TABLE `_ddb_table` ADD INDEX IF NOT EXISTS"));
        assert!(ddl.contains("`idx_idx1` (edbidx_idx1_pk)"));
        assert!(ddl.contains(", ADD INDEX IF NOT EXISTS `idx_idx2`"));
    }

    #[test]
    fn multiple_native_indexes_are_dropped_in_one_online_ddl() {
        let ks = vec![KeySchemaElement {
            attribute_name: "gpk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let attrs = vec![AttributeDefinition {
            attribute_name: "gpk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];
        let indexes = vec![
            NativeSecondaryIndex {
                index_id: "idx-1",
                key_schema: &ks,
            },
            NativeSecondaryIndex {
                index_id: "idx-2",
                key_schema: &ks,
            },
        ];

        let ddl = drop_native_indexes_and_columns_ddl("`_ddb_table`", &indexes, &attrs);

        assert_eq!(ddl.matches("ALTER TABLE").count(), 1);
        assert_eq!(ddl.matches("DROP INDEX IF EXISTS").count(), 2);
        assert_eq!(ddl.matches("DROP COLUMN IF EXISTS").count(), 2);
        assert!(ddl.starts_with("ALTER TABLE `_ddb_table` DROP INDEX IF EXISTS"));
        assert!(ddl.contains("`idx_idx1`"));
        assert!(ddl.contains(", DROP INDEX IF EXISTS `idx_idx2`"));
        assert!(ddl.contains(", DROP COLUMN IF EXISTS `edbidx_idx1_pk`"));
        assert!(ddl.contains(", DROP COLUMN IF EXISTS `edbidx_idx2_pk`"));
    }

    #[test]
    fn secondary_index_validation_rejects_empty_binary_key_values() {
        let indexes = vec![vec![KeySchemaElement {
            attribute_name: "gpk".to_owned(),
            key_type: KeyType::Hash,
        }]];
        let attr_defs = vec![AttributeDefinition {
            attribute_name: "gpk".to_owned(),
            attribute_type: ScalarAttributeType::B,
        }];
        let item = BTreeMap::from([("gpk".to_owned(), AttributeValue::B(Vec::new()))]);

        let err = validate_item_index_key_constraints(
            &item,
            &indexes,
            &attr_defs,
            &LimitsConfig::default(),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("key attribute cannot contain an empty binary value")
        );
    }

    #[test]
    fn secondary_index_validation_rejects_oversized_hash_key_values() {
        let indexes = vec![vec![KeySchemaElement {
            attribute_name: "gpk".to_owned(),
            key_type: KeyType::Hash,
        }]];
        let attr_defs = vec![AttributeDefinition {
            attribute_name: "gpk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];
        let item = BTreeMap::from([("gpk".to_owned(), AttributeValue::S("x".repeat(2049)))]);

        let err = validate_item_index_key_constraints(
            &item,
            &indexes,
            &attr_defs,
            &LimitsConfig::default(),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("partition key size must be between 1 and 2048 bytes")
        );
    }

    #[test]
    fn secondary_index_validation_rejects_oversized_sort_key_values() {
        let indexes = vec![vec![
            KeySchemaElement {
                attribute_name: "gpk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsk".to_owned(),
                key_type: KeyType::Range,
            },
        ]];
        let attr_defs = vec![
            AttributeDefinition {
                attribute_name: "gpk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ];
        let item = BTreeMap::from([
            ("gpk".to_owned(), AttributeValue::S("ok".to_owned())),
            ("gsk".to_owned(), AttributeValue::S("x".repeat(1025))),
        ]);

        let err = validate_item_index_key_constraints(
            &item,
            &indexes,
            &attr_defs,
            &LimitsConfig::default(),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("sort key size must be between 1 and 1024 bytes")
        );
    }

    #[test]
    fn secondary_index_validation_rejects_oversized_multipart_hash_tuple() {
        let indexes = vec![vec![
            KeySchemaElement {
                attribute_name: "gpk1".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gpk2".to_owned(),
                key_type: KeyType::Hash,
            },
        ]];
        let attr_defs = vec![
            AttributeDefinition {
                attribute_name: "gpk1".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gpk2".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ];
        let item = BTreeMap::from([
            ("gpk1".to_owned(), AttributeValue::S("x".repeat(1020))),
            ("gpk2".to_owned(), AttributeValue::S("y".repeat(1020))),
        ]);

        let err = validate_item_index_key_constraints(
            &item,
            &indexes,
            &attr_defs,
            &LimitsConfig::default(),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("partition key size must be between 1 and 2048 bytes")
        );
    }
}
