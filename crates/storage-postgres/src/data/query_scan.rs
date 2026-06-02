// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `query` and `scan` implementations for the `PostgreSQL` backend.

use extenddb_core::expression::{ExpressionMaps, KeyCondition};
use extenddb_core::types::{
    IndexInfo, Item, KeySchemaElement, ScalarAttributeType, TableKeyInfo, combined_lek_key_schema,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    encode_netstring_composite, pk_to_text, sk_column, sk_column_n, sk_info,
};
use extenddb_storage::{ExportTableItemsSummary, ItemExportSink};
use futures::TryStreamExt;

use super::query::{
    build_key, build_sk_sql, execute_query_sql, execute_scan_sql, resolve_expr_to_av,
};
use super::{all_sort_key_info, data_table_name, index_table_name, json_to_item};
use crate::PostgresEngine;

fn last_evaluated_key(
    item: &Item,
    base_key_schema: &[KeySchemaElement],
    index: Option<&IndexInfo>,
) -> Item {
    build_key(item, &combined_lek_key_schema(base_key_schema, index))
}

fn export_order_columns(
    key_schema: &[KeySchemaElement],
    attr_defs: &[extenddb_core::types::AttributeDefinition],
) -> Vec<String> {
    let mut columns = vec!["pk".to_owned()];
    columns.extend(
        all_sort_key_info(key_schema, attr_defs)
            .into_iter()
            .enumerate()
            .map(|(i, (_, sk_type))| {
                let column = sk_column_n(i, sk_type);
                if sk_type == ScalarAttributeType::S {
                    format!("{column} COLLATE \"C\"")
                } else {
                    column
                }
            }),
    );
    columns
}

fn postgres_export_sql(table_id: &str, order_columns: &[String]) -> String {
    let table = data_table_name(table_id);
    format!(
        "SELECT item_data FROM {table} ORDER BY {}",
        order_columns.join(", ")
    )
}

impl PostgresEngine {
    /// Implementation of `DataEngine::query`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn query_impl(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index: Option<&IndexInfo>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        use std::fmt::Write;

        let read_key_schema = index.map_or(key_info.key_schema.as_slice(), |idx| {
            idx.key_schema.as_slice()
        });
        let attr_defs = key_info.attribute_definitions.as_slice();
        let ddb_table = if let Some(idx) = index {
            index_table_name(&idx.index_id)
        } else {
            data_table_name(&key_info.table_id)
        };

        // Resolve partition key value(s) — for multi-part keys, encode
        // all HASH attribute values into a single composite PK text using
        // netstring encoding (matching the write path in composite_pk_to_text).
        let pk_text = if key_condition.extra_pk_conditions.is_empty() {
            let pk_expr_val = resolve_expr_to_av(&key_condition.pk_value, maps)?;
            pk_to_text(&pk_expr_val)?.into_owned()
        } else {
            let mut parts = Vec::with_capacity(1 + key_condition.extra_pk_conditions.len());
            let first_val = resolve_expr_to_av(&key_condition.pk_value, maps)?;
            parts.push(pk_to_text(&first_val)?.into_owned());
            for (_, value) in &key_condition.extra_pk_conditions {
                let val = resolve_expr_to_av(value, maps)?;
                parts.push(pk_to_text(&val)?.into_owned());
            }
            encode_netstring_composite(&parts)
        };

        let sk_info_val = sk_info(read_key_schema, attr_defs);
        let all_sks = all_sort_key_info(read_key_schema, attr_defs);

        // Build SQL query
        let mut sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1");
        let mut param_idx: u32 = 2;

        // Sort key condition SQL fragment (first RANGE key).
        let sk_sql_info = if let (Some(sk_cond), Some((_, sk_type))) =
            (&key_condition.sk_condition, sk_info_val)
        {
            Some(build_sk_sql(sk_cond, sk_column(sk_type), &mut param_idx))
        } else {
            None
        };

        if let Some(ref info) = sk_sql_info {
            sql.push_str(&info.fragment);
        }

        // Extra RANGE key equality conditions (multi-RANGE key schemas).
        // Each extra SK condition is an equality on an additional RANGE attribute.
        let mut extra_sk_col_indices: Vec<(usize, ScalarAttributeType)> = Vec::new();
        for (path, _value) in &key_condition.extra_sk_conditions {
            let attr_name = match path.first() {
                Some(extenddb_core::expression::PathElement::Attribute(name)) => {
                    if let Some(ref_name) = name.strip_prefix('#') {
                        match maps.names.get(ref_name) {
                            Some(resolved) => resolved.clone(),
                            None => {
                                tracing::warn!(name_ref = %ref_name, "unresolved expression attribute name in extra SK condition, skipping");
                                continue;
                            }
                        }
                    } else {
                        name.clone()
                    }
                }
                _ => continue,
            };
            // Find which RANGE key index this attribute corresponds to
            if let Some(pos) = all_sks
                .iter()
                .position(|(sk_name, _)| *sk_name == attr_name)
            {
                // Skip index 0 — that's the primary SK handled above
                if pos > 0 {
                    let (_, sk_type) = all_sks[pos];
                    let col = sk_column_n(pos, sk_type);
                    let _ = write!(sql, " AND {col} = ${param_idx}");
                    param_idx += 1;
                    extra_sk_col_indices.push((pos, sk_type));
                }
            }
        }

        // Pagination: exclusive start key
        if let (Some(_), Some((_, sk_type))) = (exclusive_start_key, sk_info_val) {
            let sk_col = sk_column(sk_type);
            let collate = if sk_type == ScalarAttributeType::S {
                " COLLATE \"C\""
            } else {
                ""
            };
            if forward {
                let _ = write!(sql, " AND {sk_col}{collate} > ${param_idx}");
            } else {
                let _ = write!(sql, " AND {sk_col}{collate} < ${param_idx}");
            }
        } else if exclusive_start_key.is_some() && sk_info_val.is_none() {
            // PK-only table with start key — no more items for this PK
            return Ok((Vec::new(), None));
        }

        // ORDER BY — use COLLATE "C" for string sort keys to match DynamoDB
        // UTF-8 byte order.
        if let Some((_, sk_type)) = sk_info_val {
            let sk_col = sk_column(sk_type);
            let collate = if sk_type == ScalarAttributeType::S {
                " COLLATE \"C\""
            } else {
                ""
            };
            let dir = if forward { "ASC" } else { "DESC" };
            let _ = write!(sql, " ORDER BY {sk_col}{collate} {dir}");
        }

        // LIMIT — fetch one extra to detect pagination
        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        // Execute with dynamic bindings
        let rows = execute_query_sql(
            &sql,
            &pk_text,
            key_condition,
            maps,
            sk_info_val,
            &extra_sk_col_indices,
            exclusive_start_key,
            &self.data_pool,
        )
        .await?;

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let actual_limit = limit.map_or(1_000_000_usize, |l| l.max(0) as usize);
        let has_more = rows.len() > actual_limit;
        let items: Vec<Item> = rows
            .into_iter()
            .take(actual_limit)
            .map(json_to_item)
            .collect::<Result<Vec<_>, _>>()?;

        let last_key = if has_more {
            items
                .last()
                .map(|item| last_evaluated_key(item, &key_info.key_schema, index))
        } else {
            None
        };

        Ok((items, last_key))
    }

    /// Implementation of `DataEngine::scan`.
    pub(crate) async fn scan_impl(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index: Option<&IndexInfo>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        use std::fmt::Write;

        let read_key_schema = index.map_or(key_info.key_schema.as_slice(), |idx| {
            idx.key_schema.as_slice()
        });
        let attr_defs = key_info.attribute_definitions.as_slice();
        let ddb_table = if let Some(idx) = index {
            index_table_name(&idx.index_id)
        } else {
            data_table_name(&key_info.table_id)
        };
        let sk_info_val = sk_info(read_key_schema, attr_defs);

        let mut sql = format!("SELECT item_data FROM {ddb_table}");
        let mut conditions: Vec<String> = Vec::new();
        let param_idx: u32 = 1;

        // Parallel scan: hash-based segment assignment.
        // CB-20 / SP-SCN-002: use bigint bitmask instead of abs() to avoid
        // SQL error 22003 on the one-in-4-billion hashtext() == i32::MIN case.
        if let (Some(seg), Some(total)) = (segment, total_segments) {
            conditions.push(format!(
                "(hashtext(pk)::bigint & 2147483647) % {total} = {seg}"
            ));
        }

        // Pagination via exclusive start key
        if let Some(start_key) = exclusive_start_key {
            let pk_name = &read_key_schema[0].attribute_name;
            if !start_key.contains_key(pk_name) {
                return Err(StorageError::Validation(
                    "The provided starting key is invalid: The provided key element does not match the schema".to_owned(),
                ));
            }
            // Actual PK/SK binding happens in execute_scan_sql.

            if let Some((_, sk_type)) = sk_info_val {
                let sk_col = sk_column(sk_type);
                let collate = if sk_type == ScalarAttributeType::S {
                    " COLLATE \"C\""
                } else {
                    ""
                };
                conditions.push(format!(
                    "(pk, {sk_col}{collate}) > (${param_idx}, ${next})",
                    next = param_idx + 1
                ));
            } else {
                conditions.push(format!("pk > ${param_idx}"));
            }
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        // Deterministic ordering for pagination — COLLATE "C" for string
        // sort keys to match DynamoDB UTF-8 byte order.
        if let Some((_, sk_type)) = sk_info_val {
            let sk_col = sk_column(sk_type);
            let collate = if sk_type == ScalarAttributeType::S {
                " COLLATE \"C\""
            } else {
                ""
            };
            let _ = write!(sql, " ORDER BY pk, {sk_col}{collate}");
        } else {
            sql.push_str(" ORDER BY pk");
        }

        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        // Execute
        let rows = execute_scan_sql(
            &sql,
            exclusive_start_key,
            read_key_schema,
            attr_defs,
            &self.data_pool,
        )
        .await?;

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let actual_limit = limit.map_or(1_000_000_usize, |l| l.max(0) as usize);
        let has_more = rows.len() > actual_limit;
        let items: Vec<Item> = rows
            .into_iter()
            .take(actual_limit)
            .map(json_to_item)
            .collect::<Result<Vec<_>, _>>()?;

        let last_key = if has_more {
            items
                .last()
                .map(|item| last_evaluated_key(item, &key_info.key_schema, index))
        } else {
            None
        };

        Ok((items, last_key))
    }

    pub(crate) async fn export_table_items_impl(
        &self,
        key_info: &TableKeyInfo,
        export_time_epoch: Option<f64>,
        max_items: u64,
        sink: &mut dyn ItemExportSink,
    ) -> Result<ExportTableItemsSummary, StorageError> {
        if export_time_epoch.is_some() {
            return Err(StorageError::Validation(
                "PostgreSQL backend does not support ExportTime point-in-time export".to_owned(),
            ));
        }

        let sql = postgres_export_sql(
            &key_info.table_id,
            &export_order_columns(&key_info.key_schema, &key_info.attribute_definitions),
        );
        let mut rows = sqlx::query_as::<_, (serde_json::Value,)>(&sql).fetch(&self.data_pool);
        let mut item_count = 0_u64;
        while let Some((row,)) = rows
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            item_count = item_count.saturating_add(1);
            if item_count > max_items {
                return Err(StorageError::Validation(format!(
                    "Export item count exceeds maximum ({max_items})"
                )));
            }
            let item = json_to_item(row)?;
            sink.write_item(&item).await?;
        }

        Ok(ExportTableItemsSummary {
            item_count: i64::try_from(item_count).unwrap_or(i64::MAX),
        })
    }
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::{
        AttributeValue, IndexInfo, IndexType, Item, KeySchemaElement, KeyType, Projection,
        ProjectionType, ScalarAttributeType,
    };

    use super::{export_order_columns, last_evaluated_key, postgres_export_sql};

    #[test]
    fn index_last_evaluated_key_includes_base_and_index_keys() {
        let base_key = vec![
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            },
        ];
        let index = IndexInfo {
            index_name: "by_customer".to_owned(),
            index_id: "idx-1".to_owned(),
            index_type: IndexType::Gsi,
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gpk".to_owned(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsk".to_owned(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        };
        let item = Item::from([
            ("pk".to_owned(), AttributeValue::S("p1".to_owned())),
            ("sk".to_owned(), AttributeValue::S("s1".to_owned())),
            ("gpk".to_owned(), AttributeValue::S("g1".to_owned())),
            ("gsk".to_owned(), AttributeValue::S("r1".to_owned())),
            (
                "payload".to_owned(),
                AttributeValue::S("ignored".to_owned()),
            ),
        ]);

        let key = last_evaluated_key(&item, &base_key, Some(&index));

        assert_eq!(key.len(), 4);
        assert!(key.contains_key("pk"));
        assert!(key.contains_key("sk"));
        assert!(key.contains_key("gpk"));
        assert!(key.contains_key("gsk"));
        assert!(!key.contains_key("payload"));
    }

    #[test]
    fn export_sql_orders_by_full_base_key_with_binary_string_collation() {
        let key_schema = vec![
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            },
        ];
        let attr_defs = vec![
            extenddb_core::types::AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            extenddb_core::types::AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ];

        let sql = postgres_export_sql("tableid", &export_order_columns(&key_schema, &attr_defs));

        assert_eq!(
            sql,
            "SELECT item_data FROM \"_ddb_tableid\" ORDER BY pk, sk_s COLLATE \"C\""
        );
    }
}
