// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `query` and `scan` implementations for the `TiDB` backend.

use extenddb_core::expression::PathElement;
use extenddb_core::expression::{ExpressionMaps, KeyCondition};
use extenddb_core::types::{
    AttributeDefinition, IndexInfo, Item, KeySchemaElement, ScalarAttributeType, TableKeyInfo,
    combined_lek_key_schema,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{sk_column, sk_column_n, sk_info};
use extenddb_storage::{ExportTableItemsSummary, ItemExportSink};
use futures::TryStreamExt;

use super::index::{
    native_index_hash_column, native_index_key_tuple_columns, native_index_name,
    native_index_non_null_predicates, native_index_sort_columns,
};
use super::query::{
    build_key, build_sk_sql, execute_query_sql, execute_scan_sql, resolve_expr_to_av,
    scan_segment_range,
};
use super::{all_sort_key_info, data_table_name, json_to_item, physical_pk_bytes_from_values};
use crate::TidbEngine;
use crate::tidb_util::{
    current_tidb_tso, map_tidb_snapshot_read_sqlx_error, tidb_as_of_epoch_clause,
    tidb_as_of_tso_clause,
};

fn sort_key_columns(
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Vec<String> {
    all_sort_key_info(key_schema, attr_defs)
        .into_iter()
        .enumerate()
        .map(|(i, (_, sk_type))| sk_column_n(i, sk_type))
        .collect()
}

fn key_tuple_columns(
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Vec<String> {
    let mut columns = vec!["pk".to_owned()];
    columns.extend(sort_key_columns(key_schema, attr_defs));
    columns
}

fn tuple_comparison(columns: &[String], op: &str) -> String {
    debug_assert!(!columns.is_empty());
    if columns.len() == 1 {
        format!("{} {op} ?", columns[0])
    } else {
        format!(
            "({}) {op} ({})",
            columns.join(", "),
            std::iter::repeat_n("?", columns.len())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

fn push_order_by(sql: &mut String, columns: &[String], forward: bool) {
    if columns.is_empty() {
        return;
    }
    let dir = if forward { "ASC" } else { "DESC" };
    sql.push_str(" ORDER BY ");
    sql.push_str(
        &columns
            .iter()
            .map(|col| format!("{col} {dir}"))
            .collect::<Vec<_>>()
            .join(", "),
    );
}

struct CursorColumns {
    seek: Vec<String>,
    order: Vec<String>,
}

impl CursorColumns {
    fn same(columns: Vec<String>) -> Self {
        Self {
            seek: columns.clone(),
            order: columns,
        }
    }
}

fn native_index_query_cursor_columns(
    sort_columns: &[String],
    base_key_schema: &[KeySchemaElement],
    base_attr_defs: &[AttributeDefinition],
) -> CursorColumns {
    // TiDB secondary-index entries are ordered by the native index key followed
    // by the clustered primary-key handle. Make the handle columns explicit in
    // ORDER BY so LastEvaluatedKey pagination remains deterministic when many
    // items share the same index key. The index hash column is already fixed by
    // the equality predicate for Query, so only range columns plus the handle
    // participate in the ordered cursor.
    let mut columns = sort_columns.to_vec();
    columns.extend(key_tuple_columns(base_key_schema, base_attr_defs));
    CursorColumns::same(columns)
}

fn native_index_scan_cursor_columns(
    index_columns: Vec<String>,
    base_key_schema: &[KeySchemaElement],
    base_attr_defs: &[AttributeDefinition],
) -> CursorColumns {
    let mut columns = index_columns;
    columns.extend(key_tuple_columns(base_key_schema, base_attr_defs));
    CursorColumns::same(columns)
}

fn key_condition_attribute_name(
    path: &[PathElement],
    maps: &ExpressionMaps,
) -> Result<String, StorageError> {
    match path.first() {
        Some(PathElement::Attribute(name)) => {
            if let Some(ref_name) = name.strip_prefix('#') {
                maps.names.get(ref_name).cloned().ok_or_else(|| {
                    StorageError::Validation(format!(
                        "An expression attribute name used in the document path \
                         is not defined; attribute name: #{ref_name}"
                    ))
                })
            } else {
                Ok(name.clone())
            }
        }
        _ => Err(StorageError::Validation(
            "Invalid key condition path".to_owned(),
        )),
    }
}

fn table_ref_for_read(table_id: &str, index: Option<&IndexInfo>) -> String {
    let table = data_table_name(table_id);
    match index {
        Some(idx) => {
            let index_name = native_index_name(&idx.index_id);
            format!("{table} FORCE INDEX (`{index_name}`)")
        }
        None => table,
    }
}

fn last_evaluated_key(
    item: &Item,
    base_key_schema: &[KeySchemaElement],
    index: Option<&IndexInfo>,
) -> Item {
    build_key(item, &combined_lek_key_schema(base_key_schema, index))
}

fn tidb_export_sql(table_id: &str, snapshot_clause: &str, order_columns: &[String]) -> String {
    let table = data_table_name(table_id);
    format!(
        "SELECT item_data FROM {table} {snapshot_clause} ORDER BY {}",
        order_columns.join(", ")
    )
}

impl TidbEngine {
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
        consistent_read: bool,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        use std::fmt::Write;

        let read_key_schema = index.map_or(key_info.key_schema.as_slice(), |idx| {
            idx.key_schema.as_slice()
        });
        let attr_defs = key_info.attribute_definitions.as_slice();
        let ddb_table = table_ref_for_read(&key_info.table_id, index);
        let pk_column = index.map_or_else(
            || "pk".to_owned(),
            |idx| native_index_hash_column(&idx.index_id),
        );
        let sort_columns = index.map_or_else(
            || sort_key_columns(read_key_schema, attr_defs),
            |idx| native_index_sort_columns(&idx.index_id, read_key_schema, attr_defs),
        );

        let first_pk_value = resolve_expr_to_av(&key_condition.pk_value, maps)?;
        let mut pk_values = Vec::with_capacity(1 + key_condition.extra_pk_conditions.len());
        pk_values.push(&first_pk_value);
        let extra_pk_values = key_condition
            .extra_pk_conditions
            .iter()
            .map(|(_, value)| resolve_expr_to_av(value, maps))
            .collect::<Result<Vec<_>, _>>()?;
        pk_values.extend(extra_pk_values.iter());
        let pk = physical_pk_bytes_from_values(&pk_values)?;

        let sk_info_val = sk_info(read_key_schema, attr_defs);
        let all_sks = all_sort_key_info(read_key_schema, attr_defs);

        // Build SQL query
        let mut sql = format!("SELECT item_data FROM {ddb_table} WHERE {pk_column} = ?");
        let mut param_idx: u32 = 2;

        if let Some(idx) = index {
            for predicate in
                native_index_non_null_predicates(&idx.index_id, read_key_schema, attr_defs)
            {
                sql.push_str(" AND ");
                sql.push_str(&predicate);
            }
        }

        // Sort key condition SQL fragment (first RANGE key).
        let sk_sql_info = if let (Some(sk_cond), Some((_, sk_type))) =
            (&key_condition.sk_condition, sk_info_val)
        {
            let sk_col = if index.is_some() {
                sort_columns
                    .first()
                    .cloned()
                    .unwrap_or_else(|| sk_column(sk_type).to_owned())
            } else {
                sk_column(sk_type).to_owned()
            };
            Some(build_sk_sql(
                sk_cond,
                &sk_col,
                sk_type,
                maps,
                &mut param_idx,
            )?)
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
            let attr_name = key_condition_attribute_name(path, maps)?;
            // Find which RANGE key index this attribute corresponds to
            if let Some(pos) = all_sks
                .iter()
                .position(|(sk_name, _)| *sk_name == attr_name)
            {
                // Skip index 0 — that's the primary SK handled above
                if pos > 0 {
                    let (_, sk_type) = all_sks[pos];
                    let col = if index.is_some() {
                        sort_columns
                            .get(pos)
                            .cloned()
                            .unwrap_or_else(|| sk_column_n(pos, sk_type))
                    } else {
                        sk_column_n(pos, sk_type)
                    };
                    let _ = write!(sql, " AND {col} = ?");
                    param_idx += 1;
                    extra_sk_col_indices.push((pos, sk_type));
                }
            }
        }

        let cursor_columns = if index.is_some() {
            native_index_query_cursor_columns(
                &sort_columns,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
        } else {
            CursorColumns::same(sort_columns.clone())
        };

        if exclusive_start_key.is_some() {
            if cursor_columns.seek.is_empty() {
                return Ok((Vec::new(), None));
            }
            let op = if forward { ">" } else { "<" };
            let _ = write!(sql, " AND {}", tuple_comparison(&cursor_columns.seek, op));
        }

        push_order_by(&mut sql, &cursor_columns.order, forward);

        // LIMIT — fetch one extra to detect pagination
        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        // Execute with dynamic bindings
        let rows = execute_query_sql(
            &sql,
            &pk,
            key_condition,
            maps,
            read_key_schema,
            attr_defs,
            sk_info_val,
            &extra_sk_col_indices,
            exclusive_start_key,
            index.map(|_| {
                (
                    key_info.key_schema.as_slice(),
                    key_info.attribute_definitions.as_slice(),
                )
            }),
            self.data_read_pool(consistent_read),
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn scan_impl(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index: Option<&IndexInfo>,
        consistent_read: bool,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        use std::fmt::Write;

        let read_key_schema = index.map_or(key_info.key_schema.as_slice(), |idx| {
            idx.key_schema.as_slice()
        });
        let attr_defs = key_info.attribute_definitions.as_slice();
        let ddb_table = table_ref_for_read(&key_info.table_id, index);

        let mut sql = format!("SELECT item_data FROM {ddb_table}");
        let mut conditions: Vec<String> = Vec::new();
        if let Some(idx) = index {
            conditions.extend(native_index_non_null_predicates(
                &idx.index_id,
                read_key_schema,
                attr_defs,
            ));
        }
        let segment_range = if let (Some(seg), Some(total)) = (segment, total_segments) {
            Some(scan_segment_range(seg, total)?)
        } else {
            None
        };

        // Parallel scan: native range segments over the base clustered key or
        // the selected native secondary-index partition-key column. This keeps
        // each segment disjoint while allowing TiDB to seek a key range instead
        // of evaluating a row-by-row hash/modulo predicate.
        if let Some(range) = &segment_range {
            let segment_column = index.map_or_else(
                || "pk".to_owned(),
                |idx| native_index_hash_column(&idx.index_id),
            );
            conditions.extend(range.predicates(&segment_column));
        }

        let cursor_columns = if let Some(idx) = index {
            let index_columns =
                native_index_key_tuple_columns(&idx.index_id, read_key_schema, attr_defs);
            native_index_scan_cursor_columns(
                index_columns,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
        } else {
            CursorColumns::same(key_tuple_columns(read_key_schema, attr_defs))
        };

        if let Some(start_key) = exclusive_start_key {
            let pk_name = &read_key_schema[0].attribute_name;
            if !start_key.contains_key(pk_name) {
                return Err(StorageError::Validation(
                    "The provided starting key is invalid: The provided key element does not match the schema".to_owned(),
                ));
            }
            conditions.push(tuple_comparison(&cursor_columns.seek, ">"));
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        let _ = write!(sql, " ORDER BY {}", cursor_columns.order.join(", "));

        let fetch_limit = limit.map_or(1_000_001, |l| l + 1);
        let _ = write!(sql, " LIMIT {fetch_limit}");

        // Execute
        let rows = execute_scan_sql(
            &sql,
            segment_range.as_ref(),
            exclusive_start_key,
            read_key_schema,
            attr_defs,
            index.map(|_| {
                (
                    key_info.key_schema.as_slice(),
                    key_info.attribute_definitions.as_slice(),
                )
            }),
            self.data_read_pool(consistent_read),
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
        let snapshot_clause = if let Some(export_time) = export_time_epoch {
            tidb_as_of_epoch_clause(export_time)?
        } else {
            tidb_as_of_tso_clause(current_tidb_tso(&self.data_pool).await?)?
        };
        let sql = tidb_export_sql(
            &key_info.table_id,
            &snapshot_clause,
            &key_tuple_columns(&key_info.key_schema, &key_info.attribute_definitions),
        );
        let mut rows =
            sqlx::query_as::<_, (serde_json::Value,)>(&sql).fetch(self.data_read_pool(false));
        let mut item_count = 0_u64;
        while let Some((row,)) = rows
            .try_next()
            .await
            .map_err(map_tidb_snapshot_read_sqlx_error)?
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
        AttributeDefinition, AttributeValue, IndexInfo, IndexType, Item, KeySchemaElement, KeyType,
        Projection, ProjectionType, ScalarAttributeType,
    };

    use super::{
        key_condition_attribute_name, last_evaluated_key, native_index_query_cursor_columns,
        native_index_scan_cursor_columns, push_order_by, table_ref_for_read, tidb_export_sql,
        tuple_comparison,
    };
    use crate::tidb_util::{tidb_as_of_epoch_clause, tidb_as_of_tso_clause};

    #[test]
    fn tuple_comparison_uses_all_cursor_columns() {
        let cols = vec![
            "sk_s".to_owned(),
            "base_pk".to_owned(),
            "base_sk_n".to_owned(),
        ];
        assert_eq!(
            tuple_comparison(&cols, ">"),
            "(sk_s, base_pk, base_sk_n) > (?, ?, ?)"
        );
    }

    #[test]
    fn tuple_comparison_keeps_single_column_syntax_simple() {
        let cols = vec!["base_pk".to_owned()];
        assert_eq!(tuple_comparison(&cols, "<"), "base_pk < ?");
    }

    #[test]
    fn key_condition_attribute_name_rejects_unresolved_name_reference() {
        let maps = extenddb_core::expression::ExpressionMaps::default();
        let err = key_condition_attribute_name(
            &[extenddb_core::expression::PathElement::Attribute(
                "#missing".to_owned(),
            )],
            &maps,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("expression attribute name used in the document path is not defined"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn native_index_query_orders_by_index_sort_columns_then_clustered_handle() {
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
        let base_attrs = vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::N,
            },
        ];
        let sort_columns = vec!["edbidx_idx1_sk_s".to_owned()];

        let columns = native_index_query_cursor_columns(&sort_columns, &base_key, &base_attrs);

        assert_eq!(
            columns.seek,
            vec![
                "edbidx_idx1_sk_s".to_owned(),
                "pk".to_owned(),
                "sk_n".to_owned()
            ]
        );
        assert_eq!(
            columns.order,
            vec![
                "edbidx_idx1_sk_s".to_owned(),
                "pk".to_owned(),
                "sk_n".to_owned()
            ]
        );
    }

    #[test]
    fn native_index_query_sql_order_by_includes_clustered_handle() {
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
        let base_attrs = vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::N,
            },
        ];
        let sort_columns = vec!["edbidx_idx1_sk_s".to_owned()];
        let columns = native_index_query_cursor_columns(&sort_columns, &base_key, &base_attrs);
        let mut sql = "SELECT item_data FROM `_ddb_table` WHERE edbidx_idx1_pk = ?".to_owned();

        push_order_by(&mut sql, &columns.order, true);

        assert!(sql.ends_with(" ORDER BY edbidx_idx1_sk_s ASC, pk ASC, sk_n ASC"));
    }

    #[test]
    fn secondary_index_reads_force_the_requested_native_index() {
        let index = IndexInfo {
            index_name: "by_customer".to_owned(),
            index_id: "idx-1".to_owned(),
            index_type: IndexType::Gsi,
            key_schema: vec![KeySchemaElement {
                attribute_name: "gpk".to_owned(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        };

        assert_eq!(
            table_ref_for_read("tableid", Some(&index)),
            "`_ddb_tableid` FORCE INDEX (`idx_idx1`)"
        );
        assert_eq!(table_ref_for_read("tableid", None), "`_ddb_tableid`");
    }

    #[test]
    fn export_sql_uses_tidb_current_tso_snapshot_when_export_time_is_absent() {
        let sql = tidb_export_sql(
            "tableid",
            &tidb_as_of_tso_clause(466_712_376_294_768_640).unwrap(),
            &["pk".to_owned(), "sk_s".to_owned()],
        );

        assert_eq!(
            sql,
            "SELECT item_data FROM `_ddb_tableid` AS OF TIMESTAMP TIDB_PARSE_TSO(466712376294768640) ORDER BY pk, sk_s"
        );
    }

    #[test]
    fn export_sql_uses_tidb_epoch_snapshot_when_export_time_is_present() {
        let sql = tidb_export_sql(
            "tableid",
            &tidb_as_of_epoch_clause(1_717_171_717.123456).unwrap(),
            &["pk".to_owned()],
        );

        assert_eq!(
            sql,
            "SELECT item_data FROM `_ddb_tableid` AS OF TIMESTAMP FROM_UNIXTIME(1717171717.123456) ORDER BY pk"
        );
    }

    #[test]
    fn native_index_last_evaluated_key_includes_base_and_index_keys() {
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
    fn native_hash_only_index_query_orders_by_clustered_handle() {
        let base_key = vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let base_attrs = vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];
        let columns = native_index_query_cursor_columns(&[], &base_key, &base_attrs);

        assert_eq!(columns.seek, vec!["pk".to_owned()]);
        assert_eq!(columns.order, vec!["pk".to_owned()]);
    }

    #[test]
    fn native_index_scan_orders_by_index_columns_then_clustered_handle() {
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
        let base_attrs = vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::N,
            },
        ];
        let index_columns = vec!["edbidx_idx1_pk".to_owned(), "edbidx_idx1_sk_s".to_owned()];

        let columns =
            native_index_scan_cursor_columns(index_columns.clone(), &base_key, &base_attrs);

        assert_eq!(
            columns.seek,
            vec![
                "edbidx_idx1_pk".to_owned(),
                "edbidx_idx1_sk_s".to_owned(),
                "pk".to_owned(),
                "sk_n".to_owned()
            ]
        );
        assert_eq!(
            columns.order,
            vec![
                "edbidx_idx1_pk".to_owned(),
                "edbidx_idx1_sk_s".to_owned(),
                "pk".to_owned(),
                "sk_n".to_owned()
            ]
        );
    }
}
