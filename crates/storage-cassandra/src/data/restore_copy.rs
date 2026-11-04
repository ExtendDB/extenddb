// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The restore copy: reading a backup's payload buckets and writing their
//! items into the target.
//!
//! A restore target is `CREATING` for the whole copy: no client request
//! reaches it, and the copy is the only writer. The ordinary put path reads
//! the row first, fences the write with a lightweight transaction and carries
//! the index rows in a logged batch; none of that protects anything here, and
//! each piece costs a round trip. This path writes an item as plain `INSERT`s
//! with no read and no `IF` condition: one for the base row and one per index
//! row, issued concurrently.
//!
//! What a write leaves behind is what an ordinary put of a new item leaves:
//!
//! * the base row's `item_data` and `version`. The version is the clock
//!   reading in microseconds, the value the ordinary path gives a new row, so
//!   a later `IF version = ?` fence on the restored item compares equal to
//!   what that path reads back. The base row carries no explicit write
//!   timestamp: ordinary writes commit it through Paxos at the coordinator's
//!   clock, and a client-stamped cell ahead of that clock would shadow them;
//! * one row per GSI and LSI the item has keys for, projected as the index
//!   declares, with no explicit write timestamp either. On this tree every
//!   later index mutation (put, update, delete, transaction commit, the GSI
//!   queue worker) is stamped by the coordinator, so a coordinator-stamped
//!   restored row loses to any of them whatever the host clocks say. Once
//!   index mutations carry a version-derived timestamp, the restored rows
//!   take `version` as theirs so the same ordering holds for them.
//!
//! There is no stream record (a restored table starts with streams disabled),
//! no TTL work (TTL settings are not restored) and no `gsi_pending` entry
//! (every index row is written here, so the indexes are complete when the
//! table turns `ACTIVE`).

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::statement::StatementParamsBuilder;
use cdrs_tokio::types::value::Value;
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_core::validation::IndexKeyRef;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{composite_pk_to_text, parse_sk, sk_column, sk_info};

use super::ddl::{all_sort_key_info, data_table_name, index_table_name};
use super::index::{
    IndexMeta, index_row_insert, item_has_index_keys, project_item_for_index, sk_to_value,
};
use crate::CassandraEngine;

/// Everything a restore copy needs to write items into one target, resolved
/// once per copy rather than once per item.
pub(crate) struct RestoreWriter {
    key_info: TableKeyInfo,
    data_keyspace: String,
    indexes: Vec<IndexMeta>,
    /// The base-row `INSERT`, with a `?` for `pk`, one per sort key column,
    /// `item_data` and `version`.
    base_insert: String,
}

impl CassandraEngine {
    /// The items of one payload bucket of `backup_arn`, each with the byte
    /// length of its stored JSON.
    pub(crate) async fn read_backup_bucket(
        &self,
        account_keyspace: &str,
        backup_arn: &str,
        bucket: i64,
    ) -> Result<Vec<(Item, usize)>, StorageError> {
        use cdrs_tokio::types::IntoRustByName;

        let query = format!(
            "SELECT item_data FROM {account_keyspace}.backup_items WHERE backup_arn = ? AND bucket = ?"
        );
        #[allow(clippy::cast_possible_truncation)]
        let bucket = bucket as i32;
        let rows = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(backup_arn, bucket))
            .await
            .map_err(|e| StorageError::Internal(format!("Read backup payload: {e}")))?
            .response_body()
            .map_err(|e| StorageError::Internal(format!("Parse backup payload: {e}")))?
            .into_rows()
            .unwrap_or_default();
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let item_data: String = row
                .get_r_by_name("item_data")
                .map_err(|e| StorageError::Internal(format!("Parse backup item: {e}")))?;
            let item: Item = serde_json::from_str(&item_data)
                .map_err(|e| StorageError::Internal(format!("Deserialize backup item: {e}")))?;
            items.push((item, item_data.len()));
        }
        Ok(items)
    }

    /// Resolve the restore write path for `key_info`'s target.
    ///
    /// The target's indexes are read from the catalog at `LOCAL_QUORUM`: the
    /// restore wrote them moments ago, and an index a lagging replica omits
    /// would be left empty for every restored item.
    pub(crate) async fn restore_writer(
        &self,
        key_info: &TableKeyInfo,
    ) -> Result<RestoreWriter, StorageError> {
        let indexes = super::index::fetch_indexes_for_table_quorum(
            &key_info.table_id,
            &self.session,
            &self.catalog_keyspace(),
        )
        .await?;
        let data_keyspace = self.account_keyspace(&key_info.account_id);
        let base_insert = base_insert_cql(&data_keyspace, key_info);
        Ok(RestoreWriter {
            key_info: key_info.clone(),
            data_keyspace,
            indexes,
            base_insert,
        })
    }

    /// Write one backup item into the restore target.
    ///
    /// # Errors
    ///
    /// [`StorageError::Validation`] if the item's index key attributes do not
    /// fit the target's index key schemas, and the storage error of the first
    /// write that fails.
    pub(crate) async fn write_restored_item(
        &self,
        writer: &RestoreWriter,
        item: &Item,
    ) -> Result<(), StorageError> {
        let key_info = &writer.key_info;
        if !writer.indexes.is_empty() {
            let index_refs: Vec<IndexKeyRef<'_>> = writer
                .indexes
                .iter()
                .map(|idx| IndexKeyRef {
                    index_name: &idx.index_name,
                    key_schema: &idx.key_schema,
                })
                .collect();
            extenddb_core::validation::validate_index_keys(
                item,
                &index_refs,
                &key_info.attribute_definitions,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        }

        let version = chrono::Utc::now().timestamp_micros();
        let pk_text = composite_pk_to_text(item, &key_info.key_schema)?;
        let item_text =
            serde_json::to_value(item).map_err(|e| StorageError::Internal(e.to_string()))?;
        let item_text = item_text.to_string();

        let mut base_values: Vec<Value> = Vec::with_capacity(4);
        base_values.push(Value::from(pk_text.as_str()));
        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = item
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            base_values.push(sk_to_value(&parse_sk(sk_value, sk_type)?));
        }
        base_values.push(item_text.as_str().into());
        base_values.push(version.into());

        let index_rows = index_row_statements(writer, item)?;
        let mut writes = Vec::with_capacity(1 + index_rows.len());
        writes.push(self.restore_write(
            &writer.base_insert,
            QueryValues::SimpleValues(base_values),
            None,
        ));
        for (cql, values) in &index_rows {
            // No client timestamp: every later index mutation on this tree
            // (put, update, delete, transaction commit, the GSI queue worker)
            // is stamped by the coordinator, so the restored row must be too,
            // or a later delete whose coordinator clock trails the host that
            // ran the restore would leave it in place.
            writes.push(self.restore_write(cql, values.clone(), None));
        }
        futures::future::try_join_all(writes).await.map(|_| ())
    }

    /// One plain write at `LOCAL_QUORUM`, at `timestamp` when given.
    async fn restore_write(
        &self,
        cql: &str,
        values: QueryValues,
        timestamp: Option<i64>,
    ) -> Result<(), StorageError> {
        let mut params = StatementParamsBuilder::new()
            .with_consistency(Consistency::LocalQuorum)
            .with_values(values);
        if let Some(timestamp) = timestamp {
            params = params.with_timestamp(timestamp);
        }
        self.session
            .query_with_params(cql, params.build())
            .await
            .map_err(|e| StorageError::Internal(format!("Restore write: {e}")))?;
        Ok(())
    }
}

/// The base-row `INSERT` of a restored item: `pk`, the sort key column when
/// the table has one, `item_data` and `version`, each bound by `?`.
fn base_insert_cql(data_keyspace: &str, key_info: &TableKeyInfo) -> String {
    let data_table = data_table_name(&key_info.table_id);
    let (sort_column, sort_marker) =
        match sk_info(&key_info.key_schema, &key_info.attribute_definitions) {
            Some((_, sk_type)) => (format!(", {}", sk_column(sk_type)), ", ?"),
            None => (String::new(), ""),
        };
    format!(
        "INSERT INTO {data_keyspace}.{data_table} (pk{sort_column}, item_data, version) \
         VALUES (?{sort_marker}, ?, ?)"
    )
}

/// The index-row `INSERT`s for `item`: one per GSI and LSI whose key
/// attributes the item carries, built by the same function the ordinary put
/// path uses, so the row shape is the put path's.
fn index_row_statements(
    writer: &RestoreWriter,
    item: &Item,
) -> Result<Vec<(String, QueryValues)>, StorageError> {
    let key_info = &writer.key_info;
    let base_sks = all_sort_key_info(&key_info.key_schema, &key_info.attribute_definitions);
    let mut statements = Vec::with_capacity(writer.indexes.len());
    for idx in &writer.indexes {
        if !item_has_index_keys(item, &idx.key_schema) {
            continue;
        }
        let idx_table = index_table_name(&idx.index_id);
        let idx_sks = all_sort_key_info(&idx.key_schema, &key_info.attribute_definitions);
        let projected =
            project_item_for_index(item, &idx.key_schema, &key_info.key_schema, &idx.projection);
        statements.push(index_row_insert(
            &writer.data_keyspace,
            &idx_table,
            item,
            &projected,
            &idx.key_schema,
            &key_info.key_schema,
            &idx_sks,
            &base_sks,
        )?);
    }
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, Projection, ProjectionType,
        ScalarAttributeType,
    };

    fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        }
    }

    fn attr(name: &str, attribute_type: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type,
        }
    }

    fn key_info(key_schema: Vec<KeySchemaElement>) -> TableKeyInfo {
        TableKeyInfo {
            table_name: "T".to_owned(),
            account_id: "123456789012".to_owned(),
            table_id: "0a1b-2c3d".to_owned(),
            key_schema: key_schema.clone(),
            base_key_schema: key_schema,
            attribute_definitions: vec![
                attr("id", ScalarAttributeType::S),
                attr("sort", ScalarAttributeType::N),
                attr("gpk", ScalarAttributeType::S),
                attr("lsk", ScalarAttributeType::N),
            ],
            has_lsi: false,
            global_secondary_indexes: Vec::new(),
            local_secondary_indexes: Vec::new(),
            stream_specification: None,
            vector_indexes: Vec::new(),
        }
    }

    fn writer(key_schema: Vec<KeySchemaElement>, indexes: Vec<IndexMeta>) -> RestoreWriter {
        let key_info = key_info(key_schema);
        RestoreWriter {
            base_insert: base_insert_cql("ks", &key_info),
            key_info,
            data_keyspace: "ks".to_owned(),
            indexes,
        }
    }

    fn index(name: &str, index_type: &str, key_schema: Vec<KeySchemaElement>) -> IndexMeta {
        IndexMeta {
            index_name: name.to_owned(),
            index_id: format!("{name}-id"),
            index_type: index_type.to_owned(),
            key_schema,
            projection: Projection {
                projection_type: ProjectionType::Include,
                non_key_attributes: Some(vec!["value".to_owned()]),
            },
            propagation_delay_ms: None,
        }
    }

    fn item(pairs: &[(&str, AttributeValue)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    #[test]
    fn base_insert_names_the_sort_key_column_only_when_the_table_has_one() {
        let plain = writer(vec![ks("id", KeyType::Hash)], Vec::new());
        assert_eq!(
            plain.base_insert,
            "INSERT INTO ks.items_0a1b_2c3d (pk, item_data, version) VALUES (?, ?, ?)"
        );
        let sorted = writer(
            vec![ks("id", KeyType::Hash), ks("sort", KeyType::Range)],
            Vec::new(),
        );
        assert_eq!(
            sorted.base_insert,
            "INSERT INTO ks.items_0a1b_2c3d (pk, sk_n, item_data, version) VALUES (?, ?, ?, ?)"
        );
    }

    #[test]
    fn one_index_row_per_index_the_item_has_keys_for() {
        let writer = writer(
            vec![ks("id", KeyType::Hash), ks("sort", KeyType::Range)],
            vec![
                index("ByGroup", "GSI", vec![ks("gpk", KeyType::Hash)]),
                index(
                    "ByLocal",
                    "LSI",
                    vec![ks("id", KeyType::Hash), ks("lsk", KeyType::Range)],
                ),
            ],
        );
        let full = item(&[
            ("id", AttributeValue::S("a".to_owned())),
            ("sort", AttributeValue::N("1".to_owned())),
            ("gpk", AttributeValue::S("g".to_owned())),
            ("lsk", AttributeValue::N("7".to_owned())),
            ("value", AttributeValue::S("v".to_owned())),
            ("extra", AttributeValue::S("not projected".to_owned())),
        ]);
        let statements = index_row_statements(&writer, &full).expect("statements");
        assert_eq!(statements.len(), 2);
        assert!(
            statements[0]
                .0
                .starts_with("INSERT INTO ks.index_ByGroup_id (")
        );
        assert!(
            statements[1]
                .0
                .starts_with("INSERT INTO ks.index_ByLocal_id (")
        );
        for (cql, values) in &statements {
            assert!(
                !cql.contains(" IF "),
                "no condition on a restore write: {cql}"
            );
            let QueryValues::SimpleValues(values) = values else {
                panic!("simple values expected");
            };
            let bound = cql.matches('?').count();
            assert_eq!(bound, values.len(), "{cql}");
        }

        // No GSI key: only the LSI row.
        let sparse = item(&[
            ("id", AttributeValue::S("a".to_owned())),
            ("sort", AttributeValue::N("2".to_owned())),
            ("lsk", AttributeValue::N("8".to_owned())),
        ]);
        let statements = index_row_statements(&writer, &sparse).expect("statements");
        assert_eq!(statements.len(), 1);
        assert!(
            statements[0]
                .0
                .starts_with("INSERT INTO ks.index_ByLocal_id (")
        );
    }

    #[test]
    fn the_projection_is_applied_to_the_index_row() {
        let writer = writer(
            vec![ks("id", KeyType::Hash)],
            vec![index("ByGroup", "GSI", vec![ks("gpk", KeyType::Hash)])],
        );
        let full = item(&[
            ("id", AttributeValue::S("a".to_owned())),
            ("gpk", AttributeValue::S("g".to_owned())),
            ("value", AttributeValue::S("v".to_owned())),
            ("extra", AttributeValue::S("not projected".to_owned())),
        ]);
        let projected = project_item_for_index(
            &full,
            &writer.indexes[0].key_schema,
            &writer.key_info.key_schema,
            &writer.indexes[0].projection,
        );
        assert!(projected.contains_key("value"));
        assert!(!projected.contains_key("extra"));
        let statements = index_row_statements(&writer, &full).expect("statements");
        let QueryValues::SimpleValues(values) = &statements[0].1 else {
            panic!("simple values expected");
        };
        // item_data is the last bound value and carries the projection.
        let Some(Value::Some(bytes)) = values.last() else {
            panic!("item_data must be bound");
        };
        let item_data = String::from_utf8(bytes.clone()).expect("utf-8 item_data");
        assert!(item_data.contains("\"value\""), "{item_data}");
        assert!(!item_data.contains("not projected"), "{item_data}");
    }
}
