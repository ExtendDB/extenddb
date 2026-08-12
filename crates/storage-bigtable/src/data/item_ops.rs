//! PutItem / GetItem / DeleteItem / UpdateItem against a BigTable data table.

use std::collections::BTreeMap;

use extenddb_core::expression::{Expr, ExpressionMaps, UpdateAction, apply_update, evaluate_condition};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::{DeleteFromRow, SetCell};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Filter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    CheckAndMutateRowRequest, MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowSet, mutation, TimestampRange,
};

use crate::data::client::BigtableClient;
use crate::data::encoding::{cell, row_key};

/// Family for DDB attribute cells.
pub const FAMILY_DATA: &str = "d";

/// Holds the table-specific context for a sequence of data operations.
pub struct ItemOps<'a> {
    client: &'a BigtableClient,
    full_table_name: String,
    intent_timeout_secs: u64,
}

impl<'a> ItemOps<'a> {
    pub fn new(client: &'a BigtableClient, data_table_short: &str, intent_timeout_secs: u64) -> Self {
        Self {
            client,
            full_table_name: client.full_table_name(data_table_short),
            intent_timeout_secs,
        }
    }

    /// Read the entire row into an Item map. Returns Ok(None) if absent.
    pub async fn get(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let row_key = row_key::encode_key(key, &key_info.key_schema)?;
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![row_key],
                row_ranges: vec![],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            }),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| StorageError::Internal(format!("ReadRows: {e}")))?;
        let mut iter = resp.into_iter();
        let Some((_, cells)) = iter.next() else {
            return Ok(None);
        };
        let mut item: Item = BTreeMap::new();
        for c in cells {
            if c.family_name == FAMILY_DATA {
                let attr_name = String::from_utf8(c.qualifier).map_err(|e| {
                    StorageError::Internal(format!("decode qualifier: {e}"))
                })?;
                let value = cell::decode(&c.value)?;
                item.insert(attr_name, value);
            }
        }
        if item.is_empty() {
            return Ok(None);
        }
        Ok(Some(item))
    }

    /// Read multiple rows into Item maps. Returns items in the same order as keys.
    /// Absent rows result in None.
    pub async fn batch_get(
        &self,
        key_info: &TableKeyInfo,
        keys: &[Item],
    ) -> Result<Vec<Option<Item>>, StorageError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut row_keys = Vec::with_capacity(keys.len());
        for k in keys {
            row_keys.push(row_key::encode_key(k, &key_info.key_schema)?);
        }
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            rows_limit: keys.len() as i64,
            rows: Some(RowSet {
                row_keys,
                row_ranges: vec![],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            }),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| StorageError::Internal(format!("ReadRows batch: {e}")))?;

        let mut row_map = BTreeMap::new();
        for (rkey, cells) in resp {
            let mut item: Item = BTreeMap::new();
            for c in cells {
                if c.family_name == FAMILY_DATA {
                    let attr_name = String::from_utf8(c.qualifier).map_err(|e| {
                        StorageError::Internal(format!("decode qualifier: {e}"))
                    })?;
                    let value = cell::decode(&c.value)?;
                    item.insert(attr_name, value);
                }
            }
            if !item.is_empty() {
                row_map.insert(rkey, item);
            }
        }

        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            let rkey = row_key::encode_key(k, &key_info.key_schema)?;
            if let Some(item) = row_map.get(&rkey) {
                out.push(Some(item.clone()));
            } else {
                out.push(None);
            }
        }

        Ok(out)
    }

    /// Guarded Put: write the item only if there is no active 2PC lock.
    /// Uses DeleteFromFamily(d) to preserve family m.
    pub async fn put(
        &self,
        key_info: &TableKeyInfo,
        item: &Item,
    ) -> Result<(), StorageError> {
        let row_key = row_key::encode_key(item, &key_info.key_schema)?;
        let mut mutations = self.item_to_mutations(item, false)?;
        mutations.insert(0, Mutation {
            mutation: Some(mutation::Mutation::DeleteFromFamily(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromFamily {
                    family_name: FAMILY_DATA.to_string(),
                }
            )),
        });

        let mut data = self.client.data();

        let intent_timeout_micros = (self.intent_timeout_secs * 1_000_000) as i64;
        let now_micros = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1000) as i64;
        let min_timestamp = now_micros - intent_timeout_micros;

        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter("m".to_string())),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(b"intent:.*".to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::TimestampRangeFilter(TimestampRange {
                                start_timestamp_micros: min_timestamp,
                                end_timestamp_micros: 0,
                            })),
                        },
                    ],
                }
            )),
        };

        let req = CheckAndMutateRowRequest {
            table_name: self.full_table_name.clone(),
            row_key,
            predicate_filter: Some(predicate),
            true_mutations: vec![],
            false_mutations: mutations,
            ..CheckAndMutateRowRequest::default()
        };

        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| StorageError::Internal(format!("CheckAndMutateRow put: {e}")))?;

        if resp.predicate_matched {
            return Err(StorageError::TransactionConflict(
                "concurrent transaction holds an intent on this row".to_string()
            ));
        }

        Ok(())
    }

    /// Guarded Delete: delete data only if there is no active 2PC lock.
    /// Uses DeleteFromFamily(d) to preserve family m.
    pub async fn delete(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<(), StorageError> {
        let row_key = row_key::encode_key(key, &key_info.key_schema)?;
        let mutations = vec![Mutation {
            mutation: Some(mutation::Mutation::DeleteFromFamily(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromFamily {
                    family_name: FAMILY_DATA.to_string(),
                }
            )),
        }];

        let mut data = self.client.data();

        let intent_timeout_micros = (self.intent_timeout_secs * 1_000_000) as i64;
        let now_micros = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1000) as i64;
        let min_timestamp = now_micros - intent_timeout_micros;

        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter("m".to_string())),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(b"intent:.*".to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::TimestampRangeFilter(TimestampRange {
                                start_timestamp_micros: min_timestamp,
                                end_timestamp_micros: 0,
                            })),
                        },
                    ],
                }
            )),
        };

        let req = CheckAndMutateRowRequest {
            table_name: self.full_table_name.clone(),
            row_key,
            predicate_filter: Some(predicate),
            true_mutations: vec![],
            false_mutations: mutations,
            ..CheckAndMutateRowRequest::default()
        };

        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| StorageError::Internal(format!("CheckAndMutateRow delete: {e}")))?;

        if resp.predicate_matched {
            return Err(StorageError::TransactionConflict(
                "concurrent transaction holds an intent on this row".to_string()
            ));
        }

        Ok(())
    }

    /// Replace any existing row with the supplied item. No condition check, no lock check.
    pub async fn put_unconditional(
        &self,
        key_info: &TableKeyInfo,
        item: &Item,
    ) -> Result<(), StorageError> {
        let row_key = row_key::encode_key(item, &key_info.key_schema)?;
        let mutations = self.item_to_mutations(item, true)?;
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.full_table_name.clone(),
            row_key,
            mutations,
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| StorageError::Internal(format!("MutateRow put: {e}")))?;
        Ok(())
    }

    /// Delete a row by key. No condition check, no lock check.
    pub async fn delete_unconditional(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<(), StorageError> {
        let row_key = row_key::encode_key(key, &key_info.key_schema)?;
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.full_table_name.clone(),
            row_key,
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            }],
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| StorageError::Internal(format!("MutateRow delete: {e}")))?;
        Ok(())
    }

    /// Apply a list of mutations (SetCell / DeleteFromColumn) to a row by key.
    pub async fn mutate_cells(
        &self,
        row_key_bytes: Vec<u8>,
        mutations: Vec<Mutation>,
    ) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.full_table_name.clone(),
            row_key: row_key_bytes,
            mutations,
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| StorageError::Internal(format!("MutateRow: {e}")))?;
        Ok(())
    }

    /// Turn an Item map into BigTable Mutations. When `delete_row_first` is
    /// true, prepend a DeleteFromRow so the new item fully replaces the old.
    pub fn item_to_mutations(
        &self,
        item: &Item,
        delete_row_first: bool,
    ) -> Result<Vec<Mutation>, StorageError> {
        let mut mutations = Vec::with_capacity(item.len() + 1);
        if delete_row_first {
            mutations.push(Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            });
        }
        for (name, value) in item {
            mutations.push(Mutation {
                mutation: Some(mutation::Mutation::SetCell(SetCell {
                    family_name: FAMILY_DATA.to_string(),
                    column_qualifier: name.as_bytes().to_vec(),
                    timestamp_micros: -1,
                    value: cell::encode(value)?,
                })),
            });
        }
        Ok(mutations)
    }
}

/// Apply `UpdateAction`s to an existing item (or empty if absent). Returns the
/// new image. Used by UpdateItem.
pub fn apply_update_actions(
    existing: &Item,
    actions: &[UpdateAction],
    maps: &ExpressionMaps,
) -> Result<Item, StorageError> {
    let mut new_item = existing.clone();
    apply_update(actions, &mut new_item, maps).map_err(|e| {
        StorageError::Validation(format!("update expression evaluation: {e}"))
    })?;
    Ok(new_item)
}

/// Evaluate a ConditionExpression against the existing item. Returns Ok(())
/// if the condition holds (or is None); ConditionFailed if it doesn't.
pub fn check_condition(
    existing: Option<&Item>,
    condition: Option<&Expr>,
    maps: &ExpressionMaps,
) -> Result<(), StorageError> {
    let Some(expr) = condition else {
        return Ok(());
    };
    let empty: Item = BTreeMap::new();
    let item = existing.unwrap_or(&empty);
    match evaluate_condition(expr, item, maps) {
        Ok(true) => Ok(()),
        Ok(false) => Err(StorageError::ConditionFailed(existing.cloned())),
        Err(e) => Err(StorageError::Validation(format!("condition evaluation: {e}"))),
    }
}
