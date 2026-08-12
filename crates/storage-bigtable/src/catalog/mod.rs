//! Catalog operations over the BigTable `__extenddb_catalog__` magic table.
//!
//! Layout: every catalog record is a single BigTable row. Column family `c`,
//! single qualifier `j`, value is a serde_json::Value byte payload. Row keys
//! follow the `<kind>:<id>...` convention defined in [`keys`].

pub mod keys;

use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutate_rows_request::Entry;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::{DeleteFromRow, SetCell};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Filter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    MutateRowRequest, MutateRowsRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet,
    CheckAndMutateRowRequest, TimestampRange,
};

use crate::data::client::BigtableClient;

/// Short BigTable table id for the catalog magic table.
pub const CATALOG_TABLE: &str = "__extenddb_catalog__";

/// Column family used for catalog rows.
pub const CF: &str = "c";
/// Column qualifier used for catalog JSON payloads.
pub const QUALIFIER: &[u8] = b"j";

pub struct Catalog<'c> {
    client: &'c BigtableClient,
}

impl<'c> Catalog<'c> {
    pub fn new(client: &'c BigtableClient) -> Self {
        Self { client }
    }

    fn full_table(&self) -> String {
        self.client.full_table_name(CATALOG_TABLE)
    }

    /// Write (or overwrite) a catalog record.
    pub async fn put(&self, row_key: &str, value: &serde_json::Value) -> Result<(), String> {
        let mut data = self.client.data();
        let payload = serde_json::to_vec(value).map_err(|e| format!("catalog put json: {e}"))?;
        let req = MutateRowRequest {
            table_name: self.full_table(),
            row_key: row_key.as_bytes().to_vec(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::SetCell(SetCell {
                    family_name: CF.to_owned(),
                    column_qualifier: QUALIFIER.to_vec(),
                    timestamp_micros: -1,
                    value: payload,
                })),
            }],
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| format!("catalog put {row_key}: {e}"))?;
        Ok(())
    }

    /// Read a single catalog record. Returns None if the row is absent or
    /// the cell payload can't be parsed as JSON.
    pub async fn get(&self, row_key: &str) -> Result<Option<serde_json::Value>, String> {
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![row_key.as_bytes().to_vec()],
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
            .map_err(|e| format!("catalog get {row_key}: {e}"))?;
        for (_, cells) in resp {
            for cell in cells {
                if cell.family_name == CF && cell.qualifier == QUALIFIER {
                    return Ok(serde_json::from_slice(&cell.value).ok());
                }
            }
        }
        Ok(None)
    }

    /// Delete a single catalog record. Idempotent — no error if it's absent.
    pub async fn delete(&self, row_key: &str) -> Result<(), String> {
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.full_table(),
            row_key: row_key.as_bytes().to_vec(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            }],
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| format!("catalog delete {row_key}: {e}"))?;
        Ok(())
    }

    /// Scan all rows whose key starts with `prefix`. Returns (key, payload)
    /// pairs in lex order.
    pub async fn scan_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, serde_json::Value)>, String> {
        let mut data = self.client.data();
        // BigTable RowRange: [prefix, prefix + 0xFF...] gives a clean prefix scan.
        let mut end_key = prefix.as_bytes().to_vec();
        end_key.push(0xFF);
        let req = ReadRowsRequest {
            table_name: self.full_table(),
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(StartKey::StartKeyClosed(prefix.as_bytes().to_vec())),
                    end_key: Some(EndKey::EndKeyClosed(end_key)),
                }],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            }),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| format!("catalog scan {prefix}: {e}"))?;

        let mut out = Vec::with_capacity(resp.len());
        for (key, cells) in resp {
            let key_str = String::from_utf8(key).map_err(|e| format!("catalog scan key: {e}"))?;
            for cell in cells {
                if cell.family_name == CF && cell.qualifier == QUALIFIER {
                    if let Ok(v) = serde_json::from_slice(&cell.value) {
                        out.push((key_str.clone(), v));
                    }
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Bulk-delete every row matching the prefix. Used by destroy.
    pub async fn delete_prefix(&self, prefix: &str) -> Result<u64, String> {
        let records = self.scan_prefix(prefix).await?;
        if records.is_empty() {
            return Ok(0);
        }
        let mut data = self.client.data();
        let entries: Vec<Entry> = records
            .iter()
            .map(|(k, _)| Entry {
                row_key: k.as_bytes().to_vec(),
                mutations: vec![Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
                }],
                ..Entry::default()
            })
            .collect();
        let count = entries.len() as u64;
        let req = MutateRowsRequest {
            table_name: self.full_table(),
            entries,
            ..MutateRowsRequest::default()
        };
        data.mutate_rows(req)
            .await
            .map_err(|e| format!("catalog delete_prefix {prefix}: {e}"))?;
        Ok(count)
    }

    pub async fn try_lock(
        &self,
        lock_name: &str,
        owner: &str,
        lease_duration: std::time::Duration,
    ) -> Result<bool, String> {
        let mut data = self.client.data();
        let row_key = format!("lock:{lock_name}");
        
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        
        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(CF.to_string())),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(QUALIFIER.to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::TimestampRangeFilter(TimestampRange {
                                start_timestamp_micros: now_micros,
                                end_timestamp_micros: 0,
                            })),
                        },
                    ],
                }
            )),
        };

        let lease_micros = lease_duration.as_micros() as i64;
        let expires_at_micros = now_micros + lease_micros;

        let req = CheckAndMutateRowRequest {
            table_name: self.full_table(),
            row_key: row_key.as_bytes().to_vec(),
            predicate_filter: Some(predicate),
            true_mutations: vec![],
            false_mutations: vec![
                Mutation {
                    mutation: Some(mutation::Mutation::SetCell(SetCell {
                        family_name: CF.to_owned(),
                        column_qualifier: QUALIFIER.to_vec(),
                        timestamp_micros: expires_at_micros,
                        value: owner.as_bytes().to_vec(),
                    })),
                }
            ],
            ..Default::default()
        };

        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| format!("try_lock {lock_name}: {e}"))?;

        Ok(!resp.predicate_matched)
    }

    pub async fn release_lock(&self, lock_name: &str, owner: &str) -> Result<(), String> {
        let mut data = self.client.data();
        let row_key = format!("lock:{lock_name}");
        
        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(CF.to_string())),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(QUALIFIER.to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                        RowFilter {
                            filter: Some(Filter::ValueRegexFilter(owner.as_bytes().to_vec())),
                        },
                    ],
                }
            )),
        };

        let req = CheckAndMutateRowRequest {
            table_name: self.full_table(),
            row_key: row_key.as_bytes().to_vec(),
            predicate_filter: Some(predicate),
            true_mutations: vec![
                Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
                }
            ],
            false_mutations: vec![],
            ..Default::default()
        };

        data.check_and_mutate_row(req)
            .await
            .map_err(|e| format!("release_lock {lock_name}: {e}"))?;

        Ok(())
    }
}
