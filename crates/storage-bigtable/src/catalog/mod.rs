// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog operations over the Cloud Bigtable `__extenddb_catalog__` (and
//! `__extenddb_data_ledger__`) system tables.
//!
//! Layout: every catalog record is a single Bigtable row. Column family `c`,
//! single qualifier `j`, value is a `serde_json::Value` byte payload. Row keys
//! follow the `<kind>:<id>...` convention defined in [`keys`].

pub mod keys;

use extenddb_storage::error::StorageError;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutate_rows_request::Entry;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::{
    DeleteFromColumn, DeleteFromRow, SetCell,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Filter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    CheckAndMutateRowRequest, MutateRowRequest, MutateRowsRequest, Mutation, ReadRowsRequest,
    RowFilter, RowRange, RowSet, TimestampRange,
};

use crate::data::client::{
    BigtableClient, exact_re2_bytes, is_table_not_found_err, map_bigtable_err,
    monotonic_timestamp_after_micros, monotonic_timestamp_micros,
};

/// Short Bigtable table id for the catalog magic table.
pub const CATALOG_TABLE: &str = "__extenddb_catalog__";

/// Short Bigtable table id for the data-instance migration ledger table.
pub const DATA_LEDGER_TABLE: &str = "__extenddb_data_ledger__";

/// Column family used for catalog rows.
pub const CF: &str = "c";
/// Column qualifier used for catalog JSON payloads.
pub const QUALIFIER: &[u8] = b"j";

/// Desired mutation outcome inside [`Catalog::mutate_json_cas`].
#[derive(Debug, Clone)]
pub enum CatalogJsonAction {
    /// Leave the row untouched and return the computed output immediately.
    NoOp,
    /// Atomically insert (if previously absent) or compare-and-swap to `serde_json::Value`.
    Put(serde_json::Value),
    /// Atomically delete the row if its raw payload still matches what was read.
    Delete,
}

pub struct Catalog<'c> {
    client: &'c BigtableClient,
    table_short_name: &'c str,
}

impl<'c> Catalog<'c> {
    #[must_use]
    pub fn new(client: &'c BigtableClient) -> Self {
        Self {
            client,
            table_short_name: CATALOG_TABLE,
        }
    }

    #[must_use]
    pub fn with_table(client: &'c BigtableClient, table_short_name: &'c str) -> Self {
        Self {
            client,
            table_short_name,
        }
    }

    fn full_table(&self) -> String {
        self.client.full_table_name(self.table_short_name)
    }

    fn map_rpc_err(&self, context: &str, err: crate::data::client::BigtableError) -> StorageError {
        if is_table_not_found_err(&err) {
            StorageError::TableNotFound(self.table_short_name.to_owned())
        } else {
            map_bigtable_err(context, err)
        }
    }

    /// Write (or overwrite) a catalog record via [`Self::mutate_json_cas`] so
    /// overwrites always stamp a cell timestamp strictly greater than any
    /// existing cell on the row (`cur_ts`), preventing cross-process or
    /// post-restart clock skew from shadowing a newer catalog write behind an
    /// older cell timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if JSON serialization or the CAS RPC loop fails.
    pub async fn put(&self, row_key: &str, value: &serde_json::Value) -> Result<(), StorageError> {
        self.mutate_json_cas(row_key, std::convert::identity, |_| {
            Ok((CatalogJsonAction::Put(value.clone()), ()))
        })
        .await
    }

    /// Insert a catalog record only if the row does not currently exist.
    /// Returns `Ok(true)` if inserted, `Ok(false)` if the row already existed.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if JSON serialization or the `CheckAndMutateRow` RPC fails.
    pub async fn put_if_absent(
        &self,
        row_key: &str,
        value: &serde_json::Value,
    ) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let payload = serde_json::to_vec(value)
            .map_err(|e| StorageError::Internal(format!("catalog put_if_absent json: {e}")))?;
        let ts_micros = monotonic_timestamp_micros();
        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!("^{CF}$"))),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(b"^j$".to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                    ],
                },
            )),
        };
        let req = CheckAndMutateRowRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key.as_bytes().to_vec(),
            predicate_filter: Some(predicate),
            true_mutations: vec![],
            false_mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::SetCell(SetCell {
                    family_name: CF.to_owned(),
                    column_qualifier: QUALIFIER.to_vec(),
                    timestamp_micros: ts_micros,
                    value: payload,
                })),
            }],
            ..CheckAndMutateRowRequest::default()
        };
        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| self.map_rpc_err(&format!("catalog put_if_absent {row_key}"), e))?;
        Ok(!resp.predicate_matched)
    }

    /// Update a catalog record only if the row currently exists, advancing its
    /// cell timestamp strictly past the observed `cur_ts`.
    /// Returns `Ok(true)` if updated, `Ok(false)` if the row was absent.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if JSON serialization or the CAS RPC loop fails.
    pub async fn put_if_present(
        &self,
        row_key: &str,
        value: &serde_json::Value,
    ) -> Result<bool, StorageError> {
        const MAX_CAS_ATTEMPTS: usize = 64;
        for attempt in 0..MAX_CAS_ATTEMPTS {
            let Some((raw, cur_ts)) = self.get_raw_cell_bytes_with_ts(row_key).await? else {
                return Ok(false);
            };
            if self
                .compare_and_swap_raw_after(row_key, &raw, cur_ts, value)
                .await?
            {
                return Ok(true);
            }
            let base_ms = 1u64 << attempt.min(5);
            let jitter_ms = rand::random_range(0..=base_ms);
            tokio::time::sleep(std::time::Duration::from_millis(base_ms + jitter_ms)).await;
        }
        Err(StorageError::Transient(format!(
            "catalog put_if_present CAS contention limit exceeded on {row_key}"
        )))
    }

    /// Read raw cell bytes and cell timestamp for a single catalog row without
    /// requiring the cell payload to parse as JSON (so [`Self::put`],
    /// [`Self::put_if_present`], and [`Self::mutate_json_cas`] can overwrite
    /// damaged or non-JSON cells cleanly instead of mistaking them for absent rows).
    async fn get_raw_cell_bytes_with_ts(
        &self,
        row_key: &str,
    ) -> Result<Option<(Vec<u8>, i64)>, StorageError> {
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
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
            .map_err(|e| self.map_rpc_err(&format!("catalog get {row_key}"), e))?;
        for (_, cells) in resp {
            for cell in cells {
                if cell.family_name == CF && cell.qualifier == QUALIFIER {
                    return Ok(Some((cell.value, cell.timestamp_micros)));
                }
            }
        }
        Ok(None)
    }

    /// Read raw cell bytes and parsed JSON for a single catalog row.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if reading the row fails.
    pub async fn get_raw(
        &self,
        row_key: &str,
    ) -> Result<Option<(Vec<u8>, serde_json::Value)>, StorageError> {
        let Some((raw, _)) = self.get_raw_cell_bytes_with_ts(row_key).await? else {
            return Ok(None);
        };
        let Some(parsed) = serde_json::from_slice(&raw).ok() else {
            return Ok(None);
        };
        Ok(Some((raw, parsed)))
    }

    /// Atomically replace a catalog row only if its current latest payload matches
    /// `expected_raw`, stamping the replacement strictly after `floor_ts_micros`.
    async fn compare_and_swap_raw_after(
        &self,
        row_key: &str,
        expected_raw: &[u8],
        floor_ts_micros: i64,
        new_value: &serde_json::Value,
    ) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let payload = serde_json::to_vec(new_value)
            .map_err(|e| StorageError::Internal(format!("catalog cas json: {e}")))?;
        let ts_micros = monotonic_timestamp_after_micros(floor_ts_micros);
        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!("^{CF}$"))),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(b"^j$".to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                        RowFilter {
                            filter: Some(Filter::ValueRegexFilter(exact_re2_bytes(expected_raw))),
                        },
                    ],
                },
            )),
        };
        let req = CheckAndMutateRowRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key.as_bytes().to_vec(),
            predicate_filter: Some(predicate),
            true_mutations: vec![
                Mutation {
                    mutation: Some(mutation::Mutation::SetCell(SetCell {
                        family_name: CF.to_owned(),
                        column_qualifier: QUALIFIER.to_vec(),
                        timestamp_micros: ts_micros,
                        value: payload,
                    })),
                },
                Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: CF.to_owned(),
                        column_qualifier: QUALIFIER.to_vec(),
                        time_range: (ts_micros > 0).then_some(TimestampRange {
                            start_timestamp_micros: 0,
                            end_timestamp_micros: ts_micros,
                        }),
                    })),
                },
            ],
            false_mutations: vec![],
            ..CheckAndMutateRowRequest::default()
        };
        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| self.map_rpc_err(&format!("catalog compare_and_swap {row_key}"), e))?;
        Ok(resp.predicate_matched)
    }

    /// Atomically delete a catalog row only if its current latest raw payload matches
    /// `expected_raw`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `CheckAndMutateRow` RPC fails.
    pub async fn compare_and_delete_raw(
        &self,
        row_key: &str,
        expected_raw: &[u8],
    ) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!("^{CF}$"))),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(b"^j$".to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                        RowFilter {
                            filter: Some(Filter::ValueRegexFilter(exact_re2_bytes(expected_raw))),
                        },
                    ],
                },
            )),
        };
        let req = CheckAndMutateRowRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key.as_bytes().to_vec(),
            predicate_filter: Some(predicate),
            true_mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            }],
            false_mutations: vec![],
            ..CheckAndMutateRowRequest::default()
        };
        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| self.map_rpc_err(&format!("catalog compare_and_delete {row_key}"), e))?;
        Ok(resp.predicate_matched)
    }

    /// Execute an atomic compare-and-swap read-modify-write loop over a catalog JSON row.
    ///
    /// # Errors
    ///
    /// Returns `E` if `mutator` fails, an underlying catalog RPC fails, or CAS
    /// contention exceeds `MAX_CAS_ATTEMPTS`.
    pub async fn mutate_json_cas<T, E, M, F>(
        &self,
        row_key: &str,
        map_err: M,
        mut mutator: F,
    ) -> Result<T, E>
    where
        M: Fn(StorageError) -> E,
        F: FnMut(Option<serde_json::Value>) -> Result<(CatalogJsonAction, T), E>,
    {
        const MAX_CAS_ATTEMPTS: usize = 64;
        for attempt in 0..MAX_CAS_ATTEMPTS {
            let current = self
                .get_raw_cell_bytes_with_ts(row_key)
                .await
                .map_err(&map_err)?;
            let parsed_opt = current
                .as_ref()
                .and_then(|(raw, _)| serde_json::from_slice(raw).ok());
            let (action, out) = mutator(parsed_opt)?;
            match (current, action) {
                (_, CatalogJsonAction::NoOp) | (None, CatalogJsonAction::Delete) => {
                    return Ok(out);
                }
                (None, CatalogJsonAction::Put(next)) => {
                    if self.put_if_absent(row_key, &next).await.map_err(&map_err)? {
                        return Ok(out);
                    }
                }
                (Some((raw, cur_ts)), CatalogJsonAction::Put(next)) => {
                    if self
                        .compare_and_swap_raw_after(row_key, &raw, cur_ts, &next)
                        .await
                        .map_err(&map_err)?
                    {
                        return Ok(out);
                    }
                }
                (Some((raw, _)), CatalogJsonAction::Delete) => {
                    if self
                        .compare_and_delete_raw(row_key, &raw)
                        .await
                        .map_err(&map_err)?
                    {
                        return Ok(out);
                    }
                }
            }
            let base_ms = 1u64 << attempt.min(5);
            let jitter_ms = rand::random_range(0..=base_ms);
            tokio::time::sleep(std::time::Duration::from_millis(base_ms + jitter_ms)).await;
        }
        Err(map_err(StorageError::Transient(format!(
            "catalog CAS contention limit exceeded on {row_key}"
        ))))
    }

    /// Read a single catalog record. Returns `None` if the row is absent or
    /// the cell payload cannot be parsed as JSON.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if reading the row fails.
    pub async fn get(&self, row_key: &str) -> Result<Option<serde_json::Value>, StorageError> {
        Ok(self.get_raw(row_key).await?.map(|(_, v)| v))
    }

    /// Delete a single catalog record. Idempotent — no error if absent.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `MutateRow` RPC fails.
    pub async fn delete(&self, row_key: &str) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key.as_bytes().to_vec(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            }],
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err(&format!("catalog delete {row_key}"), e))?;
        Ok(())
    }

    /// Delete a single catalog record and return whether a cell existed before deletion.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `CheckAndMutateRow` RPC fails.
    pub async fn delete_if_present(&self, row_key: &str) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!("^{CF}$"))),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(b"^j$".to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                    ],
                },
            )),
        };
        let req = CheckAndMutateRowRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key.as_bytes().to_vec(),
            predicate_filter: Some(predicate),
            true_mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            }],
            false_mutations: vec![],
            ..CheckAndMutateRowRequest::default()
        };
        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err(&format!("catalog delete_if_present {row_key}"), e))?;
        Ok(resp.predicate_matched)
    }

    /// Scan all rows whose key starts with `prefix`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `ReadRows` scan fails.
    pub async fn scan_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        let mut end_key = prefix.as_bytes().to_vec();
        end_key.push(0xFF);
        self.scan_range(
            StartKey::StartKeyClosed(prefix.as_bytes().to_vec()),
            EndKey::EndKeyClosed(end_key),
            0,
        )
        .await
    }

    /// Scan UTF-8 row keys in `[start_key, end_open)` (when `start_inclusive`)
    /// or `(start_key, end_open)` (when `!start_inclusive`), bounded by `rows_limit`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `ReadRows` scan fails.
    pub async fn scan_str_range(
        &self,
        start_key: &str,
        start_inclusive: bool,
        end_open: &str,
        rows_limit: i64,
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        let start = if start_inclusive {
            StartKey::StartKeyClosed(start_key.as_bytes().to_vec())
        } else {
            StartKey::StartKeyOpen(start_key.as_bytes().to_vec())
        };
        self.scan_range(
            start,
            EndKey::EndKeyOpen(end_open.as_bytes().to_vec()),
            rows_limit,
        )
        .await
    }

    /// Scan rows in `[start_key, end_key]` with an optional row limit (`0` = unlimited).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `ReadRows` scan or key UTF-8 decoding fails.
    pub async fn scan_range(
        &self,
        start_key: StartKey,
        end_key: EndKey,
        rows_limit: i64,
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(start_key),
                    end_key: Some(end_key),
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
            .map_err(|e| self.map_rpc_err("catalog scan_range", e))?;

        let mut out = Vec::with_capacity(resp.len());
        for (key, cells) in resp {
            let key_str = String::from_utf8(key)
                .map_err(|e| StorageError::Internal(format!("catalog scan key: {e}")))?;
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

    /// Bulk-delete every row matching the prefix, paged in bounded chunks.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if scanning or deleting rows fails.
    pub async fn delete_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        let mut total = 0u64;
        let mut end_key = prefix.as_bytes().to_vec();
        end_key.push(0xFF);
        loop {
            let page = self
                .scan_range(
                    StartKey::StartKeyClosed(prefix.as_bytes().to_vec()),
                    EndKey::EndKeyClosed(end_key.clone()),
                    1_000,
                )
                .await?;
            if page.is_empty() {
                break;
            }
            let batch_len = page.len();
            total += self
                .delete_keys(page.iter().map(|(k, _)| k.as_str()))
                .await?;
            if batch_len < 1_000 {
                break;
            }
        }
        Ok(total)
    }

    /// Bulk-delete an explicit iterator of catalog row keys.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if any `MutateRows` batch fails.
    pub async fn delete_keys<'a>(
        &self,
        keys_iter: impl IntoIterator<Item = &'a str>,
    ) -> Result<u64, StorageError> {
        let entries: Vec<Entry> = keys_iter
            .into_iter()
            .map(|k| Entry {
                row_key: k.as_bytes().to_vec(),
                mutations: vec![Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
                }],
                ..Entry::default()
            })
            .collect();
        if entries.is_empty() {
            return Ok(0);
        }
        let count = entries.len() as u64;
        let mut data = self.client.data();
        for chunk in entries.chunks(1_000) {
            let req = MutateRowsRequest {
                table_name: self.full_table(),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                entries: chunk.to_vec(),
                ..MutateRowsRequest::default()
            };
            data.mutate_rows(req)
                .await
                .map_err(|e| map_bigtable_err("catalog delete_keys", e))?;
        }
        Ok(count)
    }

    /// Acquire a distributed lease row `lock:<lock_name>` using a
    /// millisecond-aligned expiry cell timestamp (`TimestampGranularity::MILLIS`).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `CheckAndMutateRow` RPC fails.
    pub async fn try_lock(
        &self,
        lock_name: &str,
        owner: &str,
        lease_duration: std::time::Duration,
    ) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let row_key = format!("lock:{lock_name}");

        let now_ms = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX / 1_000);
        let now_micros = now_ms.saturating_mul(1_000);

        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!("^{CF}$"))),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(b"^j$".to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                        RowFilter {
                            filter: Some(Filter::TimestampRangeFilter(TimestampRange {
                                start_timestamp_micros: now_micros,
                                end_timestamp_micros: 0,
                            })),
                        },
                    ],
                },
            )),
        };

        let lease_ms = i64::try_from(lease_duration.as_millis())
            .unwrap_or(i64::MAX / 1_000)
            .max(1);
        let expires_at_micros = now_ms.saturating_add(lease_ms).saturating_mul(1_000);

        let req = CheckAndMutateRowRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
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
                },
                Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: CF.to_owned(),
                        column_qualifier: QUALIFIER.to_vec(),
                        time_range: (expires_at_micros > 0).then_some(TimestampRange {
                            start_timestamp_micros: 0,
                            end_timestamp_micros: expires_at_micros,
                        }),
                    })),
                },
            ],
            ..Default::default()
        };

        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err(&format!("try_lock {lock_name}"), e))?;

        Ok(!resp.predicate_matched)
    }

    /// Renew an existing lease `lock:<lock_name>` **only** if `owner` still holds
    /// the latest cell AND that cell is still unexpired (`timestamp >= now_micros`).
    /// Returns `Ok(true)` if renewed, `Ok(false)` if `owner` no longer holds an
    /// active lease.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `CheckAndMutateRow` RPC fails.
    pub async fn renew_lock(
        &self,
        lock_name: &str,
        owner: &str,
        lease_duration: std::time::Duration,
    ) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let row_key = format!("lock:{lock_name}");

        let now_ms = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX / 1_000);
        let now_micros = now_ms.saturating_mul(1_000);
        let lease_ms = i64::try_from(lease_duration.as_millis())
            .unwrap_or(i64::MAX / 1_000)
            .max(1);
        let expires_at_micros = now_ms.saturating_add(lease_ms).saturating_mul(1_000);

        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!("^{CF}$"))),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(b"^j$".to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                        RowFilter {
                            filter: Some(Filter::TimestampRangeFilter(TimestampRange {
                                start_timestamp_micros: now_micros,
                                end_timestamp_micros: 0,
                            })),
                        },
                        RowFilter {
                            filter: Some(Filter::ValueRegexFilter(exact_re2_bytes(
                                owner.as_bytes(),
                            ))),
                        },
                    ],
                },
            )),
        };

        let req = CheckAndMutateRowRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key.as_bytes().to_vec(),
            predicate_filter: Some(predicate),
            true_mutations: vec![
                Mutation {
                    mutation: Some(mutation::Mutation::SetCell(SetCell {
                        family_name: CF.to_owned(),
                        column_qualifier: QUALIFIER.to_vec(),
                        timestamp_micros: expires_at_micros,
                        value: owner.as_bytes().to_vec(),
                    })),
                },
                Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: CF.to_owned(),
                        column_qualifier: QUALIFIER.to_vec(),
                        time_range: (expires_at_micros > 0).then_some(TimestampRange {
                            start_timestamp_micros: 0,
                            end_timestamp_micros: expires_at_micros,
                        }),
                    })),
                },
            ],
            false_mutations: vec![],
            ..Default::default()
        };

        let resp = data
            .check_and_mutate_row_idempotent(req)
            .await
            .map_err(|e| map_bigtable_err(&format!("renew_lock {lock_name}"), e))?;
        Ok(resp.predicate_matched)
    }

    /// Return the owner string of `lock:<lock_name>` if the lease is currently
    /// unexpired (`cell.timestamp_micros >= now_micros`), or `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if reading the lock row fails.
    pub async fn read_live_lock_owner(
        &self,
        lock_name: &str,
    ) -> Result<Option<String>, StorageError> {
        let mut data = self.client.data();
        let row_key = format!("lock:{lock_name}");
        let now_ms = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX / 1_000);
        let now_micros = now_ms.saturating_mul(1_000);

        let req = ReadRowsRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![row_key.into_bytes()],
                row_ranges: vec![],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            }),
            ..ReadRowsRequest::default()
        };
        let resp = data.read_rows(req).await.map_err(|e| {
            if is_table_not_found_err(&e) {
                StorageError::TableNotFound(self.table_short_name.to_owned())
            } else {
                map_bigtable_err(&format!("read_live_lock_owner {lock_name}"), e)
            }
        })?;
        for (_, cells) in resp {
            for cell in cells {
                if cell.family_name == CF
                    && cell.qualifier == QUALIFIER
                    && cell.timestamp_micros >= now_micros
                {
                    return Ok(Some(String::from_utf8_lossy(&cell.value).into_owned()));
                }
            }
        }
        Ok(None)
    }

    /// Release `lock:<lock_name>` if currently owned by `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `CheckAndMutateRow` RPC fails.
    pub async fn release_lock(&self, lock_name: &str, owner: &str) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let row_key = format!("lock:{lock_name}");

        let predicate = RowFilter {
            filter: Some(Filter::Chain(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!("^{CF}$"))),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(b"^j$".to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                        RowFilter {
                            filter: Some(Filter::ValueRegexFilter(exact_re2_bytes(
                                owner.as_bytes(),
                            ))),
                        },
                    ],
                },
            )),
        };

        let req = CheckAndMutateRowRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key.as_bytes().to_vec(),
            predicate_filter: Some(predicate),
            true_mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            }],
            false_mutations: vec![],
            ..Default::default()
        };

        data.check_and_mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err(&format!("release_lock {lock_name}"), e))?;

        Ok(())
    }

    /// Return up to `rows_limit` row keys present in this catalog table across
    /// any column family or qualifier (including non-`j` or non-JSON cells),
    /// stripping cell values server-side.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `ReadRows` RPC fails.
    pub async fn scan_any_row_keys(&self, rows_limit: i64) -> Result<Vec<String>, StorageError> {
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit,
            filter: Some(RowFilter {
                filter: Some(Filter::Chain(
                    googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain {
                        filters: vec![
                            RowFilter {
                                filter: Some(Filter::CellsPerRowLimitFilter(1)),
                            },
                            RowFilter {
                                filter: Some(Filter::StripValueTransformer(true)),
                            },
                        ],
                    },
                )),
            }),
            ..ReadRowsRequest::default()
        };
        let rows = data
            .read_rows(req)
            .await
            .map_err(|e| self.map_rpc_err("catalog scan_any_row_keys", e))?;
        Ok(rows
            .into_iter()
            .map(|(rk, _)| String::from_utf8_lossy(&rk).into_owned())
            .collect())
    }
}
