// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog operations over the BigTable `__extenddb_catalog__` magic table.
//!
//! Layout: every catalog record is a single BigTable row. Column family `c`,
//! single qualifier `j`, value is a `serde_json::Value` byte payload. Row keys
//! follow the `<kind>:<id>...` convention defined in [`keys`].

pub mod keys;

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
    BigtableClient, exact_re2_bytes, monotonic_timestamp_after_micros, monotonic_timestamp_micros,
};

/// Short BigTable table id for the catalog magic table.
pub const CATALOG_TABLE: &str = "__extenddb_catalog__";

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
}

impl<'c> Catalog<'c> {
    pub fn new(client: &'c BigtableClient) -> Self {
        Self { client }
    }

    fn full_table(&self) -> String {
        self.client.full_table_name(CATALOG_TABLE)
    }

    /// Write (or overwrite) a catalog record using a millisecond-aligned
    /// cell timestamp (`TimestampGranularity::MILLIS`), placing `SetCell` before
    /// bounded `DeleteFromColumn([0, ts_micros))` so concurrent `ReadRows` never
    /// observe a transiently empty row on `cbtemulator`.
    pub async fn put(&self, row_key: &str, value: &serde_json::Value) -> Result<(), String> {
        let mut data = self.client.data();
        let payload = serde_json::to_vec(value).map_err(|e| format!("catalog put json: {e}"))?;
        let ts_micros = monotonic_timestamp_micros();
        let req = MutateRowRequest {
            table_name: self.full_table(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key.as_bytes().to_vec(),
            mutations: vec![
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
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| format!("catalog put {row_key}: {e}"))?;
        Ok(())
    }

    /// Insert a catalog record only if the row does not currently exist.
    /// Returns `Ok(true)` if inserted, `Ok(false)` if the row already existed.
    pub async fn put_if_absent(
        &self,
        row_key: &str,
        value: &serde_json::Value,
    ) -> Result<bool, String> {
        let mut data = self.client.data();
        let payload =
            serde_json::to_vec(value).map_err(|e| format!("catalog put_if_absent json: {e}"))?;
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
            .map_err(|e| format!("catalog put_if_absent {row_key}: {e}"))?;
        Ok(!resp.predicate_matched)
    }

    /// Update a catalog record only if the row currently exists.
    /// Returns `Ok(true)` if updated, `Ok(false)` if the row was absent.
    pub async fn put_if_present(
        &self,
        row_key: &str,
        value: &serde_json::Value,
    ) -> Result<bool, String> {
        let mut data = self.client.data();
        let payload =
            serde_json::to_vec(value).map_err(|e| format!("catalog put_if_present json: {e}"))?;
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
            .map_err(|e| format!("catalog put_if_present {row_key}: {e}"))?;
        Ok(resp.predicate_matched)
    }

    /// Read raw cell bytes, cell timestamp, and parsed JSON for a single catalog row.
    async fn get_raw_with_ts(
        &self,
        row_key: &str,
    ) -> Result<Option<(Vec<u8>, i64, serde_json::Value)>, String> {
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
            .map_err(|e| format!("catalog get {row_key}: {e}"))?;
        for (_, cells) in resp {
            for cell in cells {
                if cell.family_name == CF && cell.qualifier == QUALIFIER {
                    let Some(parsed) = serde_json::from_slice(&cell.value).ok() else {
                        return Ok(None);
                    };
                    return Ok(Some((cell.value, cell.timestamp_micros, parsed)));
                }
            }
        }
        Ok(None)
    }

    /// Read raw cell bytes and parsed JSON for a single catalog row.
    pub async fn get_raw(
        &self,
        row_key: &str,
    ) -> Result<Option<(Vec<u8>, serde_json::Value)>, String> {
        Ok(self
            .get_raw_with_ts(row_key)
            .await?
            .map(|(raw, _, parsed)| (raw, parsed)))
    }

    /// Atomically replace a catalog row only if its current latest payload matches
    /// `expected_raw`, stamping the replacement strictly after `floor_ts_micros`
    /// and emitting `SetCell` before bounded `DeleteFromColumn([0, ts_micros))`.
    async fn compare_and_swap_raw_after(
        &self,
        row_key: &str,
        expected_raw: &[u8],
        floor_ts_micros: i64,
        new_value: &serde_json::Value,
    ) -> Result<bool, String> {
        let mut data = self.client.data();
        let payload =
            serde_json::to_vec(new_value).map_err(|e| format!("catalog cas json: {e}"))?;
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
            .map_err(|e| format!("catalog compare_and_swap {row_key}: {e}"))?;
        Ok(resp.predicate_matched)
    }

    /// Atomically replace a catalog row only if its current raw payload matches
    /// `expected_raw`.
    pub async fn compare_and_swap_raw(
        &self,
        row_key: &str,
        expected_raw: &[u8],
        new_value: &serde_json::Value,
    ) -> Result<bool, String> {
        self.compare_and_swap_raw_after(row_key, expected_raw, 0, new_value)
            .await
    }

    /// Atomically delete a catalog row only if its current latest raw payload matches
    /// `expected_raw`.
    pub async fn compare_and_delete_raw(
        &self,
        row_key: &str,
        expected_raw: &[u8],
    ) -> Result<bool, String> {
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
            .map_err(|e| format!("catalog compare_and_delete {row_key}: {e}"))?;
        Ok(resp.predicate_matched)
    }

    /// Execute an atomic compare-and-swap read-modify-write loop over a catalog JSON row
    /// (`BUG-13`, `RFC-0003` §7.4).
    ///
    /// Retries on concurrent CAS contention with bounded exponential + random jitter
    /// backoff, advancing replacement timestamps strictly past the read cell's
    /// `timestamp_micros` (`compare_and_swap_raw_after`) for multi-node clock-skew safety.
    pub async fn mutate_json_cas<T, E, M, F>(
        &self,
        row_key: &str,
        map_err: M,
        mut mutator: F,
    ) -> Result<T, E>
    where
        M: Fn(String) -> E,
        F: FnMut(Option<serde_json::Value>) -> Result<(CatalogJsonAction, T), E>,
    {
        const MAX_CAS_ATTEMPTS: usize = 64;
        for attempt in 0..MAX_CAS_ATTEMPTS {
            let current = self.get_raw_with_ts(row_key).await.map_err(&map_err)?;
            let parsed_opt = current.as_ref().map(|(_, _, v)| v.clone());
            let (action, out) = mutator(parsed_opt)?;
            match (current, action) {
                (_, CatalogJsonAction::NoOp) => return Ok(out),
                (None, CatalogJsonAction::Delete) => return Ok(out),
                (None, CatalogJsonAction::Put(next)) => {
                    if self.put_if_absent(row_key, &next).await.map_err(&map_err)? {
                        return Ok(out);
                    }
                }
                (Some((raw, cur_ts, _)), CatalogJsonAction::Put(next)) => {
                    if self
                        .compare_and_swap_raw_after(row_key, &raw, cur_ts, &next)
                        .await
                        .map_err(&map_err)?
                    {
                        return Ok(out);
                    }
                }
                (Some((raw, _, _)), CatalogJsonAction::Delete) => {
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
        Err(map_err(format!(
            "catalog CAS contention limit exceeded on {row_key}"
        )))
    }

    /// Read a single catalog record. Returns `None` if the row is absent or
    /// the cell payload can't be parsed as JSON.
    pub async fn get(&self, row_key: &str) -> Result<Option<serde_json::Value>, String> {
        Ok(self.get_raw(row_key).await?.map(|(_, v)| v))
    }

    /// Delete a single catalog record. Idempotent — no error if it's absent.
    pub async fn delete(&self, row_key: &str) -> Result<(), String> {
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
            .map_err(|e| format!("catalog delete {row_key}: {e}"))?;
        Ok(())
    }

    /// Delete a single catalog record and return whether a cell existed before deletion.
    pub async fn delete_if_present(&self, row_key: &str) -> Result<bool, String> {
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
            .map_err(|e| format!("catalog delete_if_present {row_key}: {e}"))?;
        Ok(resp.predicate_matched)
    }

    /// Scan all rows whose key starts with `prefix`. Returns `(key, payload)`
    /// pairs in lex order.
    pub async fn scan_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, serde_json::Value)>, String> {
        let mut end_key = prefix.as_bytes().to_vec();
        end_key.push(0xFF);
        self.scan_range(
            StartKey::StartKeyClosed(prefix.as_bytes().to_vec()),
            EndKey::EndKeyClosed(end_key),
            0,
        )
        .await
    }

    /// Scan rows in `[start_key, end_key]` with an optional row limit (`0` = unlimited).
    pub async fn scan_range(
        &self,
        start_key: StartKey,
        end_key: EndKey,
        rows_limit: i64,
    ) -> Result<Vec<(String, serde_json::Value)>, String> {
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
            .map_err(|e| format!("catalog scan_range: {e}"))?;

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

    /// Bulk-delete every row matching the prefix.
    pub async fn delete_prefix(&self, prefix: &str) -> Result<u64, String> {
        let records = self.scan_prefix(prefix).await?;
        if records.is_empty() {
            return Ok(0);
        }
        self.delete_keys(records.iter().map(|(k, _)| k.as_str()))
            .await
    }

    /// Bulk-delete an explicit iterator of catalog row keys.
    pub async fn delete_keys<'a>(
        &self,
        keys_iter: impl IntoIterator<Item = &'a str>,
    ) -> Result<u64, String> {
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
                .map_err(|e| format!("catalog delete_keys: {e}"))?;
        }
        Ok(count)
    }

    /// Acquire a distributed lease row `lock:<lock_name>` using a
    /// millisecond-aligned expiry cell timestamp (`TimestampGranularity::MILLIS`).
    pub async fn try_lock(
        &self,
        lock_name: &str,
        owner: &str,
        lease_duration: std::time::Duration,
    ) -> Result<bool, String> {
        let mut data = self.client.data();
        let row_key = format!("lock:{lock_name}");

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
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

        let lease_ms = (lease_duration.as_millis() as i64).max(1);
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
            .map_err(|e| format!("release_lock {lock_name}: {e}"))?;

        Ok(())
    }
}
