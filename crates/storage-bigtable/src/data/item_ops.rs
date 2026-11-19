// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PutItem / GetItem / DeleteItem / UpdateItem against a BigTable data table.

use std::collections::BTreeMap;

use bigdecimal::ToPrimitive;
use extenddb_core::expression::{
    Expr, ExpressionMaps, UpdateAction, apply_update_validated, evaluate_condition,
};
use extenddb_core::types::{AttributeDefinition, Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::{
    DeleteFromColumn, DeleteFromFamily, SetCell,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::{
    Chain, Condition, Filter, Interleave,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    CheckAndMutateRowRequest, MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowSet,
    TimestampRange, mutation,
};

use crate::data::client::{BigtableClient, map_bigtable_err, monotonic_timestamp_micros};
use crate::data::encoding::{cell, row_key};

/// Family for DDB attribute cells.
pub const FAMILY_DATA: &str = "d";
/// Family for metadata cells (OCC row version, 2PC intents).
pub const FAMILY_META: &str = "m";
/// Qualifier for OCC row version cell in family `m`.
pub const QUALIFIER_VERSION: &[u8] = b"v";
/// Qualifier prefix for 2PC intent cells in family `m`.
pub const QUALIFIER_INTENT_PREFIX: &[u8] = b"intent:";

/// Current wall-clock time in whole milliseconds since Unix epoch (at least 1).
#[inline]
pub fn now_millis() -> u64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).max(1) as u64
}

/// Current wall-clock time in microseconds aligned to Bigtable's millisecond
/// cell-timestamp granularity (`TimestampGranularity::MILLIS`).
#[inline]
pub fn now_millis_micros() -> i64 {
    (now_millis().saturating_mul(1_000)) as i64
}

/// Compute the next monotonically increasing OCC row version.
///
/// Anchoring at `now_millis()` and advancing `prev.max(now_millis()) + 1`
/// guarantees:
/// 1. Every cell in a write shares a deterministic millisecond-aligned cell
///    timestamp (`version_to_timestamp_micros(v) = v * 1_000`).
/// 2. Sub-millisecond sequential writes to the same row strictly advance both
///    `m:v` and cell timestamps without colliding on identical timestamps.
/// 3. Under `MaxNumVersions(1)` compaction, a higher OCC version strictly
///    dominates all older cell versions across every qualifier, preventing
///    torn rows.
#[inline]
pub fn next_occ_version(prev: Option<u64>) -> u64 {
    let now_ms = now_millis();
    match prev {
        Some(v) => v.max(now_ms).saturating_add(1),
        None => now_ms,
    }
}

/// Convert an OCC version into a millisecond-aligned Bigtable cell timestamp
/// in microseconds (`version * 1_000`).
#[inline]
pub const fn version_to_timestamp_micros(version: u64) -> i64 {
    (version.saturating_mul(1_000)) as i64
}

/// Parse a DynamoDB `N` string into a positive Unix epoch second (`>= 1`) for
/// TTL indexing and expiration checks (supporting integer, fractional, and
/// scientific-notation decimal strings). Zero and negative values never expire
/// in DynamoDB (matching `SqliteEngine` / `PostgresEngine` `BETWEEN 1 AND now`).
#[must_use]
pub fn parse_ttl_epoch_secs(s: &str) -> Option<i64> {
    let trimmed = s.trim();
    if let Ok(v) = trimmed.parse::<i64>() {
        return (v > 0).then_some(v);
    }
    let bd = trimmed.parse::<bigdecimal::BigDecimal>().ok()?;
    let truncated = bd.with_scale(0);
    let secs = truncated.to_i64()?;
    (secs > 0).then_some(secs)
}

/// Bounded `[0, ts_micros)` range used when pre-clearing older `m:v` or
/// `t:state` cells so a delayed mutation never deletes a strictly newer version.
#[inline]
pub(crate) fn older_than_ts_range(ts_micros: i64) -> Option<TimestampRange> {
    (ts_micros > 0).then_some(TimestampRange {
        start_timestamp_micros: 0,
        end_timestamp_micros: ts_micros,
    })
}

/// Outcome of an OCC-guarded single-row mutation (`CheckAndMutateRow`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasOutcome {
    /// Mutation succeeded at the returned OCC version.
    Applied(u64),
    /// Row's `m:v` (or presence) changed concurrently without an active 2PC
    /// intent; caller should re-read, re-evaluate conditions/updates, and retry.
    OccMismatch,
}

/// Decodes an OCC row version from cell bytes.
pub fn decode_version(bytes: &[u8]) -> Option<u64> {
    if bytes.len() == 8 {
        let arr: [u8; 8] = bytes.try_into().ok()?;
        Some(u64::from_be_bytes(arr))
    } else {
        std::str::from_utf8(bytes).ok()?.parse::<u64>().ok()
    }
}

/// Filter matching any active intent in family `m`.
fn build_active_intent_filter(cutoff_micros: i64) -> RowFilter {
    RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{FAMILY_META}$"))),
                },
                RowFilter {
                    filter: Some(Filter::ColumnQualifierRegexFilter(b"^intent:.*$".to_vec())),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
                RowFilter {
                    filter: Some(Filter::TimestampRangeFilter(TimestampRange {
                        start_timestamp_micros: cutoff_micros.max(0),
                        end_timestamp_micros: 0,
                    })),
                },
            ],
        })),
    }
}

/// Filter matching row version `m:v == expected_version`.
pub(crate) fn build_version_match_filter(expected_version: u64) -> RowFilter {
    let val_regex = format!("^{expected_version}$").into_bytes();
    RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{FAMILY_META}$"))),
                },
                RowFilter {
                    filter: Some(Filter::ColumnQualifierRegexFilter(b"^v$".to_vec())),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
                RowFilter {
                    filter: Some(Filter::ValueRegexFilter(val_regex)),
                },
            ],
        })),
    }
}

/// Filter matching if row version `m:v != expected_version` (or `m:v` is missing).
fn build_version_mismatch_filter(expected_version: u64) -> RowFilter {
    RowFilter {
        filter: Some(Filter::Condition(Box::new(Condition {
            predicate_filter: Some(Box::new(build_version_match_filter(expected_version))),
            true_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::BlockAllFilter(true)),
            })),
            false_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::PassAllFilter(true)),
            })),
        }))),
    }
}

/// De Morgan conflict filter for existing-row OCC update/delete:
/// Matches if `(active intent exists in m) OR (m:v != expected_version)`.
/// When this filter matches (conflict), mutations are skipped.
/// When this filter does NOT match (no conflict), `false_mutations` are applied.
fn build_occ_conflict_filter(cutoff_micros: i64, expected_version: u64) -> RowFilter {
    RowFilter {
        filter: Some(Filter::Interleave(Interleave {
            filters: vec![
                build_active_intent_filter(cutoff_micros),
                build_version_mismatch_filter(expected_version),
            ],
        })),
    }
}

/// Filter matching if active intent exists OR `m:v` exists OR data family `d` exists.
fn build_row_presence_conflict_filter(cutoff_micros: i64) -> RowFilter {
    RowFilter {
        filter: Some(Filter::Interleave(Interleave {
            filters: vec![
                build_active_intent_filter(cutoff_micros),
                RowFilter {
                    filter: Some(Filter::Chain(Chain {
                        filters: vec![
                            RowFilter {
                                filter: Some(Filter::FamilyNameRegexFilter(format!(
                                    "^{FAMILY_META}$"
                                ))),
                            },
                            RowFilter {
                                filter: Some(Filter::ColumnQualifierRegexFilter(b"^v$".to_vec())),
                            },
                            RowFilter {
                                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                            },
                        ],
                    })),
                },
                RowFilter {
                    filter: Some(Filter::Chain(Chain {
                        filters: vec![
                            RowFilter {
                                filter: Some(Filter::FamilyNameRegexFilter(format!(
                                    "^{FAMILY_DATA}$"
                                ))),
                            },
                            RowFilter {
                                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                            },
                        ],
                    })),
                },
            ],
        })),
    }
}

/// Conflict filter for updating/deleting a legacy row that existed in family `d`
/// without an `m:v` cell: conflicts if an active intent exists, OR `m:v` appeared
/// concurrently, OR family `d` was deleted concurrently.
fn build_unversioned_existing_conflict_filter(cutoff_micros: i64) -> RowFilter {
    let missing_data_filter = RowFilter {
        filter: Some(Filter::Condition(Box::new(Condition {
            predicate_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::Chain(Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!("^{FAMILY_DATA}$"))),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                    ],
                })),
            })),
            true_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::BlockAllFilter(true)),
            })),
            false_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::PassAllFilter(true)),
            })),
        }))),
    };
    RowFilter {
        filter: Some(Filter::Interleave(Interleave {
            filters: vec![
                build_active_intent_filter(cutoff_micros),
                RowFilter {
                    filter: Some(Filter::Chain(Chain {
                        filters: vec![
                            RowFilter {
                                filter: Some(Filter::FamilyNameRegexFilter(format!(
                                    "^{FAMILY_META}$"
                                ))),
                            },
                            RowFilter {
                                filter: Some(Filter::ColumnQualifierRegexFilter(b"^v$".to_vec())),
                            },
                            RowFilter {
                                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                            },
                        ],
                    })),
                },
                missing_data_filter,
            ],
        })),
    }
}

fn select_occ_predicate(
    cutoff_micros: i64,
    expected_version: Option<u64>,
    row_existed: bool,
) -> RowFilter {
    match (expected_version, row_existed) {
        (Some(v), _) => build_occ_conflict_filter(cutoff_micros, v),
        (None, true) => build_unversioned_existing_conflict_filter(cutoff_micros),
        (None, false) => build_row_presence_conflict_filter(cutoff_micros),
    }
}

/// Holds the table-specific context for a sequence of data operations.
pub struct ItemOps<'a> {
    client: &'a BigtableClient,
    full_table_name: String,
    intent_timeout_secs: u64,
}

impl<'a> ItemOps<'a> {
    pub fn new(
        client: &'a BigtableClient,
        data_table_short: &str,
        intent_timeout_secs: u64,
    ) -> Self {
        Self {
            client,
            full_table_name: client.full_table_name(data_table_short),
            intent_timeout_secs,
        }
    }

    fn intent_cutoff_micros(&self) -> i64 {
        let timeout_micros = (self.intent_timeout_secs.saturating_mul(1_000_000)) as i64;
        now_millis_micros().saturating_sub(timeout_micros)
    }

    /// Check whether an unexpired 2PC intent cell currently holds `row_key`.
    async fn has_active_intent(&self, row_key: &[u8]) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![row_key.to_vec()],
                row_ranges: vec![],
            }),
            filter: Some(build_active_intent_filter(self.intent_cutoff_micros())),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| map_bigtable_err("ReadRows check_intent", e))?;
        Ok(!resp.is_empty())
    }

    /// Read the entire row by raw encoded key into an `Item` map along with its
    /// captured OCC row version (`m:v`).
    pub async fn get_by_raw_key_with_version(
        &self,
        row_key: &[u8],
    ) -> Result<(Option<Item>, Option<u64>), StorageError> {
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![row_key.to_vec()],
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
            .map_err(|e| map_bigtable_err("ReadRows get_with_version", e))?;
        let mut iter = resp.into_iter();
        let Some((_, cells)) = iter.next() else {
            return Ok((None, None));
        };
        let mut item: Item = BTreeMap::new();
        let mut version: Option<u64> = None;
        for c in cells {
            if c.family_name == FAMILY_DATA {
                let attr_name = String::from_utf8(c.qualifier)
                    .map_err(|e| StorageError::Internal(format!("decode qualifier: {e}")))?;
                if let std::collections::btree_map::Entry::Vacant(e) = item.entry(attr_name) {
                    let value = cell::decode(&c.value)?;
                    e.insert(value);
                }
            } else if c.family_name == FAMILY_META
                && c.qualifier.as_slice() == QUALIFIER_VERSION
                && version.is_none()
            {
                version = decode_version(&c.value);
            }
        }
        if item.is_empty() {
            return Ok((None, version));
        }
        Ok((Some(item), version))
    }

    /// Read the entire row into an `Item` map along with its captured OCC row version.
    /// Returns `Ok((None, version))` if family `d` has no cells (including deleted
    /// rows that retain a monotonic `m:v` version marker).
    pub async fn get_with_version(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<(Option<Item>, Option<u64>), StorageError> {
        let row_key = row_key::encode_key(key, &key_info.key_schema)?;
        self.get_by_raw_key_with_version(&row_key).await
    }

    /// Read the entire row into an `Item` map. Returns `Ok(None)` if absent.
    pub async fn get(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let (item, _) = self.get_with_version(key_info, key).await?;
        Ok(item)
    }

    /// Read multiple rows into `Item` maps using pre-encoded raw row keys,
    /// returning results in the exact order of `row_keys`.
    pub async fn batch_get_by_raw_keys(
        &self,
        row_keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Item>>, StorageError> {
        if row_keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: row_keys.len() as i64,
            rows: Some(RowSet {
                row_keys: row_keys.to_vec(),
                row_ranges: vec![],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::Chain(Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!("^{FAMILY_DATA}$"))),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                    ],
                })),
            }),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| map_bigtable_err("ReadRows batch", e))?;

        let mut row_map = BTreeMap::new();
        for (rkey, cells) in resp {
            let mut item: Item = BTreeMap::new();
            for c in cells {
                if c.family_name == FAMILY_DATA {
                    let attr_name = String::from_utf8(c.qualifier)
                        .map_err(|e| StorageError::Internal(format!("decode qualifier: {e}")))?;
                    if let std::collections::btree_map::Entry::Vacant(e) = item.entry(attr_name) {
                        let value = cell::decode(&c.value)?;
                        e.insert(value);
                    }
                }
            }
            if !item.is_empty() {
                row_map.insert(rkey, item);
            }
        }

        Ok(row_keys
            .iter()
            .map(|rkey| row_map.get(rkey).cloned())
            .collect())
    }

    /// Read multiple rows into `Item` maps. Returns items in the same order as `keys`.
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
        self.batch_get_by_raw_keys(&row_keys).await
    }

    /// Read multiple rows into `Item` maps along with any active intent `txn_id` in column family `m`.
    pub async fn batch_get_with_intent_check(
        &self,
        key_info: &TableKeyInfo,
        keys: &[Item],
    ) -> Result<Vec<(Option<Item>, Option<String>)>, StorageError> {
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
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
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
            .map_err(|e| map_bigtable_err("ReadRows batch_get_with_intent_check", e))?;

        let cutoff_micros = self.intent_cutoff_micros();
        let mut row_map: BTreeMap<Vec<u8>, (Option<Item>, Option<String>)> = BTreeMap::new();

        for (rkey, cells) in resp {
            let mut item: Item = BTreeMap::new();
            let mut active_txn_id: Option<String> = None;
            for c in cells {
                if c.family_name == FAMILY_DATA {
                    let attr_name = String::from_utf8(c.qualifier)
                        .map_err(|e| StorageError::Internal(format!("decode qualifier: {e}")))?;
                    if let std::collections::btree_map::Entry::Vacant(e) = item.entry(attr_name) {
                        let value = cell::decode(&c.value)?;
                        e.insert(value);
                    }
                } else if c.family_name == FAMILY_META
                    && c.qualifier.starts_with(QUALIFIER_INTENT_PREFIX)
                    && c.timestamp_micros >= cutoff_micros
                    && active_txn_id.is_none()
                    && let Ok(qual_str) = std::str::from_utf8(&c.qualifier)
                    && let Some(tid) = qual_str.strip_prefix("intent:")
                {
                    active_txn_id = Some(tid.to_string());
                }
            }
            let item_opt = if item.is_empty() { None } else { Some(item) };
            row_map.insert(rkey, (item_opt, active_txn_id));
        }

        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            let rkey = row_key::encode_key(k, &key_info.key_schema)?;
            out.push(row_map.get(&rkey).cloned().unwrap_or((None, None)));
        }

        Ok(out)
    }

    /// OCC-guarded Put: atomically verifies no active 2PC intent AND `m:v == expected_version`
    /// (or row absence when `expected_version == None`), then replaces family `d` and updates
    /// `m:v` with a single deterministic cell timestamp `version_to_timestamp_micros(next_version)`.
    ///
    /// Distinguishes active 2PC intent conflicts (`Err(StorageError::TransactionConflict)`)
    /// from OCC version mismatches (`Ok(CasOutcome::OccMismatch)`).
    pub async fn put_cas(
        &self,
        key_info: &TableKeyInfo,
        item: &Item,
        expected_version: Option<u64>,
        row_existed: bool,
    ) -> Result<CasOutcome, StorageError> {
        let row_key = row_key::encode_key(item, &key_info.key_schema)?;
        let min_timestamp = self.intent_cutoff_micros();

        let next_version = next_occ_version(expected_version);
        let ts_micros = version_to_timestamp_micros(next_version);

        let mut mutations = self.item_to_mutations_at(item, true, ts_micros)?;
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                time_range: older_than_ts_range(ts_micros),
            })),
        });
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::SetCell(SetCell {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                timestamp_micros: ts_micros,
                value: next_version.to_string().into_bytes(),
            })),
        });

        let predicate = select_occ_predicate(min_timestamp, expected_version, row_existed);

        let mut data = self.client.data();
        let req = CheckAndMutateRowRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key.clone(),
            predicate_filter: Some(predicate),
            true_mutations: vec![],
            false_mutations: mutations,
            ..CheckAndMutateRowRequest::default()
        };

        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("CheckAndMutateRow put", e))?;

        if !resp.predicate_matched {
            return Ok(CasOutcome::Applied(next_version));
        }

        if self.has_active_intent(&row_key).await? {
            return Err(StorageError::TransactionConflict(
                "concurrent transaction holds an intent on this row".to_string(),
            ));
        }
        Ok(CasOutcome::OccMismatch)
    }

    /// Backwards-compatible guarded `put` wrapper returning `TransactionConflict`
    /// on any conflict.
    pub async fn put(
        &self,
        key_info: &TableKeyInfo,
        item: &Item,
        expected_version: Option<u64>,
        _is_conditional: bool,
    ) -> Result<u64, StorageError> {
        match self
            .put_cas(key_info, item, expected_version, expected_version.is_some())
            .await?
        {
            CasOutcome::Applied(v) => Ok(v),
            CasOutcome::OccMismatch => Err(StorageError::TransactionConflict(
                "row version changed concurrently".to_string(),
            )),
        }
    }

    /// OCC-guarded Delete: verifies no active 2PC intent AND `m:v == expected_version`
    /// (or row state when `expected_version == None`). Clears family `d` while
    /// advancing `m:v` to `next_occ_version(expected_version)` at
    /// `version_to_timestamp_micros(next_version)` whenever a row or `m:v` existed,
    /// so subsequent inserts never regress `m:v` or reuse an older cell timestamp.
    pub async fn delete_cas(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        expected_version: Option<u64>,
        row_existed: bool,
    ) -> Result<CasOutcome, StorageError> {
        let row_key = row_key::encode_key(key, &key_info.key_schema)?;
        let min_timestamp = self.intent_cutoff_micros();

        let next_version = next_occ_version(expected_version);
        let ts_micros = version_to_timestamp_micros(next_version);

        let mut mutations = vec![
            Mutation {
                mutation: Some(mutation::Mutation::DeleteFromFamily(DeleteFromFamily {
                    family_name: FAMILY_DATA.to_string(),
                })),
            },
            Mutation {
                mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                    family_name: FAMILY_META.to_string(),
                    column_qualifier: QUALIFIER_VERSION.to_vec(),
                    time_range: older_than_ts_range(ts_micros),
                })),
            },
        ];
        if expected_version.is_some() || row_existed {
            mutations.push(Mutation {
                mutation: Some(mutation::Mutation::SetCell(SetCell {
                    family_name: FAMILY_META.to_string(),
                    column_qualifier: QUALIFIER_VERSION.to_vec(),
                    timestamp_micros: ts_micros,
                    value: next_version.to_string().into_bytes(),
                })),
            });
        }

        let predicate = select_occ_predicate(min_timestamp, expected_version, row_existed);

        let mut data = self.client.data();
        let req = CheckAndMutateRowRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key.clone(),
            predicate_filter: Some(predicate),
            true_mutations: vec![],
            false_mutations: mutations,
            ..CheckAndMutateRowRequest::default()
        };

        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("CheckAndMutateRow delete", e))?;

        if !resp.predicate_matched {
            return Ok(CasOutcome::Applied(next_version));
        }

        if self.has_active_intent(&row_key).await? {
            return Err(StorageError::TransactionConflict(
                "concurrent transaction holds an intent on this row".to_string(),
            ));
        }
        Ok(CasOutcome::OccMismatch)
    }

    /// Backwards-compatible guarded `delete` wrapper.
    pub async fn delete(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        expected_version: Option<u64>,
        _is_conditional: bool,
    ) -> Result<(), StorageError> {
        match self
            .delete_cas(key_info, key, expected_version, expected_version.is_some())
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::OccMismatch => Err(StorageError::TransactionConflict(
                "row version changed concurrently".to_string(),
            )),
        }
    }

    /// Replace any existing row with the supplied item using a deterministic
    /// OCC-derived cell timestamp. No condition check, no lock check.
    pub async fn put_unconditional(
        &self,
        key_info: &TableKeyInfo,
        item: &Item,
    ) -> Result<u64, StorageError> {
        let row_key = row_key::encode_key(item, &key_info.key_schema)?;
        let version = next_occ_version(None);
        let ts_micros = version_to_timestamp_micros(version);
        let mut mutations = self.item_to_mutations_at(item, true, ts_micros)?;
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                time_range: older_than_ts_range(ts_micros),
            })),
        });
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::SetCell(SetCell {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                timestamp_micros: ts_micros,
                value: version.to_string().into_bytes(),
            })),
        });
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key,
            mutations,
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("MutateRow put_unconditional", e))?;
        Ok(version)
    }

    /// Delete a row by key, removing data and `m:v`. No condition check, no lock check.
    pub async fn delete_unconditional(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<(), StorageError> {
        let row_key = row_key::encode_key(key, &key_info.key_schema)?;
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key,
            mutations: vec![
                Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromFamily(DeleteFromFamily {
                        family_name: FAMILY_DATA.to_string(),
                    })),
                },
                Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: FAMILY_META.to_string(),
                        column_qualifier: QUALIFIER_VERSION.to_vec(),
                        time_range: None,
                    })),
                },
            ],
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("MutateRow delete_unconditional", e))?;
        Ok(())
    }

    /// Apply a list of mutations (`SetCell` / `DeleteFromColumn` / `DeleteFromRow`) to a row by key.
    pub async fn mutate_cells(
        &self,
        row_key_bytes: Vec<u8>,
        mutations: Vec<Mutation>,
    ) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key_bytes,
            mutations,
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("MutateRow mutate_cells", e))?;
        Ok(())
    }

    /// Turn an `Item` map into BigTable `Mutation`s where every `SetCell` uses
    /// the deterministic `timestamp_micros` derived from the row's OCC version.
    ///
    /// `timestamp_micros` MUST be divisible by `1_000` (`TimestampGranularity::MILLIS`).
    pub fn item_to_mutations_at(
        &self,
        item: &Item,
        delete_row_first: bool,
        timestamp_micros: i64,
    ) -> Result<Vec<Mutation>, StorageError> {
        debug_assert!(
            timestamp_micros > 0 && timestamp_micros % 1_000 == 0,
            "Bigtable cell timestamp must be positive and millisecond-aligned, got {timestamp_micros}"
        );
        let mut mutations = Vec::with_capacity(item.len() + 1);
        if delete_row_first {
            mutations.push(Mutation {
                mutation: Some(mutation::Mutation::DeleteFromFamily(DeleteFromFamily {
                    family_name: FAMILY_DATA.to_string(),
                })),
            });
        }
        for (name, value) in item {
            mutations.push(Mutation {
                mutation: Some(mutation::Mutation::SetCell(SetCell {
                    family_name: FAMILY_DATA.to_string(),
                    column_qualifier: name.as_bytes().to_vec(),
                    timestamp_micros,
                    value: cell::encode(value)?,
                })),
            });
        }
        Ok(mutations)
    }

    /// Turn an `Item` map into BigTable `Mutation`s stamped at `monotonic_timestamp_micros()`.
    pub fn item_to_mutations(
        &self,
        item: &Item,
        delete_row_first: bool,
    ) -> Result<Vec<Mutation>, StorageError> {
        self.item_to_mutations_at(item, delete_row_first, monotonic_timestamp_micros())
    }
}

/// Apply `UpdateAction`s with full vector-index and attribute-definition validation.
pub fn apply_update_actions_validated(
    existing: &Item,
    actions: &[UpdateAction],
    maps: &ExpressionMaps,
    key_info: &TableKeyInfo,
    attr_defs: &[AttributeDefinition],
) -> Result<Item, StorageError> {
    let mut new_item = existing.clone();
    apply_update_validated(
        actions,
        &mut new_item,
        maps,
        &key_info.vector_indexes,
        attr_defs,
    )
    .map_err(|e| StorageError::Validation(format!("update expression evaluation: {e}")))?;
    Ok(new_item)
}

/// Apply `UpdateAction`s to an existing item (or key seed if absent). Returns the
/// new image. Used by `UpdateItem`.
pub fn apply_update_actions(
    existing: &Item,
    actions: &[UpdateAction],
    maps: &ExpressionMaps,
) -> Result<Item, StorageError> {
    let mut new_item = existing.clone();
    apply_update_validated(actions, &mut new_item, maps, &[], &[])
        .map_err(|e| StorageError::Validation(format!("update expression evaluation: {e}")))?;
    Ok(new_item)
}

/// Evaluate a `ConditionExpression` against the existing item. Returns `Ok(())`
/// if the condition holds (or is `None`); `ConditionFailed` if it doesn't.
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
        Err(e) => Err(StorageError::Validation(format!(
            "condition evaluation: {e}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_version_string_and_bytes() {
        assert_eq!(decode_version(b"1"), Some(1));
        assert_eq!(decode_version(b"42"), Some(42));
        assert_eq!(decode_version(b"18446744073709551615"), Some(u64::MAX));

        let be_bytes = 123_456_789_012_345u64.to_be_bytes();
        assert_eq!(decode_version(&be_bytes), Some(123_456_789_012_345));

        assert_eq!(decode_version(b"not_a_number"), None);
        assert_eq!(decode_version(b""), None);
    }

    #[test]
    fn test_occ_version_and_millisecond_aligned_timestamps() {
        let v1 = next_occ_version(None);
        assert!(v1 > 0);
        let ts1 = version_to_timestamp_micros(v1);
        assert_eq!(ts1 % 1_000, 0, "timestamp must be millisecond-aligned");

        // Rapid sub-millisecond updates strictly advance both version and timestamp.
        let v2 = next_occ_version(Some(v1));
        let v3 = next_occ_version(Some(v2));
        assert!(v2 > v1);
        assert!(v3 > v2);
        assert!(version_to_timestamp_micros(v2) > ts1);
        assert!(version_to_timestamp_micros(v3) > version_to_timestamp_micros(v2));
        assert_eq!(version_to_timestamp_micros(v2) % 1_000, 0);
        assert_eq!(version_to_timestamp_micros(v3) % 1_000, 0);
    }

    #[test]
    fn test_parse_ttl_epoch_secs_positive_integer_decimal_and_scientific() {
        assert_eq!(parse_ttl_epoch_secs("1700000000"), Some(1_700_000_000));
        assert_eq!(parse_ttl_epoch_secs("1700000000.99"), Some(1_700_000_000));
        assert_eq!(parse_ttl_epoch_secs("1.7E9"), Some(1_700_000_000));
        assert_eq!(parse_ttl_epoch_secs("1"), Some(1));
        assert_eq!(parse_ttl_epoch_secs("0"), None);
        assert_eq!(parse_ttl_epoch_secs("0.99"), None);
        assert_eq!(parse_ttl_epoch_secs("-42"), None);
        assert_eq!(parse_ttl_epoch_secs("not-a-number"), None);
    }

    #[test]
    fn test_filter_builders() {
        let intent_filter = build_active_intent_filter(1000);
        assert!(intent_filter.filter.is_some());

        let version_filter = build_version_match_filter(42);
        assert!(version_filter.filter.is_some());

        let mismatch_filter = build_version_mismatch_filter(42);
        assert!(mismatch_filter.filter.is_some());

        let occ_conflict_filter = build_occ_conflict_filter(1000, 42);
        if let Some(Filter::Interleave(interleave)) = occ_conflict_filter.filter {
            assert_eq!(interleave.filters.len(), 2);
            if let Some(Filter::Condition(cond)) = &interleave.filters[1].filter {
                assert!(matches!(
                    cond.true_filter.as_ref().and_then(|f| f.filter.as_ref()),
                    Some(Filter::BlockAllFilter(true))
                ));
                assert!(matches!(
                    cond.false_filter.as_ref().and_then(|f| f.filter.as_ref()),
                    Some(Filter::PassAllFilter(true))
                ));
            } else {
                panic!("expected Condition filter in second branch");
            }
        } else {
            panic!("expected Interleave filter");
        }

        let conflict_filter = build_row_presence_conflict_filter(1000);
        assert!(conflict_filter.filter.is_some());

        let unversioned_existing = select_occ_predicate(1000, None, true);
        if let Some(Filter::Interleave(interleave)) = unversioned_existing.filter {
            assert_eq!(interleave.filters.len(), 3);
            assert!(matches!(
                interleave.filters[2].filter,
                Some(Filter::Condition(_))
            ));
        } else {
            panic!("expected 3-branch Interleave for unversioned existing row");
        }
    }
}
