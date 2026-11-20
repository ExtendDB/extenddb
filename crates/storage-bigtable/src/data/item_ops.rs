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
    DeleteFromColumn, DeleteFromFamily, DeleteFromRow, SetCell,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::{
    Chain, Condition, Filter, Interleave,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    CheckAndMutateRowRequest, MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowSet,
    TimestampRange, mutation,
};

use crate::data::client::{BigtableClient, map_bigtable_err};
use crate::data::encoding::{cell, row_key};

/// Family for DDB attribute cells.
pub const FAMILY_DATA: &str = "d";
/// Family for metadata cells (OCC row version, 2PC intents).
pub const FAMILY_META: &str = "m";
/// Qualifier for OCC row version cell in family `m`.
pub const QUALIFIER_VERSION: &[u8] = b"v";
/// Qualifier prefix for 2PC intent cells in family `m`.
pub const QUALIFIER_INTENT_PREFIX: &[u8] = b"intent:";
/// Safe per-RPC mutation batch ceiling below Bigtable's 100,000-mutation hard limit.
pub const MAX_MUTATIONS_PER_RPC: usize = 90_000;

#[cfg(feature = "test-hooks")]
tokio::task_local! {
    static TEST_MAX_MUTATIONS_PER_RPC: usize;
}

/// Active per-RPC mutation ceiling (`MAX_MUTATIONS_PER_RPC` in production;
/// task-locally overridable under `test-hooks` so emulator integration tests
/// exercise multi-chunk CAS + overflow `MutateRow` execution without hitting
/// `cbtemulator`'s quadratic in-memory column-slice insertion).
#[inline]
pub fn max_mutations_per_rpc() -> usize {
    #[cfg(feature = "test-hooks")]
    {
        TEST_MAX_MUTATIONS_PER_RPC
            .try_with(|&limit| limit.max(4))
            .unwrap_or(MAX_MUTATIONS_PER_RPC)
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        MAX_MUTATIONS_PER_RPC
    }
}

/// Execute `fut` with a task-local per-RPC mutation chunk ceiling (`test-hooks` only).
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub async fn with_max_mutations_per_rpc<F, R>(limit: usize, fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    TEST_MAX_MUTATIONS_PER_RPC.scope(limit.max(4), fut).await
}

/// Split a mutation vector into `(initial_cas_batch, overflow_chunks)` where
/// every returned batch has length `<= limit`.
pub fn split_mutation_batches(
    mut mutations: Vec<Mutation>,
    limit: usize,
) -> (Vec<Mutation>, Vec<Vec<Mutation>>) {
    let cap = limit.max(1);
    if mutations.len() <= cap {
        return (mutations, Vec::new());
    }
    let rest = mutations.split_off(cap);
    let chunks = rest.chunks(cap).map(<[Mutation]>::to_vec).collect();
    (mutations, chunks)
}

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

/// Smallest millisecond-aligned (`% 1_000 == 0`) timestamp in microseconds
/// strictly greater than `ts_micros`.
///
/// Production Cloud Bigtable (`TimestampGranularity::MILLIS`) requires
/// `TimestampRange` bounds on `TimestampRangeFilter` and `DeleteFromColumn` to
/// be multiples of `1_000` microseconds. Since OCC versions map to
/// `v * 1_000`, `next_millis_exclusive(v * 1_000) == (v + 1) * 1_000`, which
/// includes `v * 1_000` in `[0, next)` while excluding every strictly newer
/// OCC version `>= v + 1`.
#[inline]
pub const fn next_millis_exclusive(ts_micros: i64) -> i64 {
    if ts_micros <= 0 {
        1_000
    } else {
        ((ts_micros / 1_000).saturating_add(1)).saturating_mul(1_000)
    }
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

/// Bounded `[0, ts_micros)` range used when pre-clearing older `m:v` or TTL
/// index cells so a delayed mutation never deletes a strictly newer version.
#[inline]
pub(crate) fn older_than_ts_range(ts_micros: i64) -> Option<TimestampRange> {
    let aligned = (ts_micros / 1_000) * 1_000;
    (aligned > 0).then_some(TimestampRange {
        start_timestamp_micros: 0,
        end_timestamp_micros: aligned,
    })
}

/// Outcome of an OCC-guarded single-row mutation (`CheckAndMutateRow`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasOutcome {
    /// Mutation succeeded at the returned OCC version.
    Applied(u64),
    /// One or more `m:intent:<txn_id>` cells are present on the row;
    /// caller must cooperatively resolve `COMMITTED` / expired `PENDING`
    /// transactions via `__extenddb_txn_log__` before retrying or failing.
    IntentPresent,
    /// Row's `m:v` (or presence) changed concurrently without an active 2PC
    /// intent; caller should re-read, re-evaluate conditions/updates, and retry.
    OccMismatch,
}

/// Atomic single-row read snapshot capturing `d:*`, `m:v`, and all physical
/// `(txn_id, intent_cell_ts_micros)` entries regardless of wall-clock age.
#[derive(Debug, Clone, PartialEq)]
pub struct RowSnapshot {
    pub item: Option<Item>,
    pub version: Option<u64>,
    pub intents: Vec<(String, i64)>,
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

/// Filter matching ANY physical `m:intent:*` cell in family `m`, intentionally
/// WITHOUT a wall-clock `TimestampRangeFilter`: an intent older than
/// `intent_timeout_secs` may belong to a `COMMITTED` transaction in
/// `__extenddb_txn_log__` awaiting roll-forward and must never be bypassed.
pub(crate) fn build_active_intent_filter() -> RowFilter {
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
            ],
        })),
    }
}

/// Filter matching row version `m:v == expected_version` on the LATEST cell.
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
/// Matches if `(any intent exists in m) OR (m:v != expected_version)`.
fn build_occ_conflict_filter(expected_version: u64) -> RowFilter {
    RowFilter {
        filter: Some(Filter::Interleave(Interleave {
            filters: vec![
                build_active_intent_filter(),
                build_version_mismatch_filter(expected_version),
            ],
        })),
    }
}

/// Filter matching if any intent exists OR `m:v` exists OR data family `d` exists.
fn build_row_presence_conflict_filter() -> RowFilter {
    RowFilter {
        filter: Some(Filter::Interleave(Interleave {
            filters: vec![
                build_active_intent_filter(),
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
/// without an `m:v` cell: conflicts if any intent exists, OR `m:v` appeared
/// concurrently, OR family `d` was deleted concurrently.
fn build_unversioned_existing_conflict_filter() -> RowFilter {
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
                build_active_intent_filter(),
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

fn select_occ_predicate(expected_version: Option<u64>, row_existed: bool) -> RowFilter {
    match (expected_version, row_existed) {
        (Some(v), _) => build_occ_conflict_filter(v),
        (None, true) => build_unversioned_existing_conflict_filter(),
        (None, false) => build_row_presence_conflict_filter(),
    }
}

/// De Morgan conflict filter for reclaiming a version-only tombstone row at
/// `expected_version`: matches (preventing `DeleteFromRow` in `false_mutations`)
/// whenever ANY `m:intent:*` cell exists, OR `m:v != expected_version`, OR ANY
/// `d:*` data cell exists.
pub(crate) fn build_tombstone_reclaim_conflict_filter(expected_version: u64) -> RowFilter {
    RowFilter {
        filter: Some(Filter::Interleave(Interleave {
            filters: vec![
                build_active_intent_filter(),
                build_version_mismatch_filter(expected_version),
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

fn decode_snapshot_cells(
    cells: Vec<crate::data::client::RowCell>,
) -> Result<RowSnapshot, StorageError> {
    let mut item: Item = BTreeMap::new();
    let mut version: Option<u64> = None;
    let mut intents: BTreeMap<String, i64> = BTreeMap::new();
    for c in cells {
        if c.family_name == FAMILY_DATA {
            let attr_name = String::from_utf8(c.qualifier)
                .map_err(|e| StorageError::Internal(format!("decode qualifier: {e}")))?;
            if let std::collections::btree_map::Entry::Vacant(e) = item.entry(attr_name) {
                let value = cell::decode(&c.value)?;
                e.insert(value);
            }
        } else if c.family_name == FAMILY_META {
            if c.qualifier.as_slice() == QUALIFIER_VERSION && version.is_none() {
                version = decode_version(&c.value);
            } else if c.qualifier.starts_with(QUALIFIER_INTENT_PREFIX)
                && let Ok(qual_str) = std::str::from_utf8(&c.qualifier)
                && let Some(tid) = qual_str.strip_prefix("intent:")
            {
                intents
                    .entry(tid.to_string())
                    .and_modify(|ts| *ts = (*ts).max(c.timestamp_micros))
                    .or_insert(c.timestamp_micros);
            }
        }
    }
    Ok(RowSnapshot {
        item: (!item.is_empty()).then_some(item),
        version,
        intents: intents.into_iter().collect(),
    })
}

/// Holds the table-specific context for a sequence of data operations.
pub struct ItemOps<'a> {
    client: &'a BigtableClient,
    full_table_name: String,
}

impl<'a> ItemOps<'a> {
    pub fn new(
        client: &'a BigtableClient,
        data_table_short: &str,
        _intent_timeout_secs: u64,
    ) -> Self {
        Self {
            client,
            full_table_name: client.full_table_name(data_table_short),
        }
    }

    /// Check whether ANY physical 2PC intent cell currently holds `row_key`.
    pub async fn has_any_intent(&self, row_key: &[u8]) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![row_key.to_vec()],
                row_ranges: vec![],
            }),
            filter: Some(build_active_intent_filter()),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| map_bigtable_err("ReadRows check_intent", e))?;
        Ok(!resp.is_empty())
    }

    /// Read `(item, occ_version, all_intents)` for a single row by raw key.
    pub async fn get_row_snapshot_by_raw_key(
        &self,
        row_key: &[u8],
    ) -> Result<RowSnapshot, StorageError> {
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
            .map_err(|e| map_bigtable_err("ReadRows get_row_snapshot", e))?;
        let Some((_, cells)) = resp.into_iter().next() else {
            return Ok(RowSnapshot {
                item: None,
                version: None,
                intents: Vec::new(),
            });
        };
        decode_snapshot_cells(cells)
    }

    /// Batch-read `(item, occ_version, all_intents)` across pre-encoded raw keys,
    /// returning snapshots in the exact order of `row_keys`.
    pub async fn batch_get_row_snapshots_by_raw_keys(
        &self,
        row_keys: &[Vec<u8>],
    ) -> Result<Vec<RowSnapshot>, StorageError> {
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
                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            }),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| map_bigtable_err("ReadRows batch_get_row_snapshots", e))?;
        let mut map: BTreeMap<Vec<u8>, RowSnapshot> = BTreeMap::new();
        for (rk, cells) in resp {
            map.insert(rk, decode_snapshot_cells(cells)?);
        }
        Ok(row_keys
            .iter()
            .map(|rk| {
                map.get(rk).cloned().unwrap_or(RowSnapshot {
                    item: None,
                    version: None,
                    intents: Vec::new(),
                })
            })
            .collect())
    }

    /// Read the entire row by raw encoded key into an `Item` map along with its
    /// captured OCC row version (`m:v`).
    pub async fn get_by_raw_key_with_version(
        &self,
        row_key: &[u8],
    ) -> Result<(Option<Item>, Option<u64>), StorageError> {
        let snap = self.get_row_snapshot_by_raw_key(row_key).await?;
        Ok((snap.item, snap.version))
    }

    /// Read the entire row into an `Item` map along with its captured OCC row version.
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

    /// OCC-guarded Put: atomically verifies no `m:intent:*` cell AND
    /// `m:v == expected_version` (or row absence when `expected_version == None`),
    /// then replaces family `d` and updates `m:v` at
    /// `version_to_timestamp_micros(next_version)`.
    pub async fn put_cas(
        &self,
        key_info: &TableKeyInfo,
        item: &Item,
        expected_version: Option<u64>,
        row_existed: bool,
    ) -> Result<CasOutcome, StorageError> {
        let row_key = row_key::encode_key(item, &key_info.key_schema)?;
        let next_version = next_occ_version(expected_version);
        let ts_micros = version_to_timestamp_micros(next_version);

        let mut mutations = Vec::with_capacity(item.len() + 3);
        // Place `m:v` and `DeleteFromFamily("d")` at the front so the first
        // `MAX_MUTATIONS_PER_RPC` chunk atomically advances the OCC fence even
        // when an item carries > 90,000 small attributes.
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::SetCell(SetCell {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                timestamp_micros: ts_micros,
                value: next_version.to_string().into_bytes(),
            })),
        });
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                time_range: older_than_ts_range(ts_micros),
            })),
        });
        mutations.extend(self.item_to_mutations_at(item, true, ts_micros)?);

        let rpc_limit = max_mutations_per_rpc();
        let (mutations, overflow_chunks) = split_mutation_batches(mutations, rpc_limit);

        let predicate = select_occ_predicate(expected_version, row_existed);

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
            .check_and_mutate_row_idempotent(req)
            .await
            .map_err(|e| map_bigtable_err("CheckAndMutateRow put", e))?;

        if !resp.predicate_matched {
            for chunk in overflow_chunks {
                self.mutate_cells(row_key.clone(), chunk).await?;
            }
            return Ok(CasOutcome::Applied(next_version));
        }

        if self.has_any_intent(&row_key).await? {
            return Ok(CasOutcome::IntentPresent);
        }
        Ok(CasOutcome::OccMismatch)
    }

    /// Guarded `put` wrapper returning `TransactionConflict` on conflict.
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
            CasOutcome::IntentPresent => Err(StorageError::TransactionConflict(
                "concurrent transaction holds an intent on this row".to_string(),
            )),
            CasOutcome::OccMismatch => Err(StorageError::TransactionConflict(
                "row version changed concurrently".to_string(),
            )),
        }
    }

    /// OCC-guarded Delete: verifies no `m:intent:*` cell AND `m:v == expected_version`.
    /// Clears family `d` while advancing `m:v` to `next_occ_version(expected_version)`
    /// whenever a row or `m:v` existed.
    pub async fn delete_cas(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        expected_version: Option<u64>,
        row_existed: bool,
    ) -> Result<CasOutcome, StorageError> {
        let row_key = row_key::encode_key(key, &key_info.key_schema)?;
        let next_version = next_occ_version(expected_version);
        let ts_micros = version_to_timestamp_micros(next_version);

        let mut mutations = vec![Mutation {
            mutation: Some(mutation::Mutation::DeleteFromFamily(DeleteFromFamily {
                family_name: FAMILY_DATA.to_string(),
            })),
        }];
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
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                time_range: older_than_ts_range(ts_micros),
            })),
        });

        let predicate = select_occ_predicate(expected_version, row_existed);

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
            .check_and_mutate_row_idempotent(req)
            .await
            .map_err(|e| map_bigtable_err("CheckAndMutateRow delete", e))?;

        if !resp.predicate_matched {
            return Ok(CasOutcome::Applied(next_version));
        }

        if self.has_any_intent(&row_key).await? {
            return Ok(CasOutcome::IntentPresent);
        }
        Ok(CasOutcome::OccMismatch)
    }

    /// Guarded `delete` wrapper returning `TransactionConflict` on conflict.
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
            CasOutcome::IntentPresent => Err(StorageError::TransactionConflict(
                "concurrent transaction holds an intent on this row".to_string(),
            )),
            CasOutcome::OccMismatch => Err(StorageError::TransactionConflict(
                "row version changed concurrently".to_string(),
            )),
        }
    }

    /// Read `(existing_d_qualifiers, max_d_timestamp_micros, current_shadow_version)`
    /// for a GSI shadow row.
    async fn read_shadow_row_meta(
        &self,
        shadow_row_key: &[u8],
    ) -> Result<(Vec<Vec<u8>>, i64, Option<u64>), StorageError> {
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![shadow_row_key.to_vec()],
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
            .map_err(|e| map_bigtable_err("ReadRows shadow_meta", e))?;
        let mut quals: Vec<Vec<u8>> = Vec::new();
        let mut max_d_ts: i64 = 0;
        let mut ver: Option<u64> = None;
        if let Some((_, cells)) = resp.into_iter().next() {
            for c in cells {
                if c.family_name == FAMILY_DATA {
                    max_d_ts = max_d_ts.max(c.timestamp_micros);
                    if !quals.iter().any(|q| q.as_slice() == c.qualifier.as_slice()) {
                        quals.push(c.qualifier);
                    }
                } else if c.family_name == FAMILY_META
                    && c.qualifier.as_slice() == QUALIFIER_VERSION
                    && ver.is_none()
                {
                    ver = decode_version(&c.value);
                }
            }
        }
        Ok((quals, max_d_ts, ver))
    }

    fn missing_version_filter() -> RowFilter {
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
                ],
            })),
        }
    }

    /// Execute a version-guarded `CheckAndMutateRow` against a shadow index row
    /// (`cur_ver == Some(cv)` or `cur_ver == None`), transparently flushing any
    /// overflow mutations (`> MAX_MUTATIONS_PER_RPC`) once the atomic version
    /// fence succeeds.
    async fn apply_shadow_version_cas(
        &self,
        op_name: &str,
        shadow_row_key: &[u8],
        cur_ver: Option<u64>,
        mutations: Vec<Mutation>,
    ) -> Result<bool, StorageError> {
        let rpc_limit = max_mutations_per_rpc();
        let (mutations, overflow_chunks) = split_mutation_batches(mutations, rpc_limit);

        let req = match cur_ver {
            Some(cv) => CheckAndMutateRowRequest {
                table_name: self.full_table_name.clone(),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                row_key: shadow_row_key.to_vec(),
                predicate_filter: Some(build_version_match_filter(cv)),
                true_mutations: mutations,
                false_mutations: vec![],
                ..CheckAndMutateRowRequest::default()
            },
            None => CheckAndMutateRowRequest {
                table_name: self.full_table_name.clone(),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                row_key: shadow_row_key.to_vec(),
                predicate_filter: Some(Self::missing_version_filter()),
                true_mutations: vec![],
                false_mutations: mutations,
                ..CheckAndMutateRowRequest::default()
            },
        };

        let mut data = self.client.data();
        let resp = data
            .check_and_mutate_row_idempotent(req)
            .await
            .map_err(|e| map_bigtable_err(op_name, e))?;
        let applied = if cur_ver.is_some() {
            resp.predicate_matched
        } else {
            !resp.predicate_matched
        };
        if applied {
            for chunk in overflow_chunks {
                self.mutate_cells(shadow_row_key.to_vec(), chunk).await?;
            }
        }
        Ok(applied)
    }

    pub(crate) fn build_shadow_put_mutations(
        projected_item: &Item,
        existing_quals: Vec<Vec<u8>>,
        max_d_ts: i64,
        cur_ver: Option<u64>,
        new_version: u64,
    ) -> Result<Vec<Mutation>, StorageError> {
        let base_ts_micros = version_to_timestamp_micros(new_version);
        let ts_micros =
            if (cur_ver.is_none() && max_d_ts >= base_ts_micros) || max_d_ts > base_ts_micros {
                next_millis_exclusive(max_d_ts)
            } else {
                base_ts_micros
            };

        let mut mutations = Vec::with_capacity(projected_item.len() + existing_quals.len() + 2);
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::SetCell(SetCell {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                timestamp_micros: ts_micros,
                value: new_version.to_string().into_bytes(),
            })),
        });
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                time_range: older_than_ts_range(ts_micros),
            })),
        });

        // One `SetCell` per retained projected attribute at `ts_micros >= max_d_ts`:
        // with `MaxNumVersions(1)` on family `d` and `CellsPerColumnLimitFilter(1)`
        // on every reader, retained attributes never need a duplicate
        // `DeleteFromColumn`, keeping 50,000-attribute `ALL`-projected items at
        // `50,002` mutations inside a single atomic `CheckAndMutateRow` RPC (#37).
        for (name, value) in projected_item {
            mutations.push(Mutation {
                mutation: Some(mutation::Mutation::SetCell(SetCell {
                    family_name: FAMILY_DATA.to_string(),
                    column_qualifier: name.as_bytes().to_vec(),
                    timestamp_micros: ts_micros,
                    value: cell::encode(value)?,
                })),
            });
        }

        let del_removed_end = next_millis_exclusive(ts_micros.max(max_d_ts));
        for old_q in existing_quals {
            let retained = std::str::from_utf8(&old_q)
                .ok()
                .is_some_and(|k| projected_item.contains_key(k));
            if !retained {
                mutations.push(Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: FAMILY_DATA.to_string(),
                        column_qualifier: old_q,
                        time_range: older_than_ts_range(del_removed_end),
                    })),
                });
            }
        }

        Ok(mutations)
    }

    /// Version-fenced secondary-index shadow Put:
    ///
    /// Never uses unbounded `DeleteFromFamily("d")` or `DeleteFromRow`, and places
    /// `SetCell` before bounded `DeleteFromColumn` so concurrent index `Query`/`Scan`
    /// readers never observe a transiently empty shadow row on `cbtemulator`.
    /// Rejects (`Ok(false)`) any delayed write whose `new_version < cur_ver`.
    pub async fn put_shadow_versioned(
        &self,
        shadow_row_key: &[u8],
        projected_item: &Item,
        new_version: u64,
    ) -> Result<bool, StorageError> {
        for _ in 0..32 {
            let (existing_quals, max_d_ts, cur_ver) =
                self.read_shadow_row_meta(shadow_row_key).await?;
            if let Some(cv) = cur_ver
                && cv > new_version
            {
                return Ok(false);
            }

            let mutations = Self::build_shadow_put_mutations(
                projected_item,
                existing_quals,
                max_d_ts,
                cur_ver,
                new_version,
            )?;

            if self
                .apply_shadow_version_cas(
                    "CheckAndMutateRow put_shadow_versioned",
                    shadow_row_key,
                    cur_ver,
                    mutations,
                )
                .await?
            {
                return Ok(true);
            }
        }
        Err(StorageError::Transient(
            "shadow index versioned put CAS contention exhausted".to_string(),
        ))
    }

    /// Version-fenced secondary-index shadow Delete:
    ///
    /// Never uses unbounded `DeleteFromFamily("d")` or `DeleteFromRow`. Advances
    /// shadow `m:v` to `new_version` while clearing `d:*` cells bounded by
    /// `[0, next_millis_exclusive(...))` under a `CheckAndMutateRow` guard on `cur_ver`,
    /// rejecting (`Ok(false)`) any delayed delete whose `new_version < cur_ver`.
    pub async fn delete_shadow_versioned(
        &self,
        shadow_row_key: &[u8],
        new_version: u64,
    ) -> Result<bool, StorageError> {
        let ts_micros = version_to_timestamp_micros(new_version);
        for _ in 0..32 {
            let (existing_quals, max_d_ts, cur_ver) =
                self.read_shadow_row_meta(shadow_row_key).await?;
            if let Some(cv) = cur_ver
                && cv > new_version
            {
                return Ok(false);
            }
            if existing_quals.is_empty() && cur_ver == Some(new_version) {
                return Ok(true);
            }

            let mut mutations = Vec::with_capacity(existing_quals.len() + 2);
            mutations.push(Mutation {
                mutation: Some(mutation::Mutation::SetCell(SetCell {
                    family_name: FAMILY_META.to_string(),
                    column_qualifier: QUALIFIER_VERSION.to_vec(),
                    timestamp_micros: ts_micros,
                    value: new_version.to_string().into_bytes(),
                })),
            });
            mutations.push(Mutation {
                mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                    family_name: FAMILY_META.to_string(),
                    column_qualifier: QUALIFIER_VERSION.to_vec(),
                    time_range: older_than_ts_range(ts_micros),
                })),
            });
            let end_ts = if cur_ver.is_none() && max_d_ts > 0 {
                next_millis_exclusive(ts_micros.max(max_d_ts))
            } else {
                next_millis_exclusive(ts_micros)
            };
            for q in existing_quals {
                mutations.push(Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: FAMILY_DATA.to_string(),
                        column_qualifier: q,
                        time_range: older_than_ts_range(end_ts),
                    })),
                });
            }

            if self
                .apply_shadow_version_cas(
                    "CheckAndMutateRow delete_shadow_versioned",
                    shadow_row_key,
                    cur_ver,
                    mutations,
                )
                .await?
            {
                return Ok(true);
            }
        }
        Err(StorageError::Transient(
            "shadow index versioned delete CAS contention exhausted".to_string(),
        ))
    }

    /// Reconciler-safe cleanup of an orphaned shadow row: clears observed `d:*`
    /// qualifiers via millisecond-aligned bounded `DeleteFromColumn` only if the
    /// shadow row's `m:v` state has not changed since inspection.
    pub async fn delete_shadow_orphan_if_unchanged(
        &self,
        shadow_row_key: &[u8],
    ) -> Result<bool, StorageError> {
        let (existing_quals, max_d_ts, cur_ver) = self.read_shadow_row_meta(shadow_row_key).await?;
        if existing_quals.is_empty() {
            return Ok(false);
        }
        let next_ver = next_occ_version(cur_ver);
        let ts_micros = version_to_timestamp_micros(next_ver);
        let del_end = next_millis_exclusive(ts_micros.max(max_d_ts));

        let mut mutations = Vec::with_capacity(existing_quals.len() + 2);
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::SetCell(SetCell {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                timestamp_micros: ts_micros,
                value: next_ver.to_string().into_bytes(),
            })),
        });
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                family_name: FAMILY_META.to_string(),
                column_qualifier: QUALIFIER_VERSION.to_vec(),
                time_range: older_than_ts_range(ts_micros),
            })),
        });
        for q in existing_quals {
            mutations.push(Mutation {
                mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                    family_name: FAMILY_DATA.to_string(),
                    column_qualifier: q,
                    time_range: older_than_ts_range(del_end),
                })),
            });
        }

        self.apply_shadow_version_cas(
            "CheckAndMutateRow delete_shadow_orphan",
            shadow_row_key,
            cur_ver,
            mutations,
        )
        .await
    }

    /// Atomically reclaim a version-only tombstone row (base table or shadow
    /// index table) once the intent-timeout grace window has elapsed.
    ///
    /// Executes `DeleteFromRow` in `false_mutations` iff the row simultaneously
    /// has NO `m:intent:*` cells, NO `d:*` data cells, AND `m:v == expected_version`.
    /// If any live cell, active intent, or newer OCC version arrived in the
    /// meantime, `predicate_matched` is `true` and the row is left untouched.
    pub async fn reclaim_version_only_row(
        &self,
        row_key_bytes: &[u8],
        expected_version: u64,
    ) -> Result<bool, StorageError> {
        let req = CheckAndMutateRowRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: row_key_bytes.to_vec(),
            predicate_filter: Some(build_tombstone_reclaim_conflict_filter(expected_version)),
            true_mutations: vec![],
            false_mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            }],
            ..CheckAndMutateRowRequest::default()
        };
        let mut data = self.client.data();
        let resp = data
            .check_and_mutate_row_idempotent(req)
            .await
            .map_err(|e| map_bigtable_err("CheckAndMutateRow reclaim_version_only_row", e))?;
        Ok(!resp.predicate_matched)
    }

    /// Apply a list of mutations to a row by raw encoded key, splitting batches
    /// larger than `MAX_MUTATIONS_PER_RPC` across multiple `MutateRow` calls.
    pub async fn mutate_cells(
        &self,
        row_key_bytes: Vec<u8>,
        mutations: Vec<Mutation>,
    ) -> Result<(), StorageError> {
        if mutations.is_empty() {
            return Ok(());
        }
        let rpc_limit = max_mutations_per_rpc();
        let mut data = self.client.data();
        for chunk in mutations.chunks(rpc_limit) {
            let req = MutateRowRequest {
                table_name: self.full_table_name.clone(),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                row_key: row_key_bytes.clone(),
                mutations: chunk.to_vec(),
                ..MutateRowRequest::default()
            };
            data.mutate_row(req)
                .await
                .map_err(|e| map_bigtable_err("MutateRow mutate_cells", e))?;
        }
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
    .map_err(|e| StorageError::Validation(e.to_string()))?;
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
        Err(e) => Err(StorageError::Validation(e.to_string())),
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
        assert_eq!(next_millis_exclusive(ts1), ts1 + 1_000);
        assert_eq!(next_millis_exclusive(ts1) % 1_000, 0);
        assert_eq!(next_millis_exclusive(1_500), 2_000);
        assert_eq!(
            older_than_ts_range(next_millis_exclusive(5_000))
                .unwrap()
                .end_timestamp_micros,
            6_000
        );

        let v2 = next_occ_version(Some(v1));
        let v3 = next_occ_version(Some(v2));
        assert!(v2 > v1);
        assert!(v3 > v2);
        assert!(version_to_timestamp_micros(v2) >= next_millis_exclusive(ts1));
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
    fn test_filter_builders_have_no_wall_clock_intent_cutoff() {
        let intent_filter = build_active_intent_filter();
        if let Some(Filter::Chain(chain)) = intent_filter.filter {
            assert!(
                !chain
                    .filters
                    .iter()
                    .any(|f| matches!(f.filter, Some(Filter::TimestampRangeFilter(_)))),
                "intent filter must NOT drop older intents via wall-clock TimestampRangeFilter"
            );
        } else {
            panic!("expected Chain filter");
        }

        let occ_conflict_filter = build_occ_conflict_filter(42);
        if let Some(Filter::Interleave(interleave)) = occ_conflict_filter.filter {
            assert_eq!(interleave.filters.len(), 2);
        } else {
            panic!("expected Interleave filter");
        }

        let unversioned_existing = select_occ_predicate(None, true);
        if let Some(Filter::Interleave(interleave)) = unversioned_existing.filter {
            assert_eq!(interleave.filters.len(), 3);
        } else {
            panic!("expected 3-branch Interleave for unversioned existing row");
        }
    }

    #[test]
    fn shadow_put_50k_attributes_fits_within_single_check_and_mutate_row() {
        let mut item = Item::new();
        for i in 0..50_000usize {
            item.insert(
                format!("a{i}"),
                extenddb_core::types::AttributeValue::S("x".to_owned()),
            );
        }
        let existing_quals = vec![b"a0".to_vec(), b"removed_attr".to_vec()];
        let mutations =
            ItemOps::build_shadow_put_mutations(&item, existing_quals, 1_000, Some(1), 2)
                .expect("build_shadow_put_mutations");
        // 2 (`m:v` SetCell + DeleteFromColumn) + 50,000 retained `SetCell` + 1 removed `DeleteFromColumn`.
        assert_eq!(mutations.len(), 50_003);
        assert!(mutations.len() < MAX_MUTATIONS_PER_RPC);

        // Overwrite with 105,000 retained/new attributes + 60,000 removed attributes (165,002 mutations):
        // `split_mutation_batches(..., MAX_MUTATIONS_PER_RPC)` keeps every RPC batch <= 90,000 (< 100,000).
        let mut wide = Item::new();
        for i in 0..105_000usize {
            wide.insert(
                format!("a{i:06}"),
                extenddb_core::types::AttributeValue::Bool(true),
            );
        }
        let removed: Vec<Vec<u8>> = (0..60_000usize)
            .map(|i| format!("old{i:06}").into_bytes())
            .collect();
        let wide_muts =
            ItemOps::build_shadow_put_mutations(&wide, removed, 1_000, Some(1), 2).unwrap();
        assert_eq!(wide_muts.len(), 165_002);
        let (cas_batch, overflow_chunks) = split_mutation_batches(wide_muts, MAX_MUTATIONS_PER_RPC);
        assert_eq!(cas_batch.len(), MAX_MUTATIONS_PER_RPC);
        assert_eq!(overflow_chunks.len(), 1);
        assert_eq!(overflow_chunks[0].len(), 75_002);
    }
}
