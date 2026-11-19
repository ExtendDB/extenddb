// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Coordinator-row 2PC for `TransactWriteItems` and fenced single-item stream writes.
//!
//! State machine (per coordinator entry):
//!   PENDING   — coordinator row exists; intents are being placed.
//!   COMMITTED — every condition passed and every intent landed; commit point.
//!   CLEANED   — every mutation and stream record has been applied and intents cleared.
//!
//! Per-row intent markers live in column family `m` under qualifier
//! `intent:<txn_id>`. When a writer wants to fence a row, it places its intent
//! via `CheckAndMutateRow` with a predicate that succeeds only when no other
//! unexpired intent exists — guaranteeing at most one in-flight coordinator
//! entry per row at a time.

use std::time::Duration;

use extenddb_core::types::{CancellationReason, Item};
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
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::data::client::{
    BigtableClient, exact_re2_bytes, map_bigtable_err, monotonic_timestamp_after_micros,
    monotonic_timestamp_micros,
};
use crate::data::item_ops::{
    FAMILY_DATA, QUALIFIER_VERSION, build_version_match_filter, now_millis_micros,
    older_than_ts_range, version_to_timestamp_micros,
};

pub const TXN_LOG_TABLE: &str = "__extenddb_txn_log__";
pub const TXN_FAMILY: &str = "t";
pub const INTENT_FAMILY: &str = "m";
pub const INTENT_QUALIFIER_PREFIX: &str = "intent:";

const STATE_PENDING: &[u8] = b"PENDING";
const STATE_COMMITTED: &[u8] = b"COMMITTED";
const STATE_CLEANED: &[u8] = b"CLEANED";
const STATE_ABORTED: &[u8] = b"ABORTED";

/// Target row for a 2PC participant or GSI shadow mutation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParticipantRow {
    pub data_table: String, // BigTable table id
    pub row_key: Vec<u8>,   // encoded composite key
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum TxnOpPayload {
    Put { item: Item },
    Delete,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ParticipantMutation {
    pub participant: ParticipantRow,
    pub payload: TxnOpPayload,
    #[serde(default)]
    pub expected_version: Option<u64>,
    #[serde(default)]
    pub row_existed: bool,
    #[serde(default)]
    pub new_version: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TxnStreamRecord {
    pub stream_arn: String,
    #[serde(default)]
    pub shard_id: String,
    pub record: extenddb_core::types::StreamRecord,
}

#[derive(Debug)]
pub struct TxnState {
    pub state: String,
    pub state_ts_micros: i64,
    pub started_at: Option<i64>,
    pub participants: Option<Vec<ParticipantRow>>,
    pub mutations: Option<Vec<ParticipantMutation>>,
    pub stream_records: Option<Vec<TxnStreamRecord>>,
}

#[derive(Clone, Copy)]
pub struct TxnCoordinator<'a> {
    client: &'a BigtableClient,
    intent_max_age: Duration,
}

impl<'a> TxnCoordinator<'a> {
    pub fn new(client: &'a BigtableClient, intent_max_age: Duration) -> Self {
        Self {
            client,
            intent_max_age,
        }
    }

    /// New txn id — `txn-<uuid-no-dashes>`.
    pub fn new_txn_id() -> String {
        format!("txn-{}", Uuid::new_v4().simple())
    }

    pub fn coordinator_row_key(txn_id: &str) -> String {
        format!("txn:{txn_id}")
    }

    fn intent_qualifier(txn_id: &str) -> Vec<u8> {
        let mut q = INTENT_QUALIFIER_PREFIX.as_bytes().to_vec();
        q.extend_from_slice(txn_id.as_bytes());
        q
    }

    /// Write the coordinator row in `PENDING` state if it doesn't exist.
    /// Returns `Ok((created, started_at_micros))`.
    pub async fn open(
        &self,
        txn_id: &str,
        participants: &[ParticipantRow],
    ) -> Result<(bool, i64), StorageError> {
        let mut data = self.client.data();
        let now_micros = monotonic_timestamp_micros();
        let serialized = serde_json::to_vec(participants)
            .map_err(|e| StorageError::Internal(format!("serialize participants: {e}")))?;

        let predicate = RowFilter {
            filter: Some(Filter::FamilyNameRegexFilter(format!("^{TXN_FAMILY}$"))),
        };

        let req = CheckAndMutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            predicate_filter: Some(predicate),
            true_mutations: vec![],
            false_mutations: vec![
                Self::set_cell_at(TXN_FAMILY, b"state", STATE_PENDING.to_vec(), now_micros),
                Self::set_cell_at(
                    TXN_FAMILY,
                    b"started_at",
                    now_micros.to_string().into_bytes(),
                    now_micros,
                ),
                Self::set_cell_at(TXN_FAMILY, b"participants", serialized, now_micros),
            ],
            ..CheckAndMutateRowRequest::default()
        };

        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("txn open", e))?;

        Ok((!resp.predicate_matched, now_micros))
    }

    /// Read the latest coordinator state from `__extenddb_txn_log__`.
    ///
    /// Chains `FamilyNameRegexFilter("^t$")` with `CellsPerColumnLimitFilter(1)`
    /// because Bigtable's `MaxNumVersions(1)` GC rule is applied lazily during
    /// compaction; without `CellsPerColumnLimitFilter(1)` a `ReadRows` response
    /// returns `[COMMITTED, PENDING]` in descending timestamp order.
    pub async fn get_state(&self, txn_id: &str) -> Result<Option<TxnState>, StorageError> {
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![Self::coordinator_row_key(txn_id).into_bytes()],
                row_ranges: vec![],
            }),
            filter: Some(build_txn_log_latest_filter()),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| map_bigtable_err("ReadRows get_state", e))?;
        let mut iter = resp.into_iter();
        let Some((_, cells)) = iter.next() else {
            return Ok(None);
        };

        let mut state: Option<String> = None;
        let mut state_ts_micros: i64 = 0;
        let mut started_at: Option<i64> = None;
        let mut participants: Option<Vec<ParticipantRow>> = None;
        let mut mutations: Option<Vec<ParticipantMutation>> = None;
        let mut stream_records: Option<Vec<TxnStreamRecord>> = None;

        for cell in cells {
            if cell.family_name == TXN_FAMILY {
                match cell.qualifier.as_slice() {
                    b"state" if state.is_none() => {
                        state_ts_micros = cell.timestamp_micros;
                        state = String::from_utf8(cell.value).ok();
                    }
                    b"started_at" if started_at.is_none() => {
                        started_at = String::from_utf8(cell.value)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok());
                    }
                    b"participants" if participants.is_none() => {
                        participants = serde_json::from_slice(&cell.value).ok();
                    }
                    b"mutations" if mutations.is_none() => {
                        mutations = serde_json::from_slice(&cell.value).ok();
                    }
                    b"stream_records" if stream_records.is_none() => {
                        stream_records = serde_json::from_slice(&cell.value).ok();
                    }
                    _ => {}
                }
            }
        }

        let Some(st) = state else {
            return Err(StorageError::Internal("missing state in txn log".into()));
        };

        Ok(Some(TxnState {
            state: st,
            state_ts_micros,
            started_at,
            participants,
            mutations,
            stream_records,
        }))
    }

    /// Place an intent marker on a participant row. Returns `Ok(true)` if the
    /// intent landed; `Ok(false)` if an unexpired conflicting intent holds the row.
    pub async fn place_intent(
        &self,
        txn_id: &str,
        participant: &ParticipantRow,
    ) -> Result<bool, StorageError> {
        let mut attempts = 0;
        let max_attempts = 5;
        let mut delay = Duration::from_millis(10);

        loop {
            let mut data = self.client.data();
            let qualifier = Self::intent_qualifier(txn_id);
            let write_ts = monotonic_timestamp_micros();
            let cutoff = now_millis_micros().saturating_sub(self.intent_max_age.as_micros() as i64);
            let predicate = build_intent_presence_filter(cutoff);

            let req = CheckAndMutateRowRequest {
                table_name: self.client.full_table_name(&participant.data_table),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                row_key: participant.row_key.clone(),
                predicate_filter: Some(predicate),
                true_mutations: vec![],
                false_mutations: vec![Self::set_cell_at(
                    INTENT_FAMILY,
                    &qualifier,
                    txn_id.as_bytes().to_vec(),
                    write_ts,
                )],
                ..CheckAndMutateRowRequest::default()
            };
            let resp = data
                .check_and_mutate_row(req)
                .await
                .map_err(|e| map_bigtable_err("place_intent", e))?;

            if !resp.predicate_matched {
                return Ok(true);
            }

            attempts += 1;
            if attempts >= max_attempts {
                return Ok(false);
            }

            let jitter = rand::random_range(0..delay.as_millis() as u64);
            let sleep_duration = delay + Duration::from_millis(jitter);
            tokio::time::sleep(sleep_duration).await;
            delay *= 2;
        }
    }

    /// Move the coordinator row to `COMMITTED` — the atomic commit point.
    /// Guarantees `commit_ts > started_at_micros` across any node clock skew and
    /// returns `commit_ts` so subsequent `CLEANED` transitions strictly dominate it.
    pub async fn commit(
        &self,
        txn_id: &str,
        started_at_micros: i64,
        mutations: &[ParticipantMutation],
        stream_records: Option<&[TxnStreamRecord]>,
    ) -> Result<i64, StorageError> {
        let ts_micros = monotonic_timestamp_after_micros(started_at_micros);
        let mut txn_mutations = vec![
            Mutation {
                mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                    family_name: TXN_FAMILY.to_string(),
                    column_qualifier: b"state".to_vec(),
                    time_range: older_than_ts_range(ts_micros),
                })),
            },
            Self::set_cell_at(TXN_FAMILY, b"state", STATE_COMMITTED.to_vec(), ts_micros),
        ];

        let serialized_muts = serde_json::to_vec(mutations)
            .map_err(|e| StorageError::Internal(format!("serialize mutations: {e}")))?;
        txn_mutations.push(Self::set_cell_at(
            TXN_FAMILY,
            b"mutations",
            serialized_muts,
            ts_micros,
        ));

        if let Some(records) = stream_records {
            let serialized = serde_json::to_vec(records)
                .map_err(|e| StorageError::Internal(format!("serialize stream records: {e}")))?;
            txn_mutations.push(Self::set_cell_at(
                TXN_FAMILY,
                b"stream_records",
                serialized,
                ts_micros,
            ));
        }
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            mutations: txn_mutations,
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("txn commit", e))?;
        Ok(ts_micros)
    }

    /// Mark the coordinator `CLEANED` at a timestamp strictly greater than
    /// `after_ts_micros` after all participant mutations and stream records applied.
    pub async fn cleaned(&self, txn_id: &str, after_ts_micros: i64) -> Result<(), StorageError> {
        self.set_state_after(txn_id, STATE_CLEANED, after_ts_micros)
            .await
    }

    /// Mark the coordinator `ABORTED` at a timestamp strictly greater than
    /// `after_ts_micros` after rollback failed to clear all intents.
    pub async fn aborted(&self, txn_id: &str, after_ts_micros: i64) -> Result<(), StorageError> {
        self.set_state_after(txn_id, STATE_ABORTED, after_ts_micros)
            .await
    }

    /// Apply participant mutation in Phase 2 fenced with predicate asserting BOTH:
    /// 1. Coordinator's own intent `m:intent:<txn_id>` is still present, AND
    /// 2. Row's OCC state still matches `(expected_version, row_existed)`.
    ///
    /// Stamps every `SetCell` (`d:*` and `m:v`) with the deterministic cell
    /// timestamp `version_to_timestamp_micros(new_version)` and keeps
    /// `m:intent:<txn_id>` in place until dependent GSI shadow, TTL index, and
    /// stream writes complete before `clear_intent` releases the row.
    /// Checking `(expected_version, row_existed)` also guarantees that a delayed
    /// `roll_forward` after lease expiry is a clean no-op both when Phase 2
    /// already applied `new_version` and when a newer writer advanced `m:v`.
    pub async fn apply_fenced_mutation(
        &self,
        txn_id: &str,
        participant: &ParticipantRow,
        payload: &TxnOpPayload,
        expected_version: Option<u64>,
        row_existed: bool,
        new_version: u64,
    ) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let ts_micros = version_to_timestamp_micros(new_version);
        let predicate = build_fenced_apply_predicate(txn_id, expected_version, row_existed);

        let mut true_mutations = Vec::new();
        match payload {
            TxnOpPayload::Put { item } => {
                true_mutations.push(Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromFamily(DeleteFromFamily {
                        family_name: FAMILY_DATA.to_string(),
                    })),
                });
                for (name, value) in item {
                    true_mutations.push(Mutation {
                        mutation: Some(mutation::Mutation::SetCell(SetCell {
                            family_name: FAMILY_DATA.to_string(),
                            column_qualifier: name.as_bytes().to_vec(),
                            timestamp_micros: ts_micros,
                            value: crate::data::encoding::cell::encode(value)?,
                        })),
                    });
                }
                true_mutations.push(Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: INTENT_FAMILY.to_string(),
                        column_qualifier: QUALIFIER_VERSION.to_vec(),
                        time_range: older_than_ts_range(ts_micros),
                    })),
                });
                true_mutations.push(Mutation {
                    mutation: Some(mutation::Mutation::SetCell(SetCell {
                        family_name: INTENT_FAMILY.to_string(),
                        column_qualifier: QUALIFIER_VERSION.to_vec(),
                        timestamp_micros: ts_micros,
                        value: new_version.to_string().into_bytes(),
                    })),
                });
            }
            TxnOpPayload::Delete => {
                true_mutations.push(Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromFamily(DeleteFromFamily {
                        family_name: FAMILY_DATA.to_string(),
                    })),
                });
                true_mutations.push(Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: INTENT_FAMILY.to_string(),
                        column_qualifier: QUALIFIER_VERSION.to_vec(),
                        time_range: older_than_ts_range(ts_micros),
                    })),
                });
                true_mutations.push(Mutation {
                    mutation: Some(mutation::Mutation::SetCell(SetCell {
                        family_name: INTENT_FAMILY.to_string(),
                        column_qualifier: QUALIFIER_VERSION.to_vec(),
                        timestamp_micros: ts_micros,
                        value: new_version.to_string().into_bytes(),
                    })),
                });
            }
        }

        let req = CheckAndMutateRowRequest {
            table_name: self.client.full_table_name(&participant.data_table),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: participant.row_key.clone(),
            predicate_filter: Some(predicate),
            true_mutations,
            false_mutations: vec![],
            ..CheckAndMutateRowRequest::default()
        };

        let resp = data
            .check_and_mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("apply_fenced_mutation", e))?;

        Ok(resp.predicate_matched)
    }

    /// Clear an intent cell.
    pub async fn clear_intent(
        &self,
        txn_id: &str,
        participant: &ParticipantRow,
    ) -> Result<(), StorageError> {
        let qualifier = Self::intent_qualifier(txn_id);
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.client.full_table_name(&participant.data_table),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: participant.row_key.clone(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                    family_name: INTENT_FAMILY.to_string(),
                    column_qualifier: qualifier,
                    time_range: None,
                })),
            }],
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("clear_intent", e))?;
        Ok(())
    }

    /// Delete the coordinator row entirely (post-clean GC).
    pub async fn drop(&self, txn_id: &str) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            }],
            ..MutateRowRequest::default()
        };
        let _ = data.mutate_row(req).await;
        Ok(())
    }

    async fn set_state_after(
        &self,
        txn_id: &str,
        state: &[u8],
        after_ts_micros: i64,
    ) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let ts_micros = monotonic_timestamp_after_micros(after_ts_micros);
        let req = MutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            mutations: vec![
                Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: TXN_FAMILY.to_string(),
                        column_qualifier: b"state".to_vec(),
                        time_range: older_than_ts_range(ts_micros),
                    })),
                },
                Self::set_cell_at(TXN_FAMILY, b"state", state.to_vec(), ts_micros),
            ],
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("set_state", e))?;
        Ok(())
    }

    fn set_cell_at(
        family: &str,
        qualifier: &[u8],
        value: Vec<u8>,
        timestamp_micros: i64,
    ) -> Mutation {
        debug_assert!(
            timestamp_micros > 0 && timestamp_micros % 1_000 == 0,
            "Bigtable cell timestamp must be positive and millisecond-aligned, got {timestamp_micros}"
        );
        Mutation {
            mutation: Some(mutation::Mutation::SetCell(SetCell {
                family_name: family.to_string(),
                column_qualifier: qualifier.to_vec(),
                timestamp_micros,
                value,
            })),
        }
    }
}

/// Predicate matching iff `m:intent:<txn_id>` is present AND the row's OCC
/// state matches `(expected_version, row_existed)`:
/// - `(Some(v), _)`  -> `m:v == v`
/// - `(None, false)` -> neither `m:v` nor `d:*` exists
/// - `(None, true)`  -> `d:*` exists and `m:v` does not exist (legacy unversioned row)
fn build_fenced_apply_predicate(
    txn_id: &str,
    expected_version: Option<u64>,
    row_existed: bool,
) -> RowFilter {
    let qualifier = TxnCoordinator::intent_qualifier(txn_id);
    let qualifier_regex = exact_re2_bytes(&qualifier);
    let intent_present = RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{INTENT_FAMILY}$"))),
                },
                RowFilter {
                    filter: Some(Filter::ColumnQualifierRegexFilter(qualifier_regex)),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
            ],
        })),
    };

    let has_v = RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{INTENT_FAMILY}$"))),
                },
                RowFilter {
                    filter: Some(Filter::ColumnQualifierRegexFilter(b"^v$".to_vec())),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
            ],
        })),
    };
    let has_d = RowFilter {
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
    };

    let state_guard = match (expected_version, row_existed) {
        (Some(v), _) => build_version_match_filter(v),
        (None, false) => {
            let has_v_or_d = RowFilter {
                filter: Some(Filter::Interleave(Interleave {
                    filters: vec![has_v, has_d],
                })),
            };
            RowFilter {
                filter: Some(Filter::Condition(Box::new(Condition {
                    predicate_filter: Some(Box::new(has_v_or_d)),
                    true_filter: Some(Box::new(RowFilter {
                        filter: Some(Filter::BlockAllFilter(true)),
                    })),
                    false_filter: Some(Box::new(RowFilter {
                        filter: Some(Filter::PassAllFilter(true)),
                    })),
                }))),
            }
        }
        (None, true) => RowFilter {
            filter: Some(Filter::Condition(Box::new(Condition {
                predicate_filter: Some(Box::new(has_d)),
                true_filter: Some(Box::new(RowFilter {
                    filter: Some(Filter::Condition(Box::new(Condition {
                        predicate_filter: Some(Box::new(has_v)),
                        true_filter: Some(Box::new(RowFilter {
                            filter: Some(Filter::BlockAllFilter(true)),
                        })),
                        false_filter: Some(Box::new(RowFilter {
                            filter: Some(Filter::PassAllFilter(true)),
                        })),
                    }))),
                })),
                false_filter: Some(Box::new(RowFilter {
                    filter: Some(Filter::BlockAllFilter(true)),
                })),
            }))),
        },
    };

    RowFilter {
        filter: Some(Filter::Condition(Box::new(Condition {
            predicate_filter: Some(Box::new(intent_present)),
            true_filter: Some(Box::new(state_guard)),
            false_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::BlockAllFilter(true)),
            })),
        }))),
    }
}

/// Filter reading only the newest cell per qualifier in family `t` of `__extenddb_txn_log__`.
pub fn build_txn_log_latest_filter() -> RowFilter {
    RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{TXN_FAMILY}$"))),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
            ],
        })),
    }
}

/// Filter that matches any cell in family `m` with qualifier prefix `intent:`
/// and timestamp >= cutoff micros.
fn build_intent_presence_filter(cutoff_micros: i64) -> RowFilter {
    RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{INTENT_FAMILY}$"))),
                },
                RowFilter {
                    filter: Some(Filter::ColumnQualifierRegexFilter(
                        format!("^{INTENT_QUALIFIER_PREFIX}.*$").into_bytes(),
                    )),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
                RowFilter {
                    filter: Some(Filter::TimestampRangeFilter(TimestampRange {
                        start_timestamp_micros: cutoff_micros.max(0),
                        end_timestamp_micros: 0, // 0 = unbounded
                    })),
                },
            ],
        })),
    }
}

/// Cancellation reason convenience builder.
pub fn cancellation_reason(
    code: &str,
    message: Option<&str>,
    item: Option<Item>,
) -> CancellationReason {
    CancellationReason {
        code: code.to_string(),
        message: message.map(str::to_owned),
        item,
    }
}

/// Make sure the `__extenddb_txn_log__` admin table exists on the BigTable
/// instance. Called once at startup from `runtime_hooks` (idempotent).
pub async fn ensure_txn_log_table(client: &BigtableClient) -> Result<(), String> {
    let mut admin = crate::data::admin::AdminClient::connect(client).await?;
    admin
        .create_table(
            TXN_LOG_TABLE,
            &[(TXN_FAMILY, Some(crate::data::admin::gc_max_versions(1)))],
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_id_format() {
        let id = TxnCoordinator::new_txn_id();
        assert!(id.starts_with("txn-"));
        assert_eq!(id.len(), 4 + 32); // txn- + 32 hex chars
    }

    #[test]
    fn test_intent_qualifier_and_row_key() {
        let id = "txn-12345";
        let qual = TxnCoordinator::intent_qualifier(id);
        assert_eq!(qual, b"intent:txn-12345");
        let rkey = TxnCoordinator::coordinator_row_key(id);
        assert_eq!(rkey, "txn:txn-12345");
    }

    #[test]
    fn test_participant_mutation_serde_with_version_and_legacy_defaults() {
        let pm = ParticipantMutation {
            participant: ParticipantRow {
                data_table: "t123".to_string(),
                row_key: vec![1, 2, 3],
            },
            payload: TxnOpPayload::Delete,
            expected_version: Some(41),
            row_existed: true,
            new_version: Some(99),
        };
        let json_str = serde_json::to_string(&pm).unwrap();
        assert!(json_str.contains("expected_version"));
        assert!(json_str.contains("new_version"));

        let deserialized: ParticipantMutation = serde_json::from_str(&json_str).unwrap();
        assert_eq!(deserialized.expected_version, Some(41));
        assert!(deserialized.row_existed);
        assert_eq!(deserialized.new_version, Some(99));

        let legacy_json =
            r#"{"participant":{"data_table":"t123","row_key":[1,2,3]},"payload":"Delete"}"#;
        let legacy_deserialized: ParticipantMutation = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(legacy_deserialized.expected_version, None);
        assert!(!legacy_deserialized.row_existed);
        assert_eq!(legacy_deserialized.new_version, None);
    }

    #[test]
    fn test_build_fenced_apply_predicate_uses_outer_condition() {
        for (expected, existed) in [(Some(7), true), (None, false), (None, true)] {
            let pred = build_fenced_apply_predicate("txn-abc.123", expected, existed);
            if let Some(Filter::Condition(cond)) = pred.filter {
                assert!(cond.predicate_filter.is_some());
                assert!(cond.true_filter.is_some());
                assert!(matches!(
                    cond.false_filter.as_ref().and_then(|f| f.filter.as_ref()),
                    Some(Filter::BlockAllFilter(true))
                ));
            } else {
                panic!("expected outer Filter::Condition so true_filter inspects full row");
            }
        }
    }

    #[test]
    fn test_txn_log_latest_filter_has_cells_limit() {
        let filter = build_txn_log_latest_filter();
        if let Some(Filter::Chain(chain)) = filter.filter {
            assert_eq!(chain.filters.len(), 2);
            assert!(matches!(
                chain.filters[1].filter,
                Some(Filter::CellsPerColumnLimitFilter(1))
            ));
        } else {
            panic!("expected Filter::Chain");
        }
    }

    #[test]
    fn test_build_intent_presence_filter_order() {
        let filter = build_intent_presence_filter(1000);
        if let Some(Filter::Chain(chain)) = filter.filter {
            assert_eq!(chain.filters.len(), 4);
            if let Some(Filter::FamilyNameRegexFilter(fam)) = &chain.filters[0].filter {
                assert_eq!(fam, "^m$");
            } else {
                panic!("expected FamilyNameRegexFilter");
            }
            assert!(matches!(
                chain.filters[1].filter,
                Some(Filter::ColumnQualifierRegexFilter(_))
            ));
            assert!(matches!(
                chain.filters[2].filter,
                Some(Filter::CellsPerColumnLimitFilter(1))
            ));
            assert!(matches!(
                chain.filters[3].filter,
                Some(Filter::TimestampRangeFilter(_))
            ));
        } else {
            panic!("expected Filter::Chain");
        }
    }
}
