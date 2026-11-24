// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Coordinator-row 2PC for `TransactWriteItems` and fenced single-item stream writes.
//!
//! State machine (per coordinator entry in `__extenddb_txn_log__`):
//!   PENDING   — coordinator row exists (`t:started_at`); intents are being placed.
//!   ABORTED   — coordinator was CAS-aborted (`PENDING -> ABORTED`) before intent cleanup.
//!   COMMITTED — every condition passed and `PENDING -> COMMITTED` CAS succeeded; commit point.
//!   CLEANED   — every mutation, GSI shadow, TTL delta, stream record, and `idem:` entry applied.

use std::time::Duration;

use extenddb_core::types::Item;
use extenddb_storage::error::StorageError;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::{
    DeleteFromColumn, DeleteFromFamily, DeleteFromRow, SetCell,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::{
    Chain, Condition, Filter, Interleave,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    CheckAndMutateRowRequest, MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowSet,
    mutation,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::data::client::{
    BigtableClient, exact_re2_bytes, is_table_not_found_err, map_bigtable_err,
    monotonic_timestamp_after_micros, monotonic_timestamp_micros,
};
use crate::data::item_ops::{
    FAMILY_DATA, QUALIFIER_VERSION, build_version_match_filter, max_mutations_per_rpc,
    next_millis_exclusive, older_than_ts_range, version_to_timestamp_micros,
};

pub const TXN_LOG_TABLE: &str = "__extenddb_txn_log__";
pub const TXN_FAMILY: &str = "t";
pub const INTENT_FAMILY: &str = "m";
pub const INTENT_QUALIFIER_PREFIX: &str = "intent:";

/// Coordinator ID prefix identifying single-item stream-fenced writes
/// so concurrent single-item writers serialize/retry rather than raising
/// `TransactionConflictException`.
pub const SINGLE_TXN_PREFIX: &str = "sgl-";

const STATE_PENDING: &[u8] = b"PENDING";
const STATE_COMMITTED: &[u8] = b"COMMITTED";
const STATE_CLEANED: &[u8] = b"CLEANED";
const STATE_ABORTED: &[u8] = b"ABORTED";

/// Target row for a 2PC participant or GSI shadow mutation.
/// Derives canonical `(data_table, row_key)` ordering to prevent 2PC lock-order
/// deadlocks.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ParticipantRow {
    pub data_table: String,
    pub row_key: Vec<u8>,
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
    /// Base-table participant that generated this stream event, so
    /// `roll_forward` suppresses phantom stream records when a stale coordinator
    /// was superseded by a newer write.
    #[serde(default)]
    pub participant: Option<ParticipantRow>,
    /// Optional explicit deduplication key (`<txn_id>:<op_idx>`) supplementing
    /// `record.event_id` across crash recovery replays.
    #[serde(default)]
    pub dedup_key: Option<String>,
}

/// Logged `__extenddb_ttl_index__` transition persisted at `COMMITTED`
/// so crash recovery (`roll_forward`) maintains TTL index completeness.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TxnTtlMutation {
    pub participant: ParticipantRow,
    pub account_id: String,
    pub table_name: String,
    #[serde(default)]
    pub table_id: Option<String>,
    #[serde(default)]
    pub old_expiry: Option<i64>,
    #[serde(default)]
    pub new_expiry: Option<i64>,
    pub new_version: u64,
}

/// `ClientRequestToken` metadata persisted in `t:token_meta` from `open_with_token`
/// onward so concurrent callers and `sweeper` enforce fingerprint
/// validation and persist `idem:<account_id>:<token>` before dropping `txn:<id>`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TxnTokenMeta {
    pub account_id: String,
    pub token: String,
    pub fingerprint: String,
}

/// Base-row version fence recorded on `post-<uuid>` side-effect recovery
/// entries (`t:base_guard`) so `roll_forward` skips stale shadow `Put` and TTL
/// additions when the base row has already moved past `base_version` (while
/// still executing `Delete` cleanups for multi-hop index updates).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SideEffectBaseGuard {
    pub participant: ParticipantRow,
    pub base_version: u64,
}

#[derive(Debug, Clone)]
pub struct TxnState {
    pub state: String,
    pub state_ts_micros: i64,
    pub started_at: Option<i64>,
    pub open_nonce: Option<String>,
    pub participants: Option<Vec<ParticipantRow>>,
    pub mutations: Option<Vec<ParticipantMutation>>,
    pub stream_records: Option<Vec<TxnStreamRecord>>,
    pub ttl_mutations: Option<Vec<TxnTtlMutation>>,
    pub token_meta: Option<TxnTokenMeta>,
    pub base_guard: Option<SideEffectBaseGuard>,
}

/// Decode a coordinator row's `t:*` cells into [`TxnState`], returning `None`
/// if `t:state` is absent (for example during concurrent row deletion).
#[must_use]
pub(crate) fn decode_txn_state_cells(cells: Vec<crate::data::client::RowCell>) -> Option<TxnState> {
    let mut state: Option<String> = None;
    let mut state_ts_micros: i64 = 0;
    let mut started_at: Option<i64> = None;
    let mut open_nonce: Option<String> = None;
    let mut participants: Option<Vec<ParticipantRow>> = None;
    let mut mutations: Option<Vec<ParticipantMutation>> = None;
    let mut stream_records: Option<Vec<TxnStreamRecord>> = None;
    let mut ttl_mutations: Option<Vec<TxnTtlMutation>> = None;
    let mut token_meta: Option<TxnTokenMeta> = None;
    let mut base_guard: Option<SideEffectBaseGuard> = None;

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
                b"open_nonce" if open_nonce.is_none() => {
                    open_nonce = String::from_utf8(cell.value).ok();
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
                b"ttl_mutations" if ttl_mutations.is_none() => {
                    ttl_mutations = serde_json::from_slice(&cell.value).ok();
                }
                b"token_meta" if token_meta.is_none() => {
                    token_meta = serde_json::from_slice(&cell.value).ok();
                }
                b"base_guard" if base_guard.is_none() => {
                    base_guard = serde_json::from_slice(&cell.value).ok();
                }
                _ => {}
            }
        }
    }

    let st = state?;
    Some(TxnState {
        state: st,
        state_ts_micros,
        started_at,
        open_nonce,
        participants,
        mutations,
        stream_records,
        ttl_mutations,
        token_meta,
        base_guard,
    })
}

/// Returns `true` when a retried `open_with_token` call observes the exact
/// `PENDING` incarnation written by its own earlier attempt (`started_at` and
/// per-call `open_nonce` both match).
#[must_use]
pub(crate) fn is_own_retried_open(
    st: &TxnState,
    expected_started_at: i64,
    expected_nonce: &str,
) -> bool {
    st.state == "PENDING"
        && st.started_at == Some(expected_started_at)
        && st.open_nonce.as_deref() == Some(expected_nonce)
}

#[derive(Clone, Copy)]
pub struct TxnCoordinator<'a> {
    client: &'a BigtableClient,
    intent_max_age: Duration,
}

impl<'a> TxnCoordinator<'a> {
    #[must_use]
    pub fn new(client: &'a BigtableClient, intent_max_age: Duration) -> Self {
        Self {
            client,
            intent_max_age,
        }
    }

    #[must_use]
    pub fn intent_max_age(&self) -> Duration {
        self.intent_max_age
    }

    /// New multi-item 2PC txn id — `txn-<uuid-no-dashes>`.
    #[must_use]
    pub fn new_txn_id() -> String {
        format!("txn-{}", Uuid::new_v4().simple())
    }

    /// New single-item stream-fenced txn id — `sgl-<uuid-no-dashes>`.
    #[must_use]
    pub fn new_single_txn_id() -> String {
        format!("{SINGLE_TXN_PREFIX}{}", Uuid::new_v4().simple())
    }

    #[must_use]
    pub fn is_single_item_txn(txn_id: &str) -> bool {
        txn_id.starts_with(SINGLE_TXN_PREFIX)
    }

    #[must_use]
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
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if serialization or the `CheckAndMutateRow` RPC fails.
    pub async fn open(
        &self,
        txn_id: &str,
        participants: &[ParticipantRow],
    ) -> Result<(bool, i64), StorageError> {
        self.open_with_token(txn_id, participants, None).await
    }

    /// Atomically open a `PENDING` coordinator row with optional `TxnTokenMeta`.
    ///
    /// Stamps a per-call `t:open_nonce` cell alongside `t:started_at` so if a
    /// transport retry (`attempt > 0`) runs after Bigtable already applied
    /// Attempt 0's `false_mutations`, the caller recognizes its own `PENDING`
    /// incarnation instead of colliding with itself for `intent_timeout_secs`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if serialization or the `CheckAndMutateRow` RPC fails.
    pub async fn open_with_token(
        &self,
        txn_id: &str,
        participants: &[ParticipantRow],
        token_meta: Option<&TxnTokenMeta>,
    ) -> Result<(bool, i64), StorageError> {
        let mut data = self.client.data();
        let now_micros = monotonic_timestamp_micros();
        let open_nonce = Uuid::new_v4().simple().to_string();
        let serialized = serde_json::to_vec(participants)
            .map_err(|e| StorageError::Internal(format!("serialize participants: {e}")))?;

        let predicate = RowFilter {
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
        };

        let mut false_mutations = vec![
            Self::set_cell_at(TXN_FAMILY, b"state", STATE_PENDING.to_vec(), now_micros),
            Self::set_cell_at(
                TXN_FAMILY,
                b"started_at",
                now_micros.to_string().into_bytes(),
                now_micros,
            ),
            Self::set_cell_at(
                TXN_FAMILY,
                b"open_nonce",
                open_nonce.as_bytes().to_vec(),
                now_micros,
            ),
            Self::set_cell_at(TXN_FAMILY, b"participants", serialized, now_micros),
        ];
        if let Some(meta) = token_meta {
            let meta_bytes = serde_json::to_vec(meta)
                .map_err(|e| StorageError::Internal(format!("serialize token_meta: {e}")))?;
            false_mutations.push(Self::set_cell_at(
                TXN_FAMILY,
                b"token_meta",
                meta_bytes,
                now_micros,
            ));
        }

        let req = CheckAndMutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            predicate_filter: Some(predicate),
            true_mutations: vec![],
            false_mutations,
            ..CheckAndMutateRowRequest::default()
        };

        match data.check_and_mutate_row_idempotent_with_attempt(req).await {
            Ok((resp, attempt)) => {
                if !resp.predicate_matched {
                    return Ok((true, now_micros));
                }
                if attempt > 0
                    && let Some(st) = self.get_state(txn_id).await?
                    && is_own_retried_open(&st, now_micros, &open_nonce)
                {
                    return Ok((true, now_micros));
                }
                Ok((false, now_micros))
            }
            Err(e) => {
                let mapped = map_bigtable_err("txn open", e);
                if let Ok(Some(st)) = self.get_state(txn_id).await
                    && is_own_retried_open(&st, now_micros, &open_nonce)
                {
                    let _ = self.drop_matching_start(txn_id, now_micros).await;
                }
                Err(mapped)
            }
        }
    }

    /// Read the latest coordinator state from `__extenddb_txn_log__`.
    ///
    /// Never emits `DeleteFromColumn` on `t:state` during transitions,
    /// retries transient gRPC glitches, and treats a row lacking `t:state` as
    /// `Ok(None)` so concurrent cleanup never triggers `StorageError::Internal` HTTP 500s.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if reading `__extenddb_txn_log__` fails after retries.
    pub async fn get_state(&self, txn_id: &str) -> Result<Option<TxnState>, StorageError> {
        let mut last_err: Option<StorageError> = None;
        let mut resp_opt = None;
        for attempt in 0..4u8 {
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
            match data
                .read_rows(req)
                .await
                .map_err(|e| map_bigtable_err("ReadRows get_state", e))
            {
                Ok(r) => {
                    resp_opt = Some(r);
                    break;
                }
                Err(e @ (StorageError::Transient(_) | StorageError::Connection(_)))
                    if attempt < 3 =>
                {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(5 * u64::from(attempt + 1))).await;
                }
                Err(e) => return Err(e),
            }
        }
        let Some(resp) = resp_opt else {
            return Err(last_err.unwrap_or_else(|| {
                StorageError::Transient("ReadRows get_state retry exhausted".to_owned())
            }));
        };
        let Some((_, cells)) = resp.into_iter().next() else {
            return Ok(None);
        };
        Ok(decode_txn_state_cells(cells))
    }

    /// Single-RPC attempt to place `m:intent:<txn_id>` if and only if NO
    /// physical `m:intent:*` cell currently exists on `participant`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `CheckAndMutateRow` or verification RPC fails.
    pub async fn place_intent_once(
        &self,
        txn_id: &str,
        participant: &ParticipantRow,
    ) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let qualifier = Self::intent_qualifier(txn_id);
        let write_ts = monotonic_timestamp_micros();
        let predicate = build_intent_presence_filter();

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
            .check_and_mutate_row_idempotent(req)
            .await
            .map_err(|e| map_bigtable_err("place_intent", e))?;

        if !resp.predicate_matched {
            return Ok(true);
        }
        // If an idempotent transport retry ran after our own intent was placed,
        // `predicate_matched` is true because `m:intent:<txn_id>` already exists.
        self.has_own_intent(txn_id, participant).await
    }

    async fn has_own_intent(
        &self,
        txn_id: &str,
        participant: &ParticipantRow,
    ) -> Result<bool, StorageError> {
        let qualifier = Self::intent_qualifier(txn_id);
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.client.full_table_name(&participant.data_table),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![participant.row_key.clone()],
                row_ranges: vec![],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::Chain(Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!(
                                "^{INTENT_FAMILY}$"
                            ))),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(exact_re2_bytes(
                                &qualifier,
                            ))),
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
            .map_err(|e| map_bigtable_err("ReadRows has_own_intent", e))?;
        Ok(!resp.is_empty())
    }

    /// Place an intent marker on a participant row with short jittered retries.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn place_intent(
        &self,
        txn_id: &str,
        participant: &ParticipantRow,
    ) -> Result<bool, StorageError> {
        let mut attempts = 0;
        let max_attempts = 5;
        let mut delay = Duration::from_millis(10);

        loop {
            if self.place_intent_once(txn_id, participant).await? {
                return Ok(true);
            }
            attempts += 1;
            if attempts >= max_attempts {
                return Ok(false);
            }
            let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
            let jitter = rand::random_range(0..delay_ms);
            tokio::time::sleep(delay + Duration::from_millis(jitter)).await;
            delay *= 2;
        }
    }

    /// Atomically transition `(t:state == "PENDING" && t:started_at == started_at_micros)`
    /// to `"COMMITTED"`.
    ///
    /// Returns `Ok(Some(commit_ts_micros))` if committed, or `Ok(None)` if the
    /// coordinator incarnation was already aborted or swept.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if serialization or the `CheckAndMutateRow` RPC fails.
    pub async fn try_commit_full(
        &self,
        txn_id: &str,
        started_at_micros: i64,
        mutations: &[ParticipantMutation],
        stream_records: Option<&[TxnStreamRecord]>,
        ttl_mutations: Option<&[TxnTtlMutation]>,
    ) -> Result<Option<i64>, StorageError> {
        let ts_micros = monotonic_timestamp_after_micros(started_at_micros);
        let serialized_muts = serde_json::to_vec(mutations)
            .map_err(|e| StorageError::Internal(format!("serialize mutations: {e}")))?;

        let mut true_mutations = vec![
            Self::set_cell_at(TXN_FAMILY, b"state", STATE_COMMITTED.to_vec(), ts_micros),
            Self::set_cell_at(TXN_FAMILY, b"mutations", serialized_muts, ts_micros),
        ];

        if let Some(records) = stream_records
            && !records.is_empty()
        {
            let serialized = serde_json::to_vec(records)
                .map_err(|e| StorageError::Internal(format!("serialize stream records: {e}")))?;
            true_mutations.push(Self::set_cell_at(
                TXN_FAMILY,
                b"stream_records",
                serialized,
                ts_micros,
            ));
        }

        if let Some(ttls) = ttl_mutations
            && !ttls.is_empty()
        {
            let serialized = serde_json::to_vec(ttls)
                .map_err(|e| StorageError::Internal(format!("serialize ttl mutations: {e}")))?;
            true_mutations.push(Self::set_cell_at(
                TXN_FAMILY,
                b"ttl_mutations",
                serialized,
                ts_micros,
            ));
        }

        let mut data = self.client.data();
        let req = CheckAndMutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            predicate_filter: Some(build_pending_incarnation_predicate(started_at_micros)),
            true_mutations,
            false_mutations: vec![],
            ..CheckAndMutateRowRequest::default()
        };
        let resp = data
            .check_and_mutate_row_idempotent(req)
            .await
            .map_err(|e| map_bigtable_err("txn try_commit_full", e))?;
        if resp.predicate_matched {
            Ok(Some(ts_micros))
        } else if let Some(st) = self.get_state(txn_id).await?
            && st.state == "COMMITTED"
            && st.started_at == Some(started_at_micros)
        {
            Ok(Some(st.state_ts_micros))
        } else {
            Ok(None)
        }
    }

    /// Persist a directly `COMMITTED` recovery record for post-`put_cas` /
    /// `delete_cas` shadow-index or TTL side effects that encountered a
    /// transient failure after base commit, so `sweeper` finishes them cleanly
    /// without returning a misleading error on an already-durable write.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if serialization or the `MutateRow` RPC fails.
    pub async fn write_committed_side_effects(
        &self,
        txn_id: &str,
        mutations: &[ParticipantMutation],
        ttl_mutations: &[TxnTtlMutation],
    ) -> Result<(), StorageError> {
        self.write_committed_side_effects_guarded(txn_id, None, mutations, ttl_mutations)
            .await
    }

    /// Persist a `COMMITTED` post-commit side-effect recovery record carrying an
    /// optional base-row `(participant, base_version)` guard so `roll_forward`
    /// skips stale shadow puts and TTL additions if the base row has moved past
    /// `base_version`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if serialization or the `MutateRow` RPC fails.
    pub async fn write_committed_side_effects_guarded(
        &self,
        txn_id: &str,
        base_guard: Option<&SideEffectBaseGuard>,
        mutations: &[ParticipantMutation],
        ttl_mutations: &[TxnTtlMutation],
    ) -> Result<(), StorageError> {
        if mutations.is_empty() && ttl_mutations.is_empty() {
            return Ok(());
        }
        let ts_micros = monotonic_timestamp_micros();
        let empty_parts = serde_json::to_vec(&Vec::<ParticipantRow>::new())
            .map_err(|e| StorageError::Internal(format!("serialize empty participants: {e}")))?;
        let serialized_muts = serde_json::to_vec(mutations)
            .map_err(|e| StorageError::Internal(format!("serialize side-effect mutations: {e}")))?;

        let mut muts = vec![
            Self::set_cell_at(TXN_FAMILY, b"state", STATE_COMMITTED.to_vec(), ts_micros),
            Self::set_cell_at(
                TXN_FAMILY,
                b"started_at",
                ts_micros.to_string().into_bytes(),
                ts_micros,
            ),
            Self::set_cell_at(TXN_FAMILY, b"participants", empty_parts, ts_micros),
            Self::set_cell_at(TXN_FAMILY, b"mutations", serialized_muts, ts_micros),
        ];
        if let Some(guard) = base_guard {
            let serialized_guard = serde_json::to_vec(guard)
                .map_err(|e| StorageError::Internal(format!("serialize base_guard: {e}")))?;
            muts.push(Self::set_cell_at(
                TXN_FAMILY,
                b"base_guard",
                serialized_guard,
                ts_micros,
            ));
        }
        if !ttl_mutations.is_empty() {
            let serialized_ttls = serde_json::to_vec(ttl_mutations)
                .map_err(|e| StorageError::Internal(format!("serialize side-effect ttls: {e}")))?;
            muts.push(Self::set_cell_at(
                TXN_FAMILY,
                b"ttl_mutations",
                serialized_ttls,
                ts_micros,
            ));
        }

        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            mutations: muts,
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| map_bigtable_err("txn write_committed_side_effects", e))?;
        Ok(())
    }

    /// CAS-guarded `commit` convenience wrapper returning `TransactionConflict`
    /// if the `PENDING` incarnation was already aborted or dropped.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn commit(
        &self,
        txn_id: &str,
        started_at_micros: i64,
        mutations: &[ParticipantMutation],
        stream_records: Option<&[TxnStreamRecord]>,
    ) -> Result<i64, StorageError> {
        self.try_commit_full(txn_id, started_at_micros, mutations, stream_records, None)
            .await?
            .ok_or_else(|| {
                StorageError::TransactionConflict(
                    "coordinator row is no longer PENDING (rolled back by sweeper)".into(),
                )
            })
    }

    /// Atomically CAS `(t:state == "PENDING" && t:started_at == started_at_micros)`
    /// to `"ABORTED"` BEFORE clearing any participant intent.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `CheckAndMutateRow` or state-verification RPC fails.
    pub async fn try_abort_pending(
        &self,
        txn_id: &str,
        started_at_micros: i64,
        after_ts_micros: i64,
    ) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let ts_micros = monotonic_timestamp_after_micros(after_ts_micros.max(started_at_micros));
        let req = CheckAndMutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            predicate_filter: Some(build_pending_incarnation_predicate(started_at_micros)),
            true_mutations: vec![Self::set_cell_at(
                TXN_FAMILY,
                b"state",
                STATE_ABORTED.to_vec(),
                ts_micros,
            )],
            false_mutations: vec![],
            ..CheckAndMutateRowRequest::default()
        };
        let resp = data
            .check_and_mutate_row_idempotent(req)
            .await
            .map_err(|e| map_bigtable_err("txn try_abort_pending", e))?;
        if resp.predicate_matched {
            Ok(true)
        } else if let Some(st) = self.get_state(txn_id).await?
            && st.state == "ABORTED"
            && st.started_at == Some(started_at_micros)
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Mark a `COMMITTED` coordinator `CLEANED` at `ts > after_ts_micros`
    /// without resurrecting a row that was already dropped.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the `CheckAndMutateRow` RPC fails.
    pub async fn cleaned(&self, txn_id: &str, after_ts_micros: i64) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let ts_micros = monotonic_timestamp_after_micros(after_ts_micros);
        let req = CheckAndMutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            predicate_filter: Some(build_latest_txn_qualifier_match(b"state", STATE_COMMITTED)),
            true_mutations: vec![Self::set_cell_at(
                TXN_FAMILY,
                b"state",
                STATE_CLEANED.to_vec(),
                ts_micros,
            )],
            false_mutations: vec![],
            ..CheckAndMutateRowRequest::default()
        };
        data.check_and_mutate_row_idempotent(req)
            .await
            .map_err(|e| map_bigtable_err("txn cleaned", e))?;
        Ok(())
    }

    async fn read_own_intent_stage(
        &self,
        txn_id: &str,
        participant: &ParticipantRow,
    ) -> Result<Option<(Vec<u8>, i64)>, StorageError> {
        let qualifier = Self::intent_qualifier(txn_id);
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.client.full_table_name(&participant.data_table),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![participant.row_key.clone()],
                row_ranges: vec![],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::Chain(Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!(
                                "^{INTENT_FAMILY}$"
                            ))),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(exact_re2_bytes(
                                &qualifier,
                            ))),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                    ],
                })),
            }),
            ..ReadRowsRequest::default()
        };
        match data.read_rows(req).await {
            Ok(resp) => {
                for (_, cells) in resp {
                    for cell in cells {
                        if cell.family_name == INTENT_FAMILY && cell.qualifier == qualifier {
                            return Ok(Some((cell.value, cell.timestamp_micros)));
                        }
                    }
                }
                Ok(None)
            }
            Err(e) if is_table_not_found_err(&e) => Ok(None),
            Err(e) => Err(map_bigtable_err("ReadRows read_own_intent_stage", e)),
        }
    }

    /// Apply participant mutation in Phase 2 fenced with predicate asserting BOTH:
    /// 1. Coordinator's own intent `m:intent:<txn_id>` matches the expected stage, AND
    /// 2. Row's OCC state still matches `(expected_version, row_existed)`.
    ///
    /// When a `Put` exceeds `max_mutations_per_rpc()`, executes progressive
    /// `CheckAndMutateRow` chunks fenced on `rpc_limit`-independent attribute-offset
    /// markers (`txn_id -> attr:<offset> -> ... -> chunk:done`) and `m:v`,
    /// advancing `m:v = new_version` only on the final chunk so concurrent
    /// `roll_forward` callers cooperatively complete remaining attributes without
    /// premature intent clearing, torn reads, or post-delete zombie cells.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if cell encoding or any `CheckAndMutateRow` RPC fails.
    // Justification: fenced Phase 2 apply covers Put, Delete, single-RPC CAS, and progressive multi-chunk attribute-offset CAS in one atomic state machine.
    #[allow(clippy::too_many_lines)]
    pub async fn apply_fenced_mutation(
        &self,
        txn_id: &str,
        participant: &ParticipantRow,
        payload: &TxnOpPayload,
        expected_version: Option<u64>,
        row_existed: bool,
        new_version: u64,
    ) -> Result<bool, StorageError> {
        let ts_micros = version_to_timestamp_micros(new_version);
        let rpc_limit = max_mutations_per_rpc().max(4);
        let mv_mutations = vec![
            Mutation {
                mutation: Some(mutation::Mutation::SetCell(SetCell {
                    family_name: INTENT_FAMILY.to_string(),
                    column_qualifier: QUALIFIER_VERSION.to_vec(),
                    timestamp_micros: ts_micros,
                    value: new_version.to_string().into_bytes(),
                })),
            },
            Mutation {
                mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                    family_name: INTENT_FAMILY.to_string(),
                    column_qualifier: QUALIFIER_VERSION.to_vec(),
                    time_range: older_than_ts_range(ts_micros),
                })),
            },
        ];

        match payload {
            TxnOpPayload::Delete => {
                let mut true_mutations = Vec::with_capacity(3);
                true_mutations.push(Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromFamily(DeleteFromFamily {
                        family_name: FAMILY_DATA.to_string(),
                    })),
                });
                true_mutations.extend(mv_mutations);
                let mut data = self.client.data();
                let req = CheckAndMutateRowRequest {
                    table_name: self.client.full_table_name(&participant.data_table),
                    app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                    row_key: participant.row_key.clone(),
                    predicate_filter: Some(build_fenced_apply_predicate(
                        txn_id,
                        expected_version,
                        row_existed,
                    )),
                    true_mutations,
                    false_mutations: vec![],
                    ..CheckAndMutateRowRequest::default()
                };
                match data.check_and_mutate_row_idempotent(req).await {
                    Ok(r) => Ok(r.predicate_matched),
                    Err(e) if is_table_not_found_err(&e) => Ok(false),
                    Err(e) => Err(map_bigtable_err("apply_fenced_mutation", e)),
                }
            }
            TxnOpPayload::Put { item } => {
                let mut data_muts = Vec::with_capacity(item.len());
                for (name, value) in item {
                    data_muts.push(Mutation {
                        mutation: Some(mutation::Mutation::SetCell(SetCell {
                            family_name: FAMILY_DATA.to_string(),
                            column_qualifier: name.as_bytes().to_vec(),
                            timestamp_micros: ts_micros,
                            value: crate::data::encoding::cell::encode(value)?,
                        })),
                    });
                }

                if data_muts.len().saturating_add(3) <= rpc_limit {
                    let mut true_mutations = Vec::with_capacity(data_muts.len() + 3);
                    true_mutations.extend(mv_mutations.iter().cloned());
                    true_mutations.push(Mutation {
                        mutation: Some(mutation::Mutation::DeleteFromFamily(DeleteFromFamily {
                            family_name: FAMILY_DATA.to_string(),
                        })),
                    });
                    true_mutations.extend(data_muts.iter().cloned());
                    let mut data = self.client.data();
                    let req = CheckAndMutateRowRequest {
                        table_name: self.client.full_table_name(&participant.data_table),
                        app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                        row_key: participant.row_key.clone(),
                        predicate_filter: Some(build_fenced_stage_predicate(
                            txn_id,
                            txn_id.as_bytes(),
                            expected_version,
                            row_existed,
                            false,
                        )),
                        true_mutations,
                        false_mutations: vec![],
                        ..CheckAndMutateRowRequest::default()
                    };
                    match data.check_and_mutate_row_idempotent(req).await {
                        Ok(r) if r.predicate_matched => return Ok(true),
                        Ok(_) => {
                            // Fall through to inspect `read_own_intent_stage` in case a peer
                            // with a smaller per-RPC mutation limit already advanced `m:intent`
                            // to an in-progress `attr:<offset>` or `chunk:done` stage.
                        }
                        Err(e) if is_table_not_found_err(&e) => return Ok(false),
                        Err(e) => return Err(map_bigtable_err("apply_fenced_mutation", e)),
                    }
                }

                let Some((cur_stage, cur_intent_ts)) =
                    self.read_own_intent_stage(txn_id, participant).await?
                else {
                    return Ok(false);
                };
                let mut floor_ts = cur_intent_ts;
                let mut stage = match parse_chunk_stage(txn_id, &cur_stage) {
                    Some(ChunkStage::Initial) if data_muts.len().saturating_add(3) > rpc_limit => {
                        ChunkStage::Initial
                    }
                    Some(ChunkStage::AttrOffset(off)) if off <= data_muts.len() => {
                        ChunkStage::AttrOffset(off)
                    }
                    Some(ChunkStage::Done) => return Ok(true),
                    _ => return Ok(false),
                };

                let qualifier = Self::intent_qualifier(txn_id);
                let full_table = self.client.full_table_name(&participant.data_table);
                let app_profile = self.client.app_profile_id.clone().unwrap_or_default();

                loop {
                    let (expected_stage, is_cont, body_muts, next_stage, target_stage) = match stage
                    {
                        ChunkStage::Initial => {
                            let take = (rpc_limit - 3).min(data_muts.len());
                            let mut body = Vec::with_capacity(take + 1);
                            body.push(Mutation {
                                mutation: Some(mutation::Mutation::DeleteFromFamily(
                                    DeleteFromFamily {
                                        family_name: FAMILY_DATA.to_string(),
                                    },
                                )),
                            });
                            body.extend_from_slice(&data_muts[..take]);
                            (
                                txn_id.as_bytes().to_vec(),
                                false,
                                body,
                                format!("attr:{take}").into_bytes(),
                                ChunkStage::AttrOffset(take),
                            )
                        }
                        ChunkStage::AttrOffset(offset) => {
                            let rem = data_muts.len().saturating_sub(offset);
                            if rem.saturating_add(4) <= rpc_limit {
                                let mut body = Vec::with_capacity(rem + 2);
                                body.extend_from_slice(&data_muts[offset..]);
                                body.extend(mv_mutations.iter().cloned());
                                (
                                    format!("attr:{offset}").into_bytes(),
                                    true,
                                    body,
                                    b"chunk:done".to_vec(),
                                    ChunkStage::Done,
                                )
                            } else {
                                let take = (rpc_limit - 2).min(rem);
                                let next_off = offset + take;
                                let body = data_muts[offset..next_off].to_vec();
                                (
                                    format!("attr:{offset}").into_bytes(),
                                    true,
                                    body,
                                    format!("attr:{next_off}").into_bytes(),
                                    ChunkStage::AttrOffset(next_off),
                                )
                            }
                        }
                        ChunkStage::Done => return Ok(true),
                    };

                    let predicate = build_fenced_stage_predicate(
                        txn_id,
                        &expected_stage,
                        expected_version,
                        row_existed,
                        is_cont,
                    );
                    let stage_ts = monotonic_timestamp_after_micros(floor_ts);
                    floor_ts = stage_ts;

                    let mut true_mutations = Vec::with_capacity(body_muts.len() + 2);
                    true_mutations.push(Self::set_cell_at(
                        INTENT_FAMILY,
                        &qualifier,
                        next_stage,
                        stage_ts,
                    ));
                    true_mutations.push(Mutation {
                        mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                            family_name: INTENT_FAMILY.to_string(),
                            column_qualifier: qualifier.clone(),
                            time_range: older_than_ts_range(stage_ts),
                        })),
                    });
                    true_mutations.extend(body_muts);

                    let mut data = self.client.data();
                    let req = CheckAndMutateRowRequest {
                        table_name: full_table.clone(),
                        app_profile_id: app_profile.clone(),
                        row_key: participant.row_key.clone(),
                        predicate_filter: Some(predicate),
                        true_mutations,
                        false_mutations: vec![],
                        ..CheckAndMutateRowRequest::default()
                    };
                    let resp = match data.check_and_mutate_row_idempotent(req).await {
                        Ok(r) => r,
                        Err(e) if is_table_not_found_err(&e) => return Ok(false),
                        Err(e) => {
                            return Err(map_bigtable_err("apply_fenced_mutation chunk", e));
                        }
                    };
                    if resp.predicate_matched {
                        if target_stage == ChunkStage::Done {
                            return Ok(true);
                        }
                        stage = target_stage;
                    } else {
                        let Some((fresh_stage, fresh_ts)) =
                            self.read_own_intent_stage(txn_id, participant).await?
                        else {
                            return Ok(false);
                        };
                        floor_ts = floor_ts.max(fresh_ts);
                        match (stage, parse_chunk_stage(txn_id, &fresh_stage)) {
                            (_, Some(ChunkStage::Done)) => return Ok(true),
                            (ChunkStage::Initial, Some(ChunkStage::AttrOffset(off)))
                                if off <= data_muts.len() =>
                            {
                                stage = ChunkStage::AttrOffset(off);
                            }
                            (ChunkStage::AttrOffset(cur), Some(ChunkStage::AttrOffset(off)))
                                if off > cur && off <= data_muts.len() =>
                            {
                                stage = ChunkStage::AttrOffset(off);
                            }
                            _ => return Ok(false),
                        }
                    }
                }
            }
        }
    }

    /// Clear an intent cell unconditionally (used while this incarnation still owns `txn:<id>`).
    /// Treats `NotFound` on a deleted table as completed so surviving participants'
    /// intents are never stranded.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if `MutateRow` fails with an error other than `NotFound`.
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
        match data.mutate_row(req).await {
            Ok(_) => Ok(()),
            Err(e) if is_table_not_found_err(&e) => Ok(()),
            Err(e) => Err(map_bigtable_err("clear_intent", e)),
        }
    }

    /// Clear an orphaned/cleaned intent cell bounded to `[0, next_millis_exclusive(max_ts_micros))`
    /// so delayed cleanup of an earlier deterministic `ClientRequestToken` incarnation
    /// can NEVER delete a newer incarnation's newly placed intent.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if `MutateRow` fails with an error other than `NotFound`.
    pub async fn clear_intent_up_to(
        &self,
        txn_id: &str,
        participant: &ParticipantRow,
        max_ts_micros: i64,
    ) -> Result<(), StorageError> {
        let qualifier = Self::intent_qualifier(txn_id);
        let end_ts = next_millis_exclusive(max_ts_micros);
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.client.full_table_name(&participant.data_table),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: participant.row_key.clone(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                    family_name: INTENT_FAMILY.to_string(),
                    column_qualifier: qualifier,
                    time_range: older_than_ts_range(end_ts),
                })),
            }],
            ..MutateRowRequest::default()
        };
        match data.mutate_row(req).await {
            Ok(_) => Ok(()),
            Err(e) if is_table_not_found_err(&e) => Ok(()),
            Err(e) => Err(map_bigtable_err("clear_intent_up_to", e)),
        }
    }

    /// Delete the coordinator row only if `t:started_at` still matches
    /// `started_at_micros`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if `CheckAndMutateRow` fails with an error other than `NotFound`.
    pub async fn drop_matching_start(
        &self,
        txn_id: &str,
        started_at_micros: i64,
    ) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let req = CheckAndMutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            predicate_filter: Some(build_latest_txn_qualifier_match(
                b"started_at",
                started_at_micros.to_string().as_bytes(),
            )),
            true_mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            }],
            false_mutations: vec![],
            ..CheckAndMutateRowRequest::default()
        };
        match data.check_and_mutate_row_idempotent(req).await {
            Ok(_) => Ok(()),
            Err(e) if is_table_not_found_err(&e) => Ok(()),
            Err(e) => Err(map_bigtable_err("txn drop_matching_start", e)),
        }
    }

    /// Unconditional coordinator row delete (post-clean GC when `started_at` is unknown).
    ///
    /// # Errors
    ///
    /// Infallible best-effort cleanup; always returns `Ok(())`.
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

/// Predicate matching the LATEST cell (`CellsPerColumnLimitFilter(1)` *before*
/// `ValueRegexFilter` so lazy `MaxNumVersions(1)` compaction can never match a
/// historical `PENDING` cell after transition!) of `t:<qualifier> == expected_val`.
fn build_latest_txn_qualifier_match(qualifier: &[u8], expected_val: &[u8]) -> RowFilter {
    RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{TXN_FAMILY}$"))),
                },
                RowFilter {
                    filter: Some(Filter::ColumnQualifierRegexFilter(exact_re2_bytes(
                        qualifier,
                    ))),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
                RowFilter {
                    filter: Some(Filter::ValueRegexFilter(exact_re2_bytes(expected_val))),
                },
            ],
        })),
    }
}

/// Predicate matching iff BOTH `latest(t:state) == "PENDING"` AND
/// `latest(t:started_at) == started_at_micros`.
fn build_pending_incarnation_predicate(started_at_micros: i64) -> RowFilter {
    let state_pending = build_latest_txn_qualifier_match(b"state", STATE_PENDING);
    let start_matches =
        build_latest_txn_qualifier_match(b"started_at", started_at_micros.to_string().as_bytes());
    RowFilter {
        filter: Some(Filter::Condition(Box::new(Condition {
            predicate_filter: Some(Box::new(state_pending)),
            true_filter: Some(Box::new(start_matches)),
            false_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::BlockAllFilter(true)),
            })),
        }))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkStage {
    Initial,
    AttrOffset(usize),
    Done,
}

fn parse_chunk_stage(txn_id: &str, raw: &[u8]) -> Option<ChunkStage> {
    if raw == txn_id.as_bytes() {
        return Some(ChunkStage::Initial);
    }
    if raw == b"chunk:done" {
        return Some(ChunkStage::Done);
    }
    let rest = raw.strip_prefix(b"attr:")?;
    let offset = std::str::from_utf8(rest).ok()?.parse::<usize>().ok()?;
    Some(ChunkStage::AttrOffset(offset))
}

fn build_has_version_filter() -> RowFilter {
    RowFilter {
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
    }
}

fn build_has_data_limit1_filter() -> RowFilter {
    RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{FAMILY_DATA}$"))),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerRowLimitFilter(1)),
                },
            ],
        })),
    }
}

fn combine_intent_and_occ_flat(
    intent_limit1: RowFilter,
    expected_version: Option<u64>,
    row_existed: bool,
    is_continuation_chunk: bool,
) -> RowFilter {
    let has_v = build_has_version_filter();
    match (expected_version, is_continuation_chunk, row_existed) {
        (Some(v), _, _) => RowFilter {
            filter: Some(Filter::Condition(Box::new(Condition {
                predicate_filter: Some(Box::new(intent_limit1)),
                true_filter: Some(Box::new(build_version_match_filter(v))),
                false_filter: Some(Box::new(RowFilter {
                    filter: Some(Filter::BlockAllFilter(true)),
                })),
            }))),
        },
        (None, true, _) => RowFilter {
            filter: Some(Filter::Condition(Box::new(Condition {
                predicate_filter: Some(Box::new(has_v)),
                true_filter: Some(Box::new(RowFilter {
                    filter: Some(Filter::BlockAllFilter(true)),
                })),
                false_filter: Some(Box::new(intent_limit1)),
            }))),
        },
        (None, false, false) => {
            let has_v_or_d = RowFilter {
                filter: Some(Filter::Interleave(Interleave {
                    filters: vec![has_v, build_has_data_limit1_filter()],
                })),
            };
            RowFilter {
                filter: Some(Filter::Condition(Box::new(Condition {
                    predicate_filter: Some(Box::new(has_v_or_d)),
                    true_filter: Some(Box::new(RowFilter {
                        filter: Some(Filter::BlockAllFilter(true)),
                    })),
                    false_filter: Some(Box::new(intent_limit1)),
                }))),
            }
        }
        (None, false, true) => {
            // Exact logical AND of two 1-cell-bounded presence filters without a
            // nested Condition: `Interleave([intent_limit1, has_d_limit1])` emits
            // 2 cells iff both hold, so `CellsPerRowOffsetFilter(1)` emits 1 cell
            // iff both hold and 0 cells otherwise.
            let both_present = RowFilter {
                filter: Some(Filter::Chain(Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::Interleave(Interleave {
                                filters: vec![intent_limit1, build_has_data_limit1_filter()],
                            })),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerRowOffsetFilter(1)),
                        },
                    ],
                })),
            };
            RowFilter {
                filter: Some(Filter::Condition(Box::new(Condition {
                    predicate_filter: Some(Box::new(has_v)),
                    true_filter: Some(Box::new(RowFilter {
                        filter: Some(Filter::BlockAllFilter(true)),
                    })),
                    false_filter: Some(Box::new(both_present)),
                }))),
            }
        }
    }
}

fn build_fenced_stage_predicate(
    txn_id: &str,
    expected_stage: &[u8],
    expected_version: Option<u64>,
    row_existed: bool,
    is_continuation_chunk: bool,
) -> RowFilter {
    let qualifier = TxnCoordinator::intent_qualifier(txn_id);
    let stage_limit1 = RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{INTENT_FAMILY}$"))),
                },
                RowFilter {
                    filter: Some(Filter::ColumnQualifierRegexFilter(exact_re2_bytes(
                        &qualifier,
                    ))),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
                RowFilter {
                    filter: Some(Filter::ValueRegexFilter(exact_re2_bytes(expected_stage))),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerRowLimitFilter(1)),
                },
            ],
        })),
    };
    combine_intent_and_occ_flat(
        stage_limit1,
        expected_version,
        row_existed,
        is_continuation_chunk,
    )
}

/// Predicate matching iff `m:intent:<txn_id>` is present AND the row's OCC
/// state matches `(expected_version, row_existed)`, using a single flat outer
/// `Filter::Condition` (zero nested `Condition`s).
fn build_fenced_apply_predicate(
    txn_id: &str,
    expected_version: Option<u64>,
    row_existed: bool,
) -> RowFilter {
    let qualifier = TxnCoordinator::intent_qualifier(txn_id);
    let intent_limit1 = RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{INTENT_FAMILY}$"))),
                },
                RowFilter {
                    filter: Some(Filter::ColumnQualifierRegexFilter(exact_re2_bytes(
                        &qualifier,
                    ))),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerRowLimitFilter(1)),
                },
            ],
        })),
    };
    combine_intent_and_occ_flat(intent_limit1, expected_version, row_existed, false)
}

/// Filter reading only the newest cell per qualifier in family `t` of `__extenddb_txn_log__`.
#[must_use]
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

/// Server-side `RowFilter` returning the newest cell per qualifier in family `t`
/// of `__extenddb_txn_log__` strictly for coordinator rows whose latest `t:state`
/// equals `exact_state` (e.g. `b"PENDING"` or `b"COMMITTED"`), skipping
/// non-matching coordinator rows on the server without consuming `rows_limit`.
#[must_use]
pub fn build_txn_log_state_filter(exact_state: &[u8]) -> RowFilter {
    RowFilter {
        filter: Some(Filter::Condition(Box::new(Condition {
            predicate_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::Chain(Chain {
                    filters: vec![
                        RowFilter {
                            filter: Some(Filter::FamilyNameRegexFilter(format!("^{TXN_FAMILY}$"))),
                        },
                        RowFilter {
                            filter: Some(Filter::ColumnQualifierRegexFilter(b"^state$".to_vec())),
                        },
                        RowFilter {
                            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                        },
                        RowFilter {
                            filter: Some(Filter::ValueRegexFilter(exact_re2_bytes(exact_state))),
                        },
                    ],
                })),
            })),
            true_filter: Some(Box::new(build_txn_log_latest_filter())),
            false_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::BlockAllFilter(true)),
            })),
        }))),
    }
}

/// Filter matching ANY physical `m:intent:*` cell in family `m`, with no wall-clock
/// `TimestampRangeFilter`.
fn build_intent_presence_filter() -> RowFilter {
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
            ],
        })),
    }
}

/// Ensure the `__extenddb_txn_log__` system table exists on the Bigtable
/// data instance (called from Migration 1 `M0001DataBaseline`).
///
/// # Errors
///
/// Returns [`crate::data::admin::AdminError`] if connecting or creating the table fails.
pub async fn ensure_txn_log_table(
    client: &BigtableClient,
) -> Result<(), crate::data::admin::AdminError> {
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
    fn test_txn_id_and_single_id_formats() {
        let id = TxnCoordinator::new_txn_id();
        assert!(id.starts_with("txn-"));
        assert_eq!(id.len(), 4 + 32);
        assert!(!TxnCoordinator::is_single_item_txn(&id));

        let sid = TxnCoordinator::new_single_txn_id();
        assert!(sid.starts_with(SINGLE_TXN_PREFIX));
        assert!(TxnCoordinator::is_single_item_txn(&sid));
    }

    #[test]
    fn test_open_with_token_retry_nonce_disambiguates_own_pending_incarnation() {
        use crate::data::client::RowCell;

        let cells = vec![
            RowCell {
                family_name: TXN_FAMILY.to_owned(),
                qualifier: b"state".to_vec(),
                value: b"PENDING".to_vec(),
                timestamp_micros: 1_700_000_000_000,
                labels: vec![],
            },
            RowCell {
                family_name: TXN_FAMILY.to_owned(),
                qualifier: b"started_at".to_vec(),
                value: b"1700000000000".to_vec(),
                timestamp_micros: 1_700_000_000_000,
                labels: vec![],
            },
            RowCell {
                family_name: TXN_FAMILY.to_owned(),
                qualifier: b"open_nonce".to_vec(),
                value: b"nonce-caller-a".to_vec(),
                timestamp_micros: 1_700_000_000_000,
                labels: vec![],
            },
        ];
        let decoded = decode_txn_state_cells(cells).expect("decoded TxnState");
        assert!(is_own_retried_open(
            &decoded,
            1_700_000_000_000,
            "nonce-caller-a"
        ));
        assert!(
            !is_own_retried_open(&decoded, 1_700_000_000_000, "nonce-caller-b"),
            "concurrent peer with identical millisecond timestamp must NOT claim another caller's row"
        );
        assert!(!is_own_retried_open(
            &decoded,
            1_700_000_001_000,
            "nonce-caller-a"
        ));

        let mut committed = decoded.clone();
        committed.state = "COMMITTED".to_owned();
        assert!(!is_own_retried_open(
            &committed,
            1_700_000_000_000,
            "nonce-caller-a"
        ));
    }

    #[test]
    fn test_participant_row_canonical_ordering() {
        let a = ParticipantRow {
            data_table: "t0001".into(),
            row_key: vec![0x02],
        };
        let b = ParticipantRow {
            data_table: "t0001".into(),
            row_key: vec![0x01],
        };
        let c = ParticipantRow {
            data_table: "t0002".into(),
            row_key: vec![0x00],
        };
        let mut v = vec![c.clone(), a.clone(), b.clone()];
        v.sort();
        assert_eq!(v, vec![b, a, c]);
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
    fn test_pending_incarnation_predicate_limits_cells_before_value_regex() {
        let pred = build_pending_incarnation_predicate(1_700_000_000_000);
        if let Some(Filter::Condition(cond)) = pred.filter {
            let Some(Filter::Chain(state_chain)) = cond
                .predicate_filter
                .as_ref()
                .and_then(|f| f.filter.as_ref())
            else {
                panic!("expected Chain on state predicate");
            };
            assert!(matches!(
                state_chain.filters[2].filter,
                Some(Filter::CellsPerColumnLimitFilter(1))
            ));
            assert!(matches!(
                state_chain.filters[3].filter,
                Some(Filter::ValueRegexFilter(_))
            ));
        } else {
            panic!("expected outer Condition");
        }
    }

    fn assert_no_nested_condition(filter: &RowFilter, inside_condition: bool) {
        match &filter.filter {
            Some(Filter::Condition(cond)) => {
                assert!(
                    !inside_condition,
                    "Cloud Bigtable forbids nested Filter::Condition inside another Condition"
                );
                if let Some(p) = &cond.predicate_filter {
                    assert_no_nested_condition(p, true);
                }
                if let Some(t) = &cond.true_filter {
                    assert_no_nested_condition(t, true);
                }
                if let Some(f) = &cond.false_filter {
                    assert_no_nested_condition(f, true);
                }
            }
            Some(Filter::Chain(chain)) => {
                for sub in &chain.filters {
                    assert_no_nested_condition(sub, inside_condition);
                }
            }
            Some(Filter::Interleave(interleave)) => {
                for sub in &interleave.filters {
                    assert_no_nested_condition(sub, inside_condition);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn test_build_fenced_apply_and_stage_predicates_use_single_flat_outer_condition() {
        assert_no_nested_condition(
            &build_pending_incarnation_predicate(1_700_000_000_000),
            false,
        );
        assert_no_nested_condition(&build_txn_log_state_filter(b"PENDING"), false);
        assert_no_nested_condition(&build_txn_log_state_filter(b"COMMITTED"), false);
        for (expected, existed) in [
            (Some(7), true),
            (Some(7), false),
            (None, false),
            (None, true),
        ] {
            let pred = build_fenced_apply_predicate("txn-abc.123", expected, existed);
            assert_no_nested_condition(&pred, false);
            if let Some(Filter::Condition(cond)) = pred.filter {
                assert!(cond.predicate_filter.is_some());
                assert!(cond.true_filter.is_some());
                assert!(cond.false_filter.is_some());
            } else {
                panic!("expected outer Filter::Condition");
            }
            for is_cont in [false, true] {
                let spred = build_fenced_stage_predicate(
                    "txn-abc.123",
                    b"attr:4",
                    expected,
                    existed,
                    is_cont,
                );
                assert_no_nested_condition(&spred, false);
            }
        }
        assert_eq!(
            parse_chunk_stage("txn-1", b"txn-1"),
            Some(ChunkStage::Initial)
        );
        assert_eq!(
            parse_chunk_stage("txn-1", b"attr:42"),
            Some(ChunkStage::AttrOffset(42))
        );
        assert_eq!(
            parse_chunk_stage("txn-1", b"chunk:done"),
            Some(ChunkStage::Done)
        );
        assert_eq!(parse_chunk_stage("txn-1", b"other"), None);
    }
}
