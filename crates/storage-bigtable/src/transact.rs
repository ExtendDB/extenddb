//! Coordinator-row 2PC for TransactWriteItems.
//!
//! State machine (per transaction):
//!   PENDING   — coordinator row exists; intents are being placed.
//!   COMMITTED — every condition passed and every intent landed; commit point.
//!   CLEANED   — every mutation has been applied and every intent cleared.
//!
//! Per-row intent markers live in column family `m` under qualifier
//! `intent:<txn_id>`. When a new transaction wants to write a row, it places
//! its intent via CheckAndMutateRow with a predicate that succeeds only when
//! no other recent intent exists — guaranteeing at most one in-flight
//! transaction per row at a time.
//!
//! What this gets us: serializability *between* concurrent
//! TransactWriteItems. Single-row writes still go through the cheap
//! MutateRow path (no intent check) so they can race with an in-flight
//! transaction; that residual gap is documented in
//! `capabilities/bigtable.yaml` under `transactions.serializable_isolation`.
//!
//! No external sweeper in v1: stale intents become irrelevant once they
//! age past `intent_timeout_secs` because the conflict predicate ignores
//! older cells. A real recovery sweeper is a follow-up.

use std::time::{Duration, SystemTime};

use extenddb_core::types::{CancellationReason, Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use serde::{Deserialize, Serialize};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::{
    DeleteFromColumn, DeleteFromRow, SetCell,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Filter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    CheckAndMutateRowRequest, MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowSet,
    mutation,
};
use uuid::Uuid;

use crate::data::client::BigtableClient;

pub const TXN_LOG_TABLE: &str = "__extenddb_txn_log__";
pub const TXN_FAMILY: &str = "t";
pub const INTENT_FAMILY: &str = "m";
pub const INTENT_QUALIFIER_PREFIX: &str = "intent:";

const STATE_PENDING: &[u8] = b"PENDING";
const STATE_COMMITTED: &[u8] = b"COMMITTED";
const STATE_CLEANED: &[u8] = b"CLEANED";
const STATE_ABORTED: &[u8] = b"ABORTED";

/// Newtype for a per-participant target.
#[derive(Clone, Serialize, Deserialize)]
pub struct ParticipantRow {
    pub data_table: String,    // BigTable table id
    pub row_key: Vec<u8>,      // encoded composite key
}

#[derive(Serialize, Deserialize, Clone)]
pub enum TxnOpPayload {
    Put { item: Item },
    Delete,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ParticipantMutation {
    pub participant: ParticipantRow,
    pub payload: TxnOpPayload,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TxnStreamRecord {
    pub stream_arn: String,
    pub record: extenddb_core::types::StreamRecord,
}

pub struct TxnState {
    pub state: String,
    pub participants: Option<Vec<ParticipantRow>>,
    pub mutations: Option<Vec<ParticipantMutation>>,
    pub stream_records: Option<Vec<TxnStreamRecord>>,
}

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

    fn coordinator_row_key(txn_id: &str) -> String {
        format!("txn:{txn_id}")
    }

    fn intent_qualifier(txn_id: &str) -> Vec<u8> {
        let mut q = INTENT_QUALIFIER_PREFIX.as_bytes().to_vec();
        q.extend_from_slice(txn_id.as_bytes());
        q
    }

    /// Write the coordinator row in PENDING state.
    /// Write the coordinator row in PENDING state if it doesn't exist.
    /// Returns Ok(true) if created, Ok(false) if already existed.
    pub async fn open(&self, txn_id: &str, participants: &[ParticipantRow]) -> Result<bool, StorageError> {
        let mut data = self.client.data();
        let now_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        let serialized = serde_json::to_vec(participants)
            .map_err(|e| StorageError::Internal(format!("serialize participants: {e}")))?;

        // Predicate: check if family 't' exists.
        let predicate = RowFilter {
            filter: Some(Filter::FamilyNameRegexFilter(TXN_FAMILY.to_string())),
        };

        let req = CheckAndMutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            predicate_filter: Some(predicate),
            true_mutations: vec![],
            false_mutations: vec![
                Self::set_cell(TXN_FAMILY, b"state", STATE_PENDING.to_vec()),
                Self::set_cell(
                    TXN_FAMILY,
                    b"started_at",
                    now_micros.to_string().into_bytes(),
                ),
                Self::set_cell(
                    TXN_FAMILY,
                    b"participants",
                    serialized,
                ),
            ],
            ..CheckAndMutateRowRequest::default()
        };

        let resp = data.check_and_mutate_row(req)
            .await
            .map_err(|e| StorageError::Internal(format!("txn open: {e}")))?;

        Ok(!resp.predicate_matched)
    }

    pub async fn get_state(&self, txn_id: &str) -> Result<Option<TxnState>, StorageError> {
        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![Self::coordinator_row_key(txn_id).into_bytes()],
                row_ranges: vec![],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(TXN_FAMILY.to_string())),
            }),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| StorageError::Internal(format!("ReadRows get_state: {e}")))?;
        let mut iter = resp.into_iter();
        let Some((_, cells)) = iter.next() else {
            return Ok(None);
        };

        let mut state: Option<String> = None;
        let mut participants: Option<Vec<ParticipantRow>> = None;
        let mut mutations: Option<Vec<ParticipantMutation>> = None;
        let mut stream_records: Option<Vec<TxnStreamRecord>> = None;

        for cell in cells {
            if cell.family_name == TXN_FAMILY {
                match cell.qualifier.as_slice() {
                    b"state" => {
                        state = String::from_utf8(cell.value).ok();
                    }
                    b"participants" => {
                        participants = serde_json::from_slice(&cell.value).ok();
                    }
                    b"mutations" => {
                        mutations = serde_json::from_slice(&cell.value).ok();
                    }
                    b"stream_records" => {
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
            participants,
            mutations,
            stream_records,
        }))
    }

    /// Place an intent marker on a participant row. Returns Ok(true) if the
    /// intent landed; Ok(false) if a fresh-enough conflicting intent already
    /// exists; Err on transport failures.
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
            let now_micros = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or(0);
            let cutoff = now_micros - self.intent_max_age.as_micros() as i64;
            let predicate = build_intent_presence_filter(cutoff);

            let req = CheckAndMutateRowRequest {
                table_name: self.client.full_table_name(&participant.data_table),
                row_key: participant.row_key.clone(),
                predicate_filter: Some(predicate),
                true_mutations: vec![],
                false_mutations: vec![Self::set_cell(
                    INTENT_FAMILY,
                    &qualifier,
                    txn_id.as_bytes().to_vec(),
                )],
                ..CheckAndMutateRowRequest::default()
            };
            let resp = data
                .check_and_mutate_row(req)
                .await
                .map_err(|e| StorageError::Internal(format!("place_intent: {e}")))?;

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

    /// Move the coordinator row to COMMITTED — this is the commit point.
    /// Any failure here means the txn isn't yet committed; caller must roll back.
    /// Move the coordinator row to COMMITTED — this is the commit point.
    /// Any failure here means the txn isn't yet committed; caller must roll back.
    pub async fn commit(
        &self,
        txn_id: &str,
        mutations: &[ParticipantMutation],
        stream_records: Option<&[TxnStreamRecord]>,
    ) -> Result<(), StorageError> {
        let mut txn_mutations = vec![Self::set_cell(TXN_FAMILY, b"state", STATE_COMMITTED.to_vec())];
        
        let serialized_muts = serde_json::to_vec(mutations)
            .map_err(|e| StorageError::Internal(format!("serialize mutations: {e}")))?;
        txn_mutations.push(Self::set_cell(TXN_FAMILY, b"mutations", serialized_muts));

        if let Some(records) = stream_records {
            let serialized = serde_json::to_vec(records)
                .map_err(|e| StorageError::Internal(format!("serialize stream records: {e}")))?;
            txn_mutations.push(Self::set_cell(TXN_FAMILY, b"stream_records", serialized));
        }
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            mutations: txn_mutations,
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| StorageError::Internal(format!("txn commit: {e}")))?;
        Ok(())
    }

    /// Mark the coordinator CLEANED after all participant mutations applied.
    pub async fn cleaned(&self, txn_id: &str) -> Result<(), StorageError> {
        self.set_state(txn_id, STATE_CLEANED).await
    }

    /// Mark the coordinator ABORTED after rollback failed to clear all intents.
    pub async fn aborted(&self, txn_id: &str) -> Result<(), StorageError> {
        self.set_state(txn_id, STATE_ABORTED).await
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
            .map_err(|e| StorageError::Internal(format!("clear_intent: {e}")))?;
        Ok(())
    }

    /// Delete the coordinator row entirely (post-clean GC).
    pub async fn drop(&self, txn_id: &str) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
            }],
            ..MutateRowRequest::default()
        };
        let _ = data.mutate_row(req).await;
        Ok(())
    }

    async fn set_state(&self, txn_id: &str, state: &[u8]) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.client.full_table_name(TXN_LOG_TABLE),
            row_key: Self::coordinator_row_key(txn_id).into_bytes(),
            mutations: vec![Self::set_cell(TXN_FAMILY, b"state", state.to_vec())],
            ..MutateRowRequest::default()
        };
        data.mutate_row(req)
            .await
            .map_err(|e| StorageError::Internal(format!("set_state {state:?}: {e}")))?;
        Ok(())
    }

    fn set_cell(family: &str, qualifier: &[u8], value: Vec<u8>) -> Mutation {
        Mutation {
            mutation: Some(mutation::Mutation::SetCell(SetCell {
                family_name: family.to_string(),
                column_qualifier: qualifier.to_vec(),
                timestamp_micros: -1,
                value,
            })),
        }
    }
}

/// Filter that matches any cell in family `m` with qualifier prefix `intent:`
/// and timestamp ≥ cutoff micros. Used as the CheckAndMutateRow predicate
/// when placing an intent — true match → some other fresh-enough intent
/// already holds this row.
fn build_intent_presence_filter(cutoff_micros: i64) -> RowFilter {
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::{Chain, Filter};
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::TimestampRange;
    RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(
                        INTENT_FAMILY.to_string(),
                    )),
                },
                RowFilter {
                    filter: Some(Filter::ColumnQualifierRegexFilter(
                        format!("^{INTENT_QUALIFIER_PREFIX}.*$").into_bytes(),
                    )),
                },
                RowFilter {
                    filter: Some(Filter::TimestampRangeFilter(TimestampRange {
                        start_timestamp_micros: cutoff_micros.max(0),
                        end_timestamp_micros: 0, // 0 = unbounded
                    })),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
            ],
        })),
    }
}

/// Cancellation reason convenience builder.
pub fn cancellation_reason(code: &str, message: Option<&str>, item: Option<Item>) -> CancellationReason {
    CancellationReason {
        code: code.to_string(),
        message: message.map(str::to_owned),
        item,
    }
}

/// Make sure the `__extenddb_txn_log__` admin table exists on the BigTable
/// instance. Called once at startup from runtime_hooks (idempotent — admin
/// create swallows AlreadyExists).
pub async fn ensure_txn_log_table(client: &BigtableClient) -> Result<(), String> {
    let mut admin = crate::data::admin::AdminClient::connect(client).await?;
    admin
        .create_table(TXN_LOG_TABLE, &[(TXN_FAMILY, None)])
        .await
}

// Silence the unused-import warning when we extend with key_info-based helpers later.
#[allow(dead_code)]
fn _phantom_keep(_: &TableKeyInfo) {}
