// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transaction ledger access functions for DynamoDB transactions.

use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::value::Bytes;
use extenddb_storage::error::StorageError;
use uuid::Uuid;

use crate::CassandraEngine;
use crate::cassandra_util::{get_column, query_optional, query_rows};

/// Transaction states (from DynamoDB paper).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Preparing,
    Committing,
    Cancelling,
}

impl TransactionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Preparing => "PREPARING",
            Self::Committing => "COMMITTING",
            Self::Cancelling => "CANCELLING",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "PREPARING" => Some(Self::Preparing),
            "COMMITTING" => Some(Self::Committing),
            "CANCELLING" => Some(Self::Cancelling),
            _ => None,
        }
    }
}

/// A single operation stored in the transaction ledger blob.
///
/// This is the serializable, owned projection of what the PREPARE phase
/// computed. It is distinct from `TransactWriteOp`, which is a borrowed,
/// request-scoped type holding parsed expressions and references that cannot
/// survive a process restart.
///
/// By the time a `LedgerOp` is created, all conditions have been evaluated
/// and update actions applied. It therefore contains only the minimal data
/// needed to either resume a COMMIT (write `item_data` to the base table and
/// clear the transaction marker) or execute a ROLLBACK (delete the row if
/// `created_to_prepare` is set on it, otherwise clear the transaction marker).
/// No expressions, key schemas, or attribute definitions are required.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LedgerOp {
    /// Operation type: "PUT", "UPDATE", "DELETE", "CHECK"
    pub op: String,
    /// DynamoDB table ID (not table name)
    pub table_id: String,
    /// Composite partition key text (as stored in Cassandra `pk` column)
    pub pk: String,
    /// Sort key column name ("sk_s", "sk_n", "sk_b"), if table has a sort key
    pub sk_col: Option<String>,
    /// Sort key value serialized as a JSON string, if table has a sort key
    pub sk_val: Option<String>,
    /// Final item data JSON for PUT/UPDATE (the post-mutation state to write on
    /// COMMIT). None for DELETE and CHECK, which require no item data.
    pub item_data: Option<String>,
}

/// Transaction ledger entry.
#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub txn_id: Uuid,
    pub state: String,
    pub started_at: i64,
    pub client_token: Option<String>,
    pub request_fingerprint: Option<String>,
    pub items_blob: String,
}

impl LedgerEntry {
    /// Parse `items_blob` into the list of `LedgerOp`s.
    pub fn parse_ops(&self) -> Result<Vec<LedgerOp>, StorageError> {
        serde_json::from_str(&self.items_blob)
            .map_err(|e| StorageError::Internal(format!("parse ledger ops: {e}")))
    }
}

impl CassandraEngine {
    /// Write a new transaction ledger entry.
    ///
    /// Returns an error if the transaction ID already exists (LWT failure).
    pub async fn write_ledger_entry(
        &self,
        keyspace: &str,
        txn_id: Uuid,
        state: TransactionState,
        started_at: i64,
        client_token: Option<&str>,
        request_fingerprint: Option<&str>,
        items_blob: &str,
    ) -> Result<(), StorageError> {
        let query = format!(
            "INSERT INTO {}.transaction_ledger \
             (txn_id, state, started_at, client_token, request_fingerprint, items_blob) \
             VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS",
            keyspace
        );

        let result = self
            .session
            .query_with_values(
                &query,
                cdrs_tokio::query_values!(
                    Bytes::new(txn_id.as_bytes().to_vec()),
                    state.as_str(),
                    started_at,
                    client_token,
                    request_fingerprint,
                    items_blob
                ),
            )
            .await
            .map_err(|e| {
                tracing::error!("write_ledger_entry: {e}");
                StorageError::Internal("Database error".to_owned())
            })?;

        let body = result.response_body().map_err(|e| {
            tracing::error!("write_ledger_entry response_body: {e}");
            StorageError::Internal("Database error".to_owned())
        })?;

        if let Some(rows) = body.into_rows()
            && let Some(row) = rows.first() {
                let applied: bool = get_column(row, "[applied]", "write_ledger_entry")?;
                if !applied {
                    tracing::error!("write_ledger_entry: transaction ID already exists");
                    return Err(StorageError::Internal(
                        "Transaction ID already exists".to_owned(),
                    ));
                }
            }

        Ok(())
    }

    /// Update ledger state.
    pub async fn update_ledger_state(
        &self,
        keyspace: &str,
        txn_id: Uuid,
        new_state: TransactionState,
    ) -> Result<(), StorageError> {
        let query = format!(
            "UPDATE {}.transaction_ledger SET state = ? WHERE txn_id = ?",
            keyspace
        );

        self.session
            .query_with_values(
                &query,
                cdrs_tokio::query_values!(
                    new_state.as_str(),
                    Bytes::new(txn_id.as_bytes().to_vec())
                ),
            )
            .await
            .map_err(|e| {
                tracing::error!("update_ledger_state: {e}");
                StorageError::Internal("Database error".to_owned())
            })?;

        Ok(())
    }

    /// Update the items blob in the ledger (called after PREPARE succeeds, before COMMITTING).
    ///
    /// This stores the full `LedgerOp` list so the recovery worker has everything
    /// it needs to resume a COMMIT or execute a ROLLBACK without any in-memory state.
    pub async fn update_ledger_blob(
        &self,
        keyspace: &str,
        txn_id: Uuid,
        ops: &[LedgerOp],
    ) -> Result<(), StorageError> {
        let blob = serde_json::to_string(ops)
            .map_err(|e| StorageError::Internal(format!("serialize ledger ops: {e}")))?;
        let query = format!(
            "UPDATE {}.transaction_ledger SET items_blob = ? WHERE txn_id = ?",
            keyspace
        );
        self.session
            .query_with_values(
                &query,
                cdrs_tokio::query_values!(blob, Bytes::new(txn_id.as_bytes().to_vec())),
            )
            .await
            .map_err(|e| {
                tracing::error!("update_ledger_blob: {e}");
                StorageError::Internal("Database error".to_owned())
            })?;
        Ok(())
    }

    /// Read a transaction ledger entry by ID.
    pub async fn read_ledger_entry(
        &self,
        keyspace: &str,
        txn_id: Uuid,
    ) -> Result<Option<LedgerEntry>, StorageError> {
        let query = format!(
            "SELECT txn_id, state, started_at, client_token, request_fingerprint, items_blob \
             FROM {}.transaction_ledger WHERE txn_id = ?",
            keyspace
        );

        let row = query_optional::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(Bytes::new(txn_id.as_bytes().to_vec())),
            "read_ledger_entry",
        )
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let txn_id: Uuid = get_column(&row, "txn_id", "read_ledger_entry")?;

        let state: String = get_column(&row, "state", "read_ledger_entry")?;
        let started_at: i64 = get_column(&row, "started_at", "read_ledger_entry")?;
        let client_token: Option<String> = row.get_by_name("client_token").ok().flatten();
        let request_fingerprint: Option<String> =
            row.get_by_name("request_fingerprint").ok().flatten();
        let items_blob: String = get_column(&row, "items_blob", "read_ledger_entry")?;

        Ok(Some(LedgerEntry {
            txn_id,
            state,
            started_at,
            client_token,
            request_fingerprint,
            items_blob,
        }))
    }

    /// Scan for old transactions (for recovery worker).
    pub async fn scan_old_transactions(
        &self,
        keyspace: &str,
        cutoff_timestamp: i64,
    ) -> Result<Vec<LedgerEntry>, StorageError> {
        let query = format!(
            "SELECT txn_id, state, started_at, client_token, request_fingerprint, items_blob \
             FROM {}.transaction_ledger WHERE started_at < ? ALLOW FILTERING",
            keyspace
        );

        let rows = query_rows::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(cutoff_timestamp),
            "scan_old_transactions",
        )
        .await?;

        let mut entries = Vec::new();
        for row in rows {
            let txn_id: Uuid = get_column(&row, "txn_id", "scan_old_transactions")?;

            let state: String = get_column(&row, "state", "scan_old_transactions")?;
            let started_at: i64 = get_column(&row, "started_at", "scan_old_transactions")?;
            let client_token: Option<String> = row.get_by_name("client_token").ok().flatten();
            let request_fingerprint: Option<String> =
                row.get_by_name("request_fingerprint").ok().flatten();
            let items_blob: String = get_column(&row, "items_blob", "scan_old_transactions")?;

            entries.push(LedgerEntry {
                txn_id,
                state,
                started_at,
                client_token,
                request_fingerprint,
                items_blob,
            });
        }

        Ok(entries)
    }

    /// Delete a transaction from the ledger.
    pub async fn delete_ledger_entry(
        &self,
        keyspace: &str,
        txn_id: Uuid,
    ) -> Result<(), StorageError> {
        let query = format!(
            "DELETE FROM {}.transaction_ledger WHERE txn_id = ?",
            keyspace
        );

        self.session
            .query_with_values(
                &query,
                cdrs_tokio::query_values!(Bytes::new(txn_id.as_bytes().to_vec())),
            )
            .await
            .map_err(|e| {
                tracing::error!("delete_ledger_entry: {e}");
                StorageError::Internal("Database error".to_owned())
            })?;

        Ok(())
    }
}
