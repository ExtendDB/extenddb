// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB transaction implementations for Cassandra backend.

use cdrs_tokio::frame::Envelope;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::value::Bytes;
use extenddb_core::expression::{self, ExpressionMaps};
use extenddb_core::types::{
    AttributeValue, CancellationReason, Item, ReturnValuesOnConditionCheckFailure, TableKeyInfo,
};
use extenddb_core::validation;
use extenddb_storage::TransactGetOp;
use extenddb_storage::TransactWriteOp;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, composite_pk_to_text, parse_sk, sk_column, sk_info};
use uuid::Uuid;

use crate::CassandraEngine;
use crate::cassandra_util::{get_column, query_optional};
use crate::data::ddl::data_table_name;
use crate::data::transaction_ledger::{LedgerOp, TransactionState};
use crate::data::{
    query_with_item_ts_pk_sk_txnid, query_with_pk_sk_item_txnid_ts, query_with_pk_sk_txnid,
    query_with_txnid_ts_pk_sk, select_by_pk,
};

/// Create a transaction conflict cancellation reason.
fn transaction_conflict_reason() -> CancellationReason {
    CancellationReason {
        code: "TransactionConflict".to_owned(),
        message: Some("Transaction is in use by another operation".to_owned()),
        item: None,
    }
}

impl CassandraEngine {
    /// Implementation of `DataEngine::transact_get_items`.
    ///
    /// Uses a two-phase read protocol to ensure serializability:
    /// 1. Read all items with timestamps and prepared_txn_id
    /// 2. Verify timestamps and prepared_txn_id haven't changed
    pub(crate) async fn transact_get_items_impl(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> Result<Vec<Option<Item>>, StorageError> {
        // T2.1: Request parsing and validation
        // Validate key types before reading
        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        let mut any_failed = false;
        for op in ops {
            match validation::validate_key_only(
                op.key,
                &op.key_info.key_schema,
                &op.key_info.attribute_definitions,
            ) {
                Ok(()) => reasons.push(CancellationReason::none()),
                Err(e) => {
                    any_failed = true;
                    reasons.push(CancellationReason::validation_error(e.to_string()));
                }
            }
        }
        if any_failed {
            return Err(StorageError::TransactionCanceled(reasons));
        }

        // Verify all operations are in the same account (same keyspace)
        if ops.len() > 1 {
            let first_account = &ops[0].key_info.account_id;
            for op in &ops[1..] {
                if &op.key_info.account_id != first_account {
                    return Err(StorageError::Validation(
                        "Cross-account transactions not supported".to_owned(),
                    ));
                }
            }
        }

        // T2.2: Two-phase read protocol
        // Phase 1: Read all items with timestamps
        let mut phase1_results = Vec::with_capacity(ops.len());
        for op in ops {
            let result = self.read_item_with_metadata(op).await?;
            phase1_results.push(result);
        }

        // Check for prepared transactions in Phase 1 data
        for (i, result) in phase1_results.iter().enumerate() {
            if let Some((_, _, prepared_txn_id)) = result
                && prepared_txn_id.is_some()
            {
                // Item is in a prepared transaction - conflict
                let mut reasons = vec![CancellationReason::none(); ops.len()];
                reasons[i] = transaction_conflict_reason();
                return Err(StorageError::TransactionCanceled(reasons));
            }
        }

        // Phase 2: Verify timestamps haven't changed
        for (i, op) in ops.iter().enumerate() {
            let phase1_metadata = &phase1_results[i];
            let phase2_metadata = self.read_item_metadata_only(op).await?;

            // Compare timestamps and prepared_txn_id
            match (phase1_metadata, &phase2_metadata) {
                (Some((_, ts1, prep1)), Some((ts2, prep2))) => {
                    if ts1 != ts2 || prep1 != prep2 {
                        // Timestamp changed or transaction started
                        let mut reasons = vec![CancellationReason::none(); ops.len()];
                        reasons[i] = transaction_conflict_reason();
                        return Err(StorageError::TransactionCanceled(reasons));
                    }
                }
                (None, Some(_)) => {
                    // Item was created between phases
                    let mut reasons = vec![CancellationReason::none(); ops.len()];
                    reasons[i] = transaction_conflict_reason();
                    return Err(StorageError::TransactionCanceled(reasons));
                }
                _ => {
                    // Both None or phase2 is None (item deleted) - acceptable
                }
            }
        }

        // All verifications passed - return Phase 1 item data
        Ok(phase1_results
            .into_iter()
            .map(|r| r.map(|(item, _, _)| item))
            .collect())
    }

    /// Read an item with full metadata for Phase 1.
    ///
    /// Returns: Option<(Item, last_committed_txn_timestamp, prepared_txn_id)>
    async fn read_item_with_metadata(
        &self,
        op: &TransactGetOp<'_>,
    ) -> Result<Option<(Item, i64, Option<uuid::Uuid>)>, StorageError> {
        let keyspace = self.account_keyspace(&op.key_info.account_id);
        let table = data_table_name(&op.key_info.table_id);
        let pk_text = composite_pk_to_text(op.key, &op.key_info.key_schema)?;
        let (sk, sk_col) = resolve_sk_get(op)?;

        let Some(row) = select_by_pk(
            &self.session,
            &keyspace,
            &table,
            "item_data, last_committed_txn_timestamp, prepared_txn_id",
            pk_text.as_str(),
            sk.as_ref(),
            sk_col.as_deref(),
        )
        .await?
        else {
            return Ok(None);
        };

        let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
        let Some(item_data) = item_data else {
            return Ok(None);
        };
        let item: Item =
            serde_json::from_str(&item_data).map_err(|e| StorageError::Internal(e.to_string()))?;
        let last_committed: Option<i64> = row
            .get_by_name("last_committed_txn_timestamp")
            .ok()
            .flatten();
        let prepared_txn_id: Option<uuid::Uuid> = row.get_by_name("prepared_txn_id").ok().flatten();
        Ok(Some((item, last_committed.unwrap_or(0), prepared_txn_id)))
    }

    async fn read_item_metadata_only(
        &self,
        op: &TransactGetOp<'_>,
    ) -> Result<Option<(i64, Option<uuid::Uuid>)>, StorageError> {
        let keyspace = self.account_keyspace(&op.key_info.account_id);
        let table = data_table_name(&op.key_info.table_id);
        let pk_text = composite_pk_to_text(op.key, &op.key_info.key_schema)?;
        let (sk, sk_col) = resolve_sk_get(op)?;

        let Some(row) = select_by_pk(
            &self.session,
            &keyspace,
            &table,
            "item_data, last_committed_txn_timestamp, prepared_txn_id",
            pk_text.as_str(),
            sk.as_ref(),
            sk_col.as_deref(),
        )
        .await?
        else {
            return Ok(None);
        };

        let last_committed: Option<i64> = row
            .get_by_name("last_committed_txn_timestamp")
            .ok()
            .flatten();
        let prepared_txn_id: Option<uuid::Uuid> = row.get_by_name("prepared_txn_id").ok().flatten();
        let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
        if item_data.is_none() && prepared_txn_id.is_none() {
            return Ok(None);
        }
        Ok(Some((last_committed.unwrap_or(0), prepared_txn_id)))
    }

    /// Implementation of `DataEngine::transact_write_items`.
    ///
    /// Implements a two-phase commit protocol using Lightweight Transactions (LWT):
    /// 1. PREPARE: Mark all items with transaction ID using LWT
    /// 2. COMMIT/ROLLBACK: Apply or revert changes atomically
    /// Implementation of TransactWriteItems.
    pub(crate) async fn transact_write_items_impl(
        &self,
        ops: &[TransactWriteOp<'_>],
        idempotency: Option<(&str, &str, &str)>,
    ) -> Result<(), StorageError> {
        // T4.1: Request parsing and ledger creation

        // Validate: all operations in same account (same keyspace)
        if ops.is_empty() {
            return Ok(());
        }

        let first_account = transact_write_op_account_id(&ops[0]);
        for op in &ops[1..] {
            if transact_write_op_account_id(op) != first_account {
                return Err(StorageError::Validation(
                    "Cross-account transactions not supported".to_owned(),
                ));
            }
        }

        let account_keyspace = self.account_keyspace(first_account);

        // Check the account-scoped idempotency token before creating the ledger.
        if let Some((idempotency_account, token, fingerprint)) = idempotency {
            if idempotency_account != first_account {
                return Err(StorageError::Validation(
                    "Idempotency account does not match transaction account".to_owned(),
                ));
            }
            self.check_idempotency_token(
                &self.catalog_keyspace(),
                idempotency_account,
                token,
                fingerprint,
            )
            .await?;
        }

        // Generate unique transaction ID
        let txn_id = Uuid::new_v4();
        let started_at = crate::cassandra_util::now_millis();

        // Build initial ledger ops (pk/sk known; item_data for UPDATE filled in after PREPARE)
        let mut ledger_ops = Self::initial_ledger_ops(ops)?;
        let initial_blob = serde_json::to_string(&ledger_ops)
            .map_err(|e| StorageError::Internal(format!("serialize ledger ops: {e}")))?;

        // Write ledger entry with state='preparing'
        self.write_ledger_entry(
            &account_keyspace,
            txn_id,
            TransactionState::Preparing,
            started_at,
            idempotency.map(|(_, token, _)| token),
            idempotency.map(|(_, _, fingerprint)| fingerprint),
            &initial_blob,
        )
        .await?;

        // T4.2: PREPARE phase - returns computed item_data for UPDATE ops
        let prepare_result = self.execute_prepare_phase(ops, txn_id, started_at).await;

        match prepare_result {
            Ok(computed_items) => {
                // Fill in item_data for UPDATE ops now that PREPARE has computed them
                for (ledger_op, computed) in ledger_ops.iter_mut().zip(computed_items.iter()) {
                    if let Some(data) = computed {
                        ledger_op.item_data = Some(data.clone());
                    }
                }
                // Update ledger blob with full data before transitioning to COMMITTING
                self.update_ledger_blob(&account_keyspace, txn_id, &ledger_ops)
                    .await?;
                // T4.3: COMMIT phase
                self.execute_commit_phase(&account_keyspace, ops, txn_id, started_at)
                    .await
            }
            Err(reasons) => {
                // T4.4: ROLLBACK phase
                tracing::debug!(
                    "transact_write: PREPARE failed ({} reasons), rolling back txn {txn_id}",
                    reasons.len()
                );
                self.execute_rollback_phase(&account_keyspace, ops, txn_id)
                    .await?;
                Err(StorageError::TransactionCanceled(reasons))
            }
        }
    }

    /// Execute PREPARE phase: validate conditions and mark items with transaction ID.
    ///
    /// Returns `Ok(computed_items)` where each entry is `Some(item_data_json)` for
    /// UPDATE ops (the post-mutation state) and `None` for all other op types.
    async fn execute_prepare_phase(
        &self,
        ops: &[TransactWriteOp<'_>],
        txn_id: Uuid,
        txn_timestamp: i64,
    ) -> Result<Vec<Option<String>>, Vec<CancellationReason>> {
        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        let mut computed: Vec<Option<String>> = Vec::with_capacity(ops.len());
        let mut any_failed = false;

        for op in ops {
            match self
                .prepare_single_operation(op, txn_id, txn_timestamp)
                .await
            {
                Ok(item_data) => {
                    reasons.push(CancellationReason::none());
                    computed.push(item_data);
                }
                Err(r) => {
                    any_failed = true;
                    reasons.push(r);
                    computed.push(None);
                }
            }
        }

        if any_failed {
            Err(reasons)
        } else {
            Ok(computed)
        }
    }

    /// Prepare a single transactional operation.
    ///
    /// Returns `Ok(Some(item_data_json))` for UPDATE (the post-mutation state),
    /// `Ok(None)` for PUT/DELETE/CHECK, or `Err(reason)` on failure.
    async fn prepare_single_operation(
        &self,
        op: &TransactWriteOp<'_>,
        txn_id: Uuid,
        txn_timestamp: i64,
    ) -> Result<Option<String>, CancellationReason> {
        match op {
            TransactWriteOp::Put {
                key_info,
                item,
                condition,
                maps,
                return_values_on_ccf,
                ..
            } => {
                // Validate item keys
                validation::validate_item_keys(
                    item,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

                // Read existing item
                let existing = self
                    .fetch_item_for_transaction(key_info, item)
                    .await
                    .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

                tracing::debug!(
                    "prepare Put: table={} pk_keys={:?} existing={}",
                    key_info.table_name,
                    item.keys().collect::<Vec<_>>(),
                    existing.is_some()
                );

                // Evaluate condition
                let empty = Item::new();
                eval_condition(
                    *condition,
                    existing.as_ref().unwrap_or(&empty),
                    maps,
                    *return_values_on_ccf,
                    existing.as_ref(),
                )?;

                // For new items (PUT), check partition_max_delete_timestamp
                if existing.is_none() {
                    self.check_partition_max_delete_timestamp(key_info, item, txn_timestamp)
                        .await?;
                }

                // Execute PREPARE
                self.prepare_item(key_info, item, txn_id, txn_timestamp, existing.is_none())
                    .await?;
                Ok(None)
            }
            TransactWriteOp::Delete {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf,
                ..
            } => {
                // Validate key
                validation::validate_key_only(
                    key,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

                // Read existing item
                let existing = self
                    .fetch_item_for_transaction(key_info, key)
                    .await
                    .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

                // Evaluate condition
                let empty = Item::new();
                eval_condition(
                    *condition,
                    existing.as_ref().unwrap_or(&empty),
                    maps,
                    *return_values_on_ccf,
                    existing.as_ref(),
                )?;

                // Execute PREPARE
                self.prepare_item(key_info, key, txn_id, txn_timestamp, false)
                    .await?;
                Ok(None)
            }
            TransactWriteOp::Update {
                key_info,
                key,
                actions,
                condition,
                maps,
                return_values_on_ccf,
                ..
            } => {
                // Validate key
                validation::validate_key_only(
                    key,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

                // Read existing item
                let existing = self
                    .fetch_item_for_transaction(key_info, key)
                    .await
                    .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

                // Create item from key if not exists
                let mut item = existing.clone().unwrap_or_else(|| (*key).clone());

                // Evaluate condition against empty item if non-existent
                let condition_item = if existing.is_some() {
                    &item
                } else {
                    &std::collections::BTreeMap::new()
                };
                eval_condition(
                    *condition,
                    condition_item,
                    maps,
                    *return_values_on_ccf,
                    existing.as_ref(),
                )?;

                // Apply update actions
                expression::apply_update_validated(actions, &mut item, maps, &[], &[])
                    .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

                // Validate item size
                const MAX_ITEM_SIZE_BYTES: usize = 400 * 1024;
                validation::validate_item_size(&item, MAX_ITEM_SIZE_BYTES)
                    .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

                // For new items, check partition_max_delete_timestamp
                if existing.is_none() {
                    self.check_partition_max_delete_timestamp(key_info, &item, txn_timestamp)
                        .await?;
                }

                // Execute PREPARE
                self.prepare_item(key_info, key, txn_id, txn_timestamp, existing.is_none())
                    .await?;
                // Return the computed final item so the caller can update the ledger blob
                let item_json = serde_json::to_string(&item)
                    .map_err(|e| CancellationReason::validation_error(e.to_string()))?;
                Ok(Some(item_json))
            }
            TransactWriteOp::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf,
            } => {
                // Validate key
                validation::validate_key_only(
                    key,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

                // Read existing item
                let existing = self
                    .fetch_item_for_transaction(key_info, key)
                    .await
                    .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

                // Evaluate condition
                let empty = Item::new();
                let check_against = existing.as_ref().unwrap_or(&empty);
                eval_condition(
                    Some(condition),
                    check_against,
                    maps,
                    *return_values_on_ccf,
                    existing.as_ref(),
                )?;

                // ConditionCheck doesn't prepare any item
                Ok(None)
            }
        }
    }

    /// Execute COMMIT phase: apply changes and update ledger.
    async fn execute_commit_phase(
        &self,
        account_keyspace: &str,
        ops: &[TransactWriteOp<'_>],
        txn_id: Uuid,
        txn_timestamp: i64,
    ) -> Result<(), StorageError> {
        // Update ledger state to 'committing'
        self.update_ledger_state(account_keyspace, txn_id, TransactionState::Committing)
            .await?;

        // Capture the pre-commit image of every item on a TTL-enabled table.
        // Reconciliation needs it to retire the expiration entry the item had
        // *before* this transaction: unlike an ordinary write, a transactional
        // write cannot carry the queue delete in the same batch as the base
        // mutation, so without this a changed or removed TTL leaves its old
        // entry behind until that entry's original due time.
        let mut pre_commit_images: Vec<Option<Item>> = Vec::with_capacity(ops.len());
        for op in ops {
            let image = match op {
                TransactWriteOp::Put { key_info, item, .. } => {
                    self.pre_commit_ttl_image(key_info, item).await?
                }
                TransactWriteOp::Update { key_info, key, .. }
                | TransactWriteOp::Delete { key_info, key, .. } => {
                    self.pre_commit_ttl_image(key_info, key).await?
                }
                TransactWriteOp::ConditionCheck { .. } => None,
            };
            pre_commit_images.push(image);
        }

        // TODO: Execute COMMIT operations for each item in parallel
        // For now, sequential execution
        for op in ops {
            self.commit_single_operation(op, txn_id, txn_timestamp)
                .await?;
        }

        for (op, old_image) in ops.iter().zip(&pre_commit_images) {
            match op {
                TransactWriteOp::Put { key_info, item, .. } => {
                    self.reconcile_ttl_transition(key_info, old_image.as_ref(), Some(item))
                        .await?;
                }
                TransactWriteOp::Update { key_info, key, .. } => {
                    let new_image = self.get_item_impl(key_info, key).await?;
                    self.reconcile_ttl_transition(key_info, old_image.as_ref(), new_image.as_ref())
                        .await?;
                }
                TransactWriteOp::Delete { key_info, .. } => {
                    self.reconcile_ttl_transition(key_info, old_image.as_ref(), None)
                        .await?;
                }
                TransactWriteOp::ConditionCheck { .. } => {}
            }
        }

        // Delete transaction from ledger
        self.delete_ledger_entry(account_keyspace, txn_id).await?;

        Ok(())
    }

    /// Execute ROLLBACK phase: clean up prepared items.
    async fn execute_rollback_phase(
        &self,
        account_keyspace: &str,
        ops: &[TransactWriteOp<'_>],
        txn_id: Uuid,
    ) -> Result<(), StorageError> {
        // Update ledger state to 'rollback' (uses CANCELLING state)
        self.update_ledger_state(account_keyspace, txn_id, TransactionState::Cancelling)
            .await?;

        // TODO: Execute ROLLBACK operations for each item in parallel
        // For now, sequential execution
        for op in ops {
            if let Err(e) = self.rollback_single_operation(op, txn_id).await {
                tracing::error!("execute_rollback_phase: rollback_single_operation failed: {e}");
                // Continue rolling back other ops even if one fails, then return the error.
                // For now, return immediately to preserve existing behaviour.
                return Err(e);
            }
        }

        // Delete transaction from ledger
        self.delete_ledger_entry(account_keyspace, txn_id).await?;

        Ok(())
    }
}

/// Evaluate a condition expression, returning a CancellationReason on failure.
fn eval_condition(
    condition: Option<&extenddb_core::expression::Expr>,
    item: &std::collections::BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
    return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
    existing: Option<&Item>,
) -> Result<(), CancellationReason> {
    if let Some(cond) = condition {
        let passed = expression::evaluate_condition(cond, item, maps)
            .map_err(|e| CancellationReason::validation_error(e.to_string()))?;
        if !passed {
            let item_to_return =
                if return_values_on_ccf == ReturnValuesOnConditionCheckFailure::AllOld {
                    existing.cloned()
                } else {
                    None
                };
            return Err(CancellationReason::condition_check_failed_with_item(
                item_to_return,
            ));
        }
    }
    Ok(())
}

/// Extract account_id from a TransactWriteOp.
fn transact_write_op_account_id<'a>(op: &'a TransactWriteOp<'_>) -> &'a str {
    match op {
        TransactWriteOp::Put { key_info, .. }
        | TransactWriteOp::Delete { key_info, .. }
        | TransactWriteOp::Update { key_info, .. }
        | TransactWriteOp::ConditionCheck { key_info, .. } => &key_info.account_id,
    }
}

impl CassandraEngine {
    /// Serialize TransactWriteOps to JSON for ledger storage.
    /// Build initial `LedgerOp`s from the request ops.
    ///
    /// Written to the ledger before PREPARE starts. Contains pk/sk so a crash
    /// during PREPARE can be rolled back. `item_data` for UPDATE is `None` here
    /// and filled in after PREPARE succeeds (before transitioning to COMMITTING).
    fn initial_ledger_ops(ops: &[TransactWriteOp<'_>]) -> Result<Vec<LedgerOp>, StorageError> {
        ops.iter()
            .map(|op| {
                let (op_type, key_info, key) = match op {
                    TransactWriteOp::Put { key_info, item, .. } => ("PUT", *key_info, *item),
                    TransactWriteOp::Delete { key_info, key, .. } => ("DELETE", *key_info, *key),
                    TransactWriteOp::Update { key_info, key, .. } => ("UPDATE", *key_info, *key),
                    TransactWriteOp::ConditionCheck { key_info, key, .. } => {
                        ("CHECK", *key_info, *key)
                    }
                };
                let pk = composite_pk_to_text(key, &key_info.key_schema)?;
                let (sk_col, sk_val) = if let Some((sk_name, sk_type)) =
                    sk_info(&key_info.key_schema, &key_info.attribute_definitions)
                {
                    let sk_value = key
                        .get(sk_name)
                        .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
                    let sk = parse_sk(sk_value, sk_type)?;
                    let col = sk_column(sk_type).to_owned();
                    // Store as the text representation used in Cassandra queries.
                    // sk_col ("sk_s"/"sk_n"/"sk_b") encodes the type; no separate type tag needed.
                    let val = match &sk {
                        SortKeyValue::S(s) => s.clone(),
                        SortKeyValue::N(n) => n.to_string(),
                        SortKeyValue::B(b) => {
                            use base64::Engine as _;
                            base64::engine::general_purpose::STANDARD.encode(b)
                        }
                    };
                    (Some(col), Some(val))
                } else {
                    (None, None)
                };
                // For PUT, item_data is known immediately.
                // For UPDATE, item_data is filled in after PREPARE (update_ledger_blob).
                // For DELETE and CHECK, item_data is never needed.
                let item_data = if op_type == "PUT" {
                    let item = match op {
                        TransactWriteOp::Put { item, .. } => *item,
                        _ => unreachable!(),
                    };
                    Some(
                        serde_json::to_string(item)
                            .map_err(|e| StorageError::Internal(e.to_string()))?,
                    )
                } else {
                    None
                };
                Ok(LedgerOp {
                    op: op_type.to_owned(),
                    table_id: key_info.table_id.clone(),
                    pk,
                    sk_col,
                    sk_val,
                    item_data,
                })
            })
            .collect()
    }

    /// Check and reserve an account-scoped idempotency token, scoped to `account_id`.
    async fn check_idempotency_token(
        &self,
        keyspace: &str,
        account_id: &str,
        token: &str,
        fingerprint: &str,
    ) -> Result<(), StorageError> {
        let select_query = format!(
            "SELECT fingerprint FROM {keyspace}.idempotency_tokens_by_account WHERE account_id = ? AND \"token\" = ?"
        );

        let row = query_optional::<StorageError>(
            &self.session,
            &select_query,
            cdrs_tokio::query_values!(account_id, token),
            "check_idempotency_token",
        )
        .await?;

        if let Some(row) = row {
            let stored_fp: String = get_column(&row, "fingerprint", "check_idempotency_token")?;
            return if stored_fp == fingerprint {
                Err(StorageError::IdempotentReplay)
            } else {
                Err(StorageError::IdempotentMismatch)
            };
        }

        // Insert with LWT to handle concurrent requests racing on the same token.
        let insert_query = format!(
            "INSERT INTO {keyspace}.idempotency_tokens_by_account (account_id, \"token\", fingerprint, created_at) \
             VALUES (?, ?, ?, ?) IF NOT EXISTS"
        );
        let now = crate::cassandra_util::now_millis();

        let result = self
            .session
            .query_with_values(
                &insert_query,
                cdrs_tokio::query_values!(account_id, token, fingerprint, now),
            )
            .await
            .map_err(|e| {
                tracing::error!("check_idempotency_token insert: {e}");
                StorageError::Internal("Database error".to_owned())
            })?;

        // If the LWT was not applied, a concurrent request won the race.
        // Read back what was stored and return the appropriate error.
        use cdrs_tokio::types::IntoRustByName;
        let applied: bool = result
            .response_body()
            .ok()
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .and_then(|mut rows| rows.drain(..).next())
            .and_then(|row| row.get_r_by_name("[applied]").ok())
            .unwrap_or(true);

        if !applied {
            let row = query_optional::<StorageError>(
                &self.session,
                &select_query,
                cdrs_tokio::query_values!(account_id, token),
                "check_idempotency_token recheck",
            )
            .await?;
            if let Some(row) = row {
                let stored_fp: String =
                    get_column(&row, "fingerprint", "check_idempotency_token recheck")?;
                return if stored_fp == fingerprint {
                    Err(StorageError::IdempotentReplay)
                } else {
                    Err(StorageError::IdempotentMismatch)
                };
            }
        }

        Ok(())
    }

    /// Fetch an item for transaction (reads item_data and prepared_txn_id).
    async fn fetch_item_for_transaction(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let keyspace = self.account_keyspace(&key_info.account_id);
        let table = data_table_name(&key_info.table_id);
        let pk_text = composite_pk_to_text(key, &key_info.key_schema)?;
        let (sk, sk_col) = resolve_sk(key_info, key)?;

        let Some(row) = select_by_pk(
            &self.session,
            &keyspace,
            &table,
            "item_data",
            pk_text.as_str(),
            sk.as_ref(),
            sk_col.as_deref(),
        )
        .await?
        else {
            return Ok(None);
        };

        let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
        let Some(item_data) = item_data else {
            return Ok(None);
        };
        Ok(Some(
            serde_json::from_str(&item_data).map_err(|e| StorageError::Internal(e.to_string()))?,
        ))
    }

    /// Check partition_max_delete_timestamp for new items.
    async fn check_partition_max_delete_timestamp(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        txn_timestamp: i64,
    ) -> Result<(), CancellationReason> {
        let account_keyspace = self.account_keyspace(&key_info.account_id);
        let table_name = data_table_name(&key_info.table_id);

        let pk_text = composite_pk_to_text(key, &key_info.key_schema)
            .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

        let query = format!(
            "SELECT partition_max_delete_timestamp FROM {account_keyspace}.{table_name} WHERE pk = ? LIMIT 1"
        );

        let result = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(pk_text.as_str()))
            .await
            .map_err(|e| {
                tracing::error!("check_partition_max_delete_timestamp: {e}");
                CancellationReason::validation_error("Database error".to_owned())
            })?;

        let body = result.response_body().map_err(|e| {
            tracing::error!("check_partition_max_delete_timestamp response_body: {e}");
            CancellationReason::validation_error("Database error".to_owned())
        })?;

        let rows = body.into_rows().unwrap_or_default();
        if let Some(row) = rows.into_iter().next() {
            let max_ts: Option<i64> = row
                .get_by_name("partition_max_delete_timestamp")
                .ok()
                .flatten();
            if let Some(max_ts) = max_ts
                && txn_timestamp <= max_ts
            {
                return Err(CancellationReason::validation_error(
                    "Item was deleted at a later timestamp".to_owned(),
                ));
            }
        }

        Ok(())
    }

    /// PREPARE an item: mark with transaction ID using LWT.
    ///
    /// For existing items: UPDATE with IF prepared_txn_id = null
    /// For new items: INSERT with IF NOT EXISTS + created_to_prepare=true
    async fn prepare_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        txn_id: Uuid,
        txn_timestamp: i64,
        is_new_item: bool,
    ) -> Result<(), CancellationReason> {
        use cdrs_tokio::types::value::Bytes;

        let keyspace = self.account_keyspace(&key_info.account_id);
        let ddb_table = super::ddl::data_table_name(&key_info.table_id);
        let pk_text = composite_pk_to_text(key, &key_info.key_schema)
            .map_err(|e| CancellationReason::validation_error(e.to_string()))?;

        let txn_id_bytes = Bytes::new(txn_id.as_bytes().to_vec());

        let result = if let Some((sk_name, sk_type)) = sk_info(&key_info.key_schema, &key_info.attribute_definitions) {
            let sk_value = key.get(sk_name).ok_or_else(|| {
                CancellationReason::validation_error("missing sort key".to_owned())
            })?;
            let sk = parse_sk(sk_value, sk_type)
                .map_err(|e| CancellationReason::validation_error(e.to_string()))?;
            let sk_col = sk_column(sk_type);

            if is_new_item {
                let item_text = serde_json::to_value(key)
                    .map_err(|e| CancellationReason::validation_error(e.to_string()))?
                    .to_string();
                let query = format!(
                    "INSERT INTO {keyspace}.{ddb_table} (pk, {sk_col}, item_data, prepared_txn_id, prepared_txn_timestamp, created_to_prepare) \
                     VALUES (?, ?, ?, ?, ?, true) IF NOT EXISTS"
                );
                query_with_pk_sk_item_txnid_ts(&self.session, &query, pk_text.as_str(), &sk, &item_text, txn_id_bytes, txn_timestamp).await
            } else {
                let query = format!(
                    "UPDATE {keyspace}.{ddb_table} SET prepared_txn_id = ?, prepared_txn_timestamp = ? \
                     WHERE pk = ? AND {sk_col} = ? IF prepared_txn_id = null"
                );
                query_with_txnid_ts_pk_sk(&self.session, &query, txn_id_bytes, txn_timestamp, pk_text.as_str(), &sk).await
            }
        } else if is_new_item {
            let item_text = serde_json::to_value(key)
                .map_err(|e| CancellationReason::validation_error(e.to_string()))?
                .to_string();
            let query = format!(
                "INSERT INTO {keyspace}.{ddb_table} (pk, item_data, prepared_txn_id, prepared_txn_timestamp, created_to_prepare) \
                 VALUES (?, ?, ?, ?, true) IF NOT EXISTS"
            );
            self.session
                .query_with_values(&query, cdrs_tokio::query_values!(pk_text.as_str(), item_text, txn_id_bytes, txn_timestamp))
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))
        } else {
            let query = format!(
                "UPDATE {keyspace}.{ddb_table} SET prepared_txn_id = ?, prepared_txn_timestamp = ? \
                 WHERE pk = ? IF prepared_txn_id = null"
            );
            self.session
                .query_with_values(&query, cdrs_tokio::query_values!(txn_id_bytes, txn_timestamp, pk_text.as_str()))
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))
        }
        .map_err(|e| {
            tracing::error!("prepare_item: {e}");
            CancellationReason::validation_error("Database error".to_owned())
        })?;

        check_lwt_applied(&result, "prepare_item")?;
        Ok(())
    }

    /// COMMIT a single operation: apply changes.
    ///
    /// For Put/Update: Write final item_data, clear prepared_txn_id, set last_committed_txn_timestamp
    /// For Delete: Update partition_max_delete_timestamp, then delete the item
    async fn commit_single_operation(
        &self,
        op: &TransactWriteOp<'_>,
        txn_id: Uuid,
        txn_timestamp: i64,
    ) -> Result<(), StorageError> {
        use cdrs_tokio::types::value::Bytes;

        let txn_id_bytes = Bytes::new(txn_id.as_bytes().to_vec());

        match op {
            TransactWriteOp::Put { key_info, item, .. } => {
                self.commit_put_or_update(key_info, item, txn_id_bytes.clone(), txn_timestamp)
                    .await
            }
            TransactWriteOp::Update {
                key_info,
                key,
                actions,
                maps,
                ..
            } => {
                // Re-fetch and re-apply update (idempotent)
                let existing = self.fetch_item_for_transaction(key_info, key).await?;
                let mut final_item = existing.unwrap_or_else(|| (*key).clone());
                expression::apply_update_validated(actions, &mut final_item, maps, &[], &[])
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                self.commit_put_or_update(key_info, &final_item, txn_id_bytes, txn_timestamp)
                    .await
            }
            TransactWriteOp::Delete { key_info, key, .. } => {
                let keyspace = self.account_keyspace(&key_info.account_id);
                let ddb_table = super::ddl::data_table_name(&key_info.table_id);
                let pk_text = composite_pk_to_text(key, &key_info.key_schema)?;

                // Step 1: Update partition_max_delete_timestamp
                // First, try to update if it's null (most common case - first delete in partition)
                let update_max_ts_null_query = format!(
                    "UPDATE {keyspace}.{ddb_table} SET partition_max_delete_timestamp = ? WHERE pk = ? \
                     IF partition_max_delete_timestamp = null"
                );

                let result = self
                    .session
                    .query_with_values(
                        &update_max_ts_null_query,
                        cdrs_tokio::query_values!(txn_timestamp, pk_text.as_str()),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            "commit_single_operation (delete update partition_max): {e}"
                        );
                        StorageError::Internal("Database error".to_owned())
                    })?;

                // If that failed (column already has a value), try updating only if our timestamp is higher
                if !check_lwt_applied(&result, "partition_max update").is_ok() {
                    let update_max_ts_query = format!(
                        "UPDATE {keyspace}.{ddb_table} SET partition_max_delete_timestamp = ? WHERE pk = ? \
                         IF partition_max_delete_timestamp < ?"
                    );

                    let _result2 = self
                        .session
                        .query_with_values(
                            &update_max_ts_query,
                            cdrs_tokio::query_values!(
                                txn_timestamp,
                                pk_text.as_str(),
                                txn_timestamp
                            ),
                        )
                        .await
                        .map_err(|e| {
                            tracing::error!(
                                "commit_single_operation (delete update partition_max compare): {e}"
                            );
                            StorageError::Internal("Database error".to_owned())
                        })?;
                    // We don't check LWT here - if another transaction set a higher timestamp, that's fine
                }

                // Step 2: Delete the item with LWT check
                let result = if let Some((sk_name, sk_type)) =
                    sk_info(&key_info.key_schema, &key_info.attribute_definitions)
                {
                    let sk_value = key
                        .get(sk_name)
                        .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
                    let sk = parse_sk(sk_value, sk_type)?;
                    let sk_col = sk_column(sk_type);
                    let delete_query = format!(
                        "DELETE FROM {keyspace}.{ddb_table} WHERE pk = ? AND {sk_col} = ? IF prepared_txn_id = ?"
                    );
                    query_with_pk_sk_txnid(
                        &self.session,
                        &delete_query,
                        pk_text.as_str(),
                        &sk,
                        txn_id_bytes,
                    )
                    .await
                } else {
                    let delete_query = format!(
                        "DELETE FROM {keyspace}.{ddb_table} WHERE pk = ? IF prepared_txn_id = ?"
                    );
                    self.session
                        .query_with_values(
                            &delete_query,
                            cdrs_tokio::query_values!(pk_text.as_str(), txn_id_bytes),
                        )
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))
                }
                .map_err(|e| {
                    tracing::error!("commit_single_operation (delete): {e}");
                    StorageError::Internal("Database error".to_owned())
                })?;

                check_lwt_applied(&result, "commit_single_operation").map_err(|_| {
                    StorageError::Internal("Transaction conflict during commit".to_owned())
                })?;
                Ok(())
            }
            TransactWriteOp::ConditionCheck { .. } => Ok(()),
        }
    }

    /// Helper to commit a Put or Update operation.
    async fn commit_put_or_update(
        &self,
        key_info: &TableKeyInfo,
        final_item: &Item,
        txn_id_bytes: Bytes,
        txn_timestamp: i64,
    ) -> Result<(), StorageError> {
        let keyspace = self.account_keyspace(&key_info.account_id);
        let ddb_table = super::ddl::data_table_name(&key_info.table_id);
        let pk_text = composite_pk_to_text(final_item, &key_info.key_schema)?;

        let item_json =
            serde_json::to_value(final_item).map_err(|e| StorageError::Internal(e.to_string()))?;
        let item_text = item_json.to_string();

        let result = if let Some((sk_name, sk_type)) = sk_info(&key_info.key_schema, &key_info.attribute_definitions) {
            let sk_value = final_item.get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            let query = format!(
                "UPDATE {keyspace}.{ddb_table} SET item_data = ?, prepared_txn_id = NULL, version = version + 1, last_committed_txn_timestamp = ? \
                 WHERE pk = ? AND {sk_col} = ? IF prepared_txn_id = ?"
            );
            query_with_item_ts_pk_sk_txnid(&self.session, &query, &item_text, txn_timestamp, pk_text.as_str(), &sk, txn_id_bytes).await
        } else {
            let query = format!(
                "UPDATE {keyspace}.{ddb_table} SET item_data = ?, prepared_txn_id = NULL, version = version + 1, last_committed_txn_timestamp = ? \
                 WHERE pk = ? IF prepared_txn_id = ?"
            );
            self.session
                .query_with_values(&query, cdrs_tokio::query_values!(item_text, txn_timestamp, pk_text.as_str(), txn_id_bytes))
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))
        }
        .map_err(|e| {
            tracing::error!("commit_put_or_update: {e}");
            StorageError::Internal("Database error".to_owned())
        })?;

        check_lwt_applied(&result, "commit_put_or_update")
            .map_err(|_| StorageError::Internal("Transaction conflict during commit".to_owned()))?;
        Ok(())
    }

    /// ROLLBACK a single operation: clean up prepared state.
    ///
    /// For items created during PREPARE (created_to_prepare=true): DELETE the item
    /// For existing items: Clear prepared_txn_id to restore unprepared state
    async fn rollback_single_operation(
        &self,
        op: &TransactWriteOp<'_>,
        txn_id: Uuid,
    ) -> Result<(), StorageError> {
        use cdrs_tokio::types::value::Bytes;

        // ConditionCheck doesn't prepare anything, so nothing to rollback
        if matches!(op, TransactWriteOp::ConditionCheck { .. }) {
            return Ok(());
        }

        let key_info = match op {
            TransactWriteOp::Put { key_info, .. }
            | TransactWriteOp::Delete { key_info, .. }
            | TransactWriteOp::Update { key_info, .. }
            | TransactWriteOp::ConditionCheck { key_info, .. } => key_info,
        };

        let key = match op {
            TransactWriteOp::Put { item, .. } => *item,
            TransactWriteOp::Delete { key, .. } | TransactWriteOp::Update { key, .. } => *key,
            TransactWriteOp::ConditionCheck { key, .. } => *key,
        };

        let keyspace = self.account_keyspace(&key_info.account_id);
        let ddb_table = super::ddl::data_table_name(&key_info.table_id);
        let pk_text = composite_pk_to_text(key, &key_info.key_schema)?;
        let txn_id_bytes = Bytes::new(txn_id.as_bytes().to_vec());

        // We need to check if this item was created during PREPARE (created_to_prepare=true)
        // or if it was an existing item. Fetch the item to check.
        let existing = self.fetch_item_for_transaction(key_info, key).await?;

        // If item doesn't exist, it's already been cleaned up (idempotent)
        if existing.is_none() {
            return Ok(());
        }

        // Try DELETE first (for items created during PREPARE with created_to_prepare=true).
        // If that fails, fall back to clearing prepared_txn_id (for pre-existing items).
        let delete_result = if let Some((sk_name, sk_type)) = sk_info(&key_info.key_schema, &key_info.attribute_definitions) {
            let sk_value = key.get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            let delete_query = format!(
                "DELETE FROM {keyspace}.{ddb_table} WHERE pk = ? AND {sk_col} = ? IF prepared_txn_id = ? AND created_to_prepare = true"
            );
            let update_query = format!(
                "UPDATE {keyspace}.{ddb_table} SET prepared_txn_id = null WHERE pk = ? AND {sk_col} = ? IF prepared_txn_id = ?"
            );

            let dr = query_with_pk_sk_txnid(&self.session, &delete_query, pk_text.as_str(), &sk, txn_id_bytes.clone())
                .await
                .map_err(|e| { tracing::error!("rollback delete: {e}"); e })?;
            if check_lwt_applied(&dr, "rollback delete").is_ok() {
                return Ok(());
            }
            query_with_pk_sk_txnid(&self.session, &update_query, pk_text.as_str(), &sk, txn_id_bytes).await
        } else {
            let delete_query = format!(
                "DELETE FROM {keyspace}.{ddb_table} WHERE pk = ? IF prepared_txn_id = ? AND created_to_prepare = true"
            );
            let update_query = format!(
                "UPDATE {keyspace}.{ddb_table} SET prepared_txn_id = null WHERE pk = ? IF prepared_txn_id = ?"
            );

            let dr = self.session
                .query_with_values(&delete_query, cdrs_tokio::query_values!(pk_text.as_str(), txn_id_bytes.clone()))
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))
                .map_err(|e| { tracing::error!("rollback delete: {e}"); e })?;
            if check_lwt_applied(&dr, "rollback delete").is_ok() {
                return Ok(());
            }
            self.session
                .query_with_values(&update_query, cdrs_tokio::query_values!(pk_text.as_str(), txn_id_bytes))
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))
        }
        .map_err(|e| {
            tracing::error!("rollback_single_operation: {e}");
            StorageError::Internal("Database error".to_owned())
        })?;

        check_lwt_applied(&delete_result, "rollback update").map_err(|_| {
            StorageError::Internal("Transaction conflict during rollback".to_owned())
        })?;
        Ok(())
    }

    /// Recover a single stale transaction found by the recovery worker.
    ///
    /// Reads the ledger entry, parses `LedgerOp`s, then resumes COMMIT or
    /// executes ROLLBACK depending on the recorded state.  Errors during
    /// individual item operations are logged but do not abort recovery of the
    /// remaining items; the ledger entry is deleted only when all items have
    /// been processed successfully.
    pub(crate) async fn recover_transaction(
        &self,
        keyspace: &str,
        txn_id: Uuid,
    ) -> Result<(), StorageError> {
        let entry = match self.read_ledger_entry(keyspace, txn_id).await? {
            Some(e) => e,
            None => return Ok(()), // already cleaned up
        };

        let ops = entry.parse_ops()?;
        let state = TransactionState::from_str(&entry.state);
        let txn_timestamp = entry.started_at;

        match state {
            Some(TransactionState::Committing) => {
                for op in &ops {
                    if let Err(e) = self
                        .recover_commit_op(keyspace, op, txn_id, txn_timestamp)
                        .await
                    {
                        tracing::error!("recover_transaction commit op {txn_id}: {e}");
                        return Err(e);
                    }
                }
                for op in &ops {
                    if matches!(op.op.as_str(), "PUT" | "UPDATE")
                        && let Some(item_data) = op.item_data.as_deref()
                    {
                        // Recovery can only re-register the committed image:
                        // the ledger does not persist the pre-commit image,
                        // so a transaction that crashes between COMMIT and
                        // reconciliation can leave the item's previous
                        // expiration entry in the queue. That entry is
                        // harmless — the worker revalidates the item before
                        // deleting anything and retires the entry when it
                        // comes due — but it is queue garbage until then.
                        self.reconcile_ttl_item_by_table_id(&op.table_id, item_data)
                            .await?;
                    }
                }
            }
            // PREPARING or CANCELLING (or unknown) → rollback
            _ => {
                for op in &ops {
                    if let Err(e) = self.recover_rollback_op(keyspace, op, txn_id).await {
                        tracing::error!("recover_transaction rollback op {txn_id}: {e}");
                        return Err(e);
                    }
                }
            }
        }

        self.delete_ledger_entry(keyspace, txn_id).await
    }

    /// Commit a single operation during recovery.
    ///
    /// PUT/UPDATE: write `item_data`, clear `prepared_txn_id`, set `last_committed_txn_timestamp`.
    /// DELETE: update `partition_max_delete_timestamp`, then delete the row.
    /// CHECK: no-op.
    async fn recover_commit_op(
        &self,
        keyspace: &str,
        op: &crate::data::transaction_ledger::LedgerOp,
        txn_id: Uuid,
        txn_timestamp: i64,
    ) -> Result<(), StorageError> {
        if op.op == "CHECK" {
            return Ok(());
        }

        let table = data_table_name(&op.table_id);
        let txn_id_bytes = Bytes::new(txn_id.as_bytes().to_vec());
        let sk = ledger_sk(op)?;

        match op.op.as_str() {
            "PUT" | "UPDATE" => {
                let item_data = op.item_data.as_deref().ok_or_else(|| {
                    StorageError::Internal(format!(
                        "ledger op {} missing item_data for {}",
                        op.op, txn_id
                    ))
                })?;

                let result = if let Some((sk_val, sk_col)) = &sk {
                    let query = format!(
                        "UPDATE {keyspace}.{table} SET item_data = ?, prepared_txn_id = NULL, \
                         version = version + 1, last_committed_txn_timestamp = ? \
                         WHERE pk = ? AND {sk_col} = ? IF prepared_txn_id = ?",
                    );
                    query_with_item_ts_pk_sk_txnid(
                        &self.session,
                        &query,
                        item_data,
                        txn_timestamp,
                        &op.pk,
                        sk_val,
                        txn_id_bytes,
                    )
                    .await
                } else {
                    let query = format!(
                        "UPDATE {keyspace}.{table} SET item_data = ?, prepared_txn_id = NULL, \
                         version = version + 1, last_committed_txn_timestamp = ? \
                         WHERE pk = ? IF prepared_txn_id = ?",
                    );
                    self.session
                        .query_with_values(
                            &query,
                            cdrs_tokio::query_values!(
                                item_data,
                                txn_timestamp,
                                op.pk.as_str(),
                                txn_id_bytes
                            ),
                        )
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))
                }
                .map_err(|e| {
                    tracing::error!("recover_commit_op PUT/UPDATE: {e}");
                    StorageError::Internal("Database error".to_owned())
                })?;

                // LWT may not apply if item was already committed (idempotent)
                let _ = check_lwt_applied(&result, "recover_commit PUT/UPDATE");
                Ok(())
            }
            "DELETE" => {
                // Step 1: update partition_max_delete_timestamp
                let update_null = format!(
                    "UPDATE {keyspace}.{table} SET partition_max_delete_timestamp = ? \
                     WHERE pk = ? IF partition_max_delete_timestamp = null",
                );
                let r1 = self
                    .session
                    .query_with_values(
                        &update_null,
                        cdrs_tokio::query_values!(txn_timestamp, op.pk.as_str()),
                    )
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))
                    .map_err(|e| {
                        tracing::error!("recover_commit_op delete max_ts null: {e}");
                        e
                    })?;

                if check_lwt_applied(&r1, "recover_commit delete max_ts null").is_err() {
                    let update_cmp = format!(
                        "UPDATE {keyspace}.{table} SET partition_max_delete_timestamp = ? \
                         WHERE pk = ? IF partition_max_delete_timestamp < ?",
                    );
                    let _ = self
                        .session
                        .query_with_values(
                            &update_cmp,
                            cdrs_tokio::query_values!(txn_timestamp, op.pk.as_str(), txn_timestamp),
                        )
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))
                        .map_err(|e| {
                            tracing::error!("recover_commit_op delete max_ts cmp: {e}");
                            e
                        })?;
                }

                // Step 2: delete the row
                let result = if let Some((sk_val, sk_col)) = &sk {
                    let query = format!(
                        "DELETE FROM {keyspace}.{table} WHERE pk = ? AND {sk_col} = ? \
                         IF prepared_txn_id = ?",
                    );
                    query_with_pk_sk_txnid(&self.session, &query, &op.pk, sk_val, txn_id_bytes)
                        .await
                } else {
                    let query = format!(
                        "DELETE FROM {keyspace}.{table} WHERE pk = ? IF prepared_txn_id = ?",
                    );
                    self.session
                        .query_with_values(
                            &query,
                            cdrs_tokio::query_values!(op.pk.as_str(), txn_id_bytes),
                        )
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))
                }
                .map_err(|e| {
                    tracing::error!("recover_commit_op DELETE: {e}");
                    StorageError::Internal("Database error".to_owned())
                })?;

                let _ = check_lwt_applied(&result, "recover_commit DELETE");
                Ok(())
            }
            other => Err(StorageError::Internal(format!(
                "unknown op in ledger: {other}"
            ))),
        }
    }

    /// Rollback a single operation during recovery.
    ///
    /// Tries DELETE with `IF prepared_txn_id = ? AND created_to_prepare = true` first.
    /// Falls back to clearing `prepared_txn_id` for pre-existing items.
    /// CHECK: no-op.
    async fn recover_rollback_op(
        &self,
        keyspace: &str,
        op: &crate::data::transaction_ledger::LedgerOp,
        txn_id: Uuid,
    ) -> Result<(), StorageError> {
        if op.op == "CHECK" {
            return Ok(());
        }

        let table = data_table_name(&op.table_id);
        let txn_id_bytes = Bytes::new(txn_id.as_bytes().to_vec());
        let sk = ledger_sk(op)?;

        let (delete_result, update_query) = if let Some((sk_val, sk_col)) = &sk {
            let dq = format!(
                "DELETE FROM {keyspace}.{table} WHERE pk = ? AND {sk_col} = ? \
                 IF prepared_txn_id = ? AND created_to_prepare = true",
            );
            let uq = format!(
                "UPDATE {keyspace}.{table} SET prepared_txn_id = null \
                 WHERE pk = ? AND {sk_col} = ? IF prepared_txn_id = ?",
            );
            let dr =
                query_with_pk_sk_txnid(&self.session, &dq, &op.pk, sk_val, txn_id_bytes.clone())
                    .await
                    .map_err(|e| {
                        tracing::error!("recover_rollback_op delete: {e}");
                        StorageError::Internal("Database error".to_owned())
                    })?;
            (dr, uq)
        } else {
            let dq = format!(
                "DELETE FROM {keyspace}.{table} WHERE pk = ? \
                 IF prepared_txn_id = ? AND created_to_prepare = true",
            );
            let uq = format!(
                "UPDATE {keyspace}.{table} SET prepared_txn_id = null \
                 WHERE pk = ? IF prepared_txn_id = ?",
            );
            let dr = self
                .session
                .query_with_values(
                    &dq,
                    cdrs_tokio::query_values!(op.pk.as_str(), txn_id_bytes.clone()),
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))
                .map_err(|e| {
                    tracing::error!("recover_rollback_op delete: {e}");
                    e
                })?;
            (dr, uq)
        };

        if check_lwt_applied(&delete_result, "recover_rollback delete").is_ok() {
            return Ok(());
        }

        // Item was pre-existing - clear the transaction marker
        let result = if let Some((sk_val, _sk_col)) = &sk {
            query_with_pk_sk_txnid(&self.session, &update_query, &op.pk, sk_val, txn_id_bytes).await
        } else {
            self.session
                .query_with_values(
                    &update_query,
                    cdrs_tokio::query_values!(op.pk.as_str(), txn_id_bytes),
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))
        }
        .map_err(|e| {
            tracing::error!("recover_rollback_op update: {e}");
            StorageError::Internal("Database error".to_owned())
        })?;

        let _ = check_lwt_applied(&result, "recover_rollback update");
        Ok(())
    }
}

/// Resolve sort key value and column name from key_info + item.
fn resolve_sk(
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<(Option<SortKeyValue>, Option<String>), StorageError> {
    if let Some((sk_name, sk_type)) = sk_info(&key_info.key_schema, &key_info.attribute_definitions)
    {
        let sk_value = key
            .get(sk_name)
            .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
        let sk = parse_sk(sk_value, sk_type)?;
        Ok((Some(sk), Some(sk_column(sk_type).to_owned())))
    } else {
        Ok((None, None))
    }
}

/// Resolve sort key value and column name from a TransactGetOp.
fn resolve_sk_get(
    op: &TransactGetOp<'_>,
) -> Result<(Option<SortKeyValue>, Option<String>), StorageError> {
    resolve_sk(op.key_info, op.key)
}

/// Check if LWT was applied successfully.
fn check_lwt_applied(result: &Envelope, context: &str) -> Result<(), CancellationReason> {
    let body = result.response_body().map_err(|e| {
        tracing::error!("{} response_body: {e}", context);
        CancellationReason::validation_error("Database error".to_owned())
    })?;

    if let Some(rows) = body.into_rows()
        && let Some(row) = rows.first()
    {
        let applied: bool = get_column::<bool, StorageError>(row, "[applied]", context)
            .map_err(|_| transaction_conflict_reason())?;
        if !applied {
            return Err(transaction_conflict_reason());
        }
    }

    Ok(())
}

/// Extract sort key from a `LedgerOp` as `Option<(SortKeyValue, col_name)>`.
fn ledger_sk(
    op: &crate::data::transaction_ledger::LedgerOp,
) -> Result<Option<(SortKeyValue, String)>, StorageError> {
    match (&op.sk_col, &op.sk_val) {
        (Some(col), Some(val)) => Ok(Some((ledger_sk_to_sort_key(col, val)?, col.clone()))),
        _ => Ok(None),
    }
}

/// Reconstruct a `SortKeyValue` from the text representation stored in `LedgerOp`.
///
/// `sk_col` encodes the type ("sk_s" → S, "sk_n" → N, "sk_b" → B).
fn ledger_sk_to_sort_key(sk_col: &str, sk_val: &str) -> Result<SortKeyValue, StorageError> {
    match sk_col {
        "sk_s" => Ok(SortKeyValue::S(sk_val.to_owned())),
        "sk_n" => {
            let d = sk_val.parse::<bigdecimal::BigDecimal>().map_err(|e| {
                StorageError::Internal(format!("invalid numeric sk in ledger: {e}"))
            })?;
            Ok(SortKeyValue::N(d))
        }
        "sk_b" => {
            use base64::Engine as _;
            let b = base64::engine::general_purpose::STANDARD
                .decode(sk_val)
                .map_err(|e| StorageError::Internal(format!("invalid binary sk in ledger: {e}")))?;
            Ok(SortKeyValue::B(b))
        }
        other => Err(StorageError::Internal(format!(
            "unknown sk_col in ledger: {other}"
        ))),
    }
}
