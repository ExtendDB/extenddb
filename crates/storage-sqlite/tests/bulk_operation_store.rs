// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The bulk operation store contract, run against the `SQLite` backend.
//!
//! The cases live in `extenddb_storage::bulk_operation_store_contract` and are
//! shared with the `PostgreSQL` suite; this file only builds the engine. Each
//! test gets its own database file in the system temporary directory so cases
//! cannot see each other's rows and the concurrent cases contend on a real
//! file. A file rather than `:memory:` because the in-memory database pins the
//! pool to one connection, and these tests exist to race transactions. The
//! file is removed when the test passes and left behind, named `eddb_bulk_*`,
//! when it fails.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use extenddb_storage::bulk_operation_store_contract::{self as contract, Fixture};
use extenddb_storage::management_store::{ManagementStore, SettingsStore};
use extenddb_storage_sqlite::{SqliteCatalogStore, SqliteEngine};

const ACCOUNT_A: &str = "111111111111";
const ACCOUNT_B: &str = "222222222222";
const REGION: &str = "us-east-1";

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

/// A fresh file-backed engine with the schema applied, the control-plane
/// delay pinned to zero (no workers run here, so a scheduled CREATING to
/// ACTIVE transition would never happen), and both contract accounts seeded.
async fn fixture() -> (Fixture<SqliteEngine>, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "eddb_bulk_{}_{}.sqlite",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    let path_str = path.to_str().expect("a UTF-8 temporary path").to_owned();

    extenddb_storage_sqlite::migrations::migrate(&path_str, "test", std::time::Duration::ZERO)
        .await
        .expect("apply the migrations to the fresh database");

    // Seed through the catalog store, the same surface the management API
    // uses, then release its pool before the engine opens its own.
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{path_str}?mode=rwc"))
        .await
        .expect("open a seeding pool");
    let catalog = SqliteCatalogStore::new(pool.clone());
    catalog
        .set_setting("control_plane_delay_seconds", "0")
        .await
        .expect("pin the control-plane delay to zero");
    for (id, name) in [(ACCOUNT_A, "contract-a"), (ACCOUNT_B, "contract-b")] {
        catalog
            .create_account(id, name)
            .await
            .expect("seed a contract account");
    }
    pool.close().await;

    let engine = SqliteEngine::new(&path_str, 4, REGION, 409_600)
        .await
        .expect("open the engine on the seeded database");

    (
        Fixture {
            engine: Arc::new(engine),
            account_a: ACCOUNT_A.to_owned(),
            account_b: ACCOUNT_B.to_owned(),
            region: REGION.to_owned(),
        },
        path,
    )
}

/// Remove the database file, its WAL/SHM sidecars, and the engine's lock
/// file. Reached only when the case passed; a panic skips this and leaves
/// the file for inspection.
fn cleanup(fx: Fixture<SqliteEngine>, path: &std::path::Path) {
    drop(fx);
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(sidecar));
    }
}

macro_rules! contract_case {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            let (fx, path) = fixture().await;
            contract::$name(&fx).await;
            cleanup(fx, &path);
        }
    )*};
}

contract_case!(
    // The start transaction.
    second_start_on_a_busy_table_is_refused,
    the_table_guard_answers_before_the_account_limit,
    twenty_concurrent_starts_with_distinct_tokens_admit_exactly_one,
    twenty_concurrent_starts_with_one_token_create_once_and_replay_the_rest,
    a_table_is_startable_again_after_its_operation_finishes,
    the_per_table_non_terminal_count_follows_the_operation,
    a_reused_token_with_different_parameters_is_refused_and_creates_nothing,
    a_reused_token_replays_even_when_terminal_and_skips_the_table_guard,
    a_token_replays_inside_the_window_and_starts_anew_past_it,
    tokenless_starts_do_not_collide_on_the_token_index,
    sixty_concurrent_starts_against_a_limit_of_fifty_admit_exactly_fifty,
    // A start and an index creation that cross.
    a_start_and_an_index_creation_that_cross_are_never_both_accepted,
    a_refused_index_creation_does_not_refuse_a_start_that_crossed_it,
    an_accepted_index_creation_refuses_a_start_called_before_it,
    starts_beyond_the_pool_behind_an_index_creation_all_answer,
    a_stream_of_starts_does_not_hold_off_delete_table,
    // A start whose table was removed after the handler's lookup.
    a_start_on_a_table_removed_after_the_lookup_is_refused,
    // Rows born terminal (initial failure).
    an_initial_failure_starts_terminal_and_is_never_claimed,
    an_initial_failure_does_not_count_as_non_terminal,
    a_table_with_an_initial_failure_row_accepts_a_new_start,
    a_reused_token_replays_to_an_initial_failure_row,
    count_terminal_born_since_counts_only_terminal_born_rows_in_the_window,
    an_initial_failure_is_inserted_despite_a_saturated_account,
    an_initial_failure_on_a_busy_table_answers_busy_and_inserts_nothing,
    // Reads and isolation.
    an_operation_is_invisible_and_inert_to_another_account,
    get_returns_the_record_as_stored,
    list_pages_newest_first_with_an_exclusive_cursor,
    // Claim and lease.
    two_owners_claiming_concurrently_share_no_operation,
    claim_returns_cancelling_operations_too,
    a_live_lease_blocks_claim_until_it_expires,
    an_expired_unclaimed_lease_still_accepts_the_owners_checkpoint_and_finish,
    claim_prefers_the_oldest_start_and_honours_max,
    claim_skips_the_rows_of_excluded_accounts,
    claim_skips_the_operations_in_the_exclude_list,
    // Checkpoint, cancel, and finish.
    checkpoint_persists_progress_counters_usage_and_records,
    a_checkpoint_from_a_non_owner_is_lost_and_leaves_no_trace,
    a_finish_from_a_non_owner_is_lost_and_changes_nothing,
    finishing_a_cancelling_operation_lands_cancelled_whatever_the_outcome_says,
    finish_clears_the_lease_and_deletes_report_records,
    cancel_moves_a_running_operation_to_cancelling_and_repeats_harmlessly,
    cancel_on_a_terminal_operation_reports_the_status_and_changes_nothing,
    cancel_on_an_unknown_operation_reports_not_found,
    // The row-outcome ledger.
    a_marked_write_records_applied_with_the_item,
    a_marked_condition_failure_records_the_failure_and_writes_nothing,
    unmarked_writes_leave_no_ledger_row,
    ledger_deletion_is_scoped_to_one_operation,
    // Retention.
    purge_removes_only_terminal_operations_older_than_the_cutoff,
    list_purgeable_before_honors_the_limit,
    the_terminal_born_cap_is_enforced_in_the_start_transaction,
    purge_deletes_only_the_given_pairs,
    a_purged_operations_token_is_free_for_a_new_start,
    // A second write at a recorded position.
    a_marked_write_at_a_recorded_position_is_refused_and_changes_nothing,
    // Batched writes.
    a_batch_applies_every_row_and_reports_each_outcome_in_order,
    a_batch_applies_two_rows_on_one_key_in_order,
    a_batch_overwrite_reports_the_replaced_item_for_sizing,
    // The write owner and the ledger a resumed run reads back.
    every_marked_batch_row_is_recorded_with_what_it_replaced,
    a_batch_prunes_the_ledger_rows_its_checkpoint_covers,
    a_batch_of_an_earlier_owner_writes_nothing,
    a_batch_row_at_a_recorded_position_fails_the_batch,
    two_rows_of_one_batch_at_one_position_fail_the_batch,
    a_batch_past_one_statement_of_rows_records_every_row,
    a_takeover_counts_the_resumes_that_find_the_checkpoint_unmoved,
    a_takeover_waits_for_a_batch_in_flight,
    a_batch_error_leaves_nothing_written,
    an_empty_batch_is_a_no_op,
    opposite_order_batches_on_one_key_set_both_complete,
    a_token_hash_shared_across_accounts_creates_in_each_account,
    // Paging, ordering, and terminal fidelity.
    list_on_an_exact_page_multiple_ends_without_a_cursor,
    report_records_read_back_in_file_and_row_order,
    finish_with_failed_carries_the_failure_code_and_message,
    claim_returns_records_oldest_start_first_in_order,
    claim_breaks_start_time_ties_by_operation_id,
    // Checkpoint integrity.
    a_checkpoint_reinserting_an_existing_record_key_keeps_the_first_record,
    checkpoint_usage_replaces_the_stored_value,
    checkpoint_records_each_finished_files_rows_and_etag,
    checkpoint_records_each_abandoned_files_outcome,
    // A load of many input files.
    a_many_file_load_costs_the_same_per_file_as_a_small_one,
    the_boundary_read_follows_the_cancel_and_the_lease,
    // Limit validation and purge follow-through.
    list_with_limit_zero_is_refused_as_validation,
    purged_pairs_name_the_ledger_rows_to_delete,
    // Paged report reads and report totals.
    paged_report_reads_return_every_record_exactly_once,
    a_report_page_after_the_last_record_is_empty,
    a_report_page_with_limit_zero_is_refused_as_validation,
    a_report_record_with_a_nul_escape_round_trips_through_checkpoint,
    report_totals_round_trip_through_checkpoint_and_start_at_zero,
    // The non-terminal count and the corrupt-counter refusal.
    count_non_terminal_counts_only_the_accounts_running_and_cancelling_rows,
    a_negative_stored_counter_refuses_the_read,
    // The run context recorded at Start.
    the_run_context_round_trips_through_every_read,
    a_terminal_born_row_keeps_its_run_context,
);

/// A batch that cannot take the write lock because another connection holds
/// `BEGIN IMMEDIATE` past the busy timeout (a backup, a slow DDL commit)
/// answers `Transient`, the class the bulk runner resumes after. Before the
/// batch mapped its `BEGIN` through `map_sqlx_err` it answered `Internal`
/// and the operation ended `FAILED`. Takes the 5-second busy timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_blocked_by_another_writer_is_transient() {
    use extenddb_storage::DataEngine;
    use extenddb_storage::error::StorageError;
    use sqlx::Executor;

    let (fx, path) = fixture().await;
    let (ki, rows) = contract::plain_batch_on_a_new_table(&fx, "batch_busy").await;
    let holder = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "sqlite://{}",
            path.to_str().expect("a UTF-8 path")
        ))
        .await
        .expect("open a second connection");
    let mut conn = holder.acquire().await.expect("a holder connection");
    conn.execute("BEGIN IMMEDIATE")
        .await
        .expect("hold the write lock");

    let error = fx
        .engine
        .put_items_batch(&ki, rows, None)
        .await
        .expect_err("the batch cannot take the write lock");
    assert!(
        matches!(error, StorageError::Transient(_)),
        "a busy database must answer Transient, got {error:?}"
    );

    conn.execute("ROLLBACK")
        .await
        .expect("release the write lock");
    drop(conn);
    holder.close().await;
    cleanup(fx, &path);
}

/// Edits a stored operation row on a connection of its own, for the cases
/// that need a row no trait method can write.
struct SqlEditor {
    pool: sqlx::SqlitePool,
}

impl SqlEditor {
    async fn open(path: &std::path::Path) -> Self {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!(
                "sqlite://{}",
                path.to_str().expect("a UTF-8 path")
            ))
            .await
            .expect("open an editing connection");
        Self { pool }
    }

    async fn edit(&self, sql: &str, account_id: &str, operation_id: &str) {
        let updated = sqlx::query(sql)
            .bind(account_id)
            .bind(operation_id)
            .execute(&self.pool)
            .await
            .expect("edit the stored row")
            .rows_affected();
        assert_eq!(updated, 1, "the edit names exactly one row");
    }
}

impl contract::RowEditor for SqlEditor {
    fn corrupt<'a>(
        &'a self,
        account_id: &'a str,
        operation_id: &'a str,
    ) -> futures::future::BoxFuture<'a, ()> {
        Box::pin(self.edit(
            "UPDATE bulk_operation_input_files \
             SET abandon = '{\"row\": 3, \"kind\": \"NOT_A_KIND\"}' \
             WHERE account_id = ? AND operation_id = ? AND file_index = 0",
            account_id,
            operation_id,
        ))
    }

    fn repair<'a>(
        &'a self,
        account_id: &'a str,
        operation_id: &'a str,
    ) -> futures::future::BoxFuture<'a, ()> {
        Box::pin(self.edit(
            "UPDATE bulk_operation_input_files SET abandon = NULL \
             WHERE account_id = ? AND operation_id = ? AND file_index = 0",
            account_id,
            operation_id,
        ))
    }

    fn mark_table_deleting<'a>(
        &'a self,
        account_id: &'a str,
        table_id: &'a str,
    ) -> futures::future::BoxFuture<'a, ()> {
        Box::pin(self.edit(
            "UPDATE tables SET table_status = 'DELETING' WHERE account_id = ? AND table_id = ?",
            account_id,
            table_id,
        ))
    }
}

macro_rules! edited_case {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            let (fx, path) = fixture().await;
            let editor = SqlEditor::open(&path).await;
            contract::$name(&fx, &editor).await;
            editor.pool.close().await;
            cleanup(fx, &path);
        }
    )*};
}

edited_case!(
    a_claim_ends_an_unreadable_row_and_returns_the_rest,
    a_claim_ends_an_unreadable_cancelled_row_cancelled,
    a_claim_ending_an_unreadable_row_deletes_its_report_records,
    a_listing_skips_an_unreadable_row_and_pages_past_it,
    a_start_on_a_table_set_deleting_after_the_lookup_is_refused,
);
