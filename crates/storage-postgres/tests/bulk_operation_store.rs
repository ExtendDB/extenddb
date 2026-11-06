// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The bulk operation store contract, run against the `PostgreSQL` backend.
//!
//! The cases live in `extenddb_storage::bulk_operation_store_contract` and are
//! shared with the `SQLite` suite; this file only builds the engine, following
//! the conventions of `vector_control_plane.rs`: each test builds its own
//! throwaway database, applies the shipped migrations, and drops it when it
//! passes. A failing test leaves its database behind, named `eddb_bulk_*`, so
//! the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component, pointing at a server whose role may create and drop databases.
//! Without it every test reports a skip and passes.

use std::sync::Arc;

use extenddb_storage::bulk_operation_store_contract::{self as contract, Fixture};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT_A: &str = "111111111111";
const ACCOUNT_B: &str = "222222222222";
const REGION: &str = "us-east-1";

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// Report the reason a test did nothing, loudly enough to notice in a log.
fn skip(test: &str) {
    // In the CI job that provides PostgreSQL, an unset connection string is a
    // configuration error, not a skip: the summary line of a skipped run reads
    // the same as a real pass (cargo captures the message below), so the test
    // fails loudly instead. That job sets EXTENDDB_TEST_PG_REQUIRED; every
    // other job (plain `cargo test --workspace`, the MongoDB and Cassandra
    // legs) has no PostgreSQL and skips like the rest of this crate's suites.
    assert!(
        std::env::var("EXTENDDB_TEST_PG_REQUIRED").map_or(true, |v| v.is_empty()),
        "{test}: EXTENDDB_TEST_PG_REQUIRED is set but EXTENDDB_TEST_PG_CONNECTION_STRING is \
         not; the PostgreSQL bulk store tests would silently pass without a database"
    );
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch catalog in."
    );
}

/// One scratch environment at a time, across the whole test binary. Each
/// environment holds an engine pool with a floor of ten connections and these
/// tests share a server with whatever else runs against it; running them all
/// at once exhausts `max_connections` and reports a resource limit as a
/// product bug.
async fn environment_permit() -> tokio::sync::OwnedSemaphorePermit {
    static ENVIRONMENTS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    std::sync::Arc::clone(
        ENVIRONMENTS.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(1))),
    )
    .acquire_owned()
    .await
    .expect("the environment semaphore is never closed")
}

/// A throwaway database with the shipped schema applied and an engine on it.
struct Scratch {
    fixture: Fixture<PostgresEngine>,
    catalog: PgPool,
    admin: PgPool,
    db_name: String,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Scratch {
    /// Drop the scratch database. Called only on the success path, so a
    /// failure leaves the database for inspection.
    async fn cleanup(self) {
        let Scratch {
            fixture,
            catalog,
            admin,
            db_name,
            _permit,
        } = self;
        drop(fixture);
        catalog.close().await;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
        ))
        .execute(&admin)
        .await
        .expect("drop the scratch database");
        admin.close().await;
    }
}

/// The shipped migration files, read from the crate directory at run time and
/// applied in name order, so a migration added next to these tests is applied
/// without editing this list.
fn migration_files(subdir: &str) -> Vec<std::path::PathBuf> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(subdir);
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("list {}: {e}", dir.display()))
        .map(|entry| entry.expect("read a directory entry").path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "sql"))
        .collect();
    files.sort();
    files
}

/// Build a scratch database, apply the shipped migrations, pin the
/// control-plane delays to zero (no workers run here), seed the two contract
/// accounts, and open an engine.
async fn scratch(test: &str) -> Option<Scratch> {
    let Some(base) = base_conn() else {
        skip(test);
        return None;
    };
    let permit = environment_permit().await;
    let db_name = format!("eddb_bulk_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();

    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin)
        .await
        .expect("create the scratch database");

    let url = format!("{base}/{db_name}");
    let catalog = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to the scratch database");

    for file in migration_files("migrations")
        .into_iter()
        .chain(migration_files("data_migrations"))
    {
        let sql = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        sqlx::raw_sql(&sql)
            .execute(&catalog)
            .await
            .unwrap_or_else(|e| panic!("apply {}: {e}", file.display()));
    }

    for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, '0') \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .execute(&catalog)
        .await
        .expect("pin a delay setting to zero");
    }
    for (id, name) in [(ACCOUNT_A, "contract-a"), (ACCOUNT_B, "contract-b")] {
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
            .bind(id)
            .bind(format!("{name}-{db_name}"))
            .execute(&catalog)
            .await
            .expect("seed a contract account");
    }

    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: url,
            // The engine clamps anything below its floor of ten, so ask for
            // the floor.
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch database");

    Some(Scratch {
        fixture: Fixture {
            engine: Arc::new(engine),
            account_a: ACCOUNT_A.to_owned(),
            account_b: ACCOUNT_B.to_owned(),
            region: REGION.to_owned(),
        },
        catalog,
        admin,
        db_name,
        _permit: permit,
    })
}

macro_rules! contract_case {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            let Some(s) = scratch(stringify!($name)).await else {
                return;
            };
            contract::$name(&s.fixture).await;
            s.cleanup().await;
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

/// Edits a stored operation row through the scratch catalog pool, for the
/// cases that need a row no trait method can write.
struct SqlEditor<'p> {
    pool: &'p PgPool,
}

impl SqlEditor<'_> {
    async fn edit(&self, sql: &str, account_id: &str, operation_id: &str) {
        let updated = sqlx::query(sql)
            .bind(account_id)
            .bind(operation_id)
            .execute(self.pool)
            .await
            .expect("edit the stored row")
            .rows_affected();
        assert_eq!(updated, 1, "the edit names exactly one row");
    }
}

impl contract::RowEditor for SqlEditor<'_> {
    fn corrupt<'a>(
        &'a self,
        account_id: &'a str,
        operation_id: &'a str,
    ) -> futures::future::BoxFuture<'a, ()> {
        Box::pin(self.edit(
            "UPDATE bulk_operation_input_files \
             SET abandon = '{\"row\": 3, \"kind\": \"NOT_A_KIND\"}'::jsonb \
             WHERE account_id = $1 AND operation_id = $2 AND file_index = 0",
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
             WHERE account_id = $1 AND operation_id = $2 AND file_index = 0",
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
            "UPDATE tables SET table_status = 'DELETING' WHERE account_id = $1 AND table_id = $2",
            account_id,
            table_id,
        ))
    }
}

macro_rules! edited_case {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            let Some(s) = scratch(stringify!($name)).await else {
                return;
            };
            contract::$name(&s.fixture, &SqlEditor { pool: &s.catalog }).await;
            s.cleanup().await;
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

// The cases that hold a lock from a connection of their own.
#[path = "bulk_operation_store/locks.rs"]
mod locks;

// Faults a write batch meets on a row statement.
#[path = "bulk_operation_store/row_faults.rs"]
mod row_faults;
