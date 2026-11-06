// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Contract tests for [`crate::BulkOperationStore`] and the row-outcome
//! ledger, written once against the trait and run by every backend's test
//! suite. Compiled only under the `test-util` feature, the same arrangement
//! `extenddb-cache` uses for its test helpers.
//!
//! Each function is one behavioural case. A backend's integration test binary
//! builds a fresh engine per case (a throwaway database, so cases cannot see
//! each other's rows), wraps it in a [`Fixture`], and calls the function. The
//! functions assert with `panic!`, so they run under `#[tokio::test]` entry
//! points; the concurrent cases spawn real tasks and need a multi-thread
//! runtime.
//!
//! Every table a case works with is created through the engine's own
//! `create_table`, so the `table_id` the bulk operation binds to is one the
//! catalog really holds.

// Every case asserts by panicking under a #[tokio::test] entry point, so
// per-function panics documentation would restate the whole file.
#![allow(clippy::missing_panics_doc)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use extenddb_core::expression::{self, ExpressionMaps};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
    KeyType, ScalarAttributeType, TableDescription,
};
use serde_json::json;

use crate::error::StorageError;
use crate::{
    AbandonKind, AbandonedFile, BatchRow, BatchRowResult, BulkRowMarker, CANCELLED_FAILURE_CODE,
    CANCELLED_FAILURE_MESSAGE, CLIENT_TOKEN_WINDOW_MS, CancelOutcome, FileAbandon, FinishState,
    FinishedFile, InputFile, LeaseState, ListCursor, NewBulkOperation, Progress, ReportKind,
    ReportRecord, RowOutcome, StartOutcome, StorageEngine, Terminal,
};

/// One backend engine plus the two seeded accounts the cases work with.
///
/// The entry point seeds both account rows before constructing this: several
/// backends hold a foreign key from `tables` (and from the bulk operation
/// rows) to `accounts`, so an unseeded account would fail table creation, not
/// the assertion under test.
pub struct Fixture<E> {
    pub engine: Arc<E>,
    /// The account most cases run in.
    pub account_a: String,
    /// A second account, for the isolation case.
    pub account_b: String,
    /// Region used when formatting table ARNs.
    pub region: String,
}

// ── Helpers ────────────────────────────────────────────────────────────

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// An operation id in the wire shape: 14 zero-padded digits, 8 lowercase hex.
fn op_id(start_time_ms: i64) -> String {
    format!("{:014}{:08x}", start_time_ms, unique())
}

/// Deterministic per-token stand-in for the SHA-256 hex the engine computes.
/// The store treats the hash as an opaque unique key, so any injective
/// encoding exercises the same index.
fn token_hash(token: &str) -> String {
    use std::fmt::Write;
    token.bytes().fold(String::new(), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    })
}

async fn create_table<E: StorageEngine>(
    fx: &Fixture<E>,
    account_id: &str,
    name: &str,
) -> TableDescription {
    fx.engine
        .create_table(
            account_id,
            CreateTableInput {
                table_name: name.to_owned(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_owned(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                ..Default::default()
            },
        )
        .await
        .expect("create a table through the engine")
}

fn arn(fx_region: &str, account_id: &str, table_name: &str) -> String {
    format!("arn:aws:dynamodb:{fx_region}:{account_id}:table/{table_name}")
}

/// A new-operation request bound to a real table.
fn new_op<E: StorageEngine>(
    fx: &Fixture<E>,
    account_id: &str,
    table: &TableDescription,
    token: Option<&str>,
    fingerprint: &str,
    start_time_ms: i64,
) -> NewBulkOperation {
    NewBulkOperation {
        operation_id: op_id(start_time_ms),
        table_id: table.table_id.clone(),
        table_name: table.table_name.clone(),
        table_arn: arn(&fx.region, account_id, &table.table_name),
        client_token: token.map(str::to_owned),
        client_token_hash: token.map(token_hash),
        request_fingerprint: fingerprint.to_owned(),
        input_config: json!({"S3Bucket": "in-bucket", "S3KeyPrefix": "data/"}),
        output_config: json!({"S3Bucket": "out-bucket", "OutputType": ["ERROR"]}),
        input_files: vec![InputFile {
            path: "data/one.json".to_owned(),
            size: 100,
            rows: None,
            etag: None,
            abandon: None,
        }],
        max_write_request_units: Some(1000),
        performance_tier: Some("STANDARD".to_owned()),
        start_time_ms,
        total_input_bytes: Some(100),
        initial_failure: None,
        run_context: None,
    }
}

fn start_kind(outcome: &StartOutcome) -> &'static str {
    match outcome {
        StartOutcome::Created(_) => "Created",
        StartOutcome::IdempotentReplay(_) => "IdempotentReplay",
        StartOutcome::IdempotentMismatch => "IdempotentMismatch",
        StartOutcome::TableBusy => "TableBusy",
        StartOutcome::AccountLimitReached => "AccountLimitReached",
        StartOutcome::FailedStartLimit => "FailedStartLimit",
        StartOutcome::IndexOperationInProgress => "IndexOperationInProgress",
    }
}

fn created(outcome: StartOutcome) -> crate::BulkOperationRecord {
    match outcome {
        StartOutcome::Created(record) => record,
        other => panic!("expected Created, got {}", start_kind(&other)),
    }
}

async fn start<E: StorageEngine>(
    fx: &Fixture<E>,
    account_id: &str,
    req: NewBulkOperation,
    limit: u32,
) -> StartOutcome {
    fx.engine
        .start_bulk_operation(account_id, req, limit)
        .await
        .expect("start_bulk_operation must not error at the transport level")
}

/// Start an operation and claim it for `owner`, returning its id. Most of the
/// checkpoint and finish cases need a claimed row as their starting state.
async fn started_and_claimed<E: StorageEngine>(
    fx: &Fixture<E>,
    table: &TableDescription,
    owner: &str,
    start_time_ms: i64,
) -> String {
    let record = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, table, None, "fp", start_time_ms),
            50,
        )
        .await,
    );
    let claimed = fx
        .engine
        .claim_runnable(owner, Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(
        claimed
            .iter()
            .any(|r| r.operation_id == record.operation_id),
        "the freshly started operation must be claimable"
    );
    record.operation_id
}

fn progress(records: Vec<ReportRecord>) -> Progress {
    let report_record_count = u64::try_from(records.len()).unwrap_or(u64::MAX);
    Progress {
        cursor_file_index: 0,
        cursor_row_index: 25,
        processed_item_count: 25,
        failed_item_count: 1,
        processed_input_bytes: 2500,
        report_record_count,
        report_bytes: report_record_count.saturating_mul(100),
        usage: json!({"WriteRequestUnits": 25600}),
        records,
        finished_files: Vec::new(),
        abandoned_files: Vec::new(),
    }
}

fn error_record(row_index: i64) -> ReportRecord {
    ReportRecord {
        kind: ReportKind::Error,
        file_index: 0,
        row_index,
        record: json!({"itemIndex": row_index, "error": {"ErrorCode": "ValidationError"}}),
    }
}

fn terminal(status: &str, end_time_ms: i64) -> Terminal {
    Terminal {
        status: status.to_owned(),
        end_time_ms,
        failure_code: None,
        failure_message: None,
        usage: json!({"WriteRequestUnits": 25600}),
    }
}

async fn get<E: StorageEngine>(
    fx: &Fixture<E>,
    account_id: &str,
    operation_id: &str,
) -> Option<crate::BulkOperationRecord> {
    fx.engine
        .get_bulk_operation(account_id, operation_id)
        .await
        .expect("get_bulk_operation")
}

/// Every record body of one kind, read through the paged API the way the
/// report renderer reads it, with a page smaller than most fixtures so the
/// loop itself is exercised.
async fn all_report_records<E: StorageEngine>(
    fx: &Fixture<E>,
    account_id: &str,
    operation_id: &str,
    kind: ReportKind,
) -> Vec<serde_json::Value> {
    const PAGE: usize = 2;
    let mut bodies = Vec::new();
    let mut after = None;
    loop {
        let page = fx
            .engine
            .report_records_page(account_id, operation_id, kind, after, PAGE)
            .await
            .expect("report_records_page");
        let Some(last) = page.last() else {
            break;
        };
        after = Some((last.file_index, last.row_index));
        let full = page.len() == PAGE;
        bodies.extend(page.into_iter().map(|record| record.record));
        if !full {
            break;
        }
    }
    bodies
}

// Row and marker helpers shared by the ledger, batch, replay, and purge
// cases; kept here so no single concern file owns them.

async fn key_info<E: StorageEngine>(
    fx: &Fixture<E>,
    table_name: &str,
) -> extenddb_core::types::TableKeyInfo {
    fx.engine
        .table_key_info(&fx.account_a, table_name)
        .await
        .expect("key info")
}

fn item(pk: &str, v: &str) -> Item {
    Item::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("v".to_owned(), AttributeValue::S(v.to_owned())),
    ])
}

/// The write owner the ledger cases record before a marked batch.
const OWNER: &str = "owner-1";

fn marker(account_id: &str, operation_id: &str, row_index: i64) -> BulkRowMarker {
    BulkRowMarker {
        account_id: account_id.to_owned(),
        operation_id: operation_id.to_owned(),
        file_index: 0,
        row_index,
        owner: OWNER.to_owned(),
        keep_old: false,
        settled: (0, 0),
    }
}

/// Record [`OWNER`] as the operation's write owner, as a claim does before
/// its first batch; a marked batch of any other owner writes nothing.
async fn take_over<E: StorageEngine>(fx: &Fixture<E>, operation_id: &str) {
    fx.engine
        .bulk_write_takeover(&fx.account_a, operation_id, OWNER, (0, 0))
        .await
        .expect("record the write owner");
}

mod batch;
mod checkpoint;
mod counters;
mod finish;
mod index_crossing;
mod initial_failure;
mod lease;
mod ledger;
mod list;
mod lock_waits;
mod many_files;
mod purge;
mod refused_index_creation;
mod report;
mod run_context;
mod start;
mod table_deleting;
mod unreadable;
mod write_owner;

pub use self::batch::*;
pub use self::checkpoint::*;
pub use self::counters::*;
pub use self::finish::*;
pub use self::index_crossing::*;
pub use self::initial_failure::*;
pub use self::lease::*;
pub use self::ledger::*;
pub use self::list::*;
pub use self::lock_waits::*;
pub use self::many_files::*;
pub use self::purge::*;
pub use self::refused_index_creation::*;
pub use self::report::*;
pub use self::run_context::*;
pub use self::start::*;
pub use self::table_deleting::*;
pub use self::unreadable::*;
pub use self::write_owner::*;
