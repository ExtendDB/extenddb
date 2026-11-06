// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Behaviour tests for the bulk put runner, written from the specification.
//!
//! Every test drives `runner::run` against a real in-memory `SQLite` storage
//! engine, with temporary directories as the import and export roots, a
//! table created through the storage engine, and operations created through
//! `start_bulk_operation` and claimed through `claim_runnable`, so the
//! record the runner receives is the record a worker would receive.
//! Assertions read terminal state through `get_bulk_operation`, item state
//! through `get_item` and `scan`, the row-outcome ledger through
//! `bulk_row_outcome`, and the rendered report files by decompressing and
//! parsing them.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use serde_json::{Value, json};

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::metrics::{Dimension, MetricName, MetricsCollector, MetricsQuery, TimeWindow};
use extenddb_core::types::{
    AttributeValue, CreateTableInput, DeleteTableInput, DescribeStreamInput, DescribeTableInput,
    IndexInfo, Item, ListTablesInput, ListTablesOutput, StreamDescription, StreamEventName,
    StreamRecord, TableDescription, TableKeyInfo, Tag, TimeToLiveDescription, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage::{
    BoxedFuture as BoxFuture, BulkOperationListResult, BulkOperationRecord, BulkOperationStore,
    BulkRowMarker, CancelOutcome, CancellationToken, DataEngine, FinishState, IdempotencyKey,
    InputFile, ItemPairResult, LeaseState, ListCursor, MetadataEngine, NewBulkOperation, Progress,
    QueryResult, ReportKind, ReportRecord, RowOutcome, StartOutcome, StorageEngine, StreamCapture,
    StreamEngine, StreamListResult, StreamRecordsResult, TableEngine, Terminal, TransactGetOp,
    TransactWriteOp, TtlTableInfo, WorkerStore,
};

use crate::bulk_put::arn;
use crate::bulk_put::authorize::BulkAuthorizer;
use crate::bulk_put::pacer::WritePacer;
use crate::bulk_put::report;
use crate::bulk_put::run_context::STORE_FILESYSTEM;
use crate::bulk_put::runner::{self, RunnerConfig, RunnerDeps};
use crate::bulk_put::source::{
    BucketRef, BulkSink, BulkSource, FsSink, FsSource, InputCodec, LineReader, OutputSse,
    ReportWriter, SinkError, SourceError, SourceFile,
};
use crate::expression_helpers::resolve_condition;

const REGION: &str = "us-east-1";
const ACCOUNT: &str = "123456789012";
const IN_BUCKET: &str = "loads";
const OUT_BUCKET: &str = "reports";
const OUT_PREFIX: &str = "run";
const TABLE: &str = "orders";

/// The one message every parse-level row fault carries.
const ABANDON: &str = "Expected DynamoDB type to be one of [S, N, B, BOOL, NULL, L, M, SS, NS, \
                       BS]. Remainder of the file will not be processed.";
const MISSING_KEY: &str =
    "One or more parameter values were invalid: Missing the key sk in the item";
const TYPE_MISMATCH: &str =
    "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: N";
const OVERSIZED: &str = "Item size has exceeded the maximum allowed size";
const CONDITION_FAILED: &str = "The conditional request failed";
const TABLE_NOT_FOUND: &str = "Requested resource not found";

// Input rows.

/// One plain input row keyed `k{i}`, sort key `s`, small enough that its
/// write rounds up to the 1 KB minimum.
pub(super) fn row(i: usize) -> String {
    format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"k{i}\"}}, \"sk\": {{\"S\": \"s\"}}, \"v\": {{\"N\": \"{i}\"}}}}}}"
    )
}

/// A row missing the `sk` key attribute: an item-level fault.
pub(super) fn row_missing_sk(i: usize) -> String {
    format!("{{\"Item\": {{\"pk\": {{\"S\": \"k{i}\"}}, \"v\": {{\"N\": \"0\"}}}}}}")
}

/// A row whose attribute value carries an unknown type tag: a parse-level
/// fault that abandons the rest of the file.
fn row_bad_type_tag(i: usize) -> String {
    format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"k{i}\"}}, \"sk\": {{\"S\": \"s\"}}, \"v\": {{\"X\": \"1\"}}}}}}"
    )
}

/// Join rows into file contents, one per line, newline-terminated.
pub(super) fn file_of(rows: &[String]) -> Vec<u8> {
    let mut out = String::new();
    for r in rows {
        out.push_str(r);
        out.push('\n');
    }
    out.into_bytes()
}

pub(super) fn plain_rows(n: usize) -> Vec<String> {
    (0..n).map(row).collect()
}

fn gzip_bytes(content: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(content).unwrap();
    enc.finish().unwrap()
}

fn gunzip_lines(bytes: &[u8]) -> Vec<Value> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut plain = String::new();
    decoder.read_to_string(&mut plain).unwrap();
    plain
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn item(pk: &str, v: &str) -> Item {
    let mut item = Item::new();
    item.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
    item.insert("sk".to_owned(), AttributeValue::S("s".to_owned()));
    item.insert("v".to_owned(), AttributeValue::N(v.to_owned()));
    item
}

fn key(pk: &str) -> Item {
    let mut key = Item::new();
    key.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
    key.insert("sk".to_owned(), AttributeValue::S("s".to_owned()));
    key
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn input_config(gzip: bool) -> Value {
    json!({
        "S3Bucket": IN_BUCKET,
        "InputFormat": "DYNAMODB_JSON",
        "InputCompressionType": if gzip { "GZIP" } else { "NONE" },
    })
}

fn zstd_input_config() -> Value {
    json!({
        "S3Bucket": IN_BUCKET,
        "InputFormat": "DYNAMODB_JSON",
        "InputCompressionType": "ZSTD",
    })
}

fn zstd_bytes(content: &[u8]) -> Vec<u8> {
    ruzstd::encoding::compress_to_vec(content, ruzstd::encoding::CompressionLevel::Fastest)
}

fn output_config(bucket: Option<&str>, types: &[&str]) -> Value {
    let mut config = json!({ "S3KeyPrefix": OUT_PREFIX, "OutputType": types });
    if let Some(bucket) = bucket {
        config["S3Bucket"] = json!(bucket);
    }
    config
}

fn both_reports() -> Value {
    output_config(Some(OUT_BUCKET), &["ERROR", "CONDITION_CHECK_FAILURE"])
}

/// One row whose attribute value nests `depth` map levels.
fn row_nested(index: usize, depth: usize) -> String {
    let mut value = String::from(r#"{"S":"leaf"}"#);
    for _ in 0..depth {
        value = format!(r#"{{"M":{{"k":{value}}}}}"#);
    }
    format!(r#"{{"Item":{{"pk":{{"S":"k{index}"}},"sk":{{"S":"s"}},"deep":{value}}}}}"#)
}

// Replay and the row-outcome ledger.

/// A small conditional row against key `c`.
fn condition_row(v: &str, expression: &str) -> String {
    format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"c\"}}, \"sk\": {{\"S\": \"s\"}}, \"v\": {{\"N\": \"{v}\"}}}}, \
         \"ConditionExpression\": \"{expression}\"}}"
    )
}

// Report record fidelity at runner level.

/// The serialized size of one report record from a rendered file, used to
/// size a byte cap exactly. All records of the calibration run must be the
/// same size, so a cap of two sizes keeps exactly two records.
fn calibrated_record_bytes(report: &[Value]) -> u64 {
    let sizes: Vec<u64> = report.iter().map(report::record_bytes).collect();
    assert!(
        sizes.windows(2).all(|w| w[0] == w[1]),
        "calibration records differ in size: {sizes:?}"
    );
    sizes[0]
}

mod abandon_cut_restart;
mod batched;
mod cancellation;
mod caps;
mod compressed_cut;
mod disk_full;
mod durability;
mod exactly_once;
mod fake_authorizer;
mod faults;
mod faults_engines;
mod harness;
mod held_location;
mod held_resume;
mod input;
mod jail;
mod lease_in_place;
mod lease_keeper;
mod lifecycle_cells;
mod manifest;
mod open_refusal;
mod output_faults;
mod outputs_after_outcome;
mod overwritten;
mod pacing_lease;
mod parsing;
mod pins;
mod recorded_store;
mod render;
mod replay;
mod reports;
mod restart_abandons;
mod resume;
mod resume_bound;
mod rotation;
mod rows;
mod rows_faults;
mod s3_actions;
mod service_etags;
mod sink_faults;
mod source_faults;
mod store_outage;
mod stored_key;
mod table_binding;
mod takeover;
mod writes;

pub(crate) use fake_authorizer::*;
pub(crate) use faults::*;
pub(crate) use harness::*;
use sink_faults::*;
use source_faults::*;
