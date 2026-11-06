// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Behaviour tests for the bulk operation request handlers, written from
//! the specification.
//!
//! Every test drives the four operations through `dispatch`, so operation
//! routing is covered together with the handler, against a real in-memory
//! `SQLite` storage engine with a temporary directory as the bulk store
//! root. The storage harness and its fault-injecting wrapper are shared
//! with the runner behaviour tests in `runner.rs`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use extenddb_auth::AuthCacheRegistry;
use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::{
    FinishState, LeaseState, NewBulkOperation, Progress, StartOutcome, StorageEngine, Terminal,
};

use super::runner::{FaultingStorage, Harness, file_of, plain_rows, row, row_missing_sk};
use crate::OperationContext;
use crate::bulk_put::arn;
use crate::bulk_put::authorize::BulkAuthorizer;
use crate::bulk_put::runner::{RunnerConfig, run as run_operation};
use crate::bulk_put::store::BulkStore;
use crate::expression_helpers::resolve_condition;

const REGION: &str = "us-east-1";
const ACCOUNT: &str = "123456789012";
const OTHER_ACCOUNT: &str = "999999999999";
const TABLE: &str = "orders";
const IN_BUCKET: &str = "loads";
const OUT_BUCKET: &str = "reports";

/// Layer 1 of the operation ARN table.
const NOT_AN_ARN: &str = "Invalid Bulk operation ARN";
/// Layer 2 of the operation ARN table.
const BAD_ID: &str = "Invalid Request: Bulk operation ARN is invalid.";
const ION_REFUSED: &str = "InputFormat ION is not supported by this server";
const BAD_PREFIX: &str = "S3KeyPrefix contains an invalid path component";
const EMPTY_OUTPUT_TYPE: &str = "Invalid Request: OutputS3Configuration.OutputType cannot be empty";
const CONFIG_TOO_BIG: &str = "S3 configuration exceeds 8192 bytes";
const BULK_DISABLED: &str =
    "Bulk operations are disabled. Configure [bulk] in extenddb.toml to enable.";
const NO_SUCH_BUCKET: &str = "The specified bucket does not exist";
const TABLE_BUSY: &str = "Table has ongoing bulk operations, please wait and try again";
const IDEMPOTENT_MISMATCH: &str = "Specified idempotent token was already used with different \
                                   request parameters within the idempotency window";
const TABLE_NOT_FOUND: &str = "Requested resource not found";
const THROTTLED: &str = "Rate of requests exceeds the allowed throughput.";

// Context and request plumbing.

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// A `LimitsConfig` carrying the bulk operation keys, built through serde so
/// this file compiles independently of the limits struct; on a tree without
/// the fields the parse fails at run time and names what is missing.
fn bulk_limits(overrides: &[(&str, Value)]) -> LimitsConfig {
    let mut body = json!({
        "max_bulk_input_bytes": 10_737_418_240_u64,
        "max_bulk_input_files": 10_000,
        "max_concurrent_bulk_operations": 50,
        "max_bulk_client_token_bytes": 1024,
    });
    for (key, value) in overrides {
        body[*key] = value.clone();
    }
    serde_json::from_value(body).expect("LimitsConfig must carry the max_bulk_* fields")
}

fn ctx_with_limits(h: &Harness, limits: LimitsConfig) -> OperationContext {
    OperationContext {
        storage: Arc::clone(&h.storage),
        backup: Arc::new(crate::BackupRuntime::new(4)),
        limits: Arc::new(limits),
        region: Arc::from(REGION),
        account_id: Arc::from(ACCOUNT),
        // Empty on purpose: bulk operations read only `bulk`, so a
        // regression that reaches for the import or export paths fails
        // these tests instead of passing by accident.
        import_paths: Vec::new().into(),
        export_paths: Vec::new().into(),
        bulk: Some(Arc::new(BulkStore::filesystem(h.store.root()))),
        bulk_authorizer: None,
        bulk_principal: None,
        authorized_key_info: Default::default(),
        auth_cache: AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
        metrics: Arc::new(MetricsCollector::new()),
    }
}

fn base_ctx(h: &Harness) -> OperationContext {
    ctx_with_limits(h, bulk_limits(&[]))
}

fn ctx_without_store(h: &Harness) -> OperationContext {
    let mut ctx = base_ctx(h);
    ctx.bulk = None;
    ctx
}

fn ctx_for_account(h: &Harness, account: &str) -> OperationContext {
    let mut ctx = base_ctx(h);
    ctx.account_id = Arc::from(account);
    ctx
}

fn ctx_with_storage(h: &Harness, storage: Arc<dyn StorageEngine>) -> OperationContext {
    let mut ctx = base_ctx(h);
    ctx.storage = storage;
    ctx
}

async fn call(
    ctx: &OperationContext,
    operation: &str,
    body: Value,
) -> Result<Value, DynamoDbError> {
    crate::dispatch(operation, body, ctx, "127.0.0.1:0")
        .await
        .map(|result| result.body)
}

/// A valid start request: empty input prefix over the staged input bucket
/// directory, and both report kinds into the staged report bucket.
fn start_body() -> Value {
    json!({
        "TableName": TABLE,
        "InputS3Configuration": {
            "S3Bucket": IN_BUCKET,
            "InputFormat": "DYNAMODB_JSON",
            "InputCompressionType": "NONE"
        },
        "OutputS3Configuration": {
            "S3Bucket": OUT_BUCKET,
            "S3KeyPrefix": "run",
            "OutputType": ["ERROR", "CONDITION_CHECK_FAILURE"]
        }
    })
}

fn with_input(mut body: Value, key: &str, value: Value) -> Value {
    body["InputS3Configuration"][key] = value;
    body
}

fn with_output(mut body: Value, key: &str, value: Value) -> Value {
    body["OutputS3Configuration"][key] = value;
    body
}

fn with_top(mut body: Value, key: &str, value: Value) -> Value {
    body[key] = value;
    body
}

async fn start(ctx: &OperationContext, body: Value) -> Result<Value, DynamoDbError> {
    call(ctx, "StartBulkPutItems", body).await
}

async fn start_ok(ctx: &OperationContext, body: Value) -> Value {
    let response = start(ctx, body).await.expect("the start must succeed");
    let description = response["BulkOperationDescription"].clone();
    assert!(description.is_object(), "start must return a description");
    description
}

async fn describe(ctx: &OperationContext, operation_arn: &str) -> Result<Value, DynamoDbError> {
    call(
        ctx,
        "DescribeBulkOperation",
        json!({ "BulkOperationArn": operation_arn }),
    )
    .await
}

async fn cancel(ctx: &OperationContext, operation_arn: &str) -> Result<Value, DynamoDbError> {
    call(
        ctx,
        "CancelBulkOperation",
        json!({ "BulkOperationArn": operation_arn }),
    )
    .await
}

fn arn_of(account: &str, operation_id: &str) -> String {
    format!("arn:aws:dynamodb:{REGION}:{account}:bulk-operation/{operation_id}")
}

fn description_arn(description: &Value) -> String {
    description["BulkOperationArn"]
        .as_str()
        .expect("the description carries an ARN")
        .to_owned()
}

fn summaries(response: &Value) -> Vec<Value> {
    response["BulkOperationsSummaryList"]
        .as_array()
        .expect("a summary list")
        .clone()
}

fn summary_arns(response: &Value) -> Vec<String> {
    summaries(response)
        .iter()
        .map(|s| s["BulkOperationArn"].as_str().unwrap().to_owned())
        .collect()
}

/// The model constrains `NextToken` to `([0-9a-f]{16})+`, and the service
/// refuses a token under 112 characters, so an issued token is at least 112
/// characters, a multiple of 16, and lowercase hex.
fn assert_token_shape(token: &str) {
    assert!(
        token.len() >= 112 && token.len().is_multiple_of(16),
        "an issued token must be at least 112 characters and a multiple of 16: {token}"
    );
    assert!(
        token
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "token must be lowercase hex: {token}"
    );
}

// Error extraction.

#[track_caller]
fn expect_validation(result: Result<Value, DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(message)) => message,
        Err(other) => panic!("expected ValidationException, got {other:?}"),
        Ok(body) => panic!("expected ValidationException, got success: {body}"),
    }
}

#[track_caller]
fn expect_throttling(result: Result<Value, DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ThrottlingException(message)) => message,
        Err(other) => panic!("expected ThrottlingException, got {other:?}"),
        Ok(body) => panic!("expected ThrottlingException, got success: {body}"),
    }
}

#[track_caller]
fn expect_not_found(result: Result<Value, DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ResourceNotFoundException(message)) => message,
        Err(other) => panic!("expected ResourceNotFoundException, got {other:?}"),
        Ok(body) => panic!("expected ResourceNotFoundException, got success: {body}"),
    }
}

#[track_caller]
fn expect_in_use(result: Result<Value, DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ResourceInUseException(message)) => message,
        Err(other) => panic!("expected ResourceInUseException, got {other:?}"),
        Ok(body) => panic!("expected ResourceInUseException, got success: {body}"),
    }
}

#[track_caller]
fn expect_limit_exceeded(result: Result<Value, DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::LimitExceededException(message)) => message,
        Err(other) => panic!("expected LimitExceededException, got {other:?}"),
        Ok(body) => panic!("expected LimitExceededException, got success: {body}"),
    }
}

// Storage-level setup for List, Cancel, and foreign-account cases.

/// The id of table `raw{index}` of `account`, created on first use: the
/// start transaction refuses a table whose row is absent.
async fn raw_table(h: &Harness, account: &str, index: u32) -> String {
    let name = format!("raw{index}");
    let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("create table input");
    match h.storage.create_table(account, input).await {
        Ok(description) => description.table_id,
        Err(extenddb_storage::error::StorageError::TableAlreadyExists(_)) => {
            h.storage
                .describe_table(
                    account,
                    extenddb_core::types::DescribeTableInput { table_name: name },
                )
                .await
                .expect("describe the raw table")
                .table_id
        }
        Err(error) => panic!("create the raw table: {error:?}"),
    }
}

/// Insert one operation record directly, with a controlled start time and a
/// table of its own, so list ordering and terminal transitions can be
/// staged without a runner.
async fn raw_op(h: &Harness, account: &str, index: u32, start_time_ms: i64) -> String {
    let operation_id = arn::new_operation_id(u64::try_from(start_time_ms).unwrap(), index);
    let table_id = raw_table(h, account, index).await;
    let outcome = h
        .storage
        .start_bulk_operation(
            account,
            NewBulkOperation {
                operation_id: operation_id.clone(),
                table_id,
                table_name: format!("raw{index}"),
                table_arn: format!("arn:aws:dynamodb:{REGION}:{account}:table/raw{index}"),
                client_token: None,
                client_token_hash: None,
                request_fingerprint: format!("fp-{account}-{index}-{start_time_ms}"),
                input_config: json!({}),
                output_config: json!({}),
                input_files: vec![],
                max_write_request_units: None,
                performance_tier: None,
                start_time_ms,
                total_input_bytes: Some(0),
                initial_failure: None,
                run_context: None,
            },
            10_000,
        )
        .await
        .expect("raw start");
    match outcome {
        StartOutcome::Created(record) => record.operation_id,
        _ => panic!("the raw start must create the operation"),
    }
}

async fn finish_op(h: &Harness, account: &str, owner: &str, operation_id: &str, status: &str) {
    let state = h
        .storage
        .finish(
            account,
            operation_id,
            owner,
            Terminal {
                status: status.to_owned(),
                end_time_ms: now_ms(),
                failure_code: None,
                failure_message: None,
                usage: json!({}),
            },
        )
        .await
        .expect("finish");
    assert!(
        matches!(state, FinishState::Held(_)),
        "the finisher must still hold the lease"
    );
}

fn runner_config() -> RunnerConfig {
    RunnerConfig {
        batch_size: 25,
        write_batch_rows: 25,
        max_line_bytes: 4 << 20,
        max_item_count: 1_000_000,
        max_report_records: 10_000,
        max_report_bytes: 64 << 20,
        lease: Duration::from_secs(60),
    }
}

fn stored_input_config() -> Value {
    json!({
        "S3Bucket": IN_BUCKET,
        "InputFormat": "DYNAMODB_JSON",
        "InputCompressionType": "NONE",
    })
}

fn stored_output_config() -> Value {
    json!({
        "S3Bucket": OUT_BUCKET,
        "S3KeyPrefix": "run",
        "OutputType": ["ERROR", "CONDITION_CHECK_FAILURE"],
    })
}

mod cancel;
mod describe;
mod dispatch;
mod list;
mod log_targets;
mod logs;
mod run_context;
mod s3_permissions;
mod start_envelope;
mod start_fields;
mod start_locations;
mod start_outcomes;
mod start_prefix_forms;
mod start_prefix_rules;
mod start_replay;
mod start_store_faults;
mod start_terminal_born;
mod start_validation;
mod unsupported_backend;
mod update_table_guard;
mod validation;
