// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The portable adapter runtime: build an [`OperationContext`] and answer a
//! `DynamoDB` request by calling the `ExtendDB` engine in-process.
//!
//! This module is target-agnostic — it compiles and runs natively (where the
//! unit tests exercise it against the native `ExtendDB` `SQLite` build) and on
//! `wasm32` (where [`crate::wasm`] wraps it in a `wasm-bindgen` surface). It
//! contains no `wasm-bindgen` and no network code: the "transport" is a direct
//! function call into [`extenddb_engine::dispatch`], which is what the tokio
//! TCP listener would have called after parsing an HTTP request.
//!
//! # Operations the browser build cannot serve
//!
//! Some operations need a facility the in-memory browser build does not have —
//! a backup store, or the `PartiQL` surface (left out of this build). The
//! policy that answers them lives here, in [`Engine::dispatch`], and the same
//! code applies it on both targets, so the native build and the wasm build
//! refuse the same requests with the same error:
//!
//! - **Refused on both targets** with a `ValidationException` that names the
//!   build: the store-backed backup operations (`CreateBackup`,
//!   `DescribeBackup`, `DeleteBackup`, `RestoreTableFromBackup`); the three
//!   `PartiQL` operations; and `DescribeEndpoints`, as the build has no
//!   network address to report.
//! - **Refused as the server refuses them**: every
//!   `RestoreTableToPointInTime`, and an `UpdateContinuousBackups` that enables
//!   point-in-time recovery, through [`extenddb_engine::parse_request`] and
//!   [`extenddb_engine::ParsedRequest::admit`], the pre-dispatch checks the
//!   server applies too (the server also authenticates between them; this
//!   build authenticates no request). They run before any table is read.
//! - **Passed through** to the engine's handlers: everything else, including
//!   the data-plane operations, `DescribeTable` / `ListTables`, streams,
//!   time-to-live, tagging, every `UpdateTable`, `ListBackups`,
//!   `DescribeContinuousBackups`, an `UpdateContinuousBackups` that does not
//!   enable point-in-time recovery, and `ImportTable` /
//!   `ExportTableToPointInTime`.
//!   The wasm build compiles those handlers too, so a malformed request gets
//!   the handler's own validation answer on both targets. The adapter
//!   configures no backup store and no import or export paths, so `ListBackups`
//!   lists nothing and the import and export handlers answer that the
//!   operation is disabled.
//!
//! One refusal is not made here, and differs between the targets: an
//! `UpdateTable` that creates a vector index. The engine validates the request
//! first, so a missing table or a malformed `Create` gets the engine's answer
//! on both targets. The `SQLite` backend then starts the index backfill
//! natively, as the server does. On wasm it has no task runtime to run the
//! backfill, so it refuses the create with a `ValidationException` that names
//! the build, before the catalog commit, and the table is left as it was.
//!
//! # Checks the HTTP server makes that this build does not
//!
//! Before the server dispatches a request, it checks the body size, the
//! signature, the IAM policies, the names, ARNs and accounts the request
//! carries, and, when throttling is on, the table's throughput. Apart from
//! the IAM decision it makes these checks for every caller, in developer mode
//! too. [`Engine::dispatch`] makes none of them: the engine's handler for the
//! operation answers the request instead. For each check, the server's answer
//! and this build's answer to the same request follow. The parity corpus
//! records this build's answers, except the body-size one, which the unit
//! test `a_list_tables_body_over_16_mib_is_answered` records; nothing records
//! throttling.
//!
//! - **Body size.** A body over 16 MiB: the server answers
//!   `RequestEntityTooLargeException` (HTTP 413). This build has no limit and
//!   answers the request.
//! - **Signature and IAM policies.** The server answers its authentication
//!   and authorization errors. This build verifies no signature and
//!   evaluates no policy; every request runs under [`ACCOUNT_ID`].
//! - **Table names and table ARNs**, in `TableName` and the other fields
//!   that carry one. `GetItem` with `TableName` `bad name!`: the server
//!   answers `ValidationException` `1 validation error detected: Value 'bad
//!   name!' at 'tableName' failed to satisfy constraint: Member must satisfy
//!   regular expression pattern: [a-zA-Z0-9_.-]+`; this build,
//!   `ResourceNotFoundException`. `ListStreams` with `TableName` `ab`: the
//!   server answers `ValidationException` `TableName must be at least 3
//!   characters long and at most 255 characters long`; this build lists no
//!   streams.
//! - **A `StreamArn` that is not a stream ARN.** `DescribeStream` with a table
//!   ARN: the server answers `ValidationException` `Invalid StreamArn`; this
//!   build, `ValidationException` `Invalid stream ARN format: <the ARN>`.
//! - **A `StreamArn` outside the server's partition, service and region.**
//!   `DescribeStream` and `GetShardIterator` with a stream's ARN respelled with
//!   region `us-west-2`: the server answers `ValidationException`
//!   `Invalid StreamArn`; this build describes the stream and issues an
//!   iterator for it.
//! - **A resource named by an ARN in another account.** `GetItem` with
//!   another account's table ARN: the server answers `AccessDeniedException`,
//!   for an account it does not hold `The caller is not authorized to access
//!   the specified resource.`; this build, `ResourceNotFoundException`.
//!   `DescribeStream` with a stream's ARN respelled with another account: the
//!   server answers `AccessDeniedException`, for an account it does not hold
//!   `Access is denied`; this build describes the stream.
//! - **A tag operation's `ResourceArn` that names an index.** Both answer the
//!   same `ValidationException`; this build's comes from the tag handler.
//! - **A `BackupArn` that is not a backup ARN.** The server answers
//!   `ValidationException` `Invalid Backup ARN`; this build refuses
//!   `DescribeBackup` and `DeleteBackup` whatever the body (above).
//! - **An export `TableArn` that does not parse.** The server answers
//!   `ValidationException` `One or more parameter values were invalid:
//!   tableArn is not a valid ARN`; this build, the export handler's
//!   `ValidationException` that export is disabled.
//! - **A `GetRecords` iterator.** One that does not decode, and one from
//!   before iterators carried their stream ARN, get the same answers from both
//!   (`ValidationException` `Invalid ShardIterator` and
//!   `ExpiredIteratorException`); this build's come from the `GetRecords`
//!   handler. One whose stream ARN is outside the server's partition, service
//!   and region or the caller's account: the server answers
//!   `ValidationException` `Invalid ShardIterator`; this build reads the
//!   stream's records.
//! - **Throttling.** A server with throttling on answers
//!   `ProvisionedThroughputExceededException` to reads or writes past a
//!   table's capacity. This build never throttles.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use extenddb_core::error::{DynamoDbError, error_document};
use extenddb_core::limits::{DEFAULT_MAX_ITEM_SIZE_BYTES, LimitsConfig};
#[cfg(not(target_arch = "wasm32"))]
use extenddb_engine::BackupRuntime;
use extenddb_engine::OperationContext;
use extenddb_storage::StorageEngine;
use extenddb_storage_sqlite::SqliteEngine;
use serde_json::Value;

/// Region the build reports. Fixed because the browser build serves a single
/// in-memory database with no account or region configuration.
const REGION: &str = "us-east-1";

/// Account id every request runs under. A syntactically valid 12-digit
/// placeholder; the build authenticates no request and authorizes none.
const ACCOUNT_ID: &str = "000000000000";

/// The server address [`extenddb_engine::dispatch`] takes. Dispatch reads it
/// only to answer `DescribeEndpoints`, which this build refuses before
/// dispatch, so it is never reported.
const SERVER_ADDR: &str = "";

/// Backup job-slot count for the native backup runtime. One is enough: the
/// runtime has no backup store, and the operations that would use a slot are
/// refused before dispatch. Native only — the field it feeds is absent on wasm.
#[cfg(not(target_arch = "wasm32"))]
const BACKUP_JOB_SLOTS: usize = 1;

/// The in-process `ExtendDB` engine a JavaScript caller talks to.
///
/// Holds the shared, per-process state the engine needs (storage backend,
/// limits, identity, and — off wasm — the backup runtime). Cloning is cheap:
/// every field is an `Arc` or a small owned value, which lets the
/// `wasm-bindgen` layer hand an owned copy to each async request without
/// borrowing across the JS boundary.
#[derive(Clone)]
pub struct Engine {
    storage: Arc<dyn StorageEngine>,
    /// The process's backup job slots and (unconfigured) backup store. The
    /// store-backed backup subsystem is compiled only off wasm, so this field
    /// is absent in the wasm build; the adapter refuses the operations that
    /// read it before dispatch.
    #[cfg(not(target_arch = "wasm32"))]
    backup: Arc<BackupRuntime>,
    limits: Arc<LimitsConfig>,
    region: Arc<str>,
    account_id: Arc<str>,
}

impl Engine {
    /// Build an engine over a fresh in-browser in-memory `SQLite` database.
    ///
    /// Opens a `:memory:` `SQLite` database through the `ExtendDB` `SQLite` backend
    /// (the synchronous `sqlx`-shaped shim over `sqlite-wasm-rs` on wasm, and
    /// `sqlx` with C `SQLite` natively), runs the catalog migrations, and
    /// registers the account with a synchronous control plane.
    ///
    /// # Errors
    ///
    /// The database cannot be opened or initialized (the migrations or the
    /// account setup fail). The browser surface delivers this as a rejected
    /// `create`.
    pub async fn new() -> Result<Self, String> {
        let storage = SqliteEngine::new_in_memory(REGION, ACCOUNT_ID, DEFAULT_MAX_ITEM_SIZE_BYTES)
            .await
            .map_err(|e| format!("initialize in-memory SQLite backend: {e}"))?;
        Ok(Self {
            storage: Arc::new(storage),
            #[cfg(not(target_arch = "wasm32"))]
            backup: Arc::new(BackupRuntime::new(BACKUP_JOB_SLOTS)),
            limits: Arc::new(LimitsConfig::default()),
            region: Arc::from(REGION),
            account_id: Arc::from(ACCOUNT_ID),
        })
    }

    /// Answer one `DynamoDB` request, returning the response as a JSON string.
    ///
    /// Errors are values, not exceptions: an engine error, a malformed request
    /// body, and a request the browser build does not support all come back as
    /// a DynamoDB-shaped error document with a `__type` member — the same JSON
    /// the HTTP server would have put in the response body. The caller tells
    /// success from failure by the presence of `__type`, as a `DynamoDB` SDK
    /// does.
    ///
    /// `operation` is the `X-Amz-Target` operation name (e.g. `"DescribeLimits"`);
    /// `body_json` is the request body as a JSON string (`"{}"` for an
    /// operation that takes no parameters).
    #[must_use]
    pub async fn submit(&self, operation: &str, body_json: &str) -> String {
        let value = match self.dispatch(operation, body_json).await {
            Ok(body) => body,
            Err(err) => error_document(&err),
        };
        serde_json::to_string(&value).unwrap_or_else(|_| {
            String::from_utf8_lossy(extenddb_core::error::SERIALIZATION_FAILURE_BODY).into_owned()
        })
    }

    /// The response to a call whose `argument` (`"operation"` or `"body"`) is
    /// not a string: a `ValidationException` error document, serialized as
    /// [`Engine::submit`] serializes every answer. The `wasm-bindgen` surface
    /// accepts any JavaScript value and answers with this instead of passing a
    /// non-string into the engine.
    #[must_use]
    pub fn non_string_argument(argument: &str) -> String {
        let err = DynamoDbError::ValidationException(format!("{argument} must be a string"));
        serde_json::to_string(&error_document(&err)).unwrap_or_else(|_| {
            String::from_utf8_lossy(extenddb_core::error::SERIALIZATION_FAILURE_BODY).into_owned()
        })
    }

    /// Apply the browser-build operation policy and the engine's pre-dispatch
    /// checks, then dispatch into the engine.
    async fn dispatch(&self, operation: &str, body_json: &str) -> Result<Value, DynamoDbError> {
        // Refusals that need no request body: the store-backed backup
        // operations and PartiQL. Answered before the operation-known check so
        // a PartiQL request is refused for the stated reason rather than
        // reported as unknown — this build is compiled without the PartiQL
        // surface, so `parse_request` would report it as unknown otherwise.
        if let Some(reason) = unsupported_reason(operation) {
            return Err(unsupported(operation, reason));
        }

        // The checks every entry point applies before dispatch, in the
        // engine's order: the operation name, then the body parse, then the
        // engine's refusals of requests no deployment serves (point-in-time
        // recovery). The server authenticates between the first two steps and
        // the last; this build authenticates no request.
        let body = extenddb_engine::parse_request(operation, body_json.as_bytes())?.admit()?;

        let ctx = self.operation_context();
        let result = extenddb_engine::dispatch(operation, body, &ctx, SERVER_ADDR).await?;
        Ok(result.body)
    }

    /// Build the per-request [`OperationContext`].
    ///
    /// The server builds this per request after authentication. The browser
    /// build verifies no signature and runs no authorization, so the
    /// auth-derived fields are empty: no pre-read key info, an empty auth cache,
    /// no cached key lookup. There is no item authorizer (which would authorize
    /// the item reads and writes `PartiQL` names in its statement body) because
    /// the `PartiQL` operations are refused before dispatch. Imports and exports
    /// are disabled (empty allow-lists — the browser has no filesystem). The
    /// backup runtime is present only off wasm; the store-backed backup
    /// operations that would read it are refused before dispatch on both
    /// targets.
    fn operation_context(&self) -> OperationContext {
        OperationContext {
            storage: Arc::clone(&self.storage),
            #[cfg(not(target_arch = "wasm32"))]
            backup: Arc::clone(&self.backup),
            limits: Arc::clone(&self.limits),
            region: Arc::clone(&self.region),
            account_id: Arc::clone(&self.account_id),
            import_paths: Arc::from(Vec::<Arc<PathBuf>>::new()),
            export_paths: Arc::from(Vec::<Arc<PathBuf>>::new()),
            authorized_key_info: HashMap::new(),
            auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
            table_key_info_lookup: None,
            item_authorizer: None,
        }
    }
}

/// The reason an operation is refused outright in the browser build, or `None`
/// if it is not refused here. The store-backed backup operations need a backup
/// store the in-memory build does not have; the `PartiQL` operations are not part
/// of the build; `DescribeEndpoints` reports the address a client sends
/// requests to, and the build is called in-process with no network address.
fn unsupported_reason(operation: &str) -> Option<&'static str> {
    match operation {
        "CreateBackup" | "DescribeBackup" | "DeleteBackup" | "RestoreTableFromBackup" => {
            Some("the browser build runs entirely in memory with no backup store")
        }
        "ExecuteStatement" | "BatchExecuteStatement" | "ExecuteTransaction" => {
            Some("the PartiQL surface is not part of the browser build")
        }
        "DescribeEndpoints" => {
            Some("it is called in-process and has no network endpoint to report")
        }
        _ => None,
    }
}

/// The `ValidationException` the browser build returns for an operation it
/// cannot serve. `ValidationException` (HTTP 400) is deliberate: an SDK does not
/// retry it, unlike the 500 an `InternalServerError` would carry. The message
/// names the build so the reason is clear to a caller.
fn unsupported(operation: &str, reason: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(extenddb_core::error::webassembly_unsupported_message(
        operation, reason,
    ))
}

// These tests drive the portable runtime on a current-thread tokio runtime,
// which is a non-wasm dev-dependency, so the module compiles only off wasm.
// The wasm build is exercised through the module's wasm-bindgen surface.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    /// Build a ready in-memory engine for a test, panicking on failure.
    async fn engine() -> Engine {
        Engine::new().await.expect("in-memory engine initializes")
    }

    /// Parse a submit response, asserting it is not an error document.
    fn ok_value(out: &str) -> Value {
        let value: Value = serde_json::from_str(out).expect("response is JSON");
        assert!(
            value.get("__type").is_none(),
            "expected a success response, got an error document: {out}"
        );
        value
    }

    /// Parse a submit response, asserting it is an error document with `__type`.
    fn error_type(out: &str) -> String {
        let value: Value = serde_json::from_str(out).expect("response is JSON");
        value["__type"]
            .as_str()
            .unwrap_or_else(|| panic!("expected an error document with __type: {out}"))
            .to_owned()
    }

    /// `DescribeLimits` answers through the adapter with the engine's configured
    /// account and table limits — a no-storage operation.
    #[tokio::test]
    async fn describe_limits_answers_with_configured_limits() {
        let engine = engine().await;
        let out = engine.submit("DescribeLimits", "{}").await;
        let value = ok_value(&out);
        let limits = LimitsConfig::default();
        assert_eq!(
            value["AccountMaxReadCapacityUnits"].as_i64(),
            Some(i64::try_from(limits.per_account_max_rcu).unwrap()),
        );
        assert_eq!(
            value["TableMaxWriteCapacityUnits"].as_i64(),
            Some(i64::try_from(limits.per_table_max_wcu).unwrap()),
        );
    }

    /// Validation order: an unknown op with a malformed body is still
    /// `UnknownOperationException`, matching the server's pre-auth order.
    #[tokio::test]
    async fn an_unknown_operation_with_a_malformed_body_is_unknown_operation() {
        let engine = engine().await;
        let out = engine.submit("NoSuchOperation", "{ this is not json").await;
        assert_eq!(
            error_type(&out),
            "com.amazon.coral.service#UnknownOperationException",
            "{out}"
        );
    }

    /// The HTTP server refuses a body over 16 MiB with
    /// `RequestEntityTooLargeException` before it parses it; the adapter has
    /// no size limit, so a `ListTables` body of 17 MiB (padded with JSON
    /// whitespace) is answered.
    #[tokio::test]
    async fn a_list_tables_body_over_16_mib_is_answered() {
        let engine = engine().await;
        let body = format!("{{{}}}", " ".repeat(17 << 20));
        let out = engine.submit("ListTables", &body).await;
        assert_eq!(
            ok_value(&out),
            serde_json::json!({ "TableNames": [] }),
            "{out}"
        );
    }

    /// The answer to a non-string `submit` argument is a `ValidationException`
    /// document that names the argument.
    #[test]
    fn a_non_string_argument_answers_a_validation_exception() {
        for argument in ["operation", "body"] {
            let out = Engine::non_string_argument(argument);
            assert!(error_type(&out).ends_with("#ValidationException"), "{out}");
            let value: Value = serde_json::from_str(&out).expect("response is JSON");
            assert_eq!(
                value["message"],
                format!("{argument} must be a string"),
                "{out}"
            );
        }
    }
}
