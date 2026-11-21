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
//!   `PartiQL` operations; `DescribeEndpoints`, as the build has no network
//!   address to report; and an `UpdateTable` that creates a vector index
//!   (refused before any catalog write, as the build cannot run the index
//!   backfill — create the table with its vector indexes instead).
//! - **Refused as the server refuses them**: every
//!   `RestoreTableToPointInTime`, and an `UpdateContinuousBackups` that enables
//!   point-in-time recovery, through
//!   [`extenddb_engine::refuse_point_in_time_recovery`], called where the
//!   server calls it: after the body is parsed (the server also authenticates
//!   first; this build authenticates no request) and before any table is read.
//! - **Passed through** to the engine's handlers: everything else, including
//!   the data-plane operations, `DescribeTable` / `ListTables`, streams,
//!   time-to-live, tagging, an `UpdateTable` that does not create a vector
//!   index, `ListBackups`, `DescribeContinuousBackups`, an
//!   `UpdateContinuousBackups` that does not enable point-in-time recovery, and
//!   `ImportTable` / `ExportTableToPointInTime`.
//!   The wasm build compiles those handlers too, so a malformed request gets
//!   the handler's own validation answer on both targets. The adapter
//!   configures no backup store and no import or export paths, so `ListBackups`
//!   lists nothing and the import and export handlers answer that the
//!   operation is disabled.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use extenddb_core::error::{DynamoDbError, error_document, malformed_body_error};
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

/// The in-process `ExtendDB` engine the browser app talks to.
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

    /// Apply the browser-build operation policy, then validate the operation,
    /// parse the body, and dispatch into the engine.
    async fn dispatch(&self, operation: &str, body_json: &str) -> Result<Value, DynamoDbError> {
        // Refusals that need no request body: the store-backed backup
        // operations and PartiQL. Answered before the operation-known check so
        // a PartiQL request is refused for the stated reason rather than
        // reported as unknown — this build is compiled without the PartiQL
        // surface, so `is_known_operation` would not recognize it otherwise.
        if let Some(reason) = unsupported_reason(operation) {
            return Err(unsupported(operation, reason));
        }

        // Mirror the server's pre-auth order (crates/server/src/handler.rs): the
        // operation name is validated before the body is parsed, so an unknown
        // operation returns UnknownOperationException even when the body is also
        // malformed. Parsing first would answer that case with
        // SerializationException instead.
        if !extenddb_engine::is_known_operation(operation) {
            return Err(DynamoDbError::UnknownOperationException(String::new()));
        }

        // The HTTP server parses the raw body the same way and answers a parse
        // failure with this SerializationException before auth
        // (crates/server/src/handler.rs). Built from the same shared helper as
        // the server's, so both build the error from one function.
        let body: Value = serde_json::from_str(body_json).map_err(|e| malformed_body_error(&e))?;

        // The server refuses a point-in-time restore and an enable at this
        // point, after the body parse and before any table is read
        // (crates/server/src/handler.rs). Calling the same engine function
        // here gives both builds the server's answer.
        if let Some(err) = extenddb_engine::refuse_point_in_time_recovery(operation, &body) {
            return Err(err);
        }

        // An UpdateTable that creates a vector index is refused here, before
        // dispatch and so before any catalog write: the build cannot run the
        // index backfill. Every other UpdateTable change passes through.
        if operation == "UpdateTable" && creates_vector_index(&body) {
            return Err(unsupported(
                "UpdateTable",
                "it cannot build a vector index; create the table with its vector indexes instead",
            ));
        }

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
    DynamoDbError::ValidationException(format!(
        "{operation} is not supported by the ExtendDB WebAssembly build: {reason}."
    ))
}

/// Whether an `UpdateTable` body carries a `VectorIndexUpdates` entry that
/// creates an index. Inspected as JSON, before deserialization, so the refusal
/// lands before dispatch and any catalog write; a non-null `Create` member on
/// any entry is a create.
fn creates_vector_index(body: &Value) -> bool {
    body.get("VectorIndexUpdates")
        .and_then(Value::as_array)
        .is_some_and(|updates| {
            updates
                .iter()
                .any(|update| update.get("Create").is_some_and(|create| !create.is_null()))
        })
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

    /// Create a simple single-key `Products` table via the engine.
    async fn create_products(engine: &Engine) {
        let out = engine
            .submit(
                "CreateTable",
                r#"{"TableName":"Products",
                    "KeySchema":[{"AttributeName":"productId","KeyType":"HASH"}],
                    "AttributeDefinitions":[{"AttributeName":"productId","AttributeType":"S"}],
                    "BillingMode":"PAY_PER_REQUEST"}"#,
            )
            .await;
        let value = ok_value(&out);
        assert_eq!(
            value["TableDescription"]["TableName"].as_str(),
            Some("Products"),
            "{out}"
        );
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

    /// An `UpdateTable` that does not create a vector index is not refused by
    /// the adapter — it passes through to the engine (its outcome is the
    /// engine's, never the build's "not supported" answer).
    #[tokio::test]
    async fn update_table_without_a_vector_create_is_not_refused() {
        let engine = engine().await;
        create_products(&engine).await;
        let out = engine
            .submit(
                "UpdateTable",
                r#"{"TableName":"Products","DeletionProtectionEnabled":true}"#,
            )
            .await;
        assert!(
            !out.contains("ExtendDB WebAssembly build"),
            "a non-vector-create UpdateTable must pass through, not be refused: {out}"
        );
    }
}
