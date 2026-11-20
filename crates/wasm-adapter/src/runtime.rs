// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The portable adapter runtime: build an [`OperationContext`] and answer a
//! `DynamoDB` request by calling the `ExtendDB` engine in-process.
//!
//! This module is target-agnostic — it compiles and runs natively (where the
//! unit tests exercise it against the native `ExtendDB` SQLite build) and on
//! `wasm32` (where [`crate::wasm`] wraps it in a `wasm-bindgen` surface). It
//! contains no `wasm-bindgen` and no network code: the "transport" is a direct
//! function call into [`extenddb_engine::dispatch`], which is what the tokio
//! TCP listener would have called after parsing an HTTP request.
//!
//! # Operations the browser build cannot serve
//!
//! Some operations need a facility the in-memory browser build does not have —
//! a backup store, a filesystem, or the `PartiQL` surface (left out of this
//! build). The policy that answers them lives here, in [`Engine::dispatch`],
//! and is applied the same way on both targets, so the native build (the
//! fidelity oracle) and the wasm build return the same thing for the same
//! request. There are three classes:
//!
//! - **Refused on both targets** with a `ValidationException` that names the
//!   build: the store-backed backup, restore and point-in-time operations
//!   (`CreateBackup`, `DescribeBackup`, `DeleteBackup`,
//!   `RestoreTableFromBackup`, `RestoreTableToPointInTime`); the three `PartiQL`
//!   operations; and an `UpdateTable` that creates a vector index (refused
//!   before any catalog write, as the build cannot run the index backfill —
//!   create the table with its vector indexes instead).
//! - **Answered with the no-store answer**: the backup and import/export
//!   operations that a server with no backup store and no import/export paths
//!   also answers without a store — `ListBackups` (empty), continuous-backups
//!   describe/update (point-in-time recovery reported and refused as disabled),
//!   and `ImportTable` / `ExportTableToPointInTime` (disabled). Natively the
//!   engine computes these; the wasm build, which does not compile those
//!   handlers, returns the identical answer from [`no_store_answer`].
//! - **Passed through**: everything else, including the data-plane operations,
//!   `DescribeTable` / `ListTables`, streams, time-to-live, tagging, and an
//!   `UpdateTable` that does not create a vector index.

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

/// Address reported by `DescribeEndpoints`. The browser has no network
/// endpoint; this is the conventional local static-server origin, used only to
/// fill the `DescribeEndpoints` response.
const SERVER_ADDR: &str = "localhost:8000";

/// Backup job-slot count for the native backup runtime. One is enough: the
/// browser build has no backup store, and the native unit tests reach only the
/// no-store answers. Native only — the field it feeds is absent on wasm.
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
    /// backup subsystem is compiled only off wasm, so this field is absent in
    /// the wasm build, where the adapter answers the backup operations before
    /// dispatch.
    #[cfg(not(target_arch = "wasm32"))]
    backup: Arc<BackupRuntime>,
    limits: Arc<LimitsConfig>,
    region: Arc<str>,
    account_id: Arc<str>,
    server_addr: String,
}

impl Engine {
    /// Build an engine over a fresh in-browser in-memory SQLite database.
    ///
    /// Opens a `:memory:` SQLite database through the `ExtendDB` SQLite backend
    /// (the synchronous `sqlx`-shaped shim over `sqlite-wasm-rs` on wasm, and
    /// `sqlx` with C SQLite natively, which is the fidelity oracle), runs the
    /// catalog migrations, and registers the account with a synchronous control
    /// plane.
    /// The data-plane operations then answer as the native `ExtendDB` SQLite
    /// build answers them.
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
            server_addr: SERVER_ADDR.to_owned(),
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
        // Refusals that need no request body: the store-backed backup/restore/
        // point-in-time operations and PartiQL. Answered before the
        // operation-known check so a PartiQL request is refused for the stated
        // reason rather than reported as unknown — this build is compiled
        // without the PartiQL surface, so `is_known_operation` would not
        // recognize it otherwise.
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
        // (crates/server/src/handler.rs). Built from the shared helper so a
        // malformed request looks identical in the browser.
        let body: Value = serde_json::from_str(body_json).map_err(|e| malformed_body_error(&e))?;

        // An UpdateTable that creates a vector index is refused here, before
        // dispatch and so before any catalog write: the build cannot run the
        // index backfill. Every other UpdateTable change passes through.
        if operation == "UpdateTable" && creates_vector_index(&body) {
            return Err(unsupported(
                "UpdateTable",
                "it cannot build a vector index; create the table with its vector indexes instead",
            ));
        }

        // The backup and import/export operations with a no-store answer. The
        // wasm build does not compile those engine handlers, so it returns the
        // same answer the native no-store path produces. Natively this falls
        // through to the real handler, which is the oracle the wasm answer is
        // checked against.
        #[cfg(target_arch = "wasm32")]
        if let Some(answer) = no_store_answer(operation, &body) {
            return answer;
        }

        let ctx = self.operation_context();
        let result = extenddb_engine::dispatch(operation, body, &ctx, &self.server_addr).await?;
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
    /// backup runtime is present only off wasm, where the backup operations
    /// reach the engine; on wasm the adapter answers them before dispatch.
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
/// of the build.
fn unsupported_reason(operation: &str) -> Option<&'static str> {
    match operation {
        "CreateBackup"
        | "DescribeBackup"
        | "DeleteBackup"
        | "RestoreTableFromBackup"
        | "RestoreTableToPointInTime" => {
            Some("the browser build runs entirely in memory with no backup store")
        }
        "ExecuteStatement" | "BatchExecuteStatement" | "ExecuteTransaction" => {
            Some("the PartiQL surface is not part of the browser build")
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

/// The answer the browser build gives for a backup or import/export operation
/// that a server with no backup store and no import/export paths answers
/// without a store. Returned only on wasm, where the engine handlers are not
/// compiled; natively the engine computes the identical answer, which is the
/// oracle these values are checked against. The messages are the native
/// no-store messages, reproduced so the two builds match.
///
/// `UpdateContinuousBackups` is the one body-sensitive case, and it reproduces
/// native's three answers (`handle_update_continuous_backups`): disabling
/// point-in-time recovery returns the description `DescribeContinuousBackups`
/// reports; enabling it is refused with `ContinuousBackupsUnavailableException`
/// (no backend implements it); and a request whose
/// `PointInTimeRecoverySpecification` is missing or whose
/// `PointInTimeRecoveryEnabled` flag is absent or malformed is refused with the
/// service's "Member must not be null" `ValidationException`, exactly as native
/// does — not treated as an enable. It does not check that the table exists
/// first (the describe handler is not compiled here), so a request naming a
/// missing table is answered from the specification rather than with
/// `TableNotFoundException`. The README's refusal section documents this
/// missing-table difference.
#[cfg(target_arch = "wasm32")]
fn no_store_answer(operation: &str, body: &Value) -> Option<Result<Value, DynamoDbError>> {
    let answer = match operation {
        "ListBackups" => Ok(serde_json::json!({ "BackupSummaries": [] })),
        "DescribeContinuousBackups" => Ok(continuous_backups_disabled_document()),
        "UpdateContinuousBackups" => match point_in_time_recovery_enabled(body) {
            Ok(false) => Ok(continuous_backups_disabled_document()),
            Ok(true) => Err(DynamoDbError::ContinuousBackupsUnavailableException(
                "Point-in-time recovery is not supported by this storage backend".to_owned(),
            )),
            Err(err) => Err(err),
        },
        "ImportTable" => Err(DynamoDbError::ValidationException(
            "Import is disabled. Configure [import] paths in extenddb.toml to enable.".to_owned(),
        )),
        "ExportTableToPointInTime" => Err(DynamoDbError::ValidationException(
            "Export is disabled. Configure [export] paths in extenddb.toml to enable.".to_owned(),
        )),
        _ => return None,
    };
    Some(answer)
}

/// The `ContinuousBackupsDescription` document for a backend with point-in-time
/// recovery off — continuous backups enabled, point-in-time recovery disabled.
/// Both `DescribeContinuousBackups` and a disabling `UpdateContinuousBackups`
/// return it, matching the native description.
#[cfg(target_arch = "wasm32")]
fn continuous_backups_disabled_document() -> Value {
    serde_json::json!({
        "ContinuousBackupsDescription": {
            "ContinuousBackupsStatus": "ENABLED",
            "PointInTimeRecoveryDescription": { "PointInTimeRecoveryStatus": "DISABLED" }
        }
    })
}

/// `PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled` from an
/// `UpdateContinuousBackups` body, reproducing the native reader in
/// `handle_update_continuous_backups`: the boolean flag, or the JSON strings
/// `"true"`/`"false"` the service also accepts as the booleans. A request
/// without the specification, or with one that does not hold the flag as one
/// of those forms, is refused with the service's "Member must not be null"
/// text at the member's model path — the same `ValidationException` native
/// returns, so a malformed request is answered identically on both builds.
#[cfg(target_arch = "wasm32")]
fn point_in_time_recovery_enabled(body: &Value) -> Result<bool, DynamoDbError> {
    let spec = body
        .get("PointInTimeRecoverySpecification")
        .filter(|v| !v.is_null())
        .ok_or_else(|| member_null("pointInTimeRecoverySpecification"))?;
    match spec.get("PointInTimeRecoveryEnabled") {
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(Value::String(s)) if s == "true" => Ok(true),
        Some(Value::String(s)) if s == "false" => Ok(false),
        _ => Err(member_null(
            "pointInTimeRecoverySpecification.pointInTimeRecoveryEnabled",
        )),
    }
}

/// The service's refusal for a required member given as null, by its model
/// path — the message native's `member_null` produces.
#[cfg(target_arch = "wasm32")]
fn member_null(path: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "1 validation error detected: Value null at '{path}' \
         failed to satisfy constraint: Member must not be null"
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

    /// `DescribeEndpoints` answers with the build's reported server address.
    #[tokio::test]
    async fn describe_endpoints_answers_with_the_server_address() {
        let engine = engine().await;
        let out = engine.submit("DescribeEndpoints", "{}").await;
        let value = ok_value(&out);
        let endpoints = value["Endpoints"].as_array().expect("Endpoints array");
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0]["Address"].as_str(), Some(SERVER_ADDR));
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

    /// A store-backed backup operation is refused with a `ValidationException`
    /// that names the WebAssembly build — not an `InternalServerError`, which an
    /// SDK would retry. The adapter applies this on both targets.
    #[tokio::test]
    async fn create_backup_is_unsupported() {
        let engine = engine().await;
        create_products(&engine).await;
        let out = engine
            .submit(
                "CreateBackup",
                r#"{"TableName":"Products","BackupName":"snap"}"#,
            )
            .await;
        assert_eq!(
            error_type(&out),
            "com.amazon.coral.validate#ValidationException",
            "{out}"
        );
        let value: Value = serde_json::from_str(&out).unwrap();
        assert!(
            value["message"]
                .as_str()
                .unwrap_or_default()
                .contains("ExtendDB WebAssembly build"),
            "the message must name the build: {out}"
        );
    }

    /// A `PartiQL` operation is refused with a `ValidationException` naming the
    /// build, even though this build is compiled without the `PartiQL` surface
    /// (so the engine would otherwise report it as unknown). Refused before the
    /// body is parsed, so a malformed statement body does not change the answer.
    #[tokio::test]
    async fn execute_statement_is_unsupported() {
        let engine = engine().await;
        let out = engine
            .submit(
                "ExecuteStatement",
                r#"{"Statement":"SELECT * FROM Products"}"#,
            )
            .await;
        assert_eq!(
            error_type(&out),
            "com.amazon.coral.validate#ValidationException",
            "{out}"
        );
        let value: Value = serde_json::from_str(&out).unwrap();
        assert!(
            value["message"]
                .as_str()
                .unwrap_or_default()
                .contains("ExtendDB WebAssembly build"),
            "the message must name the build: {out}"
        );
        // A malformed body does not change the refusal (refused before parsing).
        let out = engine.submit("ExecuteStatement", "{ not json").await;
        assert_eq!(
            error_type(&out),
            "com.amazon.coral.validate#ValidationException",
            "{out}"
        );
    }

    /// An `UpdateTable` that creates a vector index is refused with a
    /// `ValidationException` naming the build, and because the refusal lands
    /// before dispatch — before any catalog write — the instance keeps serving:
    /// a following `PutItem`/`GetItem` round-trip on the same table succeeds.
    #[tokio::test]
    async fn update_table_creating_a_vector_index_is_refused_and_the_instance_still_serves() {
        let engine = engine().await;
        create_products(&engine).await;
        let out = engine
            .submit(
                "UpdateTable",
                r#"{"TableName":"Products",
                    "VectorIndexUpdates":[{"Create":{"IndexName":"vec",
                        "VectorAttributeName":"embedding","Dimensions":3,
                        "DistanceMetric":"cosine"}}]}"#,
            )
            .await;
        assert_eq!(
            error_type(&out),
            "com.amazon.coral.validate#ValidationException",
            "{out}"
        );
        let value: Value = serde_json::from_str(&out).unwrap();
        assert!(
            value["message"]
                .as_str()
                .unwrap_or_default()
                .contains("ExtendDB WebAssembly build"),
            "the message must name the build: {out}"
        );
        // The table still serves writes and reads after the refusal.
        ok_value(
            &engine
                .submit(
                    "PutItem",
                    r#"{"TableName":"Products","Item":{"productId":{"S":"p1"},"name":{"S":"Widget"}}}"#,
                )
                .await,
        );
        let out = engine
            .submit(
                "GetItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"p1"}}}"#,
            )
            .await;
        assert_eq!(
            ok_value(&out)["Item"]["name"]["S"].as_str(),
            Some("Widget"),
            "the instance must still serve after the refusal: {out}"
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

    /// `ListBackups` with no backup store answers with an empty list. Natively
    /// this is the engine's no-store answer; the wasm build returns the same
    /// document from `no_store_answer`.
    #[tokio::test]
    async fn list_backups_returns_an_empty_list() {
        let engine = engine().await;
        let out = engine.submit("ListBackups", "{}").await;
        let value = ok_value(&out);
        assert_eq!(
            value["BackupSummaries"].as_array().map(Vec::len),
            Some(0),
            "{out}"
        );
        assert!(
            value.get("LastEvaluatedBackupArn").is_none(),
            "an empty listing has no cursor: {out}"
        );
    }

    /// `DescribeContinuousBackups` reports point-in-time recovery as disabled.
    /// Natively this is the engine's answer for an existing table; the wasm
    /// build returns the same document from `no_store_answer`.
    #[tokio::test]
    async fn describe_continuous_backups_reports_disabled() {
        let engine = engine().await;
        create_products(&engine).await;
        let out = engine
            .submit("DescribeContinuousBackups", r#"{"TableName":"Products"}"#)
            .await;
        let value = ok_value(&out);
        assert_eq!(
            value["ContinuousBackupsDescription"]["ContinuousBackupsStatus"].as_str(),
            Some("ENABLED"),
            "{out}"
        );
        assert_eq!(
            value["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
                ["PointInTimeRecoveryStatus"]
                .as_str(),
            Some("DISABLED"),
            "{out}"
        );
    }

    /// Enabling point-in-time recovery through `UpdateContinuousBackups` is
    /// refused with `ContinuousBackupsUnavailableException`. Natively this is
    /// the engine's answer; the wasm build returns the same error from
    /// `no_store_answer`.
    #[tokio::test]
    async fn update_continuous_backups_enable_is_unavailable() {
        let engine = engine().await;
        create_products(&engine).await;
        let out = engine
            .submit(
                "UpdateContinuousBackups",
                r#"{"TableName":"Products",
                    "PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true}}"#,
            )
            .await;
        assert_eq!(
            error_type(&out),
            "com.amazonaws.dynamodb.v20120810#ContinuousBackupsUnavailableException",
            "{out}"
        );
    }

    /// `ImportTable` is disabled (no import paths). Natively this is the
    /// engine's answer; the wasm build returns the same `ValidationException`
    /// from `no_store_answer`.
    #[tokio::test]
    async fn import_table_is_disabled() {
        let engine = engine().await;
        let out = engine
            .submit(
                "ImportTable",
                r#"{"TableCreationParameters":{"TableName":"T",
                    "KeySchema":[{"AttributeName":"k","KeyType":"HASH"}],
                    "AttributeDefinitions":[{"AttributeName":"k","AttributeType":"S"}]},
                    "InputFormat":"DYNAMODB_JSON",
                    "S3BucketSource":{"S3Bucket":"b"}}"#,
            )
            .await;
        assert_eq!(
            error_type(&out),
            "com.amazon.coral.validate#ValidationException",
            "{out}"
        );
        let value: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            value["message"].as_str(),
            Some("Import is disabled. Configure [import] paths in extenddb.toml to enable."),
            "{out}"
        );
    }

    /// `ExportTableToPointInTime` is disabled (no export paths). Natively this
    /// is the engine's answer; the wasm build returns the same
    /// `ValidationException` from `no_store_answer`.
    #[tokio::test]
    async fn export_table_is_disabled() {
        let engine = engine().await;
        let out = engine
            .submit(
                "ExportTableToPointInTime",
                r#"{"TableArn":"arn:aws:dynamodb:us-east-1:000000000000:table/Products",
                    "S3Bucket":"b","ExportFormat":"DYNAMODB_JSON"}"#,
            )
            .await;
        assert_eq!(
            error_type(&out),
            "com.amazon.coral.validate#ValidationException",
            "{out}"
        );
        let value: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            value["message"].as_str(),
            Some("Export is disabled. Configure [export] paths in extenddb.toml to enable."),
            "{out}"
        );
    }
}
