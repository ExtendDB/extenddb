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
        if let Some(answer) = no_store_answer(operation) {
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
#[cfg(target_arch = "wasm32")]
fn no_store_answer(operation: &str) -> Option<Result<Value, DynamoDbError>> {
    let answer = match operation {
        "ListBackups" => Ok(serde_json::json!({ "BackupSummaries": [] })),
        "DescribeContinuousBackups" => Ok(serde_json::json!({
            "ContinuousBackupsDescription": {
                "ContinuousBackupsStatus": "ENABLED",
                "PointInTimeRecoveryDescription": { "PointInTimeRecoveryStatus": "DISABLED" }
            }
        })),
        "UpdateContinuousBackups" => Err(DynamoDbError::ContinuousBackupsUnavailableException(
            "Point-in-time recovery is not supported by this storage backend".to_owned(),
        )),
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

    /// An unknown operation comes back as a `DynamoDB` error document.
    #[tokio::test]
    async fn an_unknown_operation_is_an_error_document() {
        let engine = engine().await;
        let out = engine.submit("NoSuchOperation", "{}").await;
        assert_eq!(
            error_type(&out),
            "com.amazon.coral.service#UnknownOperationException",
            "{out}"
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

    /// A malformed body on a known operation is a `SerializationException`.
    #[tokio::test]
    async fn a_malformed_body_is_a_serialization_exception() {
        let engine = engine().await;
        let out = engine.submit("DescribeLimits", "{ this is not json").await;
        assert_eq!(
            error_type(&out),
            "com.amazon.coral.service#SerializationException",
            "{out}"
        );
    }

    /// The full data-plane round-trip through the in-memory SQLite backend:
    /// `CreateTable`, `PutItem`, `GetItem`, Scan, `UpdateItem`, `DeleteItem`, then `GetItem`
    /// again to confirm the delete.
    #[tokio::test]
    async fn data_plane_round_trip_put_get_scan_update_delete() {
        let engine = engine().await;
        create_products(&engine).await;

        let out = engine
            .submit(
                "PutItem",
                r#"{"TableName":"Products",
                    "Item":{"productId":{"S":"p1"},"name":{"S":"Widget"},"priceCents":{"N":"1999"}}}"#,
            )
            .await;
        ok_value(&out);

        let out = engine
            .submit(
                "GetItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"p1"}}}"#,
            )
            .await;
        let value = ok_value(&out);
        assert_eq!(value["Item"]["name"]["S"].as_str(), Some("Widget"), "{out}");
        assert_eq!(
            value["Item"]["priceCents"]["N"].as_str(),
            Some("1999"),
            "{out}"
        );

        let out = engine.submit("Scan", r#"{"TableName":"Products"}"#).await;
        assert_eq!(ok_value(&out)["Count"].as_i64(), Some(1), "{out}");

        let out = engine
            .submit(
                "UpdateItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"p1"}},
                    "UpdateExpression":"SET priceCents = :p",
                    "ExpressionAttributeValues":{":p":{"N":"2499"}}}"#,
            )
            .await;
        ok_value(&out);
        let out = engine
            .submit(
                "GetItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"p1"}}}"#,
            )
            .await;
        assert_eq!(
            ok_value(&out)["Item"]["priceCents"]["N"].as_str(),
            Some("2499"),
            "{out}"
        );

        let out = engine
            .submit(
                "DeleteItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"p1"}}}"#,
            )
            .await;
        ok_value(&out);
        let out = engine
            .submit(
                "GetItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"p1"}}}"#,
            )
            .await;
        assert!(
            ok_value(&out).get("Item").is_none(),
            "expected no Item after delete, got: {out}"
        );
    }

    /// `Query` on a composite-key table returns the matching partition's items.
    #[tokio::test]
    async fn query_returns_partition_items() {
        let engine = engine().await;
        let out = engine
            .submit(
                "CreateTable",
                r#"{"TableName":"Cart",
                    "KeySchema":[{"AttributeName":"accountId","KeyType":"HASH"},
                                 {"AttributeName":"productId","KeyType":"RANGE"}],
                    "AttributeDefinitions":[{"AttributeName":"accountId","AttributeType":"S"},
                                            {"AttributeName":"productId","AttributeType":"S"}],
                    "BillingMode":"PAY_PER_REQUEST"}"#,
            )
            .await;
        ok_value(&out);
        for pid in ["p1", "p2"] {
            let body = format!(
                r#"{{"TableName":"Cart","Item":{{"accountId":{{"S":"a1"}},"productId":{{"S":"{pid}"}},"qty":{{"N":"3"}}}}}}"#
            );
            ok_value(&engine.submit("PutItem", &body).await);
        }
        let out = engine
            .submit(
                "Query",
                r#"{"TableName":"Cart",
                    "KeyConditionExpression":"accountId = :a",
                    "ExpressionAttributeValues":{":a":{"S":"a1"}}}"#,
            )
            .await;
        assert_eq!(ok_value(&out)["Count"].as_i64(), Some(2), "{out}");
    }

    /// A conditional `UpdateItem` whose condition holds is applied.
    #[tokio::test]
    async fn conditional_update_accepts_when_condition_holds() {
        let engine = engine().await;
        create_products(&engine).await;
        ok_value(
            &engine
                .submit(
                    "PutItem",
                    r#"{"TableName":"Products","Item":{"productId":{"S":"p1"},"priceCents":{"N":"1000"}}}"#,
                )
                .await,
        );
        let out = engine
            .submit(
                "UpdateItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"p1"}},
                    "UpdateExpression":"SET priceCents = :new",
                    "ConditionExpression":"priceCents = :expected",
                    "ExpressionAttributeValues":{":new":{"N":"1200"},":expected":{"N":"1000"}}}"#,
            )
            .await;
        ok_value(&out);
        let out = engine
            .submit(
                "GetItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"p1"}}}"#,
            )
            .await;
        assert_eq!(
            ok_value(&out)["Item"]["priceCents"]["N"].as_str(),
            Some("1200"),
            "{out}"
        );
    }

    /// A conditional `UpdateItem` whose condition fails is rejected with
    /// `ConditionalCheckFailedException` and leaves the item unchanged. This is
    /// the error-mapping path a condition failure takes
    /// (`StorageError::ConditionFailed` → the `DynamoDB` error document).
    #[tokio::test]
    async fn conditional_update_rejects_when_condition_fails() {
        let engine = engine().await;
        create_products(&engine).await;
        ok_value(
            &engine
                .submit(
                    "PutItem",
                    r#"{"TableName":"Products","Item":{"productId":{"S":"p1"},"priceCents":{"N":"1000"}}}"#,
                )
                .await,
        );
        let out = engine
            .submit(
                "UpdateItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"p1"}},
                    "UpdateExpression":"SET priceCents = :new",
                    "ConditionExpression":"priceCents = :expected",
                    "ExpressionAttributeValues":{":new":{"N":"1200"},":expected":{"N":"999"}}}"#,
            )
            .await;
        assert_eq!(
            error_type(&out),
            "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
            "{out}"
        );
        let out = engine
            .submit(
                "GetItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"p1"}}}"#,
            )
            .await;
        assert_eq!(
            ok_value(&out)["Item"]["priceCents"]["N"].as_str(),
            Some("1000"),
            "unchanged after a refused conditional update: {out}"
        );
    }

    /// `GetItem` for an absent key returns a success response with no `Item`.
    #[tokio::test]
    async fn get_item_absent_key_returns_no_item() {
        let engine = engine().await;
        create_products(&engine).await;
        let out = engine
            .submit(
                "GetItem",
                r#"{"TableName":"Products","Key":{"productId":{"S":"missing"}}}"#,
            )
            .await;
        assert!(
            ok_value(&out).get("Item").is_none(),
            "absent key must yield no Item: {out}"
        );
    }

    /// An operation against a table that does not exist is a
    /// `ResourceNotFoundException`.
    #[tokio::test]
    async fn operation_on_missing_table_is_resource_not_found() {
        let engine = engine().await;
        let out = engine
            .submit(
                "GetItem",
                r#"{"TableName":"Nope","Key":{"productId":{"S":"p1"}}}"#,
            )
            .await;
        assert_eq!(
            error_type(&out),
            "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
            "{out}"
        );
    }

    /// Create an `Account` table and seed one account (PK `accountId`;
    /// `storeCreditCents` + `accruedBalanceCents`).
    async fn create_account(engine: &Engine, store_credit: i64, accrued: i64) {
        ok_value(
            &engine
                .submit(
                    "CreateTable",
                    r#"{"TableName":"Account",
                        "KeySchema":[{"AttributeName":"accountId","KeyType":"HASH"}],
                        "AttributeDefinitions":[{"AttributeName":"accountId","AttributeType":"S"}],
                        "BillingMode":"PAY_PER_REQUEST"}"#,
                )
                .await,
        );
        let body = format!(
            r#"{{"TableName":"Account","Item":{{"accountId":{{"S":"demo"}},
                "storeCreditCents":{{"N":"{store_credit}"}},
                "accruedBalanceCents":{{"N":"{accrued}"}}}}}}"#
        );
        ok_value(&engine.submit("PutItem", &body).await);
    }

    /// Arithmetic in an `UpdateExpression SET` (draw credit, accrue the
    /// remainder) guarded by a comparison-only `ConditionExpression`: when the
    /// balance stays within the cap the write applies and `ALL_NEW` reports the
    /// drawn-down credit and accrued remainder.
    #[tokio::test]
    async fn conditional_update_with_arithmetic_accepts_within_cap() {
        let engine = engine().await;
        create_account(&engine, 50_000, 0).await;
        let total_cents = 60_000_i64;
        let credit_used = 50_000_i64;
        let remainder = total_cents - credit_used;
        let max_current_accrued = 100_000 - remainder;
        let body = format!(
            r#"{{"TableName":"Account","Key":{{"accountId":{{"S":"demo"}}}},
                "UpdateExpression":"SET storeCreditCents = storeCreditCents - :creditUsed, accruedBalanceCents = accruedBalanceCents + :remainder",
                "ConditionExpression":"accruedBalanceCents <= :maxCurrentAccrued AND storeCreditCents >= :creditUsed",
                "ExpressionAttributeValues":{{
                    ":creditUsed":{{"N":"{credit_used}"}},
                    ":remainder":{{"N":"{remainder}"}},
                    ":maxCurrentAccrued":{{"N":"{max_current_accrued}"}}}},
                "ReturnValues":"ALL_NEW"}}"#
        );
        let out = engine.submit("UpdateItem", &body).await;
        let value = ok_value(&out);
        assert_eq!(
            value["Attributes"]["storeCreditCents"]["N"].as_str(),
            Some("0"),
            "credit fully drawn: {out}"
        );
        assert_eq!(
            value["Attributes"]["accruedBalanceCents"]["N"].as_str(),
            Some("10000"),
            "remainder accrued: {out}"
        );
    }

    /// The same arithmetic+conditional write is refused with
    /// `ConditionalCheckFailedException` when it would push the accrued balance
    /// over the cap, and the account is left unchanged.
    #[tokio::test]
    async fn conditional_update_with_arithmetic_rejects_over_cap() {
        let engine = engine().await;
        create_account(&engine, 0, 95_000).await;
        let total_cents = 10_000_i64;
        let credit_used = 0_i64;
        let remainder = total_cents - credit_used;
        let max_current_accrued = 100_000 - remainder;
        let body = format!(
            r#"{{"TableName":"Account","Key":{{"accountId":{{"S":"demo"}}}},
                "UpdateExpression":"SET storeCreditCents = storeCreditCents - :creditUsed, accruedBalanceCents = accruedBalanceCents + :remainder",
                "ConditionExpression":"accruedBalanceCents <= :maxCurrentAccrued AND storeCreditCents >= :creditUsed",
                "ExpressionAttributeValues":{{
                    ":creditUsed":{{"N":"{credit_used}"}},
                    ":remainder":{{"N":"{remainder}"}},
                    ":maxCurrentAccrued":{{"N":"{max_current_accrued}"}}}}}}"#
        );
        let out = engine.submit("UpdateItem", &body).await;
        assert_eq!(
            error_type(&out),
            "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
            "{out}"
        );
        let out = engine
            .submit(
                "GetItem",
                r#"{"TableName":"Account","Key":{"accountId":{"S":"demo"}}}"#,
            )
            .await;
        let value = ok_value(&out);
        assert_eq!(
            value["Item"]["accruedBalanceCents"]["N"].as_str(),
            Some("95000"),
            "accrued unchanged after refused checkout: {out}"
        );
        assert_eq!(
            value["Item"]["storeCreditCents"]["N"].as_str(),
            Some("0"),
            "credit unchanged after refused checkout: {out}"
        );
    }

    /// A `PutItem` guarded by `attribute_not_exists(<pk>)` creates the item the
    /// first time and is refused with `ConditionalCheckFailedException` the
    /// second time — create-once semantics.
    #[tokio::test]
    async fn put_item_attribute_not_exists_creates_once() {
        let engine = engine().await;
        ok_value(
            &engine
                .submit(
                    "CreateTable",
                    r#"{"TableName":"Account",
                        "KeySchema":[{"AttributeName":"accountId","KeyType":"HASH"}],
                        "AttributeDefinitions":[{"AttributeName":"accountId","AttributeType":"S"}],
                        "BillingMode":"PAY_PER_REQUEST"}"#,
                )
                .await,
        );
        let put = r#"{"TableName":"Account",
                      "Item":{"accountId":{"S":"demo"},"storeCreditCents":{"N":"50000"},
                              "accruedBalanceCents":{"N":"0"}},
                      "ConditionExpression":"attribute_not_exists(accountId)"}"#;
        ok_value(&engine.submit("PutItem", put).await);
        let out = engine.submit("PutItem", put).await;
        assert_eq!(
            error_type(&out),
            "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
            "{out}"
        );
    }

    /// Creating a table whose name is already taken is a
    /// `ResourceInUseException`, not a silent success. This runs natively, so
    /// it exercises the real `sqlx` + C SQLite unique-violation classification
    /// (`StorageError::TableAlreadyExists` → `ResourceInUseException`).
    #[tokio::test]
    async fn create_table_with_taken_name_is_resource_in_use() {
        let engine = engine().await;
        create_products(&engine).await;
        let out = engine
            .submit(
                "CreateTable",
                r#"{"TableName":"Products",
                    "KeySchema":[{"AttributeName":"productId","KeyType":"HASH"}],
                    "AttributeDefinitions":[{"AttributeName":"productId","AttributeType":"S"}],
                    "BillingMode":"PAY_PER_REQUEST"}"#,
            )
            .await;
        assert_eq!(
            error_type(&out),
            "com.amazonaws.dynamodb.v20120810#ResourceInUseException",
            "{out}"
        );
    }

    /// `DescribeTable` on a freshly created table reports `TableStatus: ACTIVE`:
    /// the in-memory control plane is synchronous, so a created table is ACTIVE
    /// at once and a status poll returns immediately.
    #[tokio::test]
    async fn describe_table_reports_active_immediately_in_memory() {
        let engine = engine().await;
        create_products(&engine).await;
        let out = engine
            .submit("DescribeTable", r#"{"TableName":"Products"}"#)
            .await;
        assert_eq!(
            ok_value(&out)["Table"]["TableStatus"].as_str(),
            Some("ACTIVE"),
            "a freshly created in-memory table must be ACTIVE at once: {out}"
        );
    }

    // --- Operations the browser build cannot serve (the op-policy) ---

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
