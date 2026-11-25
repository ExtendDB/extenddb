// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The server's own throttle refusal, driven through `handle_request` against
//! an in-memory engine: the error class, the reasons field and its casing, the
//! reason text and the table ARN the body carries for each bucket that refuses.
//! Also the metrics `handle_request` records for a batch a backend throttled,
//! against the engine tests' storage double.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use extenddb_auth::{AuthIdentity, AuthProvider};
use extenddb_core::error::{
    DynamoDbError, MAX_ON_DEMAND_THROUGHPUT_MESSAGE, PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
    THROTTLING_EXCEPTION_MESSAGE,
};
use serde_json::{Value, json};

use crate::AppState;

/// Auth stub returning a fixed user identity without SigV4, so the test drives
/// the handler below the signature layer while the throttle segment, the
/// dispatch and the response building run as in production. `dev_mode` opens
/// the IAM decision for the authenticated caller.
struct StubAuth {
    account_id: String,
}

#[async_trait::async_trait]
impl AuthProvider for StubAuth {
    async fn authenticate(
        &self,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<AuthIdentity, DynamoDbError> {
        Ok(AuthIdentity::User {
            account_id: self.account_id.clone(),
            user_name: "dev".to_owned(),
        })
    }
}

fn cache_cfg(name: &'static str) -> extenddb_cache::SwrCacheConfig {
    extenddb_cache::SwrCacheConfig {
        ttl: std::time::Duration::from_secs(60),
        soft_ttl: std::time::Duration::from_secs(30),
        negative_ttl: std::time::Duration::from_secs(5),
        max_entries: 128,
        name,
    }
}

/// An `AppState` on a fresh in-memory SQLite engine with a bootstrapped
/// catalog, the stub auth bound to the default account, throttling on, and
/// `dev_mode` on. Returns the state and the account id the stub answers with,
/// so the expected ARN can be formed.
pub(super) async fn build_state() -> (Arc<AppState>, String) {
    build_state_with(None, stub_auth, None).await
}

/// The stub auth provider bound to `account_id`: `build_state`'s provider,
/// and the one for a test that replaces the storage but not the auth.
pub(super) fn stub_auth(account_id: String) -> Arc<dyn AuthProvider> {
    Arc::new(StubAuth { account_id })
}

/// `build_state` with `storage`, when given, in place of the SQLite engine
/// for the table and item calls (the catalog and the authorization stores
/// stay on SQLite), with the auth provider built by `auth` from the
/// bootstrapped default account id, and with an optional request observer,
/// for a test that needs its own provider or observer.
pub(super) async fn build_state_with(
    storage: Option<Arc<dyn extenddb_storage::StorageEngine>>,
    auth: impl FnOnce(String) -> Arc<dyn AuthProvider>,
    request_observer: Option<Arc<dyn crate::extensions::RequestObserver>>,
) -> (Arc<AppState>, String) {
    let _ = extenddb_storage::set_backend(extenddb_storage_sqlite::backend());
    let config = extenddb_storage_sqlite::SqliteConfig {
        path: ":memory:".to_owned(),
        pool_size: 2,
    };
    let mut options = extenddb_storage::server_components::ServerComponentsOptions::default();
    options.bootstrap_if_uninitialized = true;
    let components = extenddb_storage::create_server_components(&config, "us-east-1", options)
        .await
        .expect("in-memory server components");
    let account_id = components
        .catalog_store
        .default_account_id()
        .await
        .expect("default account query")
        .expect("bootstrapped default account");

    let authz_cache = Arc::new(crate::CachedAuthzStore::pass_through(
        components.catalog_store.clone()
            as Arc<dyn extenddb_storage::authorization_store::AuthorizationStore>,
        crate::AuthzCacheConfig {
            identity_policies: cache_cfg("identity_policies"),
            group_policies: cache_cfg("group_policies"),
            boundary: cache_cfg("boundary"),
            principal_tags: cache_cfg("principal_tags"),
            resource_tags: cache_cfg("resource_tags"),
            session_data: cache_cfg("session_data"),
        },
    ));
    let engine = storage.unwrap_or_else(|| components.engine.clone());
    let table_key_info_cache = Arc::new(crate::CachedTableKeyInfoStore::pass_through(
        engine.clone(),
        cache_cfg("table_key_info"),
    ));
    let limits = Arc::new(extenddb_core::limits::LimitsConfig::default());
    let empty_paths: Arc<[Arc<std::path::PathBuf>]> = Arc::from(Vec::new());
    let throttle = Arc::new(extenddb_core::throttle::ThrottleManager::new(
        limits.per_account_max_rcu,
        limits.per_account_max_wcu,
        true,
    ));

    let state = Arc::new(AppState {
        storage: engine,
        backup_runtime: Arc::new(extenddb_engine::BackupRuntime::new(1)),
        auth: auth(account_id.clone()),
        throttle,
        limits,
        region: Arc::from("us-east-1"),
        server_addr: "localhost:0".to_owned(),
        catalog_store: Some(components.catalog_store),
        version_info: Arc::from("test"),
        metrics: Arc::new(extenddb_core::metrics::MetricsCollector::new()),
        tls_enabled: false,
        request_timeout: std::time::Duration::from_secs(30),
        dev_mode: true,
        import_paths: empty_paths.clone(),
        export_paths: empty_paths,
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        authz_cache,
        table_key_info_cache,
        config_entries: Vec::new(),
        docs_store: None,
        request_observer,
        admission: crate::registration::Admission::default(),
    });
    (state, account_id)
}

pub(super) async fn call(state: &Arc<AppState>, operation: &str, body: Value) -> (u16, Value) {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        format!("DynamoDB_20120810.{operation}")
            .parse()
            .expect("target header"),
    );
    let response = super::handle_request(
        State(state.clone()),
        "/".parse().expect("uri"),
        axum::extract::ConnectInfo(std::net::SocketAddr::from(([127, 0, 0, 1], 40000))),
        headers,
        Ok(Bytes::from(body.to_string())),
    )
    .await;
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("read body");
    let body: Value = serde_json::from_slice(&bytes).expect("json body");
    (status, body)
}

/// Create `table` with the given billing fields and wait for ACTIVE. No
/// worker runs in this test, so the CREATING to ACTIVE transition is driven
/// here.
pub(super) async fn create_table(state: &Arc<AppState>, table: &str, billing: Value) {
    let mut create = json!({
        "TableName": table,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
    });
    for (k, v) in billing.as_object().expect("billing object") {
        create[k] = v.clone();
    }
    let (status, body) = call(state, "CreateTable", create).await;
    assert_eq!(status, 200, "{body}");
    let describe = json!({"TableName": table});
    for _ in 0..200 {
        state
            .storage
            .process_control_plane_transitions()
            .await
            .expect("transitions");
        let (_, body) = call(state, "DescribeTable", describe.clone()).await;
        if body["Table"]["TableStatus"] == "ACTIVE" {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("table {table} did not become ACTIVE");
}

pub(super) fn arn(account_id: &str, table: &str) -> String {
    format!("arn:aws:dynamodb:us-east-1:{account_id}:table/{table}")
}

pub(super) fn item(size: usize) -> Value {
    json!({"pk": {"S": "k"}, "v": {"S": "x".repeat(size)}})
}

/// A 1 WCU table starts with one second of burst; writes past it are refused
/// with the provisioned class, `ThrottlingReasons` (capital T), the write
/// reason and the table ARN formed from the server region and the account.
#[tokio::test]
async fn provisioned_write_throttle_names_the_table_arn() {
    let (state, account_id) = build_state().await;
    let table = "throttled-writes";
    create_table(
        &state,
        table,
        json!({"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}}),
    )
    .await;
    // The manager is drained directly, far past the one second of burst,
    // rather than by many writes, so the test does not depend on timing.
    state.throttle.consume(&account_id, table, 0.0, 1.0e9);
    let (status, body) = call(
        &state,
        "PutItem",
        json!({"TableName": table, "Item": item(16)}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body,
        json!({
            "__type": "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
            "message": PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
            "ThrottlingReasons": [{
                "reason": "TableWriteProvisionedThroughputExceeded",
                "resource": arn(&account_id, table),
            }],
        })
    );
}

/// A read refusal names the read reason.
#[tokio::test]
async fn provisioned_read_throttle_names_the_read_reason() {
    let (state, account_id) = build_state().await;
    let table = "throttled-reads";
    create_table(
        &state,
        table,
        json!({"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}}),
    )
    .await;
    state.throttle.consume(&account_id, table, 1.0e9, 0.0);
    let (status, body) = call(
        &state,
        "GetItem",
        json!({"TableName": table, "Key": {"pk": {"S": "k"}}}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["__type"],
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException"
    );
    assert_eq!(
        body["ThrottlingReasons"],
        json!([{
            "reason": "TableReadProvisionedThroughputExceeded",
            "resource": arn(&account_id, table),
        }])
    );
}

/// A hot key on an on-demand table: the write carries `throttlingReasons`
/// (lower t) in the availability namespace; the read carries no reasons list.
#[tokio::test]
async fn on_demand_hot_key_write_names_the_key_range_and_read_carries_no_reasons() {
    let (state, account_id) = build_state().await;
    let table = "hot-key";
    create_table(&state, table, json!({"BillingMode": "PAY_PER_REQUEST"})).await;
    // Drain the partition bucket for key "k" in both directions and leave
    // the table bucket full: the drain runs while the table is not
    // registered, and the request registers it again from DescribeTable, as
    // after a restart.
    state.throttle.remove_table(&account_id, table);
    state
        .throttle
        .consume_with_partition(&account_id, table, 1.0e9, 1.0e9, Some("k"), None);
    let (status, body) = call(
        &state,
        "PutItem",
        json!({"TableName": table, "Item": item(16)}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body,
        json!({
            "__type": "com.amazon.coral.availability#ThrottlingException",
            "message": THROTTLING_EXCEPTION_MESSAGE,
            "throttlingReasons": [{
                "reason": "TableWriteKeyRangeThroughputExceeded",
                "resource": arn(&account_id, table),
            }],
        })
    );
    let (status, body) = call(
        &state,
        "GetItem",
        json!({"TableName": table, "Key": {"pk": {"S": "k"}}}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body,
        json!({
            "__type": "com.amazon.coral.availability#ThrottlingException",
            "message": THROTTLING_EXCEPTION_MESSAGE,
        })
    );
}

/// An on-demand table created with `OnDemandThroughput` caps is sized by the
/// caps and refuses with the caps message and limit.
#[tokio::test]
async fn on_demand_caps_throttle_names_the_max_on_demand_limit() {
    let (state, account_id) = build_state().await;
    let table = "capped";
    create_table(
        &state,
        table,
        json!({
            "BillingMode": "PAY_PER_REQUEST",
            "OnDemandThroughput": {"MaxReadRequestUnits": 1, "MaxWriteRequestUnits": 1},
        }),
    )
    .await;
    state.throttle.consume(&account_id, table, 1.0e9, 1.0e9);
    for (operation, body, reason) in [
        (
            "Query",
            json!({
                "TableName": table,
                "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": {"S": "k"}},
            }),
            "TableReadMaxOnDemandThroughputExceeded",
        ),
        (
            "PutItem",
            json!({"TableName": table, "Item": item(16)}),
            "TableWriteMaxOnDemandThroughputExceeded",
        ),
    ] {
        let (status, got) = call(&state, operation, body).await;
        assert_eq!(status, 400, "{operation}: {got}");
        assert_eq!(
            got,
            json!({
                "__type": "com.amazon.coral.availability#ThrottlingException",
                "message": MAX_ON_DEMAND_THROUGHPUT_MESSAGE,
                "throttlingReasons": [{
                    "reason": reason,
                    "resource": arn(&account_id, table),
                }],
            }),
            "{operation}"
        );
    }
}

/// A table capped in one direction reports the caps body only for a refusal
/// in that direction; the other direction is the default ceiling's body.
#[tokio::test]
async fn half_capped_table_reports_the_cap_for_its_direction_only() {
    let (state, account_id) = build_state().await;
    let table = "half-capped";
    create_table(
        &state,
        table,
        json!({
            "BillingMode": "PAY_PER_REQUEST",
            "OnDemandThroughput": {"MaxWriteRequestUnits": 1},
        }),
    )
    .await;
    // Drain the write cap of 1 and the default read ceiling of 12,000.
    state.throttle.consume(&account_id, table, 1.0e9, 1.0e9);
    let (status, body) = call(
        &state,
        "PutItem",
        json!({"TableName": table, "Item": item(16)}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["message"], MAX_ON_DEMAND_THROUGHPUT_MESSAGE);
    assert_eq!(
        body["throttlingReasons"][0]["reason"],
        "TableWriteMaxOnDemandThroughputExceeded"
    );
    let (status, body) = call(
        &state,
        "GetItem",
        json!({"TableName": table, "Key": {"pk": {"S": "k"}}}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["message"], THROTTLING_EXCEPTION_MESSAGE);
    assert_eq!(
        body["throttlingReasons"][0]["reason"],
        "TableReadAccountLimitExceeded"
    );
}

/// The account bucket can refuse a request whose table the engine has never
/// seen. A `TableName` that is the ARN of a table in this account and Region
/// names that table in the reasons list.
#[tokio::test]
async fn account_throttle_reason_names_the_table_of_an_arn() {
    let (state, account_id) = build_state().await;
    // Drain the account aggregate (its limits are the per-account maxima).
    state.throttle.consume(&account_id, "any", 1.0e9, 1.0e9);
    let (status, body) = call(
        &state,
        "GetItem",
        json!({"TableName": arn(&account_id, "absent"), "Key": {"pk": {"S": "k"}}}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["__type"],
        "com.amazonaws.dynamodb.v20120810#RequestLimitExceeded"
    );
    assert_eq!(
        body["ThrottlingReasons"],
        json!([{"reason": "TableReadAccountLimitExceeded", "resource": arn(&account_id, "absent")}])
    );
}

/// A `TableName` outside the table name rules is not throttled: with the
/// account bucket drained, the server answers it as it did with capacity
/// left, and with no reasons list. The service refuses such a name before
/// any throttle (measured 2026-11-25 with GetItem and PutItem, twice each).
#[tokio::test]
async fn names_outside_the_table_name_rules_are_not_throttled() {
    let (state, account_id) = build_state().await;
    let long = "a".repeat(256);
    let other_region = format!("arn:aws:dynamodb:eu-west-1:{account_id}:table/absent");
    let mut requests = Vec::new();
    for name in ["ab", "", long.as_str(), other_region.as_str()] {
        requests.push((
            "GetItem",
            json!({"TableName": name, "Key": {"pk": {"S": "k"}}}),
        ));
        requests.push(("PutItem", json!({"TableName": name, "Item": item(16)})));
    }
    let mut with_capacity = Vec::new();
    for (operation, request) in &requests {
        with_capacity.push(call(&state, operation, request.clone()).await);
    }
    state.throttle.consume(&account_id, "any", 1.0e9, 1.0e9);
    for ((operation, request), before) in requests.iter().zip(&with_capacity) {
        let drained = call(&state, operation, request.clone()).await;
        assert_eq!(&drained, before, "{operation} {request}");
        assert_eq!(drained.0, 400, "{operation} {request}");
    }
}

/// With capacity available the same requests succeed, so the refusals above
/// come from the throttle segment and not from the request shape.
#[tokio::test]
async fn requests_with_capacity_are_served() {
    let (state, _) = build_state().await;
    let table = "served";
    create_table(
        &state,
        table,
        json!({"ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}}),
    )
    .await;
    let (status, body) = call(
        &state,
        "PutItem",
        json!({"TableName": table, "Item": item(16)}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = call(
        &state,
        "GetItem",
        json!({"TableName": table, "Key": {"pk": {"S": "k"}}}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["Item"]["pk"], json!({"S": "k"}));
}

#[path = "../../../engine/tests/batch_partial/failing_store.rs"]
#[allow(dead_code)]
mod failing_store;

/// The metrics of a batch a backend throttled, read from the collector after
/// `handle_request`. On a 200, one read or write throttle event per throttled
/// entry, one data point per table, and no `ThrottledRequests`. When every
/// entry was throttled, the same events, `ThrottledRequests` and no
/// `UserErrors`. Each row: the operation, the request, the status, and the
/// (sum, data points) expected for each metric.
#[tokio::test]
async fn a_batch_a_backend_throttled_records_the_throttle_metrics() {
    use extenddb_core::metrics::MetricName;
    let puts = |pks: &[&str]| -> Value {
        pks.iter()
            .map(|pk| json!({"PutRequest": {"Item": {"pk": {"S": pk}}}}))
            .collect()
    };
    let keys = |pks: &[&str]| -> Value { pks.iter().map(|pk| json!({"pk": {"S": pk}})).collect() };
    let rows = [
        (
            "BatchGetItem",
            json!({"RequestItems": {"T": {"Keys": keys(&["k1", "k2", "k3"])}}}),
            200,
            [(2.0, 1), (0.0, 0), (0.0, 0), (0.0, 0)],
        ),
        (
            "BatchWriteItem",
            json!({"RequestItems": {"T": puts(&["p1", "k2"])}}),
            200,
            [(0.0, 0), (1.0, 1), (0.0, 0), (0.0, 0)],
        ),
        (
            "BatchGetItem",
            json!({"RequestItems": {"T": {"Keys": keys(&["k2", "k3"])}}}),
            400,
            [(2.0, 1), (0.0, 0), (1.0, 1), (0.0, 0)],
        ),
    ];
    for (operation, request, status, expected) in rows {
        let store = failing_store::FailingStore::throttling()
            .with_items(&["k1", "k2", "k3"])
            .failing_on(&["k2", "k3"]);
        let (state, account_id) = build_state_with(Some(Arc::new(store)), stub_auth, None).await;
        let (got, body) = call(&state, operation, request).await;
        assert_eq!(got, status, "{operation}: {body}");
        for (metric, want) in [
            MetricName::ReadThrottleEvents,
            MetricName::WriteThrottleEvents,
            MetricName::ThrottledRequests,
            MetricName::UserErrors,
        ]
        .into_iter()
        .zip(expected)
        {
            let got = crate::throttle_helpers::points(&state.metrics, &account_id, metric, None);
            assert_eq!(got, want, "{operation} {status}: {metric:?}");
        }
    }
}
