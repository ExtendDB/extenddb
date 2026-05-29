# extenddb — Component Design: HTTP Server

**Version:** 1.0
**Date:** 2026-04-03
**Status:** Draft
**Crate:** `dynamodb-server`

## 1. Purpose

The `server` crate provides the HTTP server, request routing, middleware pipeline, and response formatting. It wires together the `core`, `auth`, and `storage` crates into a running service. Built on axum + tower for composable async middleware.

## 2. Module Structure

```
crates/server/src/
├── lib.rs                # Server builder, startup, shutdown
├── router.rs             # X-Amz-Target routing → operation dispatch
├── request.rs            # Request parsing, header extraction
├── response.rs           # Response formatting, CRC32, compression, error serialization
├── middleware/
│   ├── mod.rs
│   ├── request_id.rs     # Assign x-amzn-RequestId
│   ├── logging.rs        # Request/response logging
│   ├── auth.rs           # Authentication + authorization layer
│   ├── capacity.rs       # Capacity tracking + throughput enforcement (ThroughputTracker, TokenBucket)
│   ├── rate_limit.rs     # Global and per-table rate limiting
│   ├── metrics.rs        # Per-operation metrics collection
│   ├── request_size.rs   # Request body size limit enforcement
│   └── compression.rs    # Response gzip compression + CRC32
├── health.rs             # /health and /metrics endpoints
└── tls.rs                # TLS configuration (rustls)
```

## 3. Server Startup

### 3.1 Why `AppState` is generic over the storage backend

`AppState<S>` carries a generic `S: StorageEngine` while `AuthProvider` is erased behind `Arc<dyn>`. This asymmetry is intentional:

- **Storage uses static dispatch (generic `S`).** The storage engine is on the hot path — every request makes 1-3 storage calls. RPITIT on the storage traits avoids a `Box<dyn Future>` heap allocation per call. In absolute terms this saves ~50ns per call against a PostgreSQL round-trip of 0.5-5ms, so it's not measurable today. But this is a database compatibility layer where we control the full stack — there's no reason to leave performance on the table, and the ergonomic cost is small (the generic appears on ~6 function signatures, all in `server` and `engine`; it never leaks into `core`, `auth`, or middleware).
- **Auth uses dynamic dispatch (`Arc<dyn>`).** Auth is called once per request, involves HMAC-SHA256 crypto that dwarfs any vtable cost, and benefits from runtime pluggability (swap providers via config without recompilation).
- **The generic does NOT propagate into middleware.** `RequestIdLayer`, `LoggingLayer`, `MetricsLayer`, `RequestSizeLayer`, `CompressionLayer` — none of these touch storage. Only the main request handler and `OperationContext` carry the generic.
- **Testing:** Test code needs a concrete `S` type. Use a `MockStorage` struct that implements all storage sub-traits. This is more verbose than `Arc<dyn>` mocks but straightforward with hand-written mocks or `mockall`.
- **If this decision is revisited:** The simplest migration path is to add `#[async_trait]` to the storage traits and replace `Arc<S>` with `Arc<dyn StorageEngine>`. This removes all generics from the server/engine crates at the cost of one heap allocation per storage call. The change is mechanical and confined to trait definitions + `AppState`.

```rust
use axum::Router;
use std::sync::Arc;
use tower::ServiceBuilder;

pub struct ServerConfig {
    pub bind_addr: String,
    pub port: u16,
    pub tls: Option<TlsConfig>,
    pub max_request_size: usize,
    pub shutdown_drain_secs: u64,
    pub rate_limit_rps: Option<f64>,
}

pub struct AppState<S: StorageEngine> {
    pub storage: Arc<S>,
    pub auth: Arc<dyn AuthProvider>,
    pub credential_cache: Arc<CachedCredentialStore>,
    pub limits: Arc<LimitsConfig>,
    pub capacity_tracker: Arc<ThroughputTracker>,
    pub metrics: Arc<MetricsCollector>,
}

/// Build the axum Router with explicit state type.
///
/// The caller provides a pre-bound `TcpListener`. This supports the
/// bind-before-fork pattern: the socket is bound in the sync context
/// before daemonizing, so port conflicts are reported to stderr before
/// the parent process exits.
pub async fn start_server<S: StorageEngine + 'static>(
    listener: tokio::net::TcpListener,
    state: AppState<S>,
) -> Result<(), anyhow::Error> {
    let shared_state = Arc::new(state);

    // The turbofish on `post` is required to monomorphize the handler
    // with the correct storage type. Without it, Rust cannot infer `S`
    // from `Router::new()` alone.
    let app = Router::new()
        .route("/", post(handle_dynamodb_request::<S>))
        .route("/health", get(health::health_check))
        .route("/metrics", get(health::metrics_endpoint))
        .layer(
            ServiceBuilder::new()
                .layer(middleware::request_id::RequestIdLayer)
                .layer(middleware::logging::LoggingLayer)
                .layer(middleware::request_size::RequestSizeLayer::new(config.max_request_size))
                .layer(middleware::metrics::MetricsLayer::new(shared_state.metrics.clone()))
        )
        .with_state(shared_state);

    let local_addr = listener.local_addr()?;
    tracing::info!("extenddb listening on {}", local_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm.recv() => {},
    }
    tracing::info!("Shutdown signal received, draining connections...");
    // The drain timeout is enforced by axum::serve's graceful shutdown.
    // After the timeout, in-flight requests are cancelled and connections are dropped.
}
```

## 4. Request Routing

DynamoDB uses a single endpoint (`POST /`) with the operation name in the `X-Amz-Target` header.

```rust
/// The `S: StorageEngine + 'static` bound is required by axum's `State` extractor
/// (state must be `Clone + Send + Sync + 'static`). `Arc<AppState<S>>` satisfies
/// all of these when `S: StorageEngine + 'static`.
async fn handle_dynamodb_request<S: StorageEngine + 'static>(
    State(state): State<Arc<AppState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // 1. Extract operation from X-Amz-Target
    let operation = match extract_operation(&headers) {
        Ok(op) => op,
        Err(e) => return error_response(e),
    };

    // 2. Parse request body
    let input: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return error_response(DynamoDbError::serialization_error(e.to_string())),
    };

    // 3. Authenticate and authorize
    let header_map = extract_headers(&headers);
    let identity = match state.auth.authenticate(&AuthRequest {
        headers: &header_map,
        body: &body,
        operation: &operation,
        resource_arn: &extract_resource_arn(&input, &operation),
    }).await {
        Ok(id) => id,
        Err(e) => return error_response(e),
    };

    // 4. Authorize
    let resource_tags = fetch_resource_tags(&state.storage, &input, &operation).await;
    let request_params = extract_request_params(&input, &operation);
    let context = RequestContext::build(&identity, &operation, &resource_tags, &request_params);
    if let Err(e) = check_authorization(&state.auth, &identity, &operation, &input, &context).await {
        return error_response(e);
    }

    // 5. Check capacity / rate limits
    if let Err(e) = check_capacity(&state, &operation, &input).await {
        return error_response(e);
    }

    // 6. Dispatch to operation handler
    let request_id = generate_request_id();
    let ctx = OperationContext {
        request_id: request_id.clone(),
        storage: state.storage.clone(),
        identity: Some(identity),
        limits: state.limits.clone(),
    };

    let result = dispatch(&operation, input, &ctx).await;

    // 7. Format response
    match result {
        Ok(response_body) => success_response(response_body, &request_id),
        Err(e) => error_response_with_id(e, &request_id),
    }
}

fn extract_operation(headers: &HeaderMap) -> Result<String, DynamoDbError> {
    let target = headers.get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| DynamoDbError::missing_auth_token("Missing X-Amz-Target header"))?;

    // DynamoDB and DynamoDB Streams are separate services in every SDK but
    // extenddb serves both on the same port. The X-Amz-Target prefix distinguishes them:
    //   DynamoDB:         "DynamoDB_20120810.PutItem"
    //   DynamoDB Streams: "DynamoDBStreams_20120810.GetRecords"
    // Both use signing_name="dynamodb" so SigV4 auth is identical.
    target.strip_prefix("DynamoDB_20120810.")
        .or_else(|| target.strip_prefix("DynamoDBStreams_20120810."))
        .map(|s| s.to_string())
        .ok_or_else(|| DynamoDbError::unknown_operation(
            format!("Invalid X-Amz-Target: {target}")
        ))
}
```

## 5. Response Formatting

### 5.1 Success Response

```rust
fn success_response(body: Value, request_id: &str) -> Response {
    let body_bytes = match serde_json::to_vec(&body) {
        Ok(b) => b,
        Err(e) => return internal_error_response(request_id, &e.to_string()),
    };
    let crc32 = crc32fast::hash(&body_bytes);

    Response::builder()
        .status(200)
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amzn-requestid", request_id)
        .header("x-amz-crc32", crc32.to_string())
        .body(Body::from(body_bytes))
        .expect("valid response: all header values are controlled ASCII strings")
}
```

### 5.2 Error Response

```rust
fn error_response(error: DynamoDbError) -> Response {
    let body = json!({
        "__type": format!("com.amazonaws.dynamodb.v20120810#{}", error.error_type()),
        "message": error.message()
    });
    let body_bytes = match serde_json::to_vec(&body) {
        Ok(b) => b,
        Err(_) => br#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"Internal error"}"#.to_vec(),
    };
    let crc32 = crc32fast::hash(&body_bytes);

    Response::builder()
        .status(error.status_code())
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amzn-requestid", generate_request_id())
        .header("x-amz-crc32", crc32.to_string())
        .body(Body::from(body_bytes))
        .expect("valid response: all header values are controlled ASCII strings")
}
```

### 5.3 Gzip Compression

Applied as a tower layer when the client sends `Accept-Encoding: gzip`:

```rust
pub struct CompressionLayer;

impl<S> Layer<S> for CompressionLayer {
    type Service = CompressionService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        CompressionService { inner }
    }
}
```

The CRC32 header is computed on the uncompressed body (matching DynamoDB behavior — SDKs validate CRC32 after decompression).

## 6. Middleware Pipeline

Middleware is implemented as tower `Layer`s, composed in order:

```
Request
  → RequestIdLayer        (assign UUID, add to response headers)
  → RequestSizeLayer      (reject if body > max_request_size)
  → LoggingLayer          (log request start, response status + latency)
  → MetricsLayer          (record per-operation counters and latency histograms)
  → [Auth + Capacity are handled inline in the handler, not as layers,
     because they need access to the parsed operation name and request body]
Response
  → CompressionLayer      (gzip if Accept-Encoding: gzip)
  → CRC32 is added in response formatting (not a separate layer)
```

**Why auth/capacity are inline, not layers:**
Auth needs the raw body bytes for SigV4 signature computation, and the operation name from X-Amz-Target. Capacity needs the table name from the parsed JSON body. These require parsing the request, which is most naturally done in the handler. Tower layers that need to inspect/buffer the body add complexity without benefit here.

## 7. Health & Metrics Endpoints

### 7.1 Health Check

```
GET /health → 200 OK {"status": "healthy"}
```

Returns 200 when the server is accepting requests. Can optionally check storage backend connectivity.

### 7.2 Metrics

```
GET /metrics → 200 OK (application/json)
```

This is **not** Prometheus exposition format. The response is a custom JSON
schema that uses DynamoDB CloudWatch-style metric names and dimensions.

**Query parameters** (`MetricsQuery` in `crates/core/src/metrics/types.rs`):

- `window`: `LastMinute` | `Last5Minutes` | `LastHour` | `LastDay` | `AllTime`
- `start`, `end`: ISO 8601 (custom range, alternative to `window`)
- `granularity`: `1m` | `5m` | `15m` | `1h` (auto-selected if omitted)
- `table_name`: filter by table
- `metric`: filter by metric name

**Response shape** (`MetricsResponse`):

```json
{
  "metrics":  [ { "metric": "SuccessfulRequestLatency",
                  "dimensions": [ { "TableName": "Users" },
                                  { "Operation":  "GetItem" } ],
                  "window": "Last5Minutes",
                  "sum": 26033.0, "count": 50, "min": 321.0, "max": 714.0 } ],
  "buckets": [ { "timestamp": "2026-05-29T00:02:00Z",
                  "metric": "SuccessfulRequestLatency",
                  "dimensions": [ { "TableName": "Users" } ],
                  "sum": 1234.0, "count": 5, "min": 200.0, "max": 410.0 } ],
  "segments": [ { "operation": "GetItem", "count": 50,
                  "avg": { "auth_us": 12.0, "authz_us": 4.0,
                            "throttle_us": 1.0, "dispatch_us": 280.0,
                            "response_us": 8.0, "total_us": 305.0 } } ],
  "source":  "database"
}
```

- `metrics` (`Vec<MetricSnapshot>`): aggregate over the requested window.
- `buckets` (`Vec<MetricsBucket>`): time-series at the requested granularity
  (omitted in the in-memory fallback path).
- `segments` (`Vec<OperationSegments>`): per-operation latency breakdown
  (auth / authz / throttle / dispatch / response in microseconds), in-memory only.
- `source`: `"database"` when served from the persistent `MetricsStore`,
  `"memory"` when served from the in-process `MetricsCollector` fallback.

**Metric names** (from the `MetricName` enum in `crates/core/src/metrics/types.rs`):

DynamoDB CloudWatch-aligned:
- `ConsumedReadCapacityUnits`, `ConsumedWriteCapacityUnits`
- `SuccessfulRequestLatency` (microseconds)
- `SystemErrors`, `UserErrors`
- `ThrottledRequests`, `ReadThrottleEvents`, `WriteThrottleEvents`
- `ConditionalCheckFailedRequests`, `TransactionConflict`
- `ReturnedItemCount`, `ReturnedBytes`
- `TimeToLiveDeletedItemCount`, `TtlDeletionStaleness`

ExtendDB-internal:
- `RequestCount` (HTTP request count, dimension: `Operation`)
- `StorageQueryCount`, `StorageQueryLatency` (dimensions: source, category)
- `PoolActiveConnections`, `PoolIdleConnections`, `PoolAcquireLatency`
- `WorkerLastSuccess`, `WorkerCycleLatency`, `WorkerErrorCount`

**Dimensions** (from the `Dimension` enum):

- `TableName(String)`
- `GlobalSecondaryIndexName(String)`
- `Operation(String)`

Clients that need Prometheus, OpenMetrics, or CloudWatch wire formats must
convert from this JSON externally.

## 8. Management API

The management API provides identity and credential management for extenddb's built-in auth provider. These endpoints are the only way to create users, roles, groups, credentials, and policies — the DynamoDB API surface is not used for identity management.

### 8.1 Authentication Model

The management API is unauthenticated by default. When `auth.provider = "builtin"` is configured, the management API requires admin credentials. For production deployments, always use `auth.provider = "builtin"` to protect management operations.

### 8.2 Routes

All management routes use `POST /management/<action>` with JSON request/response bodies. Content-Type is `application/json`.

| Route | Description | Storage method(s) | Cache invalidation |
|-------|-------------|-------------------|-------------------|
| `POST /management/create-user` | Create an IAM user | `IdentityEngine::create_user` | — |
| `POST /management/delete-user` | Delete an IAM user | `IdentityEngine::delete_user` | `invalidate_user`, `invalidate_policies` (user ARN + all group ARNs) |
| `POST /management/create-role` | Create an IAM role with trust policy | `IdentityEngine::create_role` | — |
| `POST /management/delete-role` | Delete an IAM role | `IdentityEngine::delete_role` | `invalidate_role`, `invalidate_policies` |
| `POST /management/create-group` | Create an IAM group | `IdentityEngine::create_group` | — |
| `POST /management/delete-group` | Delete an IAM group | `IdentityEngine::delete_group` | `invalidate_policies` (group ARN) |
| `POST /management/add-user-to-group` | Add a user to a group | `IdentityEngine::add_user_to_group` | `invalidate_user`, `invalidate_policies` (user ARN) |
| `POST /management/remove-user-from-group` | Remove a user from a group | `IdentityEngine::remove_user_from_group` | `invalidate_user`, `invalidate_policies` (user ARN) |
| `POST /management/attach-policy` | Attach a policy to a user, group, or role | `IdentityEngine::store_policy` | `invalidate_policies` (principal ARN) |
| `POST /management/detach-policy` | Detach a policy from a principal | `IdentityEngine::detach_policy` | `invalidate_policies` (principal ARN) |
| `POST /management/set-permissions-boundary` | Set or clear a permissions boundary | `IdentityEngine::set_permissions_boundary` | `invalidate_boundary` |
| `POST /management/set-principal-tags` | Set tags on a user or role | `IdentityEngine::set_user_tags` or `set_role_tags` | `invalidate_user` or `invalidate_role` |
| `POST /management/store-credential` | Create an access key for a user | `CredentialEngine::store_credential` | — |
| `POST /management/deactivate-credential` | Deactivate an access key | `CredentialEngine::deactivate_credential` | `invalidate_credential` |
| `POST /management/assume-role` | Assume a role, get temporary credentials | Load role via `IdentityEngine::get_role` → evaluate trust policy via `evaluate_trust_policy` with `AssumeRoleContext` → `SessionEngine::create_session` + `CredentialEngine::store_credential` | — (creates new entries; no stale cache possible) |
| `POST /management/revoke-session` | Revoke a role session | `SessionEngine::revoke_session` | `invalidate_session`, `invalidate_credential` |

### 8.3 Request/Response Formats

**Create User:**
```json
// Request
{ "UserName": "developer", "AccountId": "<account-id>" }
// Response
{ "UserArn": "arn:aws:iam::<account-id>:user/developer" }
```

**Create Role:**
```json
// Request
{
  "RoleName": "data-reader",
  "AccountId": "<account-id>",
  "TrustPolicy": {
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::<account-id>:user/developer"},
      "Action": "sts:AssumeRole"
    }]
  }
}
// Response
{ "RoleArn": "arn:aws:iam::<account-id>:role/data-reader" }
```

**Attach Policy:**
```json
// Request
{
  "PrincipalArn": "arn:aws:iam::<account-id>:user/developer",
  "PolicyName": "allow-users-table",
  "PolicyDocument": {
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["dynamodb:GetItem", "dynamodb:PutItem"],
      "Resource": "arn:aws:dynamodb:*:*:table/Users"
    }]
  }
}
// Response
{ "Status": "attached" }
```

**Store Credential:**
```json
// Request
{ "UserName": "developer" }
// Response
{
  "AccessKeyId": "AKIAIOSFODNN7EXAMPLE",
  "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
}
```

> **Note:** The `SecretAccessKey` is returned only in this response and is not retrievable afterward. The storage layer stores the raw secret (not a hash) because SigV4 authentication requires the original secret key for HMAC computation. Secrets are encrypted at rest with AES-256-GCM using a per-catalog encryption key generated during `extenddb init`.

**Assume Role:**
```json
// Request
{
  "CallerArn": "arn:aws:iam::<account-id>:user/developer",
  "RoleName": "data-reader",
  "SessionName": "test-session",
  "SessionTags": [{"Key": "Project", "Value": "Alpha"}],
  "SessionPolicy": { ... },
  "DurationSeconds": 3600,
  "ExternalId": "optional-external-id"
}
// Response
{
  "AccessKeyId": "ASIAIOSFODNN7EXAMPLE",
  "SecretAccessKey": "...",
  "SessionToken": "...",
  "Expiration": "2026-04-06T20:00:00Z"
}
```

`CallerArn` is required — it identifies the principal assuming the role. It must match the format `arn:aws:iam::{account_id}:user/{user_name}`. If the format is invalid, the handler returns 400 with `{"Error": "ValidationError", "Message": "CallerArn must be a valid IAM user ARN"}`. Role-chaining (a role assuming another role) is deferred to a future version; in v1, `CallerArn` must reference a user. The handler loads the caller's tags via `IdentityEngine`, builds an `AssumeRoleContext` from the caller's tags + `SessionTags` + `ExternalId`, loads the role's trust policy, and evaluates it using `evaluate_trust_policy`. If the trust policy denies the assumption, the handler returns 403 with `{"Error": "AccessDenied", "Message": "Trust policy does not allow this principal to assume the role"}`. This ensures developers can test trust policy conditions (e.g., `sts:ExternalId`, `aws:PrincipalTag/*` restrictions) through the management API.

### 8.4 Error Responses

Management API errors use a simple JSON format:
```json
{ "Error": "UserAlreadyExists", "Message": "User 'developer' already exists" }
```

HTTP status codes: 200 for success, 400 for client errors (already exists, not found, validation), 401 for missing/invalid admin secret, 500 for internal errors.

### 8.5 Router Integration

Management routes are registered alongside the DynamoDB and health routes:

```rust
let app = Router::new()
    .route("/", post(handle_dynamodb_request::<S>))
    .route("/health", get(health::health_check))
    .route("/metrics", get(health::metrics_endpoint))
    .route("/management/:action", post(handle_management_request::<S>))
    .with_state(shared_state);
```

The management handler has access to `AppState` which contains both the storage engine (for write operations) and the `CachedCredentialStore` (for cache invalidation after writes).

## 9. TLS Configuration

When TLS is enabled, the server uses `axum_server` (a separate crate from `axum` that adds rustls support). The non-TLS path uses `axum::serve` directly.

```rust
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

/// TLS variant of start_server. Uses `axum_server::bind_rustls` instead of
/// `axum::serve` because axum's built-in serve doesn't support TLS directly.
pub async fn start_tls_server<S: StorageEngine + 'static>(
    config: ServerConfig,
    state: AppState<S>,
) -> Result<(), anyhow::Error> {
    let tls = config.tls.as_ref().expect("TLS config required");
    let rustls_config = RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path).await?;

    let shared_state = Arc::new(state);
    let app = Router::new()
        .route("/", post(handle_dynamodb_request::<S>))
        .route("/health", get(health::health_check))
        .route("/metrics", get(health::metrics_endpoint))
        .with_state(shared_state);

    let addr = format!("{}:{}", config.bind_addr, config.port);
    axum_server::bind_rustls(addr.parse()?, rustls_config)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}
```

## 10. Rate Limiting

Two levels of rate limiting, both using token bucket:

- **Global rate limit**: Configurable requests per second across all operations
- **Per-table rate limit**: Configurable requests per second per table name

```rust
pub struct RateLimiter {
    global: Option<TokenBucket>,
    /// Per-table rate limit buckets. Uses moka cache with max-size eviction
    /// to prevent unbounded growth as new table names are seen. Evicted entries
    /// simply reset their token bucket on next access (conservative — may
    /// briefly allow a burst after eviction, but prevents memory leaks).
    per_table: moka::sync::Cache<String, Arc<TokenBucket>>,
    per_table_rps: f64,
}

impl RateLimiter {
    pub fn check(&self, table_name: Option<&str>) -> Result<(), DynamoDbError> {
        if let Some(ref global) = self.global {
            if !global.try_acquire() {
                return Err(DynamoDbError::throttling_error("Rate exceeded"));
            }
        }
        if let Some(name) = table_name {
            if self.per_table_rps > 0.0 {
                let bucket = self.per_table.get_with(name.to_string(), || {
                    Arc::new(TokenBucket::new(self.per_table_rps))
                });
                if !bucket.try_acquire() {
                    return Err(DynamoDbError::throttling_error("Rate exceeded"));
                }
            }
        }
        Ok(())
    }
}
```

## 11. Throughput Tracking

Throughput tracking enforces DynamoDB's per-partition and per-table capacity limits (REQ-CAP-004, REQ-CAP-005). This is a runtime stateful component — it lives in the server crate, not in `core` (which only contains the pure-math RCU/WCU calculator).

```rust
use std::sync::Mutex;
use std::time::Instant;

pub struct ThroughputTracker {
    /// Per-partition buckets (partition key hash → bucket)
    partition_buckets: DashMap<u64, TokenBucket>,
    /// Per-table buckets (table name → bucket)
    table_buckets: DashMap<String, TokenBucket>,
}

pub struct TokenBucket {
    /// Mutex is appropriate here — token bucket operations are fast (no I/O),
    /// so lock contention is negligible even at high throughput.
    state: Mutex<TokenBucketState>,
    max_tokens: f64,
    refill_rate: f64,  // tokens per second
}

struct TokenBucketState {
    tokens: f64,
    last_refill: Instant,
}
```

The handler calls `core::capacity::calculator` to compute consumed RCU/WCU, then calls `ThroughputTracker::consume()` to deduct tokens. If insufficient tokens, returns `ProvisionedThroughputExceededException`.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
