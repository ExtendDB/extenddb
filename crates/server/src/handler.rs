// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Main `DynamoDB` request handler with per-segment latency instrumentation.
//!
//! Extracted from `lib.rs` to keep both files under the 500-line limit.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use axum::response::Response;
use extenddb_core::error::DynamoDbError;
use extenddb_engine::OperationContext;
use serde_json::Value;

use crate::AppState;
use crate::request_helpers::{authorize_request, extract_operation, extract_table_name};
use crate::response::{error_response, record_error_metrics, success_response};
use crate::throttle_helpers::{
    classify_data_operation, extract_partition_value, record_partial_batch_throttle,
    table_description_to_throughput, throttle_error, update_throttle_buckets,
};

/// Main Virtual `DynamoDB` request handler.
/// REQ-WIRE-001: Accept HTTP POST to `/`.
/// REQ-WIRE-002: Parse X-Amz-Target header.
/// SP-WIRE-007: Reject bodies exceeding 16 MB with `RequestEntityTooLargeException`.
#[allow(clippy::too_many_lines)] // sequential request pipeline — splitting would obscure the flow
#[allow(clippy::similar_names)] // auth_start/authz_start and auth_us/authz_us are intentionally parallel
pub(crate) async fn handle_request(
    State(state): State<Arc<AppState>>,
    uri: axum::http::Uri,
    ConnectInfo(peer_addr): ConnectInfo<std::net::SocketAddr>,
    mut headers: HeaderMap,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Response {
    let handler_start = std::time::Instant::now();

    // HTTP/2 uses :authority instead of Host. Ensure the Host header is
    // populated so SigV4 verification can find it in the canonical request.
    if !headers.contains_key("host")
        && let Some(authority) = uri.authority()
        && let Ok(val) = authority.as_str().parse()
    {
        headers.insert("host", val);
    }
    let request_id = uuid::Uuid::new_v4().to_string();

    // SP-WIRE-007: body limit exceeded → 413
    let Ok(body) = body else {
        return error_response(
            &DynamoDbError::RequestEntityTooLargeException(
                "Request size has exceeded the maximum allowed size".to_owned(),
            ),
            &request_id,
        );
    };

    // Extract operation from X-Amz-Target
    let operation = match extract_operation(&headers) {
        Ok(op) => op,
        Err(e) => return error_response(&e, &request_id),
    };

    // --- Pre-auth operation validation ---
    // Real DynamoDB validates the operation name before checking authentication.
    // An unknown operation returns UnknownOperationException even without auth headers.
    if !extenddb_engine::is_known_operation(&operation) {
        return error_response(
            &DynamoDbError::UnknownOperationException(String::new()),
            &request_id,
        );
    }

    // --- Pre-auth body validation ---
    // Real DynamoDB validates request format (empty body, invalid JSON) before
    // authentication. Malformed requests get 400 regardless of auth state.
    let input: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return error_response(
                &DynamoDbError::SerializationException(format!(
                    "Start of structure or map found where not expected: {e}"
                )),
                &request_id,
            );
        }
    };

    // --- Authentication through dispatch, inside the request scope ---
    // The scope carries the request id and operation (and the two SDK
    // correlation headers when the client sent them) to every storage read
    // the request makes and to the engine's error mappers, so a fault log line
    // can name the x-amzn-RequestId the client received. The early returns
    // above touch no storage and the response building below runs outside it.
    let Served {
        account_id,
        table_name,
        partition_value,
        dispatch_result,
        auth_us,
        authz_us,
        throttle_us,
        dispatch_us,
    } = match extenddb_storage::request_scope::with_scope(
        request_scope_for(&request_id, &operation, &headers),
        serve_request(&state, &headers, &body, peer_addr, &operation, input),
    )
    .await
    {
        Ok(served) => served,
        Err(e) => return error_response(&e, &request_id),
    };

    // --- Response segment ---
    let response_start = std::time::Instant::now();
    let response = match dispatch_result {
        Ok(result) => {
            update_throttle_buckets(
                &state.throttle,
                &operation,
                &account_id,
                table_name.as_deref(),
                &result.body,
            );
            // A batch a backend served partially is a throttle the server's own
            // bucket never saw; count it so the metrics do not read as zero.
            record_partial_batch_throttle(&state.metrics, &operation, &result.body);

            let m = &result.metrics;
            if let Some(ref tn) = table_name {
                if m.read_capacity_units > 0.0 {
                    state.metrics.record(
                        extenddb_core::metrics::MetricName::ConsumedReadCapacityUnits,
                        m.read_capacity_units,
                        Some(tn),
                        m.index_name.as_deref(),
                        Some(&operation),
                    );
                }
                if m.write_capacity_units > 0.0 {
                    state.metrics.record(
                        extenddb_core::metrics::MetricName::ConsumedWriteCapacityUnits,
                        m.write_capacity_units,
                        Some(tn),
                        m.index_name.as_deref(),
                        Some(&operation),
                    );
                }
                if m.returned_item_count > 0 {
                    state
                        .metrics
                        .record_returned_items(tn, &operation, m.returned_item_count);
                }
                if m.returned_bytes > 0 {
                    state
                        .metrics
                        .record_returned_bytes(tn, &operation, m.returned_bytes);
                }
                state.throttle.consume_with_partition(
                    &account_id,
                    tn,
                    m.read_capacity_units,
                    m.write_capacity_units,
                    partition_value.as_deref(),
                );
            }

            success_response(&result.body, &request_id)
        }
        Err(e) => {
            record_error_metrics(&state.metrics, &e, table_name.as_deref(), &operation);
            error_response(&e, &request_id)
        }
    };
    #[allow(clippy::cast_precision_loss)]
    let response_us = response_start.elapsed().as_micros() as f64;

    // Record per-segment latency breakdown and total latency.
    #[allow(clippy::cast_precision_loss)]
    let total_us = handler_start.elapsed().as_micros() as f64;
    state
        .metrics
        .record_latency(table_name.as_deref(), &operation, total_us);
    state.metrics.record_request_count(&operation);
    state.metrics.record_segments(
        &operation,
        table_name.as_deref(),
        extenddb_core::metrics::LatencySegments {
            auth_us,
            authz_us,
            throttle_us,
            dispatch_us,
            response_us,
            total_us,
        },
    );

    // P79: Request-level trace logging.
    tracing::debug!(
        request_id = %request_id,
        operation = %operation,
        table = table_name.as_deref().unwrap_or("-"),
        auth_us = auth_us,
        authz_us = authz_us,
        throttle_us = throttle_us,
        dispatch_us = dispatch_us,
        response_us = response_us,
        total_us = total_us,
        "request complete"
    );

    response
}

/// What the request produced between authentication and the end of dispatch:
/// the dispatch result and what the response segment needs to account for it.
struct Served {
    account_id: Arc<str>,
    table_name: Option<String>,
    partition_value: Option<String>,
    dispatch_result: Result<extenddb_engine::DispatchResult, DynamoDbError>,
    auth_us: f64,
    authz_us: f64,
    throttle_us: f64,
    dispatch_us: f64,
}

/// Authenticate, authorize, throttle and dispatch one request. Every storage
/// read the request makes happens here, so `handle_request` runs this whole
/// future inside the request scope. A refusal before dispatch returns `Err`;
/// a dispatch error is inside `Served::dispatch_result`, because the response
/// segment accounts for it differently.
#[allow(clippy::too_many_lines)] // sequential request pipeline
async fn serve_request(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    body: &Bytes,
    peer_addr: std::net::SocketAddr,
    operation: &str,
    input: Value,
) -> Result<Served, DynamoDbError> {
    // --- Auth segment ---
    let auth_start = std::time::Instant::now();
    let identity = state.auth.authenticate(headers, body).await?;
    #[allow(clippy::cast_precision_loss)]
    let auth_us = auth_start.elapsed().as_micros() as f64;

    let account_id: Arc<str> = match &identity {
        extenddb_auth::AuthIdentity::User { account_id, .. }
        | extenddb_auth::AuthIdentity::RoleSession { account_id, .. } => {
            Arc::from(account_id.as_str())
        }
    };

    // --- Authz segment ---
    let authz_start = std::time::Instant::now();
    let authorized_key_info;
    {
        if state.catalog_store.is_none() {
            tracing::error!("Authorization required but catalog_store is not configured");
            return Err(DynamoDbError::AccessDeniedException(
                "User: is not authorized to perform this operation".to_owned(),
            ));
        }
        // aws:SourceIp is the connection peer address; aws:UserAgent is the
        // User-Agent header. Behind a proxy only the peer address is trusted,
        // never an X-Forwarded-For header.
        let source_ip = crate::authorization::condition_source_ip(&peer_addr);
        let user_agent = headers
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        authorized_key_info = authorize_request(
            state,
            &identity,
            &input,
            operation,
            &account_id,
            Some(source_ip.as_str()),
            user_agent.as_deref(),
        )
        .await?;
    }
    #[allow(clippy::cast_precision_loss)]
    let authz_us = authz_start.elapsed().as_micros() as f64;

    // Dispatch
    let ctx = OperationContext {
        storage: state.storage.clone(),
        limits: state.limits.clone(),
        region: state.region.clone(),
        account_id,
        import_paths: state.import_paths.clone(),
        export_paths: state.export_paths.clone(),
        authorized_key_info,
        auth_cache: state.auth_cache.clone(),
        table_key_info_lookup: Some(
            state.table_key_info_cache.clone() as Arc<dyn extenddb_storage::TableKeyInfoLookup>
        ),
    };

    let table_name = extract_table_name(&input);

    // --- Throttle segment ---
    let throttle_start = std::time::Instant::now();
    let (is_read_op, is_write_op) = classify_data_operation(operation);
    let partition_value = extract_partition_value(&input, operation);
    if let Some(ref tn) = table_name
        && (is_read_op || is_write_op)
    {
        if !state.throttle.is_registered(&ctx.account_id, tn)
            && let Ok(desc) = ctx
                .storage
                .describe_table(
                    &ctx.account_id,
                    extenddb_core::types::DescribeTableInput {
                        table_name: tn.clone(),
                    },
                )
                .await
        {
            let throughput = table_description_to_throughput(&desc);
            state
                .throttle
                .register_table(&ctx.account_id, tn, throughput);
        }

        let result = state.throttle.check_capacity_with_partition(
            &ctx.account_id,
            tn,
            is_read_op,
            is_write_op,
            partition_value.as_deref(),
        );
        if let Some(cause) = result.cause() {
            let throttled_read = matches!(
                result,
                extenddb_core::throttle::ThrottleResult::ThrottledRead(_)
            );
            let metric = if throttled_read {
                extenddb_core::metrics::MetricName::ReadThrottleEvents
            } else {
                extenddb_core::metrics::MetricName::WriteThrottleEvents
            };
            state
                .metrics
                .record(metric, 1.0, Some(tn), None, Some(operation));
            state.metrics.record(
                extenddb_core::metrics::MetricName::ThrottledRequests,
                1.0,
                Some(tn),
                None,
                Some(operation),
            );
            // The service names the throttled resource and the limit on a
            // throttle: one reason with the table ARN, the same string
            // DescribeTable returns as TableArn. The error class, the message
            // and the limit follow the bucket that refused and the table's
            // billing mode. The account bucket can refuse before the engine
            // has seen the name, so an ARN naming a table in this account is
            // reduced to the bare name first, and a name the engine would
            // refuse gets no resource at all.
            let name = crate::authz_resource::key_lookup_name(tn, &state.region, &ctx.account_id);
            let table_arn = extenddb_core::validation::validate_table_name_chars(&name)
                .is_ok()
                .then(|| extenddb_storage::util::table_arn(&state.region, &ctx.account_id, &name));
            return Err(throttle_error(cause, throttled_read, table_arn));
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let throttle_us = throttle_start.elapsed().as_micros() as f64;

    // --- Dispatch segment ---
    let dispatch_start = std::time::Instant::now();
    let dispatch_result =
        extenddb_engine::dispatch(operation, input, &ctx, &state.server_addr).await;
    #[allow(clippy::cast_precision_loss)]
    let dispatch_us = dispatch_start.elapsed().as_micros() as f64;
    // P120c: Record storage query metrics for the request's dispatch phase.
    let category = match operation {
        "CreateTable"
        | "DeleteTable"
        | "DescribeTable"
        | "ListTables"
        | "UpdateTable"
        | "TagResource"
        | "UntagResource"
        | "ListTagsOfResource"
        | "DescribeTimeToLive"
        | "UpdateTimeToLive"
        | "UpdateContinuousBackups"
        | "DescribeContinuousBackups"
        | "CreateGlobalSecondaryIndex"
        | "DeleteGlobalSecondaryIndex" => extenddb_core::metrics::QueryCategory::Catalog,
        "AssumeRole" => extenddb_core::metrics::QueryCategory::Auth,
        _ => extenddb_core::metrics::QueryCategory::Data,
    };
    state.metrics.record_storage_query(
        extenddb_core::metrics::QuerySource::Request,
        category,
        dispatch_us,
    );

    Ok(Served {
        account_id: ctx.account_id,
        table_name,
        partition_value,
        dispatch_result,
        auth_us,
        authz_us,
        throttle_us,
        dispatch_us,
    })
}

/// The scope the request runs inside from authentication through dispatch:
/// the request id the response will carry in `x-amzn-RequestId`, the
/// operation from `X-Amz-Target`, and the two SDK correlation headers when the
/// client sent them. Cost: two `String` clones per request plus the header
/// copies.
fn request_scope_for(
    request_id: &str,
    operation: &str,
    headers: &HeaderMap,
) -> extenddb_storage::RequestScope {
    extenddb_storage::RequestScope::new(request_id.to_owned(), operation.to_owned())
        .with_sdk_headers(
            header_string(headers, "amz-sdk-invocation-id"),
            header_string(headers, "amz-sdk-request"),
        )
}

/// The longest SDK correlation header value the scope carries. The SDKs send
/// a UUID and a short attempt counter; a longer value is read as absent, so a
/// client cannot place arbitrary text of any length in a backend's log line.
const MAX_SDK_HEADER_LEN: usize = 256;

/// The value of header `name` as an owned string, or `None` when the header is
/// absent, is not visible ASCII, or is longer than [`MAX_SDK_HEADER_LEN`].
fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .filter(|s| s.len() <= MAX_SDK_HEADER_LEN)
        .map(str::to_owned)
}

#[cfg(test)]
mod request_scope_tests;
#[cfg(test)]
mod throttle_wire_tests;
