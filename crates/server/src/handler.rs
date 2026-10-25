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
    extract_index_name, extract_partition_value, update_throttle_buckets,
};

mod throttle_segment;

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

    // A thin binary's observer receives one record per request that reached
    // authentication. The body hash is taken from the raw bytes before they
    // are parsed, and only when an observer is installed; with none, nothing
    // below allocates for a record.
    let observing = state.request_observer.is_some();
    let record_body_hash = observing.then(|| crate::extensions::body_hash(body.as_ref()));

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
    // One record per observed request, built here once with everything known
    // before authentication. The guard holds it: if the client goes away
    // while the request is in flight, hyper drops this future and the guard's
    // Drop reports the record with status 0. On the normal path the guard
    // hands the record back, the exit completes it (account, status, error
    // type, latency) and reports it once.
    let mut abandon_guard = state
        .request_observer
        .as_ref()
        .map(|observer| AbandonGuard {
            observer: Arc::clone(observer),
            record: Some(crate::extensions::RequestRecord {
                request_id: request_id.clone(),
                operation: operation.clone(),
                table_name: extract_table_name(&input),
                account_id: None,
                status: 0,
                error_type: None,
                latency_us: 0,
                body_hash: record_body_hash.unwrap_or_default(),
                sdk_invocation_id: header_string(&headers, "amz-sdk-invocation-id"),
                sdk_request: header_string(&headers, "amz-sdk-request"),
            }),
            started: handler_start,
        });
    let served = extenddb_storage::request_scope::with_scope(
        request_scope_for(&request_id, &operation, &headers),
        serve_request(&state, &headers, &body, peer_addr, &operation, input),
    )
    .await;
    let mut record = abandon_guard.as_mut().and_then(AbandonGuard::disarm);
    let Served {
        account_id,
        table_name,
        partition_value,
        index_name,
        dispatch_result,
        auth_us,
        authz_us,
        throttle_us,
        dispatch_us,
    } = match served {
        Ok(served) => served,
        Err(refused) => {
            // A refusal before dispatch (authentication, authorization or
            // the throttle) is observed too: the operation and table are
            // known and the error names the cause, and a throttle report that
            // could not see throttled requests would be wrong about retries.
            let response = error_response(&refused.error, &request_id);
            if let (Some(observer), Some(mut record)) =
                (state.request_observer.as_ref(), record.take())
            {
                record.account_id = refused.account_id;
                record.status = response.status().as_u16();
                record.error_type = Some(refused.error.error_type().to_owned());
                record.latency_us = elapsed_us(handler_start);
                observer.on_response(record);
            }
            return response;
        }
    };

    // --- Response segment ---
    let response_start = std::time::Instant::now();
    let mut record_error_type = None;
    let response = match dispatch_result {
        Ok(result) => {
            update_throttle_buckets(
                &state.throttle,
                &operation,
                &account_id,
                table_name.as_deref(),
                &result.body,
            );

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
                    index_name.as_deref(),
                );
            }

            success_response(&result.body, &request_id)
        }
        Err(e) => {
            record_error_metrics(&state.metrics, &e, table_name.as_deref(), &operation);
            if observing {
                record_error_type = Some(e.error_type().to_owned());
            }
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

    // The observed exit for every request that reached dispatch.
    if let (Some(observer), Some(mut record)) = (state.request_observer.as_ref(), record.take()) {
        record.account_id = Some(account_id.to_string());
        record.status = response.status().as_u16();
        record.error_type = record_error_type;
        record.latency_us = elapsed_us(handler_start);
        observer.on_response(record);
    }

    response
}

/// Microseconds since `start`, saturating.
fn elapsed_us(start: std::time::Instant) -> u64 {
    u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// Holds the one record of an observed request while `serve_request` runs.
/// If the handler future is dropped before its normal exit (the client went
/// away while the request was in flight), `Drop` reports the record with
/// status 0, so a record of attempts includes the request the client gave up
/// on. On the normal path `disarm` hands the record back for the exit to
/// complete and report. A handler that panics drops the guard during
/// unwinding; the observer is not called then, since a panic in the observer
/// at that point would abort the process.
struct AbandonGuard {
    observer: Arc<dyn crate::extensions::RequestObserver>,
    record: Option<crate::extensions::RequestRecord>,
    started: std::time::Instant,
}

impl AbandonGuard {
    fn disarm(&mut self) -> Option<crate::extensions::RequestRecord> {
        self.record.take()
    }
}

impl Drop for AbandonGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        if let Some(mut record) = self.record.take() {
            record.latency_us = elapsed_us(self.started);
            self.observer.on_response(record);
        }
    }
}

/// A refusal before dispatch, with the account authentication had produced
/// when it had. The table name is on the record already, from the parsed
/// body; there is no `From` for the bare error, so every refusal site states
/// the account.
struct Refused {
    error: DynamoDbError,
    account_id: Option<String>,
}

/// What the request produced between authentication and the end of dispatch:
/// the dispatch result and what the response segment needs to account for it.
struct Served {
    account_id: Arc<str>,
    table_name: Option<String>,
    partition_value: Option<String>,
    /// The index a `Query` or `Scan` named, charged in place of the table on
    /// a read when it is a provisioned global secondary index.
    index_name: Option<String>,
    dispatch_result: Result<extenddb_engine::DispatchResult, DynamoDbError>,
    auth_us: f64,
    authz_us: f64,
    throttle_us: f64,
    dispatch_us: f64,
}

/// Authenticate, authorize, throttle and dispatch one request. Every storage
/// read the request makes happens here, so `handle_request` runs this whole
/// future inside the request scope. A refusal before dispatch returns `Err`
/// with the account the refusal had resolved; a dispatch error is inside
/// `Served::dispatch_result`, because the response segment accounts for it
/// differently.
#[allow(clippy::too_many_lines)] // sequential request pipeline
async fn serve_request(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    body: &Bytes,
    peer_addr: std::net::SocketAddr,
    operation: &str,
    input: Value,
) -> Result<Served, Refused> {
    // --- Auth segment ---
    let auth_start = std::time::Instant::now();
    let identity = state
        .auth
        .authenticate(headers, body)
        .await
        .map_err(|error| Refused {
            error,
            account_id: None,
        })?;
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
            return Err(Refused {
                error: DynamoDbError::AccessDeniedException(
                    "User: is not authorized to perform this operation".to_owned(),
                ),
                account_id: Some(account_id.to_string()),
            });
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
        .await
        .map_err(|error| Refused {
            error,
            account_id: Some(account_id.to_string()),
        })?;
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
    let partition_value = extract_partition_value(&input, operation);
    let index_name = extract_index_name(&input, operation);
    throttle_segment::check(
        state,
        &ctx,
        operation,
        table_name.as_deref(),
        partition_value.as_deref(),
        index_name.as_deref(),
    )
    .await?;
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
        index_name,
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
mod observer_tests;
#[cfg(test)]
mod request_scope_tests;
#[cfg(test)]
mod start_server_tests;
#[cfg(test)]
mod throttle_wire_tests;

#[cfg(test)]
mod index_throttle_wire_tests;
