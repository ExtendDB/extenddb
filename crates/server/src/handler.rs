// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Main `DynamoDB` request handler with per-segment latency instrumentation.
//!
//! Extracted from `lib.rs` to keep both files under the 500-line limit.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;
use extenddb_core::error::DynamoDbError;
use extenddb_engine::OperationContext;
use serde_json::Value;

use crate::AppState;
use crate::request_helpers::{authorize_request, extract_operation, extract_table_name};
use crate::response::{error_response, record_error_metrics, success_response};
use crate::throttle_helpers::{
    classify_data_operation, extract_partition_value, table_description_to_throughput,
    update_throttle_buckets,
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

    // --- Auth segment ---
    let auth_start = std::time::Instant::now();
    let identity = match state.auth.authenticate(&headers, &body).await {
        Ok(id) => id,
        Err(e) => return error_response(&e, &request_id),
    };
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
    let pre_fetched_key_info;
    {
        if state.catalog_store.is_none() {
            tracing::error!("Authorization required but catalog_store is not configured");
            return error_response(
                &DynamoDbError::AccessDeniedException(
                    "User: is not authorized to perform this operation".to_owned(),
                ),
                &request_id,
            );
        };
        match authorize_request(&state, &identity, &input, &operation, &account_id).await {
            Ok(ki) => pre_fetched_key_info = ki,
            Err(e) => return error_response(&e, &request_id),
        }
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
        pre_fetched_key_info,
        auth_cache: state.auth_cache.clone(),
        table_key_info_lookup: Some(
            state.table_key_info_cache.clone() as Arc<dyn extenddb_storage::TableKeyInfoLookup>
        ),
    };

    let table_name = extract_table_name(&input);

    // --- Throttle segment ---
    let throttle_start = std::time::Instant::now();
    let (is_read_op, is_write_op) = classify_data_operation(&operation);
    let partition_value = extract_partition_value(&input, &operation);
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
        if result != extenddb_core::throttle::ThrottleResult::Allowed {
            let metric = if result == extenddb_core::throttle::ThrottleResult::ThrottledRead {
                extenddb_core::metrics::MetricName::ReadThrottleEvents
            } else {
                extenddb_core::metrics::MetricName::WriteThrottleEvents
            };
            state
                .metrics
                .record(metric, 1.0, Some(tn), None, Some(&operation));
            state.metrics.record(
                extenddb_core::metrics::MetricName::ThrottledRequests,
                1.0,
                Some(tn),
                None,
                Some(&operation),
            );
            return error_response(
                &DynamoDbError::ProvisionedThroughputExceededException(
                    "The level of configured provisioned throughput for the table \
                         was exceeded. Consider increasing your provisioning level \
                         with the UpdateTable API."
                        .to_owned(),
                ),
                &request_id,
            );
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let throttle_us = throttle_start.elapsed().as_micros() as f64;

    // --- Dispatch segment ---
    let dispatch_start = std::time::Instant::now();
    let dispatch_result =
        extenddb_engine::dispatch(&operation, input, &ctx, &state.server_addr).await;
    #[allow(clippy::cast_precision_loss)]
    let dispatch_us = dispatch_start.elapsed().as_micros() as f64;
    // P120c: Record storage query metrics for the request's dispatch phase.
    let category = match operation.as_str() {
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

    // --- Response segment ---
    let response_start = std::time::Instant::now();
    let response = match dispatch_result {
        Ok(result) => {
            update_throttle_buckets(
                &state.throttle,
                &operation,
                &ctx.account_id,
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
                    &ctx.account_id,
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
