// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The throttle segment of a data-plane request: the table's buckets are
//! registered the first time the manager sees the table, checked, and a
//! refusal becomes the throttle error the service sends.

use std::sync::Arc;

use extenddb_engine::OperationContext;

use super::Refused;
use crate::AppState;
use crate::throttle_helpers::{
    classify_data_operation, table_description_to_index_throughputs,
    table_description_to_throughput, throttle_error,
};

/// Check the throttle buckets for `operation` on `table_name`, registering
/// the table and its provisioned global secondary indexes from its
/// description first when the manager has no buckets for it. A control-plane
/// operation, or a request with no table name, is not throttled. `index_name`
/// is the index a `Query` or `Scan` names; a provisioned global secondary
/// index is checked on its own read bucket, and a write is checked on every
/// registered index's write bucket after the table's. A refusal carries the
/// account the request authenticated as.
pub(super) async fn check(
    state: &Arc<AppState>,
    ctx: &OperationContext,
    operation: &str,
    table_name: Option<&str>,
    partition_value: Option<&str>,
    index_name: Option<&str>,
) -> Result<(), Refused> {
    let (is_read_op, is_write_op) = classify_data_operation(operation);
    if let Some(tn) = table_name
        && (is_read_op || is_write_op)
    {
        if !state.throttle.is_registered(&ctx.account_id, tn)
            && let Ok(desc) = ctx
                .storage
                .describe_table(
                    &ctx.account_id,
                    extenddb_core::types::DescribeTableInput {
                        table_name: tn.to_owned(),
                    },
                )
                .await
        {
            let throughput = table_description_to_throughput(&desc);
            state
                .throttle
                .register_table(&ctx.account_id, tn, throughput);
            state.throttle.register_indexes(
                &ctx.account_id,
                tn,
                table_description_to_index_throughputs(&desc),
            );
        }

        let result = state.throttle.check_capacity_with_partition(
            &ctx.account_id,
            tn,
            is_read_op,
            is_write_op,
            partition_value,
            index_name,
        );
        if let Some(cause) = result.cause() {
            let throttled_read = matches!(
                result,
                extenddb_core::throttle::ThrottleResult::ThrottledRead(_)
            );
            // The throttle event metrics carry the index that refused as
            // their index dimension, as the service publishes
            // Read/WriteThrottleEvents per table and per global secondary
            // index; ThrottledRequests is published per table and operation
            // only, so a count by table sees an index refusal too.
            let refused_index = cause.index_name().map(str::to_owned);
            let metric = if throttled_read {
                extenddb_core::metrics::MetricName::ReadThrottleEvents
            } else {
                extenddb_core::metrics::MetricName::WriteThrottleEvents
            };
            state.metrics.record(
                metric,
                1.0,
                Some(tn),
                refused_index.as_deref(),
                Some(operation),
            );
            state.metrics.record(
                extenddb_core::metrics::MetricName::ThrottledRequests,
                1.0,
                Some(tn),
                None,
                Some(operation),
            );
            // The service names the throttled resource and the limit on a
            // throttle: one reason with the table ARN, the same string
            // DescribeTable returns as TableArn, or the index ARN (its
            // IndexArn) when an index's bucket refused. The error class, the
            // message and the limit follow the bucket that refused and the
            // table's billing mode. The account bucket can refuse before the
            // engine has seen the name, so an ARN naming a table in this
            // account is reduced to the bare name first, and a name the
            // engine would refuse gets no resource at all.
            let name = crate::authz_resource::key_lookup_name(tn, &state.region, &ctx.account_id);
            let resource_arn = extenddb_core::validation::validate_table_name_chars(&name)
                .is_ok()
                .then(|| match &refused_index {
                    Some(index) => extenddb_storage::util::index_arn(
                        &state.region,
                        &ctx.account_id,
                        &name,
                        index,
                    ),
                    None => {
                        extenddb_storage::util::table_arn(&state.region, &ctx.account_id, &name)
                    }
                });
            return Err(Refused {
                error: throttle_error(cause, throttled_read, resource_arn),
                account_id: Some(ctx.account_id.to_string()),
            });
        }
    }
    Ok(())
}
