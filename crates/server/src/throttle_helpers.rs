// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Throttle integration helpers for the server layer.
//!
//! Bridges the `ThrottleManager` (in `extenddb_core::throttle`) with the server's
//! request handling: classifying operations, extracting throughput from
//! responses, and managing bucket lifecycle.

use extenddb_core::error::DynamoDbError;
use extenddb_core::throttle::ThrottleManager;
use serde_json::Value;

/// Convert a `TableDescription` to a `TableThroughput` for throttle registration.
pub(crate) fn table_description_to_throughput(
    desc: &extenddb_core::types::TableDescription,
) -> extenddb_core::throttle::TableThroughput {
    let is_on_demand = desc
        .billing_mode_summary
        .as_ref()
        .is_some_and(|bms| bms.billing_mode == extenddb_core::types::BillingMode::PayPerRequest);
    if is_on_demand {
        // A table whose OnDemandThroughput caps are set is sized by them; the
        // service names that limit on a throttle (MaxOnDemandThroughputExceeded,
        // measured 2026-10-24), so the manager must know which case it is.
        match desc.on_demand_throughput.as_ref() {
            Some(caps) => extenddb_core::throttle::TableThroughput::on_demand_capped(
                caps.max_read_request_units,
                caps.max_write_request_units,
            ),
            None => extenddb_core::throttle::TableThroughput::on_demand(),
        }
    } else {
        extenddb_core::throttle::TableThroughput::provisioned(
            desc.provisioned_throughput.read_capacity_units,
            desc.provisioned_throughput.write_capacity_units,
        )
    }
}

/// Classify a `DynamoDB` operation as read, write, or control-plane.
///
/// Returns `(is_read, is_write)`. Control-plane operations (`CreateTable`,
/// `DeleteTable`, etc.) return `(false, false)` and are not throttled.
/// Batch and transact operations are classified by their dominant type.
pub(crate) fn classify_data_operation(operation: &str) -> (bool, bool) {
    match operation {
        "GetItem" | "Query" | "Scan" | "BatchGetItem" | "TransactGetItems" => (true, false),
        "PutItem" | "DeleteItem" | "UpdateItem" | "BatchWriteItem" | "TransactWriteItems" => {
            (false, true)
        }
        _ => (false, false),
    }
}

/// Update throttle buckets after a successful table lifecycle operation.
///
/// - `CreateTable` / `DescribeTable`: registers the table's throughput.
/// - `UpdateTable`: updates the table's throughput.
/// - `DeleteTable`: removes the table's buckets.
///
/// Extracts throughput from the response body's `TableDescription` to avoid
/// an extra database round-trip.
pub(crate) fn update_throttle_buckets(
    throttle: &ThrottleManager,
    operation: &str,
    account_id: &str,
    table_name: Option<&str>,
    body: &Value,
) {
    let Some(tn) = table_name else { return };
    match operation {
        "CreateTable" | "UpdateTable" | "DescribeTable" => {
            if let Some(throughput) = extract_throughput_from_response(body) {
                throttle.register_table(account_id, tn, throughput);
            }
        }
        "DeleteTable" => {
            throttle.remove_table(account_id, tn);
        }
        _ => {}
    }
}

/// Extract table throughput configuration from a `CreateTable`/`UpdateTable`/
/// `DescribeTable` response body.
fn extract_throughput_from_response(
    body: &Value,
) -> Option<extenddb_core::throttle::TableThroughput> {
    let desc = body.get("TableDescription").or_else(|| body.get("Table"))?;

    // Check billing mode.
    let is_on_demand = desc
        .get("BillingModeSummary")
        .and_then(|bms| bms.get("BillingMode"))
        .and_then(|bm| bm.as_str())
        .is_some_and(|bm| bm == "PAY_PER_REQUEST");

    if is_on_demand {
        // Caps set on the table size its buckets; see table_description_to_throughput.
        return Some(match desc.get("OnDemandThroughput") {
            Some(caps) => extenddb_core::throttle::TableThroughput::on_demand_capped(
                caps.get("MaxReadRequestUnits").and_then(Value::as_i64),
                caps.get("MaxWriteRequestUnits").and_then(Value::as_i64),
            ),
            None => extenddb_core::throttle::TableThroughput::on_demand(),
        });
    }

    let pt = desc.get("ProvisionedThroughput")?;
    let rcu = pt.get("ReadCapacityUnits")?.as_i64()?;
    let wcu = pt.get("WriteCapacityUnits")?.as_i64()?;
    Some(extenddb_core::throttle::TableThroughput::provisioned(
        rcu, wcu,
    ))
}

/// Record throttle metrics for a batch response a backend served partially.
///
/// The server's own bucket records `ThrottledRequests` and the read or write
/// throttle event when it refuses a request. A backend throttle that leaves
/// entries under `UnprocessedKeys` or `UnprocessedItems` returns a 200 and
/// would otherwise count as no throttle at all, so this records
/// `ThrottledRequests` and the read or write throttle event once per table
/// named in the non-empty map, with the table as the dimension the bucket's
/// own records carry. The map's keys are the table names, which the batch
/// operations do not carry at the top level.
pub(crate) fn record_partial_batch_throttle(
    metrics: &extenddb_core::metrics::MetricsCollector,
    operation: &str,
    body: &Value,
) {
    use extenddb_core::metrics::MetricName;
    let (field, event) = match operation {
        "BatchGetItem" => ("UnprocessedKeys", MetricName::ReadThrottleEvents),
        "BatchWriteItem" => ("UnprocessedItems", MetricName::WriteThrottleEvents),
        _ => return,
    };
    let Some(tables) = body.get(field).and_then(Value::as_object) else {
        return;
    };
    if tables.is_empty() {
        return;
    }
    for table in tables.keys() {
        metrics.record(
            MetricName::ThrottledRequests,
            1.0,
            Some(table),
            None,
            Some(operation),
        );
        metrics.record(event, 1.0, Some(table), None, Some(operation));
    }
}

/// Extract the partition key value from a request body for per-partition throttling.
///
/// For item-level operations (`GetItem`, `PutItem`, `DeleteItem`, `UpdateItem`),
/// extracts the first attribute value from `Key` or `Item` as a string.
/// Returns `None` for batch, transact, query, scan, and control-plane operations.
pub(crate) fn extract_partition_value(input: &Value, operation: &str) -> Option<String> {
    let map = match operation {
        "GetItem" | "DeleteItem" | "UpdateItem" => input.get("Key")?,
        "PutItem" => input.get("Item")?,
        _ => return None,
    };
    // The first key in the map is the partition key (DynamoDB key maps are ordered).
    let obj = map.as_object()?;
    let (_, type_val) = obj.iter().next()?;
    let type_obj = type_val.as_object()?;
    let (_, val) = type_obj.iter().next()?;
    Some(val.as_str().unwrap_or_default().to_owned())
}

/// The throttle error the service sends for a refusal by `cause` on a table
/// billed as the cause says, naming `table_arn` as the throttled resource.
/// `None` (a name no table can have) sends the class and message with no
/// reasons list, since the service cannot name a resource that cannot exist.
///
/// Measured against the service (2026-10-20 and 2026-10-24, two Regions): a
/// provisioned table's own capacity gives `ProvisionedThroughputExceededException`
/// with `Table{Read|Write}ProvisionedThroughputExceeded` on every read and
/// write operation; a hot partition key on an on-demand table gives
/// `ThrottlingException` with `TableWriteKeyRangeThroughputExceeded` on a
/// write and no reasons list at all on a read; an on-demand table at its
/// `OnDemandThroughput` caps gives `ThrottlingException` with the caps
/// message and `Table{Read|Write}MaxOnDemandThroughputExceeded`. A hot key on a
/// provisioned table was captured once on 2026-10-20 (a `BatchWriteItem`):
/// the provisioned class, the table message and
/// `TableWriteKeyRangeThroughputExceeded`; the read form follows the
/// documented grammar. Documented in the API Reference (ThrottlingReason,
/// PutItem Errors; read 2026-10-20) and not produced by the probes: an
/// on-demand table past its default ceiling and the account aggregate carry
/// `Table{Read|Write}AccountLimitExceeded`, on `ThrottlingException` and
/// `RequestLimitExceeded` respectively.
pub(crate) fn throttle_error(
    cause: extenddb_core::throttle::ThrottleCause,
    read: bool,
    table_arn: Option<String>,
) -> DynamoDbError {
    use extenddb_core::error::{
        MAX_ON_DEMAND_THROUGHPUT_MESSAGE, PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
        REQUEST_LIMIT_EXCEEDED_MESSAGE, THROTTLING_EXCEPTION_MESSAGE, ThrottlingReason,
    };
    use extenddb_core::throttle::ThrottleCause;
    let op = if read { "Read" } else { "Write" };
    let reason = |limit: &str| {
        table_arn
            .iter()
            .map(|resource| ThrottlingReason {
                reason: format!("Table{op}{limit}"),
                resource: resource.clone(),
            })
            .collect::<Vec<_>>()
    };
    match cause {
        ThrottleCause::Table {
            on_demand: false, ..
        } => DynamoDbError::provisioned_throughput_exceeded(
            PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
            reason("ProvisionedThroughputExceeded"),
        ),
        ThrottleCause::Partition { on_demand: false } => {
            DynamoDbError::provisioned_throughput_exceeded(
                PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
                reason("KeyRangeThroughputExceeded"),
            )
        }
        // A hot-key read on an on-demand table carries no reasons list
        // (5,615 of 5,615 captures); the write form names the key range.
        ThrottleCause::Partition { on_demand: true } if read => {
            DynamoDbError::throttling(THROTTLING_EXCEPTION_MESSAGE, vec![])
        }
        ThrottleCause::Partition { on_demand: true } => DynamoDbError::throttling(
            THROTTLING_EXCEPTION_MESSAGE,
            reason("KeyRangeThroughputExceeded"),
        ),
        // `capped` is the refusing direction's own cap, so a table capped in
        // one direction sends the default-ceiling body for the other.
        ThrottleCause::Table {
            on_demand: true,
            capped: true,
        } => DynamoDbError::throttling(
            MAX_ON_DEMAND_THROUGHPUT_MESSAGE,
            reason("MaxOnDemandThroughputExceeded"),
        ),
        ThrottleCause::Table {
            on_demand: true,
            capped: false,
        } => {
            DynamoDbError::throttling(THROTTLING_EXCEPTION_MESSAGE, reason("AccountLimitExceeded"))
        }
        ThrottleCause::Account => DynamoDbError::request_limit_exceeded(
            REQUEST_LIMIT_EXCEEDED_MESSAGE,
            reason("AccountLimitExceeded"),
        ),
    }
}

#[cfg(test)]
mod extract_throughput_tests {
    use extenddb_core::types::{
        BillingMode, BillingModeSummary, OnDemandThroughput, TableDescription,
    };
    use serde_json::json;

    use super::{extract_throughput_from_response, table_description_to_throughput};

    fn on_demand_description(caps: Option<OnDemandThroughput>) -> TableDescription {
        TableDescription {
            billing_mode_summary: Some(BillingModeSummary {
                billing_mode: BillingMode::PayPerRequest,
                last_update_to_pay_per_request_date_time: None,
            }),
            on_demand_throughput: caps,
            ..TableDescription::default()
        }
    }

    // The typed path registers a table lazily after a restart; it reads the
    // caps the same way the response-body path does.
    #[test]
    fn typed_on_demand_description_with_caps_is_sized_by_them() {
        let t = table_description_to_throughput(&on_demand_description(Some(OnDemandThroughput {
            max_read_request_units: Some(1),
            max_write_request_units: Some(2),
        })));
        assert_eq!((t.rcu, t.wcu, t.on_demand), (1.0, 2.0, true));
        assert!(t.read_capped && t.write_capped);
    }

    #[test]
    fn typed_on_demand_description_without_caps_or_with_removed_caps_keeps_the_defaults() {
        let defaults = extenddb_core::throttle::TableThroughput::on_demand();
        for caps in [
            None,
            Some(OnDemandThroughput {
                max_read_request_units: Some(-1),
                max_write_request_units: None,
            }),
        ] {
            let t = table_description_to_throughput(&on_demand_description(caps));
            assert!(t.on_demand && !t.read_capped && !t.write_capped);
            assert_eq!((t.rcu, t.wcu), (defaults.rcu, defaults.wcu));
        }
    }

    #[test]
    fn provisioned_description_gives_its_units() {
        let t = extract_throughput_from_response(&json!({
            "TableDescription": {"ProvisionedThroughput": {"ReadCapacityUnits": 7, "WriteCapacityUnits": 3}}
        }))
        .expect("throughput");
        assert_eq!((t.rcu, t.wcu, t.on_demand), (7.0, 3.0, false));
        assert!(!t.read_capped && !t.write_capped);
    }

    #[test]
    fn on_demand_description_without_caps_gives_the_default_ceilings() {
        let t = extract_throughput_from_response(&json!({
            "Table": {"BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"}}
        }))
        .expect("throughput");
        assert!(t.on_demand && !t.read_capped && !t.write_capped);
        let defaults = extenddb_core::throttle::TableThroughput::on_demand();
        assert_eq!((t.rcu, t.wcu), (defaults.rcu, defaults.wcu));
    }

    #[test]
    fn on_demand_description_with_caps_is_sized_by_them() {
        let t = extract_throughput_from_response(&json!({
            "Table": {
                "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"},
                "OnDemandThroughput": {"MaxReadRequestUnits": 1, "MaxWriteRequestUnits": 2}
            }
        }))
        .expect("throughput");
        assert_eq!((t.rcu, t.wcu, t.on_demand), (1.0, 2.0, true));
        assert!(t.read_capped && t.write_capped);
    }
}

#[cfg(test)]
mod throttle_error_tests {
    use extenddb_core::error::{
        DynamoDbError, MAX_ON_DEMAND_THROUGHPUT_MESSAGE, PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
        REQUEST_LIMIT_EXCEEDED_MESSAGE, THROTTLING_EXCEPTION_MESSAGE,
    };
    use extenddb_core::throttle::ThrottleCause;

    use super::throttle_error;

    const ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/T";

    fn parts(err: &DynamoDbError) -> (&'static str, String, Vec<(String, String)>) {
        let class = match err {
            DynamoDbError::ProvisionedThroughputExceededException { .. } => "provisioned",
            DynamoDbError::ThrottlingException { .. } => "throttling",
            DynamoDbError::RequestLimitExceeded { .. } => "request-limit",
            other => panic!("not a throttle error: {other:?}"),
        };
        let reasons = err
            .throttling_reasons()
            .expect("throttle error")
            .iter()
            .map(|r| (r.reason.clone(), r.resource.clone()))
            .collect();
        (class, err.message().to_owned(), reasons)
    }

    /// Every arm of `throttle_error`, both directions: the error class, the
    /// message, the reason and the resource.
    #[test]
    fn maps_every_cause_and_direction() {
        let table = |on_demand, capped| ThrottleCause::Table { on_demand, capped };
        let one = |limit: &str| vec![(limit.to_owned(), ARN.to_owned())];
        // (cause, read, class, message, reasons)
        type Case = (
            ThrottleCause,
            bool,
            &'static str,
            &'static str,
            Vec<(String, String)>,
        );
        let cases: Vec<Case> = vec![
            (
                table(false, false),
                true,
                "provisioned",
                PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
                one("TableReadProvisionedThroughputExceeded"),
            ),
            (
                table(false, false),
                false,
                "provisioned",
                PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
                one("TableWriteProvisionedThroughputExceeded"),
            ),
            (
                ThrottleCause::Partition { on_demand: false },
                true,
                "provisioned",
                PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
                one("TableReadKeyRangeThroughputExceeded"),
            ),
            (
                ThrottleCause::Partition { on_demand: false },
                false,
                "provisioned",
                PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
                one("TableWriteKeyRangeThroughputExceeded"),
            ),
            (
                ThrottleCause::Partition { on_demand: true },
                true,
                "throttling",
                THROTTLING_EXCEPTION_MESSAGE,
                vec![],
            ),
            (
                ThrottleCause::Partition { on_demand: true },
                false,
                "throttling",
                THROTTLING_EXCEPTION_MESSAGE,
                one("TableWriteKeyRangeThroughputExceeded"),
            ),
            (
                table(true, true),
                true,
                "throttling",
                MAX_ON_DEMAND_THROUGHPUT_MESSAGE,
                one("TableReadMaxOnDemandThroughputExceeded"),
            ),
            (
                table(true, true),
                false,
                "throttling",
                MAX_ON_DEMAND_THROUGHPUT_MESSAGE,
                one("TableWriteMaxOnDemandThroughputExceeded"),
            ),
            (
                table(true, false),
                true,
                "throttling",
                THROTTLING_EXCEPTION_MESSAGE,
                one("TableReadAccountLimitExceeded"),
            ),
            (
                table(true, false),
                false,
                "throttling",
                THROTTLING_EXCEPTION_MESSAGE,
                one("TableWriteAccountLimitExceeded"),
            ),
            (
                ThrottleCause::Account,
                true,
                "request-limit",
                REQUEST_LIMIT_EXCEEDED_MESSAGE,
                one("TableReadAccountLimitExceeded"),
            ),
            (
                ThrottleCause::Account,
                false,
                "request-limit",
                REQUEST_LIMIT_EXCEEDED_MESSAGE,
                one("TableWriteAccountLimitExceeded"),
            ),
        ];
        for (cause, read, class, message, reasons) in cases {
            let err = throttle_error(cause, read, Some(ARN.to_owned()));
            assert_eq!(
                parts(&err),
                (class, message.to_owned(), reasons),
                "{cause:?} read={read}"
            );
            assert_eq!(err.status_code(), 400);
        }
    }

    /// Without a resource (a name no table can have) every arm keeps its class
    /// and message and sends no reasons list.
    #[test]
    fn no_resource_means_no_reasons_list() {
        for cause in [
            ThrottleCause::Table {
                on_demand: false,
                capped: false,
            },
            ThrottleCause::Partition { on_demand: true },
            ThrottleCause::Account,
        ] {
            let err = throttle_error(cause, false, None);
            assert_eq!(err.throttling_reasons(), Some(&[][..]), "{cause:?}");
            assert!(!err.message().is_empty());
        }
    }
}

#[cfg(test)]
mod partial_batch_throttle_tests {
    use extenddb_core::metrics::{MetricName, MetricsCollector, MetricsQuery, TimeWindow};
    use serde_json::json;

    use super::record_partial_batch_throttle;

    fn sum(c: &MetricsCollector, metric: MetricName, table: Option<&str>) -> f64 {
        c.query(&MetricsQuery {
            table_name: table.map(str::to_owned),
            metric: Some(metric),
            window: Some(TimeWindow::AllTime),
            ..Default::default()
        })
        .iter()
        .map(|s| s.sum)
        .sum()
    }

    #[test]
    fn a_partial_batch_get_records_a_request_and_a_read_event_per_table() {
        let c = MetricsCollector::new();
        record_partial_batch_throttle(
            &c,
            "BatchGetItem",
            &json!({"Responses": {"T": []}, "UnprocessedKeys": {"T": {"Keys": []}, "U": {"Keys": []}}}),
        );
        assert_eq!(sum(&c, MetricName::ThrottledRequests, None), 2.0);
        assert_eq!(sum(&c, MetricName::ThrottledRequests, Some("T")), 1.0);
        assert_eq!(sum(&c, MetricName::ThrottledRequests, Some("U")), 1.0);
        assert_eq!(sum(&c, MetricName::ReadThrottleEvents, Some("T")), 1.0);
        assert_eq!(sum(&c, MetricName::ReadThrottleEvents, Some("U")), 1.0);
        assert_eq!(sum(&c, MetricName::WriteThrottleEvents, None), 0.0);
    }

    #[test]
    fn a_partial_batch_write_records_a_write_event() {
        let c = MetricsCollector::new();
        record_partial_batch_throttle(
            &c,
            "BatchWriteItem",
            &json!({"UnprocessedItems": {"T": [{"PutRequest": {"Item": {}}}]}}),
        );
        assert_eq!(sum(&c, MetricName::ThrottledRequests, Some("T")), 1.0);
        assert_eq!(sum(&c, MetricName::WriteThrottleEvents, Some("T")), 1.0);
    }

    /// A fully served batch (the map present and empty) and a non-batch
    /// operation record nothing.
    #[test]
    fn a_full_batch_or_another_operation_records_nothing() {
        let c = MetricsCollector::new();
        record_partial_batch_throttle(&c, "BatchGetItem", &json!({"UnprocessedKeys": {}}));
        record_partial_batch_throttle(&c, "BatchWriteItem", &json!({"UnprocessedItems": {}}));
        record_partial_batch_throttle(&c, "GetItem", &json!({"UnprocessedKeys": {"T": {}}}));
        assert_eq!(sum(&c, MetricName::ThrottledRequests, None), 0.0);
        assert_eq!(sum(&c, MetricName::ReadThrottleEvents, None), 0.0);
    }
}
