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

/// Record the throttle events of a batch response a backend served partially.
///
/// A backend throttle that leaves requests or keys unprocessed returns a 200
/// that the server's own bucket never saw. CloudWatch counts each throttled
/// entry of a batch as a read or write throttle event, and counts the batch
/// in `ThrottledRequests` only when every entry was throttled, which answers
/// the throttle error (`record_error_metrics` counts that one). So this
/// records one data point per table the engine reports
/// (`DispatchMetrics::throttled_tables`), with the number of the table's
/// throttled entries as its value, and the account and the table as the
/// dimensions the bucket's own records carry. The response's
/// `UnprocessedItems` is no throttle count: it also holds requests deferred
/// for a transient, conflict or connection failure.
pub(crate) fn record_partial_batch_throttle(
    metrics: &extenddb_core::metrics::MetricsCollector,
    account_id: &str,
    operation: &str,
    throttled_tables: &std::collections::BTreeMap<String, u32>,
) {
    use extenddb_core::metrics::MetricName;
    let event = match operation {
        "BatchGetItem" => MetricName::ReadThrottleEvents,
        "BatchWriteItem" => MetricName::WriteThrottleEvents,
        _ => return,
    };
    for (table, entries) in throttled_tables {
        metrics.record(
            event,
            f64::from(*entries),
            Some(account_id),
            Some(table),
            None,
            Some(operation),
        );
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
    table_arn: &str,
) -> DynamoDbError {
    use extenddb_core::error::{
        MAX_ON_DEMAND_THROUGHPUT_MESSAGE, PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
        REQUEST_LIMIT_EXCEEDED_MESSAGE, THROTTLING_EXCEPTION_MESSAGE, ThrottlingReason,
    };
    use extenddb_core::throttle::ThrottleCause;
    let op = if read { "Read" } else { "Write" };
    let reason = |limit: &str| {
        vec![ThrottlingReason {
            reason: format!("Table{op}{limit}"),
            resource: table_arn.to_owned(),
        }]
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
            let err = throttle_error(cause, read, ARN);
            assert_eq!(
                parts(&err),
                (class, message.to_owned(), reasons),
                "{cause:?} read={read}"
            );
            assert_eq!(err.status_code(), 400);
        }
    }
}

#[cfg(test)]
mod partial_batch_throttle_tests {
    use std::collections::BTreeMap;

    use extenddb_core::metrics::{MetricName, MetricsCollector, MetricsQuery, TimeWindow};

    use super::record_partial_batch_throttle;

    const ACCOUNT: &str = "111111111111";

    /// The sum and the number of data points of `metric` for `table`, or for
    /// every table with `None`.
    fn points(c: &MetricsCollector, metric: MetricName, table: Option<&str>) -> (f64, u64) {
        c.query(&MetricsQuery {
            account_id: Some(ACCOUNT.to_owned()),
            table_name: table.map(str::to_owned),
            metric: Some(metric),
            window: Some(TimeWindow::AllTime),
            ..Default::default()
        })
        .iter()
        .fold((0.0, 0), |(sum, count), s| (sum + s.sum, count + s.count))
    }

    /// CloudWatch counts each throttled entry of a batch as a read or write
    /// throttle event and leaves a partly served batch out of
    /// `ThrottledRequests`. Each row: the operation, the throttled entries per
    /// table, and the read and write events expected per table as (sum, data
    /// points), one point per table carrying its entry count.
    #[test]
    fn a_partial_batch_records_one_throttle_event_per_throttled_entry() {
        type Events<'a> = &'a [(&'a str, (f64, u64))];
        type Row<'a> = (&'a str, &'a [(&'a str, u32)], Events<'a>, Events<'a>);
        let rows: [Row; 4] = [
            (
                "BatchGetItem",
                &[("T", 3), ("U", 1)],
                &[("T", (3.0, 1)), ("U", (1.0, 1))],
                &[],
            ),
            ("BatchWriteItem", &[("T", 25)], &[], &[("T", (25.0, 1))]),
            ("BatchGetItem", &[], &[], &[]),
            ("GetItem", &[("T", 1)], &[], &[]),
        ];
        for (operation, throttled, reads, writes) in rows {
            let c = MetricsCollector::new();
            let throttled: BTreeMap<String, u32> = throttled
                .iter()
                .map(|(t, n)| ((*t).to_owned(), *n))
                .collect();
            record_partial_batch_throttle(&c, ACCOUNT, operation, &throttled);
            let row = format!("{operation} {throttled:?}");
            assert_eq!(
                points(&c, MetricName::ThrottledRequests, None),
                (0.0, 0),
                "{row}"
            );
            for (metric, expected) in [
                (MetricName::ReadThrottleEvents, reads),
                (MetricName::WriteThrottleEvents, writes),
            ] {
                let total = expected
                    .iter()
                    .fold((0.0, 0), |(sum, count), (_, (s, n))| (sum + s, count + n));
                assert_eq!(points(&c, metric, None), total, "{row} {metric:?}");
                for (table, sum_and_points) in expected {
                    assert_eq!(
                        points(&c, metric, Some(table)),
                        *sum_and_points,
                        "{row} {metric:?} {table}"
                    );
                }
            }
        }
    }
}
