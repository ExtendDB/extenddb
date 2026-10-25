// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use extenddb_core::error::{
    DynamoDbError, INDEX_WRITE_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
    PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
};
use extenddb_core::throttle::{IndexThroughput, ThrottleCause};
use extenddb_core::types::{
    BillingMode, BillingModeSummary, GsiDescription, ProvisionedThroughputDescription,
    TableDescription,
};
use serde_json::json;

use super::{
    extract_index_name, extract_index_throughputs_from_response,
    table_description_to_index_throughputs, throttle_error,
};

fn gsi(name: &str, throughput: Option<(i64, i64)>) -> GsiDescription {
    GsiDescription {
        index_name: name.to_owned(),
        key_schema: Vec::new(),
        projection: extenddb_core::types::Projection {
            projection_type: extenddb_core::types::ProjectionType::All,
            non_key_attributes: None,
        },
        index_status: "ACTIVE".to_owned(),
        provisioned_throughput: throughput.map(|(rcu, wcu)| ProvisionedThroughputDescription {
            read_capacity_units: rcu,
            write_capacity_units: wcu,
            number_of_decreases_today: 0,
            last_increase_date_time: None,
            last_decrease_date_time: None,
        }),
        index_size_bytes: 0,
        item_count: 0,
        index_arn: String::new(),
    }
}

fn description(on_demand: bool, gsis: Vec<GsiDescription>) -> TableDescription {
    TableDescription {
        billing_mode_summary: on_demand.then_some(BillingModeSummary {
            billing_mode: BillingMode::PayPerRequest,
            last_update_to_pay_per_request_date_time: None,
        }),
        global_secondary_indexes: Some(gsis),
        ..TableDescription::default()
    }
}

/// The typed path: every provisioned index in order; one without throughput
/// and one at the 0/0 placeholder give no buckets; an on-demand table's
/// indexes give none.
#[test]
fn typed_description_lists_the_provisioned_indexes_in_order() {
    let desc = description(
        false,
        vec![
            gsi("b", Some((5, 6))),
            gsi("none", None),
            gsi("zero", Some((0, 0))),
            gsi("a", Some((1, 2))),
        ],
    );
    assert_eq!(
        table_description_to_index_throughputs(&desc),
        vec![
            IndexThroughput::provisioned("b", 5, 6),
            IndexThroughput::provisioned("a", 1, 2),
        ]
    );
    let on_demand = description(true, vec![gsi("b", Some((5, 6)))]);
    assert!(table_description_to_index_throughputs(&on_demand).is_empty());
    let no_indexes = TableDescription::default();
    assert!(table_description_to_index_throughputs(&no_indexes).is_empty());
}

/// The response-body path reads the same list under `TableDescription` or
/// `Table`, and gives `None` only when neither is present.
#[test]
fn response_body_lists_the_provisioned_indexes() {
    let body = json!({"TableDescription": {
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        "GlobalSecondaryIndexes": [
            {"IndexName": "b", "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 6}},
            {"IndexName": "none"},
            {"IndexName": "zero", "ProvisionedThroughput": {"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}},
            {"IndexName": "a", "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 2}},
        ],
    }});
    assert_eq!(
        extract_index_throughputs_from_response(&body),
        Some(vec![
            IndexThroughput::provisioned("b", 5, 6),
            IndexThroughput::provisioned("a", 1, 2),
        ])
    );
    let on_demand = json!({"Table": {
        "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"},
        "GlobalSecondaryIndexes": [
            {"IndexName": "b", "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 6}},
        ],
    }});
    assert_eq!(
        extract_index_throughputs_from_response(&on_demand),
        Some(Vec::new())
    );
    let without_indexes = json!({"Table": {
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    }});
    assert_eq!(
        extract_index_throughputs_from_response(&without_indexes),
        Some(Vec::new())
    );
    assert_eq!(
        extract_index_throughputs_from_response(&json!({"Items": []})),
        None
    );
}

/// Only `Query` and `Scan` name an index.
#[test]
fn index_name_is_read_from_query_and_scan_only() {
    let input = json!({"TableName": "t", "IndexName": "i"});
    assert_eq!(extract_index_name(&input, "Query").as_deref(), Some("i"));
    assert_eq!(extract_index_name(&input, "Scan").as_deref(), Some("i"));
    for operation in ["GetItem", "PutItem", "BatchGetItem", "UpdateTable"] {
        assert_eq!(extract_index_name(&input, operation), None, "{operation}");
    }
    assert_eq!(
        extract_index_name(&json!({"TableName": "t"}), "Query"),
        None
    );
    assert_eq!(extract_index_name(&json!({"IndexName": 7}), "Scan"), None);
}

/// The two measured index bodies: the provisioned class for both; the table
/// message on a read and the index message (no trailing period) on a write;
/// the reason in the `Index` scope; the resource as given. Without a resource
/// there is no reasons list, as for the table arms.
#[test]
fn index_cause_maps_to_the_two_measured_bodies() {
    const INDEX_ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/T/index/I";
    let cause = ThrottleCause::Index {
        index_name: "I".to_owned(),
    };
    for (read, message, reason) in [
        (
            true,
            PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
            "IndexReadProvisionedThroughputExceeded",
        ),
        (
            false,
            INDEX_WRITE_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
            "IndexWriteProvisionedThroughputExceeded",
        ),
    ] {
        let err = throttle_error(&cause, read, Some(INDEX_ARN.to_owned()));
        assert!(
            matches!(
                err,
                DynamoDbError::ProvisionedThroughputExceededException { .. }
            ),
            "read={read}: {err:?}"
        );
        assert_eq!(err.message(), message, "read={read}");
        let reasons = err.throttling_reasons().expect("reasons");
        assert_eq!(reasons.len(), 1, "read={read}");
        assert_eq!(reasons[0].reason, reason, "read={read}");
        assert_eq!(reasons[0].resource, INDEX_ARN, "read={read}");

        let without = throttle_error(&cause, read, None);
        assert_eq!(without.throttling_reasons(), Some(&[][..]), "read={read}");
    }
    assert!(!INDEX_WRITE_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE.ends_with('.'));
    assert!(PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE.ends_with('.'));
}
