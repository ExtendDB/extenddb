// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The measured sentences are the ones Amazon `DynamoDB` returned on
//! 2026-10-31 in us-east-1, on an account whose per-table and account maxima
//! were both 150,000 read and 200,000 write units, holding 61 and 46.

use super::*;
use crate::types::{
    BillingModeSummary, GsiDescription, Projection, ProjectionType,
    ProvisionedThroughputDescription,
};
use serde_json::json;

const REGION: &str = "us-east-1";

/// The limits of the measured account.
fn measured_limits() -> LimitsConfig {
    LimitsConfig {
        per_table_max_rcu: 150_000,
        per_table_max_wcu: 200_000,
        per_account_max_rcu: 150_000,
        per_account_max_wcu: 200_000,
        ..LimitsConfig::default()
    }
}

/// What the measured account held.
const MEASURED_RESERVED: CapacityUnits = CapacityUnits {
    read: 61,
    write: 46,
};

fn units(read: i64, write: i64) -> CapacityUnits {
    CapacityUnits { read, write }
}

fn throughput(read: i64, write: i64) -> ProvisionedThroughput {
    ProvisionedThroughput {
        read_capacity_units: read,
        write_capacity_units: write,
    }
}

fn gsi(name: &str, read: i64, write: i64) -> GsiDescription {
    GsiDescription {
        index_name: name.to_owned(),
        key_schema: Vec::new(),
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
        index_status: "ACTIVE".to_owned(),
        provisioned_throughput: Some(ProvisionedThroughputDescription {
            read_capacity_units: read,
            write_capacity_units: write,
            ..ProvisionedThroughputDescription::default()
        }),
        index_size_bytes: 0,
        item_count: 0,
        index_arn: String::new(),
    }
}

/// A provisioned table holding `read` and `write` units, with `gsis`.
fn table(read: i64, write: i64, gsis: Vec<GsiDescription>) -> TableDescription {
    TableDescription {
        provisioned_throughput: ProvisionedThroughputDescription {
            read_capacity_units: read,
            write_capacity_units: write,
            ..ProvisionedThroughputDescription::default()
        },
        global_secondary_indexes: (!gsis.is_empty()).then_some(gsis),
        ..TableDescription::default()
    }
}

fn update(body: &serde_json::Value) -> UpdateTableInput {
    serde_json::from_value(body.clone()).expect("UpdateTable input")
}

fn create(body: &serde_json::Value) -> CreateTableInput {
    serde_json::from_value(body.clone()).expect("CreateTable input")
}

fn refusal(result: Result<(), DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::LimitExceededException(msg)) => msg,
        other => panic!("expected LimitExceededException, got {other:?}"),
    }
}

#[test]
fn update_table_over_the_per_table_maximum_is_the_measured_sentence() {
    let existing = table(5, 5, Vec::new());
    let check = |read, write| {
        let request = throughput(read, write);
        let after = units(read, write);
        check_update_table_capacity(
            Some(&request),
            &existing,
            MEASURED_RESERVED,
            after,
            &measured_limits(),
            REGION,
        )
    };
    assert_eq!(
        refusal(check(150_001, 5)),
        "The requested ReadCapacityUnits, 150001, is above the per table maximum for the \
         account in us-east-1. Per table maximum: 150000. Refer to the Amazon DynamoDB \
         Developer Guide for current limits and how to request higher limits."
    );
    assert_eq!(
        refusal(check(5, 200_001)),
        "The requested WriteCapacityUnits, 200001, is above the per table maximum for the \
         account in us-east-1. Per table maximum: 200000. Refer to the Amazon DynamoDB \
         Developer Guide for current limits and how to request higher limits."
    );
}

#[test]
fn create_table_over_the_maximum_is_the_measured_account_sentence() {
    let check = |read, write| {
        check_create_table_capacity(
            units(read, write),
            MEASURED_RESERVED,
            &measured_limits(),
            REGION,
        )
    };
    let read_sentence = "This request would have caused the ReadCapacityUnits limit to be \
                         exceeded for the account in us-east-1. Current ReadCapacityUnits \
                         reserved by the account: 61. Limit: 150000. Requested: 150001. Refer \
                         to the Amazon DynamoDB Developer Guide for current limits and how to \
                         request higher limits.";
    assert_eq!(refusal(check(150_001, 1)), read_sentence);
    assert_eq!(
        refusal(check(1, 200_001)),
        "This request would have caused the WriteCapacityUnits limit to be exceeded for the \
         account in us-east-1. Current WriteCapacityUnits reserved by the account: 46. Limit: \
         200000. Requested: 200001. Refer to the Amazon DynamoDB Developer Guide for current \
         limits and how to request higher limits."
    );
    // Both over: the read sentence.
    assert_eq!(refusal(check(150_001, 200_001)), read_sentence);
}

#[test]
fn a_gsi_create_over_the_maximum_is_the_measured_account_sentence() {
    let existing = table(1, 1, Vec::new());
    let check = |read, write| {
        let input = update(&json!({
            "TableName": "t",
            "AttributeDefinitions": [{ "AttributeName": "g", "AttributeType": "S" }],
            "GlobalSecondaryIndexUpdates": [{ "Create": {
                "IndexName": "gix",
                "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
                "Projection": { "ProjectionType": "ALL" },
                "ProvisionedThroughput": { "ReadCapacityUnits": read, "WriteCapacityUnits": write },
            }}],
        }));
        let after = update_table_capacity(&existing, &input).expect("a provisioned GSI");
        check_update_table_capacity(
            input.provisioned_throughput.as_ref(),
            &existing,
            MEASURED_RESERVED,
            after,
            &measured_limits(),
            REGION,
        )
    };
    assert_eq!(
        refusal(check(150_001, 1)),
        "This request would have caused the ReadCapacityUnits limit to be exceeded for the \
         account in us-east-1. Current ReadCapacityUnits reserved by the account: 61. Limit: \
         150000. Current ReadCapacityUnits provisioned for the table and indexes: 1. \
         Requested: 150002. Refer to the Amazon DynamoDB Developer Guide for current limits \
         and how to request higher limits."
    );
    assert_eq!(
        refusal(check(1, 200_001)),
        "This request would have caused the WriteCapacityUnits limit to be exceeded for the \
         account in us-east-1. Current WriteCapacityUnits reserved by the account: 46. Limit: \
         200000. Current WriteCapacityUnits provisioned for the table and indexes: 1. \
         Requested: 200002. Refer to the Amazon DynamoDB Developer Guide for current limits \
         and how to request higher limits."
    );
}

#[test]
fn create_table_at_each_maximum_is_accepted_and_one_above_is_refused() {
    let limits = LimitsConfig::default();
    // At the per-table maximum, with the account at its maximum too.
    check_create_table_capacity(
        units(40_000, 40_000),
        units(40_000, 40_000),
        &limits,
        REGION,
    )
    .expect("at both maxima");
    // One unit over the per-table maximum, the account well under its own.
    assert_eq!(
        refusal(check_create_table_capacity(
            units(40_001, 1),
            CapacityUnits::default(),
            &limits,
            REGION
        )),
        "The requested ReadCapacityUnits, 40001, is above the per table maximum for the \
         account in us-east-1. Per table maximum: 40000. Refer to the Amazon DynamoDB \
         Developer Guide for current limits and how to request higher limits."
    );
    assert!(
        refusal(check_create_table_capacity(
            units(1, 40_001),
            CapacityUnits::default(),
            &limits,
            REGION
        ))
        .starts_with("The requested WriteCapacityUnits, 40001, is above the per table maximum")
    );
    // One unit over the account maximum, the table under its own.
    assert!(
        refusal(check_create_table_capacity(
            units(40_000, 1),
            units(40_001, 0),
            &limits,
            REGION
        ))
        .starts_with(
            "This request would have caused the ReadCapacityUnits limit to be exceeded for the \
             account in us-east-1. Current ReadCapacityUnits reserved by the account: 40001. \
             Limit: 80000. Requested: 40000."
        )
    );
}

#[test]
fn update_table_at_each_maximum_is_accepted_and_one_above_is_refused() {
    let limits = LimitsConfig::default();
    let existing = table(10, 10, Vec::new());
    let check = |read, write, reserved| {
        let request = throughput(read, write);
        check_update_table_capacity(
            Some(&request),
            &existing,
            reserved,
            units(read, write),
            &limits,
            REGION,
        )
    };
    check(40_000, 40_000, units(10, 10)).expect("at the per-table maximum");
    assert!(
        refusal(check(40_001, 10, units(10, 10)))
            .starts_with("The requested ReadCapacityUnits, 40001, is above the per table maximum")
    );
    // The account holds 70,000 including this table's 10: raising it to 10,010
    // fills the account to 80,000, and 10,011 takes it one over.
    check(10_010, 10, units(70_000, 10)).expect("at the account maximum");
    assert!(refusal(check(10_011, 10, units(70_000, 10))).starts_with(
        "This request would have caused the ReadCapacityUnits limit to be exceeded for the \
             account in us-east-1. Current ReadCapacityUnits reserved by the account: 70000. \
             Limit: 80000. Current ReadCapacityUnits provisioned for the table and indexes: 10. \
             Requested: 10011."
    ));
}

#[test]
fn a_table_above_a_lowered_maximum_can_still_be_lowered() {
    let limits = LimitsConfig {
        per_table_max_rcu: 100,
        per_table_max_wcu: 100,
        per_account_max_rcu: 200,
        per_account_max_wcu: 200,
        ..LimitsConfig::default()
    };
    let existing = table(500, 500, Vec::new());
    let request = throughput(300, 300);
    check_update_table_capacity(
        Some(&request),
        &existing,
        units(1_000, 1_000),
        units(300, 300),
        &limits,
        REGION,
    )
    .expect("a decrease is not refused");
}

#[test]
fn the_table_quota_admits_the_last_table_and_refuses_the_next() {
    let limits = LimitsConfig {
        max_tables_per_account: 3,
        ..LimitsConfig::default()
    };
    check_table_count(2, &limits).expect("the third table fills the quota");
    assert_eq!(
        refusal(check_table_count(3, &limits)),
        "There is a soft account quota of 3 tables."
    );
}

#[test]
fn create_table_capacity_sums_the_table_and_its_gsis() {
    let gsi = |name: &str, pt: serde_json::Value| {
        let mut index = json!({
            "IndexName": name,
            "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" },
        });
        if !pt.is_null() {
            index["ProvisionedThroughput"] = pt;
        }
        index
    };
    let provisioned = create(&json!({
        "TableName": "t",
        "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
        "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
        "ProvisionedThroughput": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 7 },
        "GlobalSecondaryIndexes": [
            gsi("a", json!({ "ReadCapacityUnits": 10, "WriteCapacityUnits": 20 })),
            gsi("b", serde_json::Value::Null),
        ],
    }));
    assert_eq!(create_table_capacity(&provisioned), Some(units(15, 27)));
    let on_demand = create(&json!({
        "TableName": "t",
        "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
        "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
        "BillingMode": "PAY_PER_REQUEST",
    }));
    assert_eq!(create_table_capacity(&on_demand), None);
}

#[test]
fn table_capacity_counts_gsis_and_nothing_for_on_demand() {
    let provisioned = table(5, 6, vec![gsi("a", 10, 20), gsi("b", 1, 2)]);
    assert_eq!(table_capacity(&provisioned), units(16, 28));
    let on_demand = TableDescription {
        billing_mode_summary: Some(BillingModeSummary {
            billing_mode: BillingMode::PayPerRequest,
            last_update_to_pay_per_request_date_time: None,
        }),
        ..table(5, 6, vec![gsi("a", 10, 20)])
    };
    assert_eq!(table_capacity(&on_demand), CapacityUnits::default());
}

#[test]
fn update_table_capacity_replaces_the_table_and_follows_index_changes() {
    let existing = table(5, 6, vec![gsi("kept", 10, 20), gsi("dropped", 100, 200)]);
    let created = json!({ "Create": {
        "IndexName": "new",
        "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
        "Projection": { "ProjectionType": "ALL" },
        "ProvisionedThroughput": { "ReadCapacityUnits": 3, "WriteCapacityUnits": 4 },
    }});
    let input = update(&json!({
        "TableName": "t",
        "ProvisionedThroughput": { "ReadCapacityUnits": 50, "WriteCapacityUnits": 60 },
        "GlobalSecondaryIndexUpdates": [created, { "Delete": { "IndexName": "dropped" } }],
    }));
    assert_eq!(
        update_table_capacity(&existing, &input),
        Some(units(50 + 10 + 3, 60 + 20 + 4))
    );
    // Nothing that sets a throughput: no capacity to check.
    let tags_only = update(&json!({ "TableName": "t", "DeletionProtectionEnabled": true }));
    assert!(!changes_provisioned_capacity(&tags_only));
    assert_eq!(update_table_capacity(&existing, &tags_only), None);
    // Switching to on-demand: nothing provisioned afterwards.
    let to_on_demand = update(&json!({
        "TableName": "t",
        "BillingMode": "PAY_PER_REQUEST",
        "ProvisionedThroughput": { "ReadCapacityUnits": 50, "WriteCapacityUnits": 60 },
    }));
    assert_eq!(update_table_capacity(&existing, &to_on_demand), None);
}
