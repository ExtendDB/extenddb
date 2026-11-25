// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The measured sentences are the ones Amazon `DynamoDB` returned on
//! 2026-10-31 in us-east-1, on an account whose per-table and account maxima
//! were both 150,000 read and 200,000 write units, holding 61 and 46.

use super::*;
use crate::types::{BillingModeSummary, GsiDescription, Projection, ProjectionType};
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
        warm_throughput: None,
        warm_throughput_record: None,
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
    assert_eq!(
        refusal(check_create_table_capacity(
            units(1, 40_001),
            CapacityUnits::default(),
            &limits,
            REGION
        )),
        "The requested WriteCapacityUnits, 40001, is above the per table maximum for the \
         account in us-east-1. Per table maximum: 40000. Refer to the Amazon DynamoDB \
         Developer Guide for current limits and how to request higher limits."
    );
    // One unit over the account maximum, the table under its own.
    assert_eq!(
        refusal(check_create_table_capacity(
            units(40_000, 1),
            units(40_001, 0),
            &limits,
            REGION
        )),
        "This request would have caused the ReadCapacityUnits limit to be exceeded for the \
         account in us-east-1. Current ReadCapacityUnits reserved by the account: 40001. \
         Limit: 80000. Requested: 40000. Refer to the Amazon DynamoDB Developer Guide for \
         current limits and how to request higher limits."
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
    assert_eq!(
        refusal(check(40_001, 10, units(10, 10))),
        "The requested ReadCapacityUnits, 40001, is above the per table maximum for the \
         account in us-east-1. Per table maximum: 40000. Refer to the Amazon DynamoDB \
         Developer Guide for current limits and how to request higher limits."
    );
    // The account holds 70,000 including this table's 10: raising it to 10,010
    // fills the account to 80,000, and 10,011 takes it one over.
    check(10_010, 10, units(70_000, 10)).expect("at the account maximum");
    assert_eq!(
        refusal(check(10_011, 10, units(70_000, 10))),
        "This request would have caused the ReadCapacityUnits limit to be exceeded for the \
         account in us-east-1. Current ReadCapacityUnits reserved by the account: 70000. \
         Limit: 80000. Current ReadCapacityUnits provisioned for the table and indexes: 10. \
         Requested: 10011. Refer to the Amazon DynamoDB Developer Guide for current limits \
         and how to request higher limits."
    );
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
    // An on-demand table keeps the figures it held before the switch, and
    // holds none of them: a switch back to PROVISIONED at 300 raises it above
    // the per-table maximum. The account maximum admits the 300.
    let account_admits = LimitsConfig {
        per_account_max_rcu: 2_000,
        per_account_max_wcu: 2_000,
        ..limits
    };
    let on_demand = TableDescription {
        billing_mode_summary: Some(BillingModeSummary {
            billing_mode: BillingMode::PayPerRequest,
            last_update_to_pay_per_request_date_time: None,
        }),
        ..table(500, 500, Vec::new())
    };
    assert_eq!(
        refusal(check_update_table_capacity(
            Some(&request),
            &on_demand,
            units(1_000, 1_000),
            units(300, 300),
            &account_admits,
            REGION,
        )),
        "The requested ReadCapacityUnits, 300, is above the per table maximum for the account \
         in us-east-1. Per table maximum: 100. Refer to the Amazon DynamoDB Developer Guide for \
         current limits and how to request higher limits."
    );
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

/// What the service answered one row of [`CAPACITY_FIGURES`].
enum Answer {
    /// `Given value <n> for <unit> is out of bounds`.
    Bounds(&'static str, &'static str),
    /// The minimum of 1, which the service words as a member constraint and
    /// ExtendDB its own way. Both texts carry [`MINIMUM`].
    Minimum,
    Limit(&'static str),
    /// Passes these checks, with the table's write figure read as this.
    Passes(i64),
}

const MINIMUM: &str = "greater than or equal to 1";
const MAX: &str = "9223372036854775807";
const PAST_MAX: &str = "9223372036854775808";
const READ: &str = "ReadCapacityUnits";
const WRITE: &str = "WriteCapacityUnits";
const PER_INDEX: &str = "The requested WriteCapacityUnits for index by-label, \
    9223372036854775807, is above the per index maximum for the account in us-east-1. Per \
    table maximum: 200000. Refer to the Amazon DynamoDB Developer Guide for current limits and \
    how to request higher limits.";
const ACCOUNT: &str = "This request would have caused the WriteCapacityUnits limit to be \
    exceeded for the account in us-east-1. Current WriteCapacityUnits reserved by the account: \
    152. Limit: 200000. Requested: 1000000000006. Refer to the Amazon DynamoDB Developer Guide \
    for current limits and how to request higher limits.";

/// `UpdateTable` or `CreateTable`, the table's read and write figures as
/// JSON text, the write figure of an index `by-label` created beside the
/// table, and the service's answer. Measured in us-east-1 on 2026-11-14,
/// 2026-11-16 and 2026-11-18, on the account of [`measured_limits`], which
/// held 152 write units on 2026-11-18.
#[rustfmt::skip]
const CAPACITY_FIGURES: &[(bool, &str, &str, Option<&str>, Answer)] = {
    use Answer::{Bounds, Limit, Minimum, Passes};
    &[
        (true, "5", PAST_MAX, None, Bounds(MAX, WRITE)),
        (false, "5", PAST_MAX, None, Bounds(MAX, WRITE)),
        (false, "5", "5", Some(PAST_MAX), Limit(PER_INDEX)),
        (false, PAST_MAX, "5", None, Bounds(MAX, READ)),
        (false, "1000000000001", "5", None, Bounds("1000000000001", READ)),
        (false, PAST_MAX, "0", None, Minimum),
        (false, "5", "-9223372036854775809", None, Minimum),
        (false, "5", "5", Some("1000000000001"), Limit(ACCOUNT)),
        (true, "5", "1000000000000", None, Passes(1_000_000_000_000)),
        (true, "5", "1000000000001", None, Bounds("1000000000001", WRITE)),
        (false, "5", "5.5", None, Passes(5)),
    ]
};

/// The checks `CreateTable` runs on `body` in the engine's order, or the
/// check of the table's figures `UpdateTable` runs, and the table's write
/// figure as read.
fn capacity_checks(update: bool, body: serde_json::Value) -> Result<i64, DynamoDbError> {
    let unreadable = |e: serde_json::Error| DynamoDbError::SerializationException(e.to_string());
    if update {
        let input: UpdateTableInput = serde_json::from_value(body).map_err(unreadable)?;
        let throughput = input.provisioned_throughput.unwrap();
        check_table_capacity_bounds(&throughput)?;
        return Ok(throughput.write_capacity_units);
    }
    let input: CreateTableInput = serde_json::from_value(body).map_err(unreadable)?;
    let limits = measured_limits();
    crate::validation::validate_create_table(&input, &limits)?;
    let requested = create_table_capacity(&input).expect("a provisioned table");
    check_create_table_capacity(requested, units(0, 152), &limits, REGION)?;
    check_create_table_indexes(&input, &limits, REGION)?;
    Ok(input.provisioned_throughput.unwrap().write_capacity_units)
}

#[test]
fn a_capacity_figure_is_read_and_bounded_as_measured() {
    // Parsed the way the server parses a body, so a figure past the 64-bit
    // range reaches the reader in the same form.
    let figure = |text: &str| serde_json::from_str::<serde_json::Value>(text).unwrap();
    for (update, read, write, index_write, answer) in CAPACITY_FIGURES {
        let throughput =
            json!({ "ReadCapacityUnits": figure(read), "WriteCapacityUnits": figure(write) });
        let mut body = json!({ "TableName": "capacity", "ProvisionedThroughput": throughput });
        if !update {
            body["BillingMode"] = json!("PROVISIONED");
            body["KeySchema"] = json!([{ "AttributeName": "pk", "KeyType": "HASH" }]);
            body["AttributeDefinitions"] = json!([{ "AttributeName": "pk", "AttributeType": "S" }]);
        }
        if let Some(index_write) = index_write {
            let label = json!({ "AttributeName": "label", "AttributeType": "S" });
            body["AttributeDefinitions"]
                .as_array_mut()
                .unwrap()
                .push(label);
            body["GlobalSecondaryIndexes"] = json!([{
                "IndexName": "by-label",
                "KeySchema": [{ "AttributeName": "label", "KeyType": "HASH" }],
                "Projection": { "ProjectionType": "ALL" },
                "ProvisionedThroughput": { "ReadCapacityUnits": 5, "WriteCapacityUnits": figure(index_write) },
            }]);
        }
        let result = capacity_checks(*update, body.clone());
        match (answer, &result) {
            (Answer::Bounds(n, unit), Err(DynamoDbError::ValidationException(m)))
                if *m == format!("Given value {n} for {unit} is out of bounds") => {}
            (Answer::Minimum, Err(DynamoDbError::ValidationException(m)))
                if m.contains(MINIMUM) && !m.contains("out of bounds") => {}
            (Answer::Limit(text), Err(DynamoDbError::LimitExceededException(m))) if m == text => {}
            (Answer::Passes(n), Ok(write)) if write == n => {}
            _ => panic!("{body}: got {result:?}"),
        }
    }
}
