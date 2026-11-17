// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the measured `WarmThroughput` request rules. Each test names the
//! rows of the live run it reproduces.

use super::*;

fn spec(read: Option<i64>, write: Option<i64>) -> WarmThroughput {
    WarmThroughput {
        read_units_per_second: read,
        write_units_per_second: write,
    }
}

fn message(result: Result<(), DynamoDbError>) -> String {
    match result.expect_err("refused") {
        DynamoDbError::ValidationException(m) => m,
        other => panic!("{other:?}"),
    }
}

const TABLE: WarmThroughputSubject<'static> = WarmThroughputSubject::Table;
const GSI: WarmThroughputSubject<'static> = WarmThroughputSubject::Index("gsi");

const ON_DEMAND_READ_TABLE: &str = "One or more parameter values were invalid: Requested \
     ReadUnitsPerSecond for WarmThroughput for table is lower than initial throughput for \
     OnDemand. See: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/\
     on-demand-capacity-mode.html#on-demand-capacity-mode-initial";

fn five_five() -> ProvisionedThroughput {
    ProvisionedThroughput {
        read_capacity_units: 5,
        write_capacity_units: 5,
    }
}

/// Measured rows C2, C3 and C7: the whole strings.
#[test]
fn on_demand_create_below_the_minimum_reports_the_measured_sentence() {
    let read = message(validate_warm_throughput_on_create(
        &spec(Some(11_999), Some(4_000)),
        TABLE,
        BillingMode::PayPerRequest,
        None,
    ));
    assert_eq!(read, ON_DEMAND_READ_TABLE);
    let write = message(validate_warm_throughput_on_create(
        &spec(Some(12_000), Some(3_999)),
        TABLE,
        BillingMode::PayPerRequest,
        None,
    ));
    assert_eq!(
        write,
        ON_DEMAND_READ_TABLE.replace("ReadUnitsPerSecond", "WriteUnitsPerSecond")
    );
    // 0/0 reports the read side: read is checked first.
    let both = message(validate_warm_throughput_on_create(
        &spec(Some(0), Some(0)),
        TABLE,
        BillingMode::PayPerRequest,
        None,
    ));
    assert_eq!(both, read);
}

/// Rows C1 and C4: the minimum itself and a partial request are accepted.
#[test]
fn on_demand_create_at_or_above_the_minimum_is_accepted() {
    validate_warm_throughput_on_create(
        &spec(Some(12_000), Some(4_000)),
        TABLE,
        BillingMode::PayPerRequest,
        None,
    )
    .expect("the minimum");
    validate_warm_throughput_on_create(
        &spec(Some(20_000), None),
        TABLE,
        BillingMode::PayPerRequest,
        None,
    )
    .expect("read alone");
}

/// Rows C5, C6, P1 and P2: a provisioned 5/5 table refuses 1/1 (read form)
/// and 5/4 (write form), accepts 5/5 (equal) and 100/50.
#[test]
fn provisioned_create_below_the_units_reports_the_measured_sentence() {
    let units = five_five();
    let read = message(validate_warm_throughput_on_create(
        &spec(Some(1), Some(1)),
        TABLE,
        BillingMode::Provisioned,
        Some(&units),
    ));
    assert_eq!(
        read,
        "One or more parameter values were invalid: Requested ReadUnitsPerSecond for \
         WarmThroughput for table is lower than ReadCapacityUnits of ProvisionedThroughput"
    );
    let write = message(validate_warm_throughput_on_create(
        &spec(Some(5), Some(4)),
        TABLE,
        BillingMode::Provisioned,
        Some(&units),
    ));
    assert_eq!(
        write,
        "One or more parameter values were invalid: Requested WriteUnitsPerSecond for \
         WarmThroughput for table is lower than WriteCapacityUnits of ProvisionedThroughput"
    );
    validate_warm_throughput_on_create(
        &spec(Some(5), Some(5)),
        TABLE,
        BillingMode::Provisioned,
        Some(&units),
    )
    .expect("equal to the units");
    validate_warm_throughput_on_create(
        &spec(Some(100), Some(50)),
        TABLE,
        BillingMode::Provisioned,
        Some(&units),
    )
    .expect("above the units");
}

/// Rows U1 to U5, U7, UT1 and P3: equal and higher are accepted, lower is
/// refused with the decrease sentence on either billing mode and member.
#[test]
fn update_below_current_reports_the_decrease_sentence() {
    validate_warm_throughput_on_update(&spec(Some(12_000), Some(4_000)), TABLE, (12_000, 4_000))
        .expect("same values");
    validate_warm_throughput_on_update(&spec(Some(24_000), Some(8_000)), TABLE, (12_000, 4_000))
        .expect("higher");
    validate_warm_throughput_on_update(&spec(Some(30_000), None), TABLE, (12_000, 4_000))
        .expect("read alone");
    let read = message(validate_warm_throughput_on_update(
        &spec(Some(11_999), Some(4_000)),
        TABLE,
        (12_000, 4_000),
    ));
    assert_eq!(
        read,
        "One or more parameter values were invalid: Requested ReadUnitsPerSecond for \
         WarmThroughput for table is lower than current WarmThroughput, decreasing \
         WarmThroughput is not supported"
    );
    // U7: a provisioned table at 100/50 refuses 2/2 with the same sentence.
    assert_eq!(
        message(validate_warm_throughput_on_update(
            &spec(Some(2), Some(2)),
            TABLE,
            (100, 50)
        )),
        read
    );
    // UT1 and P3: the write form, whole.
    let write = message(validate_warm_throughput_on_update(
        &spec(Some(12_000), Some(3_999)),
        TABLE,
        (12_000, 4_000),
    ));
    assert_eq!(
        write,
        "One or more parameter values were invalid: Requested WriteUnitsPerSecond for \
         WarmThroughput for table is lower than current WarmThroughput, decreasing \
         WarmThroughput is not supported"
    );
    assert_eq!(
        message(validate_warm_throughput_on_update(
            &spec(Some(5), Some(4)),
            TABLE,
            (5, 5)
        )),
        write
    );
}

/// Rows G1, G2, G3, UG1 and UG2: the index form of every sentence, whole.
#[test]
fn index_sentences_name_the_index() {
    assert_eq!(
        message(validate_warm_throughput_on_create(
            &spec(Some(11_999), Some(4_000)),
            GSI,
            BillingMode::PayPerRequest,
            None,
        )),
        ON_DEMAND_READ_TABLE.replace("for table", "for index gsi")
    );
    assert_eq!(
        message(validate_warm_throughput_on_create(
            &spec(Some(12_000), Some(3_999)),
            GSI,
            BillingMode::PayPerRequest,
            None,
        )),
        "One or more parameter values were invalid: Requested WriteUnitsPerSecond for \
         WarmThroughput for index gsi is lower than initial throughput for OnDemand. See: \
         https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/\
         on-demand-capacity-mode.html#on-demand-capacity-mode-initial"
    );
    let units = five_five();
    assert_eq!(
        message(validate_warm_throughput_on_create(
            &spec(Some(1), Some(1)),
            GSI,
            BillingMode::Provisioned,
            Some(&units),
        )),
        "One or more parameter values were invalid: Requested ReadUnitsPerSecond for \
         WarmThroughput for index gsi is lower than ReadCapacityUnits of ProvisionedThroughput"
    );
    assert_eq!(
        message(validate_warm_throughput_on_update(
            &spec(Some(11_999), Some(4_000)),
            GSI,
            (12_000, 4_000),
        )),
        "One or more parameter values were invalid: Requested ReadUnitsPerSecond for \
         WarmThroughput for index gsi is lower than current WarmThroughput, decreasing \
         WarmThroughput is not supported"
    );
    assert_eq!(
        message(validate_warm_throughput_on_update(
            &spec(Some(12_000), Some(3_999)),
            GSI,
            (12_000, 4_000),
        )),
        "One or more parameter values were invalid: Requested WriteUnitsPerSecond for \
         WarmThroughput for index gsi is lower than current WarmThroughput, decreasing \
         WarmThroughput is not supported"
    );
}

#[test]
fn create_table_checks_the_table_then_each_index_by_name() {
    let input: CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": "t",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
        "WarmThroughput": {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000},
        "GlobalSecondaryIndexes": [{
            "IndexName": "by-g",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
            "WarmThroughput": {"ReadUnitsPerSecond": 1, "WriteUnitsPerSecond": 1}
        }]
    }))
    .expect("input");
    let msg = message(validate_create_table_warm_throughput(&input));
    assert_eq!(
        msg,
        ON_DEMAND_READ_TABLE.replace("for table", "for index by-g")
    );
    let mut ok = input.clone();
    ok.global_secondary_indexes = None;
    validate_create_table_warm_throughput(&ok).expect("table alone");
}

/// The per-companion rules, as measured (service-pairs run and the review
/// rounds of 2026-10-23): a table-level WarmThroughput alone is accepted;
/// SSE, TableClass and DeletionProtection companions answer their own
/// sentence, in that order; OnDemandThroughput answers its own, with or
/// without BillingMode; an index Delete answers its own; Streams, billing mode
/// and provisioned throughput answer the WarmThroughput sentence; an index
/// Update companion is accepted.
#[test]
fn a_table_warm_throughput_must_be_the_only_operation() {
    let input = |extra: serde_json::Value| -> UpdateTableInput {
        let mut v = serde_json::json!({
            "TableName": "t",
            "WarmThroughput": {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000}
        });
        for (k, val) in extra.as_object().expect("object") {
            v[k] = val.clone();
        }
        serde_json::from_value(v).expect("input")
    };
    let gsi_update = serde_json::json!({"GlobalSecondaryIndexUpdates": [{"Update": {
        "IndexName": "gsi", "WarmThroughput": {"ReadUnitsPerSecond": 12000}}}]});
    validate_warm_throughput_is_only_operation(&input(serde_json::json!({}))).expect("alone");
    validate_warm_throughput_is_only_operation(&input(serde_json::json!({
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    })))
    .expect("attribute definitions are not an operation");
    validate_warm_throughput_is_only_operation(&input(gsi_update.clone()))
        .expect("an index update beside a table WarmThroughput is accepted");

    let sse = serde_json::json!({"Enabled": true});
    let stream_off = serde_json::json!({"StreamEnabled": false});
    let stream_on = serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"});
    let delete = serde_json::json!([{"Delete": {"IndexName": "gsi"}}]);
    let cases = [
        // S1a, S1b, S1c: SSE outranks TableClass, DeletionProtection and Streams.
        (
            serde_json::json!({"SSESpecification": sse}),
            SSE_MUST_BE_ONLY_OPERATION,
        ),
        (
            serde_json::json!({"SSESpecification": sse, "TableClass": "STANDARD"}),
            SSE_MUST_BE_ONLY_OPERATION,
        ),
        (
            serde_json::json!({"SSESpecification": sse, "DeletionProtectionEnabled": false}),
            SSE_MUST_BE_ONLY_OPERATION,
        ),
        (
            serde_json::json!({"SSESpecification": sse, "StreamSpecification": stream_on}),
            SSE_MUST_BE_ONLY_OPERATION,
        ),
        (
            serde_json::json!({"TableClass": "STANDARD"}),
            TABLE_CLASS_MUST_BE_ONLY_OPERATION,
        ),
        (
            serde_json::json!({"DeletionProtectionEnabled": true}),
            DELETION_PROTECTION_MUST_BE_ONLY_OPERATION,
        ),
        // T4d: OnDemandThroughput without BillingMode.
        (
            serde_json::json!({"OnDemandThroughput": {"MaxReadRequestUnits": 10}}),
            ON_DEMAND_THROUGHPUT_NEEDS_BILLING_MODE,
        ),
        // T4a: with BillingMode the sentence is the same (the service names
        // BillingMode in it, and still refuses).
        (
            serde_json::json!({"BillingMode": "PAY_PER_REQUEST",
                "OnDemandThroughput": {"MaxReadRequestUnits": 10}}),
            ON_DEMAND_THROUGHPUT_NEEDS_BILLING_MODE,
        ),
        // T4b, T4c: TableClass and DeletionProtection outrank OnDemandThroughput.
        (
            serde_json::json!({"TableClass": "STANDARD",
                "OnDemandThroughput": {"MaxReadRequestUnits": 10}}),
            TABLE_CLASS_MUST_BE_ONLY_OPERATION,
        ),
        (
            serde_json::json!({"DeletionProtectionEnabled": true,
                "OnDemandThroughput": {"MaxReadRequestUnits": 10}}),
            DELETION_PROTECTION_MUST_BE_ONLY_OPERATION,
        ),
        // S9a: an index Delete; V3b: TableClass outranks it; V3a: with Streams
        // the service names the stream change.
        (
            serde_json::json!({"GlobalSecondaryIndexUpdates": delete}),
            DELETE_GSI_WITH_WARM_THROUGHPUT,
        ),
        (
            serde_json::json!({"GlobalSecondaryIndexUpdates": delete, "TableClass": "STANDARD"}),
            TABLE_CLASS_MUST_BE_ONLY_OPERATION,
        ),
        (
            serde_json::json!({"GlobalSecondaryIndexUpdates": delete,
                "StreamSpecification": stream_on}),
            INDEX_CHANGE_WITH_STREAM_CHANGE,
        ),
        (
            serde_json::json!({"StreamSpecification": stream_off}),
            WARM_THROUGHPUT_MUST_BE_ONLY_OPERATION,
        ),
        (
            serde_json::json!({"BillingMode": "PROVISIONED",
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}}),
            WARM_THROUGHPUT_MUST_BE_ONLY_OPERATION,
        ),
        // V2: ProvisionedThroughput without BillingMode.
        (
            serde_json::json!({"ProvisionedThroughput":
                {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}}),
            WARM_THROUGHPUT_MUST_BE_ONLY_OPERATION,
        ),
        // S1d: DeletionProtection outranks Streams.
        (
            serde_json::json!({"DeletionProtectionEnabled": true,
                "StreamSpecification": stream_off}),
            DELETION_PROTECTION_MUST_BE_ONLY_OPERATION,
        ),
        // V4: the triple answers the TableClass sentence.
        (
            serde_json::json!({"TableClass": "STANDARD", "DeletionProtectionEnabled": true,
                "StreamSpecification": stream_on}),
            TABLE_CLASS_MUST_BE_ONLY_OPERATION,
        ),
    ];
    for (extra, expected) in cases {
        assert_eq!(
            message(validate_warm_throughput_is_only_operation(&input(
                extra.clone()
            ))),
            expected,
            "{extra}"
        );
    }
    // No table-level member: this function refuses nothing, whatever else is
    // there (the companions' own rules are out of scope here).
    let no_warm: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": "t",
        "DeletionProtectionEnabled": true,
        "TableClass": "STANDARD",
        "SSESpecification": sse,
        "StreamSpecification": {"StreamEnabled": false},
        "GlobalSecondaryIndexUpdates": [{"Update": {
            "IndexName": "gsi", "WarmThroughput": {"ReadUnitsPerSecond": 12000}}}]
    }))
    .expect("input");
    validate_warm_throughput_is_only_operation(&no_warm).expect("no table-level member");
}

/// The sentences, whole (rows S1a, S9a, V3a).
#[test]
fn the_companion_sentences_are_the_measured_strings() {
    assert_eq!(
        SSE_MUST_BE_ONLY_OPERATION,
        "One or more parameter values were invalid: Server-Side Encryption modification must \
         be the only operation in the request"
    );
    assert_eq!(
        DELETE_GSI_WITH_WARM_THROUGHPUT,
        "One or more parameter values were invalid: Delete global secondary index cannot be \
         specified when updating WarmThroughput"
    );
    assert_eq!(
        INDEX_CHANGE_WITH_STREAM_CHANGE,
        "One or more parameter values were invalid: You cannot create or delete index while \
         changing stream status"
    );
}

/// Rows S10, T5a, T5c (missing table): a bad value alone, and an index
/// `Update` alone, are not refused here, so the request reaches the table
/// lookup and the service's `ResourceNotFoundException`. Rows S10b, T5b, T5d:
/// with a companion this function answers first, before the table is read.
#[test]
fn a_missing_table_reaches_the_lookup_unless_a_companion_answers_first() {
    let input = |v: serde_json::Value| -> UpdateTableInput {
        let mut v = v;
        v["TableName"] = serde_json::json!("missing");
        serde_json::from_value(v).expect("input")
    };
    // S10, T5a: a value below any default, nothing else.
    validate_warm_throughput_is_only_operation(&input(serde_json::json!({
        "WarmThroughput": {"ReadUnitsPerSecond": 11999, "WriteUnitsPerSecond": 4000}
    })))
    .expect("S10: reaches the lookup");
    validate_warm_throughput_is_only_operation(&input(serde_json::json!({
        "WarmThroughput": {"ReadUnitsPerSecond": 1}
    })))
    .expect("T5a: reaches the lookup");
    // T5c: an index Update alone.
    validate_warm_throughput_is_only_operation(&input(serde_json::json!({
        "GlobalSecondaryIndexUpdates": [{"Update": {
            "IndexName": "gsi", "WarmThroughput": {"ReadUnitsPerSecond": 12000}}}]
    })))
    .expect("T5c: reaches the lookup");
    let with = |extra: serde_json::Value| {
        let mut v = serde_json::json!({"WarmThroughput": {"ReadUnitsPerSecond": 12000}});
        v[extra
            .as_object()
            .expect("object")
            .keys()
            .next()
            .expect("key")
            .as_str()] = extra
            .as_object()
            .expect("object")
            .values()
            .next()
            .expect("value")
            .clone();
        message(validate_warm_throughput_is_only_operation(&input(v)))
    };
    assert_eq!(
        with(serde_json::json!({"TableClass": "STANDARD_INFREQUENT_ACCESS"})),
        TABLE_CLASS_MUST_BE_ONLY_OPERATION,
        "S10b"
    );
    assert_eq!(
        with(serde_json::json!({"StreamSpecification":
            {"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}})),
        WARM_THROUGHPUT_MUST_BE_ONLY_OPERATION,
        "T5b"
    );
    assert_eq!(
        with(serde_json::json!({"DeletionProtectionEnabled": false})),
        DELETION_PROTECTION_MUST_BE_ONLY_OPERATION,
        "T5d"
    );
}
