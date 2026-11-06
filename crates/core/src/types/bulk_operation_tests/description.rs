// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Serialization of the bulk operation description, usage, summary and outputs.

use super::*;

#[test]
fn description_start_time_serializes_as_a_json_number() {
    let mut description = minimal_description();
    description.start_time = Some(1_789_060_589.767);
    description.end_time = Some(1_789_060_650.634);
    let value = serde_json::to_value(&description).unwrap();
    assert!(
        value["StartTime"].is_number(),
        "StartTime must be a JSON number, got: {}",
        value["StartTime"]
    );
    assert_eq!(value["StartTime"], json!(1_789_060_589.767));
    assert!(
        value["EndTime"].is_number(),
        "EndTime must be a JSON number, got: {}",
        value["EndTime"]
    );
    assert_eq!(value["EndTime"], json!(1_789_060_650.634));
}

#[test]
fn description_with_only_required_fields_serializes_the_three_always_present_keys() {
    let value = serde_json::to_value(minimal_description()).unwrap();
    let object = value.as_object().unwrap();
    assert_eq!(
        object.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["BulkOperationArn", "BulkOperationStatus", "TableDetails"],
        "absent optional members must be omitted, not serialized as null"
    );
    assert_eq!(value["BulkOperationArn"], json!(ARN));
    assert_eq!(value["BulkOperationStatus"], json!("IN_PROGRESS"));
}

#[test]
fn fully_populated_description_uses_the_model_wire_names() {
    let value = serde_json::to_value(full_description()).unwrap();
    let object = value.as_object().unwrap();
    for key in [
        "BulkOperationArn",
        "BulkOperationStatus",
        "TableDetails",
        "TableArn",
        "TableId",
        "ClientToken",
        "InputS3Configuration",
        "OutputS3Configuration",
        "StartTime",
        "EndTime",
        "ProcessedItemCount",
        "FailedItemCount",
        "ProcessedInputBytes",
        "TotalInputBytes",
        "BulkOperationUsage",
        "FailureCode",
        "FailureMessage",
        "MaxWriteRequestUnits",
        "PerformanceTier",
    ] {
        assert!(object.contains_key(key), "missing wire member {key}");
    }
    assert!(value["BulkOperationUsage"].is_array());
    assert_eq!(value["PerformanceTier"], json!("STANDARD"));
}

#[test]
fn table_details_carries_one_entry_of_the_table_name_and_id() {
    // Measured on the service: every description carries
    // TableDetails as a one-entry list of the bare table name and the
    // table id.
    let value = serde_json::to_value(full_description()).unwrap();
    assert_eq!(
        value["TableDetails"],
        json!([{
            "TableName": "orders",
            "TableId": "0ec72c3d-0000-0000-0000-000000000000",
        }])
    );
}

#[test]
fn input_configuration_round_trips_through_the_wire_names() {
    let value = serde_json::to_value(input_configuration()).unwrap();
    assert_eq!(value, full_start_body()["InputS3Configuration"]);
}

#[test]
fn output_configuration_round_trips_through_the_wire_names() {
    let value = serde_json::to_value(output_configuration()).unwrap();
    assert_eq!(value, full_start_body()["OutputS3Configuration"]);
}

#[test]
fn output_configuration_absent_optionals_are_omitted() {
    let config: OutputS3Configuration =
        serde_json::from_value(json!({ "OutputType": ["ERROR"] })).unwrap();
    let value = serde_json::to_value(&config).unwrap();
    assert_eq!(
        value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["OutputType"],
        "absent optional members must be omitted, not serialized as null"
    );
}

#[test]
fn usage_table_member_is_an_object_carrying_bulk_put_bytes() {
    let value = serde_json::to_value(usage()).unwrap();
    assert!(
        value["Table"].is_object(),
        "Table must be a nested object, not a scalar, got: {}",
        value["Table"]
    );
    assert_eq!(value["Table"], json!({ "BulkPutBytes": 25600 }));
}

#[test]
fn usage_gsi_map_values_are_objects_carrying_bulk_put_bytes() {
    let value = serde_json::to_value(usage()).unwrap();
    assert!(
        value["GlobalSecondaryIndexes"]["gsi1"].is_object(),
        "index map values must be nested objects, not scalars, got: {}",
        value["GlobalSecondaryIndexes"]["gsi1"]
    );
    assert_eq!(
        value["GlobalSecondaryIndexes"],
        json!({ "gsi1": { "BulkPutBytes": 4096 } })
    );
    assert!(
        value.get("LocalSecondaryIndexes").is_none(),
        "an empty index map is omitted, not serialized as {{}}"
    );
}

#[test]
fn usage_vector_map_values_are_objects_carrying_bulk_put_vector_bytes() {
    let value = serde_json::to_value(usage()).unwrap();
    assert!(
        value["VectorIndexes"]["vi1"].is_object(),
        "vector map values must be nested objects, not scalars, got: {}",
        value["VectorIndexes"]["vi1"]
    );
    assert_eq!(
        value["VectorIndexes"],
        json!({ "vi1": { "BulkPutVectorBytes": 8192 } })
    );
}

#[test]
fn usage_serializes_the_top_level_table_name_and_bulk_put_bytes() {
    let value = serde_json::to_value(usage()).unwrap();
    assert_eq!(value["TableName"], json!("orders"));
    assert_eq!(value["BulkPutBytes"], json!(25600));
}

#[test]
fn summary_without_end_time_serializes_three_keys() {
    let summary = BulkOperationSummary {
        bulk_operation_arn: ARN.to_owned(),
        bulk_operation_status: BulkOperationStatus::InProgress,
        start_time: 1_789_060_589.767,
        end_time: None,
    };
    let value = serde_json::to_value(&summary).unwrap();
    assert_eq!(
        value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["BulkOperationArn", "BulkOperationStatus", "StartTime"]
    );
    assert!(value["StartTime"].is_number());
}

#[test]
fn summary_with_end_time_serializes_it_as_a_json_number() {
    let summary = BulkOperationSummary {
        bulk_operation_arn: ARN.to_owned(),
        bulk_operation_status: BulkOperationStatus::Completed,
        start_time: 1_789_060_589.767,
        end_time: Some(1_789_060_650.634),
    };
    let value = serde_json::to_value(&summary).unwrap();
    assert_eq!(value["EndTime"], json!(1_789_060_650.634));
}

#[test]
fn list_output_omits_next_token_when_absent() {
    let output = ListBulkOperationsOutput {
        bulk_operations_summary_list: vec![],
        next_token: None,
    };
    let value = serde_json::to_value(&output).unwrap();
    assert_eq!(
        value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["BulkOperationsSummaryList"]
    );
    assert_eq!(value["BulkOperationsSummaryList"], json!([]));
}

#[test]
fn cancel_output_serializes_exactly_two_keys() {
    let output = CancelBulkOperationOutput {
        bulk_operation_arn: ARN.to_owned(),
        bulk_operation_status: BulkOperationStatus::Cancelling,
    };
    let value = serde_json::to_value(&output).unwrap();
    assert_eq!(
        value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["BulkOperationArn", "BulkOperationStatus"]
    );
    assert_eq!(value["BulkOperationStatus"], json!("CANCELLING"));
}

#[test]
fn start_output_wraps_the_description() {
    let output = StartBulkPutItemsOutput {
        bulk_operation_description: minimal_description(),
    };
    let value = serde_json::to_value(&output).unwrap();
    assert_eq!(
        value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["BulkOperationDescription"]
    );
}

#[test]
fn describe_output_wraps_the_description() {
    let output = DescribeBulkOperationOutput {
        bulk_operation_description: minimal_description(),
    };
    let value = serde_json::to_value(&output).unwrap();
    assert_eq!(
        value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["BulkOperationDescription"]
    );
}

#[test]
fn usage_lsi_map_values_are_objects_carrying_bulk_put_bytes() {
    let mut with_lsi = usage();
    with_lsi.local_secondary_indexes.insert(
        "lsi1".to_owned(),
        BulkOperationBytes {
            bulk_put_bytes: 2048,
        },
    );
    let value = serde_json::to_value(with_lsi).unwrap();
    assert!(
        value["LocalSecondaryIndexes"]["lsi1"].is_object(),
        "index map values must be nested objects, not scalars, got: {}",
        value["LocalSecondaryIndexes"]["lsi1"]
    );
    assert_eq!(
        value["LocalSecondaryIndexes"],
        json!({ "lsi1": { "BulkPutBytes": 2048 } })
    );
}
