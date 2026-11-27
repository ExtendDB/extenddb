// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the `UpdateTable` request checks.

use super::*;
use extenddb_storage::error::StorageError;

/// Measured 2026-10-24: a stream change beside a billing or throughput
/// change, beside an index create, or beside a deletion protection change is
/// refused with its own sentence; alone it is not. Measured 2026-11-11:
/// beside an index delete it answers the index sentence, beside an index
/// Update carrying WarmThroughput the WarmThroughput sentence, and beside an
/// index Update of ProvisionedThroughput it is not refused. A Create beside an
/// Update (not measured) answers the index sentence: any Create or Delete
/// decides it.
#[test]
fn a_stream_change_refuses_its_companions() {
    let with = |extra: &Value| -> UpdateTableInput {
        let mut body = serde_json::json!({
            "TableName": "t",
            "StreamSpecification": {"StreamEnabled": false},
        });
        for (k, v) in extra.as_object().unwrap() {
            body[k] = v.clone();
        }
        serde_json::from_value(body).unwrap()
    };
    let iops = "One or more parameter values were invalid: You cannot modify stream status while updating table IOPS";
    let index = "One or more parameter values were invalid: You cannot create or delete index while changing stream status";
    let warm = "One or more parameter values were invalid: WarmThroughput must be the only operation in the request";
    let protection = "One or more parameter values were invalid: DeletionProtection modification must be the only operation in the request";
    let units = serde_json::json!({"ReadCapacityUnits": 5, "WriteCapacityUnits": 5});
    let create = serde_json::json!({"Create": {
        "IndexName": "g",
        "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
        "Projection": {"ProjectionType": "ALL"}
    }});
    let gsi = |update: Value| serde_json::json!({"GlobalSecondaryIndexUpdates": [update]});
    for (extra, expected) in [
        (
            serde_json::json!({"BillingMode": "PROVISIONED"}),
            Some(iops),
        ),
        (
            serde_json::json!({"ProvisionedThroughput": units}),
            Some(iops),
        ),
        (
            gsi(serde_json::json!({"Delete": {"IndexName": "g"}})),
            Some(index),
        ),
        (gsi(create.clone()), Some(index)),
        (
            serde_json::json!({"GlobalSecondaryIndexUpdates": [
                create,
                {"Update": {"IndexName": "h", "ProvisionedThroughput": units}}
            ]}),
            Some(index),
        ),
        (
            gsi(serde_json::json!({"Update": {"IndexName": "g", "ProvisionedThroughput": units}})),
            None,
        ),
        (
            gsi(serde_json::json!({"Update": {
                "IndexName": "g",
                "WarmThroughput": {"ReadUnitsPerSecond": 5, "WriteUnitsPerSecond": 5}
            }})),
            Some(warm),
        ),
        (
            serde_json::json!({"DeletionProtectionEnabled": true}),
            Some(protection),
        ),
        (serde_json::json!({}), None),
    ] {
        assert_eq!(stream_change_companion(&with(&extra)), expected, "{extra}");
    }
    let no_stream: UpdateTableInput =
        serde_json::from_value(serde_json::json!({"TableName": "t", "BillingMode": "PROVISIONED"}))
            .unwrap();
    assert_eq!(stream_change_companion(&no_stream), None);
}

#[test]
fn projected_total_counts_kept_indexes_and_created_ones() {
    use extenddb_core::types::{
        GlobalSecondaryIndexUpdate, GsiDescription, LsiDescription, Projection, ProjectionType,
        TableDescription,
    };
    let include = |n: usize| Projection {
        projection_type: ProjectionType::Include,
        non_key_attributes: Some((0..n).map(|i| format!("a{i}")).collect()),
    };
    let existing = TableDescription {
        global_secondary_indexes: Some(vec![
            GsiDescription {
                index_name: "kept".to_owned(),
                key_schema: Vec::new(),
                projection: include(30),
                index_status: "ACTIVE".to_owned(),
                provisioned_throughput: None,
                index_size_bytes: 0,
                item_count: 0,
                index_arn: String::new(),
                warm_throughput: None,
                warm_throughput_record: None,
            },
            GsiDescription {
                index_name: "dropped".to_owned(),
                key_schema: Vec::new(),
                projection: include(50),
                index_status: "ACTIVE".to_owned(),
                provisioned_throughput: None,
                index_size_bytes: 0,
                item_count: 0,
                index_arn: String::new(),
                warm_throughput: None,
                warm_throughput_record: None,
            },
        ]),
        local_secondary_indexes: Some(vec![LsiDescription {
            index_name: "lsi".to_owned(),
            key_schema: Vec::new(),
            projection: include(20),
            index_size_bytes: 0,
            item_count: 0,
            index_arn: String::new(),
        }]),
        ..Default::default()
    };
    let create = |n: usize| -> GlobalSecondaryIndexUpdate {
        serde_json::from_value(serde_json::json!({ "Create": {
            "IndexName": "new", "KeySchema": [],
            "Projection": serde_json::to_value(include(n)).unwrap(),
        }}))
        .unwrap()
    };
    let delete: GlobalSecondaryIndexUpdate =
        serde_json::from_value(serde_json::json!({ "Delete": { "IndexName": "dropped" } }))
            .unwrap();

    assert_eq!(projected_after_gsi_updates(&existing, &[create(1)]), 101);
    assert_eq!(
        projected_after_gsi_updates(&existing, &[create(1), delete]),
        51
    );
    assert!(
        extenddb_core::validation::validate_projected_attribute_total(projected_after_gsi_updates(
            &existing,
            &[create(1)]
        ))
        .is_err()
    );
}

/// A backend that cannot serve the request reports a 400, not a 500.
///
/// This arm was missing while CreateTable had it, so the same refusal
/// answered differently depending on which operation carried it. It matters
/// for a backend whose vector capability is decided at runtime rather than at
/// compile time: its refusal arrives through this path.
#[test]
fn an_unsupported_feature_is_a_validation_exception() {
    let err = update_table_err_to_dynamo(
        StorageError::Unsupported("Vector indexes are not supported here".to_owned()),
        "t",
    );
    match err {
        DynamoDbError::ValidationException(msg) => {
            assert_eq!(msg, "Vector indexes are not supported here");
        }
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

/// The backend's whole message survives, unwrapped and unprefixed, because
/// the service's own wording for this case names both the table and the index
/// and no layer above the backend knows either.
#[test]
fn a_resource_in_use_refusal_keeps_the_backend_message() {
    let measured = extenddb_core::types::vector_index_delete_in_allocation_phase("t", "vidx");
    let err = update_table_err_to_dynamo(StorageError::IndexesInUse(measured.clone()), "t");
    match err {
        DynamoDbError::ResourceInUseException(msg) => assert_eq!(msg, measured),
        other => panic!("expected ResourceInUseException, got {other:?}"),
    }
}

/// The measured whole string, byte for byte, from probe P2 on 2026-08-19.
#[test]
fn the_allocation_phase_refusal_is_the_measured_whole_string() {
    assert_eq!(
        extenddb_core::types::vector_index_delete_in_allocation_phase(
            "eddbprobe-backfill",
            "vidx2"
        ),
        "Attempt to change a resource which is still in use: Index creation is in resource \
         allocation phase. Retry deletion during backfilling phase or when the index is \
         active. Table: eddbprobe-backfill Index: vidx2"
    );
}

/// A genuine fault still reports a 500 and is still logged as one.
#[test]
fn an_internal_failure_is_still_an_internal_server_error() {
    let err = update_table_err_to_dynamo(StorageError::Internal("disk on fire".to_owned()), "t");
    assert!(matches!(err, DynamoDbError::InternalServerError(_)));
}

/// Measured 2026-10-24: a `StreamSpecification` object without `StreamEnabled`
/// gets the null-member sentence. A member that is not an object (a string, a
/// number, an array) is not that case: it is left to deserialisation, which
/// refuses it as a `SerializationException`, as before the member check.
#[test]
fn a_stream_specification_that_is_not_an_object_is_a_serialization_error() {
    let null_member = "1 validation error detected: Value null at 'streamSpecification.streamEnabled' failed to satisfy constraint: Member must not be null";
    for spec in [
        serde_json::json!({}),
        serde_json::json!({"StreamViewType": "KEYS_ONLY"}),
        serde_json::json!({"StreamEnabled": null}),
    ] {
        let body = serde_json::json!({"TableName": "t", "StreamSpecification": spec});
        match crate::require_stream_enabled_member(&body) {
            Err(DynamoDbError::ValidationException(m)) => assert_eq!(m, null_member),
            other => panic!("{other:?}"),
        }
    }
    for spec in [
        serde_json::json!("x"),
        serde_json::json!(1),
        serde_json::json!([]),
        serde_json::json!(true),
    ] {
        let body = serde_json::json!({"TableName": "t", "StreamSpecification": spec});
        crate::require_stream_enabled_member(&body).expect("not the member check's case");
        let err = serde_json::from_value::<UpdateTableInput>(body)
            .map_err(crate::deserialize_error)
            .expect_err("a non-object specification does not deserialise");
        assert!(
            matches!(err, DynamoDbError::SerializationException(_)),
            "{err:?}"
        );
    }
    let body = serde_json::json!({"TableName": "t", "StreamSpecification": null});
    crate::require_stream_enabled_member(&body).expect("an absent member");
}
