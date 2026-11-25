// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::{
    NO_RESTORE_TIME, SOURCE_NAMED_TWICE, TABLE_NAME_REQUIRED, TWO_RESTORE_TIMES,
    check_restore_time_members, default_continuous_backups_description,
    pitr_restore_unavailable_error, pitr_unsupported_error, point_in_time_recovery_enabled,
    refuse_before_authorization, restore_source_table_name, table_name_member,
};
use super::{check_source_table_name, check_table_name};
use extenddb_core::error::DynamoDbError;
use serde_json::json;

const P: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/";

/// The message of every point-in-time recovery refusal.
const REFUSAL: &str = "Point-in-time recovery is not supported by this storage backend";

/// The model's table name length bounds, the deployment default.
const BOUNDS: (usize, usize) = (3, 255);

fn validation_message(result: Result<impl std::fmt::Debug, DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(m)) => m,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

/// The description reports continuous backups ENABLED (the service reports
/// this unconditionally) with point-in-time recovery DISABLED, and omits
/// both restorable-time fields on the wire: a disabled recovery has no
/// window, so serializing the members as null would diverge from the
/// service, which drops them entirely.
#[test]
fn the_continuous_backups_description_is_enabled_with_recovery_disabled() {
    let desc = default_continuous_backups_description();
    let wire = serde_json::to_value(&desc).expect("serializes");
    assert_eq!(wire["ContinuousBackupsStatus"], "ENABLED");
    let pitr = &wire["PointInTimeRecoveryDescription"];
    assert_eq!(pitr["PointInTimeRecoveryStatus"], "DISABLED");
    assert_eq!(
        pitr.as_object().map(serde_json::Map::len),
        Some(1),
        "a disabled recovery reports its status and nothing else: {pitr}"
    );
}

/// The enable refusal is the typed exception the service models on
/// `UpdateContinuousBackups`, on HTTP 400, under the standard `DynamoDB`
/// wire prefix, with a message naming the reason.
#[test]
fn the_enable_refusal_is_the_typed_continuous_backups_exception() {
    let err = pitr_unsupported_error();
    assert_eq!(err.status_code(), 400);
    assert_eq!(
        err.full_error_type(),
        "com.amazonaws.dynamodb.v20120810#ContinuousBackupsUnavailableException"
    );
    assert_eq!(err.message(), REFUSAL);
}

/// The restore refusal is the typed exception the service models on
/// `RestoreTableToPointInTime`, distinct from the enable refusal, with the
/// message of every point-in-time recovery refusal.
#[test]
fn the_restore_refusal_is_the_typed_point_in_time_recovery_exception() {
    let err = pitr_restore_unavailable_error();
    assert_eq!(err.status_code(), 400);
    assert_eq!(
        err.full_error_type(),
        "com.amazonaws.dynamodb.v20120810#PointInTimeRecoveryUnavailableException"
    );
    assert_eq!(err.message(), REFUSAL);
}

/// Before authorization, every restore and every enable is refused, whatever
/// the request names; a disable, a malformed specification and every other
/// operation go on to authorization.
#[test]
fn a_restore_and_an_enable_are_refused_before_authorization() {
    let restores = [
        json!({}),
        json!({"SourceTableName": "Music", "TargetTableName": "Copy", "UseLatestRestorableTime": true}),
        json!({"SourceTableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/Music/index/g",
               "TargetTableName": "Copy"}),
    ];
    for body in &restores {
        let err = refuse_before_authorization("RestoreTableToPointInTime", body).expect("refused");
        assert_eq!(
            err.full_error_type(),
            "com.amazonaws.dynamodb.v20120810#PointInTimeRecoveryUnavailableException",
            "{body}"
        );
        assert_eq!(err.message(), REFUSAL, "{body}");
    }
    for flag in [json!(true), json!("true")] {
        let body = json!({"TableName": "Music",
                          "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": flag}});
        let err = refuse_before_authorization("UpdateContinuousBackups", &body).expect("refused");
        assert_eq!(
            err.full_error_type(),
            "com.amazonaws.dynamodb.v20120810#ContinuousBackupsUnavailableException",
            "{body}"
        );
        assert_eq!(err.message(), REFUSAL, "{body}");
    }
    let passed = [
        (
            "UpdateContinuousBackups",
            json!({"TableName": "Music",
            "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": false}}),
        ),
        ("UpdateContinuousBackups", json!({"TableName": "Music"})),
        (
            "UpdateContinuousBackups",
            json!({"TableName": "Music",
            "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": 1}}),
        ),
        (
            "UpdateContinuousBackups",
            json!({"TableName": "Music",
            "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": "TRUE"}}),
        ),
        ("DescribeContinuousBackups", json!({"TableName": "Music"})),
        (
            "ExportTableToPointInTime",
            json!({"TableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/Music"}),
        ),
        ("RestoreTableFromBackup", json!({"TargetTableName": "Copy"})),
    ];
    for (operation, body) in &passed {
        assert!(
            refuse_before_authorization(operation, body).is_none(),
            "{operation} {body}"
        );
    }
}

/// A missing table name member is the service's routing text, not the model
/// validation text other backup operations give.
#[test]
fn a_missing_table_name_is_the_services_required_parameter_text() {
    assert_eq!(
        TABLE_NAME_REQUIRED,
        "The parameter 'TableName' is required but was not present in the request"
    );
    for body in [
        json!({}),
        json!({ "TableName": null }),
        json!({ "TableName": 7 }),
    ] {
        assert_eq!(
            validation_message(table_name_member(&body, "TableName")),
            TABLE_NAME_REQUIRED,
            "{body}"
        );
    }
    assert_eq!(
        table_name_member(&json!({ "TableName": "Music" }), "TableName").unwrap(),
        "Music"
    );
}

/// The specification and its flag are required; the service names the
/// missing one by its model path.
#[test]
fn the_recovery_specification_and_its_flag_are_required() {
    for body in [
        json!({ "TableName": "t" }),
        json!({ "TableName": "t", "PointInTimeRecoverySpecification": null }),
    ] {
        assert_eq!(
            validation_message(point_in_time_recovery_enabled(&body)),
            "1 validation error detected: Value null at 'pointInTimeRecoverySpecification' \
             failed to satisfy constraint: Member must not be null",
            "{body}"
        );
    }
    for spec in [json!({}), json!({ "PointInTimeRecoveryEnabled": null })] {
        let body = json!({ "TableName": "t", "PointInTimeRecoverySpecification": spec });
        assert_eq!(
            validation_message(point_in_time_recovery_enabled(&body)),
            "1 validation error detected: Value null at \
             'pointInTimeRecoverySpecification.pointInTimeRecoveryEnabled' \
             failed to satisfy constraint: Member must not be null",
            "{body}"
        );
    }
    for flag in [true, false] {
        let body = json!({
            "TableName": "t",
            "PointInTimeRecoverySpecification": { "PointInTimeRecoveryEnabled": flag },
        });
        assert_eq!(point_in_time_recovery_enabled(&body).unwrap(), flag);
    }
}

/// The flag given as the JSON string `"true"` or `"false"` is taken as the
/// boolean: the service answers 200 for `"false"` (M335 probe 54). Any other
/// value that is not a boolean is refused as a missing flag.
#[test]
fn the_recovery_flag_as_a_true_or_false_string_is_the_boolean() {
    for (flag, expected) in [("true", true), ("false", false)] {
        let body = json!({
            "TableName": "t",
            "PointInTimeRecoverySpecification": { "PointInTimeRecoveryEnabled": flag },
        });
        assert_eq!(
            point_in_time_recovery_enabled(&body).unwrap(),
            expected,
            "{flag}"
        );
    }
    for flag in [
        json!("TRUE"),
        json!("yes"),
        json!("FALSE"),
        json!("no"),
        json!(""),
        json!(1),
        json!([true]),
    ] {
        let body = json!({
            "TableName": "t",
            "PointInTimeRecoverySpecification": { "PointInTimeRecoveryEnabled": flag },
        });
        assert_eq!(
            validation_message(point_in_time_recovery_enabled(&body)),
            "1 validation error detected: Value null at \
             'pointInTimeRecoverySpecification.pointInTimeRecoveryEnabled' \
             failed to satisfy constraint: Member must not be null",
            "{body}"
        );
    }
}

/// `TableName` on the two continuous-backups operations takes the model's
/// constraints before the table is looked up: an empty name fails both, in
/// the service's text (M335 probes 56 and 57). A name that starts with
/// `arn:` is left to the lookup.
#[test]
fn the_table_name_takes_the_models_constraints_before_the_lookup() {
    assert_eq!(
        validation_message(check_table_name("", BOUNDS)),
        "2 validation errors detected: Value '' at 'tableName' failed to satisfy \
         constraint: Member must have length greater than or equal to 3; Value '' at \
         'tableName' failed to satisfy constraint: Member must satisfy regular expression \
         pattern: [a-zA-Z0-9_.-]+"
    );
    assert_eq!(
        validation_message(check_table_name("ab", BOUNDS)),
        "1 validation error detected: Value 'ab' at 'tableName' failed to satisfy \
         constraint: Member must have length greater than or equal to 3"
    );
    assert_eq!(
        validation_message(check_table_name("a b", BOUNDS)),
        "1 validation error detected: Value 'a b' at 'tableName' failed to satisfy \
         constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
    );
    // The deployment's bounds, as CreateTable and CreateBackup take them.
    assert_eq!(
        validation_message(check_table_name("abcd", (5, 10))),
        "1 validation error detected: Value 'abcd' at 'tableName' failed to satisfy \
         constraint: Member must have length greater than or equal to 5"
    );
    assert_eq!(
        validation_message(check_table_name("", (0, 10))),
        "1 validation error detected: Value '' at 'tableName' failed to satisfy \
         constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
    );
    for ok in [
        "Music",
        "a.b-c_D",
        &format!("{P}Music"),
        &format!("{P}Music/index/i"),
    ] {
        assert!(check_table_name(ok, BOUNDS).is_ok(), "{ok}");
    }
}

/// `SourceTableName` on the restore: a length outside the bounds is the
/// service's "TableName must be at least ..." text (M335 probe 58, an empty
/// name), and a name that fails the pattern is the model's constraint text at
/// `sourceTableName` (probe 46, a table ARN as the name). Both come after
/// the member checks and before the lookup.
#[test]
fn the_restore_source_name_is_checked_for_length_then_pattern() {
    let length = "TableName must be at least 3 characters long and at most 255 characters long";
    for name in [String::new(), "ab".to_owned(), "x".repeat(256)] {
        assert_eq!(
            validation_message(check_source_table_name(&name, BOUNDS)),
            length,
            "{name:?}"
        );
        assert_eq!(
            validation_message(restore_source_table_name(
                &json!({ "SourceTableName": name }),
                BOUNDS
            )),
            length,
            "{name:?}"
        );
    }
    assert_eq!(
        validation_message(check_source_table_name("ab", (5, 10))),
        "TableName must be at least 5 characters long and at most 10 characters long"
    );
    let arn = format!("{P}Music");
    assert_eq!(
        validation_message(restore_source_table_name(
            &json!({ "SourceTableName": arn }),
            BOUNDS
        )),
        format!(
            "1 validation error detected: Value '{arn}' at 'sourceTableName' failed to \
             satisfy constraint: Member must satisfy regular expression pattern: \
             [a-zA-Z0-9_.-]+"
        )
    );
    // Giving both members is reported before either is checked.
    assert_eq!(
        validation_message(restore_source_table_name(
            &json!({ "SourceTableName": "", "SourceTableArn": arn }),
            BOUNDS
        )),
        SOURCE_NAMED_TWICE
    );
}

/// The source is named by exactly one member; a table ARN names it as the
/// name does, and an ARN of an index is refused with the service's text.
#[test]
fn the_restore_source_is_one_member_and_an_arn_must_name_a_table() {
    assert_eq!(
        restore_source_table_name(&json!({ "SourceTableName": "Music" }), BOUNDS).unwrap(),
        "Music"
    );
    assert_eq!(
        restore_source_table_name(&json!({ "SourceTableArn": format!("{P}Music") }), BOUNDS)
            .unwrap(),
        "Music"
    );
    assert_eq!(
        validation_message(restore_source_table_name(
            &json!({ "SourceTableArn": format!("{P}Music/index/byArtist") }),
            BOUNDS
        )),
        "Invalid Request: Table ARN is invalid."
    );
    for other in ["Music", "Albums"] {
        assert_eq!(
            validation_message(restore_source_table_name(
                &json!({
                    "SourceTableName": "Music",
                    "SourceTableArn": format!("{P}{other}"),
                }),
                BOUNDS
            )),
            SOURCE_NAMED_TWICE
        );
    }
    assert_eq!(
        validation_message(restore_source_table_name(&json!({}), BOUNDS)),
        TABLE_NAME_REQUIRED
    );
}

/// Exactly one restore time: `RestoreDateTime`, or `UseLatestRestorableTime`
/// true. False counts as not given.
#[test]
fn the_restore_takes_exactly_one_time_member() {
    let date = 1_700_000_000.0;
    for body in [json!({}), json!({ "UseLatestRestorableTime": false })] {
        assert_eq!(
            validation_message(check_restore_time_members(&body)),
            NO_RESTORE_TIME,
            "{body}"
        );
    }
    assert_eq!(
        validation_message(check_restore_time_members(
            &json!({ "RestoreDateTime": date, "UseLatestRestorableTime": true })
        )),
        TWO_RESTORE_TIMES
    );
    for body in [
        json!({ "UseLatestRestorableTime": true }),
        json!({ "RestoreDateTime": date }),
    ] {
        assert!(check_restore_time_members(&body).is_ok(), "{body}");
    }
}
