// SPDX-License-Identifier: Apache-2.0

//! Tests for the order of the request-time `Query` checks, and for a shape
//! fault reported ahead of them.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::tests::{LIMIT_ZERO, NUMERIC_BOB, RCC_BAD, message, one_error};
use super::{LIMIT_BELOW_ONE, validate_query_request_body};

#[test]
fn request_time_checks_run_limit_then_mix_then_numbers() {
    let bad_number = serde_json::json!({
        "TableName": "t",
        "KeyConditionExpression": "pk = :u",
        "ExpressionAttributeValues": {":u": {"N": "bob"}},
    });

    let mut with_limit = bad_number.clone();
    with_limit["Limit"] = serde_json::json!(0);
    assert_eq!(
        message(validate_query_request_body(&with_limit)).as_deref(),
        Some(LIMIT_BELOW_ONE)
    );

    let mut with_mix = bad_number.clone();
    with_mix["KeyConditions"] = serde_json::json!({
        "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}
    });
    assert_eq!(
        message(validate_query_request_body(&with_mix)).as_deref(),
        Some(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {KeyConditions} Expression parameters: {KeyConditionExpression}"
        )
    );

    assert_eq!(
        message(validate_query_request_body(&bad_number)).as_deref(),
        Some(NUMERIC_BOB)
    );

    // A number only a FilterExpression reads, and one nothing reads.
    for body in [
        serde_json::json!({
            "TableName": "t",
            "KeyConditionExpression": "pk = :p",
            "FilterExpression": "a = :n",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":n": {"N": "bob"}},
        }),
        serde_json::json!({
            "TableName": "t",
            "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":n": {"N": "bob"}},
        }),
    ] {
        assert_eq!(
            message(validate_query_request_body(&body)).as_deref(),
            Some(NUMERIC_BOB)
        );
    }

    // Legacy KeyConditions alone does not join the mix check; a well-formed
    // request passes.
    let legacy = serde_json::json!({
        "TableName": "t",
        "KeyConditions": {
            "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}
        },
        "FilterExpression": "a = :n",
        "ExpressionAttributeValues": {":n": {"N": "5"}},
        "Limit": 1,
    });
    assert!(validate_query_request_body(&legacy).is_ok());
}

#[test]
fn enum_checks_sit_on_either_side_of_the_numbers() {
    // Measured 2026-10-24 and 2026-11-11: an invalid ReturnConsumedCapacity is
    // reported ahead of an N value that is not a number, and that value ahead
    // of an invalid Select; a Limit of 0 ahead of either enum.
    let bad_number = serde_json::json!({
        "TableName": "t",
        "KeyConditionExpression": "pk = :u",
        "ExpressionAttributeValues": {":u": {"N": "bob"}},
    });
    let mut rcc = bad_number.clone();
    rcc["ReturnConsumedCapacity"] = serde_json::json!("bad");
    assert_eq!(
        message(validate_query_request_body(&rcc)).as_deref(),
        Some(one_error(RCC_BAD).as_str())
    );
    let mut select = bad_number.clone();
    select["Select"] = serde_json::json!("bad");
    assert_eq!(
        message(validate_query_request_body(&select)).as_deref(),
        Some(NUMERIC_BOB)
    );
    for field in ["ReturnConsumedCapacity", "Select"] {
        let mut limit = bad_number.clone();
        limit[field] = serde_json::json!("bad");
        limit["Limit"] = serde_json::json!(0);
        assert_eq!(
            message(validate_query_request_body(&limit)).as_deref(),
            Some(LIMIT_BELOW_ONE),
            "{field}"
        );
    }
    // With the numbers valid, an invalid Select is still reported.
    let mut only_select = serde_json::json!({
        "TableName": "t",
        "KeyConditionExpression": "pk = :u",
        "ExpressionAttributeValues": {":u": {"S": "bob"}},
    });
    only_select["Select"] = serde_json::json!("bad");
    assert!(
        message(validate_query_request_body(&only_select))
            .is_some_and(|m| m.contains("at 'select' failed to satisfy constraint"))
    );
}

#[test]
fn return_consumed_capacity_is_reported_ahead_of_the_parameter_mix() {
    // Measured 2026-11-11 with each non-expression parameter beside
    // KeyConditionExpression: an invalid ReturnConsumedCapacity is reported
    // ahead of the mix, and the mix ahead of an N value that is not a number
    // and of an invalid Select.
    for (name, value) in [
        ("AttributesToGet", json!(["a"])),
        (
            "QueryFilter",
            json!({"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}}),
        ),
        ("ConditionalOperator", json!("AND")),
        (
            "KeyConditions",
            json!({"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}}),
        ),
    ] {
        let mut body = json!({
            "TableName": "t",
            "KeyConditionExpression": "pk = :u",
            "ExpressionAttributeValues": {":u": {"N": "bob"}},
            "Select": "bad",
        });
        body[name] = value;
        let mix = format!(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {{{name}}} Expression parameters: \
             {{KeyConditionExpression}}"
        );
        assert_eq!(
            message(validate_query_request_body(&body)).as_deref(),
            Some(mix.as_str()),
            "{name}"
        );
        body["ReturnConsumedCapacity"] = json!("bad");
        assert_eq!(
            message(validate_query_request_body(&body)).as_deref(),
            Some(one_error(RCC_BAD).as_str()),
            "{name}"
        );
        body["Limit"] = json!(0);
        assert_eq!(
            message(validate_query_request_body(&body)).as_deref(),
            Some(LIMIT_BELOW_ONE),
            "{name}"
        );
    }
}

#[test]
fn a_shape_fault_in_another_field_is_reported_ahead_of_the_checks() {
    // Measured 2026-10-25: a field of the wrong JSON type
    // anywhere in the body answers SerializationException even when a Limit,
    // mix, enum or number check would fail.
    let shape_faults = [
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "Limit": 0, "ExpressionAttributeValues": "x",
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeNames": "x",
            "ExpressionAttributeValues": {":p": {"N": "bob"}},
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "Limit": "1", "ExpressionAttributeValues": {":p": {"N": "bob"}},
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "AttributesToGet": {"a": 1}, "ProjectionExpression": "a",
            "ExpressionAttributeValues": {":p": {"S": "x"}},
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "QueryFilter": [1], "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}},
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "Select": 5, "ExpressionAttributeValues": {":p": {"N": "bob"}},
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "Limit": 0, "Select": 5, "ExpressionAttributeValues": {":p": {"S": "x"}},
        }),
    ];
    for body in shape_faults {
        assert!(
            matches!(
                validate_query_request_body(&body),
                Err(DynamoDbError::SerializationException(_))
            ),
            "{body}"
        );
    }

    // A string enum value that is not a member is the checks' own answer, not a
    // shape fault.
    let mut select = serde_json::json!({
        "TableName": "t", "KeyConditionExpression": "pk = :p",
        "Limit": 0, "ExpressionAttributeValues": {":p": {"S": "x"}},
    });
    select["Select"] = serde_json::json!("bad");
    assert_eq!(
        message(validate_query_request_body(&select)).as_deref(),
        Some(LIMIT_BELOW_ONE)
    );
    select["ReturnConsumedCapacity"] = serde_json::json!("bad");
    select["Limit"] = serde_json::json!(1);
    assert!(
        message(validate_query_request_body(&select))
            .is_some_and(|m| m.contains("at 'returnConsumedCapacity' failed to satisfy constraint"))
    );

    // A ValidationException the deserializer raises does not preempt a check:
    // an empty NS beside a Limit of 0 answers the Limit text.
    let empty_set = serde_json::json!({
        "TableName": "t", "KeyConditionExpression": "pk = :p",
        "Limit": 0, "ExpressionAttributeValues": {":p": {"S": "x"}, ":s": {"NS": []}},
    });
    assert_eq!(
        message(validate_query_request_body(&empty_set)).as_deref(),
        Some(LIMIT_BELOW_ONE)
    );

    // Neither does an unknown type descriptor, which the deserializer reports
    // as a SerializationException: the service answers the Limit
    // ValidationException in the form that names the value (measured
    // 2026-10-25 and 2026-11-11).
    let unknown_type = serde_json::json!({
        "TableName": "t", "KeyConditionExpression": "pk = :p",
        "Limit": 0, "ExpressionAttributeValues": {":p": {"S": "x"}, ":a": {"X": "1"}},
    });
    assert_eq!(
        message(validate_query_request_body(&unknown_type)).as_deref(),
        Some(one_error(LIMIT_ZERO).as_str())
    );

    // A body that passes every check is left to the handler's deserializer.
    let shape_fault_only = serde_json::json!({
        "TableName": "t", "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": "x",
    });
    assert!(validate_query_request_body(&shape_fault_only).is_ok());
}

#[test]
fn legacy_condition_values_get_the_number_check() {
    // Rows K1 to K16, measured 2026-11-22, each request twice: table `n` has
    // `pk` (N) and `sk` (N) keys and a GSI `ngsi` on `n` (N), and table `s` a
    // `pk` (S) key. K12 (a value of the wrong key type) and K13 (two values
    // for EQ) also fail checks the handler makes after these.
    //
    // Each row holds the row, the members beside `"TableName": "n"`, and the
    // answer (null when the request passes these checks).
    let cond =
        |op: &str, values: Value| json!({"ComparisonOperator": op, "AttributeValueList": values});
    let eq = |n: &str| cond("EQ", json!([{"N": n}]));
    let num = |v: &str| format!("The parameter cannot be converted to a numeric value: {v}");
    let overflow =
        "Number overflow. Attempting to store a number with magnitude larger than supported range";
    let rcc = one_error(&RCC_BAD.replace("'bad'", "'BAD'"));
    let (k1, bob) = (json!({"pk": eq("abc")}), json!({"a": eq("bob")}));
    let between = cond("BETWEEN", json!([{"N": "1"}, {"N": "x"}]));
    let gt = cond("GT", json!([{"N": "1e200"}]));
    let two = cond("EQ", json!([{"N": "abc"}, {"N": "1"}]));
    let rows = [
        json!(["K1", {"KeyConditions": k1}, num("abc")]),
        json!(["K2", {"KeyConditions": {"pk": eq("")}}, num("")]),
        json!(["K9", {"KeyConditions": {"pk": eq(" 5")}}, num(" 5")]),
        json!(["K3", {"KeyConditions": {"pk": eq("1"), "sk": between}}, num("x")]),
        json!(["K4", {"KeyConditions": {"pk": eq("1"), "sk": gt}}, overflow]),
        json!(["K10", {"KeyConditions": {"pk": eq("1")}}, null]),
        json!(["K14", {"IndexName": "ngsi", "KeyConditions": {"n": eq("abc")}}, num("abc")]),
        json!(["K15", {"IndexName": "ngsi", "KeyConditions": {"n": eq("")}}, num("")]),
        json!(["K12", {"TableName": "s", "KeyConditions": k1}, num("abc")]),
        json!(["K13", {"KeyConditions": {"pk": two}}, num("abc")]),
        json!(["K5", {"KeyConditions": k1, "Select": "BAD"}, num("abc")]),
        json!(["K6", {"KeyConditions": k1, "ReturnConsumedCapacity": "BAD"}, rcc]),
        json!(["K8", {"KeyConditions": k1, "Limit": 0}, LIMIT_BELOW_ONE]),
        json!(["K11", {"KeyConditions": k1, "FilterExpression": "a = :v",
            "ExpressionAttributeValues": {":v": {"N": "bob"}}}, NUMERIC_BOB]),
        json!(["K7", {"KeyConditions": k1, "QueryFilter": bob}, NUMERIC_BOB]),
        json!(["K16", {"KeyConditions": {"pk": eq("1")}, "QueryFilter": bob}, NUMERIC_BOB]),
    ];
    for row in rows {
        let mut body = json!({"TableName": "n"});
        for (member, value) in row[1].as_object().into_iter().flatten() {
            body[member] = value.clone();
        }
        assert_eq!(
            message(validate_query_request_body(&body)).as_deref(),
            row[2].as_str(),
            "{}",
            row[0]
        );
    }
}
