// SPDX-License-Identifier: Apache-2.0

//! Tests for a `Query` whose `ExpressionAttributeValues` holds a value the
//! service cannot read as one type.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::tests::{body_with, message};
use super::validate_query_request_body;

const INVALID_VALUE: &str = "ExpressionAttributeValues contains invalid value: ";
const EMPTY: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";

#[test]
fn a_value_the_service_cannot_read_as_one_type_is_named_with_its_placeholder() {
    // Measured 2026-11-11, each value alone beside `:p = {"S": "x"}`.
    let several = "Supplied AttributeValue has more than one datatypes set, must contain \
                   exactly one of the supported datatypes";
    let null = "The supplied Item contains a null AttributeValue";
    for (value, reason) in [
        (json!({"X": "1"}), EMPTY.to_owned()),
        (Value::Null, null.to_owned()),
        (json!({}), EMPTY.to_owned()),
        (json!({"S": "a", "N": "1"}), several.to_owned()),
        (json!({"L": [{"X": "1"}]}), EMPTY.to_owned()),
        (json!({"L": [null]}), null.to_owned()),
        (json!({"L": [{}]}), EMPTY.to_owned()),
        (json!({"L": [{"S": "a", "N": "1"}]}), several.to_owned()),
        (json!({"L": [{"S": "a"}, {"X": "1"}]}), EMPTY.to_owned()),
        (json!({"M": {"k": {"X": "1"}}}), EMPTY.to_owned()),
        (json!({"M": {"k": {}}}), EMPTY.to_owned()),
        (json!({"M": {"k": null}}), format!("{null}: k is null")),
        (
            json!({"M": {"k": {"M": {"j": null}}}}),
            format!("{null}: j is null"),
        ),
        (json!({"M": {"k": {"L": [null]}}}), null.to_owned()),
        (
            json!({"L": [{"M": {"k": null}}]}),
            format!("{null}: k is null"),
        ),
        // The first fault in document order.
        (
            json!({"L": [{"N": "bob"}, {"X": "1"}]}),
            "The parameter cannot be converted to a numeric value: bob".to_owned(),
        ),
        (json!({"L": [{"X": "1"}, {"N": "bob"}]}), EMPTY.to_owned()),
    ] {
        let body = body_with(json!({":p": {"S": "x"}, ":z": value}), json!({}));
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{reason} for key :z")),
            "{value}"
        );
    }
}

#[test]
fn numbers_beside_a_value_the_service_cannot_read_are_wrapped_and_echoed_as_sent() {
    // Measured 2026-11-11 beside `:a = {"S": "a", "X": "1"}`, which the service
    // reads as an S: the numeric texts are wrapped with the placeholder, the
    // value is echoed without folding its digits, an empty N has no colon, and
    // a decimal digit below U+10000 still counts as a digit.
    let numeric = "The parameter cannot be converted to a numeric value";
    for (value, reason) in [
        (json!({"N": "bob"}), format!("{numeric}: bob")),
        (json!({"N": ""}), numeric.to_owned()),
        (json!({"NS": ["1", "bob"]}), format!("{numeric}: bob")),
        (json!({"L": [{"N": "bob"}]}), format!("{numeric}: bob")),
        (
            json!({"N": "\u{ff11}bob"}),
            format!("{numeric}: \u{ff11}bob"),
        ),
        (json!({"N": "\u{1d7cf}"}), format!("{numeric}: \u{1d7cf}")),
        (
            json!({"N": "1e200"}),
            "Number overflow. Attempting to store a number with magnitude larger than \
             supported range"
                .to_owned(),
        ),
        (
            json!({"N": "1".repeat(39)}),
            "Attempting to store more than 38 significant digits in a Number".to_owned(),
        ),
    ] {
        let body = body_with(
            json!({":p": {"S": "x"}, ":a": {"S": "a", "X": "1"}, ":u": value}),
            json!({}),
        );
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{reason} for key :u")),
            "{value}"
        );
    }
    let body = body_with(
        json!({":p": {"S": "x"}, ":a": {"S": "a", "X": "1"}, ":u": {"N": "\u{ff11}"}}),
        json!({}),
    );
    assert!(validate_query_request_body(&body).is_ok());
}

#[test]
fn a_value_the_service_cannot_read_is_named_ahead_of_a_number_fault() {
    // Measured 2026-11-11: beside `:u = {"N": "bob"}`, the service names `:z`
    // or `:a` when it holds a value it cannot read as one type (28 of 28
    // requests for `:z = {"X": "1"}`), and names `:u` when the other value has
    // no fault of its own.
    let several = "Supplied AttributeValue has more than one datatypes set, must contain \
                   exactly one of the supported datatypes";
    let null = "The supplied Item contains a null AttributeValue";
    let numeric = "The parameter cannot be converted to a numeric value: bob";
    for (value, reason) in [
        (json!({"X": "1"}), EMPTY.to_owned()),
        (Value::Null, null.to_owned()),
        (json!({}), EMPTY.to_owned()),
        (json!({"S": "a", "N": "1"}), several.to_owned()),
        (json!({"L": [{"X": "1"}]}), EMPTY.to_owned()),
        (json!({"L": [null]}), null.to_owned()),
        (json!({"L": [{}]}), EMPTY.to_owned()),
        (json!({"L": [{"S": "a", "N": "1"}]}), several.to_owned()),
        (json!({"M": {"k": {"X": "1"}}}), EMPTY.to_owned()),
        (json!({"M": {"k": null}}), format!("{null}: k is null")),
        (json!({"L": [{"S": "a"}, {"X": "1"}]}), EMPTY.to_owned()),
        (json!({"L": [{"N": "bob"}, {"X": "1"}]}), numeric.to_owned()),
        (json!({"L": [{"X": "1"}, {"N": "bob"}]}), EMPTY.to_owned()),
    ] {
        let body = body_with(
            json!({":p": {"S": "x"}, ":u": {"N": "bob"}, ":z": value}),
            json!({}),
        );
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{reason} for key :z")),
            "{value}"
        );
    }
    for (value, reason) in [
        (json!({"X": "1"}), EMPTY),
        (Value::Null, null),
        (json!({}), EMPTY),
        (json!({"S": "a", "N": "1"}), several),
        (json!({"L": [{"X": "1"}]}), EMPTY),
    ] {
        let body = body_with(
            json!({":p": {"S": "x"}, ":a": value, ":u": {"N": "bob"}}),
            json!({}),
        );
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{reason} for key :a")),
            "{value}"
        );
    }
    for values in [
        json!({":p": {"S": "x"}, ":u": {"N": "bob"}, ":z": {"S": "a", "X": "1"}}),
        json!({":p": {"S": "x"}, ":a": {"S": "a", "X": "1"}, ":u": {"N": "bob"}}),
    ] {
        let body = body_with(values.clone(), json!({}));
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{numeric} for key :u")),
            "{values}"
        );
    }
}

#[test]
fn beside_a_value_the_service_cannot_read_the_enums_and_limit_are_reported_together() {
    // Measured 2026-11-11 beside `:z = {"X": "1"}`: Select, then
    // ReturnConsumedCapacity, then Limit, in one message, ahead of the
    // parameter mix; the mix ahead of the values.
    let select = "Value 'bad' at 'select' failed to satisfy constraint: Member must satisfy \
                  enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, \
                  ALL_PROJECTED_ATTRIBUTES]";
    let rcc = "Value 'bad' at 'returnConsumedCapacity' failed to satisfy constraint: Member \
               must satisfy enum value set: [INDEXES, TOTAL, NONE]";
    let limit = |n: i64| {
        format!(
            "Value '{n}' at 'limit' failed to satisfy constraint: Member must have value \
             greater than or equal to 1"
        )
    };
    let mix = "Can not use both expression and non-expression parameters in the same request: \
               Non-expression parameters: {AttributesToGet} Expression parameters: \
               {KeyConditionExpression}";
    let values = json!({":p": {"S": "x"}, ":u": {"N": "bob"}, ":z": {"X": "1"}});
    for (extra, expected) in [
        (
            json!({"Limit": 0}),
            format!("1 validation error detected: {}", limit(0)),
        ),
        (
            json!({"Limit": -1}),
            format!("1 validation error detected: {}", limit(-1)),
        ),
        (
            json!({"Select": "bad"}),
            format!("1 validation error detected: {select}"),
        ),
        (
            json!({"ReturnConsumedCapacity": "bad"}),
            format!("1 validation error detected: {rcc}"),
        ),
        (
            json!({"Limit": 0, "ReturnConsumedCapacity": "bad"}),
            format!("2 validation errors detected: {rcc}; {}", limit(0)),
        ),
        (
            json!({"Limit": 0, "Select": "bad", "AttributesToGet": ["a"]}),
            format!("2 validation errors detected: {select}; {}", limit(0)),
        ),
        (
            json!({"Limit": 0, "ReturnConsumedCapacity": "bad", "Select": "bad"}),
            format!(
                "3 validation errors detected: {select}; {rcc}; {}",
                limit(0)
            ),
        ),
        (json!({"AttributesToGet": ["a"]}), mix.to_owned()),
    ] {
        let body = body_with(values.clone(), extra.clone());
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(expected),
            "{extra}"
        );
    }
    // A null value takes the same path.
    let body = body_with(
        json!({":p": {"S": "x"}, ":z": null}),
        json!({"Limit": 0, "ReturnConsumedCapacity": "bad"}),
    );
    assert_eq!(
        message(validate_query_request_body(&body)),
        Some(format!("2 validation errors detected: {rcc}; {}", limit(0)))
    );
}

#[test]
fn a_shape_fault_is_still_reported_ahead_of_a_value_the_service_cannot_read() {
    for extra in [
        json!({"Select": 5}),
        json!({"ExpressionAttributeNames": "x"}),
    ] {
        for value in [json!({"X": "1"}), Value::Null] {
            let body = body_with(json!({":p": {"S": "x"}, ":z": value}), extra.clone());
            assert!(
                matches!(
                    validate_query_request_body(&body),
                    Err(DynamoDbError::SerializationException(_))
                ),
                "{body}"
            );
        }
    }
}

#[test]
fn a_json_type_fault_inside_a_value_the_service_cannot_read_is_a_shape_fault() {
    // Measured 2026-11-11: each answers SerializationException.
    for (values, extra) in [
        (json!({":z": {"L": [null, "x"]}}), json!({})),
        (json!({":z": {"L": [{"X": "1"}, {"N": 5}]}}), json!({})),
        (json!({":z": {"S": 5, "X": "1"}}), json!({"Limit": 0})),
        (json!({":z": {"L": [null, {"B": "!!"}]}}), json!({})),
        (json!({":z": {"S": 5, "N": "1"}}), json!({"Limit": 0})),
        (json!({":z": {"L": [{"S": "a", "N": "1"}, "x"]}}), json!({})),
        (
            json!({":a": {"S": "a", "N": "1"}, ":b": {"N": 5}}),
            json!({}),
        ),
        (json!({":z": {"S": "a", "N": 1}}), json!({})),
    ] {
        let mut values = values;
        values[":p"] = json!({"S": "x"});
        let body = body_with(values, extra);
        assert!(
            matches!(
                validate_query_request_body(&body),
                Err(DynamoDbError::SerializationException(_))
            ),
            "{body}"
        );
    }
    // Two known descriptors with no JSON-type fault: the Limit text.
    let body = body_with(
        json!({":p": {"S": "x"}, ":z": {"S": "a", "N": "1"}}),
        json!({"Limit": 0}),
    );
    assert_eq!(
        message(validate_query_request_body(&body)),
        Some(
            "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: \
             Member must have value greater than or equal to 1"
                .to_owned()
        )
    );
}
