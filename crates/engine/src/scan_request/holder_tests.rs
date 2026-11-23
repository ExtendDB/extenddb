// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The values the handler holds back for their step: `SelectBody` in order
//! R, and `ExpressionValuesBody` and `ScanFilterBody` in order U, with the
//! texts the service answers at that step.

use serde_json::{Value, json};

use super::test_support::{
    EMPTY_VALUE, INVALID, MORE_THAN_ONE, NUMERIC_BOB, SELECT_BAD, check, scan, validation_message,
    with,
};
use super::{CheckOrder, ExpressionValuesBody, ScanFilterBody, SelectBody};

/// `SelectBody::take` holds an invalid `Select` back in order R, so that the
/// typed deserialization does not report it, and leaves it in order U, where
/// the request-time checks report it; no service answer is asserted here.
#[test]
fn an_invalid_select_is_held_back_in_order_r_only() {
    let mut body = scan(json!({"Select": "BAD"}));
    let select = SelectBody::take(&mut body, CheckOrder::Readable);
    assert!(body.get("Select").is_none());
    assert_eq!(
        validation_message(select.validate()).as_deref(),
        Some(SELECT_BAD)
    );
    assert!(serde_json::from_value::<extenddb_core::types::ScanInput>(body).is_ok());
    let mut body = scan(json!({"Select": "BAD", "ExclusiveStartKey": {"pk": {}}}));
    let select = SelectBody::take(&mut body, CheckOrder::Unreadable);
    assert!(body.get("Select").is_some());
    assert!(select.validate().is_ok());
    // A valid Select and a Select of the wrong JSON type stay as written.
    for value in [json!("COUNT"), json!(5)] {
        let mut body = scan(json!({"Select": value}));
        let select = SelectBody::take(&mut body, CheckOrder::Readable);
        assert_eq!(body["Select"], value);
        assert!(select.validate().is_ok());
    }
}

/// `ExpressionValuesBody::take` puts a readable stand-in in the body for an
/// entry the checks classify and keeps the fault with the service's text: in
/// order U the `ExpressionAttributeValues contains invalid value: ... for key
/// :a` text for an empty value, an unknown descriptor and two descriptors
/// (2026-10-31 B4-7, 2026-11-05); in order R the set texts with the members
/// JSON-quoted (2026-11-05). An entry of the wrong JSON shape stays in the
/// body.
#[test]
fn a_faulty_expression_value_is_held_back_and_a_stand_in_takes_its_place() {
    let wrapper =
        |text: &str| format!("ExpressionAttributeValues contains invalid value: {text} for key :a");
    for (value, text) in [
        (json!({}), wrapper(EMPTY_VALUE)),
        (json!({"X": "1"}), wrapper(EMPTY_VALUE)),
        (json!({"S": "x", "N": "1"}), wrapper(MORE_THAN_ONE)),
        (json!({"L": [{}]}), wrapper(EMPTY_VALUE)),
    ] {
        let mut body = scan(json!({
            "FilterExpression": "a = :a",
            "ExpressionAttributeValues": {":a": value},
        }));
        let order = CheckOrder::of(&body);
        assert_eq!(order, CheckOrder::Unreadable, "{value}");
        let values = ExpressionValuesBody::take(&mut body, order, None);
        assert_eq!(body["ExpressionAttributeValues"], json!({":a": {"S": ""}}));
        assert_eq!(validation_message(values.validate()), Some(text), "{value}");
    }
    // In order R the set faults are request-time checks, with the texts of the
    // newer path, and nothing is taken.
    for (value, text) in [
        (json!({"NS": []}), "An number set  may not be empty"),
        (
            json!({"SS": ["a", "a"]}),
            r#"Input collection ["a", "a"] contains duplicates."#,
        ),
    ] {
        let mut body = scan(json!({"ExpressionAttributeValues": {":a": value}}));
        let order = CheckOrder::of(&body);
        assert_eq!(order, CheckOrder::Readable, "{value}");
        assert_eq!(check(&body), Some(format!("{INVALID}{text}")), "{value}");
        let values = ExpressionValuesBody::take(&mut body, order, None);
        assert_eq!(body["ExpressionAttributeValues"], json!({":a": value}));
        assert!(values.validate().is_ok(), "{value}");
    }
    // Measured 2026-11-23 (us-east-1, each request twice): `:a` with `{}`
    // beside `:n` with `{"N": "bob"}` names `:n` (bucket 0, ahead of `:a` in
    // 3), `{"X": "1"}` that no expression reads is named, and a float `Limit`
    // that satisfies its constraint puts the request in order U, where a
    // malformed number takes the invalid-value form (18:45 UTC).
    for (fields, text) in [
        (
            json!({"FilterExpression": "a = :a OR a = :n",
                   "ExpressionAttributeValues": {":a": {}, ":n": {"N": "bob"}}}),
            format!("ExpressionAttributeValues contains invalid value: {NUMERIC_BOB} for key :n"),
        ),
        (
            json!({"FilterExpression": "attribute_exists(a)",
                   "ExpressionAttributeValues": {":x": {"X": "1"}}}),
            wrapper(EMPTY_VALUE).replace(":a", ":x"),
        ),
        (
            json!({"Limit": 1.5, "FilterExpression": "a = :n",
                   "ExpressionAttributeValues": {":n": {"N": "bob"}}}),
            format!("ExpressionAttributeValues contains invalid value: {NUMERIC_BOB} for key :n"),
        ),
    ] {
        let mut body = scan(fields);
        assert_eq!(check(&body), None, "{body}");
        let order = CheckOrder::of(&body);
        assert_eq!(order, CheckOrder::Unreadable, "{body}");
        let values = ExpressionValuesBody::take(&mut body, order, None);
        assert_eq!(validation_message(values.validate()), Some(text), "{body}");
    }
    // An empty name comes after the entries (measured 2026-11-19): beside a
    // faulty entry the entry's text, alone the empty name's, and the name is
    // given a stand-in so that the typed deserialization passes.
    let empty_name =
        "ExpressionAttributeNames contains invalid value: Empty attribute name for key #a";
    for (values, text) in [
        (
            json!({":x": {"X": "1"}}),
            wrapper(EMPTY_VALUE).replace(":a", ":x"),
        ),
        (json!({":x": {"S": "x"}}), empty_name.to_owned()),
    ] {
        let mut body = scan(json!({
            "FilterExpression": "#a = :x",
            "ExpressionAttributeNames": {"#a": ""},
            "ExpressionAttributeValues": values,
            "ExclusiveStartKey": {"pk": {"X": "1"}},
        }));
        let order = CheckOrder::of(&body);
        assert_eq!(order, CheckOrder::Unreadable);
        assert_eq!(check(&body), None);
        let values = ExpressionValuesBody::take(&mut body, order, None);
        assert_eq!(validation_message(values.validate()), Some(text));
        assert_eq!(body["ExpressionAttributeNames"], json!({"#a": "a"}));
    }
    // Two faulty entries: the first in the fitted order is reported (measured
    // 2026-11-05: `:b` ahead of `:a`, which sorted order would reverse).
    let mut body =
        scan(json!({"ExpressionAttributeValues": {":p": {"S": "x"}, ":b": {}, ":a": {"X": "1"}}}));
    let values = ExpressionValuesBody::take(&mut body, CheckOrder::Unreadable, None);
    assert_eq!(
        validation_message(values.validate()),
        Some(wrapper(EMPTY_VALUE).replace(":a", ":b"))
    );
    // A readable value and a value of the wrong JSON shape stay as written.
    for value in [json!({"S": "x"}), json!({"N": 5})] {
        let mut body = scan(json!({"ExpressionAttributeValues": {":a": value}}));
        let values = ExpressionValuesBody::take(&mut body, CheckOrder::Readable, None);
        assert_eq!(body["ExpressionAttributeValues"], json!({":a": value}));
        assert!(values.validate().is_ok());
    }
}

/// Measured 2026-11-05 (order U): a malformed number in `ExpressionAttributeValues`
/// is reported at the map's step in the invalid-value form, the entries visited
/// in the fitted order: `{":p": {"S": "x"}, ":a": {"N": "bob"}, ":e": {}}`
/// names `:a`, the same with `:n` names `:n`, and the request-time checks pass
/// over the number. In order R the number is a request-time check with the
/// plain text.
#[test]
fn a_malformed_expression_value_number_in_order_u_takes_the_invalid_value_form() {
    let wrapper = |name: &str| {
        format!("ExpressionAttributeValues contains invalid value: {NUMERIC_BOB} for key {name}")
    };
    for (name, request_order) in [(":a", [":p", ":a", ":e"]), (":n", [":p", ":n", ":e"])] {
        let mut body = scan(json!({
            "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, name: {"N": "bob"}, ":e": {}},
        }));
        assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable);
        assert_eq!(check(&body), None, "{name}");
        let names: Vec<String> = request_order.iter().map(|n| (*n).to_owned()).collect();
        let values = ExpressionValuesBody::take(&mut body, CheckOrder::Unreadable, Some(&names));
        assert_eq!(
            validation_message(values.validate()),
            Some(wrapper(name)),
            "{name}"
        );
        assert_eq!(body["ExpressionAttributeValues"][name], json!({"S": ""}));
    }
    // With the unreadable value in the key, the number is still held back for
    // the map's step, after the key's faults.
    let mut body = scan(json!({
        "FilterExpression": "a = :n",
        "ExpressionAttributeValues": {":n": {"N": "bob"}},
        "ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}},
    }));
    assert_eq!(check(&body), None);
    let values = ExpressionValuesBody::take(&mut body, CheckOrder::Unreadable, None);
    assert_eq!(validation_message(values.validate()), Some(wrapper(":n")));
    // Order R: the request-time check, the plain text.
    let body = scan(json!({"ExpressionAttributeValues": {":n": {"N": "bob"}}}));
    assert_eq!(CheckOrder::of(&body), CheckOrder::Readable);
    assert_eq!(check(&body).as_deref(), Some(NUMERIC_BOB));
}

/// Measured 2026-11-05 (order U): a set fault in an `ExpressionAttributeValues`
/// entry takes the invalid-value form at the map's step, with the set texts of
/// order U, in the entries' hash order: `{":p": {"S": "x"}, ":s": {"NS": []},
/// ":e": {}}` names `:s` (bucket 1, ahead of `:e` in 3); an `SS` with
/// duplicates answers `... Input collection [a, a] contains duplicates. for key
/// :s`. Inside one entry the first fault in the value's element order is named:
/// `{"L": [{"N": "bob"}, {}]}` answers the number (measured), `{"L": [{},
/// {"N": "bob"}]}` the empty value (not measured, the element order).
#[test]
fn an_order_u_set_fault_in_an_expression_value_takes_the_invalid_value_form() {
    let wrapper = |text: &str, name: &str| {
        format!("ExpressionAttributeValues contains invalid value: {text} for key {name}")
    };
    for (set, text) in [
        (
            json!({"NS": []}),
            format!("{INVALID}An number set  may not be empty"),
        ),
        (
            json!({"SS": ["a", "a"]}),
            format!("{INVALID}Input collection [a, a] contains duplicates."),
        ),
        (
            json!({"BS": []}),
            format!("{INVALID}Binary sets should not be empty"),
        ),
    ] {
        let mut body = scan(json!({
            "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":s": set, ":e": {}},
        }));
        assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable);
        assert_eq!(check(&body), None, "{set}");
        let values = ExpressionValuesBody::take(&mut body, CheckOrder::Unreadable, None);
        assert_eq!(
            validation_message(values.validate()),
            Some(wrapper(&text, ":s")),
            "{set}"
        );
        assert_eq!(body["ExpressionAttributeValues"][":s"], json!({"S": ""}));
        assert_eq!(body["ExpressionAttributeValues"][":e"], json!({"S": ""}));
    }
    // Inside one entry: the element order.
    for (value, text) in [
        (json!({"L": [{"N": "bob"}, {}]}), NUMERIC_BOB),
        (json!({"L": [{}, {"N": "bob"}]}), EMPTY_VALUE),
        (
            json!({"L": [{"NS": []}, {}]}),
            "One or more parameter values were invalid: An number set  may not be empty",
        ),
        (
            json!({"L": [{"L": [{"N": "bob"}]}, {"S": "x", "N": "1"}]}),
            NUMERIC_BOB,
        ),
    ] {
        let mut body = scan(json!({
            "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":l": value},
        }));
        assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable, "{value}");
        let values = ExpressionValuesBody::take(&mut body, CheckOrder::Unreadable, None);
        assert_eq!(
            validation_message(values.validate()),
            Some(wrapper(text, ":l")),
            "{value}"
        );
    }
}

/// Measured 2026-11-05: in order U `ScanFilterBody::take` holds a `ScanFilter`
/// list value back for the step after the no-expression checks: a value that is
/// not one AttributeValue answers `Supplied AttributeValue is empty, must
/// contain exactly one of the supported datatypes` with no prefix, for `{}` and
/// for `{"X": "1"}`. In order R nothing is taken. A malformed number in a list
/// is #507's check (#564 item 3): the request-time checks pass it and `take`
/// leaves it in the body in both orders.
#[test]
fn a_scan_filter_list_value_is_checked_at_its_step() {
    let list = |values: Value| {
        scan(
            json!({"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": values}}}),
        )
    };
    // The numbers of the lists are #507's: not a request-time check here.
    let mut bob = list(json!([{"N": "bob"}, {"NS": ["bob"]}]));
    assert_eq!(CheckOrder::of(&bob), CheckOrder::Readable);
    assert_eq!(check(&bob), None);
    let beside_key = with(
        list(json!([{"N": "bob"}])),
        json!({"ExclusiveStartKey": {"pk": {"N": "fred"}, "sk": {"S": "x"}}}),
    );
    assert_eq!(check(&beside_key), None);
    assert!(
        ScanFilterBody::take(&mut bob, CheckOrder::Readable, None)
            .validate()
            .is_ok()
    );
    assert_eq!(
        bob["ScanFilter"]["a"]["AttributeValueList"],
        json!([{"N": "bob"}, {"NS": ["bob"]}])
    );
    // Order U: the list value is held back.
    for value in [json!({}), json!({"X": "1"}), json!({"L": [{}]})] {
        let mut body = list(json!([value]));
        assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable, "{value}");
        assert_eq!(check(&body), None, "{value}");
        let filter = ScanFilterBody::take(&mut body, CheckOrder::Unreadable, None);
        assert_eq!(
            body["ScanFilter"]["a"]["AttributeValueList"],
            json!([{"S": ""}]),
            "{value}"
        );
        assert_eq!(
            validation_message(filter.validate()).as_deref(),
            Some(EMPTY_VALUE),
            "{value}"
        );
        assert!(
            serde_json::from_value::<extenddb_core::types::ScanInput>(body).is_ok(),
            "{value}"
        );
    }
    let mut two = list(json!([{"S": "x", "N": "1"}]));
    let filter = ScanFilterBody::take(&mut two, CheckOrder::Unreadable, None);
    assert_eq!(
        validation_message(filter.validate()).as_deref(),
        Some(MORE_THAN_ONE)
    );
    // In order U the list's step names the first faulty value in the list's
    // order, a number or a set fault included (measured 2026-11-23: `[{},
    // {"NS": []}]` answers the empty value, `[{"N": "bob"}, {}]` the number).
    for (values, text) in [
        (json!([{}, {"NS": []}]), EMPTY_VALUE.to_owned()),
        (json!([{"N": "bob"}, {}]), NUMERIC_BOB.to_owned()),
        (
            json!([{"NS": []}, {}]),
            format!("{INVALID}An number set  may not be empty"),
        ),
    ] {
        let mut body = list(values.clone());
        assert_eq!(check(&body), None, "{values}");
        let filter = ScanFilterBody::take(&mut body, CheckOrder::Unreadable, None);
        assert_eq!(
            body["ScanFilter"]["a"]["AttributeValueList"],
            json!([{"S": ""}, {"S": ""}]),
            "{values}"
        );
        assert_eq!(
            validation_message(filter.validate()),
            Some(text),
            "{values}"
        );
    }
    // A number beside an unreadable key member is the list's fault too.
    let mut number = with(
        list(json!([{"N": "bob"}])),
        json!({"ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}}),
    );
    assert_eq!(check(&number), None);
    let filter = ScanFilterBody::take(&mut number, CheckOrder::Unreadable, None);
    assert_eq!(
        validation_message(filter.validate()).as_deref(),
        Some(NUMERIC_BOB)
    );
    // Order R: nothing is taken, a readable list stays as written.
    let mut readable = list(json!([{"N": "1"}]));
    let filter = ScanFilterBody::take(&mut readable, CheckOrder::Readable, None);
    assert_eq!(
        readable["ScanFilter"]["a"]["AttributeValueList"],
        json!([{"N": "1"}])
    );
    assert!(filter.validate().is_ok());
    // No ScanFilter at all.
    let mut none = scan(json!({}));
    assert!(
        ScanFilterBody::take(&mut none, CheckOrder::Unreadable, None)
            .validate()
            .is_ok()
    );
}
