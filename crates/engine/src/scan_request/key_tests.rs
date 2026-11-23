// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `ExclusiveStartKey` checks of each order and the member order they
//! visit, fitted to the service's hash table. The rows are the service's
//! answers; the tests of `StartKeyBody::take` check which values it holds
//! back, with no service answer, and the rows with two value faults in two
//! key members accept either member's fault.

use extenddb_core::error::DynamoDbError;
use serde_json::json;

use super::member_order::{MemberOrders, fitted_member_order, hash_table_mask, java_hash};
use super::test_support::{
    EMPTY_VALUE, INVALID, KEY_PREFIX, MORE_THAN_ONE, NUMERIC_BOB, check, check_key, check_key_in,
    filter, scan, validation_message, with, with_key,
};
use super::{CheckOrder, StartKeyBody, member_orders};

#[test]
fn a_malformed_number_in_the_exclusive_start_key_is_reported_with_the_number_text() {
    let key = |number: &str| json!({"pk": {"N": number}, "sk": {"S": "x"}});
    let bad = with(with_key(key("bob")), filter("1"));
    // The request-time checks leave the number to the key's stage.
    assert_eq!(check(&bad), None);
    assert_eq!(check_key(&bad).as_deref(), Some(NUMERIC_BOB));
    assert_eq!(check_key(&with(with_key(key("1")), filter("1"))), None);
    // Measured 2026-10-30 12:40 UTC: with no filter, the same text.
    assert_eq!(
        check_key(&with_key(key("bob"))).as_deref(),
        Some(NUMERIC_BOB)
    );
    // Measured 2026-10-31 between 23:54 and 23:56 UTC: a number as an NS
    // member, as an L member, and in the L of an attribute that is not a key
    // attribute; the number texts for a leading space, an empty text and 39
    // digits (on an N hash key). Measured 2026-11-05: `{"pk": {"N": "bob"}}`
    // alone, with no `sk`, on the `pk`, `sk` table.
    for key in [
        json!({"pk": {"NS": ["bob"]}, "sk": {"S": "x"}}),
        json!({"pk": {"L": [{"N": "bob"}]}, "sk": {"S": "x"}}),
        json!({"pk": {"S": "p0"}, "sk": {"S": "x"}, "z": {"L": [{"N": "bob"}]}}),
        json!({"pk": {"N": "bob"}}),
    ] {
        assert_eq!(
            check_key(&with_key(key.clone())).as_deref(),
            Some(NUMERIC_BOB),
            "{key}"
        );
    }
    for (number, text) in [
        (
            " 1",
            "The parameter cannot be converted to a numeric value:  1",
        ),
        ("", "The parameter cannot be converted to a numeric value: "),
        (
            "111111111111111111111111111111111111111",
            "Attempting to store more than 38 significant digits in a Number",
        ),
    ] {
        let key = with_key(json!({"pk": {"N": number}}));
        assert_eq!(check_key(&key).as_deref(), Some(text), "{number:?}");
    }
}

/// Measured 2026-11-05: a key value that is empty, carries an unknown or
/// lower-case type descriptor, or carries two, at its top or inside an `L` or
/// `M`, in a key member or in a member that is not a key attribute, answers the
/// service's text with the key prefix at the key's stage of order U.
#[test]
fn a_shape_fault_in_a_key_value_answers_the_prefixed_text() {
    let empty = format!("{KEY_PREFIX}{EMPTY_VALUE}");
    for key in [
        json!({"pk": {}, "sk": {"S": "x"}}),
        json!({"pk": {"X": "1"}, "sk": {"S": "x"}}),
        json!({"pk": {"s": "x"}, "sk": {"S": "x"}}),
        json!({"pk": {"L": [{}]}, "sk": {"S": "x"}}),
        json!({"pk": {"M": {"k": {}}}, "sk": {"S": "x"}}),
        json!({"pk": {"S": "p0"}, "sk": {"S": "x"}, "z": {}}),
        json!({"pk": {}}),
    ] {
        let body = with_key(key.clone());
        assert_eq!(check(&body), None, "{key}");
        assert_eq!(check_key(&body).as_deref(), Some(empty.as_str()), "{key}");
    }
    let two = with_key(json!({"pk": {"S": "x", "N": "1"}, "sk": {"S": "x"}}));
    assert_eq!(
        check_key(&two),
        Some(format!("{KEY_PREFIX}{MORE_THAN_ONE}"))
    );
    // Inside one member the first fault in the value's element order: the
    // members of an `M` in sorted order (2026-11-05, `{"M": {"k": {"N":
    // "bob"}, "j": {}}}` in a non-key member answers the empty value), the
    // members of an `L` in their order (2026-11-23, `{"L": [{"N": "bob"},
    // {}]}` answers the number).
    let inside = with_key(json!({
        "pk": {"S": "x"}, "sk": {"S": "x"}, "z": {"M": {"k": {"N": "bob"}, "j": {}}},
    }));
    assert_eq!(check_key(&inside).as_deref(), Some(empty.as_str()));
    let in_a_list = with_key(json!({"pk": {"L": [{"N": "bob"}, {}]}, "sk": {"S": "s"}}));
    assert_eq!(check_key(&in_a_list).as_deref(), Some(NUMERIC_BOB));
}

/// Measured 2026-11-05: in order R the set faults of a key answer the service's
/// texts with no prefix, the members JSON-quoted, after every number in the
/// key.
#[test]
fn a_set_fault_in_a_key_value_answers_the_order_r_text_without_a_prefix() {
    for (key, text) in [
        (json!({"NS": []}), "An number set  may not be empty"),
        (json!({"SS": []}), "An string set  may not be empty"),
        (json!({"BS": []}), "Binary sets should not be empty"),
        (
            json!({"NS": ["1", "1"]}),
            "Input collection contains duplicates.",
        ),
        (
            json!({"NS": ["1", "1.0"]}),
            "Input collection contains duplicates.",
        ),
        (
            json!({"SS": ["a", "a"]}),
            r#"Input collection ["a", "a"] contains duplicates."#,
        ),
        (
            json!({"SS": ["a\"b", "a\"b"]}),
            r#"Input collection ["a\"b", "a\"b"] contains duplicates."#,
        ),
        (
            json!({"BS": ["YQ==", "YQ=="]}),
            r#"Input collection ["YQ==","YQ=="]of type BS contains duplicates."#,
        ),
    ] {
        let body = with_key(json!({"pk": key, "sk": {"S": "x"}}));
        assert_eq!(CheckOrder::of(&body), CheckOrder::Readable, "{key}");
        assert_eq!(check_key(&body), Some(format!("{INVALID}{text}")), "{key}");
    }
    // A number in a set is reported with the number text, ahead of a
    // duplicate in the same set (2026-11-05, `["bob", "1", "1"]` and `["1",
    // "1", "bob"]`).
    for members in [
        json!(["bob"]),
        json!(["bob", "1", "1"]),
        json!(["1", "1", "bob"]),
    ] {
        let bob = with_key(json!({"pk": {"S": "x"}, "sk": {"S": "x"}, "z": {"NS": members}}));
        assert_eq!(check_key(&bob).as_deref(), Some(NUMERIC_BOB), "{members}");
    }
    // A set with distinct numbers passes.
    assert_eq!(
        check_key(&with_key(
            json!({"pk": {"NS": ["1", "2"]}, "sk": {"S": "x"}})
        )),
        None
    );
}

/// Measured 2026-11-05: in order U (here, the other member of the key is
/// unreadable and the set member comes first in the fitted order) the set texts
/// have no JSON quotes, the `NS` duplicate text is bare, and the empty `SS`
/// text carries the key prefix. The key's number keeps its text.
#[test]
fn a_set_fault_in_a_key_value_answers_the_order_u_text() {
    for (sk, text) in [
        (
            json!({"NS": []}),
            format!("{INVALID}An number set  may not be empty"),
        ),
        (
            json!({"SS": []}),
            format!("{KEY_PREFIX}{INVALID}An string set  may not be empty"),
        ),
        (
            json!({"NS": ["1", "1"]}),
            "Input collection contains duplicates".to_owned(),
        ),
        (
            json!({"SS": ["a", "a"]}),
            format!("{INVALID}Input collection [a, a] contains duplicates."),
        ),
        (json!({"N": "bob"}), NUMERIC_BOB.to_owned()),
    ] {
        for pk in [json!({}), json!({"X": "1"}), json!({"S": "x", "N": "1"})] {
            let body = with_key(json!({"pk": pk, "sk": sk}));
            assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable);
            assert_eq!(check_key(&body).as_deref(), Some(text.as_str()), "{body}");
        }
    }
    // Measured 2026-11-05 beside an unreadable key member: an empty BS with
    // the key prefix, as the empty SS; a BS with duplicates with the members
    // unquoted and no prefix.
    let bs_empty = with_key(json!({"pk": {}, "sk": {"BS": []}}));
    assert_eq!(
        check_key(&bs_empty),
        Some(format!(
            "{KEY_PREFIX}{INVALID}Binary sets should not be empty"
        ))
    );
    let bs_dup = with_key(json!({"pk": {}, "sk": {"BS": ["YQ==", "YQ=="]}}));
    assert_eq!(
        check_key(&bs_dup),
        Some(format!(
            "{INVALID}Input collection [YQ==, YQ==]of type BS contains duplicates."
        ))
    );
}

/// The fitted member order: `String.hashCode()` of the name, spread and masked
/// to four buckets, buckets ascending, the request's order inside a bucket
/// (measured 2026-10-31, 2026-11-04 and 2026-11-05 on five tables).
#[test]
fn the_fitted_member_order_follows_the_hash_buckets_then_the_request() {
    for (name, bucket) in [
        ("sk", 0),
        ("a", 1),
        ("b", 2),
        ("pk", 3),
        ("gpk", 3),
        ("id", 3),
        ("ts", 3),
        ("\u{e4}", 0),
        ("\u{e9}", 1),
        (":n", 0),
        (":b", 0),
        (":p", 2),
        (":a", 3),
        (":e", 3),
    ] {
        assert_eq!(java_hash(name) & 3, bucket, "{name}");
    }
    // The table doubles when it holds more than three quarters of its size.
    // The names `n0`, `n1`, ... spread over the buckets.
    let spread =
        |count: usize| -> Vec<u32> { (0..count).map(|i| java_hash(&format!("n{i}"))).collect() };
    for (names, mask) in [
        (0, 3),
        (1, 3),
        (3, 3),
        (4, 7),
        (6, 7),
        (7, 15),
        (12, 15),
        (13, 31),
    ] {
        assert_eq!(hash_table_mask(&spread(names)), mask, "{names} names");
    }
    for (name, bucket) in [("sk", 0), ("gpk", 3), ("pk", 3), ("gsk", 6)] {
        assert_eq!(java_hash(name) & 7, bucket, "{name}");
    }
    let order = |names: &[&str]| fitted_member_order(names.iter().copied());
    // The pk, sk table: sk first in both request orders.
    assert_eq!(order(&["pk", "sk"]), ["sk", "pk"]);
    assert_eq!(order(&["sk", "pk"]), ["sk", "pk"]);
    // HASH a, RANGE b: a first; HASH pk, RANGE a: a first.
    assert_eq!(order(&["b", "a"]), ["a", "b"]);
    assert_eq!(order(&["pk", "a"]), ["a", "pk"]);
    assert_eq!(order(&["a", "pk"]), ["a", "pk"]);
    // ts and id share a bucket: the request's order.
    assert_eq!(order(&["ts", "id"]), ["ts", "id"]);
    assert_eq!(order(&["id", "ts"]), ["id", "ts"]);
    // The GSI key of three members: sk, then gpk and pk in the request's order.
    assert_eq!(order(&["gpk", "pk", "sk"]), ["sk", "gpk", "pk"]);
    assert_eq!(order(&["pk", "sk", "gpk"]), ["sk", "pk", "gpk"]);
    // Four members (measured 2026-11-05 on a GSI with a range key): eight
    // buckets, so gpk and pk (3) come ahead of gsk (6) in both request orders;
    // with four buckets gsk (2) would come ahead of them.
    assert_eq!(
        order(&["gpk", "gsk", "pk", "sk"]),
        ["sk", "gpk", "pk", "gsk"]
    );
    assert_eq!(
        order(&["sk", "pk", "gsk", "gpk"]),
        ["sk", "pk", "gpk", "gsk"]
    );
    // HASH \u{e4}, RANGE \u{e9}: \u{e4} first in both request orders.
    assert_eq!(order(&["\u{e4}", "\u{e9}"]), ["\u{e4}", "\u{e9}"]);
    assert_eq!(order(&["\u{e9}", "\u{e4}"]), ["\u{e4}", "\u{e9}"]);
    // ExpressionAttributeValues entries (order U, 2026-11-05): :n ahead of :e,
    // :b ahead of :p and :a, :a and :e in the request's order.
    assert_eq!(order(&[":p", ":a", ":e"]), [":p", ":a", ":e"]);
    assert_eq!(order(&[":p", ":n", ":e"]), [":n", ":p", ":e"]);
    assert_eq!(order(&[":p", ":b", ":a"]), [":b", ":p", ":a"]);
    // Seven names: sixteen buckets.
    let seven = ["a", "b", "c", "d", "e", "f", "g"];
    assert_eq!(order(&seven).len(), 7);
    assert_eq!(
        hash_table_mask(&seven.iter().map(|name| java_hash(name)).collect::<Vec<_>>()),
        15
    );
    // Measured 2026-11-23 at 21:49 UTC: every name below falls in bucket 10 of
    // sixteen. Eight names keep sixteen buckets and the request's order; the
    // ninth name in that bucket doubles the table, and with 32 buckets `:dt`
    // falls in bucket 10, ahead of the others in 26.
    let eight = [":aa", ":bb", ":cc", ":dd", ":ee", ":ff", ":gg", ":dt"];
    let nine = [
        ":aa", ":bb", ":cc", ":dd", ":ee", ":ff", ":gg", ":hh", ":dt",
    ];
    let ten = [
        ":aa", ":bb", ":cc", ":dd", ":ee", ":ff", ":gg", ":hh", ":ii", ":dt",
    ];
    let hashes = |names: &[&str]| names.iter().map(|name| java_hash(name)).collect::<Vec<_>>();
    for name in ten {
        assert_eq!(java_hash(name) & 15, 10, "{name}");
    }
    assert_eq!(java_hash(":aa") & 31, 26);
    assert_eq!(java_hash(":dt") & 31, 10);
    assert_eq!(hash_table_mask(&hashes(&eight)), 15);
    assert_eq!(hash_table_mask(&hashes(&nine)), 31);
    assert_eq!(hash_table_mask(&hashes(&ten)), 31);
    assert_eq!(order(&eight)[0], ":aa");
    assert_eq!(order(&nine)[0], ":dt");
    assert_eq!(order(&ten)[0], ":dt");
}

/// `member_orders` reads the member names of the three fields from the
/// request text in document order, in one pass; a repeated name keeps its
/// first position, a repeated field is read at its first position; a body
/// without a field gives `None` for it, and text that does not parse or a
/// field that is not an object gives `None` for all three.
#[test]
fn the_request_order_of_the_members_is_read_from_the_raw_body() {
    let raw = br#"{"TableName": "t", "ExclusiveStartKey": {"ts": {"N": "bob"}, "id": {}}, "ExpressionAttributeValues": {":p": {"S": "x"}, ":n": {"N": "bob"}}, "ScanFilter": {"b": {}, "a": {}}}"#;
    let names = |names: &[&str]| Some(names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>());
    assert_eq!(
        member_orders(raw),
        MemberOrders {
            key: names(&["ts", "id"]),
            values: names(&[":p", ":n"]),
            filter: names(&["b", "a"]),
        }
    );
    let repeated = br#"{"ExclusiveStartKey": {"b": {}, "a": {}, "b": {"S": "x"}}, "TableName": "t", "ExclusiveStartKey": {"c": {}}}"#;
    assert_eq!(member_orders(repeated).key, names(&["b", "a"]));
    assert_eq!(
        member_orders(br#"{"TableName": "t"}"#),
        MemberOrders::default()
    );
    for text in [
        &br#"{"TableName": "t", "ExclusiveStartKey": "x", "ScanFilter": {"a": {}}}"#[..],
        b"not json",
        b"",
    ] {
        assert_eq!(member_orders(text), MemberOrders::default());
    }
}

/// Faults in two members of one key: the first faulty member in the fitted
/// order is reported. Measured rows: `{"pk": {}, "sk": {"N": "bob"}}` and the
/// reversed request order answer `bob` (2026-10-31); `{"pk": {"X": "1"}, "sk":
/// {"N": "bob"}}` answers `bob`; `{"pk": {}, "sk": {"NS": []}}` answers the
/// empty NS text and `{"pk": {"S": "x", "N": "1"}, "sk": {"SS": []}}` the
/// prefixed SS text (2026-11-05); on HASH `a`, RANGE `b` `{"a": {}, "b": {"N":
/// "bob"}}` answers the empty value and `{"a": {"N": "bob"}, "b": {}}` `bob`;
/// on HASH `pk`, RANGE `a` `{"pk": {}, "a": {"N": "bob"}}` answers `bob` in
/// both request orders; on HASH `id`, RANGE `ts` the member first in the
/// request; on the GSI `{"gpk": {}, "pk": {"N": "bob"}, "sk": {"S": "x"}}`
/// the empty value. With two value faults the service's two sends disagreed on
/// 8 of 22 rows: those rows accept either member's fault.
#[test]
fn two_faulty_members_answer_the_first_in_the_fitted_order() {
    let empty = format!("{KEY_PREFIX}{EMPTY_VALUE}");
    let ns_empty = format!("{INVALID}An number set  may not be empty");
    let ss_empty = format!("{KEY_PREFIX}{INVALID}An string set  may not be empty");
    for (key, request_order, text) in [
        (
            json!({"pk": {}, "sk": {"N": "bob"}}),
            ["pk", "sk"],
            NUMERIC_BOB,
        ),
        (
            json!({"sk": {"N": "bob"}, "pk": {}}),
            ["sk", "pk"],
            NUMERIC_BOB,
        ),
        (
            json!({"pk": {"X": "1"}, "sk": {"N": "bob"}}),
            ["pk", "sk"],
            NUMERIC_BOB,
        ),
        (
            json!({"pk": {"N": "bob"}, "sk": {}}),
            ["pk", "sk"],
            empty.as_str(),
        ),
        (
            json!({"pk": {}, "sk": {"NS": []}}),
            ["pk", "sk"],
            ns_empty.as_str(),
        ),
        (
            json!({"pk": {"S": "x", "N": "1"}, "sk": {"SS": []}}),
            ["pk", "sk"],
            ss_empty.as_str(),
        ),
        (
            json!({"a": {}, "b": {"N": "bob"}}),
            ["a", "b"],
            empty.as_str(),
        ),
        (json!({"a": {"N": "bob"}, "b": {}}), ["a", "b"], NUMERIC_BOB),
        (
            json!({"pk": {}, "a": {"N": "bob"}}),
            ["pk", "a"],
            NUMERIC_BOB,
        ),
        (
            json!({"a": {"N": "bob"}, "pk": {}}),
            ["a", "pk"],
            NUMERIC_BOB,
        ),
        (
            json!({"id": {}, "ts": {"N": "bob"}}),
            ["id", "ts"],
            empty.as_str(),
        ),
        (
            json!({"ts": {"N": "bob"}, "id": {}}),
            ["ts", "id"],
            NUMERIC_BOB,
        ),
    ] {
        let body = with_key(key.clone());
        assert_eq!(
            check_key_in(&body, &request_order).as_deref(),
            Some(text),
            "{key} in the order {request_order:?}"
        );
    }
    let gsi = with_key(json!({"gpk": {}, "pk": {"N": "bob"}, "sk": {"S": "x"}}));
    assert_eq!(
        check_key_in(&gsi, &["gpk", "pk", "sk"]).as_deref(),
        Some(empty.as_str())
    );
    // Two value faults: either member's fault.
    let fred = NUMERIC_BOB.replace("bob", "fred");
    for (key, either) in [
        (
            json!({"pk": {"N": "bob"}, "sk": {"N": "fred"}}),
            [NUMERIC_BOB, fred.as_str()],
        ),
        (
            json!({"pk": {"NS": []}, "sk": {"N": "bob"}}),
            [ns_empty.as_str(), NUMERIC_BOB],
        ),
    ] {
        let answer = check_key_in(&with_key(key.clone()), &["pk", "sk"]);
        assert!(
            either.contains(&answer.as_deref().unwrap_or_default()),
            "{key}: {answer:?}"
        );
    }
}

/// `StartKeyBody::take` holds back a key the deserializer refuses for its
/// value and leaves one of the wrong JSON shape in the body, where the typed
/// deserialization reports it ahead of every check; no service answer is
/// asserted here.
#[test]
fn a_key_of_the_wrong_shape_stays_in_the_body_and_an_unreadable_value_is_held_back() {
    for key in [json!("x"), json!({"pk": "x"}), json!({"pk": {"N": 5}})] {
        let mut body = with_key(key.clone());
        let order = CheckOrder::of(&body);
        let start_key = StartKeyBody::take(&mut body, None);
        assert!(body.get("ExclusiveStartKey").is_some(), "{key}");
        assert!(start_key.validate(order).is_ok(), "{key}");
        let shape = serde_json::from_value::<extenddb_core::types::ScanInput>(body)
            .map_err(crate::deserialize_error);
        assert!(
            matches!(shape, Err(DynamoDbError::SerializationException(_))),
            "{key}: {shape:?}"
        );
    }
    for key in [
        json!({"pk": {}}),
        json!({"pk": {"X": "1"}}),
        json!({"pk": {"S": "x", "N": "1"}}),
        json!({"pk": {"NS": []}}),
        json!({"pk": {"SS": ["a", "a"]}}),
    ] {
        let mut body = with_key(key.clone());
        let order = CheckOrder::of(&body);
        let start_key = StartKeyBody::take(&mut body, None);
        assert!(body.get("ExclusiveStartKey").is_none(), "{key}");
        assert!(start_key.validate(order).is_err(), "{key}");
        assert!(
            serde_json::from_value::<extenddb_core::types::ScanInput>(body).is_ok(),
            "{key}"
        );
    }
    // A readable key stays in the body and passes.
    let mut body = with_key(json!({"pk": {"S": "p0"}, "sk": {"S": "x"}}));
    let start_key = StartKeyBody::take(&mut body, None);
    assert!(body.get("ExclusiveStartKey").is_some());
    assert!(start_key.validate(CheckOrder::Readable).is_ok());
    // No key at all.
    let mut body = scan(json!({}));
    let start_key = StartKeyBody::take(&mut body, None);
    assert!(start_key.validate(CheckOrder::Readable).is_ok());
    // A request order that does not name the key's members is ignored.
    let mut body = with_key(json!({"pk": {}, "sk": {"N": "bob"}}));
    let names = ["a".to_owned(), "b".to_owned()];
    let start_key = StartKeyBody::take(&mut body, Some(&names));
    assert_eq!(
        validation_message(start_key.validate(CheckOrder::Unreadable)).as_deref(),
        Some(NUMERIC_BOB)
    );
}
