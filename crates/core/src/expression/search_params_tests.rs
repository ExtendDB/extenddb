// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Each expectation is a live SearchVectors answer, measured twice in
//! eu-north-1 on 2026-11-21.

use std::collections::HashMap;

use std::collections::BTreeSet;

use super::{check_search_maps, check_unused_keys, check_unused_names, with_unused_names};
use crate::error::DynamoDbError;
use crate::types::AttributeValue;

fn names(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn values(keys: &[&str]) -> HashMap<String, AttributeValue> {
    keys.iter()
        .map(|k| ((*k).to_owned(), AttributeValue::S("x".to_owned())))
        .collect()
}

/// Names, values, and the expected message.
type Case<'a> = (
    Option<HashMap<String, String>>,
    HashMap<String, AttributeValue>,
    &'a str,
);

fn refused(result: Result<(), DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(m)) => m,
        other => format!("not a ValidationException: {other:?}"),
    }
}

fn maps(
    n: Option<&HashMap<String, String>>,
    v: Option<&HashMap<String, AttributeValue>>,
) -> String {
    refused(check_search_maps(n, v, true, false))
}

#[test]
fn an_empty_map_is_refused_names_first() {
    let empty_n = names(&[]);
    let empty_v = values(&[]);
    assert_eq!(
        maps(None, Some(&empty_v)),
        "ExpressionAttributeValues must not be empty"
    );
    assert_eq!(
        maps(Some(&empty_n), Some(&empty_v)),
        "ExpressionAttributeNames must not be empty"
    );
    assert_eq!(
        refused(check_search_maps(None, Some(&empty_v), false, false)),
        "ExpressionAttributeValues must not be empty"
    );
}

#[test]
fn an_empty_attribute_name_answers_before_the_keys_and_the_values() {
    let empty_v = values(&[]);
    assert_eq!(
        maps(Some(&names(&[("#c", "")])), Some(&empty_v)),
        "ExpressionAttributeNames contains invalid value: Empty attribute name for key #c"
    );
    assert_eq!(
        maps(Some(&names(&[("#a-b", "")])), Some(&values(&[":us"]))),
        "ExpressionAttributeNames contains invalid value: Empty attribute name for key #a-b"
    );
}

/// An attribute name of 65536 bytes or more answers ahead of a bad key, as
/// an empty one does (measured on DynamoDB 2026-11-24, `{"n": <65536 bytes>}`
/// twice in each of two regions).
#[test]
fn a_too_large_attribute_name_answers_before_the_keys() {
    let large = "C".repeat(65536);
    assert_eq!(
        maps(Some(&names(&[("n", large.as_str())])), None),
        "Attribute name is too large, must be less than 65536 bytes for key n"
    );
    assert_eq!(
        maps(
            Some(&names(&[("#a-b", large.as_str())])),
            Some(&values(&[":us"]))
        ),
        "Attribute name is too large, must be less than 65536 bytes for key #a-b"
    );
}

#[test]
fn a_bad_name_key_answers_before_a_bad_value_key() {
    assert_eq!(
        maps(
            Some(&names(&[("#a-b", "x")])),
            Some(&values(&[":us", ":b-k"]))
        ),
        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"#a-b\""
    );
}

#[test]
fn each_key_fault_has_its_own_message() {
    let long_v = format!(":{}", "k".repeat(300));
    let long_n = format!("#{}", "k".repeat(300));
    let cases: Vec<Case<'_>> = vec![
        (
            None,
            values(&[":s", ":b-k"]),
            "ExpressionAttributeValues contains invalid key: Syntax error; key: \":b-k\"",
        ),
        (
            None,
            values(&[":s", ":"]),
            "ExpressionAttributeValues contains invalid key: Syntax error; key: \":\"",
        ),
        (
            None,
            values(&[":s", &long_v]),
            "ExpressionAttributeValues contains invalid key: The expression attribute map \
             contains a key that is too long; size of key: 301",
        ),
        (
            None,
            values(&[":s", ""]),
            "ExpressionAttributeValues contains invalid key: The expression attribute map \
             contains an empty key;",
        ),
        (
            Some(names(&[("#", "x")])),
            values(&[":us"]),
            "ExpressionAttributeNames contains invalid key: Syntax error; key: \"#\"",
        ),
        (
            Some(names(&[(&long_n, "x")])),
            values(&[":us"]),
            "ExpressionAttributeNames contains invalid key: The expression attribute map \
             contains a key that is too long; size of key: 301",
        ),
        (
            Some(names(&[("", "x")])),
            values(&[":us"]),
            "ExpressionAttributeNames contains invalid key: The expression attribute map \
             contains an empty key;",
        ),
    ];
    for (n, v, expected) in cases {
        assert_eq!(maps(n.as_ref(), Some(&v)), expected);
    }
}

#[test]
fn an_attribute_name_must_be_shorter_than_65536_bytes() {
    let at_limit = "C".repeat(65536);
    assert_eq!(
        maps(Some(&names(&[("#c", &at_limit)])), Some(&values(&[":us"]))),
        "Attribute name is too large, must be less than 65536 bytes for key #c"
    );
    let below = "C".repeat(65535);
    assert!(check_search_maps(Some(&names(&[("#c", &below)])), None, true, false).is_ok());
}

#[test]
fn a_map_needs_the_expression_that_reads_it() {
    let n = names(&[("#p", "Price")]);
    let v = values(&[":us"]);
    let without = |n, v, cond, proj| refused(check_search_maps(n, v, cond, proj));
    assert_eq!(
        without(Some(&n), None, false, false),
        "ExpressionAttributeNames can only be specified when using expressions"
    );
    assert_eq!(
        without(Some(&n), Some(&v), false, false),
        "ExpressionAttributeNames can only be specified when using expressions"
    );
    assert_eq!(
        without(None, Some(&v), false, false),
        "ExpressionAttributeValues can only be specified when using expressions"
    );
    assert_eq!(
        without(Some(&n), Some(&v), false, true),
        "ExpressionAttributeValues can only be specified when using expressions"
    );
    assert!(check_search_maps(Some(&n), None, false, true).is_ok());
    assert!(check_search_maps(Some(&n), Some(&v), true, false).is_ok());
}

#[test]
fn a_name_is_used_by_the_condition_or_the_projection() {
    let n = names(&[("#p", "Price"), ("#c", "Country")]);
    let cond = |keys: &[&'static str]| keys.iter().copied().collect::<BTreeSet<_>>();
    assert!(check_unused_names(Some(&n), &cond(&["#c"]), "#p").is_ok());
    assert!(check_unused_names(Some(&n), &cond(&[]), "#c, a.#p[0]").is_ok());
    assert!(check_unused_names(None, &cond(&[]), "").is_ok());
    assert_eq!(
        refused(check_unused_names(Some(&n), &cond(&[]), "Price")),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#c, #p}"
    );
    assert_eq!(
        refused(check_unused_names(Some(&n), &cond(&["#c"]), "")),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#p}"
    );
}

#[test]
fn unused_keys_are_listed_in_sorted_order() {
    let v: HashMap<String, u8> = [(":b", 1), (":a", 2), (":c", 3)]
        .into_iter()
        .map(|(k, x)| (k.to_owned(), x))
        .collect();
    let used: BTreeSet<&str> = [":c"].into_iter().collect();
    assert_eq!(
        refused(check_unused_keys(
            "ExpressionAttributeValues",
            Some(&v),
            &used
        )),
        "Value provided in ExpressionAttributeValues unused in expressions: keys: {:a, :b}"
    );
    assert!(
        check_unused_keys(
            "ExpressionAttributeValues",
            None::<&HashMap<String, u8>>,
            &used
        )
        .is_ok()
    );
}

#[test]
fn unused_names_answer_after_a_parse_error_and_before_unused_values() {
    let n = names(&[("#u", "x")]);
    let syntax = || -> Result<(), DynamoDbError> {
        Err(DynamoDbError::ValidationException(
            "Invalid SearchConditionExpression: Syntax error".to_owned(),
        ))
    };
    let unused_values = |m: &str| -> Result<(), DynamoDbError> {
        Err(DynamoDbError::ValidationException(m.to_owned()))
    };
    let names_msg = "Value provided in ExpressionAttributeNames unused in expressions: keys: {#u}";
    let none = BTreeSet::new();
    assert_eq!(
        refused(with_unused_names(syntax(), Some(&n), &none, "")),
        "Invalid SearchConditionExpression: Syntax error"
    );
    assert_eq!(
        refused(with_unused_names(Ok(()), Some(&n), &none, "")),
        names_msg
    );
    for values_msg in [
        "Value provided in ExpressionAttributeValues unused in expressions: keys: {:w}",
        "Value provided in ExpressionAttributeValues unused in expressions",
    ] {
        assert_eq!(
            refused(with_unused_names(
                unused_values(values_msg),
                Some(&n),
                &none,
                ""
            )),
            names_msg
        );
        assert_eq!(
            refused(with_unused_names(
                unused_values(values_msg),
                None,
                &none,
                ""
            )),
            values_msg
        );
    }
}

/// The equality-only grammar resolves `#alias` through the key `#alias` and
/// then the key `alias`, so `##c` uses the key `#c`, and its names check
/// counts the key the resolution reads.
#[test]
fn equality_grammar_names_are_the_keys_it_resolves() {
    let refs = crate::expression::search_condition_name_refs("##c = :v AND #p = :w");
    for key in ["#c", "#p"] {
        assert!(refs.contains(key), "{key} is used: {refs:?}");
    }
    let n = names(&[("#c", "cat")]);
    assert!(check_unused_names(Some(&n), &refs, "").is_ok());
}
