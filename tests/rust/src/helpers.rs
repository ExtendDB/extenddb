// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! AttributeValue constructors, assertion helpers, and utility functions.

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::test_base::get_key;

// ========== Error helpers ==========

/// Extract the DynamoDB error code from an `SdkError`.
///
/// Uses `ProvideErrorMetadata::code()` to get the error code string
/// (e.g., "ResourceNotFoundException", "ConditionalCheckFailedException").
/// Returns `None` for non-service errors (dispatch failures, timeouts, etc.).
pub fn err_code<E, R>(err: &aws_smithy_runtime_api::client::result::SdkError<E, R>) -> Option<&str>
where
    E: ProvideErrorMetadata,
{
    ProvideErrorMetadata::code(err)
}

/// Extract the DynamoDB error message from an `SdkError` (empty string if none).
pub fn err_msg<E, R>(err: &aws_smithy_runtime_api::client::result::SdkError<E, R>) -> String
where
    E: ProvideErrorMetadata,
{
    ProvideErrorMetadata::message(err)
        .unwrap_or_default()
        .to_string()
}

// ========== AttributeValue constructors ==========

pub fn s(val: &str) -> AttributeValue {
    AttributeValue::S(val.into())
}

pub fn n(val: i64) -> AttributeValue {
    AttributeValue::N(val.to_string())
}

pub fn b(val: &str) -> AttributeValue {
    AttributeValue::B(aws_smithy_types::Blob::new(val.as_bytes()))
}

pub fn ss(vals: &[&str]) -> AttributeValue {
    AttributeValue::Ss(vals.iter().map(|v| (*v).into()).collect())
}

pub fn ns(vals: &[i64]) -> AttributeValue {
    AttributeValue::Ns(vals.iter().map(|v| v.to_string()).collect())
}

pub fn bs(vals: &[&str]) -> AttributeValue {
    AttributeValue::Bs(
        vals.iter()
            .map(|v| aws_smithy_types::Blob::new(v.as_bytes()))
            .collect(),
    )
}

pub fn bool_val(val: bool) -> AttributeValue {
    AttributeValue::Bool(val)
}

pub fn null_val() -> AttributeValue {
    AttributeValue::Null(true)
}

pub fn list_val(vals: Vec<AttributeValue>) -> AttributeValue {
    AttributeValue::L(vals)
}

pub fn map_val(vals: HashMap<String, AttributeValue>) -> AttributeValue {
    AttributeValue::M(vals)
}

// ========== Assertion helpers ==========

/// Assert two items are equal (all attributes match).
pub fn assert_item_eq(
    expected: &HashMap<String, AttributeValue>,
    actual: &HashMap<String, AttributeValue>,
) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "Attribute count mismatch: expected {:?}, actual {:?}",
        expected.keys().collect::<Vec<_>>(),
        actual.keys().collect::<Vec<_>>()
    );
    for (k, v) in expected {
        let actual_v = actual
            .get(k)
            .unwrap_or_else(|| panic!("Missing attribute: {k}"));
        assert_attr_eq(v, actual_v, k);
    }
}

fn assert_attr_eq(expected: &AttributeValue, actual: &AttributeValue, ctx: &str) {
    match (expected, actual) {
        (AttributeValue::S(e), AttributeValue::S(a)) => assert_eq!(e, a, "{ctx}"),
        (AttributeValue::N(e), AttributeValue::N(a)) => assert_eq!(e, a, "{ctx}"),
        (AttributeValue::B(e), AttributeValue::B(a)) => assert_eq!(e, a, "{ctx}"),
        (AttributeValue::Ss(e), AttributeValue::Ss(a)) => {
            let e_set: HashSet<_> = e.iter().collect();
            let a_set: HashSet<_> = a.iter().collect();
            assert_eq!(e_set, a_set, "{ctx}");
        }
        (AttributeValue::Ns(e), AttributeValue::Ns(a)) => {
            let e_set: HashSet<_> = e.iter().collect();
            let a_set: HashSet<_> = a.iter().collect();
            assert_eq!(e_set, a_set, "{ctx}");
        }
        (AttributeValue::Bs(e), AttributeValue::Bs(a)) => {
            let e_set: HashSet<_> = e.iter().map(|b| b.as_ref()).collect();
            let a_set: HashSet<_> = a.iter().map(|b| b.as_ref()).collect();
            assert_eq!(e_set, a_set, "{ctx}");
        }
        (AttributeValue::Bool(e), AttributeValue::Bool(a)) => assert_eq!(e, a, "{ctx}"),
        (AttributeValue::Null(e), AttributeValue::Null(a)) => assert_eq!(e, a, "{ctx}"),
        (AttributeValue::L(e), AttributeValue::L(a)) => {
            assert_eq!(e.len(), a.len(), "{ctx} list length");
            for (i, (ev, av)) in e.iter().zip(a.iter()).enumerate() {
                assert_attr_eq(ev, av, &format!("{ctx}[{i}]"));
            }
        }
        (AttributeValue::M(e), AttributeValue::M(a)) => {
            assert_eq!(e.len(), a.len(), "{ctx} map size");
            for (k, v) in e {
                let av = a.get(k).unwrap_or_else(|| panic!("{ctx}.{k} missing"));
                assert_attr_eq(v, av, &format!("{ctx}.{k}"));
            }
        }
        _ => panic!("{ctx}: type mismatch: expected {expected:?}, actual {actual:?}"),
    }
}

/// Verify an item exists in the table and matches expected.
pub async fn check_item(
    c: &Client,
    table: &str,
    expected: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    let key = get_key(table, expected);
    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let item = resp.item().expect("Item should exist");
    let actual: HashMap<String, AttributeValue> =
        item.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_item_eq(expected, &actual);
    actual
}

/// Create a map with all DynamoDB attribute types.
pub fn create_map_with_all_types() -> HashMap<String, AttributeValue> {
    let mut map = HashMap::new();
    map.insert("stringAttr".into(), s("testString"));
    map.insert("numberAttr".into(), n(123));
    map.insert("binaryAttr".into(), b("testBinary"));
    map.insert("stringSetAttr".into(), ss(&["a", "b", "c"]));
    map.insert("numberSetAttr".into(), ns(&[1, 2, 3]));
    map.insert("binarySetAttr".into(), bs(&["bin1", "bin2"]));
    map.insert("boolTrueAttr".into(), bool_val(true));
    map.insert("boolFalseAttr".into(), bool_val(false));
    map.insert("nullAttr".into(), null_val());
    map.insert("listAttr".into(), list_val(vec![s("l1"), n(2)]));
    let mut nested = HashMap::new();
    nested.insert("nested".into(), s("value"));
    map.insert("mapAttr".into(), map_val(nested));
    map
}

/// Generate a string of the given byte length.
pub fn generate_string(byte_length: usize) -> String {
    "a".repeat(byte_length)
}

/// Timestamp in milliseconds — for unique table name suffixes.
pub fn ts() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
