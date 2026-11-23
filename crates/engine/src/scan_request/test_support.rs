// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The helpers and the texts the request, key and holder tests share.
//!
//! The `ScanFilter` in the parameter mix, the `AttributesToGet` length, the
//! nesting depth of `ExpressionAttributeValues`, the two check orders, and the
//! `ExclusiveStartKey` and `ExpressionAttributeValues` checks of each order.
//!
//! Every row is a request sent to the service (us-east-1, each request twice,
//! a Scan of a table the probe created) with the answer it gave: on 2026-10-25
//! at 10:38 and 10:55 UTC, on 2026-10-30 at 12:40 UTC, on 2026-10-31 between
//! 23:54 and 23:56 UTC and on 2026-11-05 where a test says so. The test of the
//! PartiQL Scan's checks names its own measurement. The tests of
//! `StartKeyBody::take` and `SelectBody::take` check which values they hold
//! back, with no service answer; the tests of `ExpressionValuesBody::take` and
//! `ScanFilterBody::take` also check the measured texts they keep; the tests of
//! the member order check the fitted rule's buckets and masks; the rows with
//! two value faults in two key members accept either member's fault.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::{CheckOrder, SelectBody, StartKeyBody, ValuesDepth, request_checks};

pub(super) const MIX: &str = "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}";

pub(super) const LENGTH: &str = "1 validation error detected: Value at 'AttributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1";

pub(super) const NUMERIC_BOB: &str = "The parameter cannot be converted to a numeric value: bob";

pub(super) const NESTING: &str = "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit";

pub(super) const SELECT_BAD: &str = "1 validation error detected: Value 'BAD' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]";

pub(super) const KEY_PREFIX: &str = "The provided starting key is invalid: ";

pub(super) const EMPTY_VALUE: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";

pub(super) const MORE_THAN_ONE: &str = "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes";

pub(super) const INVALID: &str = "One or more parameter values were invalid: ";

/// The message of a `ValidationException`, `None` for `Ok`.
pub(super) fn validation_message(result: Result<(), DynamoDbError>) -> Option<String> {
    match result {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// The request-time checks of a `Scan` request on `body`, in the order of the
/// body's class.
pub(super) fn check(body: &Value) -> Option<String> {
    check_with(body, ValuesDepth::Checked)
}

/// The request-time checks on `body` with the nesting depth checked as
/// `depth` says, in the order of the body's class.
pub(super) fn check_with(body: &Value, depth: ValuesDepth) -> Option<String> {
    validation_message(request_checks(body, depth).map(drop))
}

/// A Scan body with the fields of `extra` added to the table name.
pub(super) fn scan(extra: Value) -> Value {
    let mut body = json!({"TableName": "t"});
    for (field, value) in extra.as_object().expect("an object") {
        body[field] = value.clone();
    }
    body
}

/// A filter that reads `:n`, which holds `number`.
pub(super) fn filter(number: &str) -> Value {
    json!({
        "FilterExpression": "a = :n",
        "ExpressionAttributeValues": {":n": {"N": number}},
    })
}

pub(super) fn with(mut body: Value, extra: Value) -> Value {
    for (field, value) in extra.as_object().expect("an object") {
        body[field] = value.clone();
    }
    body
}

/// `leaf` inside `levels` lists, each list one level.
pub(super) fn nested(leaf: &Value, levels: usize) -> Value {
    (0..levels).fold(leaf.clone(), |inner, _| json!({"L": [inner]}))
}

/// A filter that reads `:p`, with `value` under `:d`, which nothing reads.
pub(super) fn with_unused_value(value: Value) -> Value {
    scan(json!({
        "FilterExpression": "a = :p",
        "ExpressionAttributeValues": {":p": {"S": "x"}, ":d": value},
    }))
}

/// A Scan body with `key` as its `ExclusiveStartKey`.
pub(super) fn with_key(key: Value) -> Value {
    scan(json!({"ExclusiveStartKey": key}))
}

/// The key's check on `body` in the order of the body's class, as the handler
/// makes it: `StartKeyBody::take` on a copy of the body with the members in
/// `request_order` (the `json!` macro sorts them), then `validate`.
pub(super) fn check_key_in(body: &Value, request_order: &[&str]) -> Option<String> {
    let mut body = body.clone();
    let order = CheckOrder::of(&body);
    let names: Vec<String> = request_order
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    let request_order = if names.is_empty() {
        None
    } else {
        Some(names.as_slice())
    };
    validation_message(StartKeyBody::take(&mut body, request_order).validate(order))
}

/// [`check_key_in`] with the parsed body's member order.
pub(super) fn check_key(body: &Value) -> Option<String> {
    check_key_in(body, &[])
}

/// `SelectBody::take` on a copy of `body`, then `validate`.
pub(super) fn check_select(body: &Value) -> Option<String> {
    let mut body = body.clone();
    let order = CheckOrder::of(&body);
    validation_message(SelectBody::take(&mut body, order).validate())
}
