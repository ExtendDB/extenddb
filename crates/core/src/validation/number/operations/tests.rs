// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use serde_json::{Value, json};

use super::*;
use crate::types::{BatchWriteItemInput, TransactGetItem, TransactWriteItem};

// Every value below was sent to DynamoDB twice on 2026-10-27 (us-east-1) on
// the operation path the test names, with the same answer both times, except
// where a comment says the test pins a choice of these checks.
const NUMERIC: &str = "The parameter cannot be converted to a numeric value";
const OVERFLOW: &str =
    "Number overflow. Attempting to store a number with magnitude larger than supported range";
const UNDERFLOW: &str =
    "Number underflow. Attempting to store a number with magnitude smaller than supported range";
const DIGITS: &str = "Attempting to store more than 38 significant digits in a Number";

fn refused(result: Result<(), DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(message)) => message,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

fn i32_scale(n: &str) -> Result<(), String> {
    validate_number_i32_scale(n)
}

#[test]
fn i32_scale_accepts_what_the_service_accepts() {
    for n in [
        "6180".to_owned(),
        format!("1{}", "0".repeat(38)),
        format!("1{}", "0".repeat(100)),
        format!("1.{}", "0".repeat(40)),
        format!("{}0", "1".repeat(38)),
        format!("{}.0", "1".repeat(38)),
        "0".repeat(40),
        format!("0.{}", "0".repeat(40)),
        format!("{}1", "0".repeat(40)),
        "+5".to_owned(),
        "5.".to_owned(),
        ".5".to_owned(),
        "+.5".to_owned(),
        "-.5".to_owned(),
        "1E+5".to_owned(),
        format!("1E{}5", "0".repeat(24)),
        format!("1E-{}5", "0".repeat(24)),
        "0.0E126".to_owned(),
        "0E125".to_owned(),
        "0E-130".to_owned(),
    ] {
        assert_eq!(i32_scale(&n), Ok(()), "{n:?}");
    }
    // A lowercase exponent marker: accepted on BatchWriteItem and PutItem and
    // stored as the 64-bit reader writes it (measured 2026-10-27, twice each).
    for (n, stored) in [
        ("1e5", "100000"),
        ("1e+5", "100000"),
        ("1.5e-3", "0.0015"),
        ("0e125", "0"),
    ] {
        assert_eq!(i32_scale(n), Ok(()), "{n:?}");
        assert_eq!(validate_and_normalize_number(n).unwrap(), stored, "{n:?}");
    }
}

#[test]
fn i32_scale_refuses_an_exponent_or_scale_outside_32_bits_as_malformed() {
    for n in [
        "abc",
        " 5",
        "1E2147483648",
        "1E4294967296",
        "1E9223372036854775807",
        "1E9223372036854775808",
        "1E-2147483648",
        "1.5E-2147483648",
        "1E-2147483649",
        "0E2147483648",
        "0E9223372036854775807",
        "1E",
        "1E+",
        ".",
        "-",
        "1E5.5",
        "\u{b2}",
        "1.5a",
        "1.\u{b2}",
        "1.5.5",
        "1.e",
    ] {
        assert_eq!(i32_scale(n), Err(format!("{NUMERIC}: {n}")), "{n:?}");
    }
    // An empty value answers the message with no colon.
    assert_eq!(i32_scale(""), Err(NUMERIC.to_owned()));
}

#[test]
fn i32_scale_magnitude_wraps_and_comes_before_the_digit_count() {
    let ones = "1".repeat(39);
    for n in [
        "1E126".to_owned(),
        format!("1E{}126", "0".repeat(24)),
        format!("{ones}E200"),
    ] {
        assert_eq!(i32_scale(&n), Err(OVERFLOW.to_owned()), "{n:?}");
    }
    for n in [
        "1E-131".to_owned(),
        "1E-2147483647".to_owned(),
        // The magnitude passes the 32-bit maximum and wraps to a negative.
        "10E2147483647".to_owned(),
        "1E2147483647".to_owned(),
        format!("{ones}E2147483647"),
        format!("{ones}E-200"),
    ] {
        assert_eq!(i32_scale(&n), Err(UNDERFLOW.to_owned()), "{n:?}");
    }
    assert_eq!(i32_scale(&ones), Err(DIGITS.to_owned()));
}

/// A deterministic xorshift generator for the reader comparison below.
struct Generator(u64);

impl Generator {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }

    fn pick<T: Copy>(&mut self, from: &[T]) -> T {
        from[usize::try_from(self.below(from.len() as u64)).unwrap()]
    }

    /// `count` digits, about half of them zeros.
    fn digits(&mut self, count: usize) -> String {
        (0..count)
            .map(|_| self.pick(&['0', '0', '0', '1', '2', '5', '9']))
            .collect()
    }

    /// A string shaped like a number, malformed about one time in twenty.
    fn number_like(&mut self) -> String {
        let mut s = self.pick(&["", "", "+", "-"]).to_owned();
        let integer_digits = self.pick(&[0, 1, 2, 3, 38, 39, 45]);
        s.push_str(&self.digits(integer_digits));
        if self.below(2) == 0 {
            s.push('.');
            let fraction_digits = self.pick(&[0, 1, 2, 5, 39, 45]);
            s.push_str(&self.digits(fraction_digits));
        }
        if self.below(2) == 0 {
            s.push_str(self.pick(&["e", "E", "E+", "e-", "E-"]));
            let exponent = match self.below(5) {
                0 => self.below(300),
                1 => 120 + self.below(15),
                2 => 2_147_483_640 + self.below(16),
                3 => 9_223_372_036_854_775_800 + self.below(9),
                _ => self.below(10_000_000_000),
            };
            s.push_str(&exponent.to_string());
        }
        if self.below(20) == 0 {
            // The string is ASCII so far, so every index is a char boundary.
            let at = usize::try_from(self.below(s.len() as u64 + 1)).unwrap();
            let junk = self.pick(&[' ', 'a', '.', '+', 'e', '\u{b2}', '\u{ff15}']);
            s.insert(at, junk);
        }
        s
    }
}

#[test]
fn the_two_readers_agree_outside_the_documented_differences() {
    // `validate_number_i32_scale` and `validate_and_normalize_number` read
    // one grammar. They must accept the same values and refuse the others
    // with the same message, except in four cases the docs describe:
    // 1. a zero whose exponent fits in 32 bits, which the 64-bit reader
    //    accepts as 0 (#618) and the 32-bit reader range checks;
    // 2. an exponent, a scale (fraction digits less the exponent) or a
    //    magnitude (digits less the scale) that does not fit in 32 bits,
    //    where the 32-bit reader cannot convert the value or wraps its
    //    magnitude, and both refuse it;
    // 3. more than 38 digits and a magnitude out of range, where the 32-bit
    //    reader names the magnitude and the 64-bit reader the digit count;
    // 4. a zero whose exponent fits in 32 bits and whose scale does not
    //    (`0.0E-2147483648`), which the 64-bit reader accepts as 0 and the
    //    32-bit reader cannot convert.
    // The empty value's text differs too (module doc), and is tested above.
    let mut generator = Generator(0x9E37_79B9_7F4A_7C15);
    let mut agreed = 0;
    for _ in 0..4_000 {
        let s = generator.number_like();
        if s.is_empty() {
            continue;
        }
        let narrow = i32_scale(&s);
        let wide = validate_and_normalize_number(&s);
        let wide_answer = wide.clone().map(drop).map_err(message);
        if narrow == wide_answer {
            agreed += 1;
            continue;
        }
        // The exponent as written, when it is a plain signed integer, and the
        // scale it gives with the fraction digits.
        let (mantissa, exponent) = s
            .split_once(['e', 'E'])
            .map_or((s.as_str(), None), |(m, e)| (m, e.parse::<i64>().ok()));
        let fraction_digits = mantissa.split_once('.').map_or(0, |(_, f)| f.len());
        let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
        let precision = digits.trim_start_matches('0').len().max(1);
        let exponent_wide = exponent.is_some_and(|e| i32::try_from(e).is_err());
        let scale = exponent.map(|e| i128::try_from(fraction_digits).unwrap() - i128::from(e));
        let scale_wide = scale.is_some_and(|scale| i32::try_from(scale).is_err());
        let magnitude_wide = scale.is_some_and(|scale| {
            i32::try_from(i128::try_from(precision).unwrap() - scale).is_err()
        });
        let magnitude = |m: &str| m == OVERFLOW || m == UNDERFLOW;
        let malformed = |m: &str| m.starts_with(NUMERIC);
        let documented = match (&narrow, &wide) {
            (Err(m), Ok(zero)) => {
                zero == "0" && (magnitude(m) || (malformed(m) && !exponent_wide && scale_wide))
            }
            (Err(_), Err(_)) if exponent_wide || scale_wide || magnitude_wide => true,
            (Err(m), Err(_)) => magnitude(m) && wide_answer == Err(DIGITS.to_owned()),
            (Ok(()), _) => false,
        };
        assert!(
            documented,
            "{s:?}: 32-bit reader {narrow:?}, 64-bit reader {wide_answer:?}"
        );
    }
    assert!(agreed > 2_000, "only {agreed} of the values agreed");
}

fn get(key: Value) -> GetItemInput {
    serde_json::from_value(json!({"TableName": "t", "Key": key})).unwrap()
}

#[test]
fn get_item_key_answers_without_a_prefix() {
    assert_eq!(
        refused(get_item(&get(json!({"pk": {"N": "abc"}})))),
        format!("{NUMERIC}: abc")
    );
    // A malformed number after a valid attribute is found too.
    assert_eq!(
        refused(get_item(&get(json!({"a": {"S": "x"}, "b": {"N": "abc"}})))),
        format!("{NUMERIC}: abc")
    );
    // The 64-bit exponent rules: an overflow where the transaction paths
    // answer an underflow, and the empty value with its colon.
    assert_eq!(
        refused(get_item(&get(json!({"pk": {"N": "10E2147483647"}})))),
        OVERFLOW
    );
    assert_eq!(
        refused(get_item(&get(json!({"pk": {"N": "0E2147483648"}})))),
        OVERFLOW
    );
    assert_eq!(
        refused(get_item(&get(json!({"pk": {"N": ""}})))),
        format!("{NUMERIC}: ")
    );
    get_item(&get(json!({"pk": {"N": "5"}, "sk": {"S": "x"}}))).unwrap();
}

#[test]
fn batch_get_keys_answer_without_a_prefix() {
    let keys: Vec<Item> = serde_json::from_value(json!([
        {"pk": {"N": "1"}},
        {"pk": {"N": "1E126"}},
    ]))
    .unwrap();
    assert_eq!(refused(batch_get_keys(&keys)), OVERFLOW);
    batch_get_keys(&keys[..1]).unwrap();
}

fn delete(body: Value) -> DeleteItemInput {
    serde_json::from_value(body).unwrap()
}

#[test]
fn delete_item_key_and_values_carry_the_prefix() {
    let input = delete(json!({
        "TableName": "t",
        "Key": {"pk": {"N": "abc"}},
        "ConditionExpression": "n = :v",
        "ExpressionAttributeValues": {":v": {"N": "1E126"}},
    }));
    assert_eq!(
        refused(delete_item_key(&input)),
        format!("1 validation error detected: {NUMERIC}: abc")
    );
    // No `ExpressionAttributeValues contains invalid value` wrapper.
    assert_eq!(
        refused(delete_item_values(&input)),
        format!("1 validation error detected: {OVERFLOW}")
    );
    let input = delete(json!({"TableName": "t", "Key": {"pk": {"N": ""}}}));
    assert_eq!(
        refused(delete_item_key(&input)),
        format!("1 validation error detected: {NUMERIC}: ")
    );
    delete_item_values(&input).unwrap();
}

#[test]
fn update_item_key_carries_the_prefix() {
    let input: UpdateItemInput = serde_json::from_value(json!({
        "TableName": "t",
        "Key": {"pk": {"N": "1E126"}},
        "UpdateExpression": "SET a = :s",
        "ExpressionAttributeValues": {":s": {"S": "x"}},
    }))
    .unwrap();
    assert_eq!(
        refused(update_item_key(&input)),
        format!("1 validation error detected: {OVERFLOW}")
    );
}

fn put(body: Value) -> PutItemInput {
    serde_json::from_value(body).unwrap()
}

#[test]
fn put_item_values_carry_the_prefix_and_follow_the_item() {
    let input = put(json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}},
        "ConditionExpression": "attribute_not_exists(n)",
        "ExpressionAttributeValues": {":v": {"NS": ["1", "abc"]}},
    }));
    assert_eq!(
        refused(put_item_values(&input)),
        format!("1 validation error detected: {NUMERIC}: abc")
    );
    // The item's number is reported first, by the item check, with its own
    // text; the values check answers only its own placeholders.
    let input = put(json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}, "n": {"N": "xyz"}},
        "ConditionExpression": "n = :v",
        "ExpressionAttributeValues": {":v": {"N": "abc"}},
    }));
    assert_eq!(refused(put_item_item(&input)), format!("{NUMERIC}: xyz"));
    assert_eq!(
        refused(put_item_values(&input)),
        format!("1 validation error detected: {NUMERIC}: abc")
    );
    // Without values, there is nothing for the values check to refuse.
    let input = put(json!({"TableName": "t", "Item": {"pk": {"S": "a"}, "n": {"N": "xyz"}}}));
    put_item_values(&input).unwrap();
}

#[test]
fn transact_get_item_uses_the_32_bit_rules() {
    let item = |n: &str| -> TransactGetItem {
        serde_json::from_value(json!({"Get": {"TableName": "t", "Key": {"pk": {"N": n}}}})).unwrap()
    };
    assert_eq!(
        refused(transact_get_item(&item("10E2147483647"))),
        UNDERFLOW
    );
    assert_eq!(
        refused(transact_get_item(&item("1E2147483648"))),
        format!("{NUMERIC}: 1E2147483648")
    );
    assert_eq!(refused(transact_get_item(&item(""))), NUMERIC);
    transact_get_item(&item("5")).unwrap();
}

#[test]
fn batch_write_item_checks_put_items_and_delete_keys() {
    let batch = |requests: Value| -> BatchWriteItemInput {
        serde_json::from_value(json!({"RequestItems": {"t": requests}})).unwrap()
    };
    for (request, want) in [
        (
            json!({"PutRequest": {"Item": {"pk": {"S": "a"}, "ns": {"NS": ["1", "abc"]}}}}),
            format!("{NUMERIC}: abc"),
        ),
        (
            json!({"PutRequest": {"Item": {"a": {"S": "x"}, "b": {"N": "abc"}}}}),
            format!("{NUMERIC}: abc"),
        ),
        (
            json!({"PutRequest": {"Item": {"pk": {"S": "a"}, "m": {"M": {"x": {"N": "1E126"}}}}}}),
            OVERFLOW.to_owned(),
        ),
        (
            json!({"PutRequest": {"Item": {"pk": {"S": "a"}, "n": {"N": "10E2147483647"}}}}),
            UNDERFLOW.to_owned(),
        ),
        (
            json!({"DeleteRequest": {"Key": {"pk": {"N": "1E126"}}}}),
            OVERFLOW.to_owned(),
        ),
        (
            json!({"DeleteRequest": {"Key": {"pk": {"N": ""}}}}),
            NUMERIC.to_owned(),
        ),
    ] {
        let input = batch(json!([{"PutRequest": {"Item": {"pk": {"S": "ok"}}}}, request]));
        assert_eq!(refused(batch_write_item(&input)), want, "{request}");
    }
    batch_write_item(&batch(
        json!([{"PutRequest": {"Item": {"n": {"N": "1E2"}}}}]),
    ))
    .unwrap();
}

fn transact(action: Value) -> TransactWriteItem {
    serde_json::from_value(action).unwrap()
}

fn wrapped(message: &str, key: &str) -> String {
    format!("ExpressionAttributeValues contains invalid value: {message} for key {key}")
}

#[test]
fn transact_write_item_checks_values_then_key_or_item() {
    // Values come before the key: the service names the value.
    let update = transact(json!({"Update": {
        "TableName": "t",
        "Key": {"pk": {"N": "abc"}},
        "UpdateExpression": "SET a = :v",
        "ExpressionAttributeValues": {":v": {"N": "xyz"}},
    }}));
    assert_eq!(
        refused(transact_write_item(&update)),
        wrapped(&format!("{NUMERIC}: xyz"), ":v")
    );
    let check = transact(json!({"ConditionCheck": {
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "ConditionExpression": "n = :v",
        "ExpressionAttributeValues": {":v": {"N": ""}},
    }}));
    assert_eq!(refused(transact_write_item(&check)), wrapped(NUMERIC, ":v"));
    let put = transact(json!({"Put": {
        "TableName": "t",
        "Item": {"pk": {"S": "a"}, "n": {"N": "xyz"}},
        "ConditionExpression": "n = :v",
        "ExpressionAttributeValues": {":v": {"L": [{"N": "1E126"}]}},
    }}));
    assert_eq!(refused(transact_write_item(&put)), wrapped(OVERFLOW, ":v"));
    let delete = transact(json!({"Delete": {
        "TableName": "t",
        "Key": {"pk": {"N": "1E2147483647"}},
    }}));
    assert_eq!(refused(transact_write_item(&delete)), UNDERFLOW);
}

#[test]
fn transact_put_and_delete_values_are_checked_only_with_a_condition() {
    for action in ["Put", "Delete"] {
        let field = if action == "Put" { "Item" } else { "Key" };
        let mut body = json!({
            "TableName": "t",
            field: {"pk": {"S": "a"}},
            "ExpressionAttributeValues": {":v": {"N": "abc"}},
        });
        transact_write_item(&transact(json!({action: body.clone()}))).unwrap();
        // An empty ConditionExpression counts as present.
        body["ConditionExpression"] = json!("");
        assert_eq!(
            refused(transact_write_item(&transact(json!({action: body})))),
            wrapped(&format!("{NUMERIC}: abc"), ":v"),
            "{action}"
        );
    }
}

#[test]
fn several_malformed_values_are_reported_in_placeholder_order() {
    // The service named either value on two sends of one PutItem request;
    // these checks pin the first placeholder in name order.
    let input = put(json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}},
        "ConditionExpression": "n = :b OR n = :a",
        "ExpressionAttributeValues": {":b": {"N": "abc"}, ":a": {"N": "xyz"}},
    }));
    assert_eq!(
        refused(put_item_values(&input)),
        format!("1 validation error detected: {NUMERIC}: xyz")
    );
}

/// A value nested `levels` maps deep.
fn deep(levels: usize) -> Value {
    let mut value = json!({"S": "leaf"});
    for _ in 0..levels {
        value = json!({"M": {"a": value}});
    }
    value
}

const NESTING: &str = "Nesting Levels have exceeded supported limits: \
                       Attributes in the item have nested levels beyond supported limit";

#[test]
fn a_value_nested_too_deep_is_reported_before_a_malformed_number() {
    // A BatchWriteItem put item holding both a 33-level value and a number
    // the service cannot read answers the nesting message, whichever comes
    // first in the item (measured 2026-10-27, the same on both sends). The
    // PutItem and DeleteItem condition values answer it too, the order they
    // answered before this check; the service answered the nesting on one
    // send and the number on the other there.
    let batch = |requests: Value| -> BatchWriteItemInput {
        serde_json::from_value(json!({"RequestItems": {"t": requests}})).unwrap()
    };

    let values = put(json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}},
        "ConditionExpression": "n = :d OR n = :v",
        "ExpressionAttributeValues": {":d": deep(33), ":v": {"N": "abc"}},
    }));
    assert_eq!(
        refused(put_item_values(&values)),
        format!("1 validation error detected: {NESTING}")
    );

    let batch_input = batch(json!([
        {"PutRequest": {"Item": {"pk": {"S": "a"}, "n": {"N": "abc"}, "d": deep(33)}}}
    ]));
    assert_eq!(refused(batch_write_item(&batch_input)), NESTING);

    // The earlier request's malformed number still wins over a later
    // request's nesting (also measured).
    let across = batch(json!([
        {"PutRequest": {"Item": {"pk": {"S": "a"}, "n": {"N": "abc"}}}},
        {"PutRequest": {"Item": {"pk": {"S": "b"}, "d": deep(33)}}}
    ]));
    assert_eq!(
        refused(batch_write_item(&across)),
        format!("{NUMERIC}: abc")
    );
}

#[test]
fn the_put_item_item_reports_a_malformed_number_before_a_value_nested_too_deep() {
    // The order `validate_put_item` checks them in, and so what PutItem
    // answered before this check. The service follows the request body,
    // which `Item` does not keep: a malformed `n` ahead of a 33-level `d`
    // answered the number on both sends (measured 2026-10-27).
    let item = put(json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}, "n": {"N": "abc"}, "d": deep(33)},
    }));
    assert_eq!(refused(put_item_item(&item)), format!("{NUMERIC}: abc"));
    let item = put(json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}, "d": deep(33)},
    }));
    assert_eq!(refused(put_item_item(&item)), NESTING);
}

#[test]
fn put_item_checks_its_item_without_expression_attribute_values() {
    // The item is checked whether or not the request carries values: measured
    // 2026-10-27, PutItem to a table that does not exist answers the item's
    // malformed number, not ResourceNotFoundException.
    let input = put(json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}, "n": {"N": "abc"}},
    }));
    assert_eq!(refused(put_item_item(&input)), format!("{NUMERIC}: abc"));
}

#[test]
fn batch_write_item_visits_tables_in_name_order() {
    // Two tables each holding a malformed number: the answer names the
    // first table in name order, so one request cannot answer differently
    // from one run to the next.
    let input: BatchWriteItemInput = serde_json::from_value(json!({"RequestItems": {
        "t2": [{"PutRequest": {"Item": {"n": {"N": "xyz"}}}}],
        "t1": [{"DeleteRequest": {"Key": {"pk": {"N": "abc"}}}}],
    }}))
    .unwrap();
    assert_eq!(refused(batch_write_item(&input)), format!("{NUMERIC}: abc"));
}
