// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use super::*;
use crate::expression::CompareOp;

const HASH_MSG_2048: &str = "One or more parameter values were invalid: \
    Size of hashkey has exceeded the maximum size limit of2048 bytes";
const HASH_TOTAL_MSG_2048: &str = "One or more parameter values were invalid: \
    Aggregated size of all hash keys has exceeded the size limit of 2048 bytes";
const RANGE_MSG_1024: &str = "One or more parameter values were invalid: \
    Aggregated size of all range keys has exceeded the size limit of 1024 bytes";

fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn path(name: &str) -> Vec<PathElement> {
    vec![PathElement::Attribute(name.to_owned())]
}

fn placeholder(name: &str) -> Expr {
    Expr::Placeholder(name.to_owned())
}

fn eq(name: &str, value: &str) -> SortKeyCondition {
    SortKeyCondition::Compare {
        path: path(name),
        op: CompareOp::Eq,
        value: placeholder(value),
    }
}

fn compare(name: &str, op: CompareOp, value: &str) -> SortKeyCondition {
    SortKeyCondition::Compare {
        path: path(name),
        op,
        value: placeholder(value),
    }
}

fn maps(values: &[(&str, usize)]) -> ExpressionMaps {
    let values: HashMap<String, AttributeValue> = values
        .iter()
        .map(|(name, len)| ((*name).to_owned(), AttributeValue::S("x".repeat(*len))))
        .collect();
    ExpressionMaps::new(HashMap::new(), values)
}

/// Placeholder values with their content, for tests where the bytes matter.
fn maps_with(values: Vec<(&str, AttributeValue)>) -> ExpressionMaps {
    let values: HashMap<String, AttributeValue> = values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
    ExpressionMaps::new(HashMap::new(), values)
}

fn s_of(fill: &str, len: usize) -> AttributeValue {
    AttributeValue::S(fill.repeat(len))
}

fn base_schema() -> Vec<KeySchemaElement> {
    vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)]
}

/// `h` plus RANGE `a`, `b`, `c`, `d`: the index `idx` of the measured rows
/// (service, 2026-10-21 and 2026-10-23).
fn multi_range_schema() -> Vec<KeySchemaElement> {
    vec![
        ks("h", KeyType::Hash),
        ks("a", KeyType::Range),
        ks("b", KeyType::Range),
        ks("c", KeyType::Range),
        ks("d", KeyType::Range),
    ]
}

/// HASH `h1`, `h2` plus RANGE `r`: the index `two-hash` of the measured rows
/// (service, 2026-10-23).
fn two_hash_schema() -> Vec<KeySchemaElement> {
    vec![
        ks("h1", KeyType::Hash),
        ks("h2", KeyType::Hash),
        ks("r", KeyType::Range),
    ]
}

fn hash_only(name: &str, value: &str) -> KeyCondition {
    KeyCondition {
        pk_path: path(name),
        pk_value: placeholder(value),
        extra_pk_conditions: Vec::new(),
        sk_condition: None,
        extra_sk_conditions: Vec::new(),
    }
}

fn check(
    kc: &KeyCondition,
    schema: &[KeySchemaElement],
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    validate_query_key_sizes(kc, schema, maps, &LimitsConfig::default())
}

fn message(result: Result<(), DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(msg)) => msg,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

#[test]
fn hash_value_of_2048_bytes_is_accepted() {
    let kc = hash_only("pk", "v");
    assert!(check(&kc, &base_schema(), &maps(&[("v", 2048)])).is_ok());
}

#[test]
fn hash_value_of_2049_bytes_is_refused_with_the_service_sentence() {
    let kc = hash_only("pk", "v");
    let msg = message(check(&kc, &base_schema(), &maps(&[("v", 2049)])));
    assert_eq!(msg, HASH_MSG_2048);
}

#[test]
fn hash_sentence_has_no_space_between_of_and_the_number() {
    let kc = hash_only("pk", "v");
    let msg = message(check(&kc, &base_schema(), &maps(&[("v", 2049)])));
    assert!(msg.ends_with("limit of2048 bytes"), "got: {msg}");
}

#[test]
fn range_value_of_1024_bytes_is_accepted() {
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(eq("sk", "v"));
    assert!(check(&kc, &base_schema(), &maps(&[("p", 1), ("v", 1024)])).is_ok());
}

#[test]
fn range_value_of_1025_bytes_is_refused_with_the_service_sentence() {
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(eq("sk", "v"));
    let msg = message(check(&kc, &base_schema(), &maps(&[("p", 1), ("v", 1025)])));
    assert_eq!(msg, RANGE_MSG_1024);
}

#[test]
fn two_range_values_are_added_together() {
    // Measured 2026-10-23 on `idx`: `a` and `c` of 600 bytes with `b` of 1
    // byte is refused, `a` of 600 bytes alone passes. Each value is under the
    // limit alone; together they are not.
    let mut kc = hash_only("h", "h");
    kc.sk_condition = Some(eq("a", "x"));
    kc.extra_sk_conditions = vec![(path("b"), placeholder("y")), (path("c"), placeholder("z"))];
    let m = maps(&[("h", 1), ("x", 600), ("y", 1), ("z", 600)]);
    assert_eq!(
        message(check(&kc, &multi_range_schema(), &m)),
        RANGE_MSG_1024
    );
}

#[test]
fn one_600_byte_range_value_on_a_multi_range_index_is_accepted() {
    // Measured 2026-10-23 on `idx`.
    let mut kc = hash_only("h", "h");
    kc.sk_condition = Some(eq("a", "x"));
    let m = maps(&[("h", 1), ("x", 600)]);
    assert!(check(&kc, &multi_range_schema(), &m).is_ok());
}

#[test]
fn two_range_values_totalling_exactly_1024_are_accepted_and_1025_refused() {
    // Measured 2026-10-23 on `idx`: 512 and 512 pass, 512 and 513 are refused.
    let mut kc = hash_only("h", "h");
    kc.sk_condition = Some(eq("a", "x"));
    kc.extra_sk_conditions = vec![(path("b"), placeholder("y"))];
    let m = maps(&[("h", 1), ("x", 512), ("y", 512)]);
    assert!(check(&kc, &multi_range_schema(), &m).is_ok());
    let m = maps(&[("h", 1), ("x", 512), ("y", 513)]);
    assert_eq!(
        message(check(&kc, &multi_range_schema(), &m)),
        RANGE_MSG_1024
    );
}

#[test]
fn a_non_equality_on_a_later_range_attribute_is_added_to_the_total() {
    // Measured 2026-10-23 on `idx`: `a` of 1 byte with `b > :v` of 1025 bytes
    // is refused.
    let mut kc = hash_only("h", "h");
    kc.extra_sk_conditions = vec![(path("a"), placeholder("x"))];
    kc.sk_condition = Some(compare("b", CompareOp::Gt, "v"));
    let m = maps(&[("h", 1), ("x", 1), ("v", 1025)]);
    assert_eq!(
        message(check(&kc, &multi_range_schema(), &m)),
        RANGE_MSG_1024
    );
}

#[test]
fn clauses_are_assigned_by_attribute_name_not_by_field() {
    // The parser leaves RANGE equalities in extra_pk_conditions until the
    // schema classifier moves them. The sizes still add up by name, so the
    // measured `idx` rows hold whether or not the classifier has run.
    let mut kc = hash_only("h", "h");
    kc.extra_pk_conditions = vec![(path("a"), placeholder("x"))];
    kc.sk_condition = Some(eq("b", "y"));
    let m = maps(&[("h", 1), ("x", 600), ("y", 600)]);
    assert_eq!(
        message(check(&kc, &multi_range_schema(), &m)),
        RANGE_MSG_1024
    );
}

fn between(name: &str, low: &str, high: &str) -> SortKeyCondition {
    SortKeyCondition::Between {
        path: path(name),
        low: placeholder(low),
        high: placeholder(high),
    }
}

#[test]
fn between_contributes_its_larger_bound_not_the_sum() {
    // Measured 2026-10-23: 600 and 600 (sum 1200) pass.
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(between("sk", "l", "u"));
    let m = maps(&[("p", 1), ("l", 600), ("u", 600)]);
    assert!(check(&kc, &base_schema(), &m).is_ok());
}

#[test]
fn between_with_a_1025_byte_upper_bound_is_refused() {
    // Measured 2026-10-23: bounds of 1 and 1025 bytes.
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(between("sk", "l", "u"));
    let m = maps(&[("p", 1), ("l", 1), ("u", 1025)]);
    assert_eq!(message(check(&kc, &base_schema(), &m)), RANGE_MSG_1024);
}

#[test]
fn between_with_a_1025_byte_lower_bound_is_refused() {
    // Measured 2026-10-23: lower bound `a` x 1025, upper bound `z` (an ordered
    // pair) is refused with the range sentence, and `a` x 1024 passes. The
    // reversed pair, a 1025-byte lower bound above a 1-byte upper bound, is
    // refused by the service with a bound-order sentence ahead of the size
    // check; ExtendDB has no bound-order check yet, and this test pins only
    // the sizing of the ordered pair.
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(between("sk", "l", "u"));
    let m = maps_with(vec![
        ("p", s_of("p", 1)),
        ("l", s_of("a", 1025)),
        ("u", s_of("z", 1)),
    ]);
    assert_eq!(message(check(&kc, &base_schema(), &m)), RANGE_MSG_1024);
    let m = maps_with(vec![
        ("p", s_of("p", 1)),
        ("l", s_of("a", 1024)),
        ("u", s_of("z", 1)),
    ]);
    assert!(check(&kc, &base_schema(), &m).is_ok());
}

#[test]
fn a_between_adds_its_larger_bound_to_the_range_total() {
    // Measured 2026-10-23 on `idx`: `a` of 600 bytes with `b BETWEEN` 1 and
    // 424 bytes (total 1024) passes, with 1 and 600 bytes (total 1200) is
    // refused.
    let mut kc = hash_only("h", "h");
    kc.extra_sk_conditions = vec![(path("a"), placeholder("x"))];
    kc.sk_condition = Some(between("b", "l", "u"));
    let m = maps(&[("h", 1), ("x", 600), ("l", 1), ("u", 424)]);
    assert!(check(&kc, &multi_range_schema(), &m).is_ok());
    let m = maps(&[("h", 1), ("x", 600), ("l", 1), ("u", 600)]);
    assert_eq!(
        message(check(&kc, &multi_range_schema(), &m)),
        RANGE_MSG_1024
    );
}

#[test]
fn every_comparison_operator_and_begins_with_are_sized() {
    // Measured 2026-10-23: a 1025-byte value on each form is refused and a
    // 1024-byte value passes.
    let forms: Vec<fn(&str) -> SortKeyCondition> = vec![
        |v| compare("sk", CompareOp::Gt, v),
        |v| compare("sk", CompareOp::Ge, v),
        |v| compare("sk", CompareOp::Lt, v),
        |v| compare("sk", CompareOp::Le, v),
        |v| SortKeyCondition::BeginsWith {
            path: path("sk"),
            prefix: placeholder(v),
        },
    ];
    for form in forms {
        let mut kc = hash_only("pk", "p");
        kc.sk_condition = Some(form("v"));
        let m = maps(&[("p", 1), ("v", 1025)]);
        assert_eq!(message(check(&kc, &base_schema(), &m)), RANGE_MSG_1024);
        let m = maps(&[("p", 1), ("v", 1024)]);
        assert!(check(&kc, &base_schema(), &m).is_ok());
    }
}

#[test]
fn the_hash_sentence_wins_when_both_limits_are_exceeded() {
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(eq("sk", "v"));
    let m = maps(&[("p", 2049), ("v", 1025)]);
    assert_eq!(message(check(&kc, &base_schema(), &m)), HASH_MSG_2048);
}

fn two_hash_condition() -> KeyCondition {
    let mut kc = hash_only("h1", "x");
    kc.extra_pk_conditions = vec![(path("h2"), placeholder("y"))];
    kc
}

#[test]
fn one_hash_value_over_the_limit_gets_the_single_sentence() {
    // Measured 2026-10-23 on `two-hash`: `h1` of 2049 bytes and `h2` of 1 byte.
    let kc = two_hash_condition();
    let m = maps(&[("x", 2049), ("y", 1)]);
    assert_eq!(message(check(&kc, &two_hash_schema(), &m)), HASH_MSG_2048);
    let m = maps(&[("x", 1), ("y", 2049)]);
    assert_eq!(message(check(&kc, &two_hash_schema(), &m)), HASH_MSG_2048);
}

#[test]
fn hash_values_that_add_up_over_the_limit_get_the_aggregated_sentence() {
    // Measured 2026-10-23 on `two-hash`: 1025 and 1024 bytes (sum 2049, neither
    // over) and 2048 and 2048 bytes are both refused.
    let kc = two_hash_condition();
    for (x, y) in [(1025, 1024), (2048, 2048)] {
        let m = maps(&[("x", x), ("y", y)]);
        assert_eq!(
            message(check(&kc, &two_hash_schema(), &m)),
            HASH_TOTAL_MSG_2048,
            "{x} + {y}"
        );
    }
}

#[test]
fn hash_values_that_add_up_to_exactly_the_limit_are_accepted() {
    // Not measured: a sum of exactly 2048 bytes. Treated as within the limit,
    // as a single 2048-byte value is (measured).
    let kc = two_hash_condition();
    let m = maps(&[("x", 1024), ("y", 1024)]);
    assert!(check(&kc, &two_hash_schema(), &m).is_ok());
}

#[test]
fn the_aggregated_hash_sentence_has_a_space_before_the_number() {
    let kc = two_hash_condition();
    let m = maps(&[("x", 1025), ("y", 1024)]);
    let msg = message(check(&kc, &two_hash_schema(), &m));
    assert!(msg.ends_with("size limit of 2048 bytes"), "got: {msg}");
}

#[test]
fn a_hash_sum_over_the_limit_wins_over_the_range_sum() {
    // Not measured: both the HASH sum and the RANGE sum over. The hash
    // sentences come first, as the single hash sentence does (measured).
    let mut kc = two_hash_condition();
    kc.sk_condition = Some(eq("r", "z"));
    let m = maps(&[("x", 1025), ("y", 1024), ("z", 1025)]);
    assert_eq!(
        message(check(&kc, &two_hash_schema(), &m)),
        HASH_TOTAL_MSG_2048
    );
}

#[test]
fn the_single_hash_sentence_wins_over_the_aggregated_one() {
    let kc = two_hash_condition();
    let m = maps(&[("x", 2049), ("y", 2048)]);
    assert_eq!(message(check(&kc, &two_hash_schema(), &m)), HASH_MSG_2048);
}

#[test]
fn binary_values_count_their_bytes() {
    let kc = hash_only("pk", "v");
    let values = HashMap::from([("v".to_owned(), AttributeValue::B(vec![0u8; 2049]))]);
    let m = ExpressionMaps::new(HashMap::new(), values);
    assert_eq!(message(check(&kc, &base_schema(), &m)), HASH_MSG_2048);
}

#[test]
fn multibyte_strings_count_utf8_bytes_not_characters() {
    // Measured 2026-10-23: 683 U+20AC (3 bytes each, 2049 bytes) in the hash
    // value is refused; 682 of them plus `xx` (2048 bytes) passes. In a range
    // value, 342 of them (1026 bytes) is refused and 341 plus `x` (1024 bytes)
    // passes.
    let euro = "\u{20ac}";
    let hash = |value: String| {
        let kc = hash_only("pk", "v");
        let m = maps_with(vec![("v", AttributeValue::S(value))]);
        check(&kc, &base_schema(), &m)
    };
    assert_eq!(message(hash(euro.repeat(683))), HASH_MSG_2048);
    assert!(hash(format!("{}xx", euro.repeat(682))).is_ok());
    let range = |value: String| {
        let mut kc = hash_only("pk", "p");
        kc.sk_condition = Some(eq("sk", "v"));
        let m = maps_with(vec![
            ("p", AttributeValue::S("p".to_owned())),
            ("v", AttributeValue::S(value)),
        ]);
        check(&kc, &base_schema(), &m)
    };
    assert_eq!(message(range(euro.repeat(342))), RANGE_MSG_1024);
    assert!(range(format!("{}x", euro.repeat(341))).is_ok());
}

#[test]
fn name_references_resolve_through_expression_attribute_names() {
    let kc = hash_only("#p", "v");
    let m = ExpressionMaps::new(
        HashMap::from([("p".to_owned(), "pk".to_owned())]),
        HashMap::from([("v".to_owned(), AttributeValue::S("x".repeat(2049)))]),
    );
    assert_eq!(message(check(&kc, &base_schema(), &m)), HASH_MSG_2048);
}

#[test]
fn limits_come_from_the_configuration_and_the_sentence_names_them() {
    let limits = LimitsConfig {
        max_partition_key_size_bytes: 10,
        max_sort_key_size_bytes: 20,
        ..LimitsConfig::default()
    };
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(eq("sk", "v"));
    let hash =
        validate_query_key_sizes(&kc, &base_schema(), &maps(&[("p", 11), ("v", 1)]), &limits);
    assert!(message(hash).ends_with("limit of10 bytes"));
    let range =
        validate_query_key_sizes(&kc, &base_schema(), &maps(&[("p", 10), ("v", 21)]), &limits);
    assert!(message(range).ends_with("limit of 20 bytes"));
}

#[test]
fn a_clause_on_an_attribute_outside_the_key_schema_adds_nothing() {
    let kc = hash_only("other", "v");
    assert!(check(&kc, &base_schema(), &maps(&[("v", 5000)])).is_ok());
}

#[test]
fn an_undefined_value_placeholder_on_the_clause_itself_returns_ok() {
    let kc = hash_only("pk", "missing");
    assert!(check(&kc, &base_schema(), &maps(&[("v", 5000)])).is_ok());
}

#[test]
fn an_undefined_name_reference_on_the_clause_itself_returns_ok() {
    let kc = hash_only("#nope", "v");
    assert!(check(&kc, &base_schema(), &maps(&[("v", 5000)])).is_ok());
}

#[test]
fn an_undefined_value_in_another_clause_wins_over_the_size_of_a_defined_one() {
    // Measured 2026-10-23: `pk = :v AND sk = :missing` with `:v` of 2049 bytes
    // is refused as an undefined attribute value, not as a size.
    let mut kc = hash_only("pk", "v");
    kc.sk_condition = Some(eq("sk", "missing"));
    assert!(check(&kc, &base_schema(), &maps(&[("v", 2049)])).is_ok());
}

#[test]
fn an_undefined_name_in_another_clause_wins_over_the_size_of_a_defined_one() {
    // Measured 2026-10-23: `pk = :v AND #n = :w` with `#n` undefined and `:v`
    // of 2049 bytes is refused as an undefined attribute name, not as a size.
    let mut kc = hash_only("pk", "v");
    kc.sk_condition = Some(eq("#n", "w"));
    assert!(check(&kc, &base_schema(), &maps(&[("v", 2049), ("w", 1)])).is_ok());
}

#[test]
fn an_undefined_reference_wins_whatever_its_position() {
    // The oversized clause may come before or after the undefined one.
    let mut kc = hash_only("pk", "missing");
    kc.sk_condition = Some(eq("sk", "v"));
    assert!(check(&kc, &base_schema(), &maps(&[("v", 5000)])).is_ok());
}

#[test]
fn an_undefined_bound_of_a_between_returns_ok() {
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(between("sk", "l", "missing"));
    assert!(check(&kc, &base_schema(), &maps(&[("p", 1), ("l", 5000)])).is_ok());
}

#[test]
fn non_scalar_values_are_skipped() {
    let kc = hash_only("pk", "v");
    let values = HashMap::from([(
        "v".to_owned(),
        AttributeValue::L(vec![AttributeValue::S("x".repeat(5000))]),
    )]);
    let m = ExpressionMaps::new(HashMap::new(), values);
    assert!(check(&kc, &base_schema(), &m).is_ok());
}

#[test]
fn number_values_are_sized_by_their_digit_string() {
    // The service refuses a number of more than 38 significant digits before
    // any size check (measured 2026-10-23). ExtendDB does not validate a
    // number in a key condition, so a long digit string reaches the size
    // check and is sized by its length.
    let kc = hash_only("pk", "v");
    let values = HashMap::from([("v".to_owned(), AttributeValue::N("1".repeat(2049)))]);
    let m = ExpressionMaps::new(HashMap::new(), values);
    assert_eq!(message(check(&kc, &base_schema(), &m)), HASH_MSG_2048);
}
